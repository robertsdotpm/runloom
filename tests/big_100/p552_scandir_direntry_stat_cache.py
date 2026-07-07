"""big_100 / 552 -- os.scandir C-iterator cursor + DirEntry lazy-stat cache under M:N.

os.scandir() returns a C-level iterator object that holds an OPEN directory
cursor (a POSIX DIR* / fdopendir handle on Linux) and yields DirEntry objects.
Two pieces of hidden C state make this a distinct M:N hazard from p304's
itertools/tee C-iterator:

  1. THE DIRECTORY CURSOR.  The ScandirIterator keeps the DIR* positioned at the
     next directory entry.  Each __next__ calls readdir() on that cursor and
     advances it.  With the GIL off, __next__ does not hold a global lock; if the
     iterator is PARKED mid-walk (a cooperative yield between two __next__ pulls)
     and RESUMED on a different hub, a desynced cursor could re-read an entry
     (DUPLICATE name), skip one (MISSING name), or torn-read the readdir buffer
     (an EXTRA / garbage name, or a crash).

  2. THE PER-DirEntry LAZY STAT CACHE.  A DirEntry caches its type and stat()
     result lazily: the FIRST is_file()/is_dir()/stat() triggers an lstat/fstatat
     (or uses the d_type readdir already returned) and MEMOIZES it on the entry;
     later calls return the cached value with no syscall.  If that memoization
     races a park+resume (the cache is written on one hub, read on another), a
     DirEntry could report a type/size that CHANGES across a yield -- a torn or
     lost cache write.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom parks a fiber at
a cooperative yield and may resume it on ANY hub.  If the ScandirIterator's DIR*
cursor, or a DirEntry's memoized stat, is not carried faithfully across that hub
migration, the walk loses/duplicates an entry or a DirEntry's cached fields
mutate under the fiber's feet -- neither of which may happen on a correct runtime.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, closed-world):

  Each fiber owns its OWN private directory, created once and NEVER touched by any
  sibling: a KNOWN, fixed set of entries -- FILE_COUNT regular files with distinct
  known byte-sizes and DIR_COUNT subdirectories, all with unique deterministic
  names.  Because the directory is single-owner and immutable for the fiber's
  lifetime, the correct result of scandir()ing it is KNOWN IN ADVANCE with zero
  baseline recording, and no cooperative lock is needed (there is no sharing to
  serialize -- this is the p490-style single-owner isolation shape, not the p405
  shared-object-under-lock shape).

  Per round the fiber walks its own directory with `with os.scandir(path) as it:`
  and, for EACH DirEntry, mid-walk:

    * CONSERVATION (closed-world multiset law).  It collects every DirEntry.name.
      After the full walk, the multiset of names it saw MUST equal exactly the set
      of names it created -- no MISSING entry (cursor skipped), no EXTRA/garbage
      entry (torn readdir), no DUPLICATE (cursor re-read).  Since names are unique
      the multiset is a set; a duplicate is itself a violation.

    * IDENTITY-STABILITY across a yield.  For each entry it reads is_file(),
      is_dir(), and (for files) stat().st_size BEFORE a cooperative yield taken
      mid-walk, then RE-READS all three AFTER the yield and asserts they are
      UNCHANGED.  The first read populates the DirEntry's lazy cache; the second
      must return the identical memoized value even though the fiber may have
      migrated hubs in between.  A changed field is a torn/lost cache write.

    * CORRECTNESS against the closed form.  Each name must be one this fiber
      created, with the RIGHT kind (file vs dir) and, for files, the RIGHT
      st_size -- so a cursor that returned a stale/aliased entry from an earlier
      state (or another fiber's entry) is caught even if it happens to be a
      well-formed name.

  We verified the closed-world expectation with a plain-threads control (8 OS
  threads, each scandiring its own private dir with yields inside the walk, GIL on
  AND off): 100% of walks return exactly the created set with identity-stable
  DirEntry fields -- 0 lost/extra/dup, 0 cache mutations.  Under a CORRECT runloom
  it must also hold, so this single-owner oracle PASSES (exit 0) when there is no
  bug.  A FAIL means the scandir cursor or the DirEntry stat cache desynced across
  a park+hub-migration -- a real runtime bug.

ORACLES:
  * LOAD-BEARING -- SCANDIR CONSERVATION + DirEntry CACHE STABILITY (worker, HARD,
    fail-fast).  Single-owner private dir; multiset-of-names == created set, and
    each DirEntry's is_file()/is_dir()/stat().st_size identity-stable across a
    yield taken mid-walk.  H.fail on missing/extra/dup name, wrong kind/size, or a
    changed cached field.  The scandir iterator is closed in a `with` (finally).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-walk (parked
    inside __next__ on a desynced DIR* cursor and never rewoken) never returns; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually walked (scans > 0 and
    entries seen > 0).

FAIL ON: a scandir walk missing/adding/duplicating a known entry, a DirEntry
reporting the wrong kind or size for a known name, or a DirEntry's cached
is_file()/is_dir()/stat().st_size CHANGING across a mid-walk yield.  There is no
shared-object arm here -- the directory is single-owner, so every observation is a
legitimate runtime oracle, not documented shared-mutation semantics.

Stresses: os.scandir ScandirIterator DIR*/readdir cursor advance across a
park+hub-migration, DirEntry lazy stat/type memoization (d_type + fstatat cache)
read/written across a yield, iterator close-in-finally, single-owner closed-world
directory conservation under M:N churn.

Good TSan / controlled-M:N-replay target: the ScandirIterator's DIR* cursor and
each DirEntry's cached stat struct are C fields advanced/written by __next__ /
is_file / stat with no GIL; a data-race report on the cursor or the cached
st_size, or a replay that drops/duplicates a readdir entry across a forced hub
migration, localizes the desync before the conservation sum even closes.
"""
import os

import harness
import runloom

# Per-fiber private directory layout.  A fixed, KNOWN set of entries so the
# closed-world oracle needs zero baseline recording.  Small enough that a full
# walk is quick (bounded work per round), large enough that the readdir cursor
# takes several __next__ pulls (so a mid-walk park lands between real advances)
# and the entries span both kinds with distinct file sizes.
FILE_COUNT = 12          # regular files f00..f11, file i has (i+1) known bytes
DIR_COUNT = 5            # subdirectories d00..d04
# Distinct, nonzero, deterministic byte-size per file so a stale/aliased stat
# cache (reporting another entry's size) is caught, not just a torn one.
FILE_SIZE = lambda i: (i + 1) * 3


def file_name(i):
    return "f{0:02d}".format(i)


def dir_name(j):
    return "d{0:02d}".format(j)


def build_private_dir(base, wid):
    """Create THIS fiber's private directory of KNOWN entries and return
    (path, expected).  expected maps name -> ("file", size) or ("dir", None).

    Single-owner: the directory belongs to exactly one fiber (subdir keyed by wid)
    and is never touched by a sibling, so its correct scandir result is fixed."""
    path = os.path.join(base, "w{0}".format(wid))
    os.makedirs(path, exist_ok=True)
    expected = {}
    for i in range(FILE_COUNT):
        name = file_name(i)
        size = FILE_SIZE(i)
        with open(os.path.join(path, name), "wb") as f:
            f.write(b"x" * size)
        expected[name] = ("file", size)
    for j in range(DIR_COUNT):
        name = dir_name(j)
        os.mkdir(os.path.join(path, name))
        expected[name] = ("dir", None)
    return path, expected


def walk_once(H, wid, path, expected, state):
    """One single-owner scandir walk with a mid-walk yield per entry.

    LOAD-BEARING oracle (fail-fast): conservation (multiset of names == created
    set), DirEntry cache identity-stability across the yield, and correctness
    (right kind + size) against the closed form.  Returns the number of entries
    walked (0 if the round was cut short by shutdown before a full walk)."""
    seen = []
    entries = 0
    with os.scandir(path) as it:            # DIR* cursor; closed in the `with`
        for entry in it:
            name = entry.name

            # First read: populates the DirEntry's lazy type/stat cache.
            is_f0 = entry.is_file()
            is_d0 = entry.is_dir()
            size0 = entry.stat().st_size if is_f0 else None

            # PARK mid-walk: the ScandirIterator's DIR* cursor is held open and
            # this fiber may resume on a DIFFERENT hub.  yield_now forces the
            # scheduler to consider a sibling; an occasional tiny sleep parks the
            # fiber long enough that a hub migration reliably interleaves.
            runloom.yield_now()
            if entries & 1:
                runloom.sleep(0.0002)

            # Second read: MUST return the identical memoized cache values even
            # though the fiber may have migrated hubs across the yield.
            is_f1 = entry.is_file()
            is_d1 = entry.is_dir()
            size1 = entry.stat().st_size if is_f1 else None

            if is_f1 != is_f0 or is_d1 != is_d0:
                H.fail("DirEntry TYPE CHANGED across a mid-walk yield: {0!r} in "
                       "wid {1} was is_file={2}/is_dir={3} before the yield and "
                       "is_file={4}/is_dir={5} after -- the DirEntry's memoized "
                       "type cache was torn/lost across a park+hub-migration"
                       .format(name, wid, is_f0, is_d0, is_f1, is_d1))
                return entries
            if size0 != size1:
                H.fail("DirEntry st_size CHANGED across a mid-walk yield: {0!r} "
                       "in wid {1} was {2} before the yield and {3} after -- the "
                       "DirEntry's cached stat was torn/lost across a park+hub-"
                       "migration".format(name, wid, size0, size1))
                return entries

            # Correctness against the closed form: known name, right kind + size.
            exp = expected.get(name)
            if exp is None:
                H.fail("scandir yielded OUT-OF-UNIVERSE name {0!r} in wid {1} -- "
                       "a torn/garbage readdir entry, or another fiber's entry "
                       "leaked through the shared DIR* cursor across a hub "
                       "migration".format(name, wid))
                return entries
            kind, exp_size = exp
            if kind == "file":
                if not is_f1 or is_d1:
                    H.fail("scandir reported wrong kind for FILE {0!r} in wid "
                           "{1}: is_file={2} is_dir={3} (expected file) -- a "
                           "desynced DirEntry type cache".format(
                               name, wid, is_f1, is_d1))
                    return entries
                if size1 != exp_size:
                    H.fail("scandir reported wrong st_size for FILE {0!r} in wid "
                           "{1}: got {2}, expected {3} -- a stale/aliased stat "
                           "cache returned another entry's or an earlier state's "
                           "size".format(name, wid, size1, exp_size))
                    return entries
            else:  # dir
                if not is_d1 or is_f1:
                    H.fail("scandir reported wrong kind for DIR {0!r} in wid {1}: "
                           "is_file={2} is_dir={3} (expected dir) -- a desynced "
                           "DirEntry type cache".format(name, wid, is_f1, is_d1))
                    return entries

            seen.append(name)
            entries += 1

    # ---- CONSERVATION (closed-world multiset law) over the completed walk -----
    # Multiset of names seen MUST equal exactly the created set.  Because names
    # are unique, len(seen) != len(expected) OR a duplicate OR a missing name is a
    # cursor desync (skip / re-read / torn advance across the park).
    if len(seen) != len(expected):
        H.fail("scandir conservation broken in wid {0}: walked {1} entries but "
               "created {2} -- the DIR* cursor {3} an entry across a park+hub-"
               "migration".format(
                   wid, len(seen), len(expected),
                   "DROPPED" if len(seen) < len(expected) else "DUPLICATED/ADDED"))
        return entries
    seen_set = set(seen)
    if len(seen_set) != len(seen):
        H.fail("scandir conservation broken in wid {0}: a name was DUPLICATED in "
               "one walk (saw {1} names, {2} distinct) -- the DIR* cursor re-read "
               "an entry across a park+hub-migration".format(
                   wid, len(seen), len(seen_set)))
        return entries
    if seen_set != set(expected):
        missing = set(expected) - seen_set
        extra = seen_set - set(expected)
        H.fail("scandir conservation broken in wid {0}: name set mismatch -- "
               "missing={1!r} extra={2!r} (cursor desync across a park)".format(
                   wid, sorted(missing), sorted(extra)))
        return entries
    return entries


def worker(H, wid, rng, state):
    """Each fiber owns a private KNOWN directory (built once) and repeatedly
    scandirs it, yielding mid-walk so a sibling reliably interleaves and the
    fiber migrates hubs with the DIR* cursor / DirEntry cache in flight.  Every
    check is single-owner -- there is no sharing to race, so a failure is a
    runloom cursor/cache desync, not documented shared-object semantics."""
    base = state["base"]
    path, expected = build_private_dir(base, wid)
    for _ in H.round_range():
        if not H.running():
            break
        n = walk_once(H, wid, path, expected, state)
        if H.failed:
            return
        state["entries"][wid] += n          # single-writer-per-slot, race-free
        state["scans"][wid] += 1            # single-writer-per-slot, race-free
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # One base tmpdir for the whole run (rmtree'd at the end); each worker carves
    # out its own private subdir under it.  Per-wid conservation/non-vacuity
    # tallies are [0]*H.funcs, single-writer-per-slot (race-free GIL-off; see
    # HARD RULE 1) -- allocated here where H.funcs is known.
    base = H.make_tmpdir("big100_scandir_")
    H.state = {
        "base": base,
        "scans": [0] * H.funcs,             # completed walks per worker
        "entries": [0] * H.funcs,           # DirEntry observations per worker
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    scans = sum(H.state["scans"])
    entries = sum(H.state["entries"])
    H.log("scandir[single-owner LOAD-BEARING]: {0} closed-world walks, {1} "
          "DirEntry observations (conservation + cache-stability all passed "
          "fail-fast); ops={2}".format(scans, entries, H.total_ops()))

    # NON-VACUITY: the load-bearing scandir hazard was actually exercised.
    H.check(scans > 0,
            "no scandir walks completed -- the single-owner cursor/cache hazard "
            "was never exercised (oracle would be vacuous)")
    H.check(entries > 0,
            "no DirEntry observations -- the walk yielded nothing (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside __next__
    # on a desynced DIR* cursor).
    H.require_no_lost("scandir cursor/cache conservation")


if __name__ == "__main__":
    harness.main(
        "p552_scandir_direntry_stat_cache", body, setup=setup, post=post,
        default_funcs=1500, max_funcs=1500,
        describe="os.scandir returns a C iterator holding an open DIR* cursor and "
                 "yields DirEntry objects that lazily memoize type/stat.  Parked "
                 "mid-walk and resumed on another hub, the cursor or a DirEntry's "
                 "cached stat could desync.  LOAD-BEARING: each fiber scandirs its "
                 "OWN private dir of KNOWN entries -- the multiset of DirEntry.name "
                 "must equal the created set (conservation) and each entry's "
                 "is_file()/is_dir()/stat().st_size must be identity-stable across "
                 "a mid-walk yield.  Fail on missing/extra/dup name, wrong "
                 "kind/size, or a changed cached field")
