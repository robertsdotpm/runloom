"""big_100 / 567 -- dbm (dbm.dumb) single-owner key/value round-trip under M:N.

dbm is the stdlib "simple database" family (dbm.gnu / dbm.ndbm / dbm.sqlite3 /
dbm.dumb).  A dbm object is a MUTABLE, dict-like per-instance store backed by real
files, and dbm.dumb -- the always-available pure-Python backend -- threads a large
amount of per-INSTANCE state through every __setitem__/__getitem__:

  * self._index   -- an in-memory dict mapping key -> (pos, siz) into the .dat file;
  * self._datfile -- the .dat data file, opened 'rb+' per store to seek-to-end,
                     tell() the position, write the value, and return (pos, len);
  * self._dirfile -- the .dir index file, APPENDED to per new key (_addkey) and
                     rewritten wholesale from self._index on close (_commit);
  * self._modified / self._readonly -- per-instance flags gating the commit path.

  __setitem__ is a per-key read-modify-write over these: for a new key it calls
  _addval() (open .dat 'rb+', seek(0, 2), tell pos, write, return (pos, siz)) then
  _addkey() (append "key, (pos, siz)" to .dir AND self._index[key] = ...).  Close
  runs _commit(), which rewrites the whole .dir from self._index.

WHY dbm.dumb AND NOT the default backend.  On this box dbm.open()'s default backend
is dbm.sqlite3, whose _Database wraps sqlite3.connect(...) at the DEFAULT
check_same_thread=True.  Under runloom M:N a fiber has NO hub-thread affinity -- a
yield OR a preemption can resume it on a different hub OS-thread -- so the very next
sqlite3 call would raise sqlite3.ProgrammingError ("SQLite objects created in a
thread can only be used in that same thread").  That is DOCUMENTED sqlite3 behavior
(p21/p174 pass check_same_thread=False for exactly this reason), NOT a runloom bug,
so a sqlite3-backed dbm across a yield would be a FALSE POSITIVE.  dbm.dumb is pure
Python over ordinary file objects and plain dicts -- no thread affinity -- so it is
the legitimate single-owner oracle here.  We therefore build the load-bearing arm
on dbm.dumb (referencing the dbm package it belongs to).

WHERE M:N BREAKS IT (the gap this program probes).  Under M:N many fibers run in
parallel across a handful of hub OS-threads with the GIL OFF.  A fiber PARKED
(yield/sleep) in the middle of populating its dumb db -- after _addval wrote the
value block but before _addkey committed the index entry, or between two stores --
lets a sibling fiber on the same hub run.  If runloom did NOT properly isolate each
fiber's dumb-db instance (a torn self._index entry, an _addval that seeked to the
wrong .dat end, a lost _addkey append, or a value block written under a sibling's
file cursor), the db a fiber builds would read back the WRONG bytes -- a key
dropped, a value truncated, or a sibling's value bleeding in.  Because every fiber
owns its OWN db path + its OWN dumb instance (nothing shared), a correct runtime
reproduces every write exactly and the program EXITS 0 (PASS).

WHICH ORACLE IS LOAD-BEARING, AND WHY (a closed-world round-trip, single-owner):

  Each fiber, per iteration, generates a KNOWN multiset of K key/value pairs whose
  keys AND values are tagged with the fiber's wid (plus a per-iteration idx and a
  per-pair index), so a byte that leaked in from a SIBLING fiber's db (a different
  wid) is immediately recognizable.  The fiber:

    1. Opens a FRESH dumb db for its own path with flag 'n' (create new empty --
       truncates any prior iteration's files).
    2. Stores all K pairs via db[key] = value, YIELDING between stores (and once
       mid-store via a tiny sleep) so a sibling reliably interleaves while this
       fiber's db sits with a half-written index/data file.
    3. Closes the db (runs _commit(), rewriting .dir from self._index).
    4. Re-opens the SAME path read-only (flag 'r') and asserts the CLOSED-WORLD
       round-trip law:
         (a) len(db) == K exactly (no pair dropped or duplicated);
         (b) every key read back is one THIS fiber wrote (an out-of-universe key is
             a cross-fiber leak);
         (c) db[key] == the EXACT known value bytes for every key (no truncation,
             no sibling bytes, no torn value block);
         (d) the set of keys read back == the set written (no missing key).

  The analogous single-owner round-trip reproduces exactly under plain OS threads
  (each thread building + reading its own dumb db, GIL on AND off): 0 mismatches --
  each dumb instance is independent and self-isolated.  Under a CORRECT runloom each
  fiber's round-trip MUST also be byte-exact.  If a fiber's read-back bytes differ
  from what it wrote, a key count is wrong, or a sibling's key/bytes appear, that is
  a runloom M:N fiber-isolation bug (a torn self._index, a mis-seeked _addval, a
  lost _addkey append, or a value written under a sibling's cursor), and the
  load-bearing single-owner oracle FAILS -- otherwise it PASSES (exit 0).

ORACLES:
  * LOAD-BEARING -- DBM ROUND-TRIP INTEGRITY (worker, HARD, fail-fast).  The
    closed-world (a)-(d) checks above on a fiber's OWN dumb db.  Single-owner: the
    db path, the write instance, the read instance, and the expected dict are all
    fiber-local, never shared.  A failure is a runloom isolation desync, never
    documented Python semantics (an unsynchronized SHARED dbm would tear exactly
    like a shared file/dict across OS threads -- documented -- so we never share
    one, and we deliberately avoid the sqlite3 backend's thread-affinity trap).
  * NON-VACUITY (post, HARD): the round-trip hazard actually ran (roundtrips > 0 --
    else the oracle is vacuous).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-store
    (stranded inside _addval/_addkey/_commit on a torn instance) never returns; the
    watchdog + require_no_lost catch it.

FAIL ON: len(db) != K, an out-of-universe (sibling) key, read-back bytes != the
known value (truncation / sibling bytes / torn value block), or a missing key on
read-back.

Stresses: dbm.dumb per-instance store state (self._index dict, .dat _addval
seek-to-end/tell/write read-modify-write, .dir _addkey append, _commit rewrite,
self._modified flag) across a fiber yield, per-fiber dumb-db isolation under M:N
with the GIL off.  File-backed, so max_funcs caps the forever loop's --funcs.

Good TSan / controlled-M:N-replay target: self._index[key] = self._addval(val) is a
get-position-then-record pair over a live dict and a live file cursor; a fiber
parked between _addval's write and _addkey's index update is the cleanest window for
a lost key or a mis-seeked value block -- a report on the .dat cursor or the _index
dict, or a single wrong byte on read-back under replay, localizes the tear before
the round-trip law even closes.
"""
import os

import dbm            # the module under test (dbm package)
import dbm.dumb       # the pure-Python, thread-affinity-free backend we build on

import harness
import runloom

# Key/value pairs per fiber-owned db.  Small enough that build+read is cheap under
# hundreds of file-backed fibers, large enough that self._index grows across
# several entries and the round-trip exercises multiple _addval/_addkey boundaries.
PAIRS_PER_DB = 8

# Value size band.  Values span well past a trivial length so _addval's write +
# __getitem__'s seek/read cover a real byte range (a torn/mis-seeked block shows a
# wrong length as well as wrong bytes), but stay small so file I/O is light.
VALUE_MIN = 48
VALUE_MAX = 512

# Sustained round-trips per worker, bounded by H.running() and --duration.  The
# isolation hazard only manifests under SUSTAINED churn -- many fibers building and
# reading dumb dbs while parked mid-store across a yield, so a sibling reliably
# interleaves before this fiber resumes.  A single round-trip per fiber barely
# overlaps a sibling's and does NOT reproduce.  File I/O per iteration self-limits
# the real count well under this ceiling.
INNER_CAP = 100000


def build_value(wid, idx, k, size):
    """Deterministic, wid-tagged value for pair k of fiber wid's idx-th db.

    Begins with an ASCII tag embedding wid/idx/k so bytes that leaked in from a
    SIBLING fiber's db (a different wid) are immediately recognizable, then fills to
    `size` with a per-(wid,k) repeating byte so a mis-seeked value block from a
    sibling shows a wrong fill value as well as a wrong tag.  Single-owner: the
    fiber that wrote it is the only one that reads it."""
    tag = "W{0}:I{1}:K{2}:".format(wid, idx, k).encode("ascii")
    if len(tag) >= size:
        return tag[:size]
    fill_byte = ((wid * 7 + k * 31 + 1) & 0xFF)
    return tag + bytes([fill_byte]) * (size - len(tag))


def round_trip(H, wid, idx, rng, path, state):
    """One single-owner dbm.dumb build + read-back round-trip.

    Builds a fiber-local dumb db of K wid-tagged pairs (yielding between stores so a
    sibling interleaves on a half-written index/data file), then re-opens the SAME
    path read-only and asserts the closed-world round-trip law.  Every object here
    is fiber-local -- a mismatch is a runloom isolation bug."""
    # ---- KNOWN multiset of pairs this fiber will store (the closed world) ------
    expected = {}
    order = []
    for k in range(PAIRS_PER_DB):
        key = "w{0}_i{1}_k{2}".format(wid, idx, k).encode("ascii")
        size = rng.randint(VALUE_MIN, VALUE_MAX)
        expected[key] = build_value(wid, idx, k, size)
        order.append(key)

    # ---- BUILD: fiber-local dumb db, flag 'n' (fresh empty, truncates prior) ---
    wdb = dbm.dumb.open(path, "n")
    try:
        for pos, key in enumerate(order):
            # __setitem__ -> _addval (open .dat 'rb+', seek end, tell, write) +
            # _addkey (append to .dir, self._index[key] = (pos, siz)).
            wdb[key] = expected[key]
            # PARK mid-build: a sibling on this hub runs now while this fiber's dumb
            # db sits with a partially-written index/data file.  If self._index /
            # the .dat cursor / the .dir append are not fiber-isolated, the sibling's
            # stores bleed into this db.
            runloom.yield_now()
            if pos == 0:
                runloom.sleep(0.0002)
    finally:
        wdb.close()          # _commit(): rewrite .dir from self._index

    if H.failed:
        return

    # ---- READ-BACK: re-open the SAME path read-only over the committed files ----
    rdb = dbm.dumb.open(path, "r")
    try:
        runloom.yield_now()          # a sibling runs while this fiber holds an open db

        keys = rdb.keys()

        # (a) pair COUNT == K exactly.
        if len(rdb) != PAIRS_PER_DB or len(keys) != PAIRS_PER_DB:
            H.fail("fiber {0} idx {1}: dbm round-trip pair COUNT wrong: len(db)={2} "
                   "keys={3}, wrote {4} -- a pair was dropped or duplicated, "
                   "self._index / the .dat cursor was torn across a yield (cross-"
                   "fiber leak into this fiber's single-owner db)".format(
                       wid, idx, len(rdb), len(keys), PAIRS_PER_DB))
            return

        seen = set()
        for key in keys:
            # (b) key must be one THIS fiber wrote -- no sibling key.
            if key not in expected:
                H.fail("fiber {0} idx {1}: dbm round-trip OUT-OF-UNIVERSE key "
                       "{2!r} -- a sibling fiber's key appeared in this fiber's "
                       "single-owner db (self._index isolation failure under "
                       "M:N)".format(wid, idx, key))
                return
            if key in seen:
                H.fail("fiber {0} idx {1}: dbm round-trip DUPLICATE key {2!r} -- "
                       "the same key was recorded twice (torn .dir append / _index "
                       "under M:N)".format(wid, idx, key))
                return
            seen.add(key)

            exp = expected[key]
            got = rdb[key]

            # (c) read-back bytes must EXACTLY equal the known value.
            if got != exp:
                got_head = repr(got[:48]) if got else "empty"
                H.fail("fiber {0} idx {1}: dbm round-trip BYTES mismatch for key "
                       "{2!r}: got {3} (len {4}), expected {5} (len {6}) -- the "
                       "value block is truncated, torn, or carries a sibling "
                       "fiber's bytes (dumb per-instance .dat/_index isolation "
                       "failure under M:N)".format(
                           wid, idx, key, got_head, len(got), repr(exp[:48]),
                           len(exp)))
                return

        # (d) every stored key was read back.
        if len(seen) != PAIRS_PER_DB:
            H.fail("fiber {0} idx {1}: dbm round-trip MISSING key(s): read back {2} "
                   "distinct keys, wrote {3} -- a key vanished from self._index "
                   "across the build (lost _addkey append under M:N)".format(
                       wid, idx, len(seen), PAIRS_PER_DB))
            return
    finally:
        rdb.close()

    state["roundtrips"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber runs sustained single-owner dumb-db build + read-back round-trips
    on its OWN db path, fail-fast on the first closed-world round-trip violation."""
    # Fiber-local db path (unique per wid) under the shared base tmpdir.  Reused
    # across iterations; flag 'n' truncates it fresh each time, so disk stays
    # bounded.  dumb creates <path>.dir / .dat / .bak here.
    path = os.path.join(state["base"], "w{0}".format(wid))
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            round_trip(H, wid, idx, rng, path, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One shared base directory (created in the root, single-threaded); each fiber
    # uses a UNIQUE filename under it (w<wid>), so db files never collide across
    # fibers.  One race-free roundtrip slot per worker (single-writer-per-slot);
    # H.funcs is already capped to max_funcs here, so the array is bounded.
    H.state = {
        "base": H.make_tmpdir("big100_dbm_"),
        "roundtrips": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("dbm.dumb: {0} single-owner build+read round-trips (every closed-world "
          "round-trip law -- count, keys, exact value bytes -- passed fail-fast); "
          "ops={1}".format(rts, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rts > 0,
            "no dbm round-trips completed -- the load-bearing dbm.dumb build/read "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-store.
    H.require_no_lost("dbm.dumb round-trip")


if __name__ == "__main__":
    harness.main(
        "p567_dbm_roundtrip", body, setup=setup, post=post,
        default_funcs=400, max_funcs=512,
        describe="dbm (via dbm.dumb, the pure-Python, thread-affinity-free backend "
                 "-- the sqlite3 default backend's check_same_thread=True would be a "
                 "documented false positive under fiber migration) is a mutable "
                 "dict-like store threading self._index / the .dat seek-write cursor "
                 "/ the .dir append through every __setitem__ read-modify-write. "
                 "Under M:N a fiber parked mid-store lets a sibling run; if the dumb "
                 "instance is not fiber-isolated, stores interleave and the db reads "
                 "back wrong bytes.  LOAD-BEARING (single-owner): each fiber builds "
                 "its OWN dumb db of K wid-tagged pairs, yielding between stores, "
                 "then re-opens it read-only and asserts the closed-world round-trip "
                 "law -- len==K, no sibling key, read-back bytes==the known value. "
                 "A mismatch (torn _index, mis-seeked .dat block, lost _addkey "
                 "append, sibling bytes) is a runloom M:N isolation bug (0 under "
                 "plain threads GIL on AND off)")
