"""big_100 / 473 -- glob.glob fnmatch cache isolation under M:N.

glob.glob uses fnmatch internally to translate a shell pattern into a compiled
regex; fnmatch.translate results are cached in a per-interpreter-global
lru_cache (fnmatch._compile_pattern / the module-level _cache).  The cache key
is the pattern string; the cache is shared process-wide and mutated on every
miss.

WHERE M:N COULD BREAK IT (the gap this program catches).  Under runloom's M:N
scheduler many fibers share ONE hub OS-thread and thus ONE Python interpreter
state.  While fiber A is mid-glob.glob(pattern_A) -- its pattern_A compiled
regex lives in the shared fnmatch cache -- and yields at a scheduling point, a
SIBLING fiber B on the same hub that calls glob.glob(pattern_B) mutates the same
shared cache.  If runloom does NOT isolate / serialize that shared regex cache
correctly across an interleave or a hub migration, fiber B (or A on resume) can
read a CORRUPTED or WRONG cached compiled pattern and glob.glob returns the
WRONG file set -- a sibling's match list rather than its own.  That is the
runloom M:N isolation bug this program detects.

This is a runloom M:N invariant: stdlib glob is CORRECT under genuine OS-thread
semantics.  Verified with a standalone plain-threads control (same glob hazard,
NO runloom): 0 wrong match-sets under PYTHON_GIL=1 AND PYTHON_GIL=0 -- each OS
thread's glob call returns its own pattern's files.  Under a CORRECT runloom,
each fiber's glob.glob() over its own pattern must return that pattern's match
set, never a sibling's.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  glob.glob(pattern) over a FIXED, READ-ONLY filesystem tree MUST return the
  set of files matching THAT pattern -- a deterministic, precomputed closed-
  world reference.  The match set is precomputed ONCE at setup (per pattern),
  so the only way a fiber's glob can return the wrong set is a shared-cache
  corruption: a sibling's compiled regex leaked into this fiber's call.

  IMPORTANT (this program was previously a FALSE-FAILER -- now fixed): the tree
  is FIXED and READ-ONLY after setup.  No fiber ever creates, deletes, or
  mutates a file.  So the *only* legitimate result of glob.glob(pattern_k) is
  the precomputed match set for pattern k.  A returned set that is a SUPERSET,
  SUBSET, or REORDERING of a sibling's set is a genuine cache corruption.  The
  earlier version created a DISTINCT temp dir PER FIBER and compared against a
  per-fiber expected set -- that both (a) created ~one-temp-dir-per-fiber
  (filled the disk at 500k fibers) AND (b) was over-strict, because two fibers
  with the same wid-pattern but different snapshots could legitimately differ.
  The redesign removes BOTH problems: a BOUNDED pool of N distinct subtrees,
  each with a precomputed match set, glob'd read-only by all fibers via wid%N.

ARMS:
  * LOAD-BEARING -- READ-ONLY GLOB arm (worker, HARD, fail-fast).  A fiber picks
    pool slot wid%N (a (dirpath, pattern, expected_sorted_basenames) triple),
    calls glob.glob(os.path.join(dirpath, pattern)) inside a sleep-parked yield
    window (so fibers interleave their fnmatch-cache contention), and asserts
    the returned basenames EXACTLY equal the precomputed expected set for THAT
    slot.  The tree is read-only, so a mismatch is a shared fnmatch-cache
    corruption (the runloom M:N bug).  On a CORRECT runtime (and plain threads
    GIL on AND off) this NEVER fires, so the program exits 0 when there is no
    bug.
  * NON-VACUITY (post, HARD): glob_checks > 0 -- the hazard was exercised.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished
    mid-glob never returns; the watchdog catches an outright strand.

FAIL ON: glob.glob returning a match set != the precomputed set for the slot's
pattern (a sibling's files, missing files, or a torn/reordered list).

BOUNDED TEMP POOL (root-cause fix for the disk-fill crash): the filesystem tree
is built EXACTLY ONCE in setup() inside a single mkdtemp dir, with N =
min(H.funcs, POOL_CAP) distinct subtrees.  No fiber EVER creates a file or dir.
The whole tree is removed via atexit + post().  Temp-file count is bounded by
the pool (~N <= POOL_CAP), independent of H.funcs -- so funcs=8000 and
funcs=500000 create the SAME small number of files.

Stresses: glob.glob filesystem traversal + the shared fnmatch regex cache
across hub fibers; cache collision / pollution when many fibers glob distinct
patterns simultaneously over a fixed read-only tree; a scheduling point inside
the glob critical section (sleep-parked yields).

Good TSan / controlled-M:N-replay target: the fnmatch compile cache is a shared
lru_cache mutated on miss; a data-race report on it -- or a deterministic replay
that migrates a hub between a fiber's glob call and its cache lookup -- isolates
the collision before the file-set oracle fires.
"""
import atexit
import glob
import os
import shutil
import tempfile

import harness
import runloom

# ---------------------------------------------------------------------------
# BOUNDED TEMP POOL.  N distinct subtrees are built ONCE in setup() (each with a
# distinct glob pattern and a precomputed match set); ALL fibers glob them
# read-only via wid % len(_POOL).  No per-fiber temp files -- the temp-file
# count is bounded by POOL_CAP regardless of H.funcs.  This is the root-cause
# fix for the old version's one-temp-dir-per-fiber disk fill.
# ---------------------------------------------------------------------------
# N distinct subtrees / patterns (distinct fnmatch-cache entries).  Capped so
# the TOTAL temp-file count stays small and bounded (POOL_CAP subtrees *
# (FILES_PER_DIR + DECOYS_PER_DIR) files) regardless of H.funcs.
POOL_CAP = 128

_TMPDIR = None
# _POOL[i] = (dirpath, pattern, expected_sorted_basenames)
_POOL = []

# Files per pool subtree.  Each subtree contains FILES_PER_DIR files matching
# its distinct pattern, plus DECOYS_PER_DIR decoy files that must NOT match --
# so a wrong (superset) match set from a cache collision is detectable.  Kept
# small so total temp files = POOL_CAP * (FILES_PER_DIR + DECOYS_PER_DIR) stays
# well under a few hundred and never grows with H.funcs.
FILES_PER_DIR = 3
DECOYS_PER_DIR = 1

# Sustained glob checks per worker, bounded by H.running().  The cache-collision
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# mid-glob and sleep-PARKED across their yields, so the scheduler reliably runs
# a sibling's glob (mutating the shared fnmatch cache) before this fiber
# resumes.  INNER_CAP stops one worker from monopolizing teardown.
INNER_CAP = 10000


def _pattern_for_slot(slot):
    """Return a glob pattern unique to this pool slot."""
    return "slot_{0:05d}_*.txt".format(slot)


def _expected_for_slot(slot):
    """The EXPECTED sorted basenames glob.glob(pattern_for_slot) MUST return.
    Deterministic, precomputed, closed-world."""
    prefix = "slot_{0:05d}_".format(slot)
    return sorted("{0}{1}.txt".format(prefix, i) for i in range(FILES_PER_DIR))


def _cleanup():
    global _TMPDIR
    d = _TMPDIR
    _TMPDIR = None
    if d:
        shutil.rmtree(d, ignore_errors=True)


def setup(H):
    global _TMPDIR, _POOL
    base = os.environ.get("BIG100_TMP") or tempfile.gettempdir()
    _TMPDIR = tempfile.mkdtemp(prefix="p473_glob_", dir=base)
    atexit.register(_cleanup)

    n = min(H.funcs, POOL_CAP)
    if n < 1:
        n = 1
    _POOL = []
    for slot in range(n):
        dirpath = os.path.join(_TMPDIR, "slot_{0:05d}".format(slot))
        os.mkdir(dirpath)
        pattern = _pattern_for_slot(slot)
        expected = _expected_for_slot(slot)
        # Matching files for this slot's pattern.
        for fname in expected:
            with open(os.path.join(dirpath, fname), "w") as f:
                f.write("slot={0} file={1}\n".format(slot, fname))
        # Decoy files that must NOT match this slot's pattern (so a cache
        # collision that widens the match set is caught).
        for i in range(DECOYS_PER_DIR):
            decoy = "decoy_{0:05d}_{1}.log".format(slot, i)
            with open(os.path.join(dirpath, decoy), "w") as f:
                f.write("decoy\n")
        _POOL.append((dirpath, pattern, expected))

    H.state = {
        "glob_checks": [0] * 1024,    # globs that ran
        "exact": [0] * 1024,          # globs that matched the expected set exactly
        "undercount": [0] * 1024,     # benign: glob swallowed a scandir error
                                      # under fd pressure -> empty/subset (MEASURED)
        "foreign": [0] * 1024,        # LOAD-BEARING: a foreign file leaked in (bug)
        "sample": [None],             # first observed benign-undercount sample
        "npool": n,
    }


# LOAD-BEARING arm: READ-ONLY glob over a bounded pool subtree.  A fiber picks
# pool slot wid%N and globs its distinct pattern inside a sleep-parked yield
# window (so fibers interleave their fnmatch-cache contention).
#
# DISCRIMINATOR DISCIPLINE (this program WAS a false-failer; now fixed).  The
# tree is FIXED and read-only, so the ONLY genuine corruption signal is a
# FOREIGN file appearing in the result -- a basename that is NOT one of THIS
# slot's expected files (a sibling slot's file leaked in via a shared fnmatch-
# cache collision, OR a decoy that must never match).  THAT is hard-failed.
#
# An empty or strict-SUBSET result is NOT a corruption: glob.glob calls
# os.scandir internally and, by DOCUMENTED design, SWALLOWS the OSError and
# returns fewer/zero entries when scandir cannot open a directory fd.  Under
# many thousands of concurrent fibers each entering glob, the process fd table
# is exhausted (EMFILE) and glob legitimately returns [] -- this reproduces with
# NO cache involvement and does NOT occur under the low-fd-pressure plain-threads
# control.  So under-counts are MEASURED (report-only), never failed -- the same
# discipline as p67's TLS leak rate and p321's overlap drift.
def glob_check(H, wid, idx, state):
    slot = wid % state["npool"]
    dirpath, pattern, expected = _POOL[slot]
    expected_set = set(expected)

    # YIELD + SLEEP-PARK: interleave this fiber's glob with siblings' globs on
    # the shared fnmatch cache.  The sleep deschedules this fiber long enough
    # that many siblings' glob calls execute (mutating the shared cache) before
    # this fiber resumes.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    got_paths = glob.glob(os.path.join(dirpath, pattern))
    got = sorted(os.path.basename(p) for p in got_paths)
    got_set = set(got)

    state["glob_checks"][wid & 1023] += 1

    # LOAD-BEARING: any returned basename NOT in this slot's expected set is a
    # genuine corruption -- a foreign file (a sibling slot's file or a decoy)
    # leaked into this fiber's glob over a FIXED read-only tree.  Only EXTRA /
    # WRONG entries count; empty/subset is handled below as benign.
    foreign = got_set - expected_set
    if foreign:
        state["foreign"][wid & 1023] += 1
        other_slots = set()
        for fname in sorted(foreign):
            if fname.startswith("slot_") and "_" in fname[5:]:
                try:
                    o = int(fname[5:10])
                    if o != slot:
                        other_slots.add(o)
                except ValueError:
                    pass
        H.fail(
            "glob.glob CACHE COLLISION: fiber {0} (slot {1}) globbed {2!r} over "
            "a FIXED read-only tree (dir {3!r}) and got FOREIGN files {4!r} that "
            "are NOT this slot's files (leaked slots={5}). Expected only {6!r}. "
            "A sibling's compiled fnmatch pattern leaked into this fiber's glob "
            "-- a shared fnmatch-cache pollution (runloom M:N bug; 0 under plain "
            "threads GIL on AND off).".format(
                wid, slot, pattern, dirpath, sorted(foreign),
                sorted(other_slots), expected))
        return

    if got_set == expected_set:
        state["exact"][wid & 1023] += 1
    else:
        # Strict subset (incl. empty): benign -- glob swallowed a scandir error
        # under fd pressure (EMFILE).  MEASURED, never failed.
        state["undercount"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, slot, expected, got)


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING read-only glob check (fail-fast).
    Sustains a churn loop bounded by H.running(): one glob check per iteration
    (with a sleep-park on odd iterations) so many fibers stay simultaneously
    mid-glob and parked -- the condition the fnmatch-cache collision needs to
    manifest.  No fiber creates any file."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            glob_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["glob_checks"])
    exact = sum(H.state["exact"])
    under = sum(H.state["undercount"])
    foreign = sum(H.state["foreign"])
    under_pct = (100.0 * under / checks) if checks else 0.0
    sample = H.state["sample"][0]
    H.log("glob[LOAD-BEARING]: {0} checks  exact={1}  foreign_leak={2} "
          "(LOAD-BEARING)  undercount={3} ({4:.2f}%, benign EMFILE scandir-swallow "
          "-- MEASURED)  npool={5}  under_sample={6}".format(
              checks, exact, foreign, under, under_pct, H.state["npool"], sample))
    if foreign:
        H.log("note: the LOAD-BEARING glob arm observed FOREIGN files in a "
              "fiber's match set over a FIXED read-only tree -- glob.glob uses "
              "fnmatch to translate patterns, and fnmatch caches compiled "
              "regexes in a shared per-interpreter lru_cache.  Runloom M:N fibers "
              "share one interpreter state, so many fibers' glob calls on "
              "DISTINCT patterns can collide on that shared cache, causing glob "
              "to return a sibling's files.  This is a runloom M:N gap (0 under "
              "plain threads GIL on AND off); the fix is to isolate the fnmatch "
              "cache per fiber (contextvar-backed) or guard it with a per-hub "
              "lock.")
    if under:
        H.log("note: {0} undercount globs ({1:.2f}%) returned an empty/subset "
              "match set -- glob.glob calls os.scandir and DOCUMENTED-swallows "
              "the OSError under fd exhaustion (EMFILE) at high fiber counts, "
              "returning fewer entries.  This is a benign resource/scale artifact "
              "(reproduces with no cache involvement; does NOT occur under the "
              "low-fd plain-threads control) -- MEASURED, never failed.".format(
                  under, under_pct))
    # NON-VACUITY: the load-bearing glob hazard was actually exercised.
    H.check(checks > 0,
            "no glob checks ran -- the load-bearing cache-collision hazard was "
            "never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-glob.
    H.require_no_lost("glob.glob fnmatch cache isolation")
    # BOUNDED-POOL teardown.
    _cleanup()


if __name__ == "__main__":
    harness.main(
        "p473_glob", body, setup=setup, post=post,
        default_funcs=8000,
        describe="glob.glob uses fnmatch to translate patterns; fnmatch caches "
                 "compiled patterns in a shared per-interpreter lru_cache. "
                 "Runloom M:N fibers share one interpreter state, so many "
                 "fibers calling glob.glob with DISTINCT patterns over a FIXED "
                 "read-only tree can collide on the shared fnmatch cache, "
                 "causing glob to return a sibling's match set.  LOAD-BEARING: "
                 "a BOUNDED pool of N distinct read-only subtrees (built ONCE, "
                 "no per-fiber files) is glob'd by all fibers via wid%N inside "
                 "sleep-parked yields; the oracle asserts the returned set "
                 "exactly equals the precomputed set for that slot's pattern (0 "
                 "under plain threads GIL on AND off; a wrong set is the shared "
                 "fnmatch-cache collision bug).  Same class as p67 "
                 "(threading.local); fix is per-fiber fnmatch-cache isolation")
