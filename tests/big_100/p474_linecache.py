"""big_100 / 474 -- linecache.cache module-global dict isolation under M:N.

linecache.getline(filename, lineno) retrieves a line from a file, caching the
entire file contents in the module-global linecache.cache dict keyed by
filename.  The cache is a plain dict (not contextvar-backed, not thread-local):
all goroutines / threads / fibers share ONE cache dict.

WHERE M:N BREAKS IT (the gap this program catches).  Under runloom's M:N
scheduler, many fibers ("goroutines") share ONE hub OS-thread and the same
linecache.cache dict.  linecache.getline(filename, lineno) populates
cache[filename] on first read and returns the cached line on subsequent reads.
If a fiber A caches a file, YIELDS at a scheduling point, and a SIBLING fiber B
on the same hub reads/caches a DIFFERENT pool file (mutating the shared cache
dict -- insert, or evict during checkcache), a data race on the cache dict
object (or a fiber-identity desync in a shared cache key) can make A's resumed
getline return the WRONG line (B's content, or garbage).

This is the shared-global-dict class: the cache dict assumes a single logical
owner per filename and no concurrent mutation of the dict object.  That holds
under run(1)/GIL and under plain OS threads, but a runloom M:N save/restore or
cache-key desync across a yield would break it.

BOUNDED POOL (root-cause fix -- DOES NOT create one temp file per fiber).
  Previously every fiber created its own NamedTemporaryFile, so at
  --funcs 500000 the program made ~500k temp files and FILLED THE DISK.  The
  hazard never required per-fiber files: it requires N DISTINCT cache entries
  (distinct filenames) exercised by many fibers.  So we create EXACTLY
  N = min(H.funcs, 512) pool .txt files ONCE in setup(), each with KNOWN,
  per-pool-index content, and every fiber reads pool file `wid % N`.  N distinct
  filenames give N distinct linecache.cache entries, mutated concurrently by all
  fibers -- the SAME cache-isolation hazard, with a BOUNDED, funcs-independent
  number of temp files.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  The LOAD-BEARING invariant is that a fiber's linecache.getline() on its
  assigned pool file MUST return the EXACT known line for that pool file at that
  line number -- the content we wrote into that pool file at setup.  Each pool
  file embeds its own pool-index marker (POOL=<n>) in every line, so a read that
  returns a SIBLING pool file's content (a different POOL= marker) or garbage is
  a false-cache-corruption bug.  We getline (populate/hit the cache), YIELD to
  let siblings mutate the shared cache dict, then re-read and assert the cached
  line is STILL our pool file's known content.

  Under plain threads (PYTHON_GIL=1 AND =0, verified via standalone control),
  the shared linecache.cache returns each filename's OWN content -- the GIL or
  per-thread serialization keeps the dict reads/writes consistent.  So getline
  ALWAYS returns the right pool file's line; the bug does NOT fire there.  A
  correct runloom MUST also keep each pool file's cached content intact across
  fiber yields on the shared hub.  If runloom desyncs the shared cache across a
  yield (returns a sibling pool file's line, or garbage), the oracle fires
  (program exits 1, not 0).

ORACLES:
  * LOAD-BEARING -- POOL-FILE-DISTINCT CACHE CONTENT (worker, HARD, fail-fast).
    Each fiber reads its assigned pool file (wid % N) via
    linecache.getline(filename, lineno), YIELDS (runloom.yield_now/sleep) to let
    siblings mutate the shared cache dict, then asserts a re-read returns the
    SAME known line for that pool file.  A read that returns a sibling pool
    file's marker (wrong POOL=), garbage, or a wrong line index is a cache
    isolation bug -- H.fail().  Non-vacuous: injecting controlled corruption
    (overwriting a pool file's expected content) makes it fire (exit 1).

Stresses: linecache.cache module-global dict shared across hub fibers,
getline() caching + cache-hit reuse, yield/sleep across getline calls,
concurrent mutation of the shared cache dict by many fibers over a BOUNDED set
of distinct filenames.

Good TSan / controlled-M:N-replay target: linecache.cache is a plain dict
mutated by many fibers' getline calls over N distinct filenames; a data race on
the dict object (insert/evict during a concurrent read), or a deterministic-M:N
replay that schedules a fiber's yield during another's cache write, would expose
the cache-corruption before the content-mismatch oracle fires.
"""
import atexit
import linecache
import os
import shutil
import tempfile

import harness
import runloom

# Reads per fiber per iteration (cache-hit reuse of the populated entry).
READS_PER_ITER = 10

# Bound on one worker's inner loop so it cannot monopolize teardown.
INNER_CAP = 100000

# Number of LINES per pool file (1-indexed in linecache.getline()).
LINES_PER_FILE = 5

# Hard cap on the number of DISTINCT pool files (= distinct cache entries =
# distinct temp files).  This is the whole point: the temp-file count is
# bounded by POOL_CAP regardless of --funcs.
POOL_CAP = 512

# --------------------------------------------------------------------------
# Module-level bounded pool (created ONCE in setup, cleaned up ONCE in post /
# atexit).  _POOL holds (filename, [line0, line1, ...]) for each pool index.
# NO per-fiber file is ever created.
# --------------------------------------------------------------------------
_TMPDIR = None
_POOL = []


def _pool_lines(pool_idx):
    """The KNOWN content for pool file `pool_idx`: a list of lines (without the
    trailing newline), each tagged with the pool index so a sibling's content
    is distinguishable from ours."""
    marker = "POOL={0}".format(pool_idx)
    return ["line {0}: {1} [idx {2}]".format(i, marker, i - 1)
            for i in range(1, LINES_PER_FILE + 1)]


def _cleanup():
    global _TMPDIR
    d = _TMPDIR
    _TMPDIR = None
    _POOL.clear()
    # Drop any linecache entries referencing the about-to-be-removed pool files
    # so the cache doesn't hold stale paths (and so a later run starts clean).
    try:
        linecache.clearcache()
    except Exception:
        pass
    if d:
        shutil.rmtree(d, ignore_errors=True)


def setup(H):
    global _TMPDIR
    _TMPDIR = tempfile.mkdtemp(
        prefix="p474_linecache_",
        dir=os.environ.get("BIG100_TMP") or tempfile.gettempdir())
    atexit.register(_cleanup)

    # Create EXACTLY N = min(H.funcs, POOL_CAP) distinct pool files ONCE.  Each
    # has known per-pool-index content.  All fibers read these via wid % N.
    n = min(max(1, H.funcs), POOL_CAP)
    for pool_idx in range(n):
        lines = _pool_lines(pool_idx)
        path = os.path.join(_TMPDIR, "pool_{0}.txt".format(pool_idx))
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")
        _POOL.append((path, lines))

    H.state = {
        "checks": [0] * 1024,           # number of getline checks per wid
        "corruption": [0] * 1024,       # getlines that returned wrong content
        "pool_n": n,
    }


def load_bearing_check(H, wid, idx, state):
    """LOAD-BEARING: read this fiber's assigned pool file via linecache.getline,
    yield to let siblings mutate the shared cache dict, then re-read and assert
    the cached line is STILL this pool file's known content."""
    filename, lines = _POOL[wid % len(_POOL)]

    # Read different line numbers across iterations.
    line_num = 1 + (idx % len(lines))      # 1-indexed (linecache convention)
    expected = lines[line_num - 1]         # 0-indexed in our list

    # First call populates the cache; later calls hit the cached entry.
    got = linecache.getline(filename, line_num)
    got_stripped = got.rstrip('\n')

    state["checks"][wid & 1023] += 1

    if got_stripped != expected:
        state["corruption"][wid & 1023] += 1
        H.fail("linecache CACHE CORRUPTION: wid {0} pool {1} line {2} expected "
               "{3!r} got {4!r} -- linecache.getline returned a SIBLING pool "
               "file's cached content or a wrong line (linecache.cache is a "
               "shared module-global dict; when many fibers on the same hub "
               "getline over DISTINCT pool files, a sibling's mutation can "
               "corrupt the cache entry, breaking cache isolation).".format(
                   wid, wid % len(_POOL), line_num, expected, got_stripped))
        return

    # Yield so siblings run and mutate the shared cache dict before we re-read.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Re-read (cache hit) and assert it is STILL our pool file's known line.
    got2 = linecache.getline(filename, line_num)
    got2_stripped = got2.rstrip('\n')

    if got2_stripped != expected:
        state["corruption"][wid & 1023] += 1
        H.fail("linecache CACHE CORRUPTION (after yield): wid {0} pool {1} "
               "line {2} expected {3!r} got {4!r} after yield -- a sibling "
               "fiber corrupted the shared cache entry while this fiber was "
               "yielded (linecache.cache is shared; a sibling's getline over a "
               "distinct pool file can desync the cached line across a "
               "yield).".format(wid, wid % len(_POOL), line_num, expected,
                                 got2_stripped))
        return


def body(H):
    def worker(H, wid, rng, state):
        for _ in H.round_range():
            if not H.running():
                break
            idx = 0
            while H.running() and idx < INNER_CAP:
                for _ in range(READS_PER_ITER):
                    if not H.running():
                        break
                    load_bearing_check(H, wid, idx, state)
                    if H.failed:
                        return
                    H.op(wid)
                idx += 1
            H.task_done(wid)

    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    corruption = sum(H.state["corruption"])
    pct = (100.0 * corruption / checks) if checks else 0.0
    H.log("linecache: {0} getline checks, {1} cache-corruption detections "
          "({2:.2f}%) over {3} distinct pool files -- each fiber's cached line "
          "must equal its pool file's known content after yielding to siblings "
          "(a mismatch is a shared-cache-dict isolation bug under M:N).".format(
              checks, corruption, pct, H.state["pool_n"]))

    # Sanity: the load-bearing cache hazard was actually exercised.
    H.check(checks > 0,
            "no getline checks ran -- the load-bearing linecache cache-dict "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-getline (stranded with
    # the linecache dict in an inconsistent state).
    H.require_no_lost("linecache.getline cache isolation")

    # Bounded-pool cleanup: remove the single mkdtemp dir holding all N pool
    # files (idempotent with the atexit handler).
    _cleanup()


if __name__ == "__main__":
    harness.main("p474_linecache", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="linecache.cache is a module-global plain dict shared "
                          "across all fibers; linecache.getline(filename, lineno) "
                          "caches file contents by filename.  A BOUNDED pool of "
                          "N=min(funcs,512) distinct .txt files (created ONCE, "
                          "NOT one-per-fiber) gives N distinct cache entries; all "
                          "fibers read pool file wid%N, yield to let siblings "
                          "mutate the shared cache dict, then re-read.  "
                          "LOAD-BEARING: each fiber's cached line must equal its "
                          "pool file's known content after a yield (a mismatch = "
                          "cache corruption from a sibling, like p66/p67)")
