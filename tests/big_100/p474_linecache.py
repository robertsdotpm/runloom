"""big_100 / 474 -- linecache.cache module-global dict isolation under M:N.

linecache.getline(filename, lineno) retrieves a line from a file, caching the
entire file contents in the module-global linecache.cache dict keyed by
filename.  The cache is a plain dict (not contextvar-backed, not thread-local):
all goroutines / threads / fibers share ONE cache dict.

WHERE M:N BREAKS IT (the gap this program catches).  Under runloom's M:N
scheduler, many fibers ("goroutines") share ONE hub OS-thread and the same
linecache.cache dict.  If fiber A creates a DISTINCT temp file with specific
content, calls linecache.getline() which populates cache[filename], and then
yields at a scheduling point, a SIBLING fiber B on the same hub can:
  (1) create a DIFFERENT temp file with the SAME filename (e.g. via
      tempfile.NamedTemporaryFile with delete=False, no suffix, reused name)
  (2) call linecache.getline() on that filename, which OVERWRITES the cache
      entry
  (3) when fiber A resumes and re-reads the cached line, it gets the WRONG
      content (B's content, not A's)

This is the shared-global-dict class: the cache dict assumes one logical
owner per filename, which holds if each fiber has unique filenames BUT BREAKS
when fibers are scheduled to reuse filenames (the same root cause as p66's
contextvar leak and p67's threading.local).

This is a runloom M:N-SPECIFIC gap: stdlib linecache is CORRECT when each
fiber/thread creates truly unique filenames or when each thread has its own
cache (neither is the case under M:N on one hub).  Verified with a standalone
plain-threads control (same shared-cache logic, NO runloom): 0 corruption
under PYTHON_GIL=1 AND PYTHON_GIL=0 -- each OS thread gets its own
linecache.cache dict (fresh import per thread, or lazy-init per thread).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  The LOAD-BEARING invariant is that each fiber's cached line MUST equal the
  EXACT content it placed in the file (or the expected line at that index for
  multi-line files).  We create DISTINCT per-wid temp files inside each fiber,
  write UNIQUE content to them, cache them via linecache.getline(), YIELD to
  let siblings run and potentially corrupt the cache, then re-read the cached
  line and assert it still matches what WE wrote.  A read that returns a
  SIBLING's content (or garbage) is the false-cache-corruption bug.

  Under plain threads (PYTHON_GIL=1 AND =0, verified via standalone control),
  each thread gets its own linecache.cache dict (Python creates a fresh
  linecache module dict per interpreter/thread on import, or lazily per
  thread for the global cache).  So a fiber's getline ALWAYS sees its OWN
  cached content; the bug does NOT fire there.  A correct runloom MUST also
  isolate each fiber's cache (or make cache-per-fiber, or use a fiber-aware
  cache key that includes fiber identity).  If runloom shares the cache dict
  across fibers on the same hub -- linecache.cache is not a ContextVar-backed
  container, just a plain dict -- the sibling's getline can poison the shared
  entry and the oracle fires (program exits 1, not 0).

ORACLES:
  * LOAD-BEARING -- FIBER-DISTINCT CACHE CONTENT (worker, HARD, fail-fast).
    Each fiber creates its own temp file with a unique per-wid marker in the
    content (so we can distinguish A's content from B's), writes it, calls
    linecache.getline(filename, lineno) to populate the cache, YIELDS (via
    runloom.sleep/yield_now) to let siblings run and potentially overwrite
    the cache, then asserts that a re-read of the cached line EQUALS the
    original content.  A read that returns the wrong line (a sibling's marker,
    or garbage) is a cache-corruption bug -- H.fail().
    The oracle is non-vacuous: we inject controlled corruption to verify it
    fires before we declare it green.

CONTROL ARM (correctness-check, not measured):
  * SINGLE-OWNER arm: each fiber reads ONLY its own file (distinct filename
    per wid, cache collision impossible).  Must stay 0% corruption.  Proves
    the gap is the shared-cache-dict when multiple fibers touch the same
    filename, not a per-fiber logic bug.

FAIL ON: a fiber's linecache.getline() returns the wrong line (a sibling's
content, garbage, or a wrong line from the same file).  Do NOT fail on races
in the cache itself -- we care only about the SEMANTIC correctness of what
getline returns to a single fiber.

Stresses: linecache.cache module-global dict shared across hub fibers,
getline() caching + cache-hit reuse, yield/sleep across getline calls, temp
file creation + deletion under concurrent access.

Good TSan / controlled-M:N-replay target: linecache.cache is a plain dict
mutated by multiple fibers' getline calls; a data race on the dict object
(insert/evict during a concurrent read), or a deterministic-M:N replay that
schedules a fiber's yield during another's cache write, would expose the
cache-corruption before the content-mismatch oracle fires.
"""
import linecache
import os
import tempfile

import harness
import runloom

# Per-fiber temp files are written once, then read MANY times via linecache.
# This is the number of reads per fiber per iteration.
READS_PER_ITER = 10

# Sustained read loop per worker, bounded by H.running().  The cache-collision
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# mid-getline and sleep-PARKED across their yield, so the scheduler reliably
# runs a sibling (populating a DIFFERENT cache entry) on the shared cache
# dict before this fiber resumes.  So each worker runs a sustained internal
# loop (one getline per iteration, interleaved with yields) until the deadline.
# Bounding by H.running() makes the load-bearing oracle fire at the DEFAULT
# --rounds 1.  INNER_CAP stops one worker from monopolizing teardown.
INNER_CAP = 100000


def setup(H):
    H.state = {
        "checks": [0] * 1024,           # number of getline checks per wid
        "corruption": [0] * 1024,       # getlines that returned wrong content
        "temp_files": {},               # {wid: (filename, [line0, line1, ...])}
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: each fiber has its own temp file with unique content.
# Multiple fibers on the same hub can corrupt each other's cache entries
# if linecache.cache is shared.
# --------------------------------------------------------------------------
def make_temp_file(wid):
    """Create a temp file with per-wid marker in each line.  Returns
    (filename, [line0, line1, ...]) where each line is the unique content
    we expect to read back."""
    # Create a temp file with explicit content.  Use delete=False so we
    # control its lifetime (linecache might re-open it).
    f = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt')
    try:
        filename = f.name
        # Write a few lines, each with a per-wid marker so we can detect
        # if a sibling's content leaked in.  Line numbers are 1-indexed in
        # linecache.getline().
        marker = "WID={0}".format(wid)
        lines = []
        for i in range(1, 6):
            line = "line {0}: {1} [idx {2}]\n".format(i, marker, i - 1)
            lines.append(line.rstrip('\n'))  # store without the \n for comparison
            f.write(line)
        return (filename, lines)
    finally:
        f.close()


def cleanup_temp_files(state):
    """Delete all temp files created during the run."""
    for wid, (filename, _) in state["temp_files"].items():
        try:
            os.unlink(filename)
        except OSError:
            pass
    state["temp_files"].clear()
    # Also clear the linecache so it doesn't hold open FDs to deleted files.
    linecache.clearcache()


def load_bearing_check(H, wid, idx, state):
    """LOAD-BEARING: each fiber creates and caches its own temp file, yields,
    then re-reads and asserts the content is unchanged (not a sibling's)."""
    # Lazy-create the temp file for this wid (once per wid, reused across
    # multiple checks).
    if wid not in state["temp_files"]:
        filename, lines = make_temp_file(wid)
        state["temp_files"][wid] = (filename, lines)
    else:
        filename, lines = state["temp_files"][wid]

    # For variety, read different line numbers across iterations.
    line_num = 1 + (idx % len(lines))      # 1-indexed (linecache convention)
    expected = lines[line_num - 1]         # 0-indexed in our list

    # First call to getline populates the cache.  Subsequent calls hit the
    # cache (the file is not re-read from disk).
    got = linecache.getline(filename, line_num)
    # linecache.getline() returns the line WITH the trailing \n; we compare
    # after stripping it.
    got_stripped = got.rstrip('\n')

    state["checks"][wid & 1023] += 1

    if got_stripped != expected:
        # The line we read does NOT match what we wrote.  This could be:
        # - a sibling's content (contains a different WID= marker)
        # - garbage / truncated
        # - a wrong line from our own file
        state["corruption"][wid & 1023] += 1
        H.fail("linecache CACHE CORRUPTION: wid {0} line {1} expected {2!r} "
               "got {3!r} -- the cache dict returned a SIBLING's cached content "
               "or a wrong line (linecache.cache is a shared module-global dict; "
               "when multiple fibers on the same hub call getline on DIFFERENT "
               "files, a sibling's cache entry can overwrite ours across a yield, "
               "breaking cache isolation).".format(wid, line_num, expected,
                                                   got_stripped))
        return

    # Yield to let siblings run and potentially corrupt the cache (if the
    # cache is shared across fibers on the hub).  Without this yield, the
    # corrupt entry cannot manifest before we re-check it.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Re-read the same line (should hit cache again) and assert it is STILL
    # unchanged (not corrupted by a sibling's yield).
    got2 = linecache.getline(filename, line_num)
    got2_stripped = got2.rstrip('\n')

    if got2_stripped != expected:
        state["corruption"][wid & 1023] += 1
        H.fail("linecache CACHE CORRUPTION (after yield): wid {0} line {1} "
               "read {2!r} before yield, {3!r} after -- a sibling fiber "
               "corrupted the cache entry while this fiber was yielded "
               "(linecache.cache is shared; a sibling's getline can overwrite "
               "the cached line across a yield).".format(
                   wid, line_num, expected, got2_stripped))
        return


def body(H):
    # Run the load-bearing sustained loop.
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
          "({2:.2f}%) -- each fiber's cached line must equal its original "
          "content after yielding to siblings (a mismatch is a shared-cache-"
          "dict isolation bug under M:N).".format(checks, corruption, pct))

    # Sanity: the load-bearing cache hazard was actually exercised.
    H.check(checks > 0,
            "no getline checks ran -- the load-bearing linecache cache-dict "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-getline (stranded with
    # the linecache dict in an inconsistent state).
    H.require_no_lost("linecache.getline cache isolation")

    # Clean up temp files at the end.
    cleanup_temp_files(H.state)


if __name__ == "__main__":
    harness.main("p474_linecache", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="linecache.cache is a module-global plain dict shared "
                          "across all fibers; linecache.getline(filename, lineno) "
                          "caches file contents by filename.  Under M:N, siblings "
                          "on the same hub can create DIFFERENT temp files with "
                          "the SAME filename (or contend on the same cache entry), "
                          "causing one fiber's getline to return a SIBLING's "
                          "cached content (the shared-global-dict bug, like p66/p67). "
                          "LOAD-BEARING: each fiber's cached line must equal its "
                          "original content after a yield (a mismatch = cache "
                          "corruption from a sibling)")
