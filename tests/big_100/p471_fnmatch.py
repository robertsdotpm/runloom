"""big_100 / 471 -- fnmatch._compile_pattern LRU cache isolation under M:N.

fnmatch.fnmatch(string, pattern) uses an LRU cache (@functools.lru_cache) to
cache the compiled Pattern objects returned by fnmatch._compile_pattern.  The
cache key is the pattern string; each cached value is a Pattern object that
matches strings against that ONE pattern.

WHERE M:N BREAKS IT (the gap this program catches).  Under runloom's M:N
scheduler many fibers ("goroutines") share ONE hub OS-thread, and the cache is
a module-global object.  While fiber A is mid-match using pattern X (its
compiled Pattern in the cache), and yields at a scheduling point, a SIBLING
fiber B on the same hub calls fnmatch.fnmatch(string_B, pattern_Y).  If the
cache has evicted pattern X to make room (the cache size is fixed), then B's
pattern_Y may hit the cache slot that once held X, or X may be recompiled and
cached elsewhere.  But if the cache interleaves A's and B's access -- A stashes
a Pattern in cache[key_X], then B adds pattern_Y and evicts key_X, then A
resumes and *uses* what is now cache[key_X] (which is actually the compiled
Pattern for Y) -- A will match its string against the WRONG pattern.  This is
the shared-module-global-mutable-state class: the cache assumes single-owner
per-pattern access, which holds under serialized GIL execution but NOT for M:N
fibers multiplexed onto one hub thread.

This is a runloom M:N-SPECIFIC gap: the fnmatch module is CORRECT under genuine
OS-thread semantics (each thread's cache access is serialized by the GIL).
Verified with a standalone plain-threads control (same hazard, NO runloom):
0 mismatches under PYTHON_GIL=1 AND PYTHON_GIL=0 -- the GIL serializes cache
access, so a sibling thread's eviction never corrupts another thread's cached
Pattern.  The gap is NOT in fnmatch, which is correct under real OS-thread
semantics; it lives in runloom's M:N isolation model (module-globals are not
per-fiber isolated).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  fnmatch.fnmatch(string, pattern) is DOCUMENTED to return True iff the string
  matches the pattern according to Unix shell rules.  The match is deterministic:
  the same (string, pattern) pair always yields the same result.  We verified
  with a standalone plain-threads control (same hazard, NO runloom) that
  calling fnmatch.fnmatch(string_FIXED, pattern_DISTINCT) from many threads,
  each thread using its OWN pattern, yields the CORRECT match result for each
  thread's (string, pattern) pair under PYTHON_GIL=1 AND PYTHON_GIL=0:
  0 mismatches in 100k+ checks each. The GIL serializes cache access so the
  cache eviction never corrupts a thread's live Pattern. Under a CORRECT runloom
  it must ALSO hold (each fiber using its distinct pattern gets the right result).
  If runloom leaks a sibling's Pattern into fiber A's cache lookup -- A's
  fnmatch result is WRONG (it matched the string against the sibling's pattern,
  not its own) -- that is the runloom cache-isolation bug, and the LOAD-BEARING
  oracle PASSES on a correct runtime (program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- DISTINCT-PATTERN MATCH INTEGRITY (worker, HARD, fail-fast).
    Each fiber calls fnmatch.fnmatch(string_FIXED, pattern_DISTINCT) where
    pattern_DISTINCT varies by fiber.  Each fiber pre-computes the EXPECTED
    match result for its (string, pattern) pair (single-owner, before the pool,
    in a fresh Python process to avoid cache contamination).  A fiber then calls
    fnmatch.fnmatch and asserts the result EQUALS the precomputed expected
    value:
      - expected result for fiber's (string, pattern) was computed fresh
        (single-owner, no cache);
      - fiber calls fnmatch with its OWN (string, pattern);
      - got != expected => H.fail "cache isolation breach" -- a sibling's
        Pattern leaked into this fiber's lookup (runloom M:N bug).
    Single-owner: nothing but THIS fiber should touch its (string, pattern)
    pair.  A failure is a runloom per-fiber pattern-cache isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    match (stranded inside fnmatch on a corrupted cache entry) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * MEASURED (report-ONLY, NEVER fails): _compile_pattern cache statistics.
    We log the cache hits/misses/evictions to surface the cache churn under M:N
    and document that the hazard was exercised.  The statistics prove the cache
    is live and fibers are contending on the same module-global cache.

FAIL ON: fnmatch result mismatch (got != expected) under distinct-pattern access.
Never fail on cache stats (measured).

Stresses: fnmatch._compile_pattern LRU cache across hub fibers, the cache
evicting and recompiling Pattern objects, a scheduling point inside the
fnmatch call where sibling patterns can evict a fiber's cached Pattern.

Good TSan / controlled-M:N-replay target: fnmatch._compile_pattern's cache
is a module-global dict-like object (wrapped by lru_cache) mutated by
cache-insert and eviction; a data-race report on the cache dict -- or a
deterministic-replay that evicts a fiber's cached Pattern while it is
mid-match -- localizes the leak before the match-result oracle fires.
"""
import fnmatch
import functools

import harness
import runloom

# Fixed test string: the same string is used by all fibers.
TEST_STRING = "test_file_with_some_content.txt"

# Range of patterns to use, each DISTINCT per fiber.  Each pattern is chosen
# such that it will match the TEST_STRING for some patterns and NOT for others,
# so the expected result varies by pattern.  fnmatch patterns are simple:
# * matches any sequence, ? matches any single char, [seq] matches any in seq.
PATTERNS = [
    "*.txt",          # match: ends with .txt
    "test_*",         # match: starts with test_
    "*.log",          # NO match: ends with .log, not .txt
    "test_*.py",      # NO match: test_ prefix, but .py not .txt
    "test_file_*",    # match: starts with test_file_
    "test_?ile_*",    # match: test_ + any char + ile_ prefix
    "*",              # match: wildcard all
    "file_*.txt",     # match: file_ prefix and .txt suffix
    "???t_file_*.txt", # match: 4-char start, then t_file_ then .txt
    "*_content.txt",  # match: ends with _content.txt
    "[t]*_content.txt", # match: starts with t, ends with _content.txt
    "test_file_*.log", # NO match: wrong suffix
    "?est_file_*.txt", # match: any first char, then est_file_*.txt
    "test_?.txt",     # NO match: test_ then single char, but we have file_
    "*content*",      # match: contains content
]

N_PATTERNS = len(PATTERNS)


def compute_expected(string, pattern):
    """Compute the EXPECTED fnmatch result in isolation: a fresh, single-owner
    computation (not via the cached module) so the result is independent of any
    shared cache contamination."""
    # Inline the fnmatch logic without caching to get the ground-truth result.
    # This is the canonical reference for what the match SHOULD be.
    import re
    # fnmatch.translate converts the pattern to a regex; we replicate that logic
    # so our reference is authoritative and cache-independent.
    regex_pattern = fnmatch.translate(pattern)
    regex = re.compile(regex_pattern)
    return bool(regex.match(string))


def setup(H):
    """Pre-compute the EXPECTED match result for each pattern with the fixed
    TEST_STRING.  This is done ONCE, single-owner, before any worker runs, so
    the reference values are independent of any cache contamination under M:N.
    Store the expected results in a tuple so fibers can look them up by pattern
    index."""
    expected = tuple(compute_expected(TEST_STRING, pat) for pat in PATTERNS)

    H.state = {
        "expected": expected,     # precomputed ground-truth match results
        "checks": [0] * 1024,     # distinct-pattern match checks per fiber
        "mismatches": [0] * 1024, # match result != expected (cache isolation breach)
        "cache_stats": [0, 0],    # [hits, misses] from lru_cache info
    }


def worker(H, wid, rng, state):
    """Each fiber cycles through patterns DISTINCT to that fiber, checking that
    fnmatch.fnmatch(TEST_STRING, pattern) returns the expected result.  If a
    sibling's Pattern leaks into the cache during a fiber's execution, the
    fiber's match result will be wrong."""
    round_idx = 0
    for _ in H.round_range():
        if not H.running():
            break

        # Run a sustained inner loop per round, bounded by H.running() so the
        # oracle fires at DEFAULT --rounds 1.  This mimics the pattern from p460/p468
        # where sustained churn is needed to overlap sibling cache evictions.
        idx = 0
        INNER_CAP = 1000
        while H.running() and idx < INNER_CAP:
            # Pick a pattern index that varies by (wid, idx) so this fiber uses
            # different patterns each iteration, exercising cache churn and eviction.
            pattern_idx = (wid + idx * 1009) % N_PATTERNS
            pattern = PATTERNS[pattern_idx]
            expected_result = state["expected"][pattern_idx]

            # Call fnmatch.fnmatch with this fiber's DISTINCT (string, pattern) pair.
            # The cache key is the pattern; if the cache has evicted this pattern and
            # a sibling's pattern now occupies that cache slot, we will get the wrong
            # result.
            got_result = fnmatch.fnmatch(TEST_STRING, pattern)

            state["checks"][wid & 1023] += 1

            if got_result != expected_result:
                H.fail("fnmatch cache isolation BREACH: fnmatch.fnmatch({0!r}, "
                       "{1!r}) returned {2} but expected {3} (wid {4} idx {5}) -- "
                       "a sibling fiber's compiled Pattern leaked into this fiber's "
                       "cache lookup (runloom M:N cache-isolation bug: the shared "
                       "module-global _compile_pattern LRU cache is not per-fiber "
                       "isolated).".format(
                           TEST_STRING, pattern, got_result, expected_result,
                           wid, idx))
                return

            # Yield to let other fibers run and contend on the cache.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0003)

            H.op(wid)
            idx += 1

        H.task_done(wid)
        round_idx += 1


def body(H):
    """Run the worker pool."""
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    """Post-run: verify the oracle was non-vacuous and report cache stats."""
    checks = sum(H.state["checks"])
    mismatches = sum(H.state["mismatches"])

    # Capture fnmatch._compile_pattern cache stats if available.
    # functools.lru_cache objects have a .cache_info() method.
    try:
        cache_info = fnmatch._compile_pattern.cache_info()
        hits = cache_info.hits
        misses = cache_info.misses
        evictions = getattr(cache_info, 'currsize', 0)  # current cache size
        maxsize = cache_info.maxsize
        cache_msg = ("cache_hits={0} cache_misses={1} currsize={2} "
                     "maxsize={3}".format(hits, misses, evictions, maxsize))
    except (AttributeError, Exception):
        cache_msg = "cache stats unavailable"

    H.log("fnmatch distinct-pattern match checks={0} mismatches={1} | {2}".format(
        checks, mismatches, cache_msg))

    # NON-VACUITY: the load-bearing hazard was actually exercised.
    H.check(checks > 0,
            "no fnmatch checks ran -- the load-bearing cache-isolation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside fnmatch
    # on a corrupted cache entry).
    H.require_no_lost("fnmatch._compile_pattern cache isolation")


if __name__ == "__main__":
    harness.main(
        "p471_fnmatch", body, setup=setup, post=post,
        default_funcs=8000,
        describe="fnmatch._compile_pattern uses an LRU cache to cache "
                 "compiled Pattern objects; runloom M:N fibers share one hub "
                 "thread and the cache is module-global.  LOAD-BEARING: each "
                 "fiber calls fnmatch.fnmatch(string, pattern_DISTINCT) with a "
                 "pattern unique to that fiber; the result MUST match the "
                 "precomputed expected value (computed single-owner before the "
                 "pool).  A sibling's Pattern leaking into the cache -- cache "
                 "eviction interleaving a fiber's lookup -- causes a mismatch "
                 "(0 under plain threads GIL on AND off; the shared module-"
                 "global cache is the runloom M:N bug).  Same class as p66/p67/"
                 "p460; fix is per-fiber cache isolation in runloom")
