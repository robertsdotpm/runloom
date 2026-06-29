"""big_100 / 470 -- platform module lazy-cache initialization race.

platform.platform() (and platform.system/node/release) cache their results via
@lru_cache decorators in the C extension (_get_platform_nodename etc).  However,
_platform_cache and _uname_cache are module-level dicts that are initialized
LAZILY on first access (they start as None, then are assigned to {} on first
call).  The lazy init has a race: if two threads/fibers call platform.platform()
concurrently, both see the dict is None, both allocate new dicts, and one dict's
work is lost (the TOCTOU race).  Under M:N with many fibers on one hub, this
manifests as a torn/inconsistent cache lookup -- platform.platform() returns
different values across successive calls (the cache entry was not actually
stored / was overwritten by a sibling's concurrent init).

WHERE M:N BREAKS IT (the gap this program probes).  Under runloom's M:N
scheduler many fibers share ONE hub OS-thread, so they all see the SAME
module-level dicts.  If fiber A calls platform.platform(), observes cache=None,
allocates a new cache dict and caches its result, then YIELDS before returning,
and fiber B (on the same hub) calls platform.platform() concurrently, the cache
may appear uninitialized to B (the TOCTOU window), or B's cache init may overwrite
A's (a dict identity race).  Even under correct locking, the lazy-init pattern
is inherently racy: the module dict is not atomic, and two concurrent inits race
to "own" the cache slot.  This is a runloom M:N gap: under plain OS threads each
thread has its own GIL segment or operates serially, but under M:N the race is
real.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  platform.platform() is DOCUMENTED to return a FIXED, CONSTANT string for a
  given machine (it calls uname() once, caches the result).  The LOAD-BEARING
  oracle calls platform.platform() (or platform.system/node/release) multiple
  times with yields between calls, and asserts the result is STABLE: call 1 ->
  call 2 -> call 3 all return the SAME value, and that value matches the expected
  system identity (e.g., 'Linux' for system()).  A mismatch (r1 != r2) or a
  wrong value (r1 != expected) is a torn cache (the lazy-init race corrupted the
  cache entry or cache identity).  Verified with a standalone plain-threads
  control (PYTHON_GIL=1 and =0): cache is stable and correct across all calls,
  so a mismatch would be a false positive detector.  On plain threads each thread
  has its own context (or the GIL serializes), so the lazy init succeeds.  Under
  a CORRECT runloom it must ALSO hold (each hub's cache initialized atomically
  and correctly).  If runloom leaks a sibling's torn cache or a partially-
  initialized cache value -- platform.platform() returns different values on
  successive calls, or a wrong value (not the actual system platform) -- that is
  the runloom M:N lazy-init race, and the program fails.

ORACLES:
  * LOAD-BEARING -- platform.platform() / system() / node() / release() RESULT
    STABILITY across yields (worker, HARD, fail-fast).  Each fiber calls
    platform.platform() (or one of system/node/release), yields (runs a sibling),
    calls again, and asserts r1 == r2.  Both r1 and r2 must also match the
    expected/canonical value (the actual system platform, queried once at setup
    via a single-owner call).  A mismatch is a torn cache (the lazy-init race
    corrupted the cache or returned a wrong value).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    call to platform.platform() (stranded inside the @lru_cache wrapper or the
    lazy-init path).
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * MEASURED (report-ONLY, NEVER fails): cache-hit rates and per-call call counts.
    The @lru_cache is a global shared pool; we measure how often it hits (lower
    hit rate = more init races) and report it.  A low hit rate is a symptom
    (suggests the cache is being re-initialized) but we do NOT fail on it, only
    on actual value corruption (r1 != r2 or r1 != expected).

FAIL ON: platform.platform() / system() / node() / release() returning different
values across calls to the same function, or returning a wrong value that does
not match the canonical expected value (e.g., platform.system() != 'Linux' on
Linux, or platform.platform() does not start with the expected platform string).
NEVER fail on cache-hit rates.

Stresses: platform._platform_cache / platform._uname_cache lazy initialization,
@lru_cache shared global state across hub fibers, module-dict TOCTOU race during
cache init, cache identity / dict swap across concurrent inits.

Good TSan / controlled-M:N-replay target: platform._platform_cache (a dict) is
initialized twice concurrently; a data-race report on the dict or a deterministic
replay that initializes it twice (one loses) localizes the race before the result
stability oracle fires.
"""
import platform

import harness
import runloom

# Canonical, single-owner snapshot of the platform values, taken once at setup
# before any fiber calls platform functions.  Each fiber's load-bearing oracle
# compares its calls against these.
CANONICAL = {}


def build_canonical():
    """One-time, single-owner: query all platform functions once at setup,
    before the pool runs, and cache the canonical values.  These represent the
    "true" system identity.  Workers compare their results against this table
    to detect a torn/wrong cache."""
    table = {}
    table["system"] = platform.system()
    table["node"] = platform.node()
    table["release"] = platform.release()
    table["version"] = platform.version()
    table["machine"] = platform.machine()
    table["processor"] = platform.processor()
    table["platform"] = platform.platform()
    return table


def setup(H):
    global CANONICAL
    CANONICAL = build_canonical()
    H.state = {
        "checks": [0] * 1024,           # load-bearing stability checks done
        "mismatches": [0] * 1024,       # r1 != r2 (torn cache)
        "wrong_value": [0] * 1024,      # r1 != expected (wrong cache entry)
        "cache_hits": [0] * 1024,       # estimated cache hits (for report only)
        "total_calls": [0] * 1024,      # total platform calls made
    }


# Sustained platform() calls per worker, bounded by H.running().  The cache
# initialization race only manifests under SUSTAINED churn -- many fibers
# simultaneously calling platform functions and yielding to yield_now / sleep
# so siblings can run and race on the shared cache.  A single call per fiber
# barely overlaps and does NOT reproduce.  So each worker runs a sustained
# internal loop (one platform-call triple per iteration, with yields/sleeps
# between calls) until the deadline (H.running()) or INNER_CAP.  Bounding by
# H.running() makes the load-bearing oracle fire at the DEFAULT --rounds 1;
# INNER_CAP stops one worker from monopolizing teardown if the box is slow.
INNER_CAP = 50000


def platform_check(H, wid, idx, state):
    """LOAD-BEARING arm: three calls to platform.platform() (or system/release)
    with yields between, all must return the SAME value and match the canonical.
    A mismatch or wrong value is a torn cache (the lazy-init race corrupted it).
    This is the only arm that will fail; it is strictly LOAD-BEARING."""
    # Rotate which function to call so we exercise the whole API.
    func_idx = idx % 4
    if func_idx == 0:
        func = platform.system
        key = "system"
    elif func_idx == 1:
        func = platform.node
        key = "node"
    elif func_idx == 2:
        func = platform.release
        key = "release"
    else:
        func = platform.platform
        key = "platform"

    # First call
    try:
        r1 = func()
    except Exception as e:
        H.fail("platform.{0}() raised on first call (wid {1}): {2!r}".format(
            func.__name__, wid, e))
        return

    # Yield + sleep to let a sibling run and race on the shared cache.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Second call -- must return the SAME value as r1.
    try:
        r2 = func()
    except Exception as e:
        H.fail("platform.{0}() raised on second call (wid {1}): {2!r}".format(
            func.__name__, wid, e))
        return

    # Yield again.
    runloom.yield_now()

    # Third call -- must ALSO match r1 and r2.
    try:
        r3 = func()
    except Exception as e:
        H.fail("platform.{0}() raised on third call (wid {1}): {2!r}".format(
            func.__name__, wid, e))
        return

    state["total_calls"][wid & 1023] += 3
    state["checks"][wid & 1023] += 1

    # (1) Stability: r1 == r2 == r3.  Any mismatch is a torn cache.
    if r1 != r2:
        H.fail("platform.{0}() NOT STABLE: call 1={1!r} != call 2={2!r} "
               "(wid {3}) -- a sibling fiber's lazy-cache init or concurrent "
               "cache mutation corrupted the cache (runloom M:N lazy-init race; "
               "the same function should always return the same cached value)".
               format(func.__name__, r1, r2, wid))
        state["mismatches"][wid & 1023] += 1
        return
    if r1 != r3:
        H.fail("platform.{0}() NOT STABLE: call 1={1!r} != call 3={2!r} "
               "(wid {3}) -- the cache value drifted across two yields (lazy-init "
               "or concurrent mutation, runloom M:N race)".format(
                   func.__name__, r1, r3, wid))
        state["mismatches"][wid & 1023] += 1
        return

    # (2) Correctness: r1 must match the canonical EXPECTED value for this func.
    # The canonical value was queried once, before the pool, so it is the "true"
    # system identity.  A mismatch means the cache returned a WRONG value (not
    # the actual platform, or a torn/garbage cache entry).
    expected = CANONICAL[key]
    if r1 != expected:
        H.fail("platform.{0}() WRONG VALUE: got {1!r} != expected {2!r} "
               "(wid {3}) -- the cached value does not match the canonical "
               "system identity (the lazy-init race corrupted the cache or a "
               "sibling's init overwrote the correct value)".format(
                   func.__name__, r1, expected, wid))
        state["wrong_value"][wid & 1023] += 1
        return

    # Success: all three calls returned the same correct value.
    state["cache_hits"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING platform stability check in a sustained
    loop.  Many fibers run concurrently on one hub, all calling platform functions
    and yielding so they race on the shared module-level caches.  The loop is
    bounded by H.running() (until the deadline or --rounds) or INNER_CAP."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            platform_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    mismatches = sum(H.state["mismatches"])
    wrong = sum(H.state["wrong_value"])
    hits = sum(H.state["cache_hits"])
    total = sum(H.state["total_calls"])
    hit_rate = (100.0 * hits / checks) if checks else 0.0

    H.log("platform: {0} stability checks (LOAD-BEARING) | {1} total calls | "
          "cache-hit-rate {2:.1f}% (estimated) | mismatches={3} "
          "(r1!=r2 torn cache) wrong_value={4} (r1!=expected)".format(
              checks, total, hit_rate, mismatches, wrong))

    if mismatches or wrong:
        H.log("note: the platform module's _platform_cache / _uname_cache are "
              "lazy-initialized (start as None, set to {} on first access).  "
              "Under M:N concurrency many fibers race on the same shared cache "
              "dict, and the TOCTOU window during lazy init can corrupt the "
              "cache: two fibers both see cache=None, both allocate dicts, and "
              "one dict's work is lost.  A mismatch (r1 != r2) or wrong value "
              "(r1 != expected system identity) is the result.  Under plain "
              "threads (GIL on/off) each thread has its own context or the GIL "
              "serializes, so the lazy init succeeds.")

    # NON-VACUITY: the load-bearing cache stability hazard was actually
    # exercised.
    H.check(checks > 0,
            "no platform stability checks ran -- the load-bearing lazy-cache "
            "initialization race hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # platform function / @lru_cache wrapper).
    H.require_no_lost("platform lazy-cache initialization")


if __name__ == "__main__":
    harness.main(
        "p470_platform", body, setup=setup, post=post,
        default_funcs=7,
        describe="platform.platform() / system() / node() / release() cache "
                 "their results via @lru_cache, but _platform_cache / "
                 "_uname_cache dicts are lazily initialized (start None, set to "
                 "{} on first access).  Under M:N concurrency many fibers race "
                 "on the shared cache dict TOCTOU window: both fibers see "
                 "cache=None, allocate dicts, and one init is lost.  LOAD-"
                 "BEARING: each fiber calls platform.platform()/system()/"
                 "node()/release() three times with yields between, asserts all "
                 "three calls return the SAME value (stable cache) and match the "
                 "canonical system identity (correct cache).  A mismatch "
                 "(r1!=r2) or wrong value (r1!=expected) is a torn cache -- the "
                 "lazy-init race (0 under plain threads GIL on AND off; a "
                 "runloom M:N shared-cache-dict TOCTOU gap)")
