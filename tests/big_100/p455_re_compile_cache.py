"""big_100 / 455 -- re.compile module-global cache (_cache / _cache2) IDENTITY
integrity under M:N.

re.compile caches compiled Patterns in TWO PROCESS-GLOBAL plain dicts (3.14's
re._compile): a fast-path `re._cache2` (cap _MAXCACHE2=256, read first) and an
LRU `re._cache` (cap _MAXCACHE=512).  On a miss `_compile` does, on the SHARED
dicts, a bare read-modify-write with NO lock:

    p = _cache.pop(key, None)                       # RMW
    if p is None:
        p = _compiler.compile(pattern, flags)
        if len(_cache) >= _MAXCACHE:
            del _cache[next(iter(_cache))]          # evict-oldest RMW
    _cache[key] = p                                 # insert RMW
    ...
    if len(_cache2) >= _MAXCACHE2:
        del _cache2[next(iter(_cache2))]            # evict RMW
    _cache2[key] = p                                # insert RMW

That is exactly the shared-mutable-dict pop/evict/insert race the suite already
landed bugs for in functools.lru_cache (CONFIRMED UPSTREAM CPython, arm64 FT) and
the OrderedDict candidate.  With the GIL off, many hubs calling re.compile of
DISTINCT patterns drive those pop/evict/insert RMWs into each other on the SAME
two dicts while a concurrent re.purge() does `_cache.clear()` / `_cache2.clear()`.

re.compile is DOCUMENTED THREAD-SAFE, so any WRONG match here is a REAL runtime
bug -- the load-bearing oracle, not a measured caveat.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  LOAD-BEARING -- CACHE IDENTITY.  Each fiber owns a UNIQUE family of patterns
  whose marker encodes its wid (+ round + slot); only its OWN input matches its
  OWN pattern, and a sibling's input must NOT.  The universe of distinct patterns
  is FAR more than 512 (wid-derived) so re._cache churns + evicts hard.  Across
  yields a fiber compiles its pattern and matches its own input, asserting the
  captured group equals its own marker -- a torn cache returning a SIBLING Pattern
  yields a wrong group / None / exception / crash.  We VERIFIED with a standalone
  plain-threads control (16 threads x 4000 rounds, 200 distinct patterns each, a
  concurrent purger) that this identity oracle is GREEN under BOTH PYTHON_GIL=1
  AND PYTHON_GIL=0 -- i.e. re.compile keeps returning the CORRECT Pattern even
  with the GIL off in plain threads (64000/64000 correct matches each).  So a
  wrong match is NOT documented-unsafe usage for any concurrency model -- it would
  be a genuine runloom M:N corruption of the shared cache.  A SINGLE-OWNER control
  fiber does the identical compile+match loop on its own private patterns and must
  also always be correct (proves the machinery itself is sound -- a wrong result
  THERE would be a CPython count-machinery bug, not contention).

  MEASURED (report-ONLY, NEVER fails) -- CACHE-SIZE OVERSHOOT.  The SAME standalone
  control showed that GIL-OFF the global re._cache grows WAY past its _MAXCACHE
  cap (512): it reached ~2900-3100 entries because the evict-oldest RMW
  (`del _cache[next(iter(_cache))]`) loses to concurrent inserts and the cap is
  not enforced -- the identical lru_cache-exceeds-maxsize overshoot the suite
  pinned upstream.  Crucially this overshoot REPRODUCES under plain-threads-GIL-
  OFF (no runloom), so hard-failing on it would be a FALSE-POSITIVE detector.  We
  MEASURE the peak cache size + compile count and REPORT them (an eviction-
  pressure rate), NEVER assert on them -- like p67's TLS leak rate.  The identity
  oracle stays GREEN even while the cache overshoots, which is the whole point:
  the cache may be over-full, but it must never hand back the WRONG Pattern.

ORACLES:
  * LOAD-BEARING -- CACHE IDENTITY (per-op, HARD, fail-fast): every re.compile of
    a fiber's own marker-pattern, matched against its own input, returns the
    CORRECT group; the sibling/foreign input never matches.  A wrong/None/torn
    match or an exception/crash out of re.compile/match is a runloom corruption of
    the shared cache.  GREEN under plain-threads-GIL-on AND a correct runloom.
  * SINGLE-OWNER CONTROL (per-op, HARD): a lone fiber doing the identical
    compile+match on its OWN private patterns is ALWAYS correct -- isolates the
    machinery from contention.
  * COMPLETENESS (post, HARD): require_no_lost -- no fiber parked-then-vanished
    inside re.compile / re.purge.

  * MEASURED (post, report-ONLY, NEVER fails): peak global re._cache / re._cache2
    size (overshoot past _MAXCACHE under GIL-off eviction-RMW loss -- documented-
    unsafe, reproduces under plain GIL-off threads) + total compiles + purges.

Stresses: re._compile pop/evict-oldest/insert RMW on the shared _cache / _cache2
dicts under GIL-off contention, re.purge() clear-vs-insert, _MAXCACHE eviction
churn over a >512 distinct-pattern universe, torn/sibling cached Pattern identity,
no-lost-wake inside re.compile / re.purge.

Good TSan / controlled-M:N-replay target: the `_cache.pop` / `del
_cache[next(iter(_cache))]` / `_cache[key]=p` sequence over a shared dict is a
textbook read-modify-write data race; a TSan report on the cache dict entry, or a
single wrong cached Pattern under replay, localizes the corruption before the
identity oracle even fires.
"""
import re

import harness
import runloom

# Cap the load-bearing pool: this is a correctness probe of the shared-cache
# identity, not a scale soak.
MAX_WORKERS = 8000

# Distinct marker-patterns each worker cycles through per round.  worker markers
# are wid-derived and unique, so the GLOBAL universe of distinct patterns is
# >> _MAXCACHE (512) once even a few hundred workers each cycle a handful -- that
# is what makes re._cache evict + churn hard (the eviction-RMW race window).
PATTERNS_PER_ROUND = 6

# Fraction of the pool dedicated to hammering re.purge() (clear-vs-insert stress).
# A handful is plenty; too many starve the identity workers of cache hits.
PURGER_FRACTION = 0.05


def compile_and_check(H, who, marker, salt):
    """Compile the UNIQUE marker-pattern, match our OWN input, and assert the
    captured group is exactly ours.  Returns True on a correct match.  A torn
    cache that hands back a SIBLING Pattern yields None / a wrong group / an
    exception -- the load-bearing corruption signal.

    The pattern is unique to `marker` (which encodes wid+round+slot, or 'ctl-'
    for the single-owner control), so:
      * our own input  'M<marker>#<want>$'  MUST match and capture <want>;
      * a foreign input 'M<marker+1>#<want>$' MUST NOT match our pattern.
    """
    # ^...$ anchored, marker baked into the literal so the compiled program is
    # genuinely distinct per marker (distinct cache key AND distinct automaton).
    pat = re.compile(r"^M{0}#(\d+)\$$".format(marker))
    want = (salt * 2654435761) & 0xFFFFF
    text = "M{0}#{1}$".format(marker, want)
    m = pat.match(text)
    if m is None:
        H.fail("CACHE IDENTITY CORRUPTED ({0}): own pattern for marker {1} did "
               "NOT match its OWN input {2!r} -- re.compile returned a torn / "
               "SIBLING Pattern from the shared global re._cache under M:N "
               "(re.compile is documented thread-safe, so this is a runloom "
               "corruption of the shared cache)".format(who, marker, text))
        return False
    g = m.group(1)
    if g != str(want):
        H.fail("CACHE IDENTITY CORRUPTED ({0}): marker {1} captured group "
               "{2!r} != own {3!r} -- the shared re._cache handed back a "
               "DIFFERENT fiber's compiled Pattern (sibling-Pattern torn read "
               "under the concurrent pop/evict/insert RMW)".format(
                   who, marker, g, want))
        return False
    # Negative: a FOREIGN input must not match our pattern.  If it does, the
    # cache gave us a looser/wrong automaton.
    if pat.match("M{0}#{1}$".format(marker + 1, want)) is not None:
        H.fail("CACHE IDENTITY CORRUPTED ({0}): a FOREIGN input matched the "
               "pattern cached for marker {1} -- re._cache returned the wrong "
               "(looser) compiled Pattern under M:N".format(who, marker))
        return False
    return True


def worker(H, wid, rng, state):
    """LOAD-BEARING identity worker: compile + match this fiber's OWN unique
    marker-patterns across yields, while siblings churn the shared cache and
    purgers clear it.  Every match must be correct for our own pattern."""
    base = wid * 0x100000          # disjoint marker space per worker (no overlap)
    n = 0
    r = 0
    for _ in H.round_range():
        if not H.running():
            break
        for slot in range(PATTERNS_PER_ROUND):
            if H.failed:
                return
            # Marker unique to (wid, r, slot): a fresh distinct pattern most of
            # the time so re._cache evicts; the disjoint per-worker base means no
            # two workers ever share a marker, so a cross-fiber hit is corruption.
            marker = base + ((r * PATTERNS_PER_ROUND + slot) & 0xFFFFF)
            if not compile_and_check(H, "wid {0}".format(wid), marker, marker):
                return
            n += 1
            # Yield / sleep BETWEEN compiles so this fiber is preempted / migrated
            # while siblings + purgers mutate the shared cache around it.
            if (slot & 1) == 0:
                runloom.yield_now()
            else:
                runloom.sleep(0.0002)
        H.op(wid)
        r += 1
    state["compiles"][wid & 1023] += n
    H.task_done(wid)


def purger(H, wid, rng, state):
    """REPORT-stress fiber: hammer re.purge() (clear-vs-insert on the shared
    cache) while identity workers compile+match.  purge() raising is itself a
    fault; otherwise this only adds clear contention."""
    p = 0
    for _ in H.round_range():
        if not H.running():
            break
        try:
            re.purge()
        except Exception as exc:           # clear-vs-insert race surfacing
            H.fail("re.purge() raised {0!r} -- clear-vs-insert race on the "
                   "shared global re._cache under M:N".format(exc))
            return
        p += 1
        runloom.sleep(0.0005)
    state["purges"][wid & 1023] += p
    H.task_done(wid)


def control_worker(H, state):
    """SINGLE-OWNER CONTROL: one fiber doing the identical compile+match on its
    OWN private marker space.  Must ALWAYS be correct -- isolates the compile/
    match machinery from contention (a wrong result here would be a CPython bug,
    not runloom contention).  Records progressively so a partial run still counts.

    Does a FIXED block of work up front (independent of the deadline, so it is
    never starved to zero by the flooded worker pool), then keeps going while the
    run is live."""
    base = 0x7F000000              # disjoint from every worker base
    r = 0
    # A guaranteed minimum block so the control is never vacuous even if the load-
    # bearing pool finishes its single round before the control is scheduled much.
    min_rounds = 64
    while not H.failed and (r < min_rounds or H.running()):
        for slot in range(PATTERNS_PER_ROUND):
            marker = base + ((r * PATTERNS_PER_ROUND + slot) & 0xFFFFF)
            if not compile_and_check(H, "single-owner control", marker, marker):
                return
            state["control_checks"][0] += 1      # progressive, single-writer
            runloom.yield_now()
        r += 1


def setup(H):
    H.state = {
        "compiles": [0] * 1024,        # successful identity compiles (load-bearing)
        "purges": [0] * 1024,          # re.purge() calls (report)
        "control_checks": [0],         # single-owner control matches
        "peak_cache": [0],             # peak len(re._cache)  (MEASURED overshoot)
        "peak_cache2": [0],            # peak len(re._cache2) (MEASURED overshoot)
    }
    # Start from a clean cache so the overshoot we report is attributable to this
    # run (not pre-existing import-time compiles).
    re.purge()


def cache_sampler(H, state):
    """Sample the GLOBAL re._cache / re._cache2 sizes while the run is hot and
    record the PEAK.  MEASURED-only: GIL-off the evict-oldest RMW loses to
    concurrent inserts so the cache overshoots _MAXCACHE -- we report that
    overshoot, never assert on it (it reproduces under plain GIL-off threads).

    Does a guaranteed minimum number of samples (independent of the deadline) so
    the overshoot measurement is never starved to zero by the flooded worker
    pool, then keeps sampling while the run is live."""
    def sample():
        c = len(re._cache)
        c2 = len(re._cache2)
        if c > state["peak_cache"][0]:
            state["peak_cache"][0] = c
        if c2 > state["peak_cache2"][0]:
            state["peak_cache2"][0] = c2

    min_samples = 256
    i = 0
    while not H.failed and (i < min_samples or H.running()):
        sample()
        i += 1
        runloom.sleep(0.002)
    sample()                       # one last peak read at teardown


def body(H):
    n = max(2, H.funcs)   # uncapped: respect --funcs (harness max_funcs is the mem backstop)
    npurge = max(1, int(n * PURGER_FRACTION))
    nident = max(1, n - npurge)

    # MEASURED cache-size sampler + the single-owner control run alongside the
    # load-bearing pool.  H.fiber forwards only the EXTRA args (it does NOT inject
    # H), so pass H explicitly.
    H.fiber(cache_sampler, H, H.state)
    H.fiber(control_worker, H, H.state)

    # The load-bearing identity pool.
    H.run_pool(nident, worker, H.state, max_concurrent=nident)
    # A small purger pool sharing the run window (clear-vs-insert stress).  Spawn
    # directly so they live for the whole run alongside the identity pool.
    for i in range(npurge):
        rng = H.derive("purger", i)
        H.expected += 1
        H.fiber(H._worker_wrap, purger, nident + i, rng, (H.state,))


def post(H):
    compiles = sum(H.state["compiles"])
    purges = sum(H.state["purges"])
    ctl = H.state["control_checks"][0]
    peak = H.state["peak_cache"][0]
    peak2 = H.state["peak_cache2"][0]

    # Reaching post with no failure means EVERY per-op identity check held (they
    # are fail-fast).  Report the load-bearing work done + the MEASURED overshoot.
    H.log(
        "re._cache IDENTITY: {0} load-bearing compiles + {1} single-owner control "
        "matches all CORRECT (a torn/sibling Pattern would have failed fast); "
        "{2} re.purge() clears | MEASURED eviction overshoot: peak "
        "len(re._cache)={3} (cap _MAXCACHE={4}), peak len(re._cache2)={5} (cap "
        "_MAXCACHE2={6}) -- REPORT ONLY: GIL-off the evict-oldest RMW loses to "
        "concurrent inserts so the cache overshoots its cap (reproduces under "
        "plain-threads-GIL-off, NOT a runloom bug; identity stays correct "
        "anyway)".format(
            compiles, ctl, purges, peak, re._MAXCACHE, peak2, re._MAXCACHE2))

    # Non-vacuity: the load-bearing hazard was actually exercised (the cache was
    # driven + the control ran), else the identity oracle would be vacuous.
    H.check(compiles > 0,
            "no load-bearing re.compile identity checks ran -- the shared-cache "
            "pop/evict/insert race window was never exercised (oracle vacuous)")
    H.check(ctl > 0,
            "the single-owner control never ran -- cannot attribute correctness "
            "to the machinery vs contention (control vacuous)")

    if peak > re._MAXCACHE:
        H.log("note: re._cache overshot its _MAXCACHE cap ({0} > {1}) -- the "
              "documented-unsafe GIL-off eviction-RMW overshoot (same class as "
              "the lru_cache-exceeds-maxsize bug, reproduces under plain GIL-off "
              "threads), REPORT ONLY; the identity oracle stayed GREEN so no "
              "WRONG Pattern was ever returned".format(peak, re._MAXCACHE))

    # COMPLETENESS: no fiber parked-then-vanished inside re.compile / re.purge.
    H.require_no_lost("re._cache identity completeness")


if __name__ == "__main__":
    harness.main(
        "p455_re_compile_cache", body, setup=setup, post=post,
        default_funcs=8000,
        describe="many hubs call re.compile of DISTINCT wid-unique patterns "
                 "(universe >> _MAXCACHE=512) while a concurrent re.purge() clears "
                 "the SHARED global re._cache/_cache2 dicts; load-bearing CACHE "
                 "IDENTITY oracle: every fiber's own pattern matches its own input "
                 "with the right group, a sibling input never does -- a torn/"
                 "sibling cached Pattern (wrong/None match or exception) is a REAL "
                 "runloom bug (re.compile is documented thread-safe).  The cache-"
                 "size OVERSHOOT past _MAXCACHE (eviction-RMW loss, reproduces "
                 "under plain GIL-off threads) is MEASURED + reported only")
