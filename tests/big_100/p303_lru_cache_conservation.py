"""big_100 / 303 -- functools.lru_cache C list+dict under cross-hub eviction churn.

No existing program touches functools at all -- and `functools.lru_cache`'s
internals are exactly the kind of MUTATING shared C container that the M:N model
stresses in a new way.  The C `_lru_cache_wrapper` keeps three coupled pieces of
state behind one per-object lock: a hash table (key -> node), a circular
doubly-linked LRU list, and the hits/misses counters.  Every lookup splices the
hit node to the front; every miss on a full cache EVICTS the LRU node (unlinks
it, re-keys its slot, relinks it at the front).  On free-threaded 3.13t with the
GIL off and tens of thousands of goroutines hammering the SAME cached function
across >= 8 hubs -- with KEYSPACE >> maxsize so EVERY round forces an eviction --
the splice/evict path runs under maximal contention.  The runloom-specific twist
is preempt-mid-splice: sysmon can yield a goroutine WHILE it is partway through
the C list relink (parallel to p211 preempt-in-dealloc), and a few workers fire
`cache_clear()` to tear the whole list down WHILE siblings are walking it.

The two failure modes that would matter:
  * a cross-key SPLICE or a stale read -- a lookup returns the value cached under
    a DIFFERENT key (key/value desync during a concurrent evict+insert), or a
    torn read of a half-relinked node; and
  * a corrupted linked list -- a dropped/duplicated node, or a self-referential
    cycle that hangs an eviction walk forever.

ORACLE (value-correctness is load-bearing):
  `f` is PURE and closed-form -- f(k) = (k*k) ^ SALT -- so the cache MUST return
  exactly what an independent recompute gives.  Every single call checks
  `H.check(cached == (k*k) ^ SALT)`; a cross-key splice or a stale/torn read
  fails fast with the offending key and both values.  This is the real bug
  signal and it needs no recorded baseline.

  hits/misses/currsize are a SANITY BOUND, NOT a strict equality: CPython does
  NOT guarantee exact hits/misses under free-threading (the counters are racy by
  design with the GIL off), so an exact `hits+misses == calls` would be a FALSE
  POSITIVE.  We assert only what must hold structurally -- `currsize <= maxsize`
  (the list never grows past its bound) and `hits+misses <= calls` and
  `hits+misses >= currsize` (you can't have more accounted calls than were made,
  nor fewer than the entries that are resident) -- plus value-correctness, which
  is exact.  cache_clear() races make exact accounting non-deterministic, so we
  only bound it.

  The harness watchdog catches a list-corruption HANG (an eviction walk caught in
  a self-cycle never returns -> no forward progress -> EXIT_HANG), and
  require_no_lost catches a worker stranded in a corrupted-list spin.

Two sub-modes per round, BOTH must hold value-correctness: a small-maxsize cache
(constant eviction churn) and a maxsize=None cache (unbounded -- no eviction, but
the same hash-table + counter contention and the same cache_clear() races).

Stresses: functools.lru_cache C linked-list+dict mutation, cross-hub eviction
churn (KEYSPACE >> maxsize), preempt-mid-splice, cache_clear() racing lookups,
value-correctness under concurrent evict+insert.

Good TSan / controlled-M:N-replay target: the list-splice vs evict vs clear is a
pure shared-memory race on a CPython-internal C container; a data-race report on
the node relink / counter increment is often the first signal, before the
value-correctness oracle even fires.
"""
import functools
import random

import harness
import runloom

SALT = 0x5BD1E995            # the closed-form fold constant; f is pure on this
MAXSIZE = 128                # small -> KEYSPACE >> maxsize forces eviction churn
KEYSPACE = 8192              # working set >> maxsize: every round evicts
CALLS_PER_ROUND = 2000       # hot loop so the splice/evict path runs under load
CLEAR_EVERY = 64             # a clear-worker calls cache_clear this often


def pure(k):
    """The exact value f MUST return for key k.  Closed form, no state."""
    return (k * k) ^ SALT


def make_caches():
    """A bounded cache (eviction churn) and an unbounded one (no eviction), each
    wrapping the SAME pure function.  Both are shared across all hubs."""

    @functools.lru_cache(maxsize=MAXSIZE)
    def f_bounded(k):
        return (k * k) ^ SALT

    @functools.lru_cache(maxsize=None)
    def f_unbounded(k):
        return (k * k) ^ SALT

    return f_bounded, f_unbounded


def hammer(H, wid, rng, f, calls):
    """Hot-call the shared cached f with keys spread over KEYSPACE >> maxsize so
    eviction churns constantly; every result must equal the pure recompute.  A
    yield_now mid-loop lets sysmon resume this goroutine on another hub partway
    between calls -- maximizing the chance of a preempt landing inside the C
    list-splice that the NEXT call drives."""
    for i in range(calls):
        if not H.running():
            return
        k = rng.randrange(KEYSPACE)
        v = f(k)
        want = (k * k) ^ SALT
        if v != want:
            H.fail("lru_cache returned WRONG value for key {0}: got {1!r} "
                   "expected {2!r} (cross-key splice or stale/torn read under "
                   "concurrent evict+insert)".format(k, v, want))
            return
        H.op(wid)
        if (i & 15) == 0:
            runloom.yield_now()     # invite a cross-hub resume mid-splice


def worker(H, wid, rng, state):
    """Most workers HAMMER both caches; a few are CLEARERS that periodically tear
    the whole linked list down with cache_clear() while siblings walk it."""
    f_bounded = state["f_bounded"]
    f_unbounded = state["f_unbounded"]
    is_clearer = wid < state["nclear"]
    for _ in H.round_range():
        if not H.running():
            break
        if is_clearer:
            # A clearer still does real lookups (so its value-correctness is also
            # checked), but every CLEAR_EVERY calls it rips the list down mid-
            # flight from siblings -- racing clear vs lookup/evict.
            for i in range(CALLS_PER_ROUND):
                if not H.running():
                    break
                k = rng.randrange(KEYSPACE)
                v = f_bounded(k)
                want = (k * k) ^ SALT
                if v != want:
                    H.fail("lru_cache (clearer) WRONG value for key {0}: got "
                           "{1!r} expected {2!r}".format(k, v, want))
                    return
                H.op(wid)
                if (i % CLEAR_EVERY) == 0:
                    f_bounded.cache_clear()
                    f_unbounded.cache_clear()
                    runloom.yield_now()
        else:
            hammer(H, wid, rng, f_bounded, CALLS_PER_ROUND)
            if H.running():
                hammer(H, wid, rng, f_unbounded, CALLS_PER_ROUND)
        H.task_done(wid)


def setup(H):
    f_bounded, f_unbounded = make_caches()
    # A handful of clearers: enough to race clear-vs-lookup hard, but a small
    # minority so the steady-state is eviction churn, not a perpetually empty
    # cache.  >= 2 even for tiny --funcs smoke runs.
    nclear = min(8, max(2, H.funcs // 200))
    H.state = {"f_bounded": f_bounded, "f_unbounded": f_unbounded,
               "nclear": nclear}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    f_bounded = H.state["f_bounded"]
    f_unbounded = H.state["f_unbounded"]
    bi = f_bounded.cache_info()
    ui = f_unbounded.cache_info()
    calls = H.total_ops()
    H.log("calls(ops)={0} clearers={1}".format(calls, H.state["nclear"]))
    H.log("bounded  : hits={0} misses={1} currsize={2} maxsize={3}".format(
        bi.hits, bi.misses, bi.currsize, bi.maxsize))
    H.log("unbounded: hits={0} misses={1} currsize={2} maxsize={3}".format(
        ui.hits, ui.misses, ui.currsize, ui.maxsize))

    H.check(calls > 0, "no cached calls happened")

    # currsize bound: the bounded cache's list must NEVER exceed maxsize -- a
    # dropped-evict or a leaked node would push it past the bound.  (The
    # unbounded cache has maxsize=None, so only the bounded one is bounded.)
    H.check(bi.currsize <= bi.maxsize,
            "lru_cache currsize {0} > maxsize {1} -- the LRU list grew past its "
            "bound (a node leaked / an eviction was dropped)".format(
                bi.currsize, bi.maxsize))

    # SANITY BOUNDS on accounting (NOT exact equality -- FT counters are racy by
    # design and cache_clear() races make exact accounting non-deterministic):
    #   * hits+misses <= total real calls          (can't account more than made)
    #   * hits+misses >= currsize                   (every resident entry was a miss)
    # A gross violation (counter underflow, or accounting wildly above the call
    # count) signals real counter/list corruption, not benign FT slack.
    for label, info in (("bounded", bi), ("unbounded", ui)):
        acc = info.hits + info.misses
        H.check(acc <= calls,
                "{0}: accounted hits+misses {1} > total calls {2} -- a "
                "double-counted/duplicated node (counter or list corruption)"
                .format(label, acc, calls))
        H.check(acc >= info.currsize,
                "{0}: accounted hits+misses {1} < currsize {2} -- a resident "
                "entry with no recorded miss (lost-count / list corruption)"
                .format(label, acc, info.currsize))

    H.require_no_lost("lru_cache walkers")


if __name__ == "__main__":
    harness.main("p303_lru_cache_conservation", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="functools.lru_cache C list+dict hammered across hubs "
                          "with KEYSPACE>>maxsize eviction churn + cache_clear() "
                          "racing lookups; cached==pure recompute (load-bearing), "
                          "currsize<=maxsize + hits/misses as a bound")
