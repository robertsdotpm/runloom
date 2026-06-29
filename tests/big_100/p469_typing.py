"""big_100 / 469 -- typing._tp_cache generic-subscription VALUE integrity under M:N.

typing._tp_cache decorates __getitem__ of generic types with a BOUNDED lru_cache
so repeated subscriptions to a generic alias (List[int], Dict[str, int], ...)
reuse a cached result.  When a fiber subscribes to a generic alias the result is
computed once and (usually) cached.  Under M:N many fibers share one hub
OS-thread, so all sibling fibers on that hub hit the SAME _tp_cache; the band of
distinct params they rotate through keeps the bounded cache under EVICTION
pressure, so an entry a fiber subscribed can be evicted before it re-subscribes.

WHICH ORACLE IS LOAD-BEARING, AND WHY (the discriminator discipline):

  THE VALUE MUST BE RIGHT.  When a fiber subscribes List[p], parks across a
  scheduling point, then re-subscribes List[p], the runtime MUST hand back an
  alias whose __origin__ is `list` and whose __args__ are exactly (p,) -- the
  value THIS fiber asked for.  That invariant holds on ANY correct runtime
  (single-thread, plain threads GIL on AND off, runloom M:N): the alias's VALUE
  is a pure function of what was subscribed, independent of caching.  If a fiber
  ever recovers an alias carrying a DIFFERENT type/args (a sibling's subscription
  leaking through a corrupted cache, or torn cache state) that is genuine
  corruption -- a real bug at any scale -- so this VALUE check is LOAD-BEARING
  and fires exit 1.

  IDENTITY (id(recovered) == id(subscribed)) IS NOT LOAD-BEARING.  The lru_cache
  is BOUNDED: even single-threaded, a correct runtime legitimately EVICTS an
  entry under band pressure and recomputes a NEW, equal alias on re-subscription
  (a different id, but with the CORRECT __args__/__origin__).  An id-mismatch is
  therefore benign cache eviction, NOT corruption.  An oracle that hard-failed on
  id(recovered) != id(subscribed) is a FALSE-POSITIVE detector: it fires on a
  perfectly correct single-threaded runtime under eviction.  Per the discipline
  ("if the oracle fires on a correct runtime, demote to measured"), the
  id-mismatch is DEMOTED to a MEASURED eviction-rate counter -- reported as a
  rate, NEVER failed.

ORACLES:
  * LOAD-BEARING -- VALUE INTEGRITY (per-check, fail-fast).  Each fiber subscribes
    a generic alias with its own param p, records (origin, expected __args__),
    parks across yield/sleep, re-subscribes the SAME (origin, p), and asserts the
    recovered alias has the CORRECT __origin__ AND the CORRECT __args__ for what
    THIS fiber subscribed.  A wrong origin/args = a sibling's value leaked through
    a corrupted cache -> H.fail -> exit 1.  Passes on every correct runtime
    (single-thread, plain GIL on/off, runloom M:N) because the value is a pure
    function of the subscription.
  * MEASURED -- EVICTION RATE (post, report-only, NEVER fails).  id(recovered) !=
    id(subscribed) counts a benign bounded-lru_cache eviction (the alias was
    re-derived, equal value, new object).  Reported as a rate so the eviction
    pressure is explicit; it NEVER fails the program.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    subscription never returns; the watchdog + require_no_lost catch hangs.
  * NON-VACUITY (post, HARD): the cache-access hazard was actually exercised.

INJECTION (self-test): with RUNLOOM_P469_INJECT=1 the recovery returns a SIBLING
alias (wrong __args__/__origin__) on a fraction of checks -- the load-bearing
VALUE oracle MUST then fire exit 1, proving it is not vacuous.

CLASSIFICATION: M:N value-integrity probe (load-bearing) + eviction-rate measure.

Stresses: typing._tp_cache lru_cache internal state under concurrent free-
threaded access from M:N hub fibers, concurrent generic-subscription churn +
bounded-cache eviction, cache access during fiber yields, cache-state corruption
that would leak a sibling's subscription value.

Good TSan / controlled-M:N-replay target: typing._tp_cache's internal lru_cache
dict or linked-list under concurrent subscription and eviction -- a torn read
that surfaced a sibling alias's __args__ is exactly what the load-bearing VALUE
oracle catches.
"""
import os
import sys
import typing
from typing import List, Dict, Tuple, Set

import harness
import runloom

# Self-test injection: when set, recovery returns a SIBLING alias (wrong
# __args__/__origin__) on a fraction of checks so the LOAD-BEARING value oracle
# must fire exit 1.  Off in normal runs (a correct runtime always passes).
INJECT = os.environ.get("RUNLOOM_P469_INJECT") == "1"

# Per-fiber parameter-band size: each fiber rotates through parameter values
# 0..PARAM_SPAN-1 when subscripting a generic (e.g., List[wid % PARAM_SPAN]).
# Small enough that the cache is under eviction pressure (cycling fibers in/out);
# large enough that a single fiber's repeated subscription doesn't always hit;
# distinct across fibers so siblings never contend on the SAME param simultaneously.
PARAM_MIN = 1
PARAM_MAX = 100
PARAM_SPAN = PARAM_MAX - PARAM_MIN + 1

# The TYPES to test: the generic origins whose subscriptions are cached.
TYPES_TO_TEST = [List, Tuple, Dict, Set]


def setup(H):
    H.state = {
        "cache_checks": [0] * 1024,      # load-bearing value-integrity checks
        "evictions": [0] * 1024,         # MEASURED: id(recovered) != id(subscribed)
    }


def _subscript(origin, p):
    """Subscribe `origin` with this fiber's param p, returning
    (alias, expected_origin, expected_args).  expected_* describe the VALUE this
    subscription MUST yield on any correct runtime, independent of caching."""
    if origin is Dict:
        return origin[str, p], dict, (str, p)
    if origin is Tuple:
        return origin[int, p], tuple, (int, p)
    # List, Set: subscript with p alone.
    expected_origin = list if origin is List else set
    return origin[p], expected_origin, (p,)


# --------------------------------------------------------------------------
# LOAD-BEARING arm: VALUE INTEGRITY (per-check, fail-fast).  A fiber subscribes a
# generic alias, parks across yield/sleep (a sibling on the same hub may evict
# its entry from the bounded _tp_cache), then re-subscribes and asserts the
# recovered alias carries the CORRECT __origin__ and __args__ for what THIS fiber
# subscribed.  The VALUE is a pure function of the subscription, so this passes on
# every correct runtime; a wrong value = a sibling's subscription leaked through a
# corrupted cache (genuine corruption) -> exit 1.
#
# MEASURED arm: id(recovered) != id(subscribed) is benign bounded-lru_cache
# EVICTION (the equal alias was recomputed as a new object); counted + reported,
# NEVER failed.
# --------------------------------------------------------------------------
def cache_check(H, wid, idx, state):
    # Rotate the param by (wid + idx) so this fiber's band differs from its hub
    # siblings' bands and from its own previous iteration.
    p = PARAM_MIN + ((wid + idx) % PARAM_SPAN)

    # Choose the type to subscript (round-robin through TYPES_TO_TEST).
    origin_idx = (wid + idx) % len(TYPES_TO_TEST)
    origin = TYPES_TO_TEST[origin_idx]

    subscripted, exp_origin, exp_args = _subscript(origin, p)
    subscribed_id = id(subscripted)

    # YIELD + optional SLEEP: a sibling fiber on this hub runs (and may evict
    # our entry from the _tp_cache's bounded lru_cache) while this fiber is
    # parked.  The optional sleep lengthens the park so concurrent evictions
    # reliably interleave across hub migrations.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Recover the SAME type by re-subscripting.  On a correct runtime the VALUE
    # (origin + args) is exactly what we subscribed -- whether the cache served a
    # hit (same id) or evicted-and-recomputed (new id, equal value).
    recovered, _, _ = _subscript(origin, p)

    # INJECTION self-test: surface a SIBLING alias (wrong value) on a fraction of
    # checks so the LOAD-BEARING value oracle must fire.  Off in normal runs.
    if INJECT and (idx % 7) == 3:
        sib_p = PARAM_MIN + ((p + 1 - PARAM_MIN) % PARAM_SPAN)  # a DIFFERENT param
        recovered, _, _ = _subscript(origin, sib_p)

    recovered_id = id(recovered)
    state["cache_checks"][wid & 1023] += 1

    # MEASURED (report-only, NEVER fails): a different id = benign cache eviction.
    if recovered_id != subscribed_id:
        state["evictions"][wid & 1023] += 1

    # LOAD-BEARING (fail-fast): the recovered alias MUST carry the CORRECT VALUE
    # (origin + args) THIS fiber subscribed.  A wrong value is a sibling's
    # subscription leaking through a corrupted cache -- genuine corruption.
    if recovered.__origin__ is not exp_origin or recovered.__args__ != exp_args:
        H.fail("typing._tp_cache VALUE CORRUPTION: wid={0} subscribed {1}[{2}] "
               "expecting __origin__={3} __args__={4} but recovered "
               "__origin__={5} __args__={6} -- a sibling's subscription value "
               "leaked through the shared _tp_cache under M:N".format(
                   wid, getattr(origin, "_name", origin), p,
                   exp_origin, exp_args,
                   getattr(recovered, "__origin__", "?"),
                   getattr(recovered, "__args__", "?")))


# Sustained cache_check blocks per worker, bounded by H.running().  The cache-
# isolation hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously mid-subscription and parked across their yield, so the scheduler
# reliably runs a sibling (at a different prec) on the shared _tp_cache before
# this fiber resumes.  A single check per fiber barely overlaps a sibling's and
# does NOT reproduce.  So each worker runs a sustained internal loop -- one
# cache_check per iteration -- until the deadline (H.running()) or INNER_CAP.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING value-integrity check in a tight loop
    bounded by H.running().  This keeps the bounded _tp_cache under concurrent
    pressure from many fibers' subscriptions and evictions, all yielding at
    strategic points."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            cache_check(H, wid, idx, state)  # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["cache_checks"])
    evictions = sum(H.state["evictions"])
    evict_pct = (100.0 * evictions / checks) if checks else 0.0
    H.log("typing._tp_cache: value-integrity checks={0} (LOAD-BEARING, all "
          "passed: recovered alias had the correct __origin__/__args__) | "
          "evictions={1} ({2:.2f}%, MEASURED bounded-lru_cache eviction rate -- "
          "id(recovered)!=id(subscribed), benign, never fails)".format(
              checks, evictions, evict_pct))

    if evictions:
        H.log("note: {0} evictions ({1:.2f}%) -- under the per-fiber param band "
              "the bounded _tp_cache legitimately evicted entries and recomputed "
              "EQUAL aliases (a correct runtime, even single-threaded, returns a "
              "NEW object with the CORRECT value).  This id-mismatch is benign "
              "eviction, NOT corruption: it is MEASURED, never failed.  The "
              "load-bearing oracle asserts only the recovered VALUE "
              "(__origin__/__args__) is right.".format(evictions, evict_pct))

    # NON-VACUITY: the cache-access hazard was actually exercised.
    H.check(checks > 0,
            "no _tp_cache checks ran -- the cache-access hazard was never "
            "exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-subscription.
    H.require_no_lost("typing._tp_cache cache access")


if __name__ == "__main__":
    harness.main(
        "p469_typing", body, setup=setup, post=post,
        default_funcs=9,
        describe="typing._tp_cache is a BOUNDED lru_cache on generic type "
                 "subscriptions (List[int], Dict[str, int], etc.).  Under M:N "
                 "many fibers share one hub -> shared _tp_cache under eviction "
                 "pressure.  LOAD-BEARING: each fiber subscribes a generic alias, "
                 "yields across the cache lookup, and MUST recover an alias with "
                 "the CORRECT __origin__/__args__ for what THIS fiber subscribed "
                 "(the VALUE is right -- holds under plain threads GIL on AND off "
                 "and single-thread; a recovered alias carrying a sibling's "
                 "type/args is the M:N cache-corruption bug -> exit 1).  MEASURED "
                 "(report-only): id(recovered)!=id(subscribed) is benign bounded-"
                 "lru_cache eviction, reported as a rate, never failed.")
