"""big_100 / 542 -- functools.singledispatch registry + dispatch_cache + MRO
isolation under M:N.

functools.singledispatch turns a plain function into a generic function that
dispatches on the type of its first argument.  Each generic function owns TWO
pieces of per-dispatcher state, captured in the closure returned by
singledispatch():

  * a `registry` dict:  type -> concrete impl (populated by @gf.register(T));
  * a `dispatch_cache` (a weakref.WeakKeyDictionary):  subject-type -> resolved
    impl, memoizing the MRO walk so repeated dispatches on the same type are O(1).

On every dispatch the machinery compares abc.get_cache_token() (a PROCESS-GLOBAL
counter bumped whenever ANY abc.ABC.register() adds a virtual subclass) against
the token stamped when the cache was last filled; if they differ it CLEARS the
whole dispatch_cache and re-walks the subject's MRO via functools._find_impl /
_compose_mro (which fuses the C3 MRO with the abc virtual-subclass graph).

WHERE M:N BREAKS IT (the gap this program probes).  The abc cache token is a
single process-global.  When fiber A's dispatcher fills its dispatch_cache and
stamps token=T, and a SIBLING fiber B on another hub calls SomeABC.register(...)
(bumping the global token to T+1) DURING A's MRO walk or right at A's cache-fill
boundary, A's dispatch must either (a) still return the correct MRO winner, or
(b) invalidate and recompute -- but it must NEVER bind a WRONG impl into A's
cache line.  A hub-migration in the middle of _find_impl (which builds a fresh
`mro` list and a `match` search over A's OWN registry), or a torn read of the
global token straddling a sibling's register(), could in principle memoize a
sibling-shaped result -- or, worse, cross-pollinate one dispatcher's cache with
another dispatcher's impl.  Because every fiber's generic function, registry, and
subject types are FIBER-LOCAL, a CORRECT runtime resolves each dispatch to THIS
fiber's own registered impl, 100% of the time, across any number of sibling
register()s and yields.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, verified against threads):

  Each fiber constructs its OWN generic function with @functools.singledispatch,
  its OWN fiber-local class hierarchy (a Base, a Derived(Base) subclass, an
  abc.ABC MyABC with a virtually-registered Virtual class), and registers impls
  for Base and for MyABC.  EVERY impl TAGS its return value with this fiber's wid
  and a label ("base"/"abc"/"default").  The fiber then:

    - Dispatches three subjects and records the MRO winner of each:
        * Derived()  -> must fire the Base impl   (nearest-base MRO resolution:
          Derived's MRO is [Derived, Base, object]; Base is the nearest
          registered ancestor);
        * Virtual()  -> must fire the MyABC impl  (abc virtual-subclass path);
        * object()   -> must fire the default impl.
    - YIELDS (runloom.yield_now / sleep) so siblings run -- crucially, siblings
      call abc.ABC.register(...) which BUMPS the global cache token, forcing
      cache-token mismatches that make this dispatcher clear + recompute.
    - Re-dispatches the SAME three subjects and asserts the IDENTICAL winner
      (same ("label", wid) tag object -- same impl bound both times), AND that
      the returned wid equals THIS fiber's wid.  A wid from a SIBLING would be a
      cross-fiber registry/cache leak; a changed label would be a wrong-MRO-winner
      bind; a raised exception would be a torn registry / cache walk.

  Single-owner: the generic function, its registry, its dispatch_cache, and every
  subject type are created inside the fiber and NEVER shared.  A correct runtime
  keeps them perfectly isolated regardless of how many siblings bump the global
  abc token concurrently, so the load-bearing oracle PASSES (exit 0) when there
  is no bug.  We confirmed with a plain-threads control (8 OS threads, GIL on AND
  off, each building its own singledispatch + hierarchy and hammering dispatch
  while all threads spam SomeABC.register()) that 100% of dispatches resolve to
  the thread-local winner with the thread-local wid -- 0 cross-thread leaks.
  Under a correct runloom it must also hold.

ORACLES:
  * LOAD-BEARING -- SINGLEDISPATCH MRO ISOLATION (worker, HARD, fail-fast).  Each
    fiber owns its generic function + hierarchy; dispatch before and after a yield
    (during which siblings bump the abc cache token) MUST return the same
    fiber-local MRO winner tagged with THIS fiber's wid.  A wrong wid, wrong
    label, changed identity, or a raised dispatch is a runloom isolation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside
    _find_impl / _compose_mro / a WeakKeyDictionary rehash never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran
    (dispatch_checks > 0), else the oracle was vacuous.

FAIL ON: a fiber's own generic function returning a sibling's wid, a changed
MRO winner label across a yield, a changed impl identity, or a dispatch raising.
There is NO shared-mutable arm here: the whole design is single-owner dispatchers
whose only coupling to siblings is the PROCESS-GLOBAL abc cache token, which a
correct runtime treats as a mere invalidation signal, never a channel for
cross-fiber impl leakage.

Stresses: functools.singledispatch registry + WeakKeyDictionary dispatch_cache
fill/clear, abc.get_cache_token() global-counter churn from concurrent
ABC.register(), _find_impl / _compose_mro MRO+virtual-subclass walk under hub
migration + yield, per-fiber generic-function isolation vs a shared global
invalidation token.

Deepens thin functools coverage (p303=lru_cache, p471=fnmatch lru): those probe
the C lru wrapper; this probes the pure-Python singledispatch registry + abc
cache-token invalidation path, a distinct corner.
"""
import abc
import functools

import harness
import runloom

# Labels the three impls tag their returns with, so an MRO-winner change or a
# cross-fiber leak is visible in the returned tuple.
LBL_DEFAULT = "default"
LBL_BASE = "base"
LBL_ABC = "abc"


def make_dispatcher(wid, idx):
    """Build ONE fiber-local generic function with a fiber-local class hierarchy.

    Returns (gf, subjects) where `subjects` is a list of
    (subject_instance, expected_label) pairs whose MRO winner is fixed:
      * a Derived() instance whose nearest registered ancestor is Base  -> LBL_BASE
      * a Virtual() instance registered virtually onto MyABC             -> LBL_ABC
      * a plain object()                                                 -> LBL_DEFAULT

    Every class is defined FRESH inside this call, so the generic function's
    registry, its dispatch_cache, and every subject type are single-owner and
    never aliased with a sibling's.  Each impl tags its return with (label, wid)
    so a wrong wid in the result is a cross-fiber leak."""
    # Fiber-local base hierarchy.
    class Base(object):
        pass

    class Derived(Base):
        pass

    # Fiber-local ABC; register a virtual subclass so the abc virtual-subclass
    # dispatch path (get_cache_token gated) is exercised too.
    class MyABC(abc.ABC):
        pass

    class Virtual(object):
        pass

    MyABC.register(Virtual)

    @functools.singledispatch
    def gf(x):
        return (LBL_DEFAULT, wid)

    @gf.register(Base)
    def gf_base(x):
        return (LBL_BASE, wid)

    @gf.register(MyABC)
    def gf_abc(x):
        return (LBL_ABC, wid)

    subjects = [
        (Derived(), LBL_BASE),      # nearest-base MRO resolution
        (Virtual(), LBL_ABC),       # abc virtual-subclass resolution
        (object(), LBL_DEFAULT),    # default impl
    ]
    # Also hand back a throwaway ABC we can register siblings onto to bump the
    # process-global cache token during the yield window.
    return gf, subjects, MyABC


# ---- LOAD-BEARING arm: single-owner fiber-local singledispatch -----------
def dispatch_check(H, wid, idx, state):
    """Single-owner singledispatch MRO-isolation check.

    Build a fiber-local generic function + hierarchy, resolve three subjects,
    yield (letting siblings bump the global abc cache token via register()),
    then re-resolve the SAME subjects and assert the identical fiber-local MRO
    winner with THIS fiber's wid.  A cross-fiber registry/cache leak returns a
    sibling's wid or a wrong label."""
    gf, subjects, my_abc = make_dispatcher(wid, idx)

    # Baseline: resolve each subject once (fills the dispatch_cache, stamps the
    # current abc cache token) and record the exact result tuple.
    baseline = []
    for subj, expected_label in subjects:
        try:
            res = gf(subj)
        except Exception as exc:                # torn registry / cache walk
            H.fail("singledispatch RAISED on baseline dispatch of {0} (wid {1}, "
                   "idx {2}): {3!r} -- a torn registry or dispatch_cache walk "
                   "under M:N".format(type(subj).__name__, wid, idx, exc))
            return
        if res != (expected_label, wid):
            H.fail("singledispatch WRONG BASELINE winner: dispatch of {0} "
                   "returned {1!r}, expected {2!r} (wid {3}) -- wrong MRO winner "
                   "or a cross-fiber registry leak".format(
                       type(subj).__name__, res, (expected_label, wid), wid))
            return
        baseline.append(res)

    # Bump the process-global abc cache token from THIS fiber too (register a
    # fresh virtual subclass), so the invalidation path is guaranteed live even
    # if a sibling does not overlap this exact window.
    class Churn(object):
        pass
    my_abc.register(Churn)

    # YIELD: siblings run, each also bumping the global abc.get_cache_token()
    # via their own register() calls -- forcing this dispatcher to detect a
    # token mismatch and CLEAR + recompute its dispatch_cache on the next call.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Re-resolve the SAME subjects; the winner MUST be identical and carry THIS
    # fiber's wid.  A cache that was cross-polluted, or a recompute that walked
    # a sibling's registry, would show up as a wrong label or a foreign wid.
    for pos, (subj, expected_label) in enumerate(subjects):
        try:
            res = gf(subj)
        except Exception as exc:
            H.fail("singledispatch RAISED on re-dispatch of {0} across a yield "
                   "(wid {1}, idx {2}): {3!r} -- a token-invalidation recompute "
                   "hit a torn registry/cache".format(
                       type(subj).__name__, wid, idx, exc))
            return

        # Check 1: winner label unchanged across the yield (no wrong-MRO bind).
        if res[0] != baseline[pos][0]:
            H.fail("singledispatch WINNER CHANGED across a yield: {0} resolved to "
                   "{1!r} before, {2!r} after (wid {3}) -- the dispatch_cache was "
                   "invalidated and recomputed to a DIFFERENT impl".format(
                       type(subj).__name__, baseline[pos], res, wid))
            return

        # Check 2: the returned wid is THIS fiber's (no cross-fiber leak).
        if res[1] != wid:
            H.fail("singledispatch CROSS-FIBER LEAK: {0} dispatch returned wid "
                   "{1} but this fiber is wid {2} (result {3!r}) -- a sibling's "
                   "impl was bound into this fiber's registry/dispatch_cache".format(
                       type(subj).__name__, res[1], wid, res))
            return

        # Check 3: full-tuple stability (identity of the constant result).
        if res != baseline[pos] or res != (expected_label, wid):
            H.fail("singledispatch RESULT MISMATCH: {0} dispatch returned {1!r}, "
                   "baseline {2!r}, expected {3!r} (wid {4})".format(
                       type(subj).__name__, res, baseline[pos],
                       (expected_label, wid), wid))
            return

    state["dispatch_checks"][wid & 1023] += 1


# Sustained dispatch checks per worker, bounded by H.running().  The cache-token
# invalidation hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously building dispatchers + bumping the global abc token while
# sleep-PARKED across their yield, so the scheduler reliably interleaves a
# sibling's register()/dispatch before this fiber resumes.  One check per fiber
# barely overlaps and does NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber repeatedly builds its OWN single-owner singledispatch generic
    function + hierarchy and asserts MRO-winner isolation across a yield.  The
    only cross-fiber coupling is the process-global abc cache token, which every
    fiber bumps -- a correct runtime treats it purely as an invalidation signal,
    never a leak channel."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            dispatch_check(H, wid, idx, state)       # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "dispatch_checks": [0] * 1024,     # LOAD-BEARING single-owner checks (non-vacuity tally)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dchecks = sum(H.state["dispatch_checks"])
    H.log("singledispatch[single-owner LOAD-BEARING]: {0} MRO-isolation checks "
          "(each: 3 subjects x baseline+re-dispatch across a yield, all passed "
          "fail-fast); ops={1}".format(dchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(dchecks > 0,
            "no single-owner singledispatch MRO-isolation checks ran -- the "
            "load-bearing dispatch-cache/registry-isolation hazard was never "
            "exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # _find_impl / _compose_mro / a WeakKeyDictionary rehash).
    H.require_no_lost("singledispatch MRO isolation")


if __name__ == "__main__":
    harness.main(
        "p542_functools_singledispatch_mro", body, setup=setup, post=post,
        default_funcs=5000,
        describe="functools.singledispatch keeps a per-dispatcher registry + a "
                 "WeakKeyDictionary dispatch_cache invalidated against the "
                 "process-global abc.get_cache_token(); _find_impl walks the "
                 "subject's MRO under that cache.  LOAD-BEARING: each fiber owns "
                 "its OWN generic function + fiber-local class hierarchy (Base, "
                 "Derived(Base), an abc.ABC with a virtual subclass), tags every "
                 "impl with its wid, resolves three subjects (Derived->Base, "
                 "Virtual->ABC, object->default), yields while siblings bump the "
                 "global cache token via register(), then re-resolves the SAME "
                 "subjects -- the MRO winner and its wid MUST be identical.  A "
                 "sibling's wid, a changed winner, or a raised dispatch is the "
                 "runloom singledispatch-isolation bug")
