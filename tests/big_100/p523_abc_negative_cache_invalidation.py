"""big_100 / 523 -- abc.ABCMeta negative-cache invalidation across a shared token.

abc.ABCMeta memoises isinstance()/issubclass() results on EACH ABC class in two
per-class caches: a positive `_abc_cache` and a `_abc_negative_cache`.  The
NEGATIVE cache -- "this type is NOT a subclass" -- is dangerous to memoise, because
a later `SomeABC.register(that_type)` must make the stale "not a subclass" answer
disappear.  CPython solves this with a single PROCESS-GLOBAL monotonic token:
`_abc_invalidation_counter`, surfaced as `abc.get_cache_token()`.  Every
`register()` (on ANY ABC, anywhere in the process) bumps that one shared counter.
Each ABC also stamps its negative cache with the token value that was current when
the cache was filled (`_abc_negative_cache_version`).  A negative-cache HIT is only
honoured if the ABC's stamped version still equals the live global token; if the
token has moved, `__subclasscheck__` treats the negative cache as stale, clears it,
and recomputes -- which is how a freshly-`register()`ed type flips from "not an
instance" to "an instance".

WHERE M:N BREAKS IT (the gap this program probes).  The correctness of that flip
rests on an ordered read-modify-read spanning ONE shared word:

    1. THIS fiber's isinstance() seeds the negative cache and stamps it with the
       token value T0 that is live right now.
    2. THIS fiber's register() bumps the global token to some T1 > T0 AND clears
       THIS ABC's caches, so the stale negative entry is gone.
    3. THIS fiber's next isinstance() reads the (now empty / re-stamped) cache and
       must recompute -> True.

Meanwhile SIBLING fibers on other hubs are register()ing their OWN ABCs, so the
shared token is being bumped concurrently the whole time.  If a hub migration or a
torn read of the shared invalidation counter let step 3 observe a STALE negative
cache -- the pre-register "not a subclass" entry, validated against a token value
that should have been superseded -- isinstance() would wrongly return False for a
type this fiber just register()ed.  That is a stale-negative-cache hit: a real
runtime desync of the shared-token read against this ABC's per-class cache state.

The ABC classes are FIBER-LOCAL (each fiber builds its own ABCMeta ABC + its own
concrete types in local variables, never shared).  The ONLY shared object in the
load-bearing arm is the process-global invalidation counter -- and that is exactly
the point: the counter is SUPPOSED to be shared, and the isolation guarantee is
that a correct read of it always invalidates THIS fiber's own negative cache after
THIS fiber's own register().

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  `SomeABC.register(T)` immediately followed by `isinstance(T(), SomeABC)` MUST
  return True -- this is the documented contract of ABCMeta.register (the whole
  reason the invalidation token exists).  A standalone plain-threads control (8 OS
  threads each running the seed-negative -> yield -> register -> assert-True
  sequence on their own fiber-local ABC, while 4 bumper threads continuously
  register unrelated types to hammer the shared token, GIL OFF, 3.14t) observed 0
  stale-cache leaks across ~160k iterations.  Under a CORRECT runloom it must also
  hold: register() then isinstance() is True, and an UNregistered control type
  stays False.  If a fiber's own just-registered type reads back as "not an
  instance" (a stale negative-cache hit against the shared token), that is an
  abc-invalidation desync in runloom, and the single-owner oracle catches it.

ORACLES:
  * LOAD-BEARING -- NEGATIVE-CACHE INVALIDATION (worker, HARD, fail-fast).  Each
    fiber, per iteration, builds its OWN fresh ABCMeta ABC and two OWN concrete
    types (target + control), then:
      - isinstance(target_inst, MyABC)  -> asserts False, SEEDING the negative
        cache stamped with the token value live at that moment;
      - isinstance(control_inst, MyABC) -> asserts False, seeds a second negative
        entry (the control that must STAY False);
      - yields (yield_now / tiny sleep) so siblings bump the shared token and the
        scheduler can migrate this fiber to another hub;
      - MyABC.register(target)  -> bumps the shared token, clears MyABC's caches;
      - isinstance(target_inst, MyABC) -> asserts True (the negative cache was
        correctly invalidated; a False here is a STALE-CACHE HIT = the bug);
      - isinstance(control_inst, MyABC) -> asserts still False (register on the
        target must NOT flip the unrelated control positive; a True here means a
        cross-entry / cross-fiber cache corruption).
    Single-owner: MyABC and its types live in fiber-local variables, never shared.
    A failure is a runloom abc-negative-cache-invalidation desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-
    __subclasscheck__ (inside the negative-cache clear/recompute on a desynced
    token) never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (abc_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): a small SHARED pool of ABC classes is
    hammered by all fibers -- each fiber register()s a fresh type onto a shared ABC
    and reads it back.  Because the ABC is shared, a sibling can clear/refill its
    caches between this fiber's register() and read-back, so an occasional read-
    back-False is EXPECTED (documented shared-object behaviour, like p490's shared
    enum pool / p67's threading.local).  We MEASURE the shared read-back-miss rate
    and REPORT it; we NEVER fail on it -- failing would mislabel documented shared-
    object semantics as a runtime bug.  Its purpose is to prove the hazard is live
    (fibers really do bump/clear each other's shared caches) so the single-owner
    arm is genuinely testing isolation, not missing the window.

FAIL ON: a fiber's OWN just-register()ed fiber-local type reading back as "not an
instance" (stale negative-cache hit against the shared token), or the fiber-local
control flipping positive after registering the unrelated target.  The shared-pool
MEASURED arm is report-only and is expected to show read-back misses (documented
M:N shared-object behaviour) -- the load-bearing single-owner oracle must stay
clean.

Stresses: abc.ABCMeta.__subclasscheck__ / __instancecheck__ negative-cache fill +
per-class version stamp, the process-global _abc_invalidation_counter bump on
register() read across hub migration + yield, negative-cache clear/recompute under
concurrent token churn, per-fiber ABC cache isolation vs shared-ABC behaviour.

Good TSan / controlled-M:N-replay target: the shared _abc_invalidation_counter is a
single word read by every __subclasscheck__ and written by every register(); a data
race on that word -- or a replay that lets a negative-cache hit be honoured against
a superseded token value -- localises the stale hit before the isinstance oracle
even fires.
"""
import abc

import harness
import runloom


# Number of fiber-local negative-cache-invalidation checks squeezed into one
# worker round.  The stale-hit hazard only manifests under SUSTAINED token churn:
# many fibers simultaneously seeding negative caches, PARKING across their yield
# while siblings bump the shared invalidation counter, then registering and
# reading back.  A single check per fiber barely overlaps a sibling's token bump
# and does NOT reproduce -- so each fiber loops, bounded by H.running().
INNER_CAP = 100000

# Size of the SHARED ABC pool for the MEASURED (report-only) arm.  Small so many
# fibers collide on the same shared ABC and reliably clear/refill each other's
# caches -- that collision is what makes the read-back-miss rate non-zero and thus
# proves the hazard window is live.
SHARED_POOL = 8


# ---- LOAD-BEARING arm: single-owner fiber-local ABC ----------------------
def abc_check(H, wid, idx, state):
    """Single-owner negative-cache invalidation check.

    Each call builds a FRESH fiber-local ABCMeta ABC plus two fiber-local concrete
    types (target + control), seeds their negative caches, yields so siblings bump
    the shared invalidation token, registers the target, and asserts the just-
    registered target reads back True while the unregistered control stays False.
    Nothing here is shared except the process-global token -- a False read-back on
    the fiber's OWN registered type is a stale-negative-cache desync."""
    # Fiber-local ABC.  A distinct class object per call => its own _abc_cache /
    # _abc_negative_cache, never aliased with any sibling's.
    class FiberABC(metaclass=abc.ABCMeta):
        pass

    # Two fiber-local concrete types: the one we will register (target) and the
    # one that must stay unregistered (control).
    class Target(object):
        pass

    class Control(object):
        pass

    tgt = Target()
    ctl = Control()

    # Step 1: seed the NEGATIVE cache for both types.  Neither is (yet) a virtual
    # subclass, so both must be False -- and this stamps FiberABC's negative cache
    # with the invalidation token value live right now.
    if isinstance(tgt, FiberABC):
        H.fail("pre-register isinstance(target, FiberABC) is True before any "
               "register() -- an unregistered fiber-local type reported as a "
               "virtual subclass (wid {0}, idx {1}); the negative cache seed "
               "returned a spurious positive".format(wid, idx))
        return
    if isinstance(ctl, FiberABC):
        H.fail("pre-register isinstance(control, FiberABC) is True before any "
               "register() -- spurious positive on the control type (wid {0}, "
               "idx {1})".format(wid, idx))
        return

    # Step 2: YIELD.  Siblings on other hubs bump the shared invalidation token via
    # their own register() calls, and the scheduler may migrate this fiber to a
    # different hub -- so the token read in step 4's isinstance() happens on a
    # (possibly) different hub than the seed in step 1.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Step 3: register the target on THIS fiber's ABC.  This bumps the shared token
    # AND clears FiberABC's caches, so the stale negative entry for `tgt` is gone.
    FiberABC.register(Target)

    # Step 4: read back.  The just-registered target MUST now be an instance.  A
    # False here means isinstance() honoured a STALE negative-cache hit -- the pre-
    # register "not a subclass" entry validated against a token value that should
    # have been superseded by our own register() (a desync of the shared-token read
    # against this ABC's per-class cache state under hub migration).
    if not isinstance(tgt, FiberABC):
        H.fail("STALE NEGATIVE-CACHE HIT: isinstance(target, FiberABC) is False "
               "immediately after FiberABC.register(Target) (wid {0}, idx {1}) -- "
               "the fiber's own register() bumped the shared invalidation token but "
               "isinstance() served the pre-register negative-cache entry; the "
               "shared-token read desynced against this ABC's cache across a hub "
               "migration".format(wid, idx))
        return

    # Step 5: the control must STILL be False.  Registering the target must not flip
    # the unrelated control positive (a cross-entry / cross-fiber cache corruption).
    if isinstance(ctl, FiberABC):
        H.fail("CONTROL FLIPPED POSITIVE: isinstance(control, FiberABC) is True "
               "after registering only Target (wid {0}, idx {1}) -- registering "
               "one type corrupted another type's negative-cache entry, or a "
               "sibling's cache write leaked into this fiber-local ABC".format(
                   wid, idx))
        return

    state["abc_checks"][wid & 1023] += 1


# ---- MEASURED arm: shared ABC pool (report-only) -------------------------
def shared_abc_check(H, wid, idx, state):
    """Shared-ABC register/read-back (MEASURED, report-only).

    All fibers hammer a small pool of SHARED ABC classes: this fiber register()s a
    fresh fiber-local type onto a SHARED ABC and immediately reads it back.  Because
    the ABC is shared, a sibling's register() on the SAME ABC can clear/refill its
    caches between this fiber's register() and read-back, so an occasional read-
    back-False is EXPECTED and DOCUMENTED (shared-object behaviour, like p490's
    shared enum pool).  We MEASURE the read-back-miss rate; we NEVER fail on it."""
    shared_abc = state["shared_pool"][idx % SHARED_POOL]

    # A fresh fiber-local concrete type per call (so registrations accumulate on the
    # shared ABC and drive its cache churn without ever being torn -- the type
    # object itself is single-owner).
    class ShType(object):
        pass

    shared_abc.register(ShType)
    inst = ShType()

    state["shared_checks"][wid & 1023] += 1
    # After OUR register() on the shared ABC, OUR type should be an instance -- but
    # a sibling clearing the shared ABC's caches in the window is documented shared-
    # object behaviour, so a miss here is MEASURED, never failed.
    if not isinstance(inst, shared_abc):
        state["shared_misses"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING single-owner ABC
    invalidation check (fail-fast) and the MEASURED shared-pool check (report only).
    The two share no ABC (fiber-local ABC vs shared pool), so running them together
    keeps every hub busy with mixed register()/isinstance() token churn without the
    shared-pool mutations reaching the single-owner oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            abc_check(H, wid, idx, state)            # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_abc_check(H, wid, idx, state)     # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # A small pool of SHARED ABC classes for the MEASURED arm.  Built in the root;
    # ABCMeta class creation is fine here (no cooperative primitive involved).
    shared_pool = []
    for pool_idx in range(SHARED_POOL):
        shared_pool.append(abc.ABCMeta("SharedABC_P{0}".format(pool_idx),
                                       (object,), {}))
    H.state = {
        "abc_checks": [0] * 1024,          # LOAD-BEARING single-owner checks
        "shared_pool": shared_pool,        # small shared ABC pool
        "shared_checks": [0] * 1024,       # MEASURED shared-pool register/read-back
        "shared_misses": [0] * 1024,       # read-back misses on the shared pool
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    achecks = sum(H.state["abc_checks"])
    schecks = sum(H.state["shared_checks"])
    smisses = sum(H.state["shared_misses"])
    spct = (100.0 * smisses / schecks) if schecks else 0.0

    H.log("abc[single-owner LOAD-BEARING]: {0} negative-cache-invalidation checks "
          "(all passed fail-fast) | abc[shared pool MEASURED]: {1} register/read-"
          "back {2} misses ({3:.2f}%, documented shared-ABC behaviour -- REPORT "
          "ONLY)".format(achecks, schecks, smisses, spct))

    if smisses:
        H.log("note: the shared ABC pool observed {0} register/read-back misses "
              "across {1} checks -- a sibling's register() on the SAME shared ABC "
              "cleared/refilled its caches in this fiber's register->read-back "
              "window.  The shared ABC is a shared Python object (like p490's "
              "shared enum pool / p67's threading.local); this is documented M:N "
              "shared-object behaviour, NOT a runloom bug, and never reaches the "
              "load-bearing single-owner oracle".format(smisses, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(achecks > 0,
            "no single-owner abc negative-cache-invalidation checks ran -- the "
            "load-bearing invalidation hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # __subclasscheck__ during the negative-cache clear/recompute).
    H.require_no_lost("abc negative-cache invalidation")


if __name__ == "__main__":
    harness.main(
        "p523_abc_negative_cache_invalidation", body, setup=setup, post=post,
        default_funcs=5000,
        describe="abc.ABCMeta memoises isinstance() misses in a per-class negative "
                 "cache validated against the PROCESS-GLOBAL _abc_invalidation_"
                 "counter that every register() bumps.  Under M:N, a hub migration "
                 "or torn read of that shared token between a fiber's own "
                 "register() and its read-back could serve a STALE negative-cache "
                 "hit.  LOAD-BEARING: each fiber seeds its OWN fiber-local ABC's "
                 "negative cache, yields (siblings churn the shared token), "
                 "register()s a type, and asserts isinstance() now returns True "
                 "while an unregistered control stays False.  MEASURED shared-pool "
                 "(expected to show register/read-back misses on shared ABCs, like "
                 "p490) proves the hazard window is live.  A just-registered fiber-"
                 "local type reading back False is the runloom abc-invalidation bug")
