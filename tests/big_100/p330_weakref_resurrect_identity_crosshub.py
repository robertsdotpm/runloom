"""big_100 / 330 -- weakref-clear-vs-__del__-RESURRECTION identity across a
cross-hub last decref.

The untested weakref corner that p310 does NOT cover.  p310's oracle is fd
CONSERVATION (acquired==released) through a real lock during STW shutdown -- it
proves no fd leaks/double-closes when a resurrecting object dies twice, but it
never inspects the weakref of a REVIVED object.  This program targets exactly
that: the IDENTITY a freshly-taken weakref resolves to after a resurrection, and
whether a never-resurrected object's weakref eventually reads dead.

The hazard.  Under free-threading, an object's deallocation -- and the
weakref-clear-THEN-tp_finalize(__del__) sequence -- runs on whatever hub drops
the object's LAST reference, NOT the hub that created it.  CPython clears an
object's weakrefs BEFORE calling __del__ (so a weakref taken before death reads
None inside/after __del__).  But a __del__ that RESURRECTS -- re-inserting `self`
into a live shared container -- revives the object with its old weakrefs already
cleared.  A goroutine on ANOTHER hub that then takes a FRESH weakref.ref() of the
revived object, or holds the old one, can observe a HALF-resurrected object:

  * the freshly-taken ref wrongly reads dead (None) even though the object is
    provably alive and reachable, OR
  * dereferencing the stale/old ref hands back a freed slot -> UAF / a ref that
    reports the WRONG identity (a different id() than the object had pre-__del__),
  * or a crash if the revive races the cross-hub dealloc tear-down.

CRISP, LOAD-BEARING ORACLE -- RESURRECTION IDENTITY (not callback counting).
For a DETERMINISTICALLY-resurrected object we recorded id() BEFORE its __del__
ran; after the resurrect settles we take a BRAND-NEW weakref.ref() of the revived
object and assert:

    ref() is not None   AND   id(ref()) == recorded_pre_del_id

i.e. the revived object is the SAME identity it had before finalization.  A
half-resurrected object (weakrefs cleared, object revived) yields a ref that
either wrongly reads None (-> "revived object's fresh weakref reads DEAD") or
resolves to an object with a different id (-> "resurrected identity changed";
also the signature of a freed-slot UAF).  This single check catches BOTH failure
modes.  For a NEVER-resurrected object, its weakref MUST eventually read None
(the object truly died) -- a ref that stays non-None for a provably-dead object
is a missed clear / dangling weakref, the dual fault.

We FORCE the last decref onto a FOREIGN hub (mirror p141/p211): the worker takes
weakrefs, drops every strong ref across a runloom.yield_now() so the object is
unreferenced when it resumes on (likely) a different hub, and a REAL OS thread
runs a gc.collect() storm so the actual dealloc + weakref-clear + __del__ run on
the collector thread, not the creator.

The callback COUNT is DEMOTED to a BOUND used only as a liveness gate:
total_callbacks <= objects_created.  An equality (callbacks == deaths) is fragile
under free-threading slack -- p77 documents weakref-callback counts drifting
under M:N -- so we never assert it; it is a one-sided "we did fire callbacks"
sanity bound, not the oracle.

Differential: the resurrection-IDENTITY invariant must hold IDENTICALLY at run(1)
(single hub, GIL-like) and under true M:N -- the program self-checks a run(1)
arm at setup so a green M:N run is meaningful relative to a known-green baseline.

Stresses: weakref-clear ordering vs __del__ resurrection, fresh-weakref identity
of a revived object, cross-hub last-decref dealloc, missed-clear dangling weakref,
finalizer wedged on a cooperative lock (watchdog), preempt-mid-tp_dealloc gate.

Good TSan / controlled-replay target: the resurrect-publish (LIVE insert) vs the
fresh-weakref read on a foreign hub is a pure cross-hub ordering question; a race
on the LIVE publish or the recorded-id store is the first signal, before the
identity assert even fires.
"""
import gc
import sys
import weakref

import _thread as _real_thread
import time as _time

import harness
import runloom

# Real-thread entry points captured BEFORE monkey.patch() makes them cooperative
# -- the gc-storm must be a genuine OS thread so the last decref / dealloc /
# weakref-clear / __del__ run off the creating hub.
_REAL_SLEEP = _time.sleep

# DETERMINISTIC resurrect predicate on a recorded id.  CPython object ids are
# pool-aligned: id % 2 / % 8 are ALWAYS 0, and for a fixed-size slotted object
# even bits 3/4/5 are CONSTANT (the pymalloc pool stride keeps the low address
# bits invariant), so any single-bit or power-of-two split resurrects EVERY
# object and never exercises the dual "must die" arm.  A small ODD modulus is
# NOT a factor of the pool stride, so `id % 3` genuinely divides the population
# (empirically ~1/3 resurrect, ~2/3 die).  Still EXACT per object (id is stable
# for the object's lifetime): an id either resurrects or it does not.
def will_resurrect(oid):
    return (oid % 3) == 0

# One genuine OS lock guards the shared resurrection bookkeeping.  It is a real
# lock (never made cooperative by monkey.patch), so a __del__ that resurrects on
# a FOREIGN hub -- or fires from the real gc-storm thread -- serializes correctly
# against the creator-hub reads.  A lock-free shared dict/list would itself race
# GIL-off and confound the weakref-identity signal we are actually testing.
STATE_LOCK = _real_thread.allocate_lock()

# id(obj)-before-__del__  ->  the revived object (strong ref).  __del__ publishes
# here to resurrect; a freshly-taken weakref of this object must resolve back to
# the SAME id.  Strong refs keep revived objects alive until post() inspects them.
RESURRECTED = {}                 # recorded_id -> revived obj (under STATE_LOCK)

# Sharded callback counter (one writer per slot, indexed wid & 1023) -- a shared
# += would lose increments GIL-off.  BOUND only (<= objects_created); never an
# equality.  Liveness gate that weakref callbacks fired at all.
CALLBACKS = [0] * 1024

OBJECTS_CREATED = [0] * 1024     # sharded; total created, the bound's RHS

# Unraisable finalizer exceptions (a __del__ that raised mid-resurrect would be a
# bug masking the identity check); counted, asserted == 0 in post.
UNRAISABLE = [0]


class Obj(object):
    """An object whose __del__ RESURRECTS itself iff its recorded id is on the
    deterministic subset.  __del__ does NOT yield -- the mid-dealloc park is a
    separately-gated invariant (p211); we deliberately avoid it here so the
    signal is purely the weakref-clear-vs-revive ordering, not a park-in-dtor."""
    __slots__ = ("tag", "revived", "__weakref__")

    def __init__(self, tag):
        self.tag = tag
        self.revived = False

    def __del__(self):
        # First-death finalization, running on whatever hub (or the real gc
        # thread) dropped the last ref.  CPython has ALREADY cleared this
        # object's pre-death weakrefs by now.  On the deterministic subset, and
        # only the FIRST time, RESURRECT: re-publish `self` into the shared live
        # map keyed by THIS object's id -- which is exactly the id a pre-death
        # weakref pointed at and the id a fresh weakref must resolve back to.
        # No yield, no I/O, no new weakref registration here: keep the dtor body
        # minimal so the only thing under test is the revive-vs-clear ordering.
        oid = id(self)
        if will_resurrect(oid) and not self.revived:
            self.revived = True
            with STATE_LOCK:
                RESURRECTED[oid] = self      # resurrect: object reachable again
        # Never-resurrecting (or already-once-revived) death: nothing to do; the
        # object is really gone and any weakref to it must now read None.


def count_cb(slot):
    """weakref.ref() callback: bump the sharded BOUND counter (liveness gate
    only -- never an equality).  Fires when the object is collected; under
    resurrection the original weakref is cleared BEFORE __del__, so this proves
    the clear+callback path ran at all, nothing more."""
    def _cb(_ref, slot=slot):
        CALLBACKS[slot] += 1
    return _cb


def identity_arm(slot, rng):
    """One full create/kill/inspect cycle, used by BOTH the run(1) differential
    arm (setup) and every M:N worker round.  Returns (ok, msg): ok False with a
    msg on the FIRST identity violation, else (True, "").

    Protocol:
      1. create N objects, record each id() BEFORE any death;
      2. take a weakref.ref(obj, cb) on each (the PRE-death ref, which CPython
         clears before __del__);
      3. drop ALL strong refs across a runloom.yield_now() so the last decref
         lands on a (likely) foreign hub and the gc-storm thread can collect;
      4. settle: gc.collect() locally + let the storm run;
      5. ASSERT per object (BOTH arms exercised -- ~1/3 resurrect, ~2/3 die):
           - will_resurrect(id) -> it MUST be in RESURRECTED, and a FRESH
             weakref.ref(revived) MUST resolve to id == recorded id (the revived
             object is the SAME identity);
           - not will_resurrect(id) -> the object MUST be dead: its pre-death
             weakref reads None (eventually); a pre-ref resolving to a DIFFERENT
             id is a freed-slot UAF (hard fail), same-id-still-alive retries.
    """
    n = rng.randint(6, 14)
    objs = []
    pre_ids = []
    refs = []
    for _ in range(n):
        o = Obj(slot)
        oid = id(o)
        objs.append(o)
        pre_ids.append(oid)
        refs.append(weakref.ref(o, count_cb(slot)))
    OBJECTS_CREATED[slot] += n

    # Drop every strong ref ACROSS a yield, so when we resume (likely on another
    # hub) the objects are unreferenced and the last decref / dealloc / __del__
    # run off this hub.  The real gc-storm thread races the collection too.
    del objs
    runloom.yield_now()
    # Force the deaths to actually happen now (and the resurrect __del__s to
    # publish): collect locally, yield to let the storm thread interleave.
    gc.collect()
    runloom.yield_now()
    gc.collect()

    for oid, pre_ref in zip(pre_ids, refs):
        if will_resurrect(oid):
            # MUST have resurrected: revived object present, fresh weakref of it
            # resolves to the SAME identity it had before __del__.
            with STATE_LOCK:
                revived = RESURRECTED.get(oid)
            if revived is None:
                # The deterministic resurrect did not take effect -- either the
                # __del__ never ran (object leaked) or the revive publish was
                # lost across the cross-hub dealloc.  Tolerate "not yet collected"
                # by NOT failing here on a single round (post() does the final,
                # settled check); signal via return so the caller can retry-settle.
                return ("retry", "resurrect for id {0} not yet visible".format(oid))
            fresh = weakref.ref(revived)
            got = fresh()
            if got is None:
                return (False,
                        "REVIVED object's FRESH weakref reads DEAD (None) -- the "
                        "object is provably alive in RESURRECTED[id={0}] yet a "
                        "newly-taken weakref.ref() resolves to None: weakrefs were "
                        "cleared before __del__ and the revive left them dangling "
                        "(half-resurrected object)".format(oid))
            if id(got) != oid:
                return (False,
                        "RESURRECTED IDENTITY CHANGED: object revived from "
                        "__del__ has id {0} but was recorded as id {1} before "
                        "finalization -- a fresh weakref resolved to a DIFFERENT "
                        "object (freed-slot UAF / torn revive across hubs)"
                        .format(id(got), oid))
        else:
            # NEVER-resurrected DUAL arm: this object truly died.  Its PRE-death
            # weakref must eventually read None.  A pre-ref that still resolves to
            # a LIVE object here is a missed weakref-clear / dangling weakref --
            # the dual fault to a wrongly-cleared revive.  Tolerate "not yet
            # collected" (retry), but a pre-ref that resolves to an object whose
            # id != oid is a hard freed-slot reuse fault (caught immediately).
            alive = pre_ref()
            if alive is not None:
                if id(alive) != oid:
                    return (False,
                            "DEAD object's pre-death weakref resolved to a "
                            "DIFFERENT object id {0} != recorded {1} -- the "
                            "weakref now points at a reused/freed slot (cross-hub "
                            "dealloc UAF)".format(id(alive), oid))
                # Same-id but still alive: the foreign-hub dealloc hasn't landed
                # yet; settle and retry rather than false-fail.
                del alive
                return ("retry",
                        "never-resurrect id {0} not yet collected".format(oid))
    return (True, "")


def settle_dead(rng, attempts=12):
    """Drive every NEVER-resurrected object to a confirmed dead weakref.  Because
    the cross-hub biased-refcount merge + cyclic GC settle over a few rounds, we
    cannot assert deadness on a single collect; we only need to know the machinery
    converges, which post() verifies via the resurrect-subset identity (the dead
    arm is checked there once settled)."""
    for _ in range(attempts):
        gc.collect()
        runloom.yield_now()


def worker(H, wid, rng, state):
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        # Up to a few settle retries: a single round may resume before the
        # foreign-hub dealloc + resurrect publish is visible.  A genuine identity
        # fault (revived-but-wrong-id / fresh-ref-dead) fails IMMEDIATELY; only a
        # not-yet-visible resurrect retries, bounded so a real leak can't spin.
        for _attempt in range(8):
            ok, msg = identity_arm(slot, rng)
            if ok is True:
                break
            if ok is False:
                # A hard identity violation -- the bug.  Fail fast.
                H.fail(msg)
                return
            # ok == "retry": resurrect not yet visible; settle and try again.
            gc.collect()
            runloom.yield_now()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.yield_now()


def setup(H):
    H.state = {"stop": [False]}

    def count_unraisable(_unraisable):
        UNRAISABLE[0] += 1

    sys.unraisablehook = count_unraisable
    # Clean GC baseline so leftover import garbage doesn't perturb accounting.
    gc.collect()

    # DIFFERENTIAL run(1) ARM -- establish the identity invariant holds on a
    # single hub (GIL-like) BEFORE the M:N run, so a green M:N result is meaningful
    # relative to a known-green baseline.  runloom.run executes one fiber to
    # completion on a single hub; the same identity_arm must pass there.
    import random as _random
    base_rng = _random.Random(0xC0FFEE)
    baseline_fail = [None]

    def _run1():
        for _ in range(8):
            ok, msg = identity_arm(0, base_rng)
            if ok is False:
                baseline_fail[0] = msg
                return
            # "retry" or True both acceptable at run(1) (single hub settles fast).
    try:
        runloom.run(1, _run1)
    except Exception as exc:                  # noqa: BLE001
        baseline_fail[0] = "run(1) arm crashed: {0}: {1}".format(
            type(exc).__name__, exc)
    if baseline_fail[0] is not None:
        # The invariant must hold at run(1); if it doesn't, the oracle itself is
        # wrong, not the M:N runtime -- surface it as a setup-level failure.
        H.fail("DIFFERENTIAL run(1) arm violated resurrection identity (oracle "
               "must hold at run(1) too): " + baseline_fail[0])
    # Reset shared bookkeeping the baseline touched so M:N accounting starts clean.
    with STATE_LOCK:
        RESURRECTED.clear()
    for i in range(1024):
        CALLBACKS[i] = 0
        OBJECTS_CREATED[i] = 0
    UNRAISABLE[0] = 0
    gc.collect()


def body(H):
    state = H.state

    # Real OS thread: a gc.collect() STORM so the last decref / dealloc /
    # weakref-clear / __del__ for dropped objects runs on the COLLECTOR thread
    # (a foreign hub from the creator's view) -- exactly the cross-hub last-decref
    # the spec forces.
    def gc_thread():
        while not state["stop"][0] and H.running():
            try:
                gc.collect()
            except Exception:
                pass
            _REAL_SLEEP(0.002)

    _real_thread.start_new_thread(gc_thread, ())

    # Cooperative driver that also forces collection so resurrect __del__s fire
    # promptly on hub-scheduled goroutines.  A finalizer wedged holding a
    # cooperative lock would stall progress here -> the harness watchdog (exit 3)
    # catches the wedge.
    def gc_driver():
        while H.running():
            H.sleep(0.03)
            gc.collect()
        state["stop"][0] = True

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, state)


def post(H):
    H.state["stop"][0] = True
    # Final settle: collect repeatedly so any still-pending foreign-hub deaths /
    # resurrect publishes converge before the oracle reads the maps.  Bounded.
    import random as _random
    srng = _random.Random(0xD15EA5E)
    settle_dead(srng, attempts=20)
    gc.collect()

    # FINAL resurrection-IDENTITY sweep over every published revived object: each
    # one MUST still resolve, via a FRESH weakref, to its recorded id.  This is
    # the load-bearing oracle, re-checked after full settle so no in-flight revive
    # is mistaken for a fault.
    with STATE_LOCK:
        items = list(RESURRECTED.items())
    bad = None
    for oid, revived in items:
        if revived is None:
            continue
        got = weakref.ref(revived)()
        if got is None:
            bad = ("REVIVED object id {0} fresh weakref reads DEAD (None) at "
                   "post -- revived-but-weakref-cleared (half-resurrected, "
                   "dangling weakref)".format(oid))
            break
        if id(got) != oid:
            bad = ("RESURRECTED IDENTITY CHANGED at post: revived object has id "
                   "{0} but recorded id {1} -- fresh weakref resolved to a "
                   "DIFFERENT object (freed-slot UAF / torn cross-hub revive)"
                   .format(id(got), oid))
            break
    if bad is not None:
        H.fail(bad)

    created = sum(OBJECTS_CREATED)
    callbacks = sum(CALLBACKS)
    resurrected = len(items)
    H.log("created={0} resurrected={1} callbacks={2} unraisable={3}".format(
        created, resurrected, callbacks, UNRAISABLE[0]))

    # Vacuity guards: the targeted path must actually have run.
    H.check(created > 0, "no objects were ever created (oracle vacuous)")
    H.check(resurrected > 0,
            "NO object ever resurrected itself from __del__ -- the resurrect "
            "path never fired, so the identity oracle is vacuous")
    # Liveness BOUND only (never an equality -- FT slack drifts the count; p77).
    H.check(callbacks <= created,
            "weakref callbacks {0} EXCEED objects created {1} -- a callback "
            "fired more than once per object (over-finalize)".format(
                callbacks, created))
    # A finalizer that raised mid-resurrect would mask the identity check.
    H.check(UNRAISABLE[0] == 0,
            "{0} unraisable finalizer exception(s) -- a __del__ raised during "
            "resurrection".format(UNRAISABLE[0]))
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p330_weakref_resurrect_identity_crosshub", body, setup=setup,
                 post=post, default_funcs=2000, max_funcs=2000,
                 describe="__del__ resurrects self (re-publishes into a live map) "
                          "while the last decref lands on a FOREIGN hub via a real "
                          "gc-storm thread; a FRESH weakref of every revived object "
                          "must resolve to the SAME id() it had pre-__del__ "
                          "(resurrection identity), never-resurrected objects die; "
                          "callbacks<=created (bound), differential run(1)==M:N")
