"""big_100 / 544 -- weakref.finalize fires EXACTLY ONCE under M:N (conservation).

weakref.finalize registers a callback that runs when its referent is garbage
collected -- once, and only once.  The machinery is process-GLOBAL and shared:
every finalize() insertion mutates the class-level ``finalize._registry`` dict and
draws an ordering index from a single shared ``itertools.count`` (finalize._index_iter);
the fire path pops ``self`` from that same shared dict and invokes the stored
callback -- and the fire is triggered by whichever hub happens to run the referent's
last decref, NOT necessarily the hub that registered it.  With the GIL off and tens
of thousands of fibers registering + dropping fiber-local objects across 8 hubs, the
exactly-once contract rides on:

  * ``finalize._registry[self] = info`` -- a plain-dict setitem on a SHARED dict
    hammered concurrently by every registering fiber.  A dropped/torn insertion
    means the finalizer is never recorded -> it NEVER fires (a dropped unit);
  * ``next(finalize._index_iter)`` -- a shared itertools.count RMW; an aliased index
    is benign for correctness (the index only orders atexit) but is the same shared-C
    counter the hazard note names, exercised here under real contention;
  * ``finalize._registry.pop(self, None)`` at fire time -- concurrent dict pop from
    an arbitrary hub; a torn pop could drop the callback (never fires) or -- in a
    buggy runtime -- route one referent's death to another registration's info
    (a cross-fiber leak: wrong callback / wrong payload / double fire).

A doubled fire, a dropped fire, or an aliased-index cross-fiber routing all break
exactly-once.  We turn that into a CLOSED-WORLD CONSERVATION law, not a racy probe.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner, exactly-once):

  Each fiber owns a PER-WID pair of race-free slots (one writer each): registered[wid]
  and fired[wid].  Per iteration the fiber:
    - creates a FIBER-LOCAL object (a fresh Cell, never shared),
    - computes a UNIQUE payload that ENCODES its own wid (payload>>PSHIFT == wid),
    - registers weakref.finalize(obj, on_finalize, fired, last_payload, wid, payload)
      and bumps registered[wid] by one,
    - YIELDS so sibling fibers register concurrently -- this is where the shared
      _registry setitem + shared itertools.count RMW actually race,
    - drops the object's ONLY strong ref (``obj = None``).  A fiber-local instance
      with no other references deallocs immediately on the dropping hub, so the
      weakref callback fires SYNCHRONOUSLY, deterministically, exactly once -- no
      dependency on gc timing (see the WHY-NOT-GC note below),
    - asserts fired[wid] moved by EXACTLY ONE (0->1 for this registration; not 0 =
      a DROPPED fire / lost registry insertion, not 2 = a DOUBLED fire) and that
      last_payload[wid] equals THIS iteration's payload with the correct embedded wid
      (a mismatch = the shared registry routed a sibling's info here, a cross-fiber
      leak).

  Why the slots are race-free: on_finalize writes fired[wid]/last_payload[wid] where
  wid is the ARG baked into THIS fiber's registration.  Only fiber wid ever registers
  a finalizer carrying wid, and that fiber serializes its own registrations (one
  object at a time, fired synchronously within the same iteration), so each slot has
  exactly ONE logical writer.  A CORRECT runtime keeps fired[wid]==registered[wid] at
  every step; the program exits 0 when there is no bug.  (If the shared registry DID
  misroute a fire to a sibling's callback, THAT is the bug -- and it surfaces as this
  fiber's slot failing to move, or a wrong payload, which is exactly what we fail on.)

  WHY NOT gc-triggered firing (verified against plain threads).  We deliberately fire
  by synchronous decref, NOT by putting the object in a cycle and calling gc.collect().
  A control with 16 OS threads each creating a cyclic object + registering finalize +
  looping gc.collect() 64x showed 28701/32000 iterations where the thread's OWN
  gc.collect() did NOT collect its own just-orphaned cycle: free-threaded gc.collect()
  is a no-op while another thread's collection is in progress, so under a herd of
  collectors a cycle fires only LATER, on some other thread's sweep.  That is
  DOCUMENTED gc timing, NOT a runloom bug -- a per-iteration "fired 0->1 after my
  gc.collect()" check there would be a FALSE-POSITIVE generator.  The synchronous-
  decref control (16 threads, 320000 registrations) fired 320000/320000 with 0 misses
  and 0 payload errors, so the decref path is the sound exactly-once oracle.  The
  shared-registry REGISTRATION race (the actual C hazard) is fully exercised either
  way, because every fiber does the concurrent ``_registry[self] = info`` + shared
  index RMW regardless of how its object later dies.

  post() closes the global law: sum(fired) == sum(registered) (every registered
  finalizer fired exactly once, none dropped or doubled across the whole run), plus
  non-vacuity (registrations > 0) and require_no_lost (no fiber vanished stranded
  inside a registry mutation / finalizer callback).

  MEASURED (report-only, NEVER fails): a driver fiber samples the live size of the
  SHARED finalize._registry to report the concurrency DEPTH the global dict actually
  sustained -- proving the shared C hazard was genuinely contended, so the single-
  owner conservation law is not vacuously green.  It is an observation only; failing
  on it would mislabel documented registry churn as a bug.

FAIL ON: a finalizer that never fires (dropped registration/pop), a finalizer that
fires twice (doubled), or a fire delivering the wrong payload / wrong embedded wid
(the shared registry aliased one fiber's info to another) -- all genuine exactly-once
violations of weakref.finalize under M:N.  NOT on GC timing or registry size.

Distinct from p310 (a __del__ that RESURRECTS itself + re-registers at shutdown):
this is STEADY-STATE exactly-once conservation with no resurrection, pinning the
shared global _registry / _index_iter directly.

Stresses: weakref.finalize registration (shared _registry setitem + shared
itertools.count RMW), fire-once pop from the shared registry on the dropping hub,
synchronous-decref fire path, exactly-once conservation under GIL-off contention.

Good TSan / controlled-M:N-replay target: the concurrent ``_registry[self] = info``
and ``next(_index_iter)`` are textbook shared-container RMWs; a data-race report on
the registry dict entry -- or a single dropped/doubled fire under replay -- localizes
the exactly-once break before the conservation sum even closes.
"""
import weakref

import harness
import runloom

# The payload embeds wid in the high bits and a per-fiber generation counter in the
# low bits, so a fire delivering a payload whose high bits != wid is a provable
# cross-fiber leak (the shared registry routed a sibling's info here) even if the raw
# fire COUNT happened to line up.  Python ints are arbitrary precision, so wid may be
# arbitrarily large (the forever loop's --funcs can be millions).
PSHIFT = 40
PMASK = (1 << PSHIFT) - 1


class Cell(object):
    """A minimal fiber-local, weak-referenceable object with no cycle: dropping its
    only strong ref reclaims it by synchronous decref, firing its finalizer at once."""
    __slots__ = ("__weakref__",)


def on_finalize(fired, last_payload, wid, payload):
    """weakref.finalize callback.  Runs exactly once per registration, on the hub
    that drops the referent's last ref.  Writes ONLY slot[wid]; only fiber wid ever
    registers a finalizer carrying wid, so each slot has one logical writer."""
    fired[wid] += 1
    last_payload[wid] = payload


# Sustained churn per worker, bounded by H.running().  The registry-contention hazard
# only manifests when MANY fibers register/fire simultaneously while parked across
# their yield, so the scheduler reliably interleaves a sibling's registry mutation
# before this fiber's fire; a single register/fire per fiber barely overlaps.
INNER_CAP = 1000000


def one_iteration(H, wid, gen, state):
    """One exactly-once conservation step (single-owner).  Returns False on a
    failure (caller stops), True otherwise."""
    fired = state["fired"]
    registered = state["registered"]
    last_payload = state["last_payload"]

    before = fired[wid]
    payload = ((wid & PMASK) << PSHIFT) | (gen & PMASK)

    obj = Cell()
    weakref.finalize(obj, on_finalize, fired, last_payload, wid, payload)
    registered[wid] += 1
    runloom.yield_now()                # siblings race the shared registry here
    obj = None                         # drop the only strong ref -> fires now

    got = fired[wid]
    if got == before:
        H.fail("weakref.finalize DROPPED: registration #{0} for wid {1} never "
               "fired (fired[wid] stayed {2}) after its referent's only ref was "
               "dropped -- a lost insertion into / torn pop from the shared "
               "finalize._registry under GIL-off contention".format(
                   gen, wid, before))
        return False
    if got != before + 1:
        H.fail("weakref.finalize EXACTLY-ONCE broken: registration #{0} for wid "
               "{1} fired {2} times (fired[wid] {3}->{4}, expected +1) -- a DOUBLED "
               "fire from the shared registry under M:N".format(
                   gen, wid, got - before, before, got))
        return False

    lp = last_payload[wid]
    if lp != payload:
        H.fail("weakref.finalize CROSS-FIBER LEAK: fire for wid {0} reg #{1} "
               "delivered payload {2} but this fiber registered {3} -- the shared "
               "registry routed a sibling's info to this referent's death".format(
                   wid, gen, lp, payload))
        return False
    if (lp >> PSHIFT) != (wid & PMASK):
        H.fail("weakref.finalize WID MISROUTE: fire for wid {0} reg #{1} carried "
               "embedded wid {2} -- the shared registry aliased a different fiber's "
               "finalizer here".format(wid, gen, lp >> PSHIFT))
        return False
    return True


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        gen = 0
        while H.running() and gen < INNER_CAP:
            if not one_iteration(H, wid, gen, state):
                return
            H.op(wid)
            gen += 1
        H.task_done(wid)


def registry_sampler(H, state):
    """MEASURED (report-only).  Samples the live size of the SHARED
    finalize._registry to report how deep the concurrent registration churn ran on
    the global dict -- proof the shared C hazard the conservation law pins was
    genuinely contended.  NEVER fails."""
    reg = getattr(weakref.finalize, "_registry", None)
    peak = 0
    samples = 0
    while H.running():
        H.sleep(0.005)
        if reg is not None:
            try:
                n = len(reg)
            except Exception:
                n = 0
            samples += 1
            if n > peak:
                peak = n
    state["registry_peak"] = peak
    state["registry_samples"] = samples


def setup(H):
    # Per-wid race-free slots (one writer each) -- allocated here where H.funcs is
    # known.  fired/registered feed the exact conservation law; last_payload feeds
    # the cross-fiber-leak check.
    H.state = {
        "fired": [0] * H.funcs,
        "registered": [0] * H.funcs,
        "last_payload": [0] * H.funcs,
        "registry_peak": 0,
        "registry_samples": 0,
    }


def body(H):
    H.fiber(registry_sampler, H, H.state)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    fired_sum = sum(H.state["fired"])
    reg_sum = sum(H.state["registered"])
    peak = H.state["registry_peak"]
    samples = H.state["registry_samples"]

    H.log("weakref.finalize exactly-once: {0} registered, {1} fired (every per-"
          "iteration exactly-once + wid-payload check passed fail-fast); MEASURED "
          "shared finalize._registry peak={2} entries over {3} samples".format(
              reg_sum, fired_sum, peak, samples))

    # NON-VACUITY: the load-bearing hazard actually ran.
    H.check(reg_sum > 0,
            "no weakref.finalize registrations ran -- the exactly-once hazard was "
            "never exercised (oracle would be vacuous)")

    # GLOBAL CONSERVATION: every registered finalizer fired exactly once.  Each
    # per-iteration check was fail-fast, so reaching post with no failure already
    # proves per-slot fired==registered; assert the closed-world sum as a backstop.
    H.check(fired_sum == reg_sum,
            "weakref.finalize conservation broken: {0} finalizers fired but {1} "
            "were registered -- {2} fire(s) were {3} across the run under GIL-off "
            "contention on the shared finalize._registry".format(
                fired_sum, reg_sum, abs(fired_sum - reg_sum),
                "DROPPED" if fired_sum < reg_sum else "DOUBLED"))

    # COMPLETENESS: no fiber parked-then-vanished inside a registry mutation /
    # finalizer callback.
    H.require_no_lost("weakref.finalize exactly-once conservation")


if __name__ == "__main__":
    harness.main(
        "p544_finalize_exactly_once_conservation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="tens of thousands of fibers register weakref.finalize on fiber-"
                 "local objects, yield so siblings race the SHARED finalize._registry "
                 "setitem + shared itertools.count RMW, then drop the object's only "
                 "ref (synchronous decref -> deterministic exactly-once fire).  "
                 "CLOSED-WORLD exactly-once: per-wid fired[wid] moves 0->1 with the "
                 "correct wid-embedded payload, and sum(fired)==sum(registered) -- a "
                 "dropped fire, a doubled fire, or a mis-routed payload (the registry "
                 "aliased a sibling's info) fails.  MEASURED registry-peak sampler is "
                 "report-only")
