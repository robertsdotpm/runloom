"""big_100 / 608 -- syslog.LOG_MASK / LOG_UPTO priority-mask PURITY under M:N.

syslog is a PROCESS-GLOBAL module: openlog()/syslog()/closelog()/setlogmask()
all mutate one per-process log connection + one per-process priority mask (the
C `S_log_open`, `S_ident_o`, and the libc `setlogmask()` state).  That global
hook is NOT single-owner and MUST NOT be the oracle -- many fibers calling
openlog()/setlogmask() concurrently is documented process-global contention,
not a runloom bug, and syslog() would also spew into the real system log.

But syslog exposes two PURE, side-effect-free integer functions that the module
produces WITHOUT touching any global state:

    syslog.LOG_MASK(pri)  == 1 << pri                        (a single bit)
    syslog.LOG_UPTO(pri)  == (1 << (pri + 1)) - 1            (all bits <= pri)

Both are thin wrappers over the C `LOG_MASK`/`LOG_UPTO` macros -- they read no
module global, allocate a fresh PyLong, and return it.  On any correct runtime
they are referentially transparent: for a fixed integer input they return a
bit-identical result every call, on every hub, regardless of what any sibling
fiber is doing.  That is a legitimate SINGLE-OWNER oracle for a process-global
module: the oracle is built on the fiber-local integer these pure functions
PRODUCE, never on the shared syslog connection.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber feeds its
OWN fiber-local priorities into LOG_MASK/LOG_UPTO, computes a fiber-local
combined mask, YIELDS (so a sibling on another hub interleaves inside / around
the C call), then recomputes and asserts the result is bit-identical to the
pre-yield value AND equal to the closed-form expected mask.  If the runtime
tore the PyLong result, leaked a sibling fiber's argument or return value across
the yield/hub-migration boundary, or corrupted the C call frame, the recomputed
mask would differ from the closed-form -- a real runtime fault (torn object,
cross-fiber leak of single-owner state, value change across a yield).

PRIORITY BAND.  We use ONLY the documented syslog priorities 0..7
(LOG_EMERG..LOG_DEBUG).  In that band `1 << pri` and `(1<<(pri+1))-1` fit in a
positive C int, so the closed-form is exact.  (At pri>=31 the C macro's signed
32-bit `1 << pri` wraps to a negative / aliased value -- documented platform C
behavior, NOT a runtime bug -- so we deliberately stay inside 0..7 to keep the
oracle a clean bit-identity law.)

THREE INDEPENDENT LAWS, all fiber-local and closed-form, checked across a yield:

  * LAW-MASK:  LOG_MASK(p) == 1 << p, a single set bit, for each fiber-local p.
  * LAW-UPTO:  LOG_UPTO(t) == (1 << (t+1)) - 1, all bits at/below t.
  * LAW-COMBINE:  LOG_UPTO(t) == OR over p in 0..t of LOG_MASK(p)  (the mask
    machinery's internal consistency: the "up to" mask is exactly the union of
    the individual priority bits).  Each fiber also ORs a RANDOM fiber-local
    subset of 0..7 into a combined mask and re-derives it after the yield; the
    subset makes the load-bearing value fiber-distinct (256 possible values),
    so a cross-fiber leak returns a mask that fails the closed-form.

ORACLES:
  * LOAD-BEARING -- MASK PURITY (worker, HARD, fail-fast).  Single-owner: every
    input priority and every mask is a fiber-local int; nothing is shared.  A
    value that changes across the yield, or disagrees with the closed form, is a
    runloom purity/isolation fault.
  * NON-VACUITY (post, HARD): mask_checks > 0 -- the purity arm actually ran.
  * COMPLETENESS (post, HARD): require_no_lost -- no fiber vanished mid-check
    (e.g. stranded inside the C LOG_MASK call across a hub migration).

FAIL ON: LOG_MASK/LOG_UPTO returning a value that differs across a yield, or
that disagrees with the closed-form (1<<p) / ((1<<(t+1))-1) / OR-of-bits law.
There is NO shared-mutable / report-only arm here: the pure functions have no
shared state to race, so the whole oracle is load-bearing and single-owner.

Stresses: syslog.LOG_MASK / LOG_UPTO pure C wrappers under M:N hub migration,
PyLong allocation + return across a yield, C call frame integrity when a sibling
interleaves, fiber-local integer purity vs cross-fiber argument/return leakage.

Good TSan / controlled-M:N-replay target: LOG_MASK/LOG_UPTO allocate a fresh
PyLong and return it with no lock; under the single-owner arm each result is
touched by exactly one fiber, so a data-race report on the returned object -- or
a deterministic replay that observes a mask mid-construction -- localizes a torn
return before the bit-identity law even closes.
"""
import syslog

import harness
import runloom

# The documented syslog priority band: LOG_EMERG (0) .. LOG_DEBUG (7).  Within
# this band 1<<pri and (1<<(pri+1))-1 are exact positive ints (no 32-bit C shift
# wraparound), so the closed-form oracle is exact.
PRIS = (syslog.LOG_EMERG, syslog.LOG_ALERT, syslog.LOG_CRIT, syslog.LOG_ERR,
        syslog.LOG_WARNING, syslog.LOG_NOTICE, syslog.LOG_INFO, syslog.LOG_DEBUG)
TOP = max(PRIS)                             # 7 == LOG_DEBUG

# Sustained checks per worker, bounded by H.running().  The purity/isolation
# hazard only manifests under SUSTAINED churn -- many fibers computing masks
# while sleep-PARKED across the yield, so a sibling reliably interleaves before
# this fiber resumes.  A single check per fiber barely overlaps a sibling's.
INNER_CAP = 100000


def expected_mask(pri):
    """Closed-form LOG_MASK: a single set bit at position `pri`."""
    return 1 << pri


def expected_upto(top):
    """Closed-form LOG_UPTO: all bits at or below `top`."""
    return (1 << (top + 1)) - 1


def mask_check(H, wid, idx, rng, state):
    """Single-owner purity check.  Every input and result is a fiber-local int.

    Compute LOG_MASK/LOG_UPTO on fiber-local priorities, build a fiber-distinct
    combined mask from a random subset, yield so siblings interleave on other
    hubs, then recompute and assert bit-identity + closed-form agreement."""
    # --- fiber-local inputs -------------------------------------------------
    # A random non-empty subset of 0..7 makes the combined mask fiber-distinct
    # (256 possible values), so a cross-fiber leak returns a mask that fails the
    # closed form rather than coincidentally matching.
    subset = [p for p in PRIS if rng.getrandbits(1)]
    if not subset:
        subset = [PRIS[idx % len(PRIS)]]
    top = subset[idx % len(subset)]         # a fiber-local "up to" priority

    # --- baseline: compute every mask BEFORE the yield ----------------------
    base_masks = [syslog.LOG_MASK(p) for p in subset]
    base_combined = 0
    for m in base_masks:
        base_combined |= m
    base_upto = syslog.LOG_UPTO(top)
    # LAW-COMBINE baseline: OR of LOG_MASK(0..top) must equal LOG_UPTO(top).
    base_upto_union = 0
    for p in range(top + 1):
        base_upto_union |= syslog.LOG_MASK(p)

    # --- YIELD: let siblings run the same pure calls on other hubs ----------
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # --- recompute AFTER the yield and check the three laws -----------------
    for p, base_m in zip(subset, base_masks):
        got = syslog.LOG_MASK(p)
        exp = expected_mask(p)
        # LAW-MASK closed form.
        if got != exp:
            H.fail("LOG_MASK({0}) == {1}, expected 1<<{0} == {2} (wid {3}) -- a "
                   "pure syslog mask function returned a wrong value under M:N "
                   "(torn PyLong or cross-fiber argument/return leak)".format(
                       p, got, exp, wid))
            return
        # Bit-identity across the yield.
        if got != base_m:
            H.fail("LOG_MASK({0}) CHANGED across a yield: was {1}, now {2} "
                   "(wid {3}) -- a pure function's result is not stable across a "
                   "hub migration; a sibling's call corrupted this fiber's "
                   "value".format(p, base_m, got, wid))
            return

    # LAW-UPTO closed form + stability.
    upto = syslog.LOG_UPTO(top)
    exp_upto = expected_upto(top)
    if upto != exp_upto:
        H.fail("LOG_UPTO({0}) == {1}, expected (1<<{2})-1 == {3} (wid {4}) -- a "
               "pure syslog mask function returned a wrong value under M:N".format(
                   top, upto, top + 1, exp_upto, wid))
        return
    if upto != base_upto:
        H.fail("LOG_UPTO({0}) CHANGED across a yield: was {1}, now {2} (wid {3}) "
               "-- pure-function result not stable across a hub migration".format(
                   top, base_upto, upto, wid))
        return

    # LAW-COMBINE: LOG_UPTO(top) == OR of LOG_MASK(0..top), recomputed here.
    upto_union = 0
    for p in range(top + 1):
        upto_union |= syslog.LOG_MASK(p)
    if upto_union != upto:
        H.fail("LAW-COMBINE broken: OR of LOG_MASK(0..{0}) == {1} but "
               "LOG_UPTO({0}) == {2} (wid {3}) -- the priority-mask machinery is "
               "internally inconsistent under M:N".format(
                   top, upto_union, upto, wid))
        return
    if upto_union != base_upto_union:
        H.fail("LAW-COMBINE union CHANGED across a yield: was {0}, now {1} "
               "(wid {2}) -- pure LOG_MASK sequence not stable across a hub "
               "migration".format(base_upto_union, upto_union, wid))
        return

    # Combined fiber-distinct mask: recompute and confirm bit-identity.
    combined = 0
    for p in subset:
        combined |= syslog.LOG_MASK(p)
    if combined != base_combined:
        H.fail("fiber-local combined mask CHANGED across a yield: was {0}, now "
               "{1} (wid {2}, subset {3}) -- a cross-fiber leak of this fiber's "
               "single-owner mask".format(base_combined, combined, wid, subset))
        return

    state["mask_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs the single-owner mask-purity oracle in a sustained inner
    loop so many fibers are simultaneously sleep-PARKED across their yield, giving
    the scheduler a reliable window to interleave a sibling's pure calls before
    this fiber resumes and re-checks its fiber-local masks."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            mask_check(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "mask_checks": [0] * 1024,          # LOAD-BEARING single-owner purity checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["mask_checks"])
    H.log("syslog mask purity [single-owner LOAD-BEARING]: {0} LOG_MASK/LOG_UPTO "
          "bit-identity + closed-form checks (all passed fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(checks > 0,
            "no syslog mask-purity checks ran -- the LOG_MASK/LOG_UPTO purity "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the C
    # LOG_MASK call across a hub migration).
    H.require_no_lost("syslog mask purity")


if __name__ == "__main__":
    harness.main(
        "p608_syslog_logmask_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="syslog is process-global (openlog/syslog/setlogmask mutate one "
                 "per-process connection + mask -- NOT single-owner), but its "
                 "LOG_MASK(pri)==1<<pri and LOG_UPTO(t)==(1<<(t+1))-1 are PURE "
                 "side-effect-free C wrappers.  LOAD-BEARING single-owner oracle: "
                 "each fiber computes masks on fiber-local priorities 0..7, builds "
                 "a fiber-distinct combined mask, yields for hub migration, then "
                 "recomputes and asserts bit-identity across the yield AND the "
                 "closed-form laws (single bit / all-bits-below / OR-union == "
                 "LOG_UPTO).  A mask that changes across a yield or disagrees with "
                 "the closed form is a torn-return / cross-fiber-leak runtime bug")
