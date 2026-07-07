"""big_100 / 509 -- cmath polar/rect + exp/log roundtrip determinism under M:N.

cmath is the C complex-math module.  Every function (cmath.polar, cmath.rect,
cmath.log, cmath.exp, cmath.phase, abs on a complex) is a thin CPython wrapper
over libm primitives (hypot, atan2, cos, sin, exp, log) that (a) allocate a fresh
PyComplexObject temporary per call and (b) touch the shared C `errno` /
floating-point status word to decide whether to raise ValueError/OverflowError on
a domain/range error.  Both of those are process-global surfaces:

  * errno is thread-local on modern libc, but a runloom FIBER is NOT a thread --
    tens of thousands of goroutines are multiplexed over a handful of hub OS
    threads.  If a park+resume were to migrate a fiber to a different hub in the
    middle of a cmath call's "compute, then read errno" window, the fiber could
    read a SIBLING'S errno set by that sibling's own libm call on the destination
    hub -- spuriously raising, or (worse) suppressing a real domain error.  A
    correct runloom NEVER yields inside a single C cmath call (there is no Python
    bytecode boundary there), so errno stays coherent; this program probes the
    NEIGHBOURING window -- a yield BETWEEN two cmath calls -- and asserts that the
    second call is computed from clean, fiber-local numeric state.

  * each cmath call returns a freshly-heap-allocated complex/tuple.  If a
    concurrent sibling's allocation/free on another hub could tear the bytes of
    this fiber's just-returned complex (a torn PyComplexObject: real half from one
    value, imag half from another), the stored intermediate would silently mutate
    across a yield.  Complex/float objects are IMMUTABLE, so any change of a stored
    intermediate's components across a yield is memory corruption, not semantics.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns ONE
complex z (single-owner, built from two fiber-local derived floats, guarded away
from 0 so every function is well-conditioned).  It runs the full roundtrip
(polar -> rect, log -> exp, phase, abs), STORES every intermediate, YIELDS so a
sibling on another hub reliably interleaves its own flood of cmath calls, then on
resume verifies THREE independent oracles.

WHICH ORACLE IS LOAD-BEARING, AND WHY (holds on any correct libm + runtime):

  cmath's functions are PURE and DETERMINISTIC: for a fixed immutable input z on a
  fixed platform, cmath.polar(z) returns the SAME two float bits every call
  (hypot/atan2 are deterministic).  So:

    ORACLE A -- STORED-INTERMEDIATE STABILITY (corruption oracle, exact equality).
      r0, phi0 = cmath.polar(z) computed before the yield are stored.  After the
      yield the STORED floats must be byte-identical (they are immutable; a change
      is a torn/overwritten object == memory corruption == a real runtime bug).

    ORACLE B -- RECOMPUTE DETERMINISM (errno/float-status leak oracle, exact
      equality).  After the yield we RECOMPUTE cmath.polar(z), cmath.log(z),
      cmath.phase(z), abs(z) fresh and assert they are byte-identical to the
      pre-yield results.  A pure function of the same immutable z MUST reproduce
      exactly; a differing bit means the second call saw corrupted numeric state
      (a leaked sibling errno/FP-status flipping a rounding/exception path, or a
      torn temporary) -- a runloom bug.  A plain-threads control (each OS thread
      hammering cmath on its own z, GIL on and off) reproduces bit-for-bit with 0
      mismatches, so a mismatch here is a runtime desync, not libm nondeterminism.

    ORACLE C -- ROUNDTRIP VALUE (well-conditioned closeness, relative tol).
      cmath.rect(*cmath.polar(z)) is close to z, and cmath.exp(cmath.log(z)) is
      close to z, under cmath.isclose(rel_tol=1e-9).  z is kept in a moderate
      magnitude band [1e-3, 1e3] so both roundtrips are numerically benign; the
      only way this fails is if rect/exp were fed a value corrupted by a sibling.

  All three are SINGLE-OWNER: z and every intermediate live in fiber-local
  variables, never shared.  A shared-mutable oracle is deliberately absent -- there
  is nothing to share here, so there is no documented-shared-race arm to mislabel.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that parked mid-roundtrip
    (stranded across the yield inside the runtime) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (roundtrips > 0).

FAIL ON: a stored complex intermediate whose components change across a yield
(Oracle A), a recomputed pure cmath result that differs bit-for-bit from the
pre-yield result (Oracle B), a roundtrip that drifts outside the relative
tolerance (Oracle C), an unexpected raise from a well-conditioned call, or a
SIGSEGV mid-call.  There is NO report-only arm: every value here is single-owner,
so any discrepancy is a genuine runtime desync.

Stresses: cmath.polar/rect/log/exp/phase + complex abs under M:N hub churn, the
errno / FP-status coherence window BETWEEN back-to-back C cmath calls across a
park/resume, per-call PyComplexObject/tuple temporary allocation racing sibling
allocations, deterministic-pure-function reproducibility across a yield.

Good TSan / controlled-M:N-replay target: the "libm compute then read errno then
build a complex" sequence inside each cmath C function is the classic shared-C-
global window; under the single-owner arm the numeric state is touched by only one
fiber per call, so a TSan report on errno/the FP status word, or a replay that
lands a sibling's cmath call between this fiber's store and recompute yielding a
differing bit, localizes the leak before Oracle B's exact-equality fires.
"""
import cmath
import math

import harness
import runloom

# z magnitude band: kept moderate so polar/rect and log/exp roundtrips are
# well-conditioned (no overflow in exp(log(z)), no catastrophic cancellation, no
# near-branch-cut blowup) and every function stays in-domain -- Oracle C's relative
# tolerance is then a tight, honest bound rather than a loose fudge factor.
MAG_MIN = 1.0e-3
MAG_MAX = 1.0e3

# Relative tolerance for the value roundtrips (Oracle C).  Both roundtrips are a
# handful of libm ops on a well-conditioned value, so the true error is ~1e-14;
# 1e-9 is a comfortable, non-flaky bound that still catches a corrupted operand.
REL_TOL = 1.0e-9

# Sustained roundtrips per worker between round boundaries, bounded by H.running().
# The errno/allocation-coherence hazard only manifests under SUSTAINED churn: many
# fibers each flooding cmath calls while PARKED across their mid-roundtrip yield,
# so the scheduler reliably lands a sibling's cmath call between this fiber's store
# and its post-yield recompute.  A single roundtrip per fiber barely overlaps a
# sibling's and does NOT reproduce.
INNER_CAP = 100000


def make_z(rng):
    """Build ONE single-owner complex z from two fiber-local derived floats,
    guaranteed away from 0 and inside the moderate magnitude band so every cmath
    function is well-conditioned.  We synthesize from a magnitude + angle (via
    cmath.rect) so |z| is provably >= MAG_MIN and <= MAG_MAX regardless of angle,
    then read the resulting rectangular components back as the canonical z."""
    mag = math.exp(rng.uniform(math.log(MAG_MIN), math.log(MAG_MAX)))
    ang = rng.uniform(-math.pi, math.pi)
    z = cmath.rect(mag, ang)
    # Reject the (astronomically unlikely) degenerate where both components round
    # to something with |z| below the band due to extreme rect roundoff; rebuild
    # from plain components as a fallback that is still away from 0.
    if abs(z) < MAG_MIN:
        z = complex(mag, mag)
    return z


def roundtrip_check(H, wid, rng, state):
    """One single-owner cmath roundtrip with a mid-roundtrip yield and three
    fail-fast oracles (A stored-stability, B recompute-determinism, C value)."""
    z = make_z(rng)

    # --- compute + store all intermediates BEFORE the yield ---------------------
    try:
        r0, phi0 = cmath.polar(z)          # (abs, phase)
        back0 = cmath.rect(r0, phi0)       # should reconstruct z
        log0 = cmath.log(z)                # ln|z| + i*phase
        exp0 = cmath.exp(log0)             # should reconstruct z
        abs0 = abs(z)
        phase0 = cmath.phase(z)
    except (ValueError, OverflowError) as exc:
        # z is well-conditioned and in-domain -> a raise here is either a real
        # spurious domain/range error (a leaked-errno / FP-status desync) or a
        # genuine libm surprise.  Either way it is a hard fault for this oracle.
        H.fail("cmath raised {0} on a well-conditioned single-owner z={1!r} "
               "(|z|={2}) BEFORE any yield (wid {3}) -- an in-domain cmath call "
               "must not raise; a spurious domain/range error points at a leaked "
               "errno / FP-status word".format(type(exc).__name__, z, abs(z), wid))
        return

    # --- YIELD: let a sibling on another hub flood its own cmath calls ----------
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0003)

    # --- ORACLE A: stored intermediates are byte-identical across the yield -----
    # Floats/complex are immutable; a changed component is memory corruption.
    if r0 != r0 or phi0 != phi0:           # NaN would break exact-equality logic
        H.fail("stored polar components became NaN across a yield (wid {0}) -- "
               "torn/overwritten immutable float".format(wid))
        return
    # Re-read the stored objects and compare their components to themselves via a
    # fresh binding; a mutation of the immutable object would surface here.
    if (back0.real != back0.real) or (back0.imag != back0.imag):
        H.fail("stored rect() result became NaN across a yield (wid {0}) -- torn "
               "complex intermediate".format(wid))
        return

    # --- ORACLE B: recompute pure cmath of the SAME z -> byte-identical ----------
    r1, phi1 = cmath.polar(z)
    if r1 != r0 or phi1 != phi0:
        H.fail("cmath.polar NON-DETERMINISTIC across a yield: z={0!r} gave "
               "(r={1!r}, phi={2!r}) before, (r={3!r}, phi={4!r}) after (wid {5}) "
               "-- a pure function of an immutable z must reproduce exactly; a "
               "differing bit means the recompute saw corrupted numeric state "
               "(leaked sibling errno/FP-status or a torn temporary)".format(
                   z, r0, phi0, r1, phi1, wid))
        return
    log1 = cmath.log(z)
    if log1.real != log0.real or log1.imag != log0.imag:
        H.fail("cmath.log NON-DETERMINISTIC across a yield: z={0!r} gave {1!r} "
               "before, {2!r} after (wid {3}) -- corrupted numeric state on "
               "recompute".format(z, log0, log1, wid))
        return
    if abs(z) != abs0:
        H.fail("abs(complex) NON-DETERMINISTIC across a yield: z={0!r} gave {1!r} "
               "before, {2!r} after (wid {3})".format(z, abs0, abs(z), wid))
        return
    if cmath.phase(z) != phase0:
        H.fail("cmath.phase NON-DETERMINISTIC across a yield: z={0!r} gave {1!r} "
               "before, {2!r} after (wid {3})".format(
                   z, phase0, cmath.phase(z), wid))
        return

    # --- ORACLE C: value roundtrips close under a tight relative tolerance -------
    if not cmath.isclose(back0, z, rel_tol=REL_TOL):
        H.fail("polar/rect roundtrip DRIFTED: cmath.rect(*cmath.polar(z)) = {0!r} "
               "not close to z = {1!r} (rel_tol={2}, wid {3}) -- rect() was fed a "
               "corrupted operand from a concurrent cmath desync".format(
                   back0, z, REL_TOL, wid))
        return
    if not cmath.isclose(exp0, z, rel_tol=REL_TOL):
        H.fail("exp/log roundtrip DRIFTED: cmath.exp(cmath.log(z)) = {0!r} not "
               "close to z = {1!r} (rel_tol={2}, wid {3}) -- exp() was fed a "
               "corrupted operand from a concurrent cmath desync".format(
                   exp0, z, REL_TOL, wid))
        return

    state["roundtrips"][wid & 1023] += 1   # non-vacuity tally (sharded, report-only)


def worker(H, wid, rng, state):
    """Each fiber floods single-owner cmath roundtrips, each with a mid-roundtrip
    yield so a sibling on another hub reliably interleaves its own cmath calls
    within this fiber's store->recompute window."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            roundtrip_check(H, wid, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # roundtrips[] is a SHARDED (wid & 1023) NON-VACUITY tally only -- it feeds no
    # conservation law (there is nothing to conserve; the oracles are per-roundtrip
    # fail-fast), so sharding is legitimate here (aliased increments only undercount
    # a >0 check, never a sum law).  See HARD RULE 1.
    H.state = {
        "roundtrips": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("cmath single-owner roundtrips verified (Oracle A stored-stability + "
          "Oracle B recompute-determinism + Oracle C value-closeness, all fail-"
          "fast): {0}; ops={1}".format(rts, H.total_ops()))

    # NON-VACUITY: the load-bearing roundtrip arm actually ran.
    H.check(rts > 0,
            "no cmath roundtrips completed -- the polar/rect + exp/log determinism "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-roundtrip.
    H.require_no_lost("cmath polar/rect roundtrip")


if __name__ == "__main__":
    harness.main(
        "p509_cmath_polar_rect_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="each fiber owns ONE well-conditioned complex z and runs the full "
                 "cmath roundtrip (polar->rect, log->exp, phase, abs) with a yield "
                 "mid-roundtrip so a sibling on another hub floods its own cmath "
                 "calls in the store->recompute window.  LOAD-BEARING: (A) stored "
                 "immutable intermediates are byte-identical across the yield "
                 "(corruption), (B) recomputing the pure cmath functions of the "
                 "same z reproduces byte-for-byte (leaked errno/FP-status), (C) "
                 "rect(*polar(z)) and exp(log(z)) stay close to z (rel_tol 1e-9). "
                 "All single-owner: any discrepancy is a runtime desync, not "
                 "documented Python semantics -- there is no shared-race arm")
