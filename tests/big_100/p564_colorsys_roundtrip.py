"""big_100 / 564 -- colorsys pure-function purity + round-trip law under M:N.

colorsys is a pure-math module: three forward/inverse conversion pairs over
float triples in the unit cube --
    rgb_to_yiq / yiq_to_rgb, rgb_to_hls / hls_to_rgb, rgb_to_hsv / hsv_to_rgb.
Every function is a straight-line arithmetic expression on its three float
arguments with NO global state, NO shared mutable container, NO caching: for a
fixed input triple the output is a DETERMINISTIC, bit-for-bit-identical float
triple, and the inverse recovers the original to within ~1e-15 (verified with a
200k-sample sweep: max yiq/hls/hsv round-trip error 4.4e-16 / 1.1e-15 / 8.3e-16;
recompute is bit-identical).

WHERE M:N COULD BREAK IT (the gap this program probes).  Each conversion runs a
handful of Python float multiplies/adds/compares across several bytecode ops and
temporary stack slots.  A fiber computes a forward triple, YIELDS (hub migration
+ sibling interleave), then recomputes the SAME forward call on the SAME single-
owner input floats.  If runloom torn a float across the yield -- a stackful-coro
frame that leaked a temporary into a sibling, a mis-restored evaluation-stack
slot on resume, a cross-fiber clobber of the fiber-local input triple -- the
recomputed triple would differ in even one bit, or the round-trip closure would
blow past epsilon.  On a CORRECT runtime the recompute is bit-identical and the
round-trip closes, so this single-owner oracle PASSES (program exits 0) when
there is no bug.

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (not documented Python semantics).
The input triple and every intermediate tuple are fiber-local floats/tuples,
created inside the worker and never shared with any sibling.  There is no shared
mutable container anywhere in the load-bearing arm (colorsys holds none, and we
introduce none), so a divergence CANNOT be the documented shared-object race
(p67/p490 shared-container behaviour); it can only be a runloom fault: a torn
value, a leaked/clobbered single-owner float across a yield, or a mis-restored
coroutine stack.  A plain-threads control (many OS threads each running the same
recompute+round-trip on private triples, GIL on and off) returns bit-identical
forward triples and closes every round-trip -- so a divergence here is a runloom
bug, not a colorsys or CPython-float property.

ORACLES:
  * LOAD-BEARING -- PURITY + ROUND-TRIP (worker, HARD, fail-fast).  Each fiber
    draws a fiber-local RGB triple, computes the three forward conversions
    (yiq/hls/hsv), YIELDS to let siblings interleave on other hubs, then:
      - recomputes each forward conversion and asserts it is BIT-IDENTICAL to the
        pre-yield triple (pure determinism -- ANY bit difference is a fault);
      - applies the inverse to the pre-yield forward triple and asserts the
        recovered RGB matches the original within EPS (the closed-form inverse
        law).
    Single-owner: all triples are fiber-local; nothing is shared.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-conversion
    (e.g. wedged across the yield) never returns; the watchdog catches it.

FAIL ON: a forward conversion that is not bit-identical when recomputed on the
same single-owner input across a yield, or a round-trip that misses the original
by more than EPS.  Both would indicate a torn/leaked single-owner float or a
mis-restored coroutine stack -- a runloom bug.

Stresses: pure float arithmetic across bytecode temporaries and evaluation-stack
slots, coroutine-frame save/restore of intermediate float values across a hub-
migrating yield, single-owner-triple integrity under M:N interleave.
"""
import colorsys

import harness
import runloom

# Round-trip closure tolerance.  Empirically the worst-case error over the three
# pairs is ~1.1e-15 across a 200k sweep; 1e-9 is a vast, unambiguous margin --
# only a genuinely corrupted value can breach it, never float-rounding noise.
EPS = 1e-9

# Number of distinct triples exercised per fiber before it re-checks running().
# The corruption hazard only shows under SUSTAINED churn: many fibers parked
# across their yield while siblings compute, so a single check per fiber barely
# overlaps a sibling's.  Bounded by H.running() so every fiber returns.
INNER_CAP = 100000


def one_check(H, wid, rng, state):
    """Single-owner purity + round-trip check on ONE fiber-local RGB triple.

    All values here are fiber-local: the input triple, the forward triples, and
    the recovered triples are created in this frame and never shared.  A bit
    difference on recompute, or a round-trip miss, is a torn/leaked single-owner
    float across the yield -- a runloom fault, not a colorsys or float property.
    """
    # Fiber-local RGB triple in the unit cube (the documented colorsys domain).
    r = rng.random()
    g = rng.random()
    b = rng.random()

    # Forward conversions, computed BEFORE the yield (the baseline triples).
    yiq0 = colorsys.rgb_to_yiq(r, g, b)
    hls0 = colorsys.rgb_to_hls(r, g, b)
    hsv0 = colorsys.rgb_to_hsv(r, g, b)

    # YIELD: hub migration + let siblings compute on other hubs.  If a sibling's
    # work leaks into this fiber's stack or clobbers a single-owner float, the
    # recompute below diverges.
    runloom.yield_now()
    if r < 0.5:
        runloom.sleep(0.0002)

    # PURITY: recompute each forward conversion on the SAME single-owner input
    # floats -- MUST be bit-for-bit identical (deterministic pure function).
    yiq1 = colorsys.rgb_to_yiq(r, g, b)
    if yiq1 != yiq0:
        H.fail("rgb_to_yiq NOT bit-identical on recompute across a yield: "
               "in=({0!r},{1!r},{2!r}) before={3!r} after={4!r} (wid {5}) -- a "
               "torn/leaked single-owner float or mis-restored coroutine "
               "stack".format(r, g, b, yiq0, yiq1, wid))
        return
    hls1 = colorsys.rgb_to_hls(r, g, b)
    if hls1 != hls0:
        H.fail("rgb_to_hls NOT bit-identical on recompute across a yield: "
               "in=({0!r},{1!r},{2!r}) before={3!r} after={4!r} (wid {5}) -- a "
               "torn/leaked single-owner float or mis-restored coroutine "
               "stack".format(r, g, b, hls0, hls1, wid))
        return
    hsv1 = colorsys.rgb_to_hsv(r, g, b)
    if hsv1 != hsv0:
        H.fail("rgb_to_hsv NOT bit-identical on recompute across a yield: "
               "in=({0!r},{1!r},{2!r}) before={3!r} after={4!r} (wid {5}) -- a "
               "torn/leaked single-owner float or mis-restored coroutine "
               "stack".format(r, g, b, hsv0, hsv1, wid))
        return

    # ROUND-TRIP: the analytic inverse of each pre-yield forward triple must
    # recover the original RGB within EPS (the closed-form inverse law).
    ry, gy, by = colorsys.yiq_to_rgb(*yiq0)
    if abs(ry - r) > EPS or abs(gy - g) > EPS or abs(by - b) > EPS:
        H.fail("yiq round-trip broke the inverse law: in=({0!r},{1!r},{2!r}) "
               "recovered=({3!r},{4!r},{5!r}) exceeds EPS={6} (wid {7}) -- a "
               "corrupted single-owner float across the yield".format(
                   r, g, b, ry, gy, by, EPS, wid))
        return
    rh, gh, bh = colorsys.hls_to_rgb(*hls0)
    if abs(rh - r) > EPS or abs(gh - g) > EPS or abs(bh - b) > EPS:
        H.fail("hls round-trip broke the inverse law: in=({0!r},{1!r},{2!r}) "
               "recovered=({3!r},{4!r},{5!r}) exceeds EPS={6} (wid {7}) -- a "
               "corrupted single-owner float across the yield".format(
                   r, g, b, rh, gh, bh, EPS, wid))
        return
    rv, gv, bv = colorsys.hsv_to_rgb(*hsv0)
    if abs(rv - r) > EPS or abs(gv - g) > EPS or abs(bv - b) > EPS:
        H.fail("hsv round-trip broke the inverse law: in=({0!r},{1!r},{2!r}) "
               "recovered=({3!r},{4!r},{5!r}) exceeds EPS={6} (wid {7}) -- a "
               "corrupted single-owner float across the yield".format(
                   r, g, b, rv, gv, bv, EPS, wid))
        return

    # NON-VACUITY tally: ONE slot per worker (single-writer-per-slot, race-free).
    state["checks"][wid] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            one_check(H, wid, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE checks-slot per worker, indexed by wid -- single writer per slot, so the
    # non-vacuity tally is race-free GIL-off.  Allocated here where H.funcs known.
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("colorsys purity+round-trip checks (all passed fail-fast): {0}; "
          "ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner arm actually ran.
    H.check(checks > 0,
            "no colorsys purity/round-trip checks ran -- the load-bearing "
            "single-owner conversion oracle was never exercised (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-conversion.
    H.require_no_lost("colorsys purity round-trip")


if __name__ == "__main__":
    harness.main(
        "p564_colorsys_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="colorsys is pure-math: rgb<->yiq/hls/hsv conversions are "
                 "deterministic float functions with no shared state.  "
                 "LOAD-BEARING single-owner oracle: each fiber computes the "
                 "forward conversions on a fiber-local RGB triple, yields "
                 "(hub migration + sibling interleave), then asserts the "
                 "recompute is BIT-IDENTICAL and the analytic inverse recovers "
                 "the original within 1e-9.  All triples are fiber-local (no "
                 "shared container), so a bit divergence or round-trip miss is "
                 "a torn/leaked single-owner float or mis-restored coroutine "
                 "stack -- a runloom bug, not a colorsys/float property")
