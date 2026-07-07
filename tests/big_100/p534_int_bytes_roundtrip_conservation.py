"""big_100 / 534 -- PyLong (bigint) digit-array round-trip conservation under M:N.

A large Python int is NOT a machine word: CPython stores it as a variable-length
array of 30-bit "digits" (`ob_digit[]` on the PyLongObject) whose length is the
int's magnitude / 30 bits.  Every non-trivial int operation -- `int.to_bytes`,
`int.from_bytes` (`_PyLong_FromByteArray` / `_PyLong_AsByteArray`), multiply,
floor-divide, `bit_length`, `bin()` -- walks that digit array digit-by-digit in C.
None of those C loops yields to the runloom scheduler mid-walk on a correct
runtime; the whole point of this probe is to confirm that.  If a hub migration or
a torn scheduler wake were ever to corrupt a live PyLongObject's digit array while
one of those C loops is mid-flight (or to hand a fiber a stale/half-updated int
object across a yield), the round-trip value would come back WRONG -- a silently
corrupted bigint, the nastiest possible data corruption because ints look
immutable and are trusted everywhere.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns a large,
private bigint N (built from its own single-owner rng -- never shared, never
mutated, an immutable int object held only by this fiber).  It computes a set of
CLOSED-WORLD algebraic identities on N, yields (so a sibling on another hub runs
and churns its own bigints / the allocator's block pools that back digit arrays),
then RE-computes the identities and asserts they still hold EXACTLY.  Because N is
single-owner and immutable, on a correct runtime every identity is a mathematical
law with NO tolerance:  the only way any of them can fail is a runtime bug (a torn
digit array, a stale object handed back across the yield, a lost/dup wake landing
a fiber on corrupted PyLong state, or a SIGSEGV inside the C digit walk).

WHICH ORACLE IS LOAD-BEARING, AND WHY.  The load-bearing oracle is the set of
single-owner closed-world bigint identities, checked fail-fast across a yield:

  (1) BIG-ENDIAN round-trip:    N == int.from_bytes(N.to_bytes(k, 'big'), 'big')
  (2) LITTLE-ENDIAN round-trip: N == int.from_bytes(N.to_bytes(k, 'little'), 'little')
  (3) SIGNED round-trip of -N:  -N == int.from_bytes((-N).to_bytes(k+1,'big',signed=True),
                                                     'big', signed=True)
  (4) MULTIPLY/DIVIDE identity: N == (N * M) // M  for a fiber-local M > 0
                                (exercises the C multiply then floor-divide over a
                                 digit array ~2x longer than N's -- the biggest
                                 digit walk in the program)
  (5) bit_length invariant:     N.bit_length() unchanged across the yield
  (6) popcount invariant:       bin(N).count('1') unchanged across the yield
  (7) value identity:           N compares == to the baseline reference captured
                                BEFORE the yield (an immutable int must never
                                change value across a scheduler point).

  These are exact laws (no float, no tolerance).  We do NOT test object identity
  (`id()`): CPython freely interns/caches small ints and reallocates big ones, so
  identity is not a runtime invariant -- VALUE conservation is.  The oracle also
  deliberately includes a NEGATIVE int (the signed-bytes path, a distinct C
  routine that must reconstruct the two's-complement magnitude) and a value
  CROSSING the small-int cache boundary (CPython caches ints in [-5, 256]; we
  round-trip 255/256/257/258 and their negatives so the freshly-allocated-vs-
  cached transition is exercised -- a from_bytes that returned a cached wrong
  singleton would be caught).

  Single-owner: N, -N, M, the boundary values, and the rng that built them are all
  fiber-local; nothing here is shared, so a failure CANNOT be documented shared-
  object M:N semantics -- it can ONLY be a runloom bug.

ORACLES:
  * LOAD-BEARING -- BIGINT ROUND-TRIP CONSERVATION (worker, HARD, fail-fast).
    The seven identities above, computed on a private N, verified to survive a
    yield unchanged.  A mismatch is a torn/corrupted PyLong -> H.fail, return.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-walk
    (stranded inside a C digit loop on a corrupted object, or SIGSEGV) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (int_checks > 0),
    so the round-trip hazard was genuinely exercised.

FAIL ON: a round-trip value that differs from N, a signed round-trip that differs
from -N, (N*M)//M != N, a bit_length/popcount that changed across the yield, a
baseline value mismatch, or a SIGSEGV inside the C digit walk.  There is NO
report-only arm: every value here is single-owner, so there is no documented
shared-object behavior to measure -- any deviation is a real bug.

Stresses: PyLongObject digit-array integrity, _PyLong_FromByteArray /
_PyLong_AsByteArray (both endiannesses, signed + unsigned), C bigint multiply and
floor-divide over multi-hundred-byte magnitudes, bit_length / bin() digit walks,
small-int cache boundary reconstruction, all across hub migration + a yield under
M:N.  Not covered by p417/p404 (which probe other int corners).

Good TSan / controlled-M:N-replay target: the C digit-array read loops in
_PyLong_AsByteArray run over an object whose block-pool backing store is being
recycled by sibling fibers' bigint churn on other hubs; a data-race report on a
PyLong's ob_digit, or a deterministic-replay that reads a digit mid-recycle,
localizes the corruption before the value law even fires.
"""

import harness
import runloom

# Size band (in bytes) for each fiber's private bigint magnitude.  Chosen so the
# digit array is genuinely multi-hundred-digit (a real variable-length walk, not a
# single-word fast path) while staying cheap enough to churn tens of thousands of
# times under the timeout.  The top bit of the top byte is forced set so the
# magnitude's bit_length is EXACTLY 8*nbytes -- makes the minimal-bytes round-trip
# length deterministic.
MIN_BYTES = 48
MAX_BYTES = 288

# Small-int cache boundary probes.  CPython caches ints in [-5, 256]; 255/256 are
# cached, 257/258 are freshly allocated.  Round-tripping across this line exercises
# the from_bytes path that must NOT return a wrong cached singleton.
BOUNDARY_VALUES = (255, 256, 257, 258, -5, -6, 0, 1)

# Sustained churn: the digit-array corruption hazard only manifests when MANY
# fibers simultaneously build/round-trip big magnitudes while sleep-PARKED across
# their yield, so the scheduler reliably migrates/interleaves a sibling before this
# fiber resumes.  One check per fiber barely overlaps a sibling and does not
# reproduce.
INNER_CAP = 100000


def build_big(rng):
    """Build one fiber-local large positive bigint with a deterministic bit_length.

    Draws a random magnitude of `nbytes` bytes from the fiber's single-owner rng,
    forces the top bit so bit_length == 8*nbytes exactly, and returns (N, nbytes).
    N is a private immutable int -- never shared."""
    nbytes = rng.randint(MIN_BYTES, MAX_BYTES)
    nbits = 8 * nbytes
    N = rng.getrandbits(nbits) | (1 << (nbits - 1))     # top bit set -> bit_length == nbits
    return N, nbytes


def int_check(H, wid, rng, state):
    """Single-owner bigint round-trip conservation check (fail-fast).

    Build a private N, capture baseline invariants, YIELD (let a sibling on another
    hub churn its own bigints / the allocator pools), then re-verify every closed-
    world identity.  Any deviation is a torn/corrupted PyLong -- a runtime bug."""
    N, nbytes = build_big(rng)

    # Baseline invariants captured BEFORE the yield.
    baseline_N = N                                   # immutable; value must survive
    bl = N.bit_length()
    pc = bin(N).count("1")
    neg = -N
    # A fiber-local odd multiplier > 1; keeps (N*M)//M exact and grows the digit
    # array ~2x for the biggest C multiply/divide walk in the program.
    M = (rng.getrandbits(160) | 1) + 2
    boundary = BOUNDARY_VALUES[rng.randrange(len(BOUNDARY_VALUES))]

    # YIELD at the hazard boundary so a sibling reliably interleaves / a hub
    # migration can occur before we re-walk N's digit array.
    runloom.yield_now()
    if rng.getrandbits(1):
        runloom.sleep(0.0003)

    # ---- (7) value identity: N unchanged across the yield ---------------------
    if N != baseline_N:
        H.fail("bigint VALUE CHANGED across a yield (wid {0}): a single-owner "
               "immutable int N ({1} bits) no longer equals its own baseline "
               "reference -- the PyLongObject was corrupted or a stale object "
               "was handed back across the scheduler point".format(wid, bl))
        return

    # ---- (5) bit_length invariant --------------------------------------------
    bl2 = N.bit_length()
    if bl2 != bl:
        H.fail("bigint bit_length CHANGED across a yield (wid {0}): was {1}, now "
               "{2} -- N's digit array was torn (a high digit lost/gained) under "
               "M:N".format(wid, bl, bl2))
        return

    # ---- (6) popcount invariant ----------------------------------------------
    pc2 = bin(N).count("1")
    if pc2 != pc:
        H.fail("bigint popcount CHANGED across a yield (wid {0}): was {1}, now {2} "
               "-- bin(N) walked a corrupted digit array under M:N".format(
                   wid, pc, pc2))
        return

    # ---- (1) big-endian round-trip -------------------------------------------
    rt_be = int.from_bytes(N.to_bytes(nbytes, "big"), "big")
    if rt_be != N:
        H.fail("bigint BIG-ENDIAN round-trip broke (wid {0}): "
               "from_bytes(N.to_bytes({1},'big'),'big') != N ({2} bits) -- "
               "_PyLong_AsByteArray/_FromByteArray produced a corrupted value "
               "under M:N (torn digit array in the C walk)".format(
                   wid, nbytes, bl))
        return

    # ---- (2) little-endian round-trip ----------------------------------------
    rt_le = int.from_bytes(N.to_bytes(nbytes, "little"), "little")
    if rt_le != N:
        H.fail("bigint LITTLE-ENDIAN round-trip broke (wid {0}): "
               "from_bytes(N.to_bytes({1},'little'),'little') != N ({2} bits) -- "
               "the little-endian C digit walk corrupted the value under "
               "M:N".format(wid, nbytes, bl))
        return

    # ---- (3) signed round-trip of the NEGATIVE int ---------------------------
    # A negative int's to_bytes(signed=True) drives a distinct two's-complement C
    # routine; +1 byte guarantees room for the sign.
    rt_neg = int.from_bytes(neg.to_bytes(nbytes + 1, "big", signed=True),
                            "big", signed=True)
    if rt_neg != neg:
        H.fail("bigint SIGNED round-trip broke (wid {0}): signed from_bytes of "
               "(-N).to_bytes({1},'big',signed=True) != -N ({2} bits) -- the "
               "two's-complement C reconstruction corrupted the magnitude under "
               "M:N".format(wid, nbytes + 1, bl))
        return

    # ---- (4) multiply / floor-divide identity --------------------------------
    # (N * M) // M == N exactly for M > 0.  Exercises the longest digit walk.
    prod = N * M
    quot = prod // M
    if quot != N:
        H.fail("bigint MULTIPLY/DIVIDE identity broke (wid {0}): (N*M)//M != N "
               "for N ({1} bits) and fiber-local M -- the C bigint multiply or "
               "floor-divide corrupted a digit array under M:N".format(wid, bl))
        return

    # ---- small-int cache boundary round-trip ---------------------------------
    # Crossing [-5, 256]: cached vs freshly allocated.  Value must survive both
    # byte orders regardless of caching.
    blen = (boundary.bit_length() // 8) + 2
    rt_b_be = int.from_bytes(boundary.to_bytes(blen, "big", signed=True),
                             "big", signed=True)
    rt_b_le = int.from_bytes(boundary.to_bytes(blen, "little", signed=True),
                             "little", signed=True)
    if rt_b_be != boundary or rt_b_le != boundary:
        H.fail("small-int-boundary round-trip broke (wid {0}): value {1} "
               "round-tripped to be={2} le={3} -- from_bytes returned a wrong "
               "cached/allocated int across the [-5,256] cache boundary under "
               "M:N".format(wid, boundary, rt_b_be, rt_b_le))
        return

    state["checks"][wid] += 1                # single-writer-per-slot (race-free)


def worker(H, wid, rng, state):
    """Each fiber runs the single-owner bigint round-trip oracle in a sustained
    inner loop so many fibers overlap their digit-array walks across yields."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            int_check(H, wid, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-wid non-vacuity tally: ONE slot per worker (single writer -> race-free),
    # allocated here where H.funcs is known.
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    ichecks = sum(H.state["checks"])
    H.log("bigint round-trip conservation: {0} single-owner closed-world checks "
          "(all 7 identities passed fail-fast); ops={1}".format(
              ichecks, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(ichecks > 0,
            "no bigint round-trip checks ran -- the load-bearing digit-array "
            "round-trip hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a C digit
    # walk on a corrupted object, or SIGSEGV mid round-trip).
    H.require_no_lost("bigint round-trip conservation")


if __name__ == "__main__":
    harness.main(
        "p534_int_bytes_roundtrip_conservation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="a large Python int is a variable-length C digit array; to_bytes/"
                 "from_bytes/multiply/divide/bit_length walk it digit-by-digit in "
                 "C.  LOAD-BEARING: each fiber owns a private bigint N and asserts "
                 "seven exact closed-world identities across a yield -- big/little/"
                 "signed byte round-trips, (N*M)//M==N, stable bit_length/popcount, "
                 "and value identity vs a baseline captured before the yield.  N is "
                 "single-owner immutable, so any deviation is a torn/corrupted "
                 "PyLong from a hub migration or lost/dup wake, NOT documented "
                 "shared-object semantics.  Includes a negative int (signed path) "
                 "and values crossing the [-5,256] small-int cache boundary")
