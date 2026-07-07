"""big_100 / 605 -- stringprep RFC-3454 table PURITY under M:N.

stringprep is a module of PURE functions implementing the RFC 3454 lookup
tables used by SASLprep/nameprep.  Every public name is a total function of a
SINGLE Unicode character:

  * the 17 membership predicates  in_table_a1 / b1 / c11 / c11_c12 / c12 /
    c21 / c21_c22 / c22 / c3 / c4 / c5 / c6 / c7 / c8 / c9 / d1 / d2  each
    return a bool ("is this character in RFC-3454 table X?");
  * the two mapping functions  map_table_b2(ch) (case-fold + NFKC, the B.2
    nameprep mapping) and map_table_b3(ch) (B.3 case-fold) each return a str.

They are deterministic and stateless: the answer for a character NEVER depends
on any prior call, on wall-clock, on which thread/hub asks, or on what any other
character mapped to.  Internally the predicates call unicodedata.category() and
bisect over module-level range tuples, and map_table_b2 chains
unicodedata.normalize("NFKC", ...) around map_table_b3 -- all reads of shared,
never-mutated C tables / module globals.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
CPython 3.14t with the GIL OFF and tens of thousands of goroutines multiplexed
across >1 hubs, a stringprep call chain runs partly in the C unicodedata
extension and partly in Python (the bisect walks, the string join in
map_table_b2).  If runloom's preemption or hub migration were to (a) corrupt a
fiber's Python frame / operand stack mid-chain, (b) let a sibling's computation
leak a value into this fiber's locals across a yield, or (c) return a torn
str from the C normalize path, then a purely functional lookup would return a
DIFFERENT answer for the SAME input across a yield -- an impossibility on a
correct runtime.  There is no shared mutable state to serialize: every input is
a fiber-local character and the RFC-3454 tables are immutable, so ANY divergence
is a runtime corruption, not documented Python semantics.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  A pure function evaluated twice on the
same fiber-local input MUST return bit-identical results, and MUST equal the
value computed single-threaded before any fibers existed.  We freeze a GOLDEN
profile table in setup() (single-threaded, in the root): for a fixed UNIVERSE of
codepoints, golden[cp] = (tuple-of-17-bools, map_table_b2(ch), map_table_b3(ch)).
The golden table is immutable and read-only-shared thereafter.  Each fiber then
recomputes the profile for its OWN fiber-local sample of codepoints, holds the
result in a single-owner local, YIELDS (so siblings interleave their own C
unicodedata calls on the same hub), recomputes, and asserts:

  * self-consistency: the profile computed AFTER the yield is bit-identical to
    the one computed BEFORE it (same bools, same mapped strings) -- a pure
    function did not change its answer across a cooperative yield + possible
    hub migration;
  * closed-form correctness: the profile equals golden[cp] exactly -- the
    fiber's answer matches the reference computed with no concurrency at all.

Both are single-owner (the sample list, the two profile dicts, and the golden
lookups are never written by anyone but this fiber / are immutable), so a
mismatch cannot be the documented "shared mutable object races" behavior -- it
would be a torn C result, a corrupted frame, or a cross-fiber local leak: a real
runloom bug.

ORACLES:
  * LOAD-BEARING -- PURITY / CLOSED-FORM (worker, HARD, fail-fast).  Per the two
    checks above, on fiber-local codepoints, across a yield, vs the golden table.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (profiles > 0),
    counted race-free in a per-wid slot (one writer per slot).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    unicodedata call / bisect (parked-then-vanished) never returns; caught here.

FAIL ON: a stringprep predicate flipping its bool, or a map_table_b2/b3 result
changing string value/length, across a yield on a fixed fiber-local input, or
disagreeing with the single-threaded golden reference -- i.e. a pure RFC-3454
lookup that is not pure under M:N.  There is no shared-mutable MEASURED arm
because stringprep exposes NO mutable state: the hazard is purely torn
computation, which the single-owner oracle covers directly.

Stresses: unicodedata.category() / normalize("NFKC") in the C extension called
concurrently across hubs, Python-level bisect table walks and str join in
map_table_b2, operand-stack / frame integrity across preemption mid-call-chain,
cross-fiber locals isolation across a yield, torn C str results.
"""
import stringprep

import harness
import runloom

# The 17 RFC-3454 membership predicates, in a FIXED order so the bool tuple is a
# stable positional fingerprint of a character.
BOOL_TABLE_NAMES = (
    "in_table_a1", "in_table_b1", "in_table_c11", "in_table_c11_c12",
    "in_table_c12", "in_table_c21", "in_table_c21_c22", "in_table_c22",
    "in_table_c3", "in_table_c4", "in_table_c5", "in_table_c6",
    "in_table_c7", "in_table_c8", "in_table_c9", "in_table_d1", "in_table_d2",
)
BOOL_FNS = tuple(getattr(stringprep, n) for n in BOOL_TABLE_NAMES)
MAP_B2 = stringprep.map_table_b2
MAP_B3 = stringprep.map_table_b3

# A curated UNIVERSE of codepoints that exercises every interesting corner of
# the RFC-3454 tables and the B.2/B.3 mapping (case-fold + NFKC): ASCII, Latin-1
# case-fold, the sharp-S / final-sigma expansions, dotted-I, ligatures, angstrom
# / composed-vs-decomposed, Roman numerals, full-width forms, super/subscripts,
# no-break / zero-width / line/para-separator spaces, soft hyphen, combining
# marks, bidi (Arabic/Hebrew) chars, control chars, non-characters, tag chars,
# and astral emoji.  Every one is a valid, non-surrogate scalar so map_table_b2
# never raises.  Plus a swept ASCII+Latin range so each fiber does real work.
_CURATED = (
    0x0000, 0x0009, 0x0020, 0x007F, 0x0041, 0x0061, 0x005A, 0x007A,
    0x0030, 0x0039, 0x00A0, 0x00AD, 0x00B2, 0x00C5, 0x00DF, 0x0100,
    0x0130, 0x0131, 0x0301, 0x03A3, 0x03C2, 0x0392, 0x0410, 0x0660,
    0x05BE, 0x05D0, 0x0627, 0x180E, 0x1E9E, 0x1F600, 0x2000, 0x200B,
    0x2028, 0x2029, 0x2060, 0x2065, 0x2160, 0x212B, 0x2126, 0xFB00,
    0xFB01, 0xFDD0, 0xFEFF, 0xFF21, 0xFF41, 0x3000, 0xE0001, 0x00B5,
)
UNIVERSE = tuple(dict.fromkeys(  # de-dup, keep order; add a swept range
    _CURATED + tuple(range(0x20, 0x0180))))
UNIVERSE_LEN = len(UNIVERSE)

# Codepoints sampled per profile pass.  Big enough that a torn result somewhere
# in the chain is likely to be hit, small enough that many passes complete under
# the timeout.  Each fiber draws its OWN fiber-local sample every pass.
SAMPLE = 48

# Sustained passes per worker, bounded by H.running().  The torn-computation
# hazard only shows under SUSTAINED churn: many fibers running C unicodedata /
# NFKC call chains simultaneously, each PARKED across its yield so the scheduler
# reliably interleaves a sibling's chain before this fiber resumes.
INNER_CAP = 100000


def profile(ch):
    """PURE positional fingerprint of a single character: (17 bools, b2, b3).

    A total, deterministic function of `ch` -- no shared mutable state, no order
    dependence.  Must return the same value on every call, on every hub."""
    bools = tuple(f(ch) for f in BOOL_FNS)
    return (bools, MAP_B2(ch), MAP_B3(ch))


def profile_pass(H, wid, rng, state, golden):
    """One single-owner purity pass.

    Draw a fiber-local sample of codepoints, compute each one's profile into a
    single-owner baseline, YIELD so siblings interleave their own C unicodedata
    chains, recompute, and assert bit-identical + equal to the golden reference.
    A mismatch is a torn / cross-fiber-leaked pure computation -- a runtime bug."""
    # Fiber-local sample (indices into UNIVERSE), never shared.
    idxs = [rng.randrange(UNIVERSE_LEN) for _ in range(SAMPLE)]
    cps = [UNIVERSE[i] for i in idxs]

    # Baseline: compute BEFORE the yield, hold single-owner.
    baseline = [profile(chr(cp)) for cp in cps]

    # YIELD: let siblings run their own unicodedata/NFKC chains on this hub.
    runloom.yield_now()
    if idxs[0] & 1:
        runloom.sleep(0.0002)

    # Recompute and verify self-consistency + closed-form correctness.
    for j in range(SAMPLE):
        cp = cps[j]
        ch = chr(cp)
        now = profile(ch)
        base = baseline[j]
        gold = golden[cp]

        # Self-consistency: pure function unchanged across the yield.
        if now[0] != base[0]:
            H.fail("stringprep predicate flipped across a yield for U+{0:04X}: "
                   "before={1} after={2} (wid {3}) -- an RFC-3454 membership "
                   "bool changed on a fixed fiber-local input: a torn C "
                   "unicodedata.category / corrupted frame under M:N".format(
                       cp, base[0], now[0], wid))
            return
        if now[1] != base[1]:
            H.fail("map_table_b2 changed across a yield for U+{0:04X}: "
                   "before={1!r} after={2!r} (wid {3}) -- a pure NFKC case-fold "
                   "mapping returned a different string on the same input: torn "
                   "C normalize result or cross-fiber local leak under M:N".format(
                       cp, base[1], now[1], wid))
            return
        if now[2] != base[2]:
            H.fail("map_table_b3 changed across a yield for U+{0:04X}: "
                   "before={1!r} after={2!r} (wid {3}) -- a pure B.3 case-fold "
                   "mapping was not stable across a cooperative yield".format(
                       cp, base[2], now[2], wid))
            return

        # Closed-form: equals the single-threaded golden reference.
        if now != gold:
            H.fail("stringprep profile disagrees with single-threaded GOLDEN for "
                   "U+{0:04X}: got (bools={1}, b2={2!r}, b3={3!r}) expected "
                   "(bools={4}, b2={5!r}, b3={6!r}) (wid {7}) -- the M:N answer "
                   "diverged from the no-concurrency reference".format(
                       cp, now[0], now[1], now[2],
                       gold[0], gold[1], gold[2], wid))
            return

    # Race-free non-vacuity: ONE writer per wid slot (see p405 HARD RULE 1).
    state["profiles"][wid] += 1
    state["chars"][wid] += SAMPLE


def worker(H, wid, rng, state):
    golden = state["golden"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            profile_pass(H, wid, rng, state, golden)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # GOLDEN reference: computed single-threaded, in the root, before any fiber
    # exists.  Immutable + read-only-shared thereafter -- the no-concurrency
    # answer every fiber must reproduce.
    golden = {}
    for cp in UNIVERSE:
        golden[cp] = profile(chr(cp))
    H.state = {
        "golden": golden,               # immutable reference table (read-only)
        "profiles": [0] * H.funcs,      # per-wid pass count (one writer/slot)
        "chars": [0] * H.funcs,         # per-wid chars profiled (one writer/slot)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    passes = sum(H.state["profiles"])
    chars = sum(H.state["chars"])
    H.log("stringprep PURITY [single-owner LOAD-BEARING]: {0} profile passes, "
          "{1} character profiles verified bit-identical across a yield AND "
          "equal to the single-threaded golden reference (all fail-fast); "
          "universe={2} codepoints; ops={3}".format(
              passes, chars, UNIVERSE_LEN, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(passes > 0,
            "no stringprep purity passes ran -- the torn-computation hazard was "
            "never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # unicodedata.category / normalize call).
    H.require_no_lost("stringprep purity")


if __name__ == "__main__":
    harness.main(
        "p605_stringprep_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="stringprep exposes only PURE functions of a single character: "
                 "17 RFC-3454 membership predicates (bool) and map_table_b2/b3 "
                 "(NFKC case-fold, str).  They are deterministic and stateless. "
                 "LOAD-BEARING: each fiber recomputes the profile (17 bools + 2 "
                 "mapped strings) for its own fiber-local codepoints, holds it "
                 "single-owner, yields so siblings interleave their C unicodedata "
                 "chains, recomputes, and asserts the result is bit-identical "
                 "across the yield AND equal to a single-threaded golden table. "
                 "A predicate flipping or a mapped string changing on a fixed "
                 "input is a torn/leaked pure computation -- a runloom bug. No "
                 "MEASURED arm: stringprep has no mutable state to race")
