"""big_100 / 577 -- keyword.iskeyword / issoftkeyword PURITY under M:N.

The `keyword` module is the thinnest kind of stdlib surface: two immutable
module constants (`kwlist`, the 35 hard keywords; `softkwlist`, the soft
keywords `_ case match type`) and two pure predicates built directly over
them --

    keyword.iskeyword     is frozenset(kwlist).__contains__
    keyword.issoftkeyword is frozenset(softkwlist).__contains__

Both are BOUND METHODS of a module-global frozenset created ONCE at import.
A predicate call is a single C-level `frozenset.__contains__(s)`: it hashes
`s`, probes the frozen table, and returns a bool.  The frozenset is never
mutated (frozensets are immutable and the module rebinds nothing after
import), so every call is a PURE function of its argument -- for a fixed `s`
the answer is a mathematical constant, identical on every hub, every fiber,
every time.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom drives tens
of thousands of goroutines across hubs>1 with the GIL OFF.  Every fiber calls
into the SAME module-global frozenset's `__contains__` concurrently.  A pure
read of a shared immutable set MUST be race-free -- but that is exactly the
property under test: if the frozen table's probe sequence, the argument's hash
cache, or the returned bool were somehow perturbed by a concurrent probe on a
different hub (a torn read of the shared table, a mis-scheduled resume landing
in the wrong frame, a bool object mixed up across fibers), a fiber could
observe iskeyword("if") == False or iskeyword("zzz") == True.  Because the
inputs are fiber-local and the closed-form answer is known exactly, any such
perturbation is caught deterministically.

SINGLE-OWNER / CLOSED-FORM ORACLE (verified against the module semantics):

  The load-bearing oracle is a PURITY / IDENTITY law over FIBER-LOCAL inputs.
  At import we snapshot the ground truth ONCE into two module-level frozensets
  (EXPECTED_HARD, EXPECTED_SOFT) -- immutable, read-only, so reading them from
  any fiber is race-free exactly like reading any constant.  A candidate string
  `s` has a CLOSED-FORM answer:

        iskeyword(s)      == (s in EXPECTED_HARD)
        issoftkeyword(s)  == (s in EXPECTED_SOFT)

  Each fiber owns a private, shuffled list of candidates (real hard keywords,
  real soft keywords, near-miss non-keywords like "IF"/"iff"/"return_"/" if",
  and random identifiers -- none of these lists is shared or mutated).  For
  each candidate the fiber:
    - computes r1_hard = keyword.iskeyword(s), r1_soft = keyword.issoftkeyword(s)
      BEFORE a yield;
    - YIELDS (runloom.yield_now / a tiny sleep) so siblings on other hubs hammer
      the same shared frozensets in the meantime;
    - recomputes r2_hard / r2_soft AFTER the yield;
    - asserts r1 == r2 (stable across the yield -- a pure function does not
      change answer while parked) AND r1 == the CLOSED-FORM expected (the module
      predicate agrees with `s in EXPECTED_*`).
  Every value compared is a fiber-local bool derived from a fiber-local string;
  the only shared objects touched are the immutable frozensets and the immutable
  candidate literals -- no shared mutable container reaches the oracle, so a
  mismatch cannot be documented shared-object behavior.  It can only be a torn
  read of the frozen table, a cross-fiber value/identity mix-up, or a lost/
  misrouted resume -- i.e. a real runtime bug.

  We also assert a self-consistency law the module documents: hard and soft
  keyword sets are DISJOINT, so no candidate is ever BOTH iskeyword and
  issoftkeyword true at once.

FAIL ON: keyword.iskeyword / issoftkeyword returning a result that (a) differs
across a yield for the same fiber-local input, (b) disagrees with the closed-
form `s in EXPECTED_*`, or (c) claims a string is simultaneously a hard AND a
soft keyword.  Any of these is a runtime purity break, not a Python semantic.

NON-VACUITY (post): the load-bearing arm actually ran (checks > 0).
COMPLETENESS (post): require_no_lost -- no fiber parked-then-vanished mid-probe.

Stresses: concurrent pure reads of a shared module-global frozenset's C
`__contains__` across hub migration + yield, argument hashing under GIL-off
contention, bool-result identity across a park/resume, closed-form predicate
purity over fiber-local inputs.

Good TSan / controlled-M:N-replay target: many hubs probe ONE shared frozenset's
backing table simultaneously; a data-race report on that immutable table object,
or a replayed probe that returns the wrong membership mid another fiber's probe,
localizes any real read hazard before the closed-form law even closes.
"""
import keyword

import harness
import runloom

# Ground truth snapshotted ONCE at import into immutable frozensets.  These are
# read-only for the whole run -- reading them from any fiber is race-free (an
# immutable object, like any module constant).  The closed-form oracle compares
# the module predicates against membership in these.
EXPECTED_HARD = frozenset(keyword.kwlist)
EXPECTED_SOFT = frozenset(keyword.softkwlist)

# Non-keyword near-misses: strings that LOOK keyword-ish but are not keywords,
# to exercise the "must return False" side of both predicates (case variants,
# truncations, trailing underscore, surrounding whitespace, dunder-ish, empty).
NEAR_MISSES = (
    "", " ", "IF", "If", "iff", "clas", "returnn", "return_", "_return",
    "els", "elsee", "whilst", "fro", "importt", "lambda_", "asyncio",
    "awaitable", "Match", "CASE", "typing", "Type", "none", "TRUE", "false",
    " if", "if ", "\tif", "def\n", "__init__", "__match_args__", "self",
    "foo", "bar", "baz", "qux", "x", "y", "value", "result", "counter",
)


def build_candidates(rng):
    """Build one fiber's PRIVATE, shuffled candidate list.

    A mix of every real hard keyword, every real soft keyword, the fixed near-
    miss pool, and a handful of random lowercase identifiers.  Fiber-local and
    never shared/mutated, so the closed-form oracle over it is single-owner."""
    cands = list(keyword.kwlist)           # a fresh private copy (not the module list)
    cands.extend(keyword.softkwlist)
    cands.extend(NEAR_MISSES)
    # A few random identifier-ish strings; overwhelmingly non-keywords, and the
    # closed-form check handles the rare collision correctly either way.
    alpha = "abcdefghijklmnopqrstuvwxyz_"
    for _ in range(8):
        n = rng.randint(1, 7)
        cands.append("".join(alpha[rng.randrange(len(alpha))] for _ in range(n)))
    rng.shuffle(cands)
    return cands


# Sustained probes per worker, bounded by H.running().  A single probe per fiber
# barely overlaps a sibling's; sustained churn keeps many fibers simultaneously
# probing the shared frozensets while parked across their yield so the scheduler
# reliably interleaves a sibling's probe before this fiber resumes.
INNER_CAP = 100000


def purity_check(H, wid, cands, state):
    """One pass of the closed-form purity oracle over a fiber's private candidate
    list.  For each candidate: predicate result must be stable across a yield and
    equal to the closed-form `s in EXPECTED_*`; hard and soft sets are disjoint."""
    # BEFORE the yield: snapshot every predicate result for this fiber's inputs.
    before_hard = []
    before_soft = []
    for s in cands:
        before_hard.append(keyword.iskeyword(s))
        before_soft.append(keyword.issoftkeyword(s))

    # YIELD: park so siblings on other hubs hammer the same shared frozensets.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    # AFTER the yield: recompute and enforce the three laws.
    for i, s in enumerate(cands):
        r2_hard = keyword.iskeyword(s)
        r2_soft = keyword.issoftkeyword(s)
        exp_hard = s in EXPECTED_HARD
        exp_soft = s in EXPECTED_SOFT

        # Law 1: STABLE across the yield (pure function, no answer change parked).
        if r2_hard != before_hard[i]:
            H.fail("keyword.iskeyword({0!r}) CHANGED across a yield: {1} -> {2} "
                   "(wid {3}) -- a pure predicate over an immutable frozenset must "
                   "return the same answer before and after a park/resume; a change "
                   "is a torn read of the shared frozen table or a misrouted "
                   "resume".format(s, before_hard[i], r2_hard, wid))
            return
        if r2_soft != before_soft[i]:
            H.fail("keyword.issoftkeyword({0!r}) CHANGED across a yield: {1} -> {2} "
                   "(wid {3}) -- pure predicate answer changed while parked".format(
                       s, before_soft[i], r2_soft, wid))
            return

        # Law 2: matches the CLOSED-FORM ground truth.
        if r2_hard != exp_hard:
            H.fail("keyword.iskeyword({0!r}) == {1} but closed-form (s in "
                   "EXPECTED_HARD) == {2} (wid {3}) -- the module predicate "
                   "disagrees with the immutable ground-truth set: a wrong-membership "
                   "read of the shared frozenset under M:N".format(
                       s, r2_hard, exp_hard, wid))
            return
        if r2_soft != exp_soft:
            H.fail("keyword.issoftkeyword({0!r}) == {1} but closed-form (s in "
                   "EXPECTED_SOFT) == {2} (wid {3}) -- module predicate disagrees "
                   "with ground-truth soft-keyword set".format(
                       s, r2_soft, exp_soft, wid))
            return

        # Law 3: hard and soft keyword universes are DISJOINT (module invariant).
        if r2_hard and r2_soft:
            H.fail("keyword purity break: {0!r} reported as BOTH a hard keyword "
                   "AND a soft keyword (wid {1}) -- the two frozensets are disjoint "
                   "by construction; a simultaneous true is a cross-set torn "
                   "read".format(s, wid))
            return

    state["checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber builds its OWN private candidate list once, then runs the
    closed-form purity oracle over it in a sustained inner loop so many fibers
    probe the shared frozensets simultaneously while parked across the yield."""
    cands = build_candidates(rng)
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, cands, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # checks[] is a sharded NON-VACUITY tally only (wid & 1023) -- it feeds no
    # conservation law, so aliasing is harmless; it just proves the oracle ran.
    H.state = {
        "checks": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("keyword purity: {0} closed-form probe passes (each pass checked every "
          "hard+soft keyword, near-miss, and random candidate stable-across-yield "
          "and equal to the immutable ground truth); ops={1}".format(
              checks, H.total_ops()))
    # NON-VACUITY: the load-bearing purity arm actually ran.
    H.check(checks > 0,
            "no keyword-purity probe passes ran -- the closed-form predicate "
            "oracle was never exercised (would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-probe.
    H.require_no_lost("keyword purity")


if __name__ == "__main__":
    harness.main(
        "p577_keyword_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="keyword.iskeyword / issoftkeyword are pure predicates over two "
                 "immutable module-global frozensets (kwlist / softkwlist).  Under "
                 "M:N tens of thousands of fibers probe the SAME shared frozensets' "
                 "C __contains__ concurrently.  LOAD-BEARING closed-form oracle: "
                 "each fiber owns a private shuffled candidate list (hard+soft "
                 "keywords, near-misses, random ids) and asserts every predicate "
                 "result is stable across a yield AND equal to membership in the "
                 "import-time ground-truth frozensets, and that hard/soft sets stay "
                 "disjoint.  A result that changes across a yield, disagrees with "
                 "the closed form, or claims a string is both hard and soft, is a "
                 "torn-read / misrouted-resume runtime bug")
