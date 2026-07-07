"""big_100 / 508 -- math big-integer purity across a yield under M:N.

The math module's big-integer routines -- factorial(), comb(), perm(), prod(),
isqrt(), gcd(), lcm() -- build their results in an internal C accumulator /
scratch buffer (a mpz-style limb array or a temporary PyLong that grows as the
partial product / binomial is assembled).  Those C routines run to completion
inside a single call, but the QUESTION this program probes is whether the pygo
M:N runtime keeps them PURE across a cooperative yield boundary:

  A fiber computes a big-int result (say factorial(300), a ~600-digit PyLong),
  snapshots it, and then YIELDS.  A sibling fiber -- possibly on a different hub
  -- runs its OWN factorial/comb/prod churn while this fiber is parked.  If the
  runtime ever leaked a partial value out of a C scratch buffer, reused a torn
  accumulator across fibers, or corrupted this fiber's snapshot PyLong while it
  was parked, then re-computing the SAME expression after the yield would return
  a DIFFERENT big integer.  Because these functions are mathematically pure and
  the inputs are fiber-local constants, ANY change across the yield is a runtime
  bug, not a documented Python semantic.

WHERE M:N COULD BREAK IT (the gap this program probes).  math.factorial and
friends allocate large PyLong temporaries; a stackful-coroutine context switch
mid-way (preemption) or a cross-hub migration must fully preserve the C call's
in-flight state and the resulting PyLong.  A lost-wakeup that resumed the fiber
with a stale register/stack frame, or a scratch-buffer aliasing bug that let a
sibling's factorial overwrite this fiber's accumulator, would surface as a big
integer that no longer satisfies its defining identity -- or that simply differs
from the value the SAME pure call produced one line earlier.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Pure single-owner integer-identity laws.  Each fiber derives its OWN inputs
  n, k, a, b from H.derive(wid, idx) -- fiber-local constants, never shared.  It
  computes six big-integer results, snapshots each as a Python int (a value, so
  the snapshot is an independent immutable object), YIELDS so siblings interleave
  their own math churn, then RE-COMPUTES each result and asserts:

    (1) the re-computed value is BIT-IDENTICAL to the pre-yield snapshot
        (purity: the same pure call with the same inputs must return the same
        integer -- a difference means the runtime corrupted the value or the
        call's scratch state across the park), AND
    (2) the value satisfies its closed-form mathematical identity:
          comb(n,k)      == comb(n-1,k-1) + comb(n-1,k)     (Pascal's rule)
          factorial(n)   == n * factorial(n-1)
          perm(n,k)      == factorial(n) // factorial(n-k)
          isqrt(m)**2 <= m < (isqrt(m)+1)**2                (integer sqrt bracket)
          gcd(a,b)*lcm(a,b) == abs(a*b)                     (gcd-lcm product law)
          prod(xs)       == reduce(mul, xs)                 (accumulator vs manual)

  Both arms are load-bearing: (1) catches a value that CHANGED across the yield
  (the M:N purity hazard directly); (2) catches a value that is internally WRONG
  the moment it is produced (a torn accumulator that yields a plausible-but-wrong
  big int, which arm (1) might miss if BOTH computations tore identically).

  These laws are exact integer equalities over fiber-local inputs with NO shared
  state whatsoever, so on a correct runtime every check passes deterministically
  (program exits 0).  A FAIL means a big-int result changed across a yield or
  violated its defining identity -- a real runtime purity/isolation bug (lost
  wakeup resuming a stale frame, cross-fiber scratch leak, torn PyLong), never a
  documented Python semantic.

ORACLES:
  * LOAD-BEARING -- BIG-INT PURITY + IDENTITY (worker, HARD, fail-fast).  Six
    exact integer laws recomputed across a yield; single-owner fiber-local
    inputs; bit-identical pre/post AND closed-form identity.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-call
    (parked inside a C factorial that never returned) never completes; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (math_checks>0).

FAIL ON: a re-computed big-int differing from its pre-yield snapshot, or any of
the six closed-form identities not holding.  There is NO shared-mutable arm here
-- the target is a pure numeric library, so the whole program is single-owner and
every observation is load-bearing.

Stresses: math.factorial / comb / perm / prod big-integer C accumulators across
a cooperative yield + hub migration, PyLong snapshot preservation while a fiber
is parked, isqrt / gcd / lcm exact-integer paths under sustained M:N churn.

Good TSan / controlled-M:N-replay target: the large PyLong temporaries built
inside factorial/comb are allocated and freed per call; a use-after-free or
cross-fiber alias on that scratch under replay would surface as a torn re-compute
before the identity law even closes.
"""
import functools
import math
import operator

import harness
import runloom

# Input bands.  n is kept in a range where factorial(n)/comb(n,k)/perm(n,k) are
# genuinely multi-limb big integers (factorial(64) already exceeds 2**296) so the
# C accumulator path is exercised, yet small enough that thousands of fibers each
# do many checks under the timeout.  a, b span a wide range so gcd/lcm are
# non-trivial and isqrt operates on a large radicand.
N_MIN = 64
N_MAX = 384
AB_MAX = 1 << 60                          # gcd/lcm/isqrt operands up to ~10**18
PROD_LEN_MIN = 2
PROD_LEN_MAX = 8
PROD_ELT_MAX = 1 << 20


def derive_inputs(rng):
    """Draw the fiber-local, per-idx constant inputs for the six laws.  Pure
    values -- never shared, so any cross-yield change is a runtime bug."""
    n = rng.randint(N_MIN, N_MAX)
    k = rng.randint(1, n - 1)             # 1 <= k <= n-1 keeps Pascal's rule valid
    a = rng.randint(0, AB_MAX)
    b = rng.randint(0, AB_MAX)
    m = rng.randint(0, AB_MAX)            # isqrt radicand
    xs = tuple(rng.randint(1, PROD_ELT_MAX)
               for _ in range(rng.randint(PROD_LEN_MIN, PROD_LEN_MAX)))
    return n, k, a, b, m, xs


def compute_all(n, k, a, b, m, xs):
    """Compute the six big-int results for the given fiber-local inputs.  Returns
    a tuple of Python ints -- immutable value snapshots, independent of any C
    scratch buffer once returned."""
    c = math.comb(n, k)
    f = math.factorial(n)
    p = math.perm(n, k)
    s = math.isqrt(m)
    g = math.gcd(a, b)
    l = math.lcm(a, b)
    pr = math.prod(xs)
    return (c, f, p, s, g, l, pr)


def check_identities(H, wid, n, k, a, b, m, xs, vals):
    """Assert the six closed-form integer identities on `vals`.  Each is an exact
    equality over fiber-local inputs -- a violation means the big-int result was
    internally torn/wrong the moment it was produced.  Returns False on failure."""
    c, f, p, s, g, l, pr = vals

    # Pascal's rule: comb(n,k) == comb(n-1,k-1) + comb(n-1,k).
    if c != math.comb(n - 1, k - 1) + math.comb(n - 1, k):
        H.fail("comb identity broken: comb({0},{1})={2} != comb(n-1,k-1)+"
               "comb(n-1,k) (wid {3}) -- torn binomial accumulator".format(
                   n, k, c, wid))
        return False

    # Factorial recurrence: factorial(n) == n * factorial(n-1).
    if f != n * math.factorial(n - 1):
        H.fail("factorial recurrence broken: factorial({0})={1} != {0}*"
               "factorial({2}) (wid {3}) -- torn factorial accumulator".format(
                   n, f, n - 1, wid))
        return False

    # perm/factorial law: perm(n,k) == factorial(n) // factorial(n-k).
    if p != math.factorial(n) // math.factorial(n - k):
        H.fail("perm identity broken: perm({0},{1})={2} != factorial(n)//"
               "factorial(n-k) (wid {3}) -- torn perm accumulator".format(
                   n, k, p, wid))
        return False

    # isqrt bracket: s**2 <= m < (s+1)**2.
    if not (s * s <= m < (s + 1) * (s + 1)):
        H.fail("isqrt bracket broken: isqrt({0})={1} but bracket "
               "{1}**2 <= {0} < ({1}+1)**2 does not hold (wid {2}) -- torn "
               "isqrt result".format(m, s, wid))
        return False

    # gcd-lcm product law: gcd(a,b)*lcm(a,b) == abs(a*b).
    if g * l != abs(a * b):
        H.fail("gcd/lcm law broken: gcd({0},{1})*lcm(a,b)={2} != abs(a*b)={3} "
               "(wid {4}) -- torn gcd or lcm result".format(a, b, g * l,
                                                            abs(a * b), wid))
        return False

    # prod vs manual reduce.
    manual = functools.reduce(operator.mul, xs, 1)
    if pr != manual:
        H.fail("prod law broken: math.prod(xs)={0} != reduce(mul,xs)={1} "
               "(wid {2}) -- torn product accumulator".format(pr, manual, wid))
        return False

    return True


# Sustained checks per worker, bounded by H.running().  The purity hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously building big-int
# accumulators while PARKED across their yield, so the scheduler reliably
# interleaves a sibling's factorial/comb before this fiber re-computes.
INNER_CAP = 100000


def math_check(H, wid, idx, state):
    """Single-owner big-int purity + identity check.

    Derive fiber-local inputs, compute the six results, snapshot them, YIELD so
    siblings churn their own big-int math, then re-compute and assert bit-identical
    AND closed-form-valid.  A cross-yield change or a broken identity is a runtime
    purity/isolation bug."""
    rng = H.derive(wid, idx)
    n, k, a, b, m, xs = derive_inputs(rng)

    # Pre-yield: compute + validate the identities on the first computation.
    pre = compute_all(n, k, a, b, m, xs)
    if not check_identities(H, wid, n, k, a, b, m, xs, pre):
        return

    # YIELD: park this fiber so a sibling on this or another hub runs its own
    # factorial/comb/prod churn while our snapshot ints sit in this frame.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # Post-yield: re-compute the SAME pure expressions from the SAME inputs.
    post = compute_all(n, k, a, b, m, xs)

    # Arm (1): bit-identical purity.  Each result is an exact integer; the same
    # pure call with the same fiber-local inputs MUST return the same value.
    labels = ("comb", "factorial", "perm", "isqrt", "gcd", "lcm", "prod")
    for i in range(len(pre)):
        if pre[i] != post[i]:
            H.fail("big-int PURITY VIOLATION across yield: math.{0} changed from "
                   "{1} to {2} (wid {3}, idx {4}, inputs n={5} k={6} a={7} b={8} "
                   "m={9}) -- the same pure call returned a DIFFERENT integer "
                   "after a cooperative yield, a cross-fiber scratch leak or a "
                   "stale-frame resume".format(labels[i], pre[i], post[i], wid,
                                               idx, n, k, a, b, m))
            return

    # Arm (2): re-validate the identities on the second computation (catches a
    # value that tore identically in both computations).
    if not check_identities(H, wid, n, k, a, b, m, xs, post):
        return

    state["math_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs the single-owner big-int purity oracle repeatedly.  The
    whole program is single-owner (pure numeric library, no shared state), so
    every check is load-bearing and fail-fast."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            math_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "math_checks": [0] * 1024,        # LOAD-BEARING single-owner checks (sharded tally)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    mchecks = sum(H.state["math_checks"])
    H.log("math[single-owner LOAD-BEARING]: {0} big-int purity+identity checks "
          "(all passed fail-fast); ops={1}".format(mchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing arm actually ran.
    H.check(mchecks > 0,
            "no big-int purity checks ran -- the load-bearing math-purity hazard "
            "was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a C
    # factorial that never returned).
    H.require_no_lost("math big-int purity")


if __name__ == "__main__":
    harness.main(
        "p508_math_int_identity_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="math.factorial/comb/perm/prod build big-integer results in an "
                 "internal C accumulator; under M:N a sibling running between a "
                 "fiber's compute and its across-yield re-check could corrupt "
                 "that scratch or leak a partial value.  LOAD-BEARING: each fiber "
                 "derives its OWN inputs, computes six big-int results, yields, "
                 "then re-computes and asserts bit-identical to the pre-yield "
                 "snapshot AND satisfying closed-form integer identities (Pascal's "
                 "rule, factorial recurrence, perm=fact//fact, isqrt bracket, "
                 "gcd*lcm=abs(a*b), prod=reduce).  Pure single-owner numeric "
                 "inputs, so any cross-yield change or broken identity is a real "
                 "runtime purity/isolation bug")
