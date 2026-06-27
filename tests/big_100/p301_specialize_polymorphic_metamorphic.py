"""big_100 / 301 -- adaptive specializer under polymorphic specialize<->deopt churn.

CPython 3.13's adaptive interpreter rewrites a code object's bytecode IN PLACE:
each LOAD_ATTR / BINARY_OP / CALL / FOR_ITER site grows a per-instruction inline
cache that, after a warm-up counter crosses ~256, specializes to a fast variant
keyed on the operand's exact type / the type's `tp_version_tag`.  A type-miss
forces a de-opt back to the generic form.  Free-threading guards those caches
with a code-object-level mechanism that assumes a bounded set of executing OS
threads -- but under runloom M:N the SAME code object is run concurrently by
goroutines that MIGRATE across hubs (and across OS threads) between calls.  So
the specialize/de-specialize transition races: a goroutine can read through a
HALF-WRITTEN inline cache (specialized opcode but stale operand / version tag)
and either SIGSEGV or -- the insidious case -- compute a SILENTLY WRONG result
when the de-opt guard is skipped.

We make the silent-wrong-result case the load-bearing oracle with a METAMORPHIC
closed-form check.  One hot function `hot_eval` is SHARED by every worker (so
there is exactly ONE code object whose caches all goroutines contend over).  It
is a deliberately POLYMORPHIC computation: a per-worker seeded RNG drives the
operand through int / float / Fraction / a custom-__add__ class in a churning
sequence, so each call site repeatedly specializes then de-opts.  `hot_eval`
exercises all four specialization surfaces in one body:

  * LOAD_ATTR   -- reads `.coeff` off the polymorphic operand wrapper
  * CALL        -- calls a bound method `.contribute()` on it
  * BINARY_OP   -- folds the (mixed-type) contributions with `+` / `*`
  * FOR_ITER    -- iterates a small per-call tuple

Each worker folds a deterministic 64-bit checksum over its whole op sequence and
asserts it equals a CLOSED-FORM reference recomputed in pure single-threaded
Python from the IDENTICAL seed (computed in setup(), before the parallel phase,
so it is a true single-thread-vs-M:N differential -- the run(1) baseline arm).
Floats are quantized to an integer before folding so the checksum is exactly
reproducible; the only way a worker's checksum can differ from its closed-form
reference is a mis-specialized / torn read returning a value the computation
never legitimately produced.

To MAXIMIZE the chance of a stale-cache de-opt under migration, setup() warms
`hot_eval` >WARMUP times single-threaded FIRST (where specialization is active),
so the code object is already specialized before the M:N phase starts migrating
it; workers then `yield_now()` every few iters to force a hub handoff
mid-warm-counter / mid-deopt.

Note: 3.13t currently gates the adaptive specializer OFF while >1 thread is live
(precisely to dodge this race), so the half-written cache may never form on this
known-good runtime -- and that is fine.  This program then doubles as a
CONFORMANCE sentinel: the metamorphic equality catches ANY mis-evaluation
regardless of cause, so if a future FT build re-enables per-code specialization
without accounting for migrating goroutines, THIS is the program that catches the
resulting silent wrong answer.

Invariant (post, per worker): worker_checksum[wid] == closed_form_reference[wid]
for every worker -- exact equality, no recorded reference file.  require_no_lost
for completeness.

Stresses: adaptive per-code specialization, inline-cache specialize<->deopt
churn, LOAD_ATTR/CALL/BINARY_OP/FOR_ITER caches, tp_version_tag invalidation,
cross-hub code-object sharing, preempt/migration mid-warm-counter.

Good TSan / controlled-M:N-replay target: the specialize-vs-execute ordering is a
pure memory-ordering race on the code object's cache words; a data-race report on
the inline-cache write/read is often the first signal, before the checksum
mismatch even fires.
"""
import random
from fractions import Fraction

import harness
import runloom

WARMUP = 600              # > 256 so each call site in hot_eval specializes
OPS_PER_ROUND = 800       # checksum-folding hot-loop iterations per round
YIELD_EVERY = 17          # force a hub handoff mid-warm-counter / mid-deopt
MASK64 = (1 << 64) - 1
SCALE = 1000              # float -> int quantization factor for a stable fold


class Lin(object):
    """A custom operand with its own __add__/__mul__ and a LOAD_ATTR target.

    Its presence in the operand stream is what keeps BINARY_OP / LOAD_ATTR / CALL
    from settling on a single specialized type -- it forces the de-opt path."""
    __slots__ = ("coeff",)

    def __init__(self, coeff):
        self.coeff = coeff

    def contribute(self):
        return self.coeff * 2 - 1

    def __add__(self, other):
        o = other.coeff if isinstance(other, Lin) else other
        return Lin(self.coeff + o)

    def __radd__(self, other):
        o = other.coeff if isinstance(other, Lin) else other
        return Lin(o + self.coeff)


def quant(v):
    """Map any numeric (int / float / Fraction / Lin) to a stable nonneg int so
    the checksum fold is bit-exact and float drift never perturbs it."""
    if isinstance(v, Lin):
        v = v.coeff
    return int(round(float(v) * SCALE)) & 0xFFFFFFFFFFFF


def make_operand(kind, n):
    """Build the polymorphic operand for this step.  Four distinct types cycle
    through the SAME call sites, driving specialize->deopt churn."""
    if kind == 0:
        return n                      # int
    if kind == 1:
        return n / 7.0                # float
    if kind == 2:
        return Fraction(n + 1, (n % 8) + 1)   # Fraction
    return Lin(n)                     # custom __add__ / LOAD_ATTR / CALL target


def hot_eval(operand, kind, tup):
    """The single SHARED hot function -- every worker (and the baseline) runs
    THIS code object, so all its inline caches are contended across hubs.

    Exercises four specialization surfaces in one body and returns a single
    numeric value the caller quantizes into its checksum.  Pure / deterministic:
    given (operand, kind, tup) it always returns the same value, so any worker
    whose folded checksum diverges from the closed-form reference observed a
    mis-specialized read."""
    # FOR_ITER over a small tuple, BINARY_OP (+) folding mixed-type terms.
    s = 0
    for t in tup:
        s = s + t                      # BINARY_OP +  (int<->int hot path)
    if kind == 3:
        # LOAD_ATTR (.coeff) + CALL (.contribute) on the custom type.
        c = operand.coeff              # LOAD_ATTR
        m = operand.contribute()       # CALL bound method
        return c * 3 + m + s           # BINARY_OP * and +
    # int / float / Fraction operand: BINARY_OP * then + with the folded tuple.
    return operand * 3 + s             # BINARY_OP * (polymorphic) and +


def fold_stream(seed, n_ops):
    """Run the seeded polymorphic op-stream through hot_eval and fold a 64-bit
    checksum.  Called BOTH single-threaded in setup() (the closed-form / run(1)
    baseline) AND inside each worker under M:N -- the two MUST agree."""
    prng = random.Random(seed)
    acc = 0
    for i in range(n_ops):
        kind = prng.randrange(4)
        n = prng.randrange(1000)
        operand = make_operand(kind, n)
        # A tiny per-call tuple feeds FOR_ITER + BINARY_OP.
        tup = (n & 7, (n >> 3) & 7, (n >> 6) & 7)
        v = hot_eval(operand, kind, tup)
        acc = (acc * 1000003 + quant(v) + kind) & MASK64
    return acc


def reader(H, wid, rng, state):
    """Fold this worker's seeded polymorphic stream under M:N, yielding often to
    force hub migration mid-specialization, and assert the checksum matches the
    pre-computed closed-form reference for the same seed."""
    seeds = state["seeds"]
    refs = state["refs"]
    got = state["got"]
    slot = wid & 1023
    seed = seeds[wid]
    expected = refs[wid]
    rno = 0
    for _ in H.round_range():
        rno += 1
        # Fold the IDENTICAL seeded polymorphic stream under M:N, yielding often
        # to force a hub handoff mid-specialization.  A single fold is short and
        # MUST run to completion to be a valid checksum, so the inner loop does
        # not break on the deadline -- shutdown is honoured at the round boundary
        # via round_range().  The per-seed result is invariant, so this must
        # reproduce the closed-form reference `expected` exactly.
        prng = random.Random(seed)
        acc = 0
        for i in range(OPS_PER_ROUND):
            kind = prng.randrange(4)
            n = prng.randrange(1000)
            operand = make_operand(kind, n)
            tup = (n & 7, (n >> 3) & 7, (n >> 6) & 7)
            v = hot_eval(operand, kind, tup)
            acc = (acc * 1000003 + quant(v) + kind) & MASK64
            if (i % YIELD_EVERY) == 0:
                runloom.yield_now()    # force a hub handoff mid-warm/deopt
            H.op(wid)
        if acc != expected:
            H.fail("worker {0} round {1}: M:N checksum {2:#x} != closed-form "
                   "single-thread reference {3:#x} -- a mis-specialized / torn "
                   "inline-cache read produced a value the computation never made"
                   .format(wid, rno, acc, expected))
            return
        got[slot] += 1
        H.task_done(wid)


def worker(H, wid, rng, state):
    reader(H, wid, rng, state)


def setup(H):
    """Build per-worker seeds and the closed-form (single-thread) reference for
    each, and WARM the shared hot_eval code object so its inline caches are armed
    before the M:N phase starts migrating it."""
    n = H.funcs
    base = H.derive("p301-stream-base")
    seeds = [base.getrandbits(48) for _ in range(n)]
    # Closed-form reference per worker, computed single-threaded NOW (this runs
    # inside the root but the worker pool has not fanned out yet, so it is the
    # run(1)-equivalent baseline arm).
    refs = [fold_stream(s, OPS_PER_ROUND) for s in seeds]
    # Warm the code object so LOAD_ATTR/CALL/BINARY_OP/FOR_ITER specialize while
    # single-thread specialization is active -- maximizing the chance the M:N
    # phase exercises a stale-cache de-opt under migration.
    warm = 0
    wprng = random.Random(0xC0FFEE)
    for _ in range(WARMUP):
        kind = wprng.randrange(4)
        nn = wprng.randrange(1000)
        operand = make_operand(kind, nn)
        tup = (nn & 7, (nn >> 3) & 7, (nn >> 6) & 7)
        warm ^= quant(hot_eval(operand, kind, tup))
    H.state = {"seeds": seeds, "refs": refs, "got": [0] * 1024, "warm": warm}
    H.log("seeded {0} streams, closed-form refs computed, hot_eval warmed "
          "({1} reads)".format(n, WARMUP))


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    matched = sum(H.state["got"])
    H.log("checksum-matched worker-rounds={0} (each == closed-form reference)"
          .format(matched))
    H.check(matched > 0, "no worker completed a full checksum fold")
    H.require_no_lost("specializer metamorphic completeness")


if __name__ == "__main__":
    harness.main("p301_specialize_polymorphic_metamorphic", body,
                 setup=setup, post=post, default_funcs=3000,
                 describe="shared hot_eval code object run across hubs over a "
                          "polymorphic int/float/Fraction/Lin stream that churns "
                          "specialize<->deopt; each worker's folded checksum must "
                          "equal the closed-form single-thread reference")
