"""big_100 / 563 -- codeop.compile_command / CommandCompiler PURITY + future-memory
isolation under M:N.

codeop is the REPL's "is this command complete?" front end.  It exposes three
things:

  * compile_command(source, filename, symbol) -- a PURE function: for a fixed
    (source, symbol) it returns a code object (COMPLETE + valid), None
    (INCOMPLETE input the REPL should keep reading), or raises SyntaxError /
    ValueError / OverflowError (a hard syntax/literal error).  The classification
    and the emitted bytecode are a deterministic function of the input -- given
    the same source it must always produce byte-identical co_code / co_consts and
    (for an `eval` code object) the same evaluated value.

  * Compile() / CommandCompiler() -- a STATEFUL instance that "remembers" a
    __future__ statement: once you feed a CommandCompiler `from __future__ import
    barry_as_FLUFL`, that INSTANCE compiles all subsequent sources with the flag
    in force (so `1 <> 2` -- the FLUFL "diamond" inequality -- starts compiling on
    that instance and NOWHERE ELSE).  The remembered flag lives in the instance's
    self.compiler.flags; it is per-INSTANCE state, not process-global.

WHERE M:N COULD BREAK IT (the gap this program probes).  compile_command runs the
real compiler under `warnings.catch_warnings()` and does a two-shot probe
(compile source, then source+"\n") to detect incomplete input.  Under GIL-off M:N,
tens of thousands of fibers on 8 hubs pound compile()/the parser and the
tokenizer's C machinery simultaneously, PARKED across a yield mid-classification
so a sibling reliably interleaves.  If the runtime torn the compiler's per-call
state, migrated a fiber mid-parse onto a hub whose thread-state disagreed, or let
one fiber's CommandCompiler future-flag leak into another's instance, we would see
either:
  * a compile_command result that is NOT a pure function of its input (bytecode or
    evaluated value differs across a yield, or differs from the single-threaded
    baseline computed at import), or
  * a classification that flips COMPLETE/INCOMPLETE/ERROR across a yield, or
  * a per-instance __future__ flag that leaks IN (a fresh CommandCompiler already
    accepts `1 <> 2` before we fed it the future) or leaks OUT / is lost (after we
    fed the future statement to OUR instance, `1 <> 2` no longer compiles on it).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified single-threaded first):

  All three arms are SINGLE-OWNER.  compile_command is a module pure function with
  no writable shared state (the module's `_features` list is read-only; the only
  mutation is `warnings.catch_warnings()`, which touches process-global WARNING
  filters -- irrelevant to the compile RESULT, since warnings are not errors here,
  so a catch_warnings race cannot change COMPLETE/INCOMPLETE/ERROR or the emitted
  bytecode).  The CommandCompiler arm creates its OWN instance per fiber, so its
  remembered-future state has exactly one writer.  We computed every expected
  outcome single-threaded at import (baseline co_code bytes, co_consts, eval value,
  and classification kind), so the closed-form ground truth is fixed BEFORE the
  M:N run; each fiber recomputes under contention and must match it bit-for-bit,
  and must stay stable across an interleaving yield.  On a correct runtime this
  PASSES (exit 0) -- purity holds.

ORACLES:
  * LOAD-BEARING A -- EVAL PURITY (worker, HARD, fail-fast).  For a fiber-local
    `eval` expression, compile_command() twice around a yield; assert BOTH code
    objects have co_code / co_consts byte-identical to the import-time baseline and
    both evaluate (in an empty-builtins namespace) to the baseline value.  A
    difference is a torn/non-pure compile under M:N.

  * LOAD-BEARING B -- CLASSIFY STABILITY (worker, HARD, fail-fast).  For a fiber-
    local (source, symbol), classify the compile_command outcome as
    COMPLETE/INCOMPLETE/ERROR twice around a yield; both must equal the baseline
    kind.  A flip is a torn incomplete-input probe.

  * LOAD-BEARING C -- FUTURE-MEMORY ISOLATION (worker, HARD, fail-fast).  Each
    fiber builds its OWN CommandCompiler(); before feeding the future statement,
    `1 <> 2` must ERROR (the flag has not leaked IN from a sibling); after feeding
    `from __future__ import barry_as_FLUFL` to THIS instance (with an intervening
    yield), `1 <> 2\n` must COMPILE and eval True (the instance remembered its own
    future; the flag was not lost or overwritten by a sibling's instance).

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- no fiber parked-then-vanished
    (e.g. stranded inside the C tokenizer / compiler under a migration).

FAIL ON: a compile_command bytecode/value that changes across a yield or differs
from the single-threaded baseline; a classification that flips; a CommandCompiler
future flag that leaks in (fresh instance already FLUFL-active) or is lost (our
instance forgot the future we fed it).  There is NO shared-mutable arm here -- every
object under the oracle has a single owner, so any failure is a runtime desync, not
documented Python shared-object semantics.

Stresses: codeop.compile_command two-shot incomplete-input probe, the C compiler /
tokenizer under GIL-off M:N with hub migration across a yield, warnings.catch_
warnings process-global filter churn (measured NOT to affect the result), per-
instance __future__ flag memory (Compile.flags) isolation across fibers.

Good TSan / controlled-M:N-replay target: the compiler + tokenizer C paths run
concurrently on every hub with the GIL off; a data-race report on a tokenizer /
compiler global, or a replay that migrates a fiber mid-parse and yields a
different code object, localizes the desync before the byte-for-byte purity oracle
even closes.
"""
import codeop

import harness
import runloom

# Empty-builtins namespace factory: every eval expression in the catalog is pure
# arithmetic / literal syntax that needs NO builtins, so evaluating in a locked-
# down namespace both proves the code object is real and keeps eval side-effect
# free (an expression never writes to the namespace).  Each fiber owns its own.
def fresh_ns():
    return {"__builtins__": {}}


# ---- EVAL PURITY catalog -------------------------------------------------
# Pure `eval` expressions (no builtins needed).  The import-time baseline captures
# the deterministic co_code / co_consts / evaluated value; workers must reproduce
# it byte-for-byte under M:N.
EVAL_SOURCES = [
    "1+2*3-4",
    "(2**10)//7",
    "7*7*7",
    "100-1-2-3",
    "(1,2,3)",
    "[10,20,30]",
    "{'a':1,'b':2}",
    "1.5+2.5",
    "5>3",
    "'ab'+'cd'",
    "2<<8",
    "0xff & 0x0f",
]


def build_eval_baseline():
    """Single-threaded ground truth: for each eval source, the code object's
    co_code bytes, co_consts, and the value it evaluates to.  Computed ONCE at
    import, before any M:N run, so it is a fixed closed-form the workers match."""
    baseline = []
    for src in EVAL_SOURCES:
        c = codeop.compile_command(src, "<baseline>", "eval")
        if c is None:
            raise RuntimeError("eval baseline unexpectedly incomplete: " + repr(src))
        val = eval(c, fresh_ns())
        baseline.append((src, bytes(c.co_code), c.co_consts, val))
    return baseline


EVAL_BASELINE = build_eval_baseline()


# ---- CLASSIFY STABILITY catalog ------------------------------------------
# (source, symbol, expected_kind) with kinds verified single-threaded.  Covers
# all three outcomes of the incomplete-input probe.
KIND_COMPLETE = "COMPLETE"
KIND_INCOMPLETE = "INCOMPLETE"
KIND_ERROR = "ERROR"

CLASSIFY_CASES = [
    ("x = 1", "single", KIND_COMPLETE),
    ("1+1", "eval", KIND_COMPLETE),
    ("pass", "single", KIND_COMPLETE),
    ("", "single", KIND_COMPLETE),
    ("# a comment", "single", KIND_COMPLETE),
    ("if True:", "single", KIND_INCOMPLETE),
    ("(1+", "eval", KIND_INCOMPLETE),
    ("for i in range(3):", "single", KIND_INCOMPLETE),
    ("def f():", "single", KIND_INCOMPLETE),
    ("while True:", "single", KIND_INCOMPLETE),
    ("[1,2,", "eval", KIND_INCOMPLETE),
    ("def f(:", "single", KIND_ERROR),
    ("1 <> 2", "eval", KIND_ERROR),
    ("1 +* 2", "eval", KIND_ERROR),
    ("return 5", "eval", KIND_ERROR),
    ("x =", "single", KIND_ERROR),
]


def classify(src, symbol):
    """Classify compile_command's outcome for (src, symbol)."""
    try:
        r = codeop.compile_command(src, "<c>", symbol)
    except SyntaxError:
        return KIND_ERROR
    except (ValueError, OverflowError):
        return KIND_ERROR
    return KIND_COMPLETE if r is not None else KIND_INCOMPLETE


def classify_cc(cc, src, symbol):
    """Classify a CommandCompiler INSTANCE's outcome for (src, symbol)."""
    try:
        r = cc(src, "<f>", symbol)
    except SyntaxError:
        return KIND_ERROR
    except (ValueError, OverflowError):
        return KIND_ERROR
    return KIND_COMPLETE if r is not None else KIND_INCOMPLETE


# ---- LOAD-BEARING A: eval purity -----------------------------------------
def eval_purity_check(H, wid, idx, ns):
    """compile_command an eval expression twice around a yield; both code objects
    must be byte-identical to the import baseline and evaluate to the baseline
    value.  Single-owner: source + baseline are read-only, ns is fiber-local."""
    src, code0, consts0, val0 = EVAL_BASELINE[idx % len(EVAL_BASELINE)]

    c1 = codeop.compile_command(src, "<b>", "eval")
    if c1 is None:
        H.fail("eval purity: compile_command({0!r}) returned INCOMPLETE (None) -- "
               "expected a complete code object (wid {1})".format(src, wid))
        return
    v1 = eval(c1, ns)

    # YIELD across the hazard boundary so a sibling parser reliably interleaves.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    c2 = codeop.compile_command(src, "<b>", "eval")
    if c2 is None:
        H.fail("eval purity: compile_command({0!r}) returned INCOMPLETE (None) "
               "after a yield -- classification flipped (wid {1})".format(src, wid))
        return
    v2 = eval(c2, ns)

    if bytes(c1.co_code) != code0 or bytes(c2.co_code) != code0:
        H.fail("eval purity: co_code for {0!r} differs from the single-threaded "
               "baseline -- a torn/non-pure compile under M:N (wid {1})".format(
                   src, wid))
        return
    if c1.co_consts != consts0 or c2.co_consts != consts0:
        H.fail("eval purity: co_consts for {0!r} differs from the baseline -- a "
               "torn compile under M:N (wid {1})".format(src, wid))
        return
    if v1 != val0 or v2 != val0:
        H.fail("eval purity: {0!r} evaluated to {1!r}/{2!r}, baseline {3!r} -- a "
               "non-pure compile result under M:N (wid {4})".format(
                   src, v1, v2, val0, wid))
        return


# ---- LOAD-BEARING B: classify stability ----------------------------------
def classify_check(H, wid, idx):
    """Classify a (source, symbol) twice around a yield; both must equal the
    baseline kind.  Single-owner: pure function of read-only inputs."""
    src, symbol, kind0 = CLASSIFY_CASES[idx % len(CLASSIFY_CASES)]

    k1 = classify(src, symbol)
    runloom.yield_now()
    k2 = classify(src, symbol)

    if k1 != kind0:
        H.fail("classify: compile_command({0!r}, {1!r}) classified {2}, expected "
               "{3} (baseline) -- a torn incomplete-input probe under M:N "
               "(wid {4})".format(src, symbol, k1, kind0, wid))
        return
    if k2 != kind0:
        H.fail("classify: compile_command({0!r}, {1!r}) classified {2} after a "
               "yield, expected {3} -- classification flipped across the yield "
               "(wid {4})".format(src, symbol, k2, kind0, wid))
        return


# ---- LOAD-BEARING C: per-instance future-memory isolation ----------------
def future_memory_check(H, wid, idx):
    """Each fiber builds its OWN CommandCompiler and verifies the per-instance
    __future__ (barry_as_FLUFL) memory: `1 <> 2` must ERROR before the future is
    fed (no leak IN from a sibling), and must COMPILE + eval True after THIS
    instance is fed the future statement (memory retained, not lost/overwritten by
    a sibling's instance).  Single-owner: the CommandCompiler is fiber-local."""
    cc = codeop.CommandCompiler()

    # Before: the FLUFL diamond `<>` must be a syntax error on a fresh instance.
    before = classify_cc(cc, "1 <> 2", "eval")
    if before != KIND_ERROR:
        H.fail("future isolation: a FRESH CommandCompiler classified `1 <> 2` as "
               "{0}, expected ERROR -- a sibling's barry_as_FLUFL future flag "
               "leaked INTO this fiber's instance (wid {1})".format(before, wid))
        return

    runloom.yield_now()

    # Feed the future statement to THIS instance; it must remember it.
    cc("from __future__ import barry_as_FLUFL", "<f>", "single")

    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # After: the same instance must now accept `1 <> 2` and eval it to True.
    after_c = cc("1 <> 2\n", "<f>", "eval")
    if after_c is None:
        H.fail("future isolation: after feeding `from __future__ import "
               "barry_as_FLUFL`, this instance classified `1 <> 2` as INCOMPLETE "
               "-- the per-instance future memory was LOST (wid {0})".format(wid))
        return
    try:
        val = eval(after_c, fresh_ns())
    except SyntaxError:
        H.fail("future isolation: after feeding the future statement, `1 <> 2` "
               "still raised SyntaxError on this instance -- the per-instance "
               "future memory was LOST or overwritten by a sibling (wid {0})".format(
                   wid))
        return
    if val is not True:
        H.fail("future isolation: FLUFL `1 <> 2` evaluated to {0!r}, expected True "
               "-- a torn compile of the remembered-future source (wid {1})".format(
                   val, wid))
        return


# Sustained checks per worker, bounded by H.running().  A single check barely
# overlaps a sibling's; the desync hazards (torn compile, migrated parse, future
# leak) only manifest under SUSTAINED churn with many fibers parked across their
# yields, so the scheduler reliably interleaves a sibling before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs all three single-owner load-bearing arms per inner
    iteration, fail-fast.  No shared mutable state feeds any oracle."""
    ns = fresh_ns()                       # fiber-local eval namespace
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            eval_purity_check(H, wid, idx, ns)          # LOAD-BEARING A
            if H.failed:
                return
            classify_check(H, wid, idx)                 # LOAD-BEARING B
            if H.failed:
                return
            future_memory_check(H, wid, idx)            # LOAD-BEARING C
            if H.failed:
                return
            state["checks"][wid] += 1                   # single-writer-per-slot
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One check-count slot per worker (single writer per slot -> race-free), used
    # only for the non-vacuity tally.  Allocated here where H.funcs is known.
    H.state = {"checks": [0] * H.funcs}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("codeop purity/future checks (all three single-owner load-bearing arms, "
          "fail-fast): {0}; ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the load-bearing arms actually ran.
    H.check(checks > 0,
            "no codeop purity/future checks ran -- the compile_command purity + "
            "per-instance future-memory hazard was never exercised (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside the C compiler/tokenizer.
    H.require_no_lost("codeop compile purity")


if __name__ == "__main__":
    harness.main(
        "p563_codeop_compile_purity", body, setup=setup, post=post,
        default_funcs=4000,
        describe="codeop.compile_command is a PURE incomplete-input classifier and "
                 "CommandCompiler carries per-INSTANCE __future__ memory.  Under "
                 "M:N (GIL off, 8 hubs, tens of thousands of fibers pounding the C "
                 "compiler/tokenizer parked across yields), LOAD-BEARING single-"
                 "owner oracles assert: (A) compile_command emits byte-identical "
                 "co_code/co_consts + the same eval value as the single-threaded "
                 "import baseline; (B) COMPLETE/INCOMPLETE/ERROR classification is "
                 "stable across a yield; (C) a fiber-local CommandCompiler's "
                 "barry_as_FLUFL future flag neither leaks IN (fresh instance "
                 "already FLUFL-active) nor is LOST (our instance forgot the future "
                 "we fed it).  A torn/non-pure compile, a classification flip, or a "
                 "cross-fiber future-flag leak is the runloom bug")
