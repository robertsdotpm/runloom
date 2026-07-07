"""big_100 / 562 -- code.InteractiveInterpreter / compile_command isolation + purity under M:N.

The `code` module is the read-eval-print machinery behind the interactive
interpreter.  Two entry points matter for an M:N single-owner probe:

  * code.InteractiveInterpreter(locals).runsource(src) compiles `src` with
    codeop.compile_command (symbol="single") and, when the source is COMPLETE,
    exec()s the resulting code object in `self.locals` -- a plain dict the caller
    supplies.  If the caller gives each interpreter its OWN private dict, that
    namespace is SINGLE-OWNER: exactly one fiber ever reads or writes it.
  * code.compile_command(src, ...) is a PURE function: complete source -> a code
    object; incomplete source ("if True:", "x = (1 +") -> None; invalid source ->
    SyntaxError.  For a fixed source string it must always produce a bit-identical
    code object (same co_code, co_consts, co_names) -- it depends on nothing but
    its arguments.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs these
fibers in PARALLEL across hubs with the GIL off, migrating a fiber across hubs at
every cooperative yield.  If the exec of a runsource() call, or the per-fiber
namespace dict, or the compile pipeline's internal state, were NOT properly
isolated per fiber, then:

  * a fiber that runs `acc += d` in its PRIVATE namespace, yields (parking / hub
    migration), and resumes could observe `acc` carrying a SIBLING's value -- a
    cross-fiber leak of single-owner exec state (a torn namespace dict, or an
    exec that landed in the wrong fiber's locals);
  * compile_command on a fiber-local source could return a code object whose
    co_consts / co_names belong to a SIBLING's source compiled concurrently -- a
    torn/leaked compilation.

Both are real runtime faults if they happen, because the state is provably
single-owner: the namespace dict is created per fiber and never shared, and the
source string is a fiber-local literal.  We verified with a plain-threads control
(8 OS threads, GIL on AND off, each with its own InteractiveInterpreter + private
dict feeding known deltas) that acc always equals the exact expected sum and
compile_command is always bit-identical -- 0 leaks.  A CORRECT runloom must match.

ORACLES:
  * LOAD-BEARING A -- SINGLE-OWNER EXEC CONSERVATION (worker, HARD, fail-fast).
    Each fiber owns interp = code.InteractiveInterpreter(ns) with a PRIVATE ns.
    It seeds `acc = wid*BASE_SCALE` (a fiber-unique base so a leaked sibling value
    is visibly wrong), then feeds a KNOWN list of deltas via runsource("acc += d"),
    yielding between feeds so a sibling reliably interleaves + the fiber migrates
    hubs.  After the feed, ns["acc"] MUST equal wid*BASE_SCALE + sum(deltas) EXACTLY
    -- the closed-world conservation law over a single-owner namespace.  A dropped
    /doubled increment or a leaked base is a runloom exec-isolation bug.  Single-
    owner: ns is never shared, so a mismatch is NOT documented dict-race behavior.

  * LOAD-BEARING B -- compile_command PURITY (worker, HARD, fail-fast).  Each fiber
    builds a FIBER-LOCAL source string (its constants + variable name encode wid),
    compiles it to a code object, snapshots (co_code, co_consts, co_names), YIELDS,
    then recompiles the same source and asserts the snapshot is BIT-IDENTICAL.  It
    also checks a fiber-local INCOMPLETE source returns None on both sides.  A
    changed co_code/co_consts/co_names across the yield, or a leaked sibling
    constant/name, is a torn/cross-fiber compilation -- a runloom bug.  Pure
    function of a fiber-local literal, so single-owner by construction.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-exec or
    mid-compile (parked and never re-woken) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (checks > 0).

  * MEASURED (report-ONLY, NEVER fails): a small pool of SHARED interpreters, each
    with a SHARED namespace dict, is hammered by all fibers via runsource("acc +=
    1").  A shared dict under M:N races EXACTLY like a shared-across-threads dict
    (documented Python behavior), so the summed acc will fall SHORT of the units
    fed -- we MEASURE the lost-increment rate to prove the hazard is real (fibers
    DO interleave on the shared exec path) and REPORT it; we NEVER fail on it.

FAIL ON: a single-owner interpreter's acc != wid*BASE_SCALE + sum(deltas), a
compile_command result that changes across a yield or carries a sibling's
constant/name, an unexpected exception on a valid fiber-local source, or a SIGSEGV
in the compile/exec path.  The shared-interpreter MEASURED arm is report-only and
is EXPECTED to lose increments (documented shared-dict semantics), never reaching
the single-owner oracle.

Stresses: code.InteractiveInterpreter.runsource compile+exec into a per-fiber
namespace across hub migration + yield, code.compile_command purity/determinism
under concurrency, per-fiber exec-namespace isolation vs shared-namespace behavior,
codeop incomplete-vs-complete detection under M:N.
"""
import code

import harness
import runloom

# Fiber-unique base for the single-owner accumulator.  acc starts at
# wid*BASE_SCALE so a leaked sibling value (a different wid's base) is visibly
# wrong, not merely off-by-a-few.
BASE_SCALE = 1000003

# The KNOWN multiset of deltas fed into each fiber's private accumulator per
# check.  Fixed + non-trivial so a dropped or doubled `acc += d` moves the final
# sum by a detectable, deterministic amount.
DELTAS = (1, 2, 3, 5, 7, 11, 13, 4, 6, 9)
DELTA_SUM = sum(DELTAS)

# Sustained checks per worker, bounded by H.running().  The isolation hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously exec/compile while
# parked across their yield, so the scheduler reliably interleaves a sibling and
# migrates hubs before this fiber resumes.
INNER_CAP = 100000

# Size of the SHARED-interpreter pool for the MEASURED (report-only) arm.
SHARED_POOL = 8


# ---- LOAD-BEARING A: single-owner exec conservation ----------------------
def exec_conservation_check(H, wid, idx, state):
    """Feed a KNOWN list of deltas into a PRIVATE interpreter namespace via
    runsource("acc += d"), yielding between feeds, then assert the accumulator
    equals wid*BASE_SCALE + sum(deltas) EXACTLY.  Single-owner: the namespace dict
    is created here and never shared, so any mismatch is a runloom exec-isolation
    bug, not documented shared-dict racing."""
    base = wid * BASE_SCALE + (idx & 7)     # fiber+iter-unique starting value
    ns = {}
    interp = code.InteractiveInterpreter(ns)

    # Seed the accumulator.  runsource returns False for a COMPLETE, executed
    # statement; a True (needs-more-input) here would mean codeop misjudged a
    # complete assignment -- a hard fault.
    if interp.runsource("acc = {0}".format(base)) is not False:
        H.fail("runsource('acc = {0}') reported incomplete for a COMPLETE "
               "statement (wid {1}) -- codeop.compile_command misjudged complete "
               "source under M:N".format(base, wid))
        return
    if ns.get("acc") != base:
        H.fail("seed exec landed wrong: acc={0!r} after 'acc = {1}' (wid {2}) -- "
               "the exec did not write this fiber's private namespace".format(
                   ns.get("acc"), base, wid))
        return

    expected = base
    for j, d in enumerate(DELTAS):
        # YIELD at the hazard boundary so a sibling exec/compile interleaves and
        # this fiber may migrate hubs before the next in-place add lands.
        runloom.yield_now()
        if j & 1:
            runloom.sleep(0.0002)
        if interp.runsource("acc += {0}".format(d)) is not False:
            H.fail("runsource('acc += {0}') reported incomplete (wid {1}) -- "
                   "codeop misjudged a complete augmented-assignment under "
                   "M:N".format(d, wid))
            return
        expected += d
        # Cross-yield stability: the private acc must reflect exactly the units
        # fed so far -- never a sibling's value.
        got = ns.get("acc")
        if got != expected:
            H.fail("exec conservation broken: private acc={0!r} after feeding {1} "
                   "of {2} deltas (expected {3}), wid {4} -- a runsource() exec was "
                   "DROPPED/DOUBLED or a sibling's value LEAKED into this fiber's "
                   "single-owner namespace across a yield".format(
                       got, j + 1, len(DELTAS), expected, wid))
            return

    if ns.get("acc") != base + DELTA_SUM:
        H.fail("exec conservation broken (final): acc={0!r} != base+sum={1} "
               "(wid {2}) -- a bulk of runsource() increments was lost/doubled "
               "over the single-owner namespace".format(
                   ns.get("acc"), base + DELTA_SUM, wid))
        return

    state["exec_checks"][wid] += 1          # single-writer-per-slot, race-free


# ---- LOAD-BEARING B: compile_command purity -----------------------------
def compile_purity_check(H, wid, idx, state):
    """Compile a FIBER-LOCAL source string, snapshot the code object's identity
    fields, yield, recompile, and assert bit-identical.  A fiber-local INCOMPLETE
    source must return None on both sides.  Pure function of a fiber-local literal,
    so single-owner by construction; a mismatch is a torn/leaked compilation."""
    # The constants + variable name encode wid+idx, so a leaked sibling
    # compilation would carry a DIFFERENT constant/name -- visibly wrong.
    a = wid * 131 + (idx & 15)
    b = (wid ^ idx) & 0xFFFF
    varname = "r_{0}_{1}".format(wid, idx & 3)
    complete_src = "{0} = {1} + {2}".format(varname, a, b)
    incomplete_src = "if {0} == {1}:".format(a, b)   # needs an indented block

    c1 = code.compile_command(complete_src)
    if c1 is None:
        H.fail("compile_command returned None for COMPLETE source {0!r} (wid {1}) "
               "-- codeop misjudged complete source under M:N".format(
                   complete_src, wid))
        return
    code0 = c1.co_code
    consts0 = c1.co_consts
    names0 = c1.co_names

    inc1 = code.compile_command(incomplete_src)
    if inc1 is not None:
        H.fail("compile_command returned non-None {0!r} for INCOMPLETE source "
               "{1!r} (wid {2}) -- codeop misjudged incomplete source".format(
                   inc1, incomplete_src, wid))
        return

    # YIELD: allow siblings to compile their own conflicting sources + migrate hub.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    c2 = code.compile_command(complete_src)
    if c2 is None:
        H.fail("compile_command returned None for COMPLETE source {0!r} on the "
               "SECOND compile (wid {1}) -- non-deterministic across a yield".format(
                   complete_src, wid))
        return
    if c2.co_code != code0:
        H.fail("compile_command NOT PURE: co_code changed across a yield for "
               "source {0!r} (wid {1}) -- a torn/cross-fiber compilation".format(
                   complete_src, wid))
        return
    if c2.co_consts != consts0:
        H.fail("compile_command NOT PURE: co_consts changed from {0!r} to {1!r} "
               "across a yield for source {2!r} (wid {3}) -- a sibling's constants "
               "leaked into this fiber's compilation".format(
                   consts0, c2.co_consts, complete_src, wid))
        return
    if c2.co_names != names0:
        H.fail("compile_command NOT PURE: co_names changed from {0!r} to {1!r} "
               "across a yield for source {2!r} (wid {3}) -- a sibling's names "
               "leaked into this fiber's compilation".format(
                   names0, c2.co_names, complete_src, wid))
        return
    # The compiled name must be THIS fiber's varname (not a leaked sibling name).
    if varname not in c2.co_names:
        H.fail("compile_command LEAK: co_names {0!r} does not contain this fiber's "
               "variable {1!r} (wid {2}) -- the compilation returned a sibling's "
               "code object".format(c2.co_names, varname, wid))
        return
    if code.compile_command(incomplete_src) is not None:
        H.fail("compile_command returned non-None for INCOMPLETE source {0!r} on "
               "the second compile (wid {1}) -- non-deterministic incomplete "
               "detection".format(incomplete_src, wid))
        return

    state["compile_checks"][wid] += 1       # single-writer-per-slot, race-free


# ---- MEASURED arm: shared interpreter + shared namespace (report-only) ---
def shared_exec_check(H, wid, state):
    """Drive one increment into a SHARED interpreter's SHARED namespace via
    runsource('acc += 1').  A shared dict under M:N races EXACTLY like a shared-
    across-threads dict (documented Python behavior), so summed acc falls SHORT of
    the units fed.  We MEASURE the lost-increment rate to prove the hazard exists
    and REPORT it -- we NEVER fail on it (that would mislabel documented shared-
    object semantics as a bug)."""
    interp = state["shared_pool"][wid % SHARED_POOL]
    interp.runsource("acc += 1")
    state["shared_fed"][wid & 1023] += 1    # units WE offered (report-only tally)


def worker(H, wid, rng, state):
    """Each fiber runs BOTH load-bearing single-owner arms (fail-fast) plus the
    MEASURED shared arm (report only) per iteration.  The single-owner state
    (private ns, fiber-local source) never touches the shared pool, so mixed churn
    keeps the hubs busy without the shared mutations reaching the fail-fast
    oracles."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            exec_conservation_check(H, wid, idx, state)     # LOAD-BEARING A
            if H.failed:
                return
            compile_purity_check(H, wid, idx, state)        # LOAD-BEARING B
            if H.failed:
                return
            shared_exec_check(H, wid, state)                # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # A small pool of SHARED interpreters, each with a SHARED namespace dict seeded
    # acc=0, for the MEASURED (report-only) arm.  These are deliberately shared so
    # the race is real; they never feed the single-owner oracle.
    shared_pool = []
    for _ in range(SHARED_POOL):
        ns = {}
        interp = code.InteractiveInterpreter(ns)
        interp.runsource("acc = 0")
        shared_pool.append(interp)

    H.state = {
        # LOAD-BEARING conservation tallies: ONE slot per worker (wid-indexed,
        # single-writer -> race-free); allocated here where H.funcs is known.
        "exec_checks": [0] * H.funcs,
        "compile_checks": [0] * H.funcs,
        # MEASURED shared arm (report-only): sharded tally is fine (non-vacuity).
        "shared_pool": shared_pool,
        "shared_fed": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    exec_checks = sum(H.state["exec_checks"])
    compile_checks = sum(H.state["compile_checks"])
    shared_fed = sum(H.state["shared_fed"])

    # Read back the SHARED interpreters' accumulators.  Under M:N racing on a
    # shared dict, the summed acc will fall SHORT of the units fed -- MEASURED,
    # report-only, proving the hazard exists.  NEVER a failure.
    shared_landed = 0
    for interp in H.state["shared_pool"]:
        v = interp.locals.get("acc", 0)
        if isinstance(v, int):
            shared_landed += v
    lost = shared_fed - shared_landed
    lpct = (100.0 * lost / shared_fed) if shared_fed else 0.0

    H.log("code[single-owner LOAD-BEARING]: {0} exec-conservation + {1} compile-"
          "purity checks (all passed fail-fast) | code[shared-ns MEASURED]: fed "
          "{2} increments, landed {3}, lost {4} ({5:.1f}%, documented shared-dict "
          "behavior -- REPORT ONLY)".format(
              exec_checks, compile_checks, shared_fed, shared_landed, lost, lpct))

    if lost:
        H.log("note: the shared-interpreter pool lost {0} of {1} increments across "
              "the shared exec namespace -- runloom hub fibers DO interleave on the "
              "shared runsource()/exec path (a shared dict, like p67's threading."
              "local shared container).  Documented M:N shared-object behavior, NOT "
              "a runloom bug, and it never reaches the single-owner oracles".format(
                  lost, shared_fed))

    # NON-VACUITY: both load-bearing arms actually exercised their hazard.
    H.check(exec_checks > 0,
            "no single-owner exec-conservation checks ran -- the load-bearing "
            "interpreter-namespace hazard was never exercised (oracle vacuous)")
    H.check(compile_checks > 0,
            "no single-owner compile-purity checks ran -- the load-bearing "
            "compile_command hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (stranded mid-exec / mid-compile).
    H.require_no_lost("code interp isolation")


if __name__ == "__main__":
    harness.main(
        "p562_code_interp", body, setup=setup, post=post,
        default_funcs=6000,
        describe="code.InteractiveInterpreter.runsource compiles+execs into a "
                 "caller-supplied namespace; code.compile_command is a pure "
                 "complete/incomplete-source compiler.  Under M:N, if per-fiber "
                 "exec state or the compile pipeline is not isolated, a fiber could "
                 "see a sibling's value in its PRIVATE namespace or a torn/leaked "
                 "compilation.  LOAD-BEARING: each fiber feeds a known delta multiset "
                 "into a PRIVATE interpreter namespace (acc must equal base+sum "
                 "exactly) and recompiles a fiber-local source across a yield "
                 "(co_code/co_consts/co_names must be bit-identical, carrying THIS "
                 "fiber's name).  MEASURED shared-namespace pool (expected to lose "
                 "increments, like p67) proves the hazard.  A leaked/dropped exec "
                 "value or a torn compilation is the runloom bug")
