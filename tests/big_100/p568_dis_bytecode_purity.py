"""big_100 / 568 -- dis.get_instructions() disassembly purity/stability under M:N.

The `dis` module disassembles a Python code object into a stream of Instruction
namedtuples (dis.get_instructions / dis.Bytecode).  Disassembly is a PURE
function of the code object: given the same immutable code object it must yield
the same instruction stream every time, and given the same source string
compile() is deterministic so two independently-compiled code objects for that
source disassemble bit-identically.  `dis` walks co_code / co_consts / the
exception table / the co_positions line info, decodes each 2-byte instruction
via the module-level (read-only) opmap/opname tables, and (in 3.14) formats
cache entries -- none of that should mutate any shared state.

WHERE M:N COULD BREAK IT (the gap this program probes).  A fiber compiles its
OWN fiber-local source into its OWN code object, disassembles it to a canonical
tuple, YIELDS (so a sibling on another hub compiles + disassembles its own,
different code object mid-flight), then re-disassembles and asserts the stream
is bit-identical.  If `dis` (or the compiler feeding it) leaked state across
fibers -- a shared decode buffer, a torn read of a module-level opname table
being rebuilt, a cross-fiber code-object reference, an instruction whose opcode
and opname desynced across a yield -- the second disassembly would differ from
the first, or an instruction's opcode/opname pair would be inconsistent.  A
correct runtime keeps each fiber's code object single-owner and dis's tables
read-only, so the stream is stable and the program exits 0.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Disassembly of a fixed code object is deterministic and pure; compile() of a
  fixed source string is deterministic.  A standalone plain-threads control (8
  OS threads, GIL on AND off, each compiling its own distinct source and
  disassembling it in a tight loop) yields byte-identical instruction tuples
  100% of the time -- 0 cross-thread divergence.  Under a CORRECT runloom it
  must also hold: a fiber's own code object, disassembled before and after a
  yield, MUST produce the identical instruction stream, and a freshly recompiled
  code object for the SAME source MUST disassemble identically to the first.  If
  it does not, that is a dis/compiler isolation or torn-table bug in runloom.

ORACLES:
  * LOAD-BEARING -- DISASSEMBLY PURITY (worker, HARD, fail-fast).  Each fiber:
      - builds a distinct fiber-local source string (deterministic from wid+idx),
      - compiles it (code object A, single-owner) and canonicalizes the full
        instruction stream (offset, opcode, opname, arg, is_jump_target) to an
        immutable tuple -- baseline,
      - self-consistency-checks every instruction (opcode/opname agree via the
        read-only dis tables; offsets strictly increase),
      - YIELDS (yield_now / tiny sleep) so siblings interleave,
      - re-disassembles code object A and asserts the canonical tuple is
        BIT-IDENTICAL to the baseline (pure function of a fixed object),
      - recompiles the SAME source into a NEW code object B and asserts its
        canonical tuple ALSO equals the baseline (compile() determinism holds
        across the yield -- no cross-fiber compiler-state leak).
    Single-owner: the source string, both code objects, and both instruction
    tuples live in fiber-local variables, never shared.  Any mismatch is a
    runloom desync, not documented Python semantics.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-decode
    (inside get_instructions walking a torn table) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0) and
    produced non-empty instruction streams (instrs > 0), else the oracle would be
    vacuously true on an empty disassembly.

FAIL ON: a re-disassembly of a fixed code object differing from its baseline, a
recompiled-same-source disassembly differing from the baseline, an
opcode/opname inconsistency, non-monotonic offsets, or a crash inside dis.  All
inputs are fiber-local and single-owner, so a failure is a real runtime bug
(torn read of dis's tables, cross-fiber code/compiler leak, or SIGSEGV), never
shared-object semantics.

Stresses: dis.get_instructions / dis.Bytecode decode loop over co_code +
exception table + co_positions, module-level opmap/opname table reads, compile()
determinism, Instruction namedtuple construction, all under M:N hub migration +
yield with tens of thousands of fibers churning distinct code objects.

Good TSan / controlled-M:N-replay target: the per-instruction opname[opcode]
table lookup is a read of a module-global list while every hub does the same; a
TSan report on that read (or a replay that returns a mismatched opname for a
fixed opcode across a yield) localizes a torn-table bug before the tuple compare
even fires.
"""
import dis

import harness
import runloom


# Small deterministic parameters woven into each fiber's source so distinct
# fibers compile DISTINCT code objects (distinct constants, loop bounds, branch
# structure) -- the disassembly streams differ across fibers, so a cross-fiber
# leak would be visible as a wrong stream, not an accidental match.
def build_source(wid, idx):
    """Return a distinct, deterministic Python source string for (wid, idx).

    The generated function has loops, a branch, arithmetic on fiber-specific
    constants, a list comprehension and a call -- enough bytecode variety that
    the instruction stream is long and structurally non-trivial (real jump
    targets, an exception-free but multi-block body).  Fiber-local: never shared."""
    a = (wid % 7) + 1
    b = (idx % 5) + 2
    c = (wid * 3 + idx) % 11
    lines = [
        "def f_{0}_{1}(x):".format(wid, idx),
        "    total = {0}".format(c),
        "    for i in range(x + {0}):".format(a),
        "        if i % {0} == 0:".format(b),
        "            total += i * {0}".format(a),
        "        else:",
        "            total -= (i + {0})".format(c),
        "    data = [j * j for j in range(total % 13)]",
        "    return sum(data) + total - {0}".format(b),
    ]
    return "\n".join(lines)


def canon(code):
    """Canonicalize a code object's full disassembly to an immutable tuple.

    Each element is (offset, opcode, opname, arg, is_jump_target) -- a pure,
    hashable, order-preserving snapshot of the instruction stream.  Also returns
    the list of (opcode, opname) pairs so the caller can self-consistency-check
    each instruction against dis's read-only opname table.  Raises nothing on a
    well-formed code object; a crash here would be a dis decode bug."""
    out = []
    for ins in dis.get_instructions(code):
        out.append((ins.offset, ins.opcode, ins.opname, ins.arg,
                    bool(ins.is_jump_target)))
    return tuple(out)


# dis.opname is a module-level list mapping opcode number -> canonical name for
# the real (< HAVE_ARGUMENT-agnostic) opcodes.  We snapshot its length once; per
# instruction we verify opcode/opname agree THROUGH this table for real opcodes,
# which is the torn-table read the M:N churn would expose.
OPNAME = dis.opname
OPNAME_LEN = len(OPNAME)


def check_instruction_consistency(H, wid, canon_tuple):
    """Self-consistency oracle over one fiber's own instruction stream:

      * every opcode is a non-negative int and every opname a non-empty str;
      * for a real opcode (0 <= opcode < len(dis.opname)), dis.opname[opcode]
        equals the Instruction's opname (a torn read of the shared opname table,
        or a cross-fiber opcode/opname desync, breaks this);
      * offsets strictly increase (the decode loop advanced monotonically -- a
        torn co_code walk would repeat or reorder an offset).

    Returns True if consistent; calls H.fail and returns False otherwise."""
    last_off = -1
    for (offset, opcode, opname, arg, is_jt) in canon_tuple:
        if not isinstance(opcode, int) or opcode < 0:
            H.fail("dis: instruction has bad opcode {0!r} (opname {1!r}) at "
                   "offset {2!r} (wid {3}) -- torn instruction decode".format(
                       opcode, opname, offset, wid))
            return False
        if not isinstance(opname, str) or not opname:
            H.fail("dis: instruction has bad opname {0!r} (opcode {1!r}) at "
                   "offset {2!r} (wid {3}) -- torn instruction decode".format(
                       opname, opcode, offset, wid))
            return False
        # For real opcodes, the Instruction's opname MUST match the module-level
        # opname table -- this is the shared read-only table lookup the M:N churn
        # races.  (Opcodes >= OPNAME_LEN would be out-of-table; the real 3.14
        # opcode space is < 256 so this always applies for a well-formed stream.)
        if 0 <= opcode < OPNAME_LEN:
            expected = OPNAME[opcode]
            if expected != opname:
                H.fail("dis: opcode/opname DESYNC: opcode {0} disassembled as "
                       "{1!r} but dis.opname[{0}]=={2!r} (offset {3}, wid {4}) "
                       "-- a torn read of the shared opname table under M:N "
                       "churn".format(opcode, opname, expected, offset, wid))
                return False
        if offset <= last_off:
            H.fail("dis: non-monotonic instruction offset {0} <= previous {1} "
                   "(opname {2!r}, wid {3}) -- the co_code decode loop repeated "
                   "or reordered an offset (torn walk)".format(
                       offset, last_off, opname, wid))
            return False
        last_off = offset
    return True


def purity_check(H, wid, idx, state):
    """Single-owner disassembly purity/stability check (fail-fast).

    Compile a fiber-local source, disassemble it to a baseline tuple, verify
    per-instruction self-consistency, YIELD so siblings interleave, then assert
    (a) re-disassembling the SAME code object reproduces the baseline exactly,
    and (b) recompiling the SAME source into a NEW code object also reproduces
    the baseline exactly (compile determinism across the yield).  Every value is
    fiber-local; a mismatch is a runtime isolation/torn-read bug."""
    src = build_source(wid, idx)
    code_a = compile(src, "<fiber w{0} i{1}>".format(wid, idx), "exec")

    baseline = canon(code_a)
    if not baseline:
        H.fail("dis: empty instruction stream for a non-trivial source "
               "(wid {0}) -- disassembly produced nothing".format(wid))
        return
    if not check_instruction_consistency(H, wid, baseline):
        return

    # YIELD: let siblings on other hubs compile + disassemble their own distinct
    # code objects mid-flight.  If any shared dis/compiler state leaks across
    # fibers, the post-yield re-disassembly or recompile will diverge.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # (a) Re-disassemble the SAME code object -- pure function of a fixed object.
    again = canon(code_a)
    if again != baseline:
        H.fail("dis: re-disassembly of a FIXED code object changed across a "
               "yield (wid {0}): {1} instrs before vs {2} after -- dis is not a "
               "pure function here, a cross-fiber leak or torn table read "
               "corrupted this fiber's stream".format(
                   wid, len(baseline), len(again)))
        return

    # (b) Recompile the SAME source into a NEW code object; compile() is
    # deterministic so its disassembly MUST match the baseline byte-for-byte.
    code_b = compile(src, "<fiber w{0} i{1}>".format(wid, idx), "exec")
    fresh = canon(code_b)
    if fresh != baseline:
        # Find the first differing element for a precise message.
        n = min(len(baseline), len(fresh))
        where = "length {0} vs {1}".format(len(baseline), len(fresh))
        for k in range(n):
            if baseline[k] != fresh[k]:
                where = "instr[{0}] {1!r} != {2!r}".format(
                    k, baseline[k], fresh[k])
                break
        H.fail("dis: recompiled-same-source disassembly differs from baseline "
               "across a yield (wid {0}): {1} -- compile()+dis lost determinism "
               "under M:N (cross-fiber compiler/dis state leak)".format(
                   wid, where))
        return

    # Non-vacuity accounting (sharded; feeds only the >0 checks, never a
    # conservation law -- HARD RULE 1 permits wid&MASK for a tally).
    slot = wid & 1023
    state["checks"][slot] += 1
    state["instrs"][slot] += len(baseline)


# Sustained churn per worker: the torn-table / cross-fiber hazard only manifests
# when many fibers are simultaneously compiling + disassembling distinct code
# objects while sleep-parked across their yield, so the scheduler reliably
# interleaves a sibling's decode before this fiber resumes.  A single check per
# fiber barely overlaps a sibling's and does not reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "checks": [0] * 1024,     # LOAD-BEARING purity checks (non-vacuity tally)
        "instrs": [0] * 1024,     # total instructions disassembled (non-vacuity)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    instrs = sum(H.state["instrs"])
    H.log("dis[single-owner LOAD-BEARING]: {0} disassembly purity checks (all "
          "passed fail-fast), {1} total instructions decoded; ops={2}".format(
              checks, instrs, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised and the
    # disassembly produced real instruction streams (not vacuously empty).
    H.check(checks > 0,
            "no disassembly purity checks ran -- the dis stability hazard was "
            "never exercised (oracle would be vacuous)")
    H.check(instrs > 0,
            "no instructions were decoded -- disassembly was empty, the purity "
            "oracle would be vacuously true")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the dis
    # decode loop over a torn table).
    H.require_no_lost("dis disassembly purity")


if __name__ == "__main__":
    harness.main(
        "p568_dis_bytecode_purity", body, setup=setup, post=post,
        default_funcs=6000,
        describe="dis.get_instructions() disassembles a code object into a "
                 "stream of Instruction namedtuples via a pure decode over "
                 "co_code + the read-only opmap/opname tables.  LOAD-BEARING: "
                 "each fiber compiles its OWN distinct source, disassembles it "
                 "to a canonical instruction tuple, yields, then asserts (a) "
                 "re-disassembling the SAME code object reproduces the stream "
                 "bit-identically and (b) recompiling the SAME source "
                 "disassembles identically (compile determinism across the "
                 "yield).  A changed stream, an opcode/opname desync, or "
                 "non-monotonic offsets is a torn-table / cross-fiber dis "
                 "isolation bug in runloom")
