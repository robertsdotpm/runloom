"""big_100 / 515 -- pickletools.genops() generator-frame cursor + optimize() round-trip under M:N.

pickletools.genops(pickle_bytes) is a GENERATOR that walks the pickle opcode
stream one instruction at a time.  Its whole state lives in the generator's
frame: a cursor `pos` into the byte string, the read file object, and the
per-opcode `arg` reader it dispatches through the shared module-global decode
tables (pickletools.opcodes / the code2op mapping).  Each `next()` advances the
cursor, decodes one opcode + its inline argument, and yields
(OpcodeInfo, arg, pos).  pickletools.optimize(pickle_bytes) drives that SAME
generator internally: it does a first genops pass to find which memo PUTs are
actually GET-referenced, then a second pass to emit a shrunk pickle with the
dead PUTs (and the FRAME) stripped.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives each
fiber its own Python frame stack, so a genops() generator instance created and
iterated entirely inside ONE fiber is single-owner: its frame, its `pos`
cursor, its arg-reader closure all belong to that fiber alone.  BUT the fiber
PARKS (yields to the scheduler) in the middle of iterating its generator -- the
generator frame is suspended, live, holding the cursor -- and a sibling on
another hub is simultaneously creating and driving its OWN genops() generator
over a DIFFERENT pickle.  If a suspended generator frame's locals tear when the
owning fiber is descheduled and later resumed on a different hub, or if the
shared read-only opcode-decode tables are corrupted by concurrent access, the
resumed generator would desync: the cursor would jump, positions would go
non-monotone, an unknown/garbage opcode name would appear, the arg decode would
raise, or -- most tellingly -- optimize() would emit a pickle that no longer
round-trips (loads(optimized) != original) or that grew instead of shrank.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  A fiber pickles its OWN wid-tagged object graph -- built with a repeated
  sub-object so the pickle necessarily contains memo opcodes (a MEMOIZE/PUT for
  the first occurrence and a GET for every reuse); those memo opcodes are
  exactly what optimize() rewrites, so they make the round-trip test load-
  bearing rather than trivial.  The fiber then, on its OWN private `data`
  bytes:

    (1) Manually drives pickletools.genops(data) to completion, PARKING
        (runloom.yield_now / sleep) INSIDE the iteration loop so a sibling
        reliably interleaves while this generator frame is suspended, and after
        each resume asserts the generator cursor is still sane:
          - `pos` is STRICTLY MONOTONE increasing across the whole walk (the
            cursor only ever moves forward; a jump/rewind means the suspended
            frame's cursor tore);
          - every yielded opcode's `.name` is in the KNOWN opcode-name set
            (pickletools.opcodes); an unknown name means a garbage decode;
          - the walk ends with the STOP opcode and consumes the whole stream.
    (2) Calls pickletools.optimize(data) (which drives its own internal genops
        passes) and asserts:
          - pickle.loads(optimized) == the original graph  (ROUND-TRIP
            IDENTITY -- optimize must preserve meaning);
          - len(optimized) <= len(data)  (optimize only ever strips bytes; a
            LARGER output means a desynced rewrite);
          - re-walking `optimized` with genops also reaches STOP cleanly.

  We verified with a plain-threads control (8 OS threads, GIL on AND off, each
  thread repeatedly building its own reuse-graph, walking genops, and calling
  optimize) that 100% of walks are monotone, every opcode name is known, and
  every optimize() round-trips with len(optimized) <= len(data) -- 0 desyncs.
  Under a CORRECT runloom the single-owner oracle must also hold: the program
  exits 0 when there is no bug.  A monotonicity break, an unknown opcode, an
  optimize() that fails to round-trip or grows, or a SIGSEGV mid-genops is a
  runloom generator-frame / shared-decode-table corruption -- a real runtime
  bug.

ORACLES:
  * LOAD-BEARING -- GENOPS CURSOR STABILITY + OPTIMIZE ROUND-TRIP (worker,
    HARD, fail-fast).  Single-owner: the pickle bytes, the genops generator
    instance, and the optimized bytes are all fiber-local, never shared.  A
    failure is a runloom suspended-generator-frame or decode-table desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    suspended genops generator (parked mid-iteration and never resumed) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): opcodes were actually walked (opcodes_walked > 0)
    AND optimize round-trips were actually checked (optimize_checks > 0) -- the
    two-layer hazard was genuinely exercised.

FAIL ON: a genops cursor that goes non-monotone across a park, an unknown/torn
opcode name, an arg-decode exception on well-formed private bytes, an
optimize() output that does NOT round-trip to the original graph or that grew
larger than the input, or a SIGSEGV mid-walk.  There is NO shared-mutable arm
here: genops generators and their byte buffers are inherently single-owner, so
every observation is load-bearing.

Stresses: pickletools.genops generator-frame cursor stability across hub
migration + park, pickletools.optimize() dual-pass memo analysis and rewrite,
the shared read-only pickletools.opcodes / code2op decode tables under
concurrent read, pickle memo PUT/GET opcode round-trip identity, generator
suspend/resume of a live frame under M:N.

Distinct from p465 (pickle memo dispatch) and p438 (PickleBuffer OOB export):
this targets the pickletools DISASSEMBLER/OPTIMIZER generator specifically --
the suspended genops frame cursor and the optimize() round-trip -- not the
pickler's memo table nor out-of-band buffer export.

Good TSan / controlled-M:N-replay target: a suspended generator frame is a live
PyFrameObject holding a cursor; a data-race report on that frame's locals when
the owning fiber migrates hubs, or a deterministic-replay that resumes the
generator with a torn cursor, localizes the desync before the monotonicity or
round-trip oracle fires.
"""
import pickle
import pickletools

import harness
import runloom

# The set of ALL valid pickle opcode names, taken from the shared read-only
# pickletools decode table.  Any name genops() yields that is NOT in here is a
# garbage/torn decode -- a hard fault.  Built once at import (read-only).
KNOWN_OPCODE_NAMES = frozenset(op.name for op in pickletools.opcodes)

# Pickle protocol to use.  Protocol 2+ emits framed pickles with MEMOIZE/BINGET
# memo opcodes (the ones optimize() rewrites); we pin a protocol so the opcode
# stream is deterministic per graph and the memo opcodes are guaranteed present.
PROTO = pickle.DEFAULT_PROTOCOL

# Sustained walks per worker, bounded by H.running().  The suspended-generator-
# frame hazard only manifests under SUSTAINED churn: many fibers simultaneously
# parked INSIDE their own genops iteration while siblings drive their own, so a
# resume reliably lands after a sibling ran.  A single walk per fiber barely
# overlaps and does NOT reproduce.
INNER_CAP = 100000


def build_reuse_graph(wid, idx):
    """Build a wid+idx-tagged object graph that pickles with MEMO opcodes.

    The `sub` object is referenced MULTIPLE times, so the pickle contains a
    memo PUT/MEMOIZE for its first occurrence and a memo GET for each reuse --
    exactly the opcodes pickletools.optimize() analyses and rewrites.  Every
    value is derived from wid+idx so distinct fibers pickle distinct byte
    streams (no accidental sharing of the `data` buffer)."""
    tag = "w{0}_i{1}".format(wid, idx)
    sub = [wid, wid + 1, wid + 2, tag]
    inner = {"lo": wid, "hi": wid + idx}
    graph = {
        "a": sub,                 # first occurrence -> memo PUT
        "b": sub,                 # reuse           -> memo GET
        "c": (sub, sub, inner),   # more reuse of sub AND first use of inner
        "d": inner,               # reuse of inner  -> memo GET
        "wid": wid,
        "idx": idx,
        "tag": tag,
        "vals": list(range(wid % 17 + 3)),
    }
    return graph


def walk_and_optimize(H, wid, idx, state):
    """Single-owner LOAD-BEARING check: drive genops() to completion parking
    mid-iteration, assert cursor stability, then optimize() + round-trip.

    All data here (graph, data bytes, generator instance, optimized bytes) is
    fiber-local.  A failure is a runloom generator-frame / decode-table
    corruption, not documented Python behavior."""
    graph = build_reuse_graph(wid, idx)
    try:
        data = pickle.dumps(graph, protocol=PROTO)
    except Exception as exc:                 # pragma: no cover -- pickling our
        H.fail("pickle.dumps of the fiber-local reuse graph raised {0!r} "
               "(wid {1}) -- building the single-owner input must never "
               "fail".format(exc, wid))
        return

    # ---- (1) drive genops() to completion, parking mid-iteration -----------
    last_pos = -1
    nops = 0
    saw_stop = False
    gen = pickletools.genops(data)
    while True:
        try:
            opinfo, arg, pos = next(gen)
        except StopIteration:
            break
        except Exception as exc:
            H.fail("genops() raised {0!r} at op #{1} while walking the fiber's "
                   "OWN well-formed pickle bytes (wid {2}) -- the suspended "
                   "generator frame's cursor or the shared decode table "
                   "desynced".format(exc, nops, wid))
            return

        # cursor must move strictly forward -- a rewind/jump means the parked
        # generator frame's `pos` cursor tore across a hub migration.
        if pos <= last_pos:
            H.fail("genops() cursor NON-MONOTONE: op #{0} at pos {1} did not "
                   "advance past previous pos {2} (wid {3}) -- the suspended "
                   "generator frame's cursor rewound/jumped across a "
                   "park".format(nops, pos, last_pos, wid))
            return
        last_pos = pos

        # opcode name must be a real, known opcode -- an unknown name is a torn
        # decode off a corrupted opcode-dispatch table.
        if opinfo.name not in KNOWN_OPCODE_NAMES:
            H.fail("genops() yielded UNKNOWN opcode name {0!r} at op #{1} "
                   "(wid {2}) -- a garbage/torn decode from the shared "
                   "pickletools decode table".format(opinfo.name, nops, wid))
            return
        if opinfo.name == "STOP":
            saw_stop = True

        nops += 1
        state["opcodes_walked"][wid & 1023] += 1

        # PARK mid-iteration: the generator frame is now suspended, live,
        # holding the cursor, while a sibling drives its own genops on another
        # hub.  This is the hazard boundary.
        runloom.yield_now()
        if nops & 3 == 0:
            runloom.sleep(0.0002)

    # a well-formed pickle's opcode walk must END with STOP and consume it all.
    if not saw_stop:
        H.fail("genops() walk of the fiber's pickle ended WITHOUT a STOP "
               "opcode ({0} ops, wid {1}) -- the generator terminated early "
               "with a torn cursor".format(nops, wid))
        return
    if nops <= 0:
        H.fail("genops() yielded ZERO opcodes for a non-empty pickle "
               "(wid {0}) -- the generator produced nothing".format(wid))
        return

    # ---- (2) optimize() + round-trip identity ------------------------------
    try:
        optimized = pickletools.optimize(data)
    except Exception as exc:
        H.fail("pickletools.optimize() raised {0!r} on the fiber's OWN pickle "
               "(wid {1}) -- optimize's internal genops passes desynced".format(
                   exc, wid))
        return

    # optimize only ever strips bytes (dead memo PUTs + FRAME); a LARGER output
    # means the dual-pass rewrite desynced.
    if len(optimized) > len(data):
        H.fail("optimize() GREW the pickle: {0} bytes -> {1} bytes (wid {2}) "
               "-- optimize must only strip dead opcodes, a larger output is a "
               "desynced rewrite".format(len(data), len(optimized), wid))
        return

    # ROUND-TRIP IDENTITY: the optimized pickle must load back to the exact
    # original graph.  A mismatch means optimize() rewrote the memo opcodes
    # wrong under concurrency.
    try:
        restored = pickle.loads(optimized)
    except Exception as exc:
        H.fail("pickle.loads(optimize(data)) RAISED {0!r} (wid {1}) -- the "
               "optimized pickle is malformed; optimize() produced a non-"
               "loadable stream".format(exc, wid))
        return
    if restored != graph:
        H.fail("optimize() ROUND-TRIP BROKEN: loads(optimize(dumps(g))) != g "
               "(wid {0}) -- the optimizer's memo PUT/GET rewrite corrupted "
               "the object graph under M:N".format(wid))
        return

    # re-walk the optimized stream: it too must be a clean, STOP-terminated,
    # monotone opcode walk (optimize must emit well-formed pickles).
    last_pos2 = -1
    saw_stop2 = False
    try:
        for opinfo2, arg2, pos2 in pickletools.genops(optimized):
            if pos2 <= last_pos2:
                H.fail("genops() over OPTIMIZED bytes NON-MONOTONE at pos {0} "
                       "(prev {1}, wid {2}) -- optimize emitted a torn "
                       "stream".format(pos2, last_pos2, wid))
                return
            last_pos2 = pos2
            if opinfo2.name not in KNOWN_OPCODE_NAMES:
                H.fail("genops() over OPTIMIZED bytes yielded UNKNOWN opcode "
                       "{0!r} (wid {1}) -- optimize emitted a garbage "
                       "opcode".format(opinfo2.name, wid))
                return
            if opinfo2.name == "STOP":
                saw_stop2 = True
    except Exception as exc:
        H.fail("genops() over OPTIMIZED bytes raised {0!r} (wid {1}) -- "
               "optimize emitted a malformed stream".format(exc, wid))
        return
    if not saw_stop2:
        H.fail("optimized pickle walk ended WITHOUT STOP (wid {0}) -- optimize "
               "emitted a truncated stream".format(wid))
        return

    state["optimize_checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber repeatedly builds its OWN reuse graph, walks genops parking
    mid-iteration, and verifies optimize round-trip -- all single-owner.  The
    sustained inner loop keeps many fibers parked inside live genops generator
    frames at once so the scheduler reliably resumes one after a sibling ran."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            walk_and_optimize(H, wid, idx, state)     # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        # Non-vacuity tallies (SHARDED wid & 1023, single-writer-per-shard --
        # these feed NO conservation law, only the non-vacuity gate, so a
        # sharded tally is correct here per the harness contract).
        "opcodes_walked": [0] * 1024,        # total genops opcodes walked
        "optimize_checks": [0] * 1024,       # optimize() round-trips verified
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    walked = sum(H.state["opcodes_walked"])
    optchecks = sum(H.state["optimize_checks"])

    H.log("pickletools[single-owner LOAD-BEARING]: {0} genops opcodes walked "
          "(cursor monotone + all opcode names known, fail-fast) | {1} "
          "optimize() round-trips verified (loads(optimize(dumps(g)))==g and "
          "len(opt)<=len(data), fail-fast); ops={2}".format(
              walked, optchecks, H.total_ops()))

    # NON-VACUITY: BOTH layers of the two-layer oracle actually ran.
    H.check(walked > 0,
            "no genops opcodes were walked -- the generator-frame cursor "
            "hazard was never exercised (oracle would be vacuous)")
    H.check(optchecks > 0,
            "no optimize() round-trips were verified -- the optimize rewrite "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a suspended genops
    # generator frame.
    H.require_no_lost("pickletools genops/optimize")


if __name__ == "__main__":
    harness.main(
        "p515_pickletools_optimize_genops_generator", body,
        setup=setup, post=post,
        default_funcs=8000,
        describe="pickletools.genops() is a generator holding a byte-stream "
                 "cursor in its frame; pickletools.optimize() drives dual "
                 "genops passes to strip dead memo PUTs.  LOAD-BEARING: each "
                 "fiber pickles its OWN reuse-graph (memo opcodes present), "
                 "drives genops to completion PARKING mid-iteration and "
                 "asserting the cursor stays strictly monotone with only known "
                 "opcode names, then optimize()s and asserts "
                 "loads(optimized)==original AND len(optimized)<=len(data). A "
                 "non-monotone cursor, unknown opcode, non-round-tripping or "
                 "grown optimize output, or SIGSEGV mid-walk is a runloom "
                 "suspended-generator-frame / shared-decode-table desync. "
                 "Single-owner throughout -- every observation is load-bearing")
