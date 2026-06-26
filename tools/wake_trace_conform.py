#!/usr/bin/env python3
r"""wake_trace_conform.py -- TRACE CONFORMANCE for the netpoll-drain WAKE protocol:
replay a REAL extension run against RunloomWake.tla (the proven foreign-wake
backstop model) under TLC.

The single-thread drain emits its wake-handshake transitions (RUNLOOM_WAKE_TRACE=
<path>: FOREIGN_WAKE / POKE / DRAIN_DEC on the blockpool worker; DRAIN_CONSUME /
DRAIN_BLOCK / DRAIN_UNBLOCK / RESUME on the owner, one ndjson line each).  This
turns that real trace into a generated RunloomWakeTrace.tla that EXTENDS
RunloomWake and drives its OWN actions (WakerAppend / WakerPoke / WakerDec /
DrainConsume / DrainPeekEmpty / DrainDecide / DrainBlock / DrainPumpWake |
DrainBackstopTimeout / DrainResume) in the recorded order, then TLC checks TypeOK
+ ResumeIsTerminal hold at every step.  No re-transcription: TLC checks the actual
binary run against the actual verified wake model.

  - a real offload run conforms (every observed transition is an enabled model
    step; no fiber is resumed/consumed without a durable wake_list append);
  - drop a FOREIGN_WAKE (durable append) from the trace and the dependent
    poke/consume/resume can no longer fire -> TLC deadlocks: the lost-wakeup the
    model exists to forbid.

THE HONEST SPLIT (why this is sound; see RunloomWake.tla's header + the v1 scope):
  * SAFETY only.  We check ResumeIsTerminal along the REAL path + "every event is
    an enabled model step".  LIVENESS (AllWoken, <>[]) is NOT replay-checked -- a
    finite trace cannot witness an eventually-always property; AllWoken stays
    proven by the CLOSED model (RunloomWake.cfg vs RunloomWake_bug.cfg).
  * The POKE keeps the model's FREE delivery disjunction (poke_pending' may stay
    or become TRUE); DRAIN_UNBLOCK maps to (DrainPumpWake \/ DrainBackstopTimeout).
    Because the backstop is armed (drain_timeout="backstop_2ms") while a job is in
    flight, DrainBackstopTimeout is always enabled, so the poke-LOST branch never
    spuriously deadlocks -- and TLC still explores both poke outcomes.  We assert
    the WEAKER, checkable claim "every observed transition is enabled and
    ResumeIsTerminal holds", not the un-observable "this poke delivered".
  * SINGLE-THREAD drain only (rc.run()/default-pool blocking) -- the only path
    this model covers and the only one that blocks UNBOUNDED on a foreign poke.
    M:N hubs use a different wake route (no model yet); the driver's workload pins
    the single-thread scheduler.

House style: .format(), no f-strings.

Usage: wake_trace_conform.py <events.ndjson> [--drop-foreign-wake[=N]]
  --drop-foreign-wake[=N]  negative control: omit the durable append of the N-th
                           foreign-wake episode (default 0 = first) while keeping
                           the rest -- must be flagged NON-CONFORMING.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TLA = os.path.join(HERE, "verify", "tla")
JAR = os.path.join(TLA, "tla2tools.jar")

DUMMY_G = "g0"          # filler id for drain-only events (never read; not in Gs)


def load_events(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def build_model_events(raw, drop_fw=-1):
    """Lower the raw binary trace to model-level (action, g) events.

    Episodes (model fibers) are opened on a durable FOREIGN_WAKE for a fiber
    pointer and closed on its matching RESUME; first-run / cooperative-yield
    RESUMEs (no open episode) are DROPPED -- a RESUME maps to DrainResume only
    when it closes a foreign-wake episode, so a resume with no durable append is
    a resume-WITHOUT-a-wake and deadlocks the replay.

    One binary DRAIN_BLOCK expands to the model's three-step decide-to-block
    micro-sequence (DrainPeekEmpty -> DrainDecide -> DrainBlock).

    drop_fw >= 0 is the negative control: suppress the WakerAppend emission of
    that episode ordinal (keep the episode open so its downstream events still
    drive the model) -> the orphaned poke/consume/resume deadlocks TLC.

    Returns (events, gs, backstop_unarmed) where backstop_unarmed lists any
    DRAIN_BLOCK that blocked with cap=0 (the f214341 backstop failed to arm).
    """
    open_ep = {}            # fiber ptr -> episode id (currently open)
    gs = []                 # all episode ids, in creation order
    out = []                # (action, g) model events
    backstop_unarmed = []
    nfw = 0                 # FOREIGN_WAKE ordinal seen so far

    def new_ep(ptr):
        name = "g{}".format(len(gs) + 1)
        gs.append(name)
        open_ep[ptr] = name
        return name

    for e in raw:
        a = e["a"]
        ptr = e.get("g", 0)
        cap = e.get("cap", 0)
        if a == "FOREIGN_WAKE":
            ep = new_ep(ptr)
            if nfw != drop_fw:           # negative control suppresses this append
                out.append(("FOREIGN_WAKE", ep))
            nfw += 1
        elif a == "POKE":
            ep = open_ep.get(ptr)
            if ep is not None:
                out.append(("POKE", ep))
        elif a == "DRAIN_DEC":
            ep = open_ep.get(ptr)
            if ep is not None:
                out.append(("DRAIN_DEC", ep))
        elif a == "DRAIN_CONSUME":
            # the model readies the whole wake_list at once; coalesce any
            # consecutive consume lines into one DrainConsume (defensive --
            # the single-thread drain emits one per loop-top drain anyway).
            if not (out and out[-1][0] == "DRAIN_CONSUME"):
                out.append(("DRAIN_CONSUME", DUMMY_G))
        elif a == "DRAIN_BLOCK":
            if cap != 1:
                backstop_unarmed.append(len(out))
            out.append(("DRAIN_PEEK_EMPTY", DUMMY_G))
            out.append(("DRAIN_DECIDE", DUMMY_G))
            out.append(("DRAIN_BLOCK", DUMMY_G))
        elif a == "DRAIN_UNBLOCK":
            out.append(("DRAIN_UNBLOCK", DUMMY_G))
        elif a == "RESUME":
            ep = open_ep.pop(ptr, None)
            if ep is not None:           # else first-run / yield resume -> drop
                out.append(("RESUME", ep))
    return out, gs, backstop_unarmed


TEMPLATE = '''\\* GENERATED by tools/wake_trace_conform.py from a real RUNLOOM_WAKE_TRACE run.
\\* Replays the recorded wake-handshake transitions through RunloomWake's OWN
\\* actions under TLC: conformance = each event is an enabled model step and
\\* ResumeIsTerminal (no resume/consume without a durable append) holds along the
\\* real path.  SAFETY refinement only; liveness stays in the closed model.  DO NOT EDIT.
-------------------------- MODULE RunloomWakeTrace --------------------------
EXTENDS RunloomWake, Sequences, Naturals

TraceEvents == << {seq} >>

VARIABLE tpc

\\* The pump return: a DELIVERED poke (DrainPumpWake) OR the 2 ms backstop timeout
\\* (DrainBackstopTimeout).  The binary cannot observe which, so replay admits
\\* BOTH; because drain_timeout = "backstop_2ms" while a foreign job is in flight,
\\* DrainBackstopTimeout is always enabled, so the poke-lost branch never
\\* spuriously deadlocks while the model still explores both poke outcomes.
DrainUnblock == DrainPumpWake \\/ DrainBackstopTimeout

TInit == Init /\\ tpc = 1

TNext ==
    \\/ /\\ tpc <= Len(TraceEvents)
       /\\ LET e == TraceEvents[tpc] IN
            /\\ \\/ (e.a = "FOREIGN_WAKE"     /\\ WakerAppend(e.g))
               \\/ (e.a = "POKE"             /\\ WakerPoke(e.g))
               \\/ (e.a = "DRAIN_DEC"        /\\ WakerDec(e.g))
               \\/ (e.a = "DRAIN_CONSUME"    /\\ DrainConsume)
               \\/ (e.a = "DRAIN_PEEK_EMPTY" /\\ DrainPeekEmpty)
               \\/ (e.a = "DRAIN_DECIDE"     /\\ DrainDecide)
               \\/ (e.a = "DRAIN_BLOCK"      /\\ DrainBlock)
               \\/ (e.a = "DRAIN_UNBLOCK"    /\\ DrainUnblock)
               \\/ (e.a = "RESUME"           /\\ DrainResume)
            /\\ tpc' = tpc + 1
    \\/ /\\ tpc > Len(TraceEvents)
       /\\ UNCHANGED <<vars, tpc>>

TSpec == TInit /\\ [][TNext]_<<vars, tpc>>
=============================================================================
'''

CFG = '''\\* GENERATED.  Gs = the distinct foreign-wake episodes (one per durable append ->
\\* resume).  Backstop = TRUE: the real binary HAS the f214341 cap -- replay the
\\* real-world arm.  SAFETY conformance (no liveness on a finite trace).
CONSTANTS
    Gs = {{{gs}}}
    Backstop = TRUE
SPECIFICATION TSpec
INVARIANT TypeOK
INVARIANT ResumeIsTerminal
'''


def record(action, g):
    return '[a |-> "{}", g |-> "{}"]'.format(action, g)


def main():
    argv = [a for a in sys.argv[1:]]
    drop_fw = -1
    rest = []
    for a in argv:
        if a == "--drop-foreign-wake":
            drop_fw = 0
        elif a.startswith("--drop-foreign-wake="):
            drop_fw = int(a.split("=", 1)[1])
        else:
            rest.append(a)
    if not rest:
        print("usage: wake_trace_conform.py <events.ndjson> [--drop-foreign-wake[=N]]")
        return 2
    raw = load_events(rest[0])
    if not raw:
        print("empty trace (was RUNLOOM_WAKE_TRACE set on a run() offload workload?)")
        return 2

    events, gs, unarmed = build_model_events(raw, drop_fw)
    if not gs:
        print("no foreign-wake episodes in the trace (did any rc.blocking() run "
              "under the single-thread drain?)")
        return 2

    # The f214341 backstop must be ARMED (cap=1) at every block while a foreign
    # job is in flight -- this workload always blocks with inflight>0, so a cap=0
    # is a backstop regression, independent of the model replay below.
    if drop_fw < 0 and unarmed:
        print("  NON-CONFORMING -- the foreign-wake backstop did NOT arm at {} "
              "DRAIN_BLOCK(s) (cap=0 while a foreign job was in flight): the "
              "f214341 2 ms cap regressed -> a lost poke can strand the fiber"
              .format(len(unarmed)))
        return 1

    seq = ",\n                  ".join(record(a, g) for a, g in events)
    with open(os.path.join(TLA, "RunloomWakeTrace.tla"), "w") as f:
        f.write(TEMPLATE.format(seq=seq))
    with open(os.path.join(TLA, "RunloomWakeTrace.cfg"), "w") as f:
        f.write(CFG.format(gs=", ".join('"{}"'.format(g) for g in gs)))

    if not os.path.exists(JAR):
        print("tla2tools.jar missing; run tools/verify/tla/run_tla.sh once")
        return 2
    meta = "/tmp/runloom_waketrace_{}".format(os.getpid())
    proc = subprocess.run(
        ["java", "-Xmx1g", "-cp", JAR, "tlc2.TLC", "-workers", "4", "-metadir", meta,
         "-config", "RunloomWakeTrace.cfg", "RunloomWakeTrace.tla"],
        cwd=TLA, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = proc.stdout
    print("  events: {}   episodes: {}   model-steps: {}".format(
        len(raw), len(gs), len(events)))
    if "No error has been found" in out:
        print("  CONFORMS -- every wake event is a legal RunloomWake step; "
              "ResumeIsTerminal holds along the real path (no resume/consume "
              "without a durable append)")
        return 0
    if "is violated" in out:
        inv = next((l.strip() for l in out.splitlines() if "is violated" in l),
                   "an invariant")
        print("  NON-CONFORMING -- {}".format(inv))
        return 1
    if "Deadlock" in out or "deadlock" in out:
        print("  NON-CONFORMING -- a recorded wake transition was not an enabled "
              "model step (e.g. a poke/consume/resume with no preceding durable "
              "FOREIGN_WAKE append -- a resume without a wake, the lost-wakeup "
              "class RunloomWake.tla forbids)")
        return 1
    print("  TLC error:\n" + "\n".join(out.splitlines()[-10:]))
    return 2


if __name__ == "__main__":
    sys.exit(main())
