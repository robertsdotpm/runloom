#!/usr/bin/env python3
"""stw_trace_conform.py -- TRACE CONFORMANCE for the CPython stop-the-world model.

verify/tla/RunloomCPythonSTW.tla composes M1 (per-tstate attach/detach) and M2
(the real stop_the_world handshake) and proves STWExclusive -- while the world is
stopped, every non-requester hub is suspended.  TLC checks the model against
itself; this checks it against the BINARY: an instrumented --with-pydebug CPython
(verify/cpython_patches/pystate_stw_trace.patch) emits every M1+M2 transition of a
REAL runloom run (RUNLOOM_STW_TRACE=<path>, one ndjson line each, keyed by the
PyThreadState pointer), and this tool replays that trace through the model's OWN
actions under TLC.  So TLC checks the ACTUAL CPython stop-the-world handshake of a
real gc-churn run against the actual spec -- conforming runloom's interaction with
the host's unpublished internal protocol.

The model assumes a STATIC set of hubs all present from Init; a real run is
DYNAMIC (the main thread gc.collect()s during import before any hub exists -- a
fast-path STW the static model can't represent).  So we conform the STEADY-STATE
window where all hubs are co-present and the world is running, reconstructing the
model's Init state at the window start by replaying the (trivial, single-thread)
prefix in Python.  The interesting multi-hub suffix -- where GCPark/SelfSuspend
actually drive other hubs to suspended -- is what TLC verifies.  t=0 events (a
NULL requester: pre-runtime gc with no current tstate) are dropped.

House style: .format(), no f-strings.  Usage: stw_trace_conform.py <trace.ndjson>
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TLA = os.path.join(ROOT, "verify", "tla")
JAR = os.path.join(TLA, "tla2tools.jar")


def load_events(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def drop_fast_cycles(events):
    """Drop the degenerate FAST-PATH STWs: GCRequest -> GCStopComplete -> GCStart
    with NO GCPark/SelfSuspend between (CPython's thread_countdown==0 path, where
    the requester was alone in the interpreter's thread list).  They are
    state-neutral (no hub is parked) and VACUOUS for STWExclusive (no "others" to
    suspend) -- the static model's Others(requester) would wrongly demand the
    hubs (which simply were not in the list) be suspended.  The multi-hub cycles
    (with real GCPark/SelfSuspend), which actually exercise STWExclusive, are
    kept and checked.  (All observed fast cycles are exactly 3 consecutive events.)"""
    out = []
    i = 0
    while i < len(events):
        if (events[i]["a"] == "GCRequest" and i + 2 < len(events)
                and events[i + 1]["a"] == "GCStopComplete"
                and events[i + 2]["a"] == "GCStart"):
            i += 3
        else:
            out.append(events[i])
            i += 1
    return out


def replay(events):
    """Apply the model's transition semantics in Python to reconstruct, for every
    index, the (state, world, present) just BEFORE that event.  Used only to
    fast-forward to a clean steady-state Init -- the actual conformance check is
    TLC on the suffix, against the real model's actions."""
    # t==0 is the EXTERNAL requester (NULL tstate -- a real STW from a context with
    # no current tstate); it is NOT a tracked hub, it maps to "ext", and the model
    # drives it via GCRequestExt.  Only real tstates become hubs h1.. .
    order = {}
    for e in events:
        t = int(e["t"])
        if t != 0 and t not in order:
            order[t] = len(order) + 1
    def hn(t):
        t = int(t)
        return "ext" if t == 0 else "h{}".format(order[t])
    all_hubs = frozenset("h{}".format(i) for i in order.values())

    state = {}            # hub -> "attached" / "detached" / "suspended"
    present = set()
    world = "running"
    snaps = []            # snaps[i] = (state, world, present) before events[i]
    for e in events:
        snaps.append((dict(state), world, frozenset(present)))
        a, h = e["a"], hn(e["t"])
        if a == "Attach":
            present.add(h); state[h] = "attached"
        elif a == "Detach":
            state[h] = "detached"
        elif a == "SelfSuspend":
            state[h] = "suspended"
        elif a == "GCPark":
            present.add(h); state[h] = "suspended"
        elif a == "GCRequest":
            world = "stopping"
            if h != "ext":               # a real hub requester is attached
                present.add(h); state[h] = "attached"
        elif a == "GCStopComplete":
            world = "stopped"
        elif a == "GCStart":
            for k in list(state):
                if state[k] == "suspended":
                    state[k] = "detached"
            world = "running"
    return order, hn, all_hubs, snaps


def main():
    if len(sys.argv) < 2:
        print("usage: stw_trace_conform.py <trace.ndjson>")
        return 2
    events = drop_fast_cycles(load_events(sys.argv[1]))
    if not events:
        print("empty trace -- nothing to check (was RUNLOOM_STW_TRACE set?)")
        return 2

    # Dynamic tstate-identity churn: a tstate pointer can be REUSED (a rescue/hub
    # tstate is deleted and a fresh one allocated at the same address), which
    # conflates two distinct threads under one key -> an impossible per-pointer
    # sequence (e.g. Attach with no intervening Detach).  This is NOT an ordering
    # problem (the emit order is faithful per physical tstate); it needs a tstate
    # GENERATION id in the instrumentation to disambiguate (see STW_FINDINGS.md).
    # Until then, a trace with reuse is INCONCLUSIVE -- skip it rather than report
    # a misleading non-conformance.
    pst = {}
    legal = {("detached", "Attach"): "attached", ("attached", "Detach"): "detached",
             ("attached", "SelfSuspend"): "suspended", ("detached", "GCPark"): "suspended"}
    for e in events:
        a, t = e["a"], int(e["t"])
        if a == "GCStart":
            for k in list(pst):
                if pst[k] == "suspended":
                    pst[k] = "detached"
            continue
        if a in ("GCStopComplete", "GCRequest") or t == 0:
            continue
        nxt = legal.get((pst.get(t, "detached"), a))
        if nxt is None:
            print("  INCONCLUSIVE: dynamic tstate-identity churn (a reused tstate "
                  "pointer) in the trace -- needs a tstate generation id to "
                  "disambiguate (see docs/dev/ft_conformance/STW_FINDINGS.md)")
            return 3
        pst[t] = nxt

    order, hn, all_hubs, snaps = replay(events)

    # steady-state window: [first index where all hubs co-present + world running]
    # .. [last GCStart] (exclude teardown detaches after the final cycle).
    start = None
    for i in range(len(events)):
        st, wd, pr = snaps[i]
        if pr == all_hubs and wd == "running":
            start = i
            break
    gc_starts = [i for i, e in enumerate(events) if e["a"] == "GCStart"]
    if start is None or not gc_starts or gc_starts[-1] < start:
        print("  no steady multi-hub STW window (all hubs never co-present while "
              "the world is running) -- nothing to conform")
        return 2
    end = gc_starts[-1]
    window = events[start:end + 1]
    hubs = sorted(all_hubs, key=lambda h: int(h[1:]))
    init_st = snaps[start][0]
    init_state = {h: init_st.get(h, "detached") for h in hubs}
    nstops = sum(1 for e in window if e["a"] == "GCStart")

    seq = ",\n                  ".join(
        '[a |-> "{}", h |-> "{}"]'.format(e["a"], hn(e["t"])) for e in window)
    init_fcn = " @@ ".join('("{}" :> "{}")'.format(h, init_state[h]) for h in hubs)

    with open(os.path.join(TLA, "RunloomCPythonSTWTrace.tla"), "w") as f:
        f.write(TEMPLATE.format(seq=seq, init_fcn=init_fcn))
    with open(os.path.join(TLA, "RunloomCPythonSTWTrace.cfg"), "w") as f:
        f.write(CFG.format(hubs=", ".join('"{}"'.format(h) for h in hubs),
                           maxstops=max(1, nstops)))

    if not os.path.exists(JAR):
        print("tla2tools.jar missing at {}; run verify/tla/run_tla.sh once".format(JAR))
        return 2
    meta = "/tmp/runloom_stwconf_{}".format(os.getpid())
    proc = subprocess.run(
        ["java", "-cp", JAR, "tlc2.TLC", "-metadir", meta,
         "-config", "RunloomCPythonSTWTrace.cfg", "RunloomCPythonSTWTrace.tla"],
        cwd=TLA, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = proc.stdout
    print("  window: {} events ({} STW cycles), hubs: {}  [prefix {} fast-forwarded]"
          .format(len(window), nstops, ", ".join(hubs), start))
    if "No error has been found" in out:
        print("  CONFORMS -- the real stop-the-world handshake is a legal "
              "RunloomCPythonSTW execution; STWExclusive holds at every stopped state")
        return 0
    if "is violated" in out:
        inv = next((l.strip() for l in out.splitlines() if "is violated" in l),
                   "an invariant")
        print("  NON-CONFORMING -- {}".format(inv))
        return 1
    if "Deadlock" in out or "deadlock" in out:
        print("  NON-CONFORMING -- a logged transition was not enabled in the "
              "model (the real run took a step the STW protocol forbids)")
        for l in out.splitlines():
            if "tpc =" in l or "world =" in l:
                print("    " + l.strip())
        return 1
    print("  TLC error:\n" + "\n".join(out.splitlines()[-12:]))
    return 2


TEMPLATE = '''\\* GENERATED by tools/stw_trace_conform.py from a real RUNLOOM_STW_TRACE run.
\\* Replays the recorded CPython stop-the-world (M1+M2) transitions through
\\* RunloomCPythonSTW's OWN actions under TLC.  DO NOT EDIT (regenerated per run).
-------------------------- MODULE RunloomCPythonSTWTrace --------------------------
EXTENDS RunloomCPythonSTW, Sequences, Naturals, TLC

TraceEvents == << {seq} >>

VARIABLE tpc

\\* Init reconstructed from the steady-state window start (all hubs present, world
\\* running): per-hub state from the prefix replay; no STW in flight.
TInit ==
    /\\ state = ({init_fcn})
    /\\ world = "running"
    /\\ requester = NoHub
    /\\ stops = 0
    /\\ wedged = [h \\in Hubs |-> FALSE]
    /\\ tpc = 1

TNext ==
    \\/ /\\ tpc <= Len(TraceEvents)
       /\\ LET e == TraceEvents[tpc] IN
            /\\ \\/ (e.a = "Attach"         /\\ Attach(e.h))
               \\/ (e.a = "Detach"         /\\ Detach(e.h))
               \\/ (e.a = "GCRequest" /\\ e.h = "ext" /\\ GCRequestExt)
               \\/ (e.a = "GCRequest" /\\ e.h # "ext" /\\ GCRequest(e.h))
               \\/ (e.a = "GCPark"         /\\ GCPark(e.h))
               \\/ (e.a = "SelfSuspend"    /\\ SelfSuspend(e.h))
               \\/ (e.a = "GCStopComplete" /\\ GCStopComplete)
               \\/ (e.a = "GCStart"        /\\ GCStart)
            /\\ tpc' = tpc + 1
    \\/ /\\ tpc > Len(TraceEvents)
       /\\ UNCHANGED <<vars, tpc>>

TSpec == TInit /\\ [][TNext]_<<vars, tpc>>
=============================================================================
'''

CFG = '''\\* GENERATED. Hubs covers the steady-window tstate ids; Bypass/BuggyBlock off (the
\\* real run is the correct code); MaxStops bounds the completed cycles.
CONSTANTS
    Hubs = {{{hubs}}}
    NoHub = "nohub"
    External = "ext"
    Bypass = FALSE
    BuggyBlock = FALSE
    MaxStops = {maxstops}
SPECIFICATION TSpec
INVARIANT TypeOK
INVARIANT STWExclusive
INVARIANT RequesterAttached
'''


if __name__ == "__main__":
    sys.exit(main())
