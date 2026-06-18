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
    raw = load_events(sys.argv[1])
    # The emit (file) order is NOT a happens-before-consistent order across threads:
    # a requester's GCPark(id) can be logged before id's own Detach whose published
    # DETACHED the GCPark CAS read (the cross-thread M1 emit-lag).  Each event carries
    # a seq-cst logical-clock stamp s (runloom_stw_tick, placed BEFORE a publishing
    # store / AFTER a dependent read in pystate.c), so sorting by s reconstructs a
    # linearization that respects every happens-before edge -- the sequential model
    # can then replay it.  Events are keyed by the tstate's UNIQUE id (field t =
    # tstate->id), so a REUSED PyThreadState pointer no longer conflates two threads.
    # Together these close the two mechanisms STW_FINDINGS.md identified (emit-lag +
    # pointer reuse), so a churny run now CONFORMS rather than being skipped.
    raw.sort(key=lambda e: e.get("s", 0))
    events = drop_fast_cycles(raw)
    if not events:
        print("empty trace -- nothing to check (was RUNLOOM_STW_TRACE set?)")
        return 2

    # Per-id sanity pre-check.  Now that events are id-keyed and seq-sorted, a churny
    # run no longer produces impossible per-id sequences (reuse disambiguated by the
    # id key, emit-lag by the seq sort).  A residual illegal transition would be a
    # REAL finding, not "inconclusive"; surface it but let TLC give the precise
    # verdict (it localizes which event the model couldn't take).
    pst = {}
    legal = {("detached", "Attach"): "attached", ("attached", "Detach"): "detached",
             ("attached", "SelfSuspend"): "suspended", ("detached", "GCPark"): "suspended"}
    illegal = 0
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
            illegal += 1
        else:
            pst[t] = nxt
    if illegal:
        print("  note: {} residual illegal per-id transition(s) after id+seq "
              "ordering -- TLC will localize".format(illegal))

    order, hn, all_hubs, snaps = replay(events)

    # Per-id lifetime (first/last index in the sorted, de-fast event list).  Real
    # runs CHURN: transient tstates -- runloom rescue tstates and short-lived native
    # threads, whose addresses the allocator recycles (hence the UNIQUE id key) --
    # enter and leave, so there is no instant when every distinct id is co-present.
    # We therefore track PRESENCE explicitly: the window opens once the long-lived
    # ("stable") hubs are all up, and the model is driven Create(h)/Destroy(h) as
    # transients enter/leave, with STW completion and STWExclusive quantified over
    # the PRESENT hubs only.  This all lives in the generated trace module; the base
    # RunloomCPythonSTW model is untouched (its 4 gated checks are unaffected).
    first = {}
    last = {}
    for i, e in enumerate(events):
        h = hn(e["t"])
        if h == "ext":
            continue
        first.setdefault(h, i)
        last[h] = i

    gc_starts = [i for i, e in enumerate(events) if e["a"] == "GCStart"]
    if not gc_starts:
        print("  no STW cycle in the trace -- nothing to conform")
        return 2
    end = gc_starts[-1]

    def present_at(i):
        return frozenset(h for h in first if first[h] <= i <= last[h])

    # "stable" hubs bound the window: a hub whose lifetime spans the last GCStart.
    stable = frozenset(h for h in first if first[h] <= end <= last[h])
    if not stable:
        print("  no hub spans the steady window -- nothing to conform")
        return 2
    # window start: first world="running" index where every stable hub is present
    # (past the spawn warm-up, where the main thread STWs alone).
    start = None
    for i in range(len(events)):
        if snaps[i][1] == "running" and stable <= present_at(i):
            start = i
            break
    if start is None or end < start:
        print("  no steady multi-hub STW window -- nothing to conform")
        return 2

    # Augment the window with injected Create/Destroy so the model's `present` set
    # tracks reality, walking a running per-hub model state (replay() semantics).
    window_ids = sorted(
        {hn(e["t"]) for e in events[start:end + 1] if hn(e["t"]) != "ext"},
        key=lambda h: int(h[1:]))
    init_st = snaps[start][0]
    st = {h: init_st.get(h, "detached") for h in window_ids}
    init_present = present_at(start) & frozenset(window_ids)
    present = set(init_present)

    def step_state(a, h):
        if a == "Attach":
            st[h] = "attached"
        elif a == "Detach":
            st[h] = "detached"
        elif a in ("SelfSuspend", "GCPark"):
            st[h] = "suspended"
        elif a == "GCRequest" and h != "ext":
            st[h] = "attached"
        elif a == "GCStart":
            for k in list(st):
                if st[k] == "suspended":
                    st[k] = "detached"

    aug = []   # list of (action, hubname), with Create/Destroy injected
    for i in range(start, end + 1):
        e = events[i]
        a = e["a"]
        h = hn(e["t"])
        if h != "ext" and h not in present:
            aug.append(("Create", h))
            present.add(h)
            st[h] = "detached"
        aug.append((a, h))
        step_state(a, h)
        # a transient whose LAST global event is here (and not the window end)
        # departs: bring it to detached, then remove it from the thread list.
        if h != "ext" and last.get(h, -1) == i and i < end and h in present:
            if st[h] == "attached":
                aug.append(("Detach", h))
                st[h] = "detached"
            if st[h] == "detached":
                aug.append(("Destroy", h))
                present.discard(h)
            # if still "suspended" it is parked -- a later GCStart resumes it; leave.

    nstops = sum(1 for a, _ in aug if a == "GCStart")
    seq = ",\n                  ".join(
        '[a |-> "{}", h |-> "{}"]'.format(a, h) for a, h in aug)
    init_fcn = " @@ ".join('("{}" :> "{}")'.format(h, init_st.get(h, "detached"))
                           for h in window_ids)
    init_present_set = "{{{}}}".format(", ".join(
        '"{}"'.format(h) for h in sorted(init_present, key=lambda h: int(h[1:]))))
    hubs = window_ids

    with open(os.path.join(TLA, "RunloomCPythonSTWTrace.tla"), "w") as f:
        f.write(TEMPLATE.format(seq=seq, init_fcn=init_fcn,
                                init_present=init_present_set))
    with open(os.path.join(TLA, "RunloomCPythonSTWTrace.cfg"), "w") as f:
        f.write(CFG.format(hubs=", ".join('"{}"'.format(h) for h in hubs),
                           maxstops=max(1, nstops)))

    if not os.path.exists(JAR):
        print("tla2tools.jar missing at {}; run verify/tla/run_tla.sh once".format(JAR))
        return 2
    meta = "/tmp/runloom_stwconf_{}".format(os.getpid())
    proc = subprocess.run(
        ["java", "-Xmx1g", "-cp", JAR, "tlc2.TLC", "-workers", "4", "-metadir", meta,
         "-config", "RunloomCPythonSTWTrace.cfg", "RunloomCPythonSTWTrace.tla"],
        cwd=TLA, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = proc.stdout
    print("  window: {} events ({} STW cycles), {} hubs ({} present at start, "
          "transients Create/Destroy'd)  [prefix {} fast-forwarded]"
          .format(len(aug), nstops, len(hubs), len(init_present), start))
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
\\*
\\* PRESENCE: a real run churns -- transient tstates (rescue tstates, short-lived
\\* native threads at recycled addresses) come and go, so not every id is co-present.
\\* This module adds a `present` set (the live tstates) driven by Create/Destroy that
\\* the tool injects from each id's first/last appearance, and quantifies STW
\\* completion + STWExclusive over PRESENT hubs only.  The base RunloomCPythonSTW
\\* model is untouched -- so its 4 gated checks (correct/bug/live/livebug) are
\\* unaffected; only this generated driver knows about presence.
-------------------------- MODULE RunloomCPythonSTWTrace --------------------------
EXTENDS RunloomCPythonSTW, Sequences, Naturals, TLC

TraceEvents == << {seq} >>

VARIABLE tpc, present

\\* A tstate enters the thread list (a fresh PyThreadState, detached) ...
TCreate(h) ==
    /\\ h \\notin present
    /\\ present' = present \\cup {{h}}
    /\\ state' = [state EXCEPT ![h] = "detached"]
    /\\ UNCHANGED <<world, requester, stops, wedged>>

\\* ... and leaves it (deleted while detached, never the requester, never mid-stop).
TDestroy(h) ==
    /\\ h \\in present
    /\\ h # requester
    /\\ state[h] = "detached"
    /\\ present' = present \\ {{h}}
    /\\ UNCHANGED <<state, world, requester, stops, wedged>>

\\* stop_the_world completes once every PRESENT other hub is suspended (a not-present
\\* tstate is not in the interpreter's thread list, so the requester never waits on it).
TGCStopComplete ==
    /\\ world = "stopping"
    /\\ \\A h \\in (present \\ {{requester}}) : state[h] = "suspended"
    /\\ world' = "stopped"
    /\\ UNCHANGED <<state, requester, stops, wedged, present>>

\\* Init reconstructed from the window start: per-hub state from the prefix replay;
\\* `present` is exactly the tstates alive at the start; no STW in flight.
TInit ==
    /\\ state = ({init_fcn})
    /\\ world = "running"
    /\\ requester = NoHub
    /\\ stops = 0
    /\\ wedged = [h \\in Hubs |-> FALSE]
    /\\ tpc = 1
    /\\ present = {init_present}

TNext ==
    \\/ /\\ tpc <= Len(TraceEvents)
       /\\ LET e == TraceEvents[tpc] IN
            /\\ \\/ (e.a = "Attach"         /\\ Attach(e.h)        /\\ UNCHANGED present)
               \\/ (e.a = "Detach" /\\ e.h # requester /\\ Detach(e.h) /\\ UNCHANGED present)
               \\/ (e.a = "Detach" /\\ e.h = requester /\\ RequesterPause(e.h) /\\ UNCHANGED present)
               \\/ (e.a = "GCRequest" /\\ e.h = "ext" /\\ GCRequestExt /\\ UNCHANGED present)
               \\/ (e.a = "GCRequest" /\\ e.h # "ext" /\\ GCRequest(e.h) /\\ UNCHANGED present)
               \\/ (e.a = "GCPark"         /\\ GCPark(e.h)       /\\ UNCHANGED present)
               \\/ (e.a = "SelfSuspend"    /\\ SelfSuspend(e.h)  /\\ UNCHANGED present)
               \\/ (e.a = "GCStopComplete" /\\ TGCStopComplete)
               \\/ (e.a = "GCStart"        /\\ GCStart           /\\ UNCHANGED present)
               \\/ (e.a = "Create"         /\\ TCreate(e.h))
               \\/ (e.a = "Destroy"        /\\ TDestroy(e.h))
            /\\ tpc' = tpc + 1
    \\/ /\\ tpc > Len(TraceEvents)
       /\\ UNCHANGED <<vars, tpc, present>>

\\* present-aware safety + typing (checked instead of the base model's, which
\\* quantify over ALL Hubs and would wrongly demand a departed tstate be suspended).
TPresentOK == present \\subseteq Hubs
TSTWExclusive ==
    (world = "stopped") => \\A h \\in (present \\ {{requester}}) : state[h] = "suspended"
TRequesterAttached ==
    (world = "stopped" /\\ requester # External)
        => (requester \\in present /\\ state[requester] = "attached")

TSpec == TInit /\\ [][TNext]_<<vars, tpc, present>>
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
INVARIANT TPresentOK
INVARIANT TSTWExclusive
INVARIANT TRequesterAttached
'''


if __name__ == "__main__":
    sys.exit(main())
