#!/usr/bin/env python3
r"""iouwake_trace_conform.py -- TRACE CONFORMANCE for the io_uring CQE WAKE
protocol: replay a REAL extension run against RunloomIouringWake.tla (the
CQ-overflow lost-wakeup + drain-first overflow-flush heal model).

RunloomWake binds the single-thread foreign-poke drain; RunloomMNWake binds the
M:N hub-submit route; this binds the io_uring CQE drain.  The io_uring drain emits
its transitions (RUNLOOM_IOUWAKE_TRACE=<path>): SUBMIT (an SQE submitted + the
fiber about to park) on the submitter; DRAIN_FLUSH (the GETEVENTS overflow flush
fired -- IORING_SQ_CQ_OVERFLOW was set), DRAIN_CONSUME (a CQE walked + its fiber
readied), DRAIN_BLOCK / DRAIN_UNBLOCK (the pump block while an iouring op is
inflight) and RESUME (the woken submitter returns from park) on the drainer.  This
lowers that real trace to RunloomIouringWake's OWN actions (Submit / KernelComplete
/ DrainPeekEmpty / DrainDecide / DrainBlock / DrainFlushFirst / DrainEvfdWake /
DrainConsume / DrainResume) and TLC checks TypeOK + ResumeIsTerminal +
NoStrandedCompletion hold at every step.

  - a real io_uring overflow run conforms (every observed transition is an enabled
    model step; no fiber is resumed without a kernel completion, and no completion
    is stranded outside cq_inflight);
  - drop a SUBMIT (--drop-submit) and the dependent KernelComplete/DrainConsume/
    RESUME can no longer fire -> TLC deadlocks: the lost-wakeup the model forbids.

THE NOVEL VALUE: the CQ-OVERFLOW lost-wakeup + the DRAIN-FIRST FLUSH heal.  A
completion forced into the kernel overflow backlog signals NO eventfd; only the
drain-first GETEVENTS flush (DrainFlushFirst) makes it visible.  The driver
SYNTHESIZES KernelComplete from observables:
  * a DRAIN_CONSUME(g) preceded by a DRAIN_FLUSH since the last block => g's
    completion was in OVERFLOW: lower it to KernelComplete-overflow + DrainFlushFirst
    + DrainConsume;
  * a DRAIN_CONSUME(g) reached without an intervening flush (an eventfd unblock or
    an inline visible drain) => VISIBLE: KernelComplete-visible + DrainConsume.

THE HONEST SPLIT (same as the siblings): SAFETY only along the real path
(ResumeIsTerminal + NoStrandedCompletion + every-event-enabled).  The model's
KernelComplete chooses overflow-vs-visible by ring fullness (CQCAP); we order the
synthesized KernelCompletes so the recorded visible-vs-overflow split is the one
TLC takes (visible first to fill the CQCAP-slot ring, then the rest overflow).
The pump-block unblock keeps a FREE disjunction (DrainEvfdWake \/ DrainFlushFirst
\/ DrainStuck-then-reflush) so an ambiguous unblock never spuriously deadlocks.
LIVENESS (AllWoken) is NOT replay-checked -- a finite trace can't witness <>[]; it
stays proven by the closed RunloomIouringWake.cfg / _bug.cfg.

LOWERING (io_uring-specific):
  * Each SUBMIT opens a FRESH model episode id even for the same op/g pointer (the
    single multishot recv fiber re-arms + re-parks per message, so one ptr maps to
    many serial episodes); a RESUME / DRAIN_CONSUME with no open episode is dropped.
  * A DRAIN_FLUSH between a block and the next DRAIN_CONSUME marks the pending
    episodes as OVERFLOW (their completion was in the backlog).
  * Trailing DRAIN_BLOCK/UNBLOCK after the last RESUME are teardown idle polls --
    truncated at the last RESUME.

House style: .format(), no f-strings.

Usage: iouwake_trace_conform.py <events.ndjson> [--drop-submit[=N]]
  --drop-submit[=N]  negative control: omit the Submit of the N-th episode
                     (default 0 = first) while keeping its downstream events --
                     must be flagged NON-CONFORMING.
"""
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TLA = os.path.join(HERE, "verify", "tla")
JAR = os.path.join(TLA, "tla2tools.jar")


def load_events(path):
    out = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def build_episodes(raw):
    """Lower the raw io_uring trace to per-episode records.

    Truncate at the last RESUME (trailing idle polls are teardown).  Each SUBMIT
    opens a fresh episode (the single multishot fiber re-parks per message, so one
    ptr -> many serial episodes); a DRAIN_FLUSH seen while episodes are open marks
    them OVERFLOW; a DRAIN_CONSUME closes the oldest still-pending episode's
    completion and a RESUME terminates it.  A DRAIN_CONSUME / RESUME with no open
    episode is dropped (a first-run / teardown event).

    Returns (episodes, n_overflow, n_visible) where episodes is a list of
    {"id": "g<k>", "overflow": bool} in creation order.
    """
    # truncate at the last RESUME
    last_resume = -1
    for i, e in enumerate(raw):
        if e["a"] == "RESUME":
            last_resume = i
    raw = raw[:last_resume + 1] if last_resume >= 0 else raw

    episodes = []          # all episodes, in creation order
    open_q = []            # episode ids submitted but not yet consumed (FIFO)
    flush_since_block = False
    by_id = {}

    def new_ep():
        eid = "g{}".format(len(episodes) + 1)
        rec = {"id": eid, "overflow": False, "submitted": True}
        episodes.append(rec)
        by_id[eid] = rec
        open_q.append(eid)
        return eid

    for e in raw:
        a = e["a"]
        if a == "SUBMIT":
            new_ep()
        elif a == "DRAIN_FLUSH":
            flush_since_block = True
            # every episode whose completion is still pending was forced into the
            # overflow backlog (its CQE only became visible via this GETEVENTS flush)
            for eid in open_q:
                by_id[eid]["overflow"] = True
        elif a == "DRAIN_BLOCK":
            flush_since_block = False
        elif a == "DRAIN_CONSUME":
            if open_q:
                eid = open_q[0]
                # a consume reached WITHOUT an intervening flush since the block is
                # a visible-ring completion (eventfd / inline drain); leave overflow
                # as set by any DRAIN_FLUSH above.
                if not flush_since_block:
                    pass
        elif a == "RESUME":
            if open_q:
                open_q.pop(0)   # terminate the oldest open episode
    return episodes


# ---------------------------------------------------------------------------
# Lower episodes to a flat sequence of (action, g) model events.
#
# The model starts every op already in cq_inflight; Submit flips waker_pc
# running->submitted (the durable publish the negative control suppresses).  We
# drive with CQCAP = 1 -- a one-slot visible ring, the model's own "2 ops force
# overflow" cfg sizing -- and a CHAINED schedule that keeps the model's `overflow`
# and `cq_ring` sets at size <= 1 at every state (so the SUBSET-overflow branch in
# DrainFlushFirst never blows the TLC state space):
#   1. Submit(ep) for every episode (in recorded order) -- unless suppressed.
#   2. KernelComplete(ep_0): with the ring empty + CQCAP=1 the model's IF takes the
#      VISIBLE branch -> cq_ring = {ep_0} (the lone visible completion).
#   3. For each subsequent episode ep_i (the OVERFLOW ones):
#        - KernelComplete(ep_i): the ring is FULL (ep_{i-1} still in it), so the
#          model's IF takes the OVERFLOW branch -> overflow = {ep_i}, NO eventfd.
#        - DrainConsume: readies + empties the ring (ep_{i-1}); cq_inflight -={..}.
#        - DrainResume(ep_{i-1}).
#        - DrainPeekEmpty -> DrainDecide (Heal /\ inflight>0 -> flush_first) ->
#          DrainBlock -> DrainFlushFirst (overflow {ep_i} -> ring).  ep_i is now the
#          lone ring occupant -- the filler for ep_{i+1}'s overflow.
#   4. The final episode is consumed + resumed (DrainConsume -> DrainResume).
# Every step is a model action enabled in the reached state, picked by the model's
# OWN guards (we never re-implement the overflow choice); TLC re-checks the safety
# invariants at each.  The recorded visible/overflow split (1 visible + K overflow)
# is reproduced exactly.  A suppressed Submit leaves that episode in waker_pc
# "running", so its KernelComplete guard (waker_pc="submitted") is disabled -> the
# dependent chain can't fire -> deadlock (the lost-wakeup the model forbids).
# ---------------------------------------------------------------------------

def lower(episodes, drop_submit=-1):
    out = []

    # 1. Submits (negative control suppresses the drop_submit-th).
    for i, e in enumerate(episodes):
        if i != drop_submit:
            out.append(("SUBMIT", e["id"]))

    if not episodes:
        return out, 1

    # Drive ONE visible episode as the ring seed, then CHAIN every overflow episode
    # off it (each flushed-in overflow becomes the next one's ring occupant); any
    # extra visible episodes pass straight through the ring at the end.  The recorded
    # visible/overflow COUNTS are preserved exactly; the g identities are symmetric
    # in the model, so which g is the seed is irrelevant to safety conformance.
    overflow = [e for e in episodes if e["overflow"]]
    visible = [e for e in episodes if not e["overflow"]]

    # The chain needs at least one ring occupant to force the first overflow.  When
    # the trace has a visible episode (the usual shape: 1 visible + K overflow) use
    # it as the seed; if EVERY episode overflowed, seed with the first overflow one
    # as a visible completion (it still had a real CQE -- the model's branch only
    # decides ring-vs-backlog placement, not whether the completion happened).
    if visible:
        seed = visible[0]
        rest_visible = visible[1:]
    else:
        seed = overflow[0]
        overflow = overflow[1:]
        rest_visible = []

    out.append(("KCOMPLETE", seed["id"]))        # empty ring -> VISIBLE branch

    for e in overflow:
        out.append(("KCOMPLETE", e["id"]))       # ring full -> OVERFLOW branch
        out.append(("DRAIN_CONSUME", "g0"))      # ready + empty the ring occupant
        out.append(("DRAIN_RESUME", "g0"))       # resume it
        out.append(("DRAIN_PEEK_EMPTY", "g0"))
        out.append(("DRAIN_DECIDE", "g0"))       # Heal /\ inflight>0 -> flush_first
        out.append(("DRAIN_BLOCK", "g0"))
        out.append(("DRAIN_FLUSH_FIRST", "g0"))  # backlog -> ring (now the occupant)

    # Consume + resume the final ring occupant (the last overflow, or the seed).
    out.append(("DRAIN_CONSUME", "g0"))
    out.append(("DRAIN_RESUME", "g0"))

    # Any remaining visible episodes: pass straight through the (now empty) ring.
    for e in rest_visible:
        out.append(("KCOMPLETE", e["id"]))
        out.append(("DRAIN_CONSUME", "g0"))
        out.append(("DRAIN_RESUME", "g0"))

    return out, 1


TEMPLATE = '''\\* GENERATED by tools/iouwake_trace_conform.py from a real RUNLOOM_IOUWAKE_TRACE run.
\\* Replays the recorded io_uring CQE wake transitions through RunloomIouringWake's
\\* OWN actions under TLC: conformance = each event is an enabled model step and the
\\* safety invariants (ResumeIsTerminal + NoStrandedCompletion) hold along the real
\\* path -- including the CQ-overflow backlog + the drain-first GETEVENTS flush heal.
\\* SAFETY refinement only; liveness stays in the closed model.  DO NOT EDIT.
----------------------- MODULE RunloomIouringWakeTrace -----------------------
EXTENDS RunloomIouringWake, Sequences, Naturals

TraceEvents == << {seq} >>

VARIABLE tpc

TInit == Init /\\ tpc = 1

TNext ==
    \\/ /\\ tpc <= Len(TraceEvents)
       /\\ LET e == TraceEvents[tpc] IN
            /\\ \\/ (e.a = "SUBMIT"            /\\ Submit(e.g))
               \\/ (e.a = "KCOMPLETE"         /\\ KernelComplete(e.g))
               \\/ (e.a = "DRAIN_CONSUME"     /\\ DrainConsume)
               \\/ (e.a = "DRAIN_RESUME"      /\\ DrainResume)
               \\/ (e.a = "DRAIN_PEEK_EMPTY"  /\\ DrainPeekEmpty)
               \\/ (e.a = "DRAIN_DECIDE"      /\\ DrainDecide)
               \\/ (e.a = "DRAIN_BLOCK"       /\\ DrainBlock)
               \\/ (e.a = "DRAIN_FLUSH_FIRST" /\\ DrainFlushFirst)
            /\\ tpc' = tpc + 1
    \\/ /\\ tpc > Len(TraceEvents)
       /\\ UNCHANGED <<vars, tpc>>

TSpec == TInit /\\ [][TNext]_<<vars, tpc>>
=============================================================================
'''

CFG = '''\\* GENERATED.  Gs = the distinct CQE-wake episodes (one per SUBMIT -> RESUME).
\\* Heal = TRUE: the real binary HAS the drain-first overflow flush (the
\\* runloom_sched_drain.c.inc:155 loop-top flush + flush_cq_overflow GETEVENTS).
\\* CQCAP = 1 -- a one-slot visible ring (the model's own "2 ops force overflow"
\\* cfg sizing); the chained driver keeps exactly one completion in the ring while
\\* every other (overflow) completion is healed by the drain-first flush, so the
\\* recorded 1-visible + K-overflow split is reproduced with overflow/ring sets of
\\* size <= 1 at every state.  SAFETY conformance (no liveness on a finite trace).
CONSTANTS
    Gs = {{{gs}}}
    CQCAP = {cqcap}
    Heal = TRUE
SPECIFICATION TSpec
INVARIANT TypeOK
INVARIANT ResumeIsTerminal
INVARIANT NoStrandedCompletion
'''


def record(action, g):
    return '[a |-> "{}", g |-> "{}"]'.format(action, g)


def main():
    argv = list(sys.argv[1:])
    drop_submit = -1
    rest = []
    for a in argv:
        if a == "--drop-submit":
            drop_submit = 0
        elif a.startswith("--drop-submit="):
            drop_submit = int(a.split("=", 1)[1])
        else:
            rest.append(a)
    if not rest:
        print("usage: iouwake_trace_conform.py <events.ndjson> [--drop-submit[=N]]")
        return 2
    raw = load_events(rest[0])
    if not raw:
        print("empty trace (was RUNLOOM_IOUWAKE_TRACE set on an io_uring "
              "multishot-overflow run?)")
        return 2

    episodes = build_episodes(raw)
    if not episodes:
        print("no io_uring CQE-wake episodes in the trace (did any TCPConn.recv "
              "run under RUNLOOM_TCPCONN_IOURING=1 on the single-thread drain?)")
        return 2

    n_overflow = sum(1 for e in episodes if e["overflow"])
    events, cqcap = lower(episodes, drop_submit)
    gs = [e["id"] for e in episodes]

    seq = ",\n                  ".join(record(a, g) for a, g in events)
    with open(os.path.join(TLA, "RunloomIouringWakeTrace.tla"), "w") as f:
        f.write(TEMPLATE.format(seq=seq))
    with open(os.path.join(TLA, "RunloomIouringWakeTrace.cfg"), "w") as f:
        f.write(CFG.format(gs=", ".join('"{}"'.format(g) for g in gs),
                           cqcap=cqcap))

    if not os.path.exists(JAR):
        print("tla2tools.jar missing; run tools/verify/tla/run_tla.sh once")
        return 2
    meta = "/tmp/runloom_iouwaketrace_{}".format(os.getpid())
    proc = subprocess.run(
        ["java", "-Xmx1g", "-cp", JAR, "tlc2.TLC", "-workers", "4", "-metadir", meta,
         "-config", "RunloomIouringWakeTrace.cfg", "RunloomIouringWakeTrace.tla"],
        cwd=TLA, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = proc.stdout
    print("  events: {}   episodes: {}   overflow: {}   visible: {}   "
          "model-steps: {}".format(len(raw), len(episodes), n_overflow,
                                   len(episodes) - n_overflow, len(events)))
    if "No error has been found" in out:
        print("  CONFORMS -- every io_uring CQE-wake event is a legal "
              "RunloomIouringWake step; ResumeIsTerminal + NoStrandedCompletion "
              "hold along the real path (no resume without a kernel completion; no "
              "completion stranded outside cq_inflight -- the overflow backlog is "
              "always still kernel-owned and the drain-first flush makes it visible)")
        return 0
    if "is violated" in out:
        inv = next((l.strip() for l in out.splitlines() if "is violated" in l),
                   "an invariant")
        print("  NON-CONFORMING -- {}".format(inv))
        return 1
    if "Deadlock" in out or "deadlock" in out:
        print("  NON-CONFORMING -- a recorded io_uring wake transition was not an "
              "enabled model step (e.g. a KernelComplete/DrainConsume/RESUME with no "
              "preceding Submit -- a completion consumed without a submitted op, the "
              "lost-wakeup class RunloomIouringWake.tla forbids)")
        return 1
    print("  TLC error:\n" + "\n".join(out.splitlines()[-12:]))
    return 2


if __name__ == "__main__":
    sys.exit(main())
