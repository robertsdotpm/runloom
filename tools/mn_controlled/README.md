# tools/mn_controlled — controlled M:N scheduler (PROTOTYPE)

The high-value follow-up to single-hub PCT: control the scheduling of the
**real M:N hubs** (work-stealing parallel OS threads) so the parallel races
where pygo's hard bugs live become reproducible and seed-explorable.

Runtime hook (`src/pygo_core/mn_sched.c`, `pygo_mn_ctrl_*`): when
`PYGO_MN_SEED` is set, goroutine execution segments across all hubs are
serialized through one **execution baton** — a hub may run a goroutine
(`pygo_coro_resume`) only while it holds the baton, gated in
`pygo_hub_resume_begin/end`; a seeded controller hands the baton to the next
waiting hub. A waiting hub detaches its Python thread state (`PyEval_SaveThread`)
so it sits at a GC safepoint (essential under free-threaded CPython). Handoff +
preemption are forced off in this mode. Off by default; zero cost when unset.

## Status: PROTOTYPE — what works, what doesn't

**Works.** Builds clean; off-path is regression-free (mn_stress + suite green
with `PYGO_MN_SEED` unset). With it on, the baton correctly serializes the real
hub/deque/park/wake path: simple + channel workloads **and the full `mn_stress`
fuzzer** (select + coordinator close, 1/2/default hubs) run to completion,
conservation-clean, no deadlock — soaked over 6 seeds × 20 iters since the
deadlock fix. Across seeds it explores **diverse** cross-hub interleavings
(10/10 distinct on the channel demo) — i.e. it is a genuine *randomized
concurrency testing* mode on the real M:N code, which single-hub PCT cannot
reach.

**Deadlock — FIXED (2026-06-03).** Earlier, multi-iter `mn_stress` (select +
coordinator) wedged. Root cause: the mode had *disabled preemption*, so a
goroutine that ran Python without yielding held the baton forever and starved
every other hub (gdb showed exactly this — a thread deep in the eval loop under
`pygo_preempt_eval_frame`, baton free, all other hubs idle). Fix: **keep
preemption ON** in controlled mode — it yields a runaway goroutine at a bytecode
boundary, which releases the baton. (Handoff stays off; its rescue thread
resumes off-baton.) Soak after the fix: `mn_stress` CLEAN over 6 seeds × 20
iters and 1/2/default hubs, off-path unchanged.

**One real gap remains (why it is NOT merged to main):**

1. **Not deterministic replay.** Same `PYGO_MN_SEED` still varies run-to-run.
   The controller's seeded choice is only among the hubs *currently* in
   `acquire`; the requester set still depends on OS timing (idle-hub re-entry,
   netpoll wakes, work-stealing). **And the deadlock fix added a second source:**
   preemption is now load-bearing for liveness, but it fires on a *wall-clock*
   sysmon threshold — inherently nondeterministic. So determinism needs both the
   requester set pinned *and* preemption made deterministic.

## What full determinism actually requires (the remaining work)

The baton controls *which hub runs a goroutine next*. Deterministic replay also
needs the rest of the schedule's nondeterminism pinned:

- **Deterministic preemption.** Replace the wall-clock sysmon preempt trigger
  (now load-bearing for liveness) with a *bytecode-count* preempt in the eval
  hook, so the runaway-goroutine yield point is schedule-determined, not timing-
  determined.
- **Gate the wake/steal/netpoll-dispatch points**, not just resume — so *which*
  goroutines are ready, and on *which* hub, is seed-determined too.
- **Barrier rendezvous** at each scheduling point: wait until every non-blocked
  hub has reached the controller before choosing, so the requester set is a
  function of the schedule, not OS timing.

That is a substantial, `verify/`-grade effort (and arguably wants a TLA+/Spin
model of the baton protocol first — which the repo is well set up for). This
prototype is the proof-of-concept, now deadlock-free, and a precise statement
of what remains for full deterministic replay.

## Demo

```sh
PYGO_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 tools/mn_controlled/demo.py
```
Serialized cross-hub channel exploration: diverse interleavings across seeds,
conservation-clean. Since the deadlock fix, `mn_stress` also runs clean under
controlled mode (`PYGO_MN_SEED=<n> … tools/mn_stress.py`) — a randomized
concurrency-testing mode on the real hubs.
