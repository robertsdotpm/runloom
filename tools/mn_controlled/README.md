# tools/mn_controlled — controlled M:N scheduler (seeded deterministic replay)

The high-value follow-up to single-hub PCT: control the scheduling of the
**real M:N hubs** (work-stealing parallel OS threads) so the parallel races
where pygo's hard bugs live become reproducible and seed-explorable.

Runtime hook (`src/pygo_core/mn_sched_hub_resume_preempt.c.inc`,
`pygo_mn_ctrl_*`): when `PYGO_MN_SEED` is set, goroutine execution segments
across all hubs are serialized through one **execution baton** — a hub may run a
goroutine (`pygo_coro_resume`) only while it holds the baton, gated in
`pygo_hub_resume_begin/end`; a seeded controller hands the baton to the next
hub. A waiting hub detaches its Python thread state (`PyEval_SaveThread`) so it
sits at a GC safepoint (essential under free-threaded CPython). Handoff is forced
off (its rescue thread resumes off-baton); preemption stays **on** (load-bearing
for liveness — see below). Off by default; zero cost when unset.

## Deterministic replay (the barrier-rendezvous)

Same `PYGO_MN_SEED` ⇒ **identical execution**, run to run — verified by
`repro_probe.py` (16/16 seeds reproduce identically; seed 1 identical over
500/500 reps under heavy CPU load; distinct seeds still explore distinct
interleavings). The baton alone is *not* enough — it serializes who runs, but the
requester set and the goroutine each hub holds still raced OS timing. Four levers,
gated together behind `PYGO_MN_BARRIER` (default on under a seed;
`PYGO_MN_BARRIER=0` reverts to timing-dependent exploration for A/B):

1. **Barrier-rendezvous census.** The controller grants the baton only once the
   requester set for the round is *complete* — every hub has checked in, as a
   wanter (in `acquire`) or as idle (in `hub_main`'s no-work branch). The single
   seeded RNG draw per grant is then over the full set, so the handoff sequence
   is a function of the schedule, not of who happened to register first. This is
   the `Barrier` constant in `verify/tla/PygoMNControl.tla` (`DeterministicGrant`
   holds with it, fails without it).
2. **Startup entry gate.** Hubs block at loop entry until `mn_run` arms the
   controller — *after* all pre-run `mn_go` placement. Without it the main thread
   places the initial goroutines concurrently with already-spinning hubs, so a
   hub could check in idle before its share landed and the first census would be
   partial. Work created *later* (by a running segment) is published at that
   segment's release and picked up in the next round, so only startup needs the
   gate.
3. **No work-stealing.** With deterministic `mn_go` placement
   (`spawn_counter % hub_count`) and hub-pinned wakes, disabling steal makes each
   hub's execution stream a fixed function of the schedule — closing the last gap
   where an identical grant sequence still produced different goroutine orderings.
   Steal is a lock-free, sub-segment CAS race; it is covered by the memory-model
   tools (`verify/` GenMC / herd7, `tools/lincheck`), not by segment-granularity
   replay, so dropping it here loses no coverage that lives at this level.

**Scope.** This pins *scheduling* nondeterminism for **closed** workloads —
CPU + channel/lock/sync among the goroutines themselves, plus logical timers.
Real network I/O is nondeterministic by nature (the wire decides when an fd is
ready), so an open workload replays its *scheduling decisions* but not external
arrival timing — the standard limit of this technique (CHESS, Coyote, rr-for-
syscalls all draw the same line). A future lever for CPU-bound-without-yield
goroutines: deterministic *bytecode-count* preemption to replace the wall-clock
sysmon trigger; not needed for the park-fast workloads the demo/probe cover,
where preemption rarely fires.

**A fourth lever was needed — the census-idle wake race (fixed).** A first cut
left a rare (~1%, load-sensitive) Heisenbug: a seed would occasionally produce a
different run. It looked sub-segment (any grant-path instrumentation hid it), but
it was a real gap in lever 1: `census_idle` declared a hub idle for the new round
based on a drain that could *predate* a wake. Sequence: hub Y drains its sub_list
(empty) → hub X's segment wakes a goroutine onto Y's sub_list → X releases (new
round) → Y checks in *idle*, having missed the freshly-woken goroutine. The
census then completed without Y and ran its goroutine a round late — and whether
Y's drain beat X's wake was pure timing. Fix: `census_idle` re-tests the sub_list
under `sub_lock` *while holding the controller lock* before declaring idle
(having observed the round, it is guaranteed to see any wake the just-ended
segment published before the release that opened it: segment-wake → release
happens-before round-observed → sub re-test); if work is there, the hub re-loops
and drains it instead of falsely idling. With the guard, seed 1 is identical over
500/500 reps under heavy load (16 CPU hogs) — previously ~1/60 there — and the
probe is 16/16 seeds stable.

**Remaining open scope** (genuinely out of the closed-workload demo's reach):
*real network I/O* arrival timing (the wire decides fd-readiness — the open-
system limit shared by CHESS/Coyote/rr) and *CPU-bound-without-yield* goroutines,
whose preemption point is still wall-clock (sysmon) rather than deterministic
bytecode-count — a future lever, not exercised by the park-fast demo where
preemption rarely fires.

**Deadlock — FIXED (2026-06-03).** An earlier version *disabled* preemption; a
goroutine that ran Python without yielding then held the baton forever and
starved every other hub (gdb: a thread deep in the eval loop under
`pygo_preempt_eval_frame`, baton free, all other hubs idle). Fix: **keep
preemption ON** — it yields a runaway goroutine at a bytecode boundary, releasing
the baton. The TLA+ model's `Preempt=FALSE` control reproduces exactly this
(`AllRun` liveness violated).

**Off-path is regression-free.** Full isolated suite green with `PYGO_MN_SEED`
unset (the controlled path is gated behind `pygo_mn_ctrl.enabled`); the only
default-path change is one predictable-false branch.

## Demo / probe

```sh
# seeded exploration, conservation-clean, each seed reproducible:
PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 tools/mn_controlled/demo.py

# deterministic-replay yardstick (same seed x N reps -> one signature):
PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 tools/mn_controlled/repro_probe.py 12 8
PYGO_MN_BARRIER=0 … repro_probe.py 12 8      # A/B: reverts to nondeterministic

# grant trace (one hub-id per baton grant, to a file):
PYGO_MN_SEED=1 PYGO_MN_TRACE=/tmp/g.txt … <workload>
```
`mn_stress` (select + coordinator close) also runs clean under controlled mode —
a randomized concurrency-testing mode on the real hubs.
