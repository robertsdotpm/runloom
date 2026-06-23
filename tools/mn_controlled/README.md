# tools/mn_controlled — controlled M:N scheduler (seeded deterministic replay)

The high-value follow-up to single-hub PCT: control the scheduling of the
**real M:N hubs** (work-stealing parallel OS threads) so the parallel races
where runloom's hard bugs live become reproducible and seed-explorable.

Runtime hook (`src/runloom_c/mn_sched_hub_resume_preempt.c.inc`,
`runloom_mn_ctrl_*`): when `RUNLOOM_MN_SEED` is set, goroutine execution segments
across all hubs are serialized through one **execution baton** — a hub may run a
goroutine (`runloom_coro_resume`) only while it holds the baton, gated in
`runloom_hub_resume_begin/end`; a seeded controller hands the baton to the next
hub. A waiting hub detaches its Python thread state (`PyEval_SaveThread`) so it
sits at a GC safepoint (essential under free-threaded CPython). Handoff is forced
off (its rescue thread resumes off-baton); preemption stays **on** (load-bearing
for liveness — see below). Off by default; zero cost when unset.

## Deterministic replay (the barrier-rendezvous)

Same `RUNLOOM_MN_SEED` ⇒ **identical execution**, run to run — verified by
`repro_probe.py` (single channel: 16/16 seeds identical, seed 1 identical over
500/500 reps under heavy CPU load), `repro_select.py` (select over multiple
channels + mid-run goroutine spawn: 8/8 over 16 seeds × 30 reps, and 8/8 under
16 CPU hogs + `setarch -R` — see lever 7), and `repro_timer.py` (same-delay +
staggered `sched_sleep`: 8/8). Distinct seeds still explore distinct
interleavings. The baton alone is *not* enough — it serializes who runs, but the
requester set, the goroutine each hub holds, the preemption point, and timer
firing all still raced OS timing. Six levers, gated together behind
`RUNLOOM_MN_BARRIER` (default on under a seed; `RUNLOOM_MN_BARRIER=0` reverts to
timing-dependent exploration for A/B):

1. **Barrier-rendezvous census.** The controller grants the baton only once the
   requester set for the round is *complete* — every hub has checked in, as a
   wanter (in `acquire`) or as idle (in `hub_main`'s no-work branch). The single
   seeded RNG draw per grant is then over the full set, so the handoff sequence
   is a function of the schedule, not of who happened to register first. This is
   the `Barrier` constant in `tools/verify/tla/RunloomMNControl.tla` (`DeterministicGrant`
   holds with it, fails without it).
2. **Startup entry gate.** Hubs block at loop entry until `mn_run` arms the
   controller — *after* all pre-run `mn_fiber` placement. Without it the main thread
   places the initial goroutines concurrently with already-spinning hubs, so a
   hub could check in idle before its share landed and the first census would be
   partial. Work created *later* (by a running segment) is published at that
   segment's release and picked up in the next round, so only startup needs the
   gate.
3. **No work-stealing.** With deterministic `mn_fiber` placement
   (`spawn_counter % hub_count`) and hub-pinned wakes, disabling steal makes each
   hub's execution stream a fixed function of the schedule — closing the gap
   where an identical grant sequence still produced different goroutine orderings.
   Steal is a lock-free, sub-segment CAS race; it is covered by the memory-model
   tools (`tools/verify/` GenMC / herd7, `tools/lincheck`), not by segment-granularity
   replay, so dropping it here loses no coverage that lives at this level.
4. **Census-idle wake-guard.** `census_idle` must not declare a hub idle on a
   drain that *predates* a wake: hub Y drains its sub_list (empty) → hub X's
   segment wakes a goroutine onto Y → X releases (new round) → Y checks in *idle*,
   having missed the goroutine, so it runs a round late — timing-decided. Fix: Y
   re-tests its sub_list under `sub_lock` *while holding the controller lock*
   before declaring idle. Having observed the round (set under the controller lock
   by the release that opened it), Y is guaranteed to see any wake the just-ended
   segment published before that release (segment-wake → release happens-before
   round-observed → sub re-test); if work is there it re-loops and drains it. This
   was a rare (~1%, load-sensitive) Heisenbug — any grant-path instrumentation hid
   it; with the guard, seed 1 is identical over 500/500 reps under heavy load
   (16 CPU hogs), previously ~1/60 there.
5. **Deterministic preemption.** Preemption is load-bearing — a goroutine that
   runs Python without yielding would hold the baton forever (the deadlock
   below) — but the sysmon fires it on a *wall-clock* threshold, the last
   nondeterministic input to the schedule (it broke the tie e.g. on which of two
   back-to-back `mn_fiber`s a segment finishes before yielding). In barrier mode the
   eval-frame wrapper ignores the wall-clock flag and instead yields the baton
   after a fixed COUNT of Python frame entries on the baton
   (`RUNLOOM_MN_PREEMPT_FRAMES`, default 4096) — a deterministic function of the
   goroutine's own execution. Cooperative goroutines park in far fewer frames, so
   they never trip it (natural, reproducible schedule); a CPU-bound goroutine that
   keeps calling functions hits the count and yields (liveness), at a *reproducible*
   frame: a `while not check(): n+=1` spinner stops at the same `n` every run
   (e.g. seed 8 → 20478 = 5·4096−2, 8/8 reps). This removed the wall-clock tie in
   the select + mid-run-spawn workload; the *remaining* residual there was the
   partial-publish race that lever 7 closes.

   *Frame granularity + a liveness backstop.* Frame-count preemption (like the
   wall-clock trigger it replaces) acts only at a Python frame boundary, so it
   can't break a goroutine spinning in ONE frame with no calls
   (`while not flag: pass`) — which, serialized under the baton with no parallel
   sibling, would hold the baton forever. The sysmon catches such a wedge and
   posts an eval-breaker pending call (`_PyEval_AddPendingCall`, flags=0 so it
   runs on the *hub* thread, not the parked main thread — the public
   `Py_AddPendingCall` is main-only), which rides CPython's backward-jump checks
   *inside* the frame and yields the baton holder. So a single-frame loop no
   longer deadlocks (`while not flag: pass` finishes ~0.1s instead of hanging).
   It fires only on a genuine single-frame wedge — never for frame-bearing or
   park-fast code, where frame-count preempt resolves things first — so it trades
   determinism for liveness in exactly that one pathological case and leaves the
   replay-gate workloads bit-identical. (3.12+; on older builds the limitation
   stands.)
6. **Logical clock (timers).** `sched_sleep` deadlines are computed against a
   logical clock, not the wall clock, and timers fire *only* when the controller
   advances it — which it does at a **quiescent census** (every hub idle, none
   wanting the baton) to the earliest pending deadline across all hubs (each hub
   reports its own sleep-heap minimum in `census_idle`). So timer firing order is
   a function of the schedule, not of when each hub's wall-clock poll happens to
   run. Same-delay timers — whose order is *purely* scheduling-decided — go from
   3-distinct/20 to one signature per seed; overlapping periodic sleeps replay
   identically and never hang (the clock keeps advancing to the next deadline).

7. **Atomic deferred publish at release.** Levers 2 and 4 assume the model in
   their own words — *"work created later (by a running segment) is published at
   that segment's release"* — but the code used to publish **immediately**: a
   cross-hub `mn_fiber` / channel-wake pushed onto the target's `sub_list` mid-segment
   (`runloom_mn_hub_submit`). A target draining its `sub_list` at loop entry could
   then catch a **partial** snapshot of an in-flight segment's submits — e.g. a
   `boot` segment that spawns producers `[1025, 1028]` onto hub 0: if hub 0's drain
   landed between the two pushes it saw only `1025`, otherwise both, so hub 0's
   runq *front* at its (deterministically-granted) segment was a function of
   `sub_lock` race timing, not the schedule. The grant sequence was byte-identical
   across reps; only *which goroutine ran in a given grant slot* differed — the
   long-standing `repro_select` residual (was ~4/8). Fix: in barrier mode a
   cross-hub submit made **during** a running segment is staged per target on the
   spawning hub and spliced onto the target's `sub_list` in **one** lock-held
   operation at the segment's **release** (`runloom_mn_ctrl_stage_flush`, before the
   baton release). The target then drains either the whole batch or none — never a
   partial set — and lever 4's existing "re-test under the controller lock after
   observing the round" guarantees the complete batch is seen at the round
   boundary. Same-hub submits (no cross-thread race), non-segment producers (the
   idle pump, pre-run placement), and the entire default scheduler fall through to
   the immediate push (zero cost; `stage_head` is `NULL`). With it `repro_select`
   is **8/8** (16 seeds × 30 reps; 8/8 under 16 CPU hogs + `setarch -R`), and the
   buffered-channel + spawn variant is stable too.

**Scope.** This pins *all scheduling* nondeterminism for **closed** workloads —
CPU + channel/lock/sync among the goroutines themselves (including CPU-bound
segments, lever 5) and `sched_sleep` timers (lever 6). The one genuinely
out-of-reach source is *real network I/O* arrival timing: the wire decides when an
fd is ready, so an open workload replays its scheduling decisions but not external
arrival timing — the standard limit of this technique (CHESS, Coyote, rr-for-
syscalls all draw the same line). (The `runloom.aio` event-loop timer path —
`call_at`/`call_later` — has its own clock and is not yet routed through the
logical clock; the `sched_sleep` primitive is.)

**Deadlock — FIXED (2026-06-03).** An earlier version *disabled* preemption; a
goroutine that ran Python without yielding then held the baton forever and
starved every other hub (gdb: a thread deep in the eval loop under
`runloom_preempt_eval_frame`, baton free, all other hubs idle). Fix: **keep
preemption ON** — it yields a runaway goroutine at a bytecode boundary, releasing
the baton. The TLA+ model's `Preempt=FALSE` control reproduces exactly this
(`AllRun` liveness violated).

**Off-path is regression-free.** Full isolated suite green with `RUNLOOM_MN_SEED`
unset (the controlled path is gated behind `runloom_mn_ctrl.enabled`); the only
default-path change is one predictable-false branch.

## PCT — bug-depth-guaranteed search (`RUNLOOM_MN_PCT=<depth d>`)

Deterministic replay pins one schedule per seed; **which** schedules a seed sweep
explores was, until now, the baton's *uniform-random* grant order
(`runloom_mn_ctrl_choose`'s else branch) — fine for shallow bugs, but with **no
guarantee** of reaching a deep one. `RUNLOOM_MN_PCT=d` upgrades the grant order to
the **PCT algorithm** (Probabilistic Concurrency Testing, Burckhardt et al.,
ASPLOS 2010), which adds a *provable* probabilistic guarantee parameterized by
**bug depth** — the number of ordering constraints a bug needs:

- every hub gets a distinct random **base priority**; the controller always grants
  the baton to the **highest-priority waiting hub** (the barrier's complete
  requester set is PCT's "enabled" set — so PCT requires the barrier, default on);
- **d-1 seeded priority change points** are planted at grant-step indices in
  `[1, k]` (`RUNLOOM_MN_PCT_STEPS`, default 4096); reaching one **demotes** the hub
  that ran that step below all base priorities. The d-1 demotions are exactly the
  d-1 ordering inversions a depth-d bug needs.

Any bug of depth ≤ d is then hit with probability **≥ 1/(n·k^(d-1))** per seed —
a lower bound the uniform draw has no analogue of. Sweep seeds; the bound says how
many you need. A separate PRNG stream drives PCT, so a run with `RUNLOOM_MN_PCT`
**unset is bit-identical** to before the feature; the pick stays a pure function of
(seed, schedule), so **replay determinism is preserved** (`repro_probe.py` is 8/8
stable with PCT on as well as off).

*Why it's safe.* PCT only changes **which** waiting hub the (already TLA-verified)
grant protocol hands the baton to — and the TLA `Grant(h)` action
(`tools/verify/tla/PygoMNControl.tla`) already leaves that a **free nondeterministic
choice**. `MutualExclusion`, `DeterministicGrant`, `AllRun` and `DeterministicTick`
constrain *when* a grant fires (barrier / quiescence) and that exactly one hub
runs — never *which* one — so replacing the selection function refines a choice
the model already abstracts over. No re-verification needed; the properties hold
by construction.

*Validated* (`pct_find.py`) on a deliberately **narrow** order-dependent bug —
goroutine B must observe a shared counter at a *late* value, an interleaving
uniform scheduling is biased away from (it runs B's read early). With 80 seeds,
n=2, k=14: **depth-1** (0 change points) finds it **0/80** (a fixed priority order
cannot sandwich B → the bug is genuinely order-dependent); **uniform** finds it
**0/80** (random misses the narrow window); **PCT depth-2** finds it **3/80**, an
empirical rate (0.037) landing on the bound 1/(n·k)=0.036 — and a finding seed
**replays the bug 12/12**, a permanent deterministic regression repro.

**Scope (honest).** PCT here schedules over **hubs** (the OS threads), so it
targets the **order-dependent** bug class — lost wakeups, cross-hub wake ordering,
deadlocks, ordering-sensitive logic — which is exactly the class the baton
serializes and can replay. It is **not** the tool for **true-simultaneity** memory
races (e.g. the free-threaded gc-churn UAFs): the baton keeps **one** Python
thread-state attached at a time, removing the very parallelism those need. Those
stay the province of TSan + the flight recorder (`RUNLOOM_DEBUG=ring`) + delay
injection (`RUNLOOM_DELAY`) under genuinely parallel execution. PCT and those
tools are complementary, not substitutes.

## Demo / probe

```sh
# seeded exploration, conservation-clean, each seed reproducible:
PYTHON_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 tools/mn_controlled/demo.py

# deterministic-replay yardsticks (same seed x N reps -> one signature):
PYTHON_GIL=0 …/python3 tools/mn_controlled/repro_probe.py 12 8    # single channel
PYTHON_GIL=0 …/python3 tools/mn_controlled/repro_select.py 10 8   # select + spawn
PYTHON_GIL=0 …/python3 tools/mn_controlled/repro_timer.py 10 8    # sched_sleep timers
RUNLOOM_MN_BARRIER=0 … repro_probe.py 12 8      # A/B: reverts to nondeterministic

# PCT bug-depth-guaranteed search (depth-1 misses an order-dependent bug,
# PCT depth-2 finds + replays it; empirical hit rate matches the 1/(n*k) bound):
PYTHON_GIL=0 …/python3 tools/mn_controlled/pct_find.py 80

# tunables:
RUNLOOM_MN_PREEMPT_FRAMES=1024 …   # frame budget before a CPU-bound g yields the baton
RUNLOOM_MN_PCT=3 RUNLOOM_MN_PCT_STEPS=64 …   # PCT depth d + change-point step bound k
RUNLOOM_MN_SEED=1 RUNLOOM_MN_TRACE=/tmp/g.txt …   # grant trace: one hub-id per baton grant
```
`mn_stress` (select + coordinator close) also runs clean under controlled mode —
a randomized concurrency-testing mode on the real hubs.
