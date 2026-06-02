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
hub/deque/park/wake path: simple and channel workloads run to completion,
conservation-clean, no deadlock, and across seeds it explores **diverse**
cross-hub interleavings (8/8 distinct on a 3-hub yield workload) — i.e. it is a
genuine *randomized concurrency testing* mode on the real M:N code, which
single-hub PCT cannot reach.

**Two real gaps remain (why it is NOT merged to main):**

1. **Not deterministic replay.** Same `PYGO_MN_SEED` does *not* yet reproduce
   the same interleaving run-to-run. The controller's seeded choice is only
   among the hubs *currently* in `acquire`; which hubs are requesting at a given
   handoff still depends on OS timing (when an idle hub re-enters the pool, when
   a netpoll wake lands, work-stealing distribution). The seed controls the
   *choice*, not the *requester set*. Full determinism needs the schedule's
   nondeterminism pinned end-to-end, not just the baton handoff.

2. **Intermittent deadlock on complex workloads.** Single `mn_stress` iters
   pass, but a multi-iter run (select consumers + coordinator close) wedges on
   some iters. The baton + the scheduler's idle/netpoll/steal paths admit a
   liveness cycle not yet diagnosed.

## What full determinism actually requires (the remaining work)

The baton controls *which hub runs a goroutine next*. Deterministic replay also
needs the rest of the schedule's nondeterminism pinned:

- **Gate the wake/steal/netpoll-dispatch points**, not just resume — so *which*
  goroutines are ready, and on *which* hub, is seed-determined too.
- **Barrier rendezvous** at each scheduling point: wait until every non-blocked
  hub has reached the controller before choosing, so the requester set is a
  function of the schedule, not OS timing.
- **A deadlock-free baton protocol** that accounts for a hub going idle in
  netpoll while holding/wanting the baton (the gap behind the hang above).

That is a substantial, `verify/`-grade effort (and arguably wants a TLA+/Spin
model of the baton protocol first — which the repo is well set up for). This
prototype is the proof-of-concept and a precise statement of the hard parts.

## Demo (the working case)

```sh
PYGO_GIL=0 ~/.pyenv/versions/3.13.13t/bin/python3 tools/mn_controlled/demo.py
```
Shows serialized cross-hub channel exploration: diverse interleavings across
seeds, conservation-clean. Do NOT point it at `mn_stress` multi-iter yet (gap 2).
