# Formal-verification coverage map

What is machine-checked in runloom, by subsystem and engine — and, honestly,
what is **not** yet modeled. Companion to [`README.md`](README.md) (which has the
prose proof-by-proof); this file is the map + the gap list, kept next to the
proofs so it can be audited against the source.

_Last audited 2026-06-17 (an independent agent verified every attribution below
against the actual model files and `src/runloom_c/`). Corrections from that audit
are folded in; remaining doc-drift is tracked in **Documentation debt** at the
bottom._

## Engines and their roles

| engine | proves | model lives in |
|--------|--------|----------------|
| **Spin** | the *algorithm* over **all** SC interleavings | `spin/*.pml` |
| **CBMC** | the **real C source** with real `__atomic_*` orders (bounded) | `cbmc/*_cbmc.c` |
| **GenMC** | the **real C** under **RC11** weak memory (every execution) | `genmc/*.c` |
| **herd7** | C11/RC11 **fence placement** (litmus) | `litmus/*.litmus` |
| **Coq** | **unbounded** inductive conservation/liveness | `coq/*.v` |
| **Iris / iRC11** | separation-logic specs (+ weak memory) | `iris/*.v`, `iris/rc11/*.v` |
| **TLA+** | **global** temporal safety + liveness (TLC) | `tla/*.tla` |
| **Alloy** | structural well-formedness invariants | `alloy/` |

Driver: `verify/run_verify.sh` (Spin + CBMC + GenMC + herd7 + Coq + Iris + Alloy),
`verify/tla/run_tla.sh` (TLC). Both fold into `scripts/check_all.sh verify`.

## Coverage map

| Subsystem | Source | Models | Depth |
|---|---|---|---|
| Chase-Lev deque | `cldeque.c` | `cldeque.pml`(Spin), `cldeque_cbmc.c` + `cldeque_disjoint.c`(CBMC, real source), `chase_lev.c`/`chase_lev2.c`/`chase_lev_real.c`(GenMC/RC11, `_real` = unmodified `cldeque.c`), `Deque.v`(Coq ∞) | **deep** (4 engines on real source) |
| Per-g `wake_state` machine | `runloom_sched_parkwake.c.inc` | `wake_state.pml` + `wake_state_fsm_cbmc.c`, `WakeState.v`(Coq ∞), `WakeQueue.v`(Iris), `RunloomGRefcount.tla` | **deep** |
| park_safe/wake_safe | `runloom_sched_parkwake.c.inc` | `parked_safe.pml`, `park_generic_timed.pml`, `sched_parkwake.c` + `_seam.c`(GenMC; the SC fence was *discovered* here), `OneShotWake.v`(Iris) | **deep** |
| Cross-thread wake routing | `mn_sched_mn_api.c.inc` | `cross_thread_wake.pml`, `wakelist_mpsc.litmus`, `WakeListHandoff.v`(iRC11) | **deep** |
| select claim / close | `chan_select_main.c.inc`, `chan_waiters.c.inc` | `select_claim.pml`, `select_close.pml`(+4 controls), `Select.v`(Coq ∞) | **deep** |
| Default M:N wake dedup | `mn_sched_mn_api.c.inc` (`hub_submit`) | `hub_submit.pml`, `sched_qref.c`(CBMC) | good |
| Ready-ring FIFO | `runloom_sched_core.c.inc` | `sched_readyring_cbmc.c`(CBMC) | good |
| netpoll commit + arm (epoll/kqueue/AFD) | `netpoll_*.c.inc`, `netpoll_iocp.c` | `netpoll_commit`,`netpoll_rearm`,`netpoll_kqueue`,`netpoll_afd`,`netpoll_multipool`,`netpoll_deadline`,`netpoll_forceunlink`(Spin), `netpoll_claim.c`(GenMC), `commit_*.litmus` | **deep** (see drift note on `netpoll_rearm`) |
| **io_uring-as-loop backend wake/re-arm** | `io_uring_l_loop.c.inc`, `netpoll_wake_iouring.c.inc`, `mn_sched_hub_main.c.inc` | **`netpoll_iouring_loop.pml`(Spin) — NEW 2026-06-17** | good (NEW) |
| Blocking-offload pool | `runloom_blockpool.c` | `blockpool.pml`, `blockpool_job.c`(GenMC), `Blockpool.v`(Coq ∞) | **deep** |
| io_uring single-op + multishot | `io_uring.c`, `io_uring_l_msclose.c.inc` | `iouring_waitcommit.c`(GenMC), `iouring_msclose.pml`(Spin) | good |
| Preemption defer-in-destruction | `mn_sched_hub_resume_preempt.c.inc` | `preempt_defer_cbmc.c`(CBMC) | good |
| Teardown / mn_fini | `mn_sched_init_fini.c.inc` | `RunloomMnFini.tla`, `RunloomHandoff.tla`(TLA) | good |
| **mn_run deadlock-census + stall-kick** | `mn_sched_init_fini.c.inc` | **`RunloomMnRun.tla`(TLA) — NEW 2026-06-17**; census-idle wake-guard also in `RunloomComposite.tla` | good (NEW) |
| Whole-scheduler composition | `mn_sched*.c.inc` | `RunloomSched.tla`, `RunloomComposite.tla`(TLA) | good |
| tstate lifecycle / STW / migration | `mn_sched_hub_main.c.inc` | `tstate_attach_detach.pml`, `RunloomCPythonSTW`,`RunloomGilstate`,`RunloomTstateMigration.tla`, `sched_pystate_cbmc.c` | **deep** |
| Controlled-replay baton | `mn_sched_runq.c.inc` | `RunloomMNControl.tla` | good |
| CPython-runtime oracles | (external) | `brc_merge.c`,`qsbr_drain.c`,`mimalloc_page_free.c`(GenMC/RC11) | good |
| Refcounts (g / chan / sent-obj / snap) | `chan.c`, `mn_sched.c` | `chan_refcount.c`(GenMC), `chan_refflow_cbmc.c`,`snap_refown_cbmc.c`(CBMC), `RunloomGRefcount.tla` | good |
| Slab / datastack / admission | `coro.c`, `mn_sched.c` | `g_slab_recycle`,`chunk_pool_alias`,`fiber_admit`(CBMC), `stack_depot`,`pbuf_bid`(Spin) | good |
| Liveness (non-starvation, lock-free progress) | scheduler + deque | `live_wake`,`live_deque`(Spin, acceptance-cycle) | good |
| netpoll bucket well-formedness | `netpoll_*.c.inc` | Alloy (`WellFormedImpliesOK`, `BucketsAlwaysOnGlobal`) | good |

Every negative control (`-DBUG_*` / bug `.cfg`) is run and asserted to **fail** —
the checks have teeth. `chase_lev_resize.c` is a **forward-looking research model**
of a hypothetical resizable deque (the production deque is fixed-capacity) and is
*not* counted as `cldeque.c` coverage.

## New this session (2026-06-17)

* **`spin/netpoll_iouring_loop.pml`** — the io_uring-as-loop backend (gap #1). Models
  the ring-blocked hub, a cross-hub `loop_wake_fd` eventfd kick with the
  `ring_waiting` **Dekker handshake** (dual SEQ_CST fences), the multishot re-arm,
  and the op-drain resume. Positive: `errors: 0` over 67,779 states. Controls
  (all caught): `BUG_NO_FENCE`, `BUG_NO_RECHECK` (Dekker halves → lost cross-hub
  kick), `BUG_NO_REARM` (un-polled wake source → parked consumer never woken),
  `BUG_DOUBLE_RESUME` (drain not gated on `prev==PARKED`). `BUG_NO_TIMEOUT`
  intentionally still passes — the `idle_ns` timeout is a latency backstop, not
  the correctness mechanism.
* **`tla/RunloomMnRun.tla`** — the `mn_run` deadlock-census + stall-kick liveness
  backstop (gap #2). SAFETY `NoFalseDeadlock` (never a deadlock verdict while a
  wake source exists), LIVENESS `EventuallyRun` (a stranded-runnable g is always
  kicked + run). Bug cfg (idle-cond-only kick) violates `EventuallyRun` — the
  documented cov_workload --hubs 4 loop-backend hang; safety cfg fires a genuine
  verdict (non-vacuous). Verified with TLC (java 21 / tla2tools 1.7.4).

## Remaining gaps (prioritized)

| Gap | Source | Risk | Suggested |
|---|---|---|---|
| Buffered-channel ring send/recv ordering | `chan_ops.c.inc` (`buf_push`/`buf_pop`) | MED | Spin for waiter-wake FIFO + buffer conservation; Coq ∞ for "no value lost/dup across N sends". Currently covered only *by composition* (`ch->lock` + verified primitives). |
| netpoll parker link/unlink list surgery | `netpoll_parker_link.c.inc` | MED | Spin for list-pointer integrity (slot-pointer trick, `by_fd` splice, stale self-ref detach). **Not** benign straight-line: the source documents a *residual M:N free-threaded race not yet fully isolated* + a previously-shipped double-decrement. `netpoll_multipool`/`netpoll_forceunlink` cover the lock hierarchy + release, not the splice. |
| Foreign-OS-thread primitive fallback | `runloom/sync.py`, peek paths | MED | The "not-a-goroutine → real OS block, don't alloc a sched" decision for a monkey-patched `Lock`/`Condition` on a stdlib daemon thread. Largely a control-flow invariant; better as a hardened runtime check + TSan corpus than a model. |
| `mn_run` *timed* detector tuning | `mn_sched_init_fini.c.inc` | LOW | `RunloomMnRun.tla` (new) covers the census/stall-kick logic; the `RUNLOOM_DEADLOCK_MS` / `RUNLOOM_STALL_KICK_MS` timing thresholds themselves are policy, not modeled. |
| timer/sleep min-heap mechanics | `runloom_sched_core.c.inc` | LOW | CBMC layout/bounds (single-owner, `pool->lock`-serialized). `netpoll_deadline` covers the claim race, not the sift/`heap_index`-remove. |
| single-thread scheduler drain + its deadlock detector | `runloom_sched_drain.c.inc` | LOW | Mostly single-threaded (pre-hub / fork-child path); the M:N drain + census is modeled (`RunloomMnRun`), this single-thread analogue is not. |

Not gaps: `netpoll_diag_fd.c.inc` / `netpoll_init.c.inc` (introspection + setup, not a live wake protocol), `runloom_stackadvice.c` (benign racy size hints), the prewarm daemon in `coro.c` (pure-C, no PyThreadState → invisible to STW). The audit found **no** subsystem the map claims as covered but is actually bare — only uncredited *bonus* coverage (now folded into the index above).

## Documentation debt (found by the 2026-06-17 audit; proofs unaffected)

These do **not** invalidate any proof — they mis-*describe* what is verified:

1. **`netpoll_rearm.pml` + README §9 model a *replaced* epoll arm scheme** (MED).
   They describe "LEVEL + `EPOLLONESHOT`, re-armed via `EPOLL_CTL_MOD` per park",
   but production (`netpoll_register.c.inc`) is now "LEVEL, register-per-direction-
   once, **no** `EPOLLONESHOT`, skip `epoll_ctl` on re-park". The proven property
   (LEVEL re-reports a still-ready fd → no edge-drop) still holds; the per-park
   MOD re-arm detail is stale. Fix: re-model the register-once LEVEL scheme.
   (The kqueue model #17, `EV_ADD|EV_ONESHOT` re-add, still matches its source.)
2. **Stale source line citations** across model headers + README (MED). The
   code-layout refactor split the monoliths into `*.c.inc`; headers still cite
   `netpoll.c:1158-2195`, `mn_sched.c:1273`, `io_uring.c:999`, etc. Fix: cite
   function names (split-proof) or re-point to the `.c.inc` files.
3. **README understates the parker link/unlink surgery** as "lock-protected
   straight-line code" despite the documented residual race (MED) — see the gap
   table above.
4. `chase_lev_resize.c` is research, not `cldeque.c` coverage (LOW) — folded in
   above.
5. `RunloomSched.tla` is a whole-scheduler composition model, not tstate-lifecycle
   (LOW) — re-rowed above.

## Run

```sh
verify/run_verify.sh          # Spin + CBMC + GenMC + herd7 + Coq + Iris + Alloy
verify/tla/run_tla.sh         # TLC
scripts/check_all.sh verify   # both, as the gate
```

## Add a model

A new Spin model registers in `run_verify.sh` via `check_spin <name> "<desc>"`
plus one `check_spin_must_fail <name> <BUG_DEFINE> "<desc>"` per negative control
(a model with no teeth is not trusted). A TLA+ model registers in
`tla/run_tla.sh` with a correct `.cfg` (expect "No error has been found") and a
bug `.cfg` (expect a violation). Cite the source by **function name**, not line
number, so the reference survives the next file split.

## Complete model index

Every model file, by engine (74 total: Spin 23, CBMC 12, GenMC 13, Coq 4,
Iris 6, TLA+ 10, herd7/litmus 5, Alloy 1). The subsystem-level grouping is the
**Coverage map** above; this is the exhaustive file list.

### Spin — `spin/*.pml` (23)
| file | what |
|---|---|
| `cldeque.pml` | Chase-Lev deque: no loss / dup / phantom |
| `wake_state.pml` | per-g wake_state machine: no lost wake / double-resume / dup runq entry |
| `parked_safe.pml` | park_safe/wake_safe handshake: no lost wake, balanced |
| `park_generic_timed.pml` | fd-free TIMED in-memory park: enqueued exactly once |
| `select_claim.pml` | select() cross-channel `fired_case` claim CAS |
| `select_close.pml` | select() Phase-2 vs send/close (+4 controls) |
| `hub_submit.pml` | default M:N wake: `in_sub_queue` dedup + done-check |
| `blockpool.pml` | blocking-offload wake order: re-queue before dec inflight |
| `netpoll_commit.pml` | netpoll park/wake commit (Go netpollblockcommit) |
| `netpoll_rearm.pml` | netpoll LT re-arm vs not-yet-linked window (⚠ drift: models replaced EPOLLONESHOT scheme) |
| `netpoll_multipool.pml` | multi-pool dispatch `pool→sub` lock hierarchy |
| `netpoll_deadline.pml` | fd-dispatch vs timeout-drain vs cancel claim race |
| `netpoll_forceunlink.pml` | force_unlink vs pump: exactly-once release / no UAF |
| `netpoll_kqueue.pml` | kqueue `EV_ADD|EV_ONESHOT` re-add arm (BSD/macOS) |
| `netpoll_afd.pml` | IOCP+AFD poll-ctx lifetime (Windows): no UAF / double-free |
| `netpoll_iouring_loop.pml` | **NEW** io_uring-as-loop backend Dekker wake + re-arm |
| `iouring_msclose.pml` | io_uring multishot handle lifetime, recv vs close |
| `cross_thread_wake.pml` | Phase C per-thread sched owner-routed wake_safe |
| `tstate_attach_detach.pml` | per-g PyThreadState resume slice attach/detach balance |
| `stack_depot.pml` | cross-hub coroutine stack-memory pool (size guard + cap) |
| `pbuf_bid.pml` | io_uring provided-buffer-ring bid ownership |
| `live_wake.pml` | LIVENESS: woken g eventually resumed (weak fairness) |
| `live_deque.pml` | LIVENESS: lock-free steal progress under any schedule |

### CBMC — `cbmc/*_cbmc.c` (12, on real C with real `__atomic`)
| file | what |
|---|---|
| `cldeque_cbmc.c` | the real `cldeque.c`: no loss/dup/phantom (bounded) |
| `wake_state_fsm_cbmc.c` | per-g wake_state FSM totality + no-lost-wake (+ `BUG_TIMER_CLAIM_DROPS`) |
| `io_classify_cbmc.c` | **I/O-return classifier FSM** totality + mask-soundness (T5) |
| `preempt_defer_cbmc.c` | preempt defer-during-destruction gate (p69b) |
| `sched_qref_cbmc.c` | default-path goroutine queue-membership ref (try_incref-before-CAS) |
| `sched_readyring_cbmc.c` | per-sched ready FIFO ring |
| `sched_pystate_cbmc.c` | per-goroutine tstate snapshot harness |
| `chan_refflow_cbmc.c` | PyObject ref conservation through a channel |
| `snap_refown_cbmc.c` | tstate-snapshot reference-ownership discipline |
| `chunk_pool_alias_cbmc.c` | datastack-chunk pool never aliases a live chunk |
| `g_slab_recycle_cbmc.c` | `runloom_g_t` slab-recycle layout |
| `fiber_admit_cbmc.c` | max-fibers admission conservation |

### GenMC — `genmc/*.c` (13, real C under RC11)
| file | what |
|---|---|
| `chase_lev.c` / `chase_lev2.c` | Chase-Lev deque oracle (1- and 2-element) |
| `chase_lev_real.c` | the **unmodified production `cldeque.c`** under RC11 |
| `chase_lev_resize.c` | *research:* hypothetical resizable deque (not shipped) |
| `sched_parkwake.c` | park_safe/wake_safe handshake (the SC fence was discovered here) |
| `sched_parkwake_seam.c` | the seam between runloom's two wake paths |
| `netpoll_claim.c` | netpoll commit-claim race |
| `blockpool_job.c` | blocking-offload job lifetime seam |
| `iouring_waitcommit.c` | io_uring single-op park/wake commit |
| `chan_refcount.c` | `runloom_chan_t` refcount free protocol |
| `brc_merge.c` | CPython biased-refcount cross-thread merge (oracle) |
| `qsbr_drain.c` | CPython QSBR grace-period reclaim (oracle) |
| `mimalloc_page_free.c` | mimalloc per-page `xthread_id` ownership (oracle) |

### Coq — `coq/*.v` (4, unbounded)
| file | what |
|---|---|
| `Deque.v` | Chase-Lev conservation, unbounded |
| `WakeState.v` | per-g wake_state machine, unbounded |
| `Select.v` | select() claim CAS, unbounded |
| `Blockpool.v` | blocking-offload wake order, unbounded |

### Iris / iRC11 — `iris/**/*.v` (6, separation logic)
| file | what |
|---|---|
| `OneShotWake.v` | CAS-based one-shot wake (HeapLang) |
| `WakeQueue.v` | wake_state protocol's two-token exclusion |
| `TreiberStack.v` | lock-free Treiber stack (the stack-pool shape) |
| `rc11/CommitPublish.v` | Stage-3 iRC11: commit-CAS-then-publish weak-memory |
| `rc11/WakeListHandoff.v` | Stage-3 iRC11: cross-thread wake_list handoff |
| `rc11/chase_lev/StealClaim.v` | *experiment:* Chase-Lev steal-claim under iRC11 |

### TLA+ — `tla/*.tla` (10, global temporal; TLC via `tla/run_tla.sh`)
| file | what |
|---|---|
| `RunloomSched.tla` | whole M:N scheduler: NoDoubleRun / DoneIsTerminal + liveness |
| `RunloomComposite.tla` | composed scheduler hang-freedom (wake/park + dispatch + routing) |
| `RunloomMnRun.tla` | **NEW** mn_run deadlock-census + stall-kick liveness backstop |
| `RunloomMnFini.tla` | teardown stop-signal handshake under `idle_lock` |
| `RunloomHandoff.tla` | wedged-hub rescue/handoff stall recovery |
| `RunloomMNControl.tla` | controlled-replay baton (acquire/release/timed) |
| `RunloomCPythonSTW.tla` | CPython free-threaded attach/detach + stop-the-world |
| `RunloomGilstate.tla` | hub-tstate gilstate create/delete on the owning thread |
| `RunloomTstateMigration.tla` | per-g tstate migration abandon/adopt page ownership |
| `RunloomGRefcount.tla` | per-g refcount ledger composed with wake_state |

### herd7 litmus — `litmus/*.litmus` (5, C11/RC11 fence placement)
| file | what |
|---|---|
| `commit_cas_then_publish.litmus` | commit-CAS acquire alone → stale read reachable (Sometimes) |
| `commit_lock_publish.litmus` | `pool->lock` round-trip closes it (Never) |
| `wakelist_mpsc.litmus` | cross-thread wake_list handoff ordering (Never) |
| `parkwake_no_fence.litmus` | park/wake StoreLoad without the SC fence → reorder |
| `parkwake_sc_fence.litmus` | park/wake with the SC fence → safe |

### Alloy — `alloy/selfcheck.als` (1)
| file | what |
|---|---|
| `selfcheck.als` | netpoll bucket well-formedness ⇒ self_check invariant (+ dangling-bucket control) |
