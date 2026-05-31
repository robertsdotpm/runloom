# pygo formal verification

Machine-checked correctness for pygo's lock-free concurrency primitives.
Two engines, used for what each is best at:

| engine | what it checks | how |
|--------|----------------|-----|
| **Spin** | the *algorithms*, exhaustively, over **all** thread interleavings (sequentially-consistent memory) | hand-written Promela models in [`spin/`](spin/) |
| **CBMC** | the **actual C source** of the deque, including the real `__atomic_*` memory orderings, over a bounded schedule | harness in [`cbmc/`](cbmc/) compiles `src/pygo_core/cldeque.c` *unmodified* |

They are complementary: Spin proves the algorithm has no bad interleaving
but abstracts the C; CBMC runs on the real code (real index arithmetic,
real acquire/release/seq_cst) but on a small bounded schedule. Together
with the runtime sanitizer stress in `tests_c/test_cldeque.c` (real
threads, millions of ops) that's three independent angles on the same
code.

## Run it

```sh
verify/run_verify.sh         # everything; ~3-4 min (CBMC dominates)
verify/run_verify.sh -q      # quieter
```

Needs `spin`, `cbmc`, and a C compiler:

```sh
sudo apt-get install spin cbmc
```

## What is proven

### 1. Chase-Lev work-stealing deque — `spin/cldeque.pml` + `cbmc/cldeque_cbmc.c`

The run-queue under each M:N hub (`src/pygo_core/cldeque.c`). One owner
pushes/pops the bottom lock-free; thieves CAS the top. Famous for being
*wrong* under weak memory in its original SPAA'05 form (see Lê, Pop,
Cohen, Nardelli, PPoPP'13); `cldeque.c` uses the corrected seq-cst
pop/steal + acquire/release push.

Proven over `owner + 2 thieves`, hitting the 1-element boundary where
the pop CAS races the steal CAS:

* **No duplication** — no work-item is ever returned to two consumers.
* **No loss** — at quiescence `consumed + deque_size == pushed`.
* **No phantom** — a returned item is always a real pushed tag.
* **No deadlock / size never negative.**

CBMC checks the same on the **unmodified `cldeque.c`** (compiled at
`-DPYGO_CLDEQUE_CAP=4` for a tractable SAT instance — the logic is
capacity-independent; production stays 4096). All 5 assertions:
`VERIFICATION SUCCESSFUL`.

### 2. Per-g `wake_state` machine — `spin/wake_state.pml`

The heart of the M:N scheduler: the 6-state CAS protocol documented on
`struct pygo_g.wake_state` (`pygo_sched.h`). Its predecessor — two
separate flags for "exactly-once wake" and "exclusive resume" — raced
into a re-push **livelock**; this model verifies the unified machine
against concurrent wakers (any thread), hubs (pull/resume/release), and
the idle-stack sweeper:

* **No duplicate / orphan run-queue entry** — `qentries == (state == QUEUED)` invariant holds at every reachable state.
* **No double resume** — at most one hub owns the g (`owners <= 1`).
* **No lost wake** — every issued wake is followed by a resume; the
  fully-drained terminal state always has `last_wake_unserved == 0`.

**Negative control:** compile with `-DBUGGY_DROP_WAKE` (a wake dropped
during `RUNNING`, the classic lost-wakeup) and Spin **finds it** —
assertion violated at depth 30 with a counterexample trail. The check
has teeth. `run_verify.sh` runs this and asserts it *does* fail.

### 3. `park_safe`/`wake_safe` handshake — `spin/parked_safe.pml`

The race-safe single-thread park used by `pygo.aio`'s `PygoTask` and the
blocking-offload pool, where a wake can arrive from another OS thread
mid-park (`pygo_sched_park_safe` / `pygo_sched_wake_safe`). Models the
`wake_pending` counter + `parked_safe` CAS handoff verbatim:

* **No lost wake** — the parker never blocks forever at the yield while a
  wake is outstanding (encoded as Spin's invalid-end-state check).
* **Balanced** — `wake_pending` nets to 0; the g is enqueued at most once
  (no double-schedule).

### 4. `select()` claim CAS — `spin/select_claim.pml`

The lock-free core of `select`: a goroutine parked on N channels shares
one `fired_case`; channels race to CAS it from −1 to their own index, and
only the winner does the handoff (`waiter_claim` in `chan.c`). Models N
channels racing to deliver:

* **Fires at most one case** — `wins <= 1` always.
* **Exactly-once wake** — `wake_count == wins`; losers leave a tombstone
  and never wake the goroutine.
* **Consistent result** — `fired_case` ends a valid index = the winner.

### 5. `select()` Phase-2 vs send/close — `spin/select_close.pml`

The full protocol where the 2026-05-31 select crash/loss arc lived (the
claim CAS alone is #4; this models install → abort-on-ready → park → wake
→ result, racing a concurrent send and close on the channel). A blocking
RECV select against a sender, a closer, and a spurious-wake source:

* **WELL-FORMED** — the result is exactly a sent value or *closed*; never
  NULL (the close-wake SIGSEGV) and never the no-case sentinel (a blocking
  select must never report "nothing ready").
* **CONSERVATION** — a value that was produced (claimed into our waiter or
  buffered) is the one returned; the abort / spurious-retry paths must not
  evict-and-drop a just-delivered value.
* **PROGRESS** — always terminates (bounded retries; no deadlock).

This model **found two additional real races** beyond the three the fuzzer
hit (close-wake-NULL, abort-bare-−1, abort-drops-value): (a) a value
buffered in the Phase-1→install window is orphaned if close claims the
waiter first — fixed by re-scanning on a close-wake (buffered drains
before closed); (b) a spurious-retry frees a waiter a racing delivery just
filled — fixed by evicting *before* re-reading `fired_case`. Five negative
controls (`-DBUG_CLOSE_NULL`, `-DBUG_ABORT_NOCASE`, `-DBUG_ABORT_DROP`,
`-DBUG_SPURIOUS`) each reintroduce a bug and make the model fail, so the
properties demonstrably have teeth.

## Scope & honesty

* Spin models are **sequentially consistent**: they prove the algorithm
  has no bad *interleaving*, not the C11 fence placement. The fence
  placement on the deque is what **CBMC** covers (it carries the real
  `__atomic_*` orders). Where the three engines agree, that's strong
  evidence; none is a substitute for the others.
* Bounds are small by necessity (BMC / explicit-state both blow up). The
  deque proofs use 2 thieves and ≤3 items; the wake machine uses 2
  wakers / 2 hubs / 1 sweeper. These are the cardinalities at which the
  known bugs reproduced, so they are the right bounds — but they are
  bounds.
* The **channel send/recv/buffer** logic itself is not modelled
  end-to-end here: it is serialized under `ch->lock`, so its concurrency
  reduces to (verified `wake_state`/`park_safe` primitives) + (verified
  `select` claim) + straight-line locked code. The integrated channel is
  exercised by `tests/test_chan.py`, `tests/test_mn.py`, and
  `tools/mn_stress.py`.

## Layout

```
verify/
  run_verify.sh            driver: runs all Spin + CBMC checks, reports PASS/FAIL
  spin/
    cldeque.pml            Chase-Lev deque (no loss / dup / phantom)
    wake_state.pml         per-g wake_state machine (+ BUGGY_DROP_WAKE control)
    parked_safe.pml        park_safe/wake_safe handshake
    select_claim.pml       select fired_case CAS
  cbmc/
    cldeque_cbmc.c         harness over the real cldeque.c
    stubs/plat_compat.h    minimal stub so cldeque.c compiles standalone under CBMC
```
