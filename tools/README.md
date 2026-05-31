# pygo concurrency tooling

Tools for exposing deadlocks, hangs, races, and crashes in the pygo
runtime — and the harnesses that drive them hard.

| tool | purpose |
|------|---------|
| [`watchdog.py`](watchdog.py) | turn a silent hang into a full state dump (thread stacks + scheduler self-check + stats + lifecycle event ring) |
| [`mn_stress.py`](mn_stress.py) | seeded randomized fuzzer for the M:N (multi-hub) scheduler: cross-hub channels + select under real parallelism, with conservation checks |
| [`run_sanitizers.sh`](run_sanitizers.sh) | build + run the C concurrency harnesses under ASan / TSan / UBSan |

See also `../verify/` (formal proofs) and `../tests_c/test_cldeque.c`
(deque stress).

## watchdog.py — hang / deadlock detector

A goroutine deadlock or a scheduler lost-wake looks like a process that
just stops: `run()` / `mn_run()` never returns, no exception. This makes
it loud and debuggable.

```python
from tools.watchdog import run_guarded, watchdog, hang_dump

# test-friendly: runs fn() in a worker thread, raises TimeoutError (after
# dumping full state) if it overruns -- works even when the scheduler is
# wedged, because the wedge is on the worker, not the caller.
run_guarded(lambda: pygo_core.run(), seconds=5.0, label="my workload")

# context-manager form (good when the block DOES return, just slowly;
# pass abort=True to os.abort() for a core dump on a true wedge):
with watchdog(5.0, label="ping-pong", abort=False):
    pygo_core.run()

# or dump state on demand from anywhere:
hang_dump(label="manual")
```

On a breach it dumps: every OS thread's stack (`faulthandler`), the
scheduler/netpoll `_self_check`, `stats()`, and the per-thread lifecycle
event ring (`_diag_dump`). For the ring to contain anything, start with
`PYGO_DEBUG=ring,gstate` (read once at import).

Self-demo (catches a deliberate non-terminating scheduler):

```sh
PYGO_DEBUG=ring,gstate python tools/watchdog.py
```

## mn_stress.py — M:N scheduler fuzzer

The rest of `tests/` runs single-threaded; this hammers the multi-hub
path. Each iteration is a seeded "token conservation" experiment:
producers push a known multiset of tokens into a channel pool, a
coordinator closes them, consumers (some `recv`-range, some `select`)
drain them — and **every token must be received exactly once**.
`_self_check()` must stay clean between iterations, all under the
watchdog so a hang prints its reproducing seed.

```sh
python tools/mn_stress.py --iters 500              # random seed
python tools/mn_stress.py --iters 1 --seed 12346   # deterministic repro
```

Exit 0 = clean; non-zero = conservation mismatch, self-check violation,
or hang (with the offending seed).

## run_sanitizers.sh — C sanitizer harnesses

```sh
tools/run_sanitizers.sh                 # quick (seconds)
tools/run_sanitizers.sh 500000 8 10     # soak
```

Builds and runs `tests_c/test_cldeque` under ASan/TSan/UBSan. TSan runs
are auto-wrapped in `setarch -R` (high-entropy ASLR on 6.x kernels makes
TSan abort otherwise).

---

## Findings (what the tooling has already turned up)

> These are **open** issues this tooling reproduces deterministically.
> They are surfaced here, not fixed.

### A. M:N scheduler corrupts goroutine tstate under contended `select()`

`tools/mn_stress.py --seed 12346` (and `tests/test_mn.py::
test_contended_select_xfail`) reliably **SIGSEGVs**. Backtrace:
`_PyEval_EvalFrameDefault` running a goroutine with a *corrupted
thread-state* (the `tstate` arg aliases a `tupleiter` object; `throwflag`
is garbage) during a `select()` tuple-unpack.

Characterization (all reproduced):
* Needs **contention**: many cross-hub consumers doing `select()` across
  a shared channel pool while producers push and a coordinator closes.
  A single selector, or any amount of plain `recv`-range + `close`, is
  **stable** (see the passing `tests/test_mn.py` cases).
* **Independent of `PYGO_HANDOFF` / `PYGO_PREEMPT`** (crashes with both
  off) and of coroutine stack size (crashes at 32 KB and 512 KB).
* Consistent with the project's known per-g-tstate / cross-hub-migration
  hazard: the corruption is in the contended `select` park/evict path.

The deque, `wake_state`, `park_safe`, and `select`-claim *algorithms* are
proven correct in `verify/`, so the bug is in the **integration** —
likely the cross-hub `select_evict_self` walk / parked-select migration
in `chan.c` under `mn_sched.c`, not the claim CAS itself.

### B. Default 32 KB coroutine stack is too small for `getaddrinfo`

`tests/test_sync.py` SIGSEGVs (standalone) on the first network call.
Root cause: the first `socket.getaddrinfo` triggers a deep C-level codec
import that overflows the 32 KB default coroutine stack — caught cleanly
by the PROT_NONE guard page (so it's a clean fault, not silent
corruption). Fixed by either pre-warming the codec before the goroutine
or raising the stack (`pygo_core.set_stack_size(256*1024)` makes it pass;
warming `getaddrinfo` once before entering a goroutine also does). Worth
a decision on the default-stack floor for I/O-heavy goroutines.
