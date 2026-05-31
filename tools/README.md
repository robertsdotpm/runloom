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

### A. M:N: `select(default=True)` busy-poll loses a parked-sender wake — OPEN

A busy-poll loop on `select([..., ("recv", ch)], default=True)` under the
M:N hub scheduler loses a wake when `ch`'s sender parks (buffer full).
The sender is then **stranded** → `mn_run()` never returns (hang), or —
timing-dependent — the received value is **corrupted** → SIGSEGV in the
result-tuple unpack (`_PyEval_EvalFrameDefault` with a `tstate` arg that
aliases a `tupleiter`; `throwflag` garbage).

Minimal deterministic repro: `tests/test_mn.py::
test_select_default_busy_poll_xfail` (one feeder + one consumer, single
hub reproduces). `tools/mn_stress.py` (full/non-`--stable` mode) also hits
it via its select consumers.

**Tightly isolated by elimination — all of these are CLEAN:**

| variant | result |
|---|---|
| blocking `select([("recv", b)])` (receiver parks) | clean |
| busy-poll `b.try_recv()` (never parks, no select) | clean |
| `select(default=True)` with a buffer big enough the sender never parks | clean |
| single-channel blocking `recv()` loop | clean |

Only `select(default=True)` **+ a sender that parks** fails. So the defect
is in `select`'s interaction with the **parked-sender wake** under the M:N
hub scheduler. It is **NOT**:
* in `select_try_each`'s value/wake handling — that block is textually
  identical to the working `chan_recv_locked` (try_recv);
* preemption/handoff — crashes with `PYGO_PREEMPT=0 PYGO_HANDOFF=0`;
* cross-hub migration — single hub (`mn_init(1)`) reproduces;
* a coroutine-stack overflow — repros at 32 KB and 512 KB.

The deque, `wake_state`, `park_safe`, and `select`-claim *algorithms* are
machine-proven in `verify/`, so this is an **integration** bug in the
select → parked-sender wake path under `mn_sched.c`, scheduling-sequence
dependent (select's slower per-call path takes a different cooperative
schedule than try_recv's). Pinpointing the exact instruction needs C-level
instrumentation of the sender park/wake under that schedule.

Until fixed: prefer **blocking** `select` over a `default=True` busy-poll
under M:N; `recv`/`try_recv` and blocking select are unaffected.

### B. `getaddrinfo` codec import overflowed the goroutine stack — FIXED

`tests/test_sync.py` used to SIGSEGV on the first network call: the first
`socket.getaddrinfo` triggers a deep C-level codec import
(`encodings.idna` → `stringprep` → `unicodedata`) that overflowed the
32 KB default coroutine stack — caught cleanly by the PROT_NONE guard
page (a clean fault, not silent corruption).

`pygo.runtime` already had `prewarm_stdlib()` (resolves that import on the
main thread's big stack before any goroutine runs) and `pygo.runtime.run`
/ the aio loop called it — but `pygo.sync.run`/`pygo.sync.go` did not.
Fixed by calling `prewarm_stdlib()` from the `pygo.sync` entry points
too, guarded so it only warms on the main thread (never on a goroutine's
small stack). `tests/test_sync.py` now passes 7/7.
