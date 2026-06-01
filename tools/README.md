# pygo concurrency tooling

Tools for exposing deadlocks, hangs, races, and crashes in the pygo
runtime -- and the harnesses that drive them hard.

| tool | purpose |
|------|---------|
| [`watchdog.py`](watchdog.py) | turn a silent hang into a full state dump (thread stacks + scheduler self-check + stats + lifecycle event ring) |
| [`mn_stress.py`](mn_stress.py) | seeded randomized fuzzer for the M:N (multi-hub) scheduler: cross-hub channels + select under real parallelism, with conservation checks |
| [`run_sanitizers.sh`](run_sanitizers.sh) | build + run the C concurrency harnesses under ASan / TSan / UBSan |

See also `../verify/` (formal proofs) and `../tests_c/test_cldeque.c`
(deque stress).

## watchdog.py -- hang / deadlock detector

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

## mn_stress.py -- M:N scheduler fuzzer

The rest of `tests/` runs single-threaded; this hammers the multi-hub
path. Each iteration is a seeded "token conservation" experiment:
producers push a known multiset of tokens into a channel pool, a
coordinator closes them, consumers (some `recv`-range, some `select`)
drain them -- and **every token must be received exactly once**.
`_self_check()` must stay clean between iterations, all under the
watchdog so a hang prints its reproducing seed.

```sh
python tools/mn_stress.py --iters 500              # random seed
python tools/mn_stress.py --iters 1 --seed 12346   # deterministic repro
```

Exit 0 = clean; non-zero = conservation mismatch, self-check violation,
or hang (with the offending seed).

## run_sanitizers.sh -- C sanitizer harnesses

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

### A. M:N: select() under contention crashed / lost values -- FIXED

Under real free-threaded parallelism, `select()` over channels with a
concurrent `close()` could SIGSEGV, hang, or silently drop values
(`tools/mn_stress.py` full mode mismatched ~1/16 iterations).  Root-caused
to **four** distinct bugs in `chan.c`'s select path, all now fixed:

1. **close-wake returned NULL** (`dcd1988`).  `close()` woke a goroutine
   parked in select Phase-2 with `value == NULL`; `m_select` put that NULL
   into the `(value, ok)` result tuple → SIGSEGV in the caller's
   `v, ok = ...` unpack.  Fixed: close-wake returns a fresh `Py_None`,
   matching every other closed-recv path.
2. **abort path returned a bare -1** (`ae2df38`).  Phase-2 install's
   "channel went ready" abort returned `select_try_each()` directly, which
   is -1 when the ready channel raced away.  For a *blocking* select a bare
   -1 became `PyLong(-1)` → the caller's unpack raised `TypeError`, killing
   the goroutine and dropping every value it had received.  Fixed: retry
   the scan-then-park instead of returning -1.
3. **abort dropped an already-delivered value** (`ae2df38`).  The abort's
   `CAS(fired_case, -1→i)` result was ignored; if a delivery had already
   fired the select on an earlier case (CAS won, value in that waiter), the
   abort evicted/freed the waiter holding it → value vanished.  Fixed: a
   lost CAS means "already fired" → stop installing and park, returning the
   delivered value.
4. **spurious wake returned -2-without-exception** (`ae2df38`).  A resume
   with `fired_case < 0` returned -2 → `m_select` returned NULL with no
   exception set → CPython `SystemError` → dead goroutine.  Fixed: retry
   (Go's spurious-wakeup behaviour), with a 10M-retry guard.

The earlier framing of this finding ("`select(default=True)` busy-poll")
was a mis-minimisation: that repro also had a *usage* bug (unpacking
select's `-1` default-sentinel as a tuple).  The real defects are the four
above and are independent of `default=`.  The deque, `wake_state`,
`park_safe`, and `select`-claim *algorithms* remain machine-proven in
`verify/`; these were integration bugs in the select → park/wake path,
surfaced by the fuzzer + watchdog under M:N.

Verified: `mn_stress` full (select consumers) CLEAN over 3000 iterations
across 6 seeds; single/multi-channel blocking-select+close clean 40/40;
guarded by `tests/test_mn.py::test_select_close_conservation`.

### B. `getaddrinfo` codec import overflowed the goroutine stack -- FIXED

`tests/test_sync.py` used to SIGSEGV on the first network call: the first
`socket.getaddrinfo` triggers a deep C-level codec import
(`encodings.idna` → `stringprep` → `unicodedata`) that overflowed the
32 KB default coroutine stack -- caught cleanly by the PROT_NONE guard
page (a clean fault, not silent corruption).

`pygo.runtime` already had `prewarm_stdlib()` (resolves that import on the
main thread's big stack before any goroutine runs) and `pygo.runtime.run`
/ the aio loop called it -- but `pygo.sync.run`/`pygo.sync.go` did not.
Fixed by calling `prewarm_stdlib()` from the `pygo.sync` entry points
too, guarded so it only warms on the main thread (never on a goroutine's
small stack). `tests/test_sync.py` now passes 7/7.
