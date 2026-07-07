# big_100 — 100 stress projects for the runloom (pygo) extension

100 self-contained workloads that hammer the `runloom` Go-style-coroutine
extension in **M:N parallel mode** (`run(n>1)`, GIL off, free-threaded CPython
3.13t), blocking-style code over `monkey.patch()` — **no `async`/`await`, no
aio bridge**. Each one fields tens of thousands of lightweight goroutines and
exercises one corner of the runtime (sockets, files, subprocess, scheduler,
sync primitives, cancellation, exception/finalizer machinery, …).

Every project shares `harness.py`, which provides the campaign-wide
requirements:

- `--duration` seconds (default **3600**, the 1–2 h design point)
- `--seed` for deterministic per-worker RNG (replay)
- `--hubs` M:N scheduler hubs (must be > 1)
- `--funcs` number of lightweight goroutines (tens of thousands)
- a progress log line every `--log-interval` seconds (default 5)
- a **watchdog** on a real OS thread that fails the process if forward progress
  stalls for `--hang-timeout` seconds (catches scheduler hangs/deadlocks)
- invariant tracking with fail-fast (`H.check` / `H.fail`) → nonzero exit
- final metrics: ops, ops/sec, completed funcs, worker exits, failures, leaked
  fds
- exit codes: `0` ok · `1` invariant failure · `2` setup/exception · `3`
  watchdog hang

## Requirements

- Free-threaded CPython 3.13t built with the extension:
  `~/.pyenv/versions/3.14.4t/bin/python3`, `PYTHON_GIL=0`.
- Build the extension once: `python setup.py build_ext --inplace` (repo root).
- The harness auto-raises `RLIMIT_NOFILE` (via `sudo -n prlimit`) so socket
  projects can open tens of thousands of fds.

## Run one project

```
PYTHON_GIL=0 ~/.pyenv/versions/3.14.4t/bin/python3 big_100/p01_tcp_echo.py \
    --duration 60 --hubs 8 --funcs 10000
```

## Run many in parallel (use the whole box)

`run_all.py` runs projects concurrently as subprocesses. The default packs the
64-core machine: 16 projects at a time × 4 hubs each ≈ 64 cores.

```
PYTHON_GIL=0 ~/.pyenv/versions/3.14.4t/bin/python3 big_100/run_all.py \
    --jobs 16 --hubs 4 --duration 3600
# a subset / a quick smoke:
big_100/run_all.py --only 1,3,7,36 --duration 30 --hubs 4
big_100/run_all.py --from 1 --to 20 --duration 600 --jobs 10 --hubs 6
```

Per-project logs land in `big_100/logs/pNN.log`; a summary table prints at the
end and the orchestrator exits nonzero if any project failed.

## Findings

Building and running the campaign surfaced **10 real bugs/limitations** in the
extension — see [FINDINGS.md](FINDINGS.md) for the full writeups with repros.
Headlines:

- **#1 (fixed):** `monkey.patch()` broke every `runloom.fiber()` (the wrapper
  dropped the stack-size positional arg). Fixed in `src/runloom/monkey/`.
- **#2:** the handoff rescue corrupts memory under high socket concurrency
  (SIGSEGV/SIGBUS). The harness disables it by default (`RUNLOOM_HANDOFF=0`);
  pass `--handoff` to reproduce.
- **#4:** high-rate `runloom.blocking` / subprocess offload deadlocks (a lost
  wakeup in the offload-result wait); worked around with `procutil`.
- **#5:** `close()` doesn't wake a goroutine parked in `accept()` (latent server
  teardown hang); worked around with `netutil.serve_forever`.
- **#6:** a goroutine can only recurse ~50–80 frames before `RecursionError`
  (the C recursion budget isn't reset for the goroutine).
- **#7:** `contextvars` / `threading.local` leak between goroutines (hub-local,
  not goroutine-local).
- **#8/#9/#10:** greenlet can't interleave with goroutine switches; concurrent
  same-module imports false-deadlock; a cooperative lock in `__del__` aborts.

Several of these share one root cause: M:N parallelism breaks CPython's
one-thread-per-unit-of-concurrency assumption. The bug-reproducing knobs are
preserved (e.g. `--handoff`, higher `--funcs`) so each finding stays
demonstrable.

**Running the orchestrator at high `--jobs` is the strongest BUG #4 reproducer**
— the filesystem / subprocess / TLS projects hang at the watchdog deadline once
the box is loaded (each passes in isolation at moderate scale). That is the
campaign's headline result, not a runner defect; the default `--jobs 8` keeps it
manageable, and a clean multi-hour soak of those specific projects needs BUG #4
fixed (or low `--funcs`).

## Project list

See the docstring at the top of each `pNN_*.py`. Numbering follows the original
100-project brief (TCP/UDP/HTTP/TLS, filesystem, subprocess, scheduler, sync
primitives, cancellation, CPython-machinery, and mini-servers).
