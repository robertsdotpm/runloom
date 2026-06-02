# pygo — project guidance

## No hosted CI
- **Do NOT add GitHub Actions or any hosted CI.** Never create
  `.github/workflows/*.yml` (the existing `.github/workflows-disabled/` is
  deliberately disabled — leave it disabled). GitHub-hosted CI is **not free**
  (especially macOS minutes), and we don't want it. This has been asked for
  repeatedly.
- Our "CI" is **local**: `scripts/check_all.sh`. Phases: `tests mn lincheck dst
  ctest sanitizers exttsan verify` (run `scripts/check_all.sh all` for
  everything, or name phases). Run that before proposing a merge.

## Build & test
- Target is **free-threaded CPython 3.13t** — the M:N scheduler is only real
  with the GIL off. Use `~/.pyenv/versions/3.13.13t/bin/python3` with
  `PYTHON_GIL=0`.
- Build: `python setup.py build_ext --inplace`; run with `PYTHONPATH=src`.
- Run the suite via `tests/run_isolated.py` (one file per subprocess); the
  in-process `pytest tests/` flakes under cross-file state leaks.

## Concurrency tooling (tools/)
- `run_sanitizers.sh` (deque ASan/TSan/UBSan), `run_sanitizers_ext.sh` (whole
  ext under TSan via preloaded libtsan + `setarch -R`), `lincheck/` (Porcupine
  + stateful select model), `dst/` (deterministic simulation), `mutate/`
  (mutation testing), `coverage.sh` (gcov), plus `../verify/` (Spin/CBMC/
  GenMC/herd7). See `tools/README.md`.

## Scheduler invariants
- **Signals deliver INTO the parked goroutine, never via the scheduler.** A
  Python signal handler that raises during a cooperative blocking call
  (`select`/`poll`/`recv`/`accept`/…) must propagate out of *that call* into the
  caller's own `try/except` — exactly as a signal interrupting a real
  `recv()`/`select()` does. The idle scheduler must NOT grab a pending handler
  and carry a raised exception out of `run()` while a goroutine is parked in a
  cooperative wait to receive it; it carries one out of `run()` *only* when
  nothing is parked to take it (the idle / sleep-only Ctrl-C case). The delivery
  path is `pygo_netpoll_signal_wake` + the `PYGO_NETPOLL_SIGNALED` sentinel that
  `wait_fd` restores on resume — backend-independent (epoll/kqueue/select).
- **Future-completion wakes must be call_soon-FIFO.** asyncio guarantees a
  future's done-callbacks run in `call_soon` (FIFO) order, so a task awaiting a
  future resumes *before* a callback scheduled later in the same `set_result`.
  A PygoTask parks on a future via `park_safe` / `wake_safe`; `wake_safe` MUST
  keep its same-thread fast-path (push the woken g straight onto the ready ring,
  like `pygo_sched_wake`) rather than routing same-thread wakes through the
  batch-drained `wake_list` — the latter lands the task *after* a later
  `call_soon`, inverting the order (crashed asyncssh: a channel-close callback
  ran before the channel-open awaiter, clearing state it needed). Detect
  same-thread by PEEKING `pygo_tls_sched`, never `pygo_sched_get()` (which
  lazily allocates a sched + runs mimalloc — fatal on a foreign waker thread, a
  run_in_executor blockpool worker / iouring CQE, that has no usable heap).
  Regression guard: `pygo_compat/call_soon_fifo.py`.

## aio bridge invariants (src/pygo/aio.py)
- **Goroutines that synchronously run user protocol callbacks need a roomy
  stack.** `data_received` / `pipe_data_received` / `connection_made` /
  `datagram_received` (and anything dispatched through `call_soon` / `call_at` /
  the keepalive) run on a goroutine's swapped C stack, and user code there can
  recurse deep into C (e.g. asyncssh runs a full SSH kex + chacha20/OpenSSL
  encrypt inside `data_received`). The scheduler's default 128 KB g-stack
  overflows the guard page → SEGV (stock asyncio runs callbacks on the 8 MB
  main-thread stack). Spawn every such goroutine via `_go_io` (`_IO_STACK`,
  default 512 KB, env `PYGO_AIO_IO_STACK`) — the same reason task drivers use
  `_TASK_STACK`. Do NOT revert these to a bare `pygo_core.go`. The 512 KB is
  virtual + pooled; only the asyncio bridge is affected (M:N paths keep 128 KB).

## Conventions
- Use `safe-rm`, never plain `rm`, for any file deletion.
