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
- There is also a **local self-hosted CI runner** (a cron-driven shell script on
  the dev box, NOT GitHub Actions) that auto-builds+tests every new `origin/main`
  on a matrix: Linux 3.13t + Windows 3.12 + Windows 3.13t. It is **post-merge
  validation, NOT a merge gate.** Do **not** wait on it or block a merge on it —
  a full Windows pass is ~15 min, too slow to sit on. Just **periodically check**
  its result: `~/projects/pygo-ci-runner/ci-status.sh` (or `cat
  ~/projects/pygo-ci-status/latest.txt`). `PASS`/`PASS_BASELINE` = fine;
  `REGRESSION` = a NEW failure worth a look (known platform gaps are baselined).

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

## aio bridge invariants (src/pygo/aio/)
- `pygo.aio` is a package (`src/pygo/aio/`): the shared foundation is
  `_base.py` (`_go_io`, `_wait_fd`, `_CURRENT_TASKS`, the lazy CoLock); the
  event loop is composed from the `loop_*.py` mixins into `loop.py`; futures /
  tasks / streams / tls_* / transport_* split out by role. `from ._base import
  *` is the foundation import. Internals stay reachable as `pygo.aio.<name>`
  via a PEP 562 `__getattr__` in `__init__.py`.
- **Goroutines that synchronously run user protocol callbacks need a roomy
  stack.** `data_received` / `pipe_data_received` / `connection_made` /
  `datagram_received` (and anything dispatched through `call_soon` / `call_at` /
  the keepalive) run on a goroutine's swapped C stack, and user code there can
  recurse deep into C (e.g. asyncssh runs a full SSH kex + chacha20/OpenSSL
  encrypt inside `data_received`). The scheduler's default 32 KB g-stack
  overflows the guard page → SEGV (stock asyncio runs callbacks on the 8 MB
  main-thread stack). Spawn every such goroutine via `_go_io` (`_IO_STACK`,
  default 512 KB, env `PYGO_AIO_IO_STACK`) — the same reason task drivers use
  `_TASK_STACK`. Do NOT revert these to a bare `pygo_core.go`. The 512 KB is
  virtual + pooled; only the asyncio bridge is affected (M:N paths keep 128 KB).
- **A timer goroutine must read its callback THROUGH the handle, never via
  closure capture.** call_at/call_later run a goroutine that `sched_sleep`s to
  the deadline then fires `handle._callback`. `asyncio.Handle.cancel()` nulls
  `_callback`/`_args`, so reading through the handle means a CANCELLED timer's
  still-sleeping goroutine holds no reference to the callback or its graph.
  Capturing `callback`/`args` in the runner closure instead leaks them (and
  everything they reach) until the original deadline — broad, since cancelled
  timers are everywhere (timeout/wait_for, retries, retransmits) and it fails
  strict gc-leak teardown checks (aiocoap). Regression guard:
  `pygo_compat/timer_leak.py`.
- **_StreamTransport must seed `self._io_g = None` BEFORE calling
  connection_made.** A protocol that writes inside connection_made (server
  greeting, aiocoap CSM, SMTP banner) reaches `_kick_io`, which reads
  `self._io_g`, while still in the transport `__init__`. Over TLS the write
  can't fast-path (a _TLSSock send can park EPOLLOUT) so it always kicks → an
  undefined `_io_g` was an AttributeError that connection_made swallowed into a
  dropped connection. Seed it None first (so the kick spawns the io goroutine),
  and make the post-connection_made spawn `if self._io_g is None` to avoid two
  io goroutines on one fd (corrupts the one-shot netpoll arm). Regression guard:
  `pygo_compat/tls_connection_made_write.py`.
- **Loop-level callbacks (call_soon / call_at / call_soon_threadsafe) must run
  with NO current task active** — route them through `_pg_run_loop_cb`, which
  clears `_CURRENT_TASKS[loop]` for the callback and restores it after, exactly
  like stock asyncio's `_run_once` (current_task() is None there; a deferred
  `Task.__step` does its own enter_task/leave_task). A PygoTask that parks
  mid-`coro.send` via a raw pygo park leaves the slot pointing at itself; without
  this clear, a deferred STOCK-Task wakeup running at loop level hits enter_task's
  "Cannot enter into task X while another task Y is being executed", and pygo
  drops the wakeup → the woken task hangs (aiohttp connector `_wait_for_close`
  teardown deadlock). Generalizes 78c1d03 (the `_wait_fd` save/restore) to the
  callback side. Regression guard: `pygo_compat/aiohttp_leak_probe.py`.

- **Server close() must wake its accept-loop goroutines.** Each create_server
  accept loop parks in `_wait_fd(listen_fd, READ)`; `close()` must
  `cancel_wait_fd()` them (then they see `_closed` and exit), else they stay
  parked forever on the closed listen fd -- a one-per-server goroutine leak in a
  long-lived loop (per-test loop reset hides it). Regression guard:
  `pygo_compat/goroutine_leak_char.py` (parked stays 0 across cycles).

- **Future done-callbacks defer through call_soon, in asyncio order.**
  `Future.__schedule_callbacks` in asyncio defers EVERY done-callback via
  `loop.call_soon` -- a waiting Task's `__wakeup` AND library/user done-callbacks
  (gather's `_done_callback`, aiojobs' job `_done_callback`, ...). So a setter
  that completes a future and KEEPS RUNNING, or a task whose own done-callback
  mutates shared state, is observed in asyncio order: the waiter scheduled first
  (by an earlier `set_result`) resumes BEFORE the future's later done-callbacks.
  `_fire_callbacks` must therefore defer every callback EXCEPT
  `PygoTask._wake_unpark` (pygo's own await-wake primitive -- deferring it would
  spawn a goroutine per await and break park/unpark; it only readies the g, which
  is FIFO-after an already-readied waiter, so ordering still holds) and callbacks
  tagged `_pygo_fire_sync` (the run loop's `_stop_on_done`, which must stop the
  drive in the same turn). Stock C-Task/`_PyTask` wakeups keep the
  `_run_stock_task_cb` trampoline (re-entry-unsafe). Firing library callbacks
  synchronously inverted the order and broke the falcon/uvicorn websocket-close
  ordering (a close frame's done-callback ran before the recv waiter) and
  aiojobs' pending-job promotion. Regression guard:
  `pygo_compat/ws_close_order_repro.py` (frame_sent True == stock).
  NOTE: this matches asyncio's *callback* order, not its inline-task-step: a
  woken PygoTask is readied (a goroutine), not run-to-next-await inline inside
  its wakeup, so a done-callback racing the woken task's NEXT step is still M:N
  (aiojobs `test_job_close_exception`).

## Conventions
- Use `safe-rm`, never plain `rm`, for any file deletion.
