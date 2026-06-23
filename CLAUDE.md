# runloom — project guidance

Full derivations for the invariants below: [docs/dev/RUNTIME_GOTCHAS.md](docs/dev/RUNTIME_GOTCHAS.md).

## Build & test
- Target **free-threaded CPython 3.13t** (M:N is only real with the GIL off):
  `~/.pyenv/versions/3.13.13t/bin/python3`, `PYTHON_GIL=0`.
- Build `python setup.py build_ext --inplace`; run with `PYTHONPATH=src`.
- Run the suite via `tests/run_isolated.py` (one file/subprocess — in-process
  `pytest tests/` flakes on cross-file state leaks).

## No hosted CI
- **Never add GitHub Actions / `.github/workflows/*.yml`** (leave
  `workflows-disabled/` disabled). Hosted CI isn't free; this has been asked for
  repeatedly.
- Gate locally: **`scripts/check_all_fast.sh` before any merge**;
  `scripts/check_all_extensive.sh` for a risky/large merge.

## Agent-shell gotchas
- Each shell caps `RLIMIT_NOFILE` at 4096 — raise it in the SAME block before a
  socket bench: `sudo -n prlimit --pid $$ --nofile=8388608:8388608` (doesn't
  persist). Kernel ceilings via `sudo -n sysctl -w …` (`vm.max_map_count`, ~2
  VMAs/goroutine, bites first at N≫100K).
- Shells run `set -e` — prefix multi-step blocks with `set +e` so a nonzero step
  (e.g. `pkill` with no match) doesn't abort the block.
- Deletions: `safe-rm`, never `rm`.

## Benching
- Measure the runtime, not setup: parallel `SO_REUSEPORT` acceptors, establish
  all N first, count round-trips over a fixed window. Race-free counters (one
  `bytearray(N)` slot per goroutine — a shared `+= 1` loses increments GIL-off).
- Loopback packets traverse the host nft ruleset (~14% throughput tax) — pass
  `--netns` to big_100 for a clean number.

## Scheduler invariants
- **A freed `runloom_g` struct never returns to the OS.** `slab_free` retains it
  (refcount 0, magic DEAD); a stale dup-wake reaches `hub_submit`, which reads
  `g->refcount` via `try_incref` — only sound while the struct is a valid g.
  Freeing → garbage refcount → SIGSEGV (arm64). Guard: `verify/cbmc/sched_qref_cbmc.c`.
- **Signals deliver INTO the parked goroutine, not via the scheduler.** A handler
  raising during a cooperative wait propagates out of *that call*; the idle
  scheduler carries one out of `run()` only when nothing is parked. Path:
  `runloom_netpoll_signal_wake` + the `RUNLOOM_NETPOLL_SIGNALED` sentinel.
- **Future-completion wakes are call_soon-FIFO.** `wake_safe` keeps its
  same-thread fast-path (ready-ring push), detected by PEEKing `runloom_tls_sched`
  — never `runloom_sched_get()` (mallocs on a foreign waker). Guard:
  `runloom_compat/call_soon_fifo.py`.
- **Preemption never yields mid object-destruction.** Both yield sites gate on
  `runloom_tstate_in_destruction` and defer (trigger stays armed); yielding inside
  a `tp_dealloc` freezes a half-dead object across a GC-safe point → UAF. Don't
  reroute via the eval-breaker.
- **Cooperative primitives are foreign-OS-thread-safe.** A non-goroutine thread
  (a patched `Lock` in an mp.Queue `_feed` thread) must detect no-goroutine (TLS
  peek NULL) and block on the real OS — never park a non-existent g, never lazily
  alloc sched state (`peek_current`, never `sched_get`).

## aio bridge invariants (src/runloom/aio/)
- Layout: `_base.py` is the foundation (`_go_io`, `_wait_fd`, `_CURRENT_TASKS`);
  the loop is composed from `loop_*.py` mixins; internals reachable via PEP 562
  `__getattr__`.
- **Protocol-callback goroutines need a roomy stack.** `data_received` /
  `connection_made` / … run user C-recursing code (asyncssh kex) → guard-page
  SEGV on a grown-down stack. Spawn via `_go_io` (`_IO_STACK`, 512 KB); don't
  revert to bare `runloom_c.fiber`.
- **Timer goroutines read the callback THROUGH the handle.** Capturing
  `callback`/`args` in the runner closure leaks cancelled timers' graphs. Guard:
  `runloom_compat/timer_leak.py`.
- **`_StreamTransport` seeds `self._io_g = None` before `connection_made`.** A
  write inside connection_made kicks io before `__init__` finishes; the post-cm
  spawn is `if self._io_g is None`. Guard: `runloom_compat/tls_connection_made_write.py`.
- **Loop-level callbacks run with no current task.** Route via `_pg_run_loop_cb`
  (clears `_CURRENT_TASKS[loop]`), else a stock-Task wakeup hits enter_task and
  the wake is dropped. Guard: `runloom_compat/aiohttp_leak_probe.py`.
- **`Server.close()` wakes its accept loops** — `cancel_wait_fd()` the parked
  accept goroutines or they leak. Guard: `runloom_compat/goroutine_leak_char.py`.
- **`loop.sock_*` releases the fd's netpoll arm on completion.**
  `@_release_fd_after` → `netpoll_release_if_idle(fd)`, else a reused fd number
  hangs on the stale arm cache. Don't drop the decorator or the register-once
  skip. Guard: `tests/test_aio_fd_reuse.py`.
- **Future done-callbacks defer through call_soon, in asyncio order.**
  `_fire_callbacks` defers all but `RunloomTask._wake_unpark` and
  `_runloom_fire_sync`-tagged callbacks. Guard: `runloom_compat/ws_close_order_repro.py`.
- **The driver resumes with `coro.send(None)`, never `send(future.result())`.** A
  custom awaitable-iterator takes the `.send()` branch on a non-None value and
  raises. Guard: `runloom_compat/aiocsv_repro.py`.
