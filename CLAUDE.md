# runloom — project guidance

## No hosted CI
- **Do NOT add GitHub Actions or any hosted CI.** Never create
  `.github/workflows/*.yml` (the existing `.github/workflows-disabled/` is
  deliberately disabled — leave it disabled). GitHub-hosted CI is **not free**
  (especially macOS minutes), and we don't want it. This has been asked for
  repeatedly.
- Our "CI" is **local**: `scripts/check_all.sh`. Phases: `tests mn lincheck dst
  ctest sanitizers exttsan verify ftconform` (or name phases). `ftconform` conforms
  the REAL CPython stop-the-world against the TLA+ model via the instrumented
  `--with-pydebug` interp; it's in both the fast and extensive lanes but **skips
  cleanly** where that oracle isn't set up, so it only runs on the dev box / CI
  runner (~5s there).
- **Run `scripts/check_all_fast.sh` before proposing a merge.** It is the routine
  pre-merge gate: the quick correctness phases + a *fast* formal lane (all Spin
  models + every cheap CBMC proof, parallelised) that **skips the 3 genuinely
  slow CBMC proofs** (the Chase-Lev concurrent deque ~148s, the INV_race
  disjointness monitor ~5-10min, and the timer min-heap ~76s — measured, not
  guessed). It keeps ~all formal coverage as a sub-minute smoke gate.
- **Run `scripts/check_all_extensive.sh` (== `check_all.sh all`) before a risky
  or large merge, or periodically.** It runs EVERYTHING incl. those 3 slow
  proofs, all sanitizers, and the static analyzer. The `verify`/`verify-fast`
  phases run their checks through a parallel worker pool (`VERIFY_JOBS`, default
  nproc; `=1` for serial) — so the full suite is far faster than the old serial
  run, but its floor is the single slowest proof (the disjoint monitor), which
  no parallelism removes. The self-hosted CI runner (below) also exercises the
  full matrix post-merge, so the slow proofs are not skipped in practice.
- There is also a **local self-hosted CI runner** (a cron-driven shell script on
  the dev box, NOT GitHub Actions) that auto-builds+tests every new `origin/main`
  on a matrix: Linux 3.13t + Windows 3.12 + Windows 3.13t + **macOS arm64 3.13t**.
  It is **post-merge validation, NOT a merge gate.** Do **not** wait on it or
  block a merge on it — a full pass is ~15 min, too slow to sit on. Just
  **periodically check** its result: `~/projects/runloom-ci-runner/ci-status.sh`
  (or `cat ~/projects/runloom-ci-status/latest.txt`). `PASS`/`PASS_BASELINE` = fine;
  `REGRESSION` = a NEW failure worth a look (known platform gaps are baselined).
  Each target deletes its built extension before the `--force` rebuild, so a
  broken build surfaces as `BUILD_FAIL` instead of hiding behind a stale `.pyd`/
  `.so`.  (`test_freethread_stress::test_gc_stw_under_goroutine_churn` was a
  baselined win-3.13t hang — a stop-the-world MONOPOLY deadlock that in fact
  reproduced on Linux too; fixed in the scheduler 2026-06-03 and un-baselined,
  so a regression now surfaces as `REGRESSION`.)

## Build & test
- Target is **free-threaded CPython 3.13t** — the M:N scheduler is only real
  with the GIL off. Use `~/.pyenv/versions/3.13.13t/bin/python3` with
  `PYTHON_GIL=0`.
- Build: `python setup.py build_ext --inplace`; run with `PYTHONPATH=src`.
- Run the suite via `tests/run_isolated.py` (one file per subprocess); the
  in-process `pytest tests/` flakes under cross-file state leaks.

## fd limits at scale (the prlimit gotcha)
- **Every shell spawned by the agent reverts `RLIMIT_NOFILE` to a HARD cap of
  4096**, even though systemd's `DefaultLimitNOFILE` and the editor process
  itself are 1M+. This is NOT a system policy you can fix in
  `/etc/security/limits.conf` or systemd: the VS Code / agent host explicitly
  `setrlimit(NOFILE, 4096)` on each shell it forks, *after* any OS policy
  applies. A process can't raise its own hard limit without privilege, so a
  fresh shell is stuck at 4096 — which silently strangles any socket bench past
  ~2000 connections with `EMFILE` (it looks like a hang / super-linear slowdown,
  not an error).
- **Fix per command block** (the only thing that works): raise this shell's
  ceiling first, in the SAME block as the run —
  `sudo -n prlimit --pid $$ --nofile=8388608:8388608`. It does NOT persist to
  the next block (each is a fresh shell). The scale benches wrap this for you:
  `tests_c/scale_bench.sh` raises its own `$$` then execs.
- Kernel ceilings (raised, persist until reboot): `fs.nr_open`,
  `vm.max_map_count` (~2 VMAs/goroutine — the first to bite at N≫100K),
  `net.core.somaxconn`. `sudo -n sysctl -w ...` to bump them.
- The shell also runs with `set -e` from the sourced snapshot, so a command
  that exits nonzero (e.g. `pkill` matching nothing) aborts the whole block.
  Prefix multi-step blocks with `set +e`.

## Steady-state throughput benching (don't measure the harness)
- A connection bench that uses ONE accept loop, a synchronized connect storm,
  and 1 round-trip/conn measures *connection setup*, not the runtime. For a real
  throughput number: parallel acceptors (`SO_REUSEPORT`, separate listener fds —
  many accept goroutines on ONE fd thunder-herd on its EPOLLONESHOT netpoll
  reg), establish all N first, then count round-trips over a fixed window.
- **Race-free counters are mandatory with the GIL off.** A shared `counter += 1`
  from N goroutines LOSES increments (read-modify-write race), which silently
  under-counts — e.g. a ramp barrier that never reaches N and stalls to a
  timeout. Use one distinct slot per goroutine (`bytearray(N)`, single writer
  each) and sum at the boundary. Cost decomposition vs Go:
  `tests_c/bench_throughput_py.py` + `bench/bench_throughput_go.go`.

## Loopback firewall tax (use a netns for clean network benchmarks)
- On this Docker host, **every loopback packet traverses the host nft ruleset**
  (Docker's `filter`/`mangle`/`nat` base chains + conntrack).  Measured on p01
  (TCP echo) under `perf`: `nft_do_chain` + conntrack/NAT is **~9% of total CPU**
  and costs **~14% of throughput** -- a measurement artifact, NOT a runloom cost,
  and absent on real-NIC paths.
- **NOTRACK on `lo` does NOT fix it** (`iptables -t raw -A OUTPUT -o lo -j
  NOTRACK`): it only skips *conntrack* (~1%); the dominant cost is *chain
  traversal* (`nft_do_chain` runs the filter/mangle base chains regardless of
  tracking).  Verified 9.4% -> 8.9% (noise).  Don't bother re-trying this.
- **The fix is a fresh network namespace** -- its empty ruleset means the chains
  never run (9.4% -> 0.2%, ~85k -> ~97k ops on p01).  Pass **`--netns`** to any
  big_100 program: the harness re-execs via `unshare --net --map-root-user` (no
  sudo).  Caveat: in that user-ns the harness's `sudo prlimit` is a no-op, so
  RLIMIT_NOFILE stays at the inherited cap (4096) -- raise it in the launching
  shell for >~2k-socket runs.
- For PROFILING with kernel symbols, drive a root netns:
  `sudo ip netns add ns; sudo ip netns exec ns bash -c 'ip link set lo up;
  perf record -F 499 -g -- env PYTHON_GIL=0 PYTHONPATH=src python big_100/pNN.py
  --hubs 8 --rounds 0 --duration 12'` with `kptr_restrict=0` +
  `perf_event_paranoid=-1` so kernel syms resolve (restore both to 1 after).

## Concurrency tooling (tools/)
- `run_sanitizers.sh` (deque ASan/TSan/UBSan), `run_sanitizers_ext.sh` (whole
  ext under TSan via preloaded libtsan + `setarch -R`), `lincheck/` (Porcupine
  + stateful select model), `dst/` (deterministic simulation), `mutate/`
  (mutation testing), `coverage.sh` (gcov), plus `../verify/` (Spin/CBMC/
  GenMC/herd7). See `tools/README.md`.

## Scheduler invariants
- **A freed `runloom_g` STRUCT must never be returned to the OS.**
  `runloom_g_slab_free` keeps every freed g struct in the per-thread slab
  (valid, refcount 0, magic=DEAD) — it does NOT `free()` past
  `RUNLOOM_G_SLAB_CAP` (the cap is a soft hint). Reason: a stale `wake_g` (the
  netpoll dup-wake) can reach `hub_submit` with a pointer to an already-freed g
  AFTER completion freed it; `hub_submit` decides whether to re-queue by reading
  `g->refcount` via `runloom_g_try_incref` (incref-iff-refcount>0), which is only
  sound while the struct is still a valid g (try_incref reads 0 → bails). If the
  struct were freed to the OS and its memory reused by a non-runloom malloc,
  try_incref reads a garbage refcount, spuriously succeeds, and enqueues garbage
  → SIGSEGV in `runloom_coro_resume` on arm64 (weak memory; x86-TSO hid it). The
  g's coro/stack ARE still released at completion — only the small struct is
  retained, bounded by peak concurrency. Pairs with the try_incref-before-CAS
  queue ref in `hub_submit` (the CAS-then-incref order has a UAF window;
  CBMC-proven in `verify/cbmc/sched_qref_cbmc.c`). A bounded QSBR/quarantine
  reclaim (free only once no wake can reference the struct) is the follow-up for
  huge burst-then-idle RSS; do NOT reintroduce an eager `free()`.
- **Signals deliver INTO the parked goroutine, never via the scheduler.** A
  Python signal handler that raises during a cooperative blocking call
  (`select`/`poll`/`recv`/`accept`/…) must propagate out of *that call* into the
  caller's own `try/except` — exactly as a signal interrupting a real
  `recv()`/`select()` does. The idle scheduler must NOT grab a pending handler
  and carry a raised exception out of `run()` while a goroutine is parked in a
  cooperative wait to receive it; it carries one out of `run()` *only* when
  nothing is parked to take it (the idle / sleep-only Ctrl-C case). The delivery
  path is `runloom_netpoll_signal_wake` + the `RUNLOOM_NETPOLL_SIGNALED` sentinel that
  `wait_fd` restores on resume — backend-independent (epoll/kqueue/select).
- **Future-completion wakes must be call_soon-FIFO.** asyncio guarantees a
  future's done-callbacks run in `call_soon` (FIFO) order, so a task awaiting a
  future resumes *before* a callback scheduled later in the same `set_result`.
  A RunloomTask parks on a future via `park_safe` / `wake_safe`; `wake_safe` MUST
  keep its same-thread fast-path (push the woken g straight onto the ready ring,
  like `runloom_sched_wake`) rather than routing same-thread wakes through the
  batch-drained `wake_list` — the latter lands the task *after* a later
  `call_soon`, inverting the order (crashed asyncssh: a channel-close callback
  ran before the channel-open awaiter, clearing state it needed). Detect
  same-thread by PEEKING `runloom_tls_sched`, never `runloom_sched_get()` (which
  lazily allocates a sched + runs mimalloc — fatal on a foreign waker thread, a
  run_in_executor blockpool worker / iouring CQE, that has no usable heap).
  Regression guard: `runloom_compat/call_soon_fifo.py`.
- **Preemption must NOT yield a goroutine mid object-destruction.** The preempt
  eval-frame wrapper and the single-frame liveness backstop fire at arbitrary
  Python-frame entries — which can be nested inside an in-flight `tp_dealloc`
  (a weakref callback or finalizer, driven by the free-threaded biased-refcount
  cross-thread merge or the trashcan unwind). Yielding there freezes a
  half-finished destructor on the goroutine's coro stack while the hub thread
  returns to hub_main, a **GC-safe point**; a concurrent stop-the-world GC /
  QSBR reclaim on another thread (e.g. a native thread's `gc.collect()`) then
  runs against partially-destroyed objects → use-after-free (crashed
  `test_weakref.test_threaded_weak_key_dict_copy`). Both yield sites gate on
  `runloom_tstate_in_destruction(ts)` (`tstate->delete_later` for the trashcan;
  `brc.local_objects_to_merge` for the merge drain) and DEFER while it is true,
  leaving the trigger armed so the next frame entry after the destructor unwinds
  takes the yield — never lost. Cooperative yields are exempt: they only happen
  at Python-level call points, never nested in a C destructor. Do NOT try to fix
  this by rerouting preemption through the eval-breaker / pending-call boundary —
  the merge's dealloc→callback→eval re-enters `_Py_HandlePending` nested, so a
  pending-call preempt still fires inside the destructor.
- **Cooperative primitives must be FOREIGN-OS-THREAD-safe.** `monkey.patch()`
  replaces `threading`/`select`/… globally, so a cooperative primitive can be
  invoked from a thread that is NOT a goroutine and NOT a hub — most commonly a
  stdlib-internal daemon thread (a `multiprocessing.Queue` `_feed` thread, a
  `concurrent.futures` worker) that takes a patched `Lock`/`Condition`. Such a
  thread has no goroutine, no hub, and no per-thread scheduler. Any primitive it
  can reach must detect this (`_in_goroutine()` is False / a TLS *peek* is NULL)
  and fall back to **real OS blocking**, never (a) park a goroutine that doesn't
  exist (`runloom_c.wait_fd` / `_Parker.park` → block the thread on the wake fd
  with raw `select` instead), nor (b) lazily allocate scheduler state
  (`current_g()` must `runloom_sched_peek_current()`, never `runloom_sched_get()`,
  which mallocs a sched + arms the wake-pump). Violating this raced → SIGSEGV /
  UAF under M:N (free-threaded mp.Queue). Regression net: the synthetic
  multiprocessing corpus under `run(8)` + `monkey.patch()` (stress many copies).

## aio bridge invariants (src/runloom/aio/)
- `runloom.aio` is a package (`src/runloom/aio/`): the shared foundation is
  `_base.py` (`_go_io`, `_wait_fd`, `_CURRENT_TASKS`, the lazy CoLock); the
  event loop is composed from the `loop_*.py` mixins into `loop.py`; futures /
  tasks / streams / tls_* / transport_* split out by role. `from ._base import
  *` is the foundation import. Internals stay reachable as `runloom.aio.<name>`
  via a PEP 562 `__getattr__` in `__init__.py`.
- **Goroutines that synchronously run user protocol callbacks need a roomy
  stack.** `data_received` / `pipe_data_received` / `connection_made` /
  `datagram_received` (and anything dispatched through `call_soon` / `call_at` /
  the keepalive) run on a goroutine's swapped C stack, and user code there can
  recurse deep into C (e.g. asyncssh runs a full SSH kex + chacha20/OpenSSL
  encrypt inside `data_received`). On a *small* g-stack (a grown-down or pinned
  size) this overflows the guard page → SEGV (stock asyncio runs callbacks on the
  8 MB main-thread stack). Spawn every such goroutine via `_go_io` (`_IO_STACK`,
  default 512 KB, env `RUNLOOM_AIO_IO_STACK`) — the same reason task drivers use
  `_TASK_STACK`. The explicit pin matters because it holds 512 KB regardless of
  the scheduler default / calibration / the M:N grow-down (which can shrink an
  unpinned goroutine toward 16 KB). Do NOT revert these to a bare `runloom_c.go`.
  The 512 KB is virtual + pooled; only the asyncio bridge pins it. (NB: the
  scheduler's *unpinned* default is `RUNLOOM_DEFAULT_STACK_SIZE` = 512 KB on both
  the single-thread and M:N paths — `mn_sched_init_fini.c.inc` resolves to
  `h->sched.stack_size`; the older "32 KB default" / "M:N keep 128 KB" figures
  here were stale.)
- **A timer goroutine must read its callback THROUGH the handle, never via
  closure capture.** call_at/call_later run a goroutine that `sched_sleep`s to
  the deadline then fires `handle._callback`. `asyncio.Handle.cancel()` nulls
  `_callback`/`_args`, so reading through the handle means a CANCELLED timer's
  still-sleeping goroutine holds no reference to the callback or its graph.
  Capturing `callback`/`args` in the runner closure instead leaks them (and
  everything they reach) until the original deadline — broad, since cancelled
  timers are everywhere (timeout/wait_for, retries, retransmits) and it fails
  strict gc-leak teardown checks (aiocoap). Regression guard:
  `runloom_compat/timer_leak.py`.
- **_StreamTransport must seed `self._io_g = None` BEFORE calling
  connection_made.** A protocol that writes inside connection_made (server
  greeting, aiocoap CSM, SMTP banner) reaches `_kick_io`, which reads
  `self._io_g`, while still in the transport `__init__`. Over TLS the write
  can't fast-path (a _TLSSock send can park EPOLLOUT) so it always kicks → an
  undefined `_io_g` was an AttributeError that connection_made swallowed into a
  dropped connection. Seed it None first (so the kick spawns the io goroutine),
  and make the post-connection_made spawn `if self._io_g is None` to avoid two
  io goroutines on one fd (corrupts the one-shot netpoll arm). Regression guard:
  `runloom_compat/tls_connection_made_write.py`.
- **Loop-level callbacks (call_soon / call_at / call_soon_threadsafe) must run
  with NO current task active** — route them through `_pg_run_loop_cb`, which
  clears `_CURRENT_TASKS[loop]` for the callback and restores it after, exactly
  like stock asyncio's `_run_once` (current_task() is None there; a deferred
  `Task.__step` does its own enter_task/leave_task). A RunloomTask that parks
  mid-`coro.send` via a raw runloom park leaves the slot pointing at itself; without
  this clear, a deferred STOCK-Task wakeup running at loop level hits enter_task's
  "Cannot enter into task X while another task Y is being executed", and runloom
  drops the wakeup → the woken task hangs (aiohttp connector `_wait_for_close`
  teardown deadlock). Generalizes 78c1d03 (the `_wait_fd` save/restore) to the
  callback side. Regression guard: `runloom_compat/aiohttp_leak_probe.py`.

- **Server close() must wake its accept-loop goroutines.** Each create_server
  accept loop parks in `_wait_fd(listen_fd, READ)`; `close()` must
  `cancel_wait_fd()` them (then they see `_closed` and exit), else they stay
  parked forever on the closed listen fd -- a one-per-server goroutine leak in a
  long-lived loop (per-test loop reset hides it). Regression guard:
  `runloom_compat/goroutine_leak_char.py` (parked stays 0 across cycles).

- **Low-level `loop.sock_*` must release the fd's netpoll registration on
  completion.** They operate on a USER-OWNED socket the caller closes with a plain
  `socket.close()`, which does NOT run the `_close_sock` / monkey netpoll-
  unregister hook (only sockets the bridge itself owns do). Without releasing, the
  single-thread netpoll's per-fd LEVEL arm cache (`runloom_fd_armed[fd]`) stays
  sticky for the closed fd; when the OS reuses that fd NUMBER, `netpoll_register`'s
  already-armed skip (`cur != 0 && target == cur` -> 0 syscalls) sees the stale
  mask and never `EPOLL_CTL_ADD`s the new fd -> its `wait_fd` parks forever (the
  old intermittent `test_recvfrom` / fast-churn hang; deterministic on the first
  fd reuse). Each `sock_*` carries `@_release_fd_after`, which calls
  `runloom_c.netpoll_release_if_idle(fd)` -- DEL + clear the arm IFF no goroutine
  is parked on it (a no-op otherwise; all under `runloom_pool.lock`, the lock
  `wait_fd`/`register` use, so a concurrent park can't have its arm DEL'd). Do NOT
  drop the decorator, and do NOT "fix" this by removing the register-once skip --
  the skip is the monkey/transport throughput hot path (removing it cost ~10% on
  the echo bench; `release_if_idle` leaves it untouched). Regression guard:
  `tests/test_aio_fd_reuse.py`.

- **Future done-callbacks defer through call_soon, in asyncio order.**
  `Future.__schedule_callbacks` in asyncio defers EVERY done-callback via
  `loop.call_soon` -- a waiting Task's `__wakeup` AND library/user done-callbacks
  (gather's `_done_callback`, aiojobs' job `_done_callback`, ...). So a setter
  that completes a future and KEEPS RUNNING, or a task whose own done-callback
  mutates shared state, is observed in asyncio order: the waiter scheduled first
  (by an earlier `set_result`) resumes BEFORE the future's later done-callbacks.
  `_fire_callbacks` must therefore defer every callback EXCEPT
  `RunloomTask._wake_unpark` (runloom's own await-wake primitive -- deferring it would
  spawn a goroutine per await and break park/unpark; it only readies the g, which
  is FIFO-after an already-readied waiter, so ordering still holds) and callbacks
  tagged `_runloom_fire_sync` (the run loop's `_stop_on_done`, which must stop the
  drive in the same turn). Stock C-Task/`_PyTask` wakeups keep the
  `_run_stock_task_cb` trampoline (re-entry-unsafe). Firing library callbacks
  synchronously inverted the order and broke the falcon/uvicorn websocket-close
  ordering (a close frame's done-callback ran before the recv waiter) and
  aiojobs' pending-job promotion. Regression guard:
  `runloom_compat/ws_close_order_repro.py` (frame_sent True == stock).
  NOTE: this matches asyncio's *callback* order, not its inline-task-step: a
  woken RunloomTask is readied (a goroutine), not run-to-next-await inline inside
  its wakeup, so a done-callback racing the woken task's NEXT step is still M:N
  (aiojobs `test_job_close_exception`).

- **The driver resumes a coroutine with `coro.send(None)`, never
  `coro.send(future.result())`.** asyncio's `Task.__step` *always* sends `None`;
  a `Future`'s `__await__` retrieves its own value (`return self.result()`) and
  ignores whatever was sent in, so injecting the result is redundant for Futures.
  It is also WRONG: a custom awaitable-iterator that propagates the sent value
  through the iterator protocol (a C `__anext__`/`__await__`→self object with
  `__next__` but no `send`, delegating to an executor future -- e.g. aiocsv's
  `_Parser`) takes `PyIter_Send`'s `.send()` branch on a non-None resume value
  instead of its `__next__` branch, raising "object has no attribute 'send'".
  `_driver` must therefore set `send_value = None` in BOTH the fast (future
  already done) and slow (parked) wake paths; exceptions/cancels still route via
  `coro.throw`. Regression guard: `runloom_compat/aiocsv_repro.py`.

## Conventions
- Use `safe-rm`, never plain `rm`, for any file deletion.
