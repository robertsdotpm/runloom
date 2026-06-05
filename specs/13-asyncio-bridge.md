# 13 — The asyncio bridge (`runloom.aio`)

Ground truth: `runloom/aio/` — `_base.py` (foundation), `tasks.py`
(`RunloomTask`), `futures.py`, `loop*.py` (the event loop mixins),
`transport_*.py`, `streams*.py`, `tls_*.py`, `subprocess.py`, `pipes.py`,
`handles.py`; `docs/asyncio.md`; and the `aio bridge invariants` block in
`CLAUDE.md` (the authoritative list — read it alongside this).

## The idea in one line

> Implement asyncio's event-loop protocol on top of goroutines: **each
> `asyncio.Task` becomes a runloom goroutine that drives `coro.send()` itself, and
> `await fut` parks that goroutine** on the park/wake primitive (spec 04). Existing
> `async def` code runs unchanged on runloom's scheduler.

`runloom.aio.run(coro)` ≈ `asyncio.run`. The win is amortizing setup across many
`await`s (multi-await pipelines run ~1.7–1.9× faster); the loss is per-task setup
(one-await fan-outs run ~5× slower — stick with asyncio there). It is a
**high-fidelity bridge, not a bit-exact emulator** — and the places it differs are
documented precisely (below), because they are *design choices*, not bugs.

> **How this spec's invariants were found:** the bridge was **derived
> empirically** — by running the test suites of 222 name-brand asyncio projects
> (plus CPython's own `test_asyncio`) under `RunloomEventLoop` and fixing every
> behavioral failure. The "compatibility-invariant catalog" below *is* that
> forensic record (each entry names the project whose test it fixed). **Read
> [spec 17](17-asyncio-bridge-derivation.md) for the method** — a re-implementer
> who builds from this spec alone will get a bridge that looks right and breaks
> aiohttp; the corpus + the conformance harness are part of the spec.

## `RunloomTask` — the heart (`tasks.py`)

A real `asyncio.Task` subclass (so `isinstance(x, asyncio.Task)` holds) but driven
by a goroutine instead of CPython's C task machinery. The critical construction
move: **initialize only the `Future` half** (`asyncio.Future.__init__`), NOT
`Task.__init__` — which would schedule the C task-step and *double-drive* the
coroutine (the C step is a C callable you can't shadow from Python). The C Task's
fields stay NULL; runloom keeps its state in `_pg*` attrs and overrides the readers
(`get_coro`, `_must_cancel`, `_fut_waiter`, …). On completion it settles the
underlying C Future so `Task.__del__` doesn't warn "destroyed but pending."

### The driver loop (the model to internalize)

```
_driver():
  self._self_g = current goroutine          # so cancel/done-cb can wake us
  loop:
    set _CURRENT_TASKS[loop] = self          # current_task() correctness
    try:
      yielded = context.run(coro.send, None) # or coro.throw(exc) on cancel/error
    except StopIteration:    set_result; settle; return
    except CancelledError:   future-cancel; settle; return
    except BaseException:    set_exception; settle; return
    finally: restore _CURRENT_TASKS

    if yielded is None:               # bare yield (sleep(0))
        netpoll_poll(); sched_yield(); continue
    assert yielded._asyncio_future_blocking is True   # an awaited Future
    if yielded.done():                # fast path: resolve without parking
        classify(cancelled/exception/result -> send None or throw); continue
    # slow path:
    yielded.add_done_callback(self._wake_unpark)
    self._pgfutwaiter = yielded
    netpoll_poll()                    # deliver ready I/O before parking
    park_self()                       # spec 04: park; the done-cb wakes us
    classify and continue
```

Several lines here are each a fixed compatibility bug; the spec keeps them because
a re-implementer *will* hit the same ones:

- **`coro.send(None)`, never `coro.send(future.result())`.** asyncio's `Task.__step`
  always sends `None`; a `Future.__await__` retrieves its own value and ignores the
  sent value. Injecting the result is redundant for Futures and *breaks* a custom
  C awaitable-iterator that propagates the sent value (e.g. aiocsv's `_Parser`,
  which delegates to an executor future) — a non-None send routes `PyIter_Send` to
  its `.send()` branch instead of `__next__`. Set `send_value = None` in **both**
  the fast and slow wake paths; only exceptions/cancels route via `coro.throw`.
- **`netpoll_poll()` before yield and before park.** Stock asyncio's `sleep(0)` and
  every loop iteration run *one selector poll* that delivers pending socket I/O.
  runloom only pumps netpoll when its ready ring drains to empty (and the keepalive
  keeps it from going idle), so without an explicit non-blocking poll here a
  `sleep(0)` loop or an `await` that parks could leave a *peer's* recv loop starved
  (a server never seeing a client's close frame). So the driver pumps ready I/O
  before round-tripping.
- **`_CURRENT_TASKS` management** — set the per-loop "current task" slot around each
  send/throw (so `asyncio.current_task()`/`timeout`/`wait_for` work). And the
  *socket-wait* version of this (`_wait_fd` in `_base.py`) snapshots and restores
  the slot across a C `wait_fd` park, because a coroutine that parks for socket I/O
  suspends the goroutine *mid-send* — its driver's `finally` can't run until the
  send returns, and meanwhile other tasks' drivers mutate the shared slot.

### Cancellation — into the awaited future, with a one-shot fallback

`cancel()` is intricate because asyncio cancellation has subtle ordering. If the
task is suspended on a future/task, propagate the cancel **into** `_pgfutwaiter`
(mirrors stock asyncio cancelling `_fut_waiter`) so the inner awaitable runs its
*own* cleanup (`async with __aexit__`/`finally`) before our `CancelledError`
surfaces. If it can't take the cancel (already cancelling/done), set a one-shot
`_pgmustcancel` and let its existing done-callback wake us — do **not** wake now (a
premature unpark would abandon the wait and leak the future half-cancelled). If the
goroutine is parked in a C `wait_fd` (a `sock_recv`/`accept`/`connect` with no coro
await-point to throw into), use **`cancel_wait_fd()`** (spec 06) — `G.wake()` only
wakes `park_self` parkers, so it would hang. The cancel **message** is preserved
(`_pgcancelmsg`) because anyio's cancel scopes recognize their own cancellation
solely by `exc.args[0]`. `cancelling()`/`uncancel()` track the counter
`asyncio.timeout`/`TaskGroup` need.

### Roomy stacks for user-callback goroutines

Task drivers and the transport I/O goroutines run **arbitrary user code that can
recurse deep into C** (a TLS/SSH handshake runs an OpenSSL key exchange
synchronously inside `data_received`; first-time imports of pydantic etc. are deep
C-recursive), which overflows a *small* g-stack → SEGV (stock asyncio runs
callbacks on the 8 MB main stack). So every such goroutine is spawned via `_go_io`
/ with `_TASK_STACK` — an **explicit 512 KB pin** ([aio/_base.py:50-86](../src/runloom/aio/_base.py#L50)),
virtual + pooled, only the bridge pays it; `RUNLOOM_AIO_{IO,TASK}_STACK`. The pin
matters because it holds the 512 KB *regardless of the scheduler default,
calibration, or the M:N grow-down* (which can shrink an unpinned goroutine toward
16 KB) — an explicit `stack_size=` always wins. **Do not revert these to a bare
`runloom_c.go`** — it's a documented invariant. (The code comments here say "the
default 32 KB g-stack"; that reflects the pre-512 KB default era — see spec 10 for
the current numbers. The "(M:N paths keep 128 KB)" note in `CLAUDE.md` is likewise
stale: M:N's unpinned default is the 512 KB calibrated `h->sched.stack_size`,
[mn_sched_init_fini.c.inc:385](../src/runloom_c/mn_sched_init_fini.c.inc#L385).)

## The loop (`loop*.py`, composed mixins)

`RunloomEventLoop` is built from `loop_core`/`loop_schedule`/`loop_io`/`loop_net`/
`loop_subprocess`/`loop_signals`/`loop_run` mixins. `runloom_c.run()` drains the
**calling thread's own** scheduler, so each loop runs on its thread, fully
independent of loops on other threads (real per-thread concurrency, like stock
asyncio) — no single-driver election, no global bootstrap. The one cross-thread
rule: a foreign-thread `go()` would land on *its* thread's sched (never drained by
this loop), so a foreign-thread spawn is marshalled onto the loop's thread via
`call_soon_threadsafe` (a lock-guarded queue drained by the loop's keepalive).

Supported surface (from `docs/asyncio.md`): streams (`open_connection`/
`start_server`), transports+protocols, UDP datagram endpoints, SSL client **and**
server (cooperative `SSLSocket`, ALPN, cert fingerprint), `add_reader`/`add_writer`,
`run_in_executor`, subprocesses. Validated by running aiohttp, uvicorn, starlette,
hypercorn, websockets, anyio test suites green under the bridge.

## The documented semantic differences (design, not bugs)

Because a task is a stackful goroutine ordered by runloom's scheduler — not a
callback on one FIFO ready-queue driven by `loop._run_once` — a thin slice of code
that pins on asyncio's *exact scheduler mechanics* can observe a difference. They
bite **loudly** (a failing test or a hang), never as silent corruption:

1. **Done-callbacks defer through `call_soon` to match asyncio order — with two
   synchronous exceptions.** *(Corrected against the code:
   [futures.py:228-296](../src/runloom/aio/futures.py#L228). The `docs/asyncio.md`
   claim that runloom "fires most callbacks inline" is **stale** — it describes a
   pre-change behavior. The bridge was changed to defer, precisely to fix the
   ordering bugs below; the live code matches asyncio more closely than that doc
   admits, and matches the `CLAUDE.md` invariant.)* `_fire_callbacks` (the
   PENDING→done transition) **defers every callback via `loop.call_soon`** *except*:
   (a) `RunloomTask._wake_unpark` — runloom's own await-wake primitive, fired inline
   because it only readies the g (FIFO-after an already-readied waiter, so ordering
   still holds) and deferring it would spawn a goroutine per await; and (b)
   callbacks tagged `_runloom_fire_sync` (the run loop's `_stop_on_done`, which must
   stop the drive in the same turn). **Stock `asyncio.Task`/`_PyTask` wakeups are
   deferred** through the `_run_stock_task_cb` trampoline (firing them inline
   re-enters the C task machinery unsafely). Separately, `add_done_callback` on an
   *already-done* future always routes through `call_soon`, never inline
   ([futures.py:195-220](../src/runloom/aio/futures.py#L195)). Firing library
   callbacks synchronously is exactly what *broke* falcon/uvicorn websocket-close
   ordering and aiojobs — which is why the default is now deferral. (So the residual
   "difference" from asyncio here is narrow: only the two internal sync exceptions,
   not a wholesale inline policy.)
2. **Timers are real wall-clock, on a per-OS-thread scheduler.** `call_later`/
   `call_at`/`asyncio.sleep` are real `sched_sleep` goroutine sleeps, not a per-loop
   timer heap compared against `loop.time()`. Consequence: **mocking `loop.time()`
   to fast-forward a timer does not advance runloom's timers** (drive such tests
   with real short durations); and two loops on one OS thread share the scheduler,
   so a timer scheduled under loop A keeps counting while loop B runs. (`call_at`
   stores `when` verbatim on the handle, matching asyncio; only *firing* differs.)
3. **Wake/callback ordering is the scheduler's, not a single FIFO.** The *set* of
   work that runs is the same; the exact interleaving between a just-woken task and
   a freshly scheduled callback can differ. Only code pinned on sub-iteration
   ordering notices.

> The useful question when a library test fails under the bridge: *does it assert
> an observable behavioral guarantee, or assume an implementation mechanism?* The
> latter (mocks `loop.time()`, relies on exact `sleep` duration, runs many loops
> per thread) is usually an over-specified test; the former is a genuine fidelity
> gap worth taking seriously. This framing is in `docs/asyncio.md` and is part of
> the design's stated contract with users.

## The compatibility-invariant catalog (in `CLAUDE.md`, summarized)

The bridge accreted a set of must-hold invariants, each a fixed real-world crash —
keep them or the corresponding framework breaks: timer goroutines read the callback
**through the handle** (so a cancelled timer leaks nothing); `_StreamTransport`
seeds `_io_g = None` before `connection_made` (so a write-in-`connection_made` over
TLS doesn't drop the connection); loop-level callbacks run with **no current task
active** (`_pg_run_loop_cb`, so a deferred stock-Task wakeup doesn't hit
enter_task's "another task is running"); `server.close()` wakes its accept-loop
goroutines via `cancel_wait_fd` (no per-server leak). Each names the framework it
fixed and ships a regression guard under `runloom_compat/`.

## Invariants

1. **`RunloomTask` initializes only the Future half** — never `Task.__init__` (no
   double-drive).
2. **The driver sends `None`, throws on cancel/error**, and pumps netpoll before
   yielding/parking.
3. **User-callback goroutines get a roomy (512 KB) stack** (`_go_io`/`_TASK_STACK`).
4. **`cancel()` propagates into the awaited future first**; `cancel_wait_fd` for a
   C-parked g; the cancel message is preserved.
5. **The three documented semantic diffs are intentional**; the inline-callback
   carve-outs (stock-Task wakeups deferred, `_wake_unpark`/`_runloom_fire_sync`
   inline) are exact and load-bearing.
