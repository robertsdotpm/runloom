# 12 — The public API and the Go-style facades

Ground truth: `runloom/__init__.py`, `runloom/runtime.py`, `runloom/sync.py`,
`runloom/time.py`, `runloom/context.py`, `docs/sync-api.md`,
`docs/quickstart.md`.

## The shape

`import runloom` is meant to be the only import you need. The package eagerly
imports its feature subpackages (importing has no side effects — `monkey` only
patches when you call `patch()`), re-exports the C primitives, and provides a few
thin wrappers. The C extension stays importable as `runloom_c` for advanced use.

## The everyday API (`runtime.py`)

Five functions carry almost all usage:

- **`go(callable, *args, stack_size=0, **kwargs)`** — spawn a fiber. The
  dispatch is the interesting part: it reads `runloom_c.mn_hub_count()` (not a mode
  flag) so the *same* `go()` works from anywhere — inside a hub fiber or from
  the main thread while hubs run. If hubs exist (`mn > 0`) it routes via `mn_go`
  (round-robins a non-hub caller) and returns `None` (M:N v1 is run-to-completion,
  no join handle); else it spawns on this thread's scheduler and returns a
  `Goroutine` handle. **Spawning via the plain scheduler inside a hub would skip
  the M:N pending-counter accounting that `mn_run()` joins on** — so this dispatch
  is required for correctness, not convenience.
  - `stack_size=` is *runloom's* keyword (the per-fiber C stack), popped before
    binding args so it pins the stack instead of being forwarded to `fn`.
  - Arg-bearing `go(fn, x)` wraps `fn` in a lambda but sets `lambda.__wrapped__ =
    fn` so the auto-sizer/prescan key on the *real* function, not the wrapper
    (spec 10).
  - Under M:N with no explicit size, it applies the **function-bound grow-down**
    (spec 10) unless the opt-in C auto-sizer is on (the user-chosen sizer wins).
- **`run(n, main_fn=None)`** — THE entry point. `n` is required and explicit
  because M:N is a *different correctness model* (fibers run Python in
  parallel, so shared state can race) — you opt in by typing the number, never by
  accident. `n=1` is single-thread (cooperative, GIL-OK); `n>1` is M:N and
  **raises on a GIL build** rather than silently running serial. It collapses the
  raw `mn_init`/`mn_go`/`mn_run`/`mn_fini` envelope and `prewarm`s the stdlib
  first.
- **`sleep(seconds)`** — cooperative; falls back to `time.sleep` outside a
  fiber (so the same name works in either context).
- **`yield_now()`** — `runtime.Gosched()` equivalent (a scheduling point for a
  long CPU loop).
- **`blocking(fn, …)`** — offload a blocking/CPU call off the hub (spec 08); runs
  inline when not on a fiber.

### `prewarm_stdlib()` — the enabler for small stacks

`getaddrinfo`'s first call lazily imports an `encodings` codec through the import
machinery — a **deep, non-yielding C-stack burst** that copy-grow can't rescue
(it only grows at yields), so a small-stack fiber hitting it cold would
overflow. `run()` (and the aio loop) call `prewarm_stdlib()` on the **main
thread's big stack** first, caching the codec + bootstrap state process-wide so
the path a fiber later takes is shallow. This is *why* small default
fiber stacks are safe in practice (spec 10).

### Fork safety + opt-in crash/autosize at import

`__init__.py` registers `os.register_at_fork(after_in_child=reset_after_fork)` so a
forked child (which keeps only the forking thread — the hub + offload threads are
gone) resets the C runtime instead of hanging on dead hubs or deadlocking on an
inherited lock. It also installs the crash reporter / stack auto-sizer if the env
vars ask, before the runtime starts.

## `runloom.sync` — Go-style straight-line code (no async)

`sync.py` is the no-`async`/`await` facade: `go`, `Chan`, `select`, cooperative
`sleep`, and a `Socket` wrapper plus `tcp_connect`/`tcp_listen`/`udp_endpoint`. The
`Socket` is an ordinary non-blocking `socket.socket` whose `recv`/`accept`/`send`
catch `BlockingIOError` and park on `runloom_c.wait_fd` — the simplest possible
cooperative socket, readable as a worked example of "how to make any fd-based API
cooperative." It exists so a library can ship an API that doesn't force `async
def` on its callers (e.g. aionetiface). `Lock`/`Event`/`Semaphore`/`Condition` are
re-exported from `monkey` (the cooperative primitives, spec 14). `park` =
`runloom_c.park_self`, exposed for library authors building their own primitives on
the park/wake core (spec 04).

## `runloom.time` — Go's `time` package subset

`time.py`: `After(d)`, `Tick(d)`, `NewTimer`/`NewTimer.Stop/Reset`,
`NewTicker`/`Stop/Reset`, `Sleep`. **All driven by a backing fiber that sleeps
then sends on a `runloom_c.Chan`** — so consumers `select` on them, exactly like
Go. The implementation pattern worth noting: a generation counter (`_gen`) makes
`Stop`/`Reset` clean — the sleeping `fire()` fiber checks `self._gen == gen` on
wake and bails if a `Reset` superseded it (no way to cancel an in-flight
`sched_sleep`, so you invalidate it instead). `try_send` on a buffer-1 channel
naturally drops backlog if the consumer is slow (matches Go's Ticker).

`_spawn` routes through `mn_go` when `mn_hub_count() > 0`, else `go` — so timers
fire under the M:N scheduler too (a recurring pattern: anything that spawns a
helper fiber must check the hub count, or it hangs under `mn_run`).

## `runloom.context` — Go's `context.Context` for cancellation

`context.py`: `Background()`, `WithCancel`, `WithDeadline`, `WithTimeout`. A
context's `done` is a **channel that closes on cancel** — producers `select` on
`ctx.done` to know when to stop. Cancellation is **transitive**: a `_CancelCtx`
holds a child list and `_cancel` fans out to all descendants (which is the whole
reason for the explicit tree vs. just passing a channel). `WithDeadline` arms a
fiber that `sched_sleep`s to the deadline then cancels (inheriting a tighter
parent deadline). Error sentinels are plain strings (`"cancelled"` /
`"deadline_exceeded"`) — deliberately matching Go's `ctx.Err()` returning
`context.Canceled`/`DeadlineExceeded`, not Python exception classes.

## Why three facades over one scheduler

`sync` / `aio` / `monkey` are *different front-ends to the same fibers* and can
be mixed in one process. The reason all three exist: they meet code where it is.
New Go-style code wants `sync` (no coloring). Existing `async def` code wants `aio`
(spec 13). Existing blocking code using `requests`/`pymysql` wants `monkey` (spec
14). None of them is a different runtime — they all bottom out in `runloom_c.go` /
`runloom_c.run` and the C scheduler.

## Invariants

1. **`go()` dispatches on `mn_hub_count()`**, not a mode flag, so it routes
   correctly from inside or outside a hub (and keeps the M:N pending-count
   accounting `mn_run` joins on).
2. **`run(n)` requires explicit `n`** and raises for `n>1` on a GIL build — M:N is
   an opt-in correctness model, never implicit.
3. **`prewarm_stdlib()` runs on the main thread before any fiber** — the
   enabler for small stacks (the deep `getaddrinfo` import burst).
4. **Any helper-fiber spawn checks the hub count** (`time`, `context`) or it
   hangs under `mn_run`.
5. **`stack_size=` is consumed by `go()`, never forwarded** to the target; an
   explicit size wins over all auto-sizing.
