# Debugging & introspection

When a runloom program hangs or misbehaves, the first question is always
*which goroutines exist and what is each one waiting on?*  runloom answers it
the way Go does — a goroutine dump — plus a structured API you can call
from your own code or a watchdog.

## Quick look

```python
import runloom

gi.count()                 # how many goroutines are live
print(gi.format(stacks=True))   # a formatted dump (string) -> log it
gi.dump()                  # write that dump to stderr
```

`gi.format(stacks=True)` prints a state histogram and one block per
goroutine, with the Python stack pinpointing where in *your* code it is
parked:

```
=== runloom goroutines: 3 live ===
  running    1
  sleep      2

goroutine 1 [running]  <function main at 0x...>:
goroutine 2 [sleep, wake_in=4.98s, age=0.0s]  <function handler at 0x...>:
    sleep (runtime.py:121)
    db_query (app/db.py:42)
    handler (app/server.py:88)
goroutine 3 [io-wait, fd=12 R, age=30.1s]  <function accept_loop at 0x...>:
    ...
```

## The structured API

`runloom.inspect.goroutines()` (or `runloom.goroutines()`) returns a list of
dicts, one per live goroutine:

| key          | meaning |
|--------------|---------|
| `id`         | per-goroutine id (Go's *goid*) |
| `state`      | `running` / `runnable` / `io-wait` / `sleep` / `chan-wait` / `park` / `done` |
| `blocked_on` | coarse class: `io` / `timer` / `chan` / `sync` / `running` |
| `fd`,`events`| the fd and `R`/`W`/`RW`, when `io-wait` |
| `wake_in`    | seconds until wakeup, when `sleep` |
| `age`        | seconds in the current parked state (needs timestamps on, below) |
| `refcount`, `noyield`, `owner` | internals; `owner` groups goroutines by OS-thread scheduler |

```python
gi.goroutines(stacks=True)   # each dict also gets 'entry' (repr) + 'stack'
gi.stack(gid)                # one goroutine's stack: [(file, line, func), ...]
```

### Park age ("stuck for how long")

Off by default (it costs one clock read per park).  Turn it on to populate
`age` and spot a wedged goroutine:

```python
gi.enable_timestamps()       # or env RUNLOOM_INTROSPECT_TIME=1
```

### Leak watchdog

A goroutine parked far longer than expected is usually a leak — an orphaned
accept loop, a never-awaited task, a waiter whose waker is gone.  `leaked()`
finds them (it turns on age tracking for you):

```python
gi.leaked(min_age=60)                                  # parked > 60s
gi.leaked(min_age=300, states=("chan-wait", "park"))   # stuck on another
                                                       # goroutine for 5 min
```

Or run a periodic watchdog inside your scheduler:

```python
gi.watch_leaks(min_age=120, interval=30)   # logs anything parked > 2 min
```

A long-lived server legitimately has old `io-wait` goroutines (its accept
loops) and old `sleep` goroutines (tickers), so narrow `states` / raise
`min_age` to match what *you* consider stuck.

### When is the Python stack available?

* **Single-thread scheduler (`runloom.aio`, the common case):** the full stack
  of any parked goroutine is reconstructed.  asyncio Tasks also expose
  their own stack via the stock `Task.get_stack()`; runloom fills in the *raw*
  goroutines (channel ops, the netpoll pump, accept loops) that
  `asyncio.all_tasks()` never sees.
* **Default M:N scheduler:** a parked goroutine can be resumed by its hub at
  any instant, so its stack is withheld (there is no safe way to freeze it);
  the structural fields above still tell the story.  Run with
  `RUNLOOM_PER_G_TSTATE=1` to get full stacks under M:N (each goroutine then
  owns a thread-state that can be claimed for the walk).
* The **currently-running** goroutine has no *saved* stack — use the normal
  `traceback` / `sys._getframe` for your own frames.

## Dumping a hung process (`kill -QUIT`)

```python
gi.install_dump_signal()     # SIGQUIT -> goroutine dump on stderr
# or set env RUNLOOM_TRACEBACK=1 before import
```

This installs a **raw C** handler, so the dump fires even when the
interpreter is wedged (a Python `signal.signal` handler only runs at a
bytecode boundary, which a fully-stalled process never reaches).  On
**Windows** there is no SIGQUIT, so the trigger is **Ctrl+Break**
(`CTRL_BREAK_EVENT`, via a console control handler) — the same dump, the same
"keep running afterwards" behaviour.  Then, on POSIX:

```
kill -QUIT <pid>
```

writes a structural dump (state histogram + per-goroutine line, no Python
stacks — touching Python objects from a signal handler is not safe) to
stderr and lets the process continue.  The underlying primitive is
`runloom.dump_goroutines(fd)`, which is async-signal-safe-ish (it
try-locks the registry and uses only `write(2)`).

## Deadlock detection

Go reports `fatal error: all goroutines are asleep - deadlock!` when the
scheduler runs out of runnable work but goroutines are still blocked on each
other.  runloom does the same: if the single-thread scheduler quiesces — nothing
runnable, no timers, no I/O, no offload in flight — while goroutines are still
parked on a channel or a `park`, those goroutines can never be woken, so it
reports the deadlock with a goroutine dump:

```
runloom: DEADLOCK -- the scheduler ran out of work with 2 goroutine(s) still
blocked on a channel/park and no way to wake them:

=== runloom goroutines: 2 live ===
  chan-wait  2
goroutine 1 [chan-wait] ...
goroutine 2 [chan-wait] ...
```

Three modes (default **warn**):

```python
import runloom
gi.set_deadlock_mode("warn")    # print the dump, keep going (default)
gi.set_deadlock_mode("raise")   # raise RuntimeError out of run()
gi.set_deadlock_mode("off")     # do nothing
```

Also via env `RUNLOOM_DEADLOCK=off|warn|raise`.  This applies to the
single-thread scheduler (which `runloom.aio` uses).  A clean `runloom.aio` shutdown
goes through `sched_stop`, which is **excluded**, so a normal loop teardown
with pending background tasks never trips the detector — only a genuine
"everyone is blocked, nothing can make progress" quiescence does.

## Bounding goroutines (backpressure)

Goroutines are cheap, but `go()` is unbounded — a runaway spawn loop (a
fan-out with no limit, an accept loop that spawns per connection under a
flood) can still exhaust memory.  An optional admission gate caps the number
of live goroutines:

```python
import runloom
gi.set_max_goroutines(100_000)   # 0 = unlimited (default); env RUNLOOM_MAX_GOROUTINES
```

Over the cap, `runloom.go` / the spawn raises `RuntimeError`, so the caller can
apply backpressure — retry after a yield, shed the request, or block the
producer:

```python
while True:
    try:
        runloom.go(handle, conn)
        break
    except RuntimeError:
        runloom.yield_()            # let some finish, then retry
```

`gi.live_goroutines()` reports the current count under the cap.  The gate has
**zero hot-path cost** when no cap is set (the live counter is only touched
while a limit is active).

## Cost

The registry that powers all of this has **no hot-path cost**: a goroutine
is registered only when its struct is first allocated from the OS and
unlinked only when returned, so the common slab-recycled spawn/complete
path never touches the registry.  `goroutines()` and `dump()` take a brief
lock to snapshot; call them from a watchdog as often as you like.

## Fork safety

After `os.fork()` the child keeps only the forking thread — the M:N hub
threads and the blocking-offload workers are gone.  runloom installs an
`os.register_at_fork(after_in_child=...)` handler that resets the runtime in
the child, so:

* A child that runs the **single-thread scheduler / `runloom.aio`** works — this
  is the `multiprocessing` (fork) and pre-fork-server pattern.  The child
  gets its own netpoll fd and a clean scheduler.
* A child that starts a **fresh `runloom.mn_init()`** works when the parent
  never used M:N.
* `runloom.mn_run()` / `runloom.run()` in the child **return** instead of
  hanging forever on the parent's dead hubs.

**Not supported:** re-initialising the M:N scheduler *inside* a fork-child of
an already-active M:N parent.  For `multiprocessing`, prefer the
`forkserver` or `spawn` start methods (which don't inherit a live runtime),
or keep the child single-threaded.
