# Debugging & introspection

When a runloom program hangs or misbehaves, the first question is always
*which fibers exist and what is each one waiting on?*  runloom answers it
the way Go does — a fiber dump — plus a structured API you can call
from your own code or a watchdog.

## Quick look

```python
import runloom

gi.count()                 # how many fibers are live
print(gi.format(stacks=True))   # a formatted dump (string) -> log it
gi.dump()                  # write that dump to stderr
```

`gi.format(stacks=True)` prints a state histogram and one block per
fiber, with the Python stack pinpointing where in *your* code it is
parked:

```
=== runloom fibers: 3 live ===
  running    1
  sleep      2

fiber 1 [running]  <function main at 0x...>:
fiber 2 [sleep, wake_in=4.98s, age=0.0s]  <function handler at 0x...>:
    sleep (runtime.py:121)
    db_query (app/db.py:42)
    handler (app/server.py:88)
fiber 3 [io-wait, fd=12 R, age=30.1s]  <function accept_loop at 0x...>:
    ...
```

## The structured API

`runloom.inspect.fibers()` (or `runloom.fibers()`) returns a list of
dicts, one per live fiber:

| key          | meaning |
|--------------|---------|
| `id`         | per-fiber id (Go's *goid*) |
| `state`      | `running` / `runnable` / `io-wait` / `sleep` / `chan-wait` / `park` / `done` |
| `blocked_on` | coarse class: `io` / `timer` / `chan` / `sync` / `running` |
| `fd`,`events`| the fd and `R`/`W`/`RW`, when `io-wait` |
| `wake_in`    | seconds until wakeup, when `sleep` |
| `age`        | seconds in the current parked state (needs timestamps on, below) |
| `refcount`, `noyield`, `owner` | internals; `owner` groups fibers by OS-thread scheduler |

```python
gi.fibers(stacks=True)   # each dict also gets 'entry' (repr) + 'stack'
gi.stack(gid)                # one fiber's stack: [(file, line, func), ...]
```

### Park age ("stuck for how long")

Off by default (it costs one clock read per park).  Turn it on to populate
`age` and spot a wedged fiber:

```python
gi.enable_timestamps()       # or env RUNLOOM_INTROSPECT_TIME=1
```

### Leak watchdog

A fiber parked far longer than expected is usually a leak — an orphaned
accept loop, a never-awaited task, a waiter whose waker is gone.  `leaked()`
finds them (it turns on age tracking for you):

```python
gi.leaked(min_age=60)                                  # parked > 60s
gi.leaked(min_age=300, states=("chan-wait", "park"))   # stuck on another
                                                       # fiber for 5 min
```

Or run a periodic watchdog inside your scheduler:

```python
gi.watch_leaks(min_age=120, interval=30)   # logs anything parked > 2 min
```

A long-lived server legitimately has old `io-wait` fibers (its accept
loops) and old `sleep` fibers (tickers), so narrow `states` / raise
`min_age` to match what *you* consider stuck.

### When is the Python stack available?

* **Single-thread scheduler (`runloom.aio`, the common case):** the full stack
  of any parked fiber is reconstructed.  asyncio Tasks also expose
  their own stack via the stock `Task.get_stack()`; runloom fills in the *raw*
  fibers (channel ops, the netpoll pump, accept loops) that
  `asyncio.all_tasks()` never sees.
* **Default M:N scheduler:** a parked fiber can be resumed by its hub at
  any instant, so its stack is withheld (there is no safe way to freeze it);
  the structural fields above still tell the story.  Run with
  `RUNLOOM_PER_G_TSTATE=1` to get full stacks under M:N (each fiber then
  owns a thread-state that can be claimed for the walk).
* The **currently-running** fiber has no *saved* stack — use the normal
  `traceback` / `sys._getframe` for your own frames.

## What is each hub doing? (`hubs()`)

`fibers()` is the per-fiber view; `runloom.inspect.hubs()` (or
`runloom.hubs()`) is the per-**hub** view — the M:N scheduler threads — and the
first thing to look at when the answer to "it hung" is *which* hub and *on
what*:

```python
import runloom
from runloom import inspect as gi
gi.print_hubs()          # one row per hub; wedged hubs flagged
hs = gi.hubs()           # the same data as a list of dicts
```

```
=== runloom hubs (4) ===
 id  label       running_g  dwell_ms pend  what
  0  running             1         0    1
  1  WEDGED/io        1025       150    1  cursor.execute (db.py:88)
  2  idle                -         -    0
  3  idle                -         -    0

1 hub(s) wedged.  Full C+Python stack of every thread:
    py-spy dump --pid 33187
```

Each dict has:

| field | meaning |
| --- | --- |
| `id` | dense hub index |
| `state` | `detached` (released its tstate — a blocking call **or** idle), `attached` (running Python / CPU-bound), `suspended` (a stop-the-world is in progress) |
| `running_g` | goid being resumed, or `None` when idle |
| `dwell_ms` | how long the **current resume** has run; a large value with `detached` is a hub wedged in a blocking call |
| `pending` | fibers owned + queued on this hub |
| `preempt_requested` | sysmon has asked this hub to yield (a CPU wedge) |
| `instrumented` | whether sysmon resume-tracking is live (it is by default on free-threaded 3.13t; `running_g`/`dwell_ms`/`blocked_at` need it) |
| `blocked_at` | best-effort Python call site of a **DETACHED-wedged** hub's blocking call, e.g. `cursor.execute (db.py:88)`, else `None` |
| `stack_cmd` | a ready-to-run `py-spy dump --pid <PID>` for **this** process — the always-safe, out-of-process full C+Python stack of every thread |

**`blocked_at` is best-effort.** It is read from another hub's thread-state, so
it only fills for a hub that is *stably* DETACHED (a fiber parked in a
blocking syscall — the owner thread won't touch its frames until the call
returns) and only when the handoff rescue isn't mid-adoption of that hub. For an
**ATTACHED** (CPU) wedge, or when the read can't be taken safely, it is `None` —
fall back to `stack_cmd`. `py-spy` reads the process out-of-process, so it
always works and gives the **complete** C + Python stack of *every* thread (the
one truly-stuck thread included); use it whenever `blocked_at` is `None` or you
want the full picture. (`dwell_ms` reflects the *current* resume — a hub rapidly
cycling through short resumes shows a small dwell even when busy; a genuinely
stuck hub's dwell climbs.)

Returns `[]` outside an M:N run (`n=1` / before `run()`). All fields are
lock-free atomic reads, so `hubs()` is cheap enough to poll from a watchdog.

## Dumping a hung process (`kill -QUIT`)

```python
gi.install_dump_signal()     # SIGQUIT -> fiber dump on stderr
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

writes a structural dump (state histogram + per-fiber line, no Python
stacks — touching Python objects from a signal handler is not safe) to
stderr and lets the process continue.  The underlying primitive is
`runloom.dump_fibers(fd)`, which is async-signal-safe-ish (it
try-locks the registry and uses only `write(2)`).

## Crash reporting (`SIGSEGV` / `SIGBUS`)

A fiber runs on a small, fixed C stack with a `PROT_NONE` **guard page**
just below it, so the commonest hard crash in runloom is a **fiber stack
overflow** — deep C recursion (a big `repr`, an OpenSSL/regex/JSON call, a
recursive protocol callback) running off the low end of that stack and into the
guard page.  By default that is a bare `Segmentation fault` with no clue which
fiber or why.

The crash reporter turns it into a classified dump:

```python
gi.install_crash_handler()       # or "all" / "wait" / "gdb" / ...
# or set env RUNLOOM_CRASH=on (auto-installs at import — every crash dumps)
```

On a fault it maps the faulting address onto the guard pages and prints, e.g.:

```
======================== runloom crash ========================
[runloom] fatal SIGSEGV at address 0x7622eca18f30  (pid 48681, thread 0x7622ebbff6c0)
[runloom] >>> GOROUTINE STACK OVERFLOW <<<
[runloom]     fiber g1 ran off the low end of its 128 KiB C stack
[runloom]     (the fault hit the guard page just below it).
[runloom]     Fix: give it a bigger stack -- runloom_c.go(fn, stack_size=N), ...
[runloom] this thread was executing fiber g1.
=== runloom fiber dump: 1 live (default stack 128 KiB) ===
  ...
```

A fault **inside** a fiber stack is reported as a likely wild pointer / UAF
on that fiber; anything else (main/hub stack, heap, a stray pointer) is
flagged as a non-fiber fault.  After the dump it **chains to the previous
handler** so a core dump / correct exit code still follow.

`level` (or the `RUNLOOM_CRASH` env value) selects behaviour, comma-separated:

| level        | effect                                                           |
|--------------|------------------------------------------------------------------|
| `on`         | classified fiber dump (the default)                          |
| `all`        | `+ backtrace + pystack`                                          |
| `backtrace`  | add a native C backtrace (`execinfo`)                            |
| `pystack`    | add the Python traceback (enables `faulthandler` and chains to it) |
| `wait`       | after the dump, **block for a debugger** — prints `gdb -p <pid>`; resume with `kill -CONT <pid>` |
| `gdb`        | fork+exec `gdb -batch -ex 'thread apply all bt full'` on self    |
| `off`        | uninstall                                                        |

`RUNLOOM_CRASH_FILE` (or `install_crash_handler(file=...)`) appends the report
to a file as well as stderr.  Call `install_crash_handler()` **before** starting
the runtime so the scheduler hubs are armed as they spawn.

It survives the very overflow it reports because every runloom OS thread (the
main thread, each scheduler hub, the blocking-offload workers) installs its own
`sigaltstack`, so the handler runs on a separate stack when the fiber stack
is exhausted.  Off by default — it does not hijack process-wide signal handlers
unless asked.  **Windows** uses a Vectored Exception Handler that dumps the
fiber registry and continues the search (the rich path is POSIX).

## Deadlock detection

Go reports `fatal error: all fibers are asleep - deadlock!` when the
scheduler runs out of runnable work but fibers are still blocked on each
other.  runloom does the same: if the single-thread scheduler quiesces — nothing
runnable, no timers, no I/O, no offload in flight — while fibers are still
parked on a channel or a `park`, those fibers can never be woken, so it
reports the deadlock with a fiber dump:

```
runloom: DEADLOCK -- the scheduler ran out of work with 2 fiber(s) still
blocked on a channel/park and no way to wake them:

=== runloom fibers: 2 live ===
  chan-wait  2
fiber 1 [chan-wait] ...
fiber 2 [chan-wait] ...
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

## Bounding fibers (backpressure)

Goroutines are cheap, but `go()` is unbounded — a runaway spawn loop (a
fan-out with no limit, an accept loop that spawns per connection under a
flood) can still exhaust memory.  An optional admission gate caps the number
of live fibers:

```python
import runloom
gi.set_max_fibers(100_000)   # 0 = unlimited (default); env RUNLOOM_MAX_GOROUTINES
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
        runloom.yield_now()         # let some finish, then retry
```

`gi.live_fibers()` reports the current count under the cap.  The gate has
**zero hot-path cost** when no cap is set (the live counter is only touched
while a limit is active).

## Cost

The registry that powers all of this has **no hot-path cost**: a fiber
is registered only when its struct is first allocated from the OS and
unlinked only when returned, so the common slab-recycled spawn/complete
path never touches the registry.  `fibers()` and `dump()` take a brief
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
* `runloom.mn_run()` / `runloom.run(1)` in the child **return** instead of
  hanging forever on the parent's dead hubs.

**Not supported:** re-initialising the M:N scheduler *inside* a fork-child of
an already-active M:N parent.  For `multiprocessing`, prefer the
`forkserver` or `spawn` start methods (which don't inherit a live runtime),
or keep the child single-threaded.
