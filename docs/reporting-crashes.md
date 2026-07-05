# Reporting a crash or hang

runloom writes a **self-contained diagnostic artifact** on a fatal fault, and
(optionally) on a self-detected hang. Pasting that artifact into an issue is
usually enough to locate the problem **without a reproduction** — which is the
whole point: field failures are rare and hard to reproduce, so the report has
to carry everything.

## Turn it on

```python
import runloom
# writes the report to ./runloom_crash.txt (append) as well as stderr:
runloom.install_crash_handler("goroutines,backtrace", file="runloom_crash.txt")
```

or via environment (no code change):

```sh
RUNLOOM_CRASH=goroutines,backtrace RUNLOOM_CRASH_FILE=runloom_crash.txt \
RUNLOOM_WATCHDOG=60 \
python your_server.py
```

- `RUNLOOM_CRASH` — what to dump: `goroutines`, `backtrace`, `gdb`, `wait` (or
  `all`, `off`).
- `RUNLOOM_CRASH_FILE` — a file to append the report to (also always on stderr).
- `RUNLOOM_WATCHDOG=<secs>` — arm the self-hang watchdog (see below).

## What the artifact contains

```
======================== runloom crash ========================
[runloom] fatal SIGSEGV at address (nil)  (pid ..., thread ...)
[runloom] --- build + runtime snapshot ---
[runloom]   version <v>  built <date>
[runloom]   backends: coro=fcontext-asm netpoll=epoll   [ASan]/[assert] if built so
[runloom]   gs: total=.. pending=.. completed=.. hubs=..
[runloom]   stacks: live=.. depot=..
[runloom]   netpoll: parked=.. heap=.. fd_armed=.. heals=..
[runloom]   inflight: blockpool=.. iouring=..
[runloom] <guard-page classification: stack overflow / in-stack / heap>
=== runloom fiber dump: N live ===        <- every live fiber + its state
[runloom] native backtrace (faulting thread)
<flight recorder tail: the recent scheduler transitions that led here>
```

The **build + runtime snapshot** is the R0 gauge surface (`runloom.stats()`)
captured async-signal-safely — the same numbers that show a leak as a rising
counter, frozen at the instant of the fault. `pending` / `completed` /
`parked` together say whether the runtime was busy, idle, or wedged; `heals`
> 0 means the app leaked sockets to the GC; a huge `fd_armed` or `parked` points
at a registration/parker leak.

## The self-hang watchdog (`RUNLOOM_WATCHDOG=secs`)

A silent hang — the server "just stops responding" — is the hardest field
failure to diagnose. The watchdog is a detached native thread that emits the
**same artifact** (snapshot + fiber dump + flight recorder) — **without
aborting the process** — when no fiber has completed for `secs` seconds *while
work is still outstanding* (a deadlock, a lost wake, or a hub frozen off the
scheduler). It re-arms once progress resumes, so a persistent wedge produces
one report per episode, not a flood.

**Scope.** The progress signal is fiber *completion*, so the watchdog fits a
continuously-active service (the soak/canary workloads it was built for). A
service whose fibers are long-lived by design (a pure keepalive server that
rarely completes a fiber) can look stalled while perfectly healthy — set
`RUNLOOM_WATCHDOG` generously, or leave it off, for that shape.

## What it does NOT contain

No request payloads, no user data, no environment variables, no memory
contents beyond the fault address and stack classification. It is a snapshot of
runloom's own scheduler state and the faulting backtrace — safe to paste into a
public issue. (If you built with `gdb` in `RUNLOOM_CRASH`, the optional gdb dump
may include more; omit it for a public report.)

## Filing

Attach the whole artifact (from the `====` banner to the end). If you have the
`.so` build flags (the snapshot's `[ASan]`/`[assert]` markers and the version
line say), include them. A crash file plus "what the workload was doing" is
almost always enough.
