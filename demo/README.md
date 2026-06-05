# mnweb — a micro web stack on runloom's M:N sync API

A self-contained demo that exercises [runloom](../README.md)'s **M:N
synchronous** API (Go-style stackful goroutines across N hub threads, GIL
off, free-threaded CPython 3.13t) under a realistic, long-running web
workload — plus a supervisor that detects crashes/hangs, gathers cores and
gdb backtraces, and restarts the stack automatically.

Everything is written in the *blocking* style: no `async`/`await`, no event
loop ceremony. Concurrency comes from `runloom_c.mn_go` + cooperative I/O
(`runloom_c.wait_fd`), channels (`runloom_c.Chan`), and a cooperative lock
(`runloom.sync.Lock`).

## Pieces

| file | what it is |
| --- | --- |
| [mnweb.py](mnweb.py) | the micro HTTP framework: routing, request parsing, a cooperative `CoSock` built on `wait_fd` (with read timeouts), keep-alive, and `dial`/`fetch` for outbound requests. Serves with `mn_init` → accept-loop goroutine → one `mn_go` handler per connection → `mn_run`. |
| [site.py](site.py) | the website: `/` (host IP + visitor count + uptime), `/ip`, `/count` (lock-guarded counter), `POST /visit`, `/health` (cheap, for the watchdog), `/slow` (timer path), `/stats` (scheduler + per-hub introspection). Every request is logged to a file **and** written to a local sqlite DB by a single dedicated writer goroutine (blocking writes, batched commits). Fetches `example.com` once at startup and **every 10 minutes**. Arms the crash + `kill -QUIT` goroutine-dump handlers and a `health.json` heartbeat. |
| [burst_client.py](burst_client.py) | load client on the **same** M:N lib: every 60 s it fires 100 concurrent requests (one goroutine each), collects results over a channel, logs a latency/status summary, repeats forever. |
| [supervisor.sh](supervisor.sh) | the watchdog (see below). |
| [gdb_dump.sh](gdb_dump.sh) / [attach.sh](attach.sh) | extract C+Python backtraces from a core or a live pid / attach an interactive gdb (with CPython's `py-bt` helpers loaded). |
| [stop.sh](stop.sh) | stop the supervisor and its children. |

## Run it

```bash
# free-threaded interpreter, GIL off
./supervisor.sh                 # server :8080 + client, forever
./stop.sh                       # stop everything
tail -f run/server.log run/client.log run/supervisor.log
cat run/status.txt              # one-glance health
```

Knobs are env vars: `SERVER_PORT`, `SERVER_HUBS`, `CLIENT_BURST`,
`CLIENT_INTERVAL`, `HEALTH_INTERVAL`, `HANG_STRIKES`, …

## Crash / hang detection

The server and client both arm runloom's in-process diagnostics:

- `install_crash_handler("goroutine,backtrace", …)` — on a fatal signal,
  dumps the goroutine registry + a native backtrace to `run/crash_report.txt`
  **and** chains to `SIG_DFL` so a core is written.
- `install_traceback_signal()` — `kill -QUIT <pid>` dumps every goroutine
  even when the interpreter is wedged.
- a `health.json` heartbeat goroutine — a stale file signals a wedge.

The supervisor polls `/health` (+ heartbeat freshness) and the client log:

- **crash** (child exits on a fatal signal) → finds the core, runs
  `gdb_dump.sh core` for C + `py-bt` backtraces → writes an incident →
  restarts.
- **hang/wedge** (sustained `/health` failure, even if the process is still
  alive with a stranded hub) → `kill -QUIT` for a goroutine dump + a live
  `gdb` snapshot (all-thread `bt` + `py-bt`) + the last `mn_hub_states()` →
  writes an incident → bounces the process.

Incidents land in `run/incidents/INCIDENT-*.md`; `run/NEW_INCIDENT` is
touched so a watcher can notice.

Core dumps: `kernel.core_pattern` → `run/cores/` and `kernel.yama.ptrace_scope`
→ 0 are set at supervisor start (needs sudo); cores are rotated (keep newest
`KEEP_CORES`, default 3).

`DEMO_ALLOW_CRASH=1` enables fault-injection routes (`/debug/segv`,
`/debug/crash`, `/debug/wedge`) used to validate the pipeline; off by default.

## Bugs found + fixed in runloom while building this

Building this surfaced two real defects in runloom's **fatal-signal crash
handler under the M:N runtime** — both made a genuine fault *wedge* the
process (a stranded hub, service dead, **no core**) instead of coring and
dying cleanly. See [BUGS_FOUND.md](BUGS_FOUND.md). Both are fixed in this
branch and validated (6/6 faults now core+die; `test_crash_handler`,
`test_mn`, `test_sysmon_oracle`, `test_sched_fairness` all green):

1. **`runloom_crash_install` was not idempotent** (`runloom_crash.c`). When
   the handler is installed twice — runloom's package `__init__` auto-installs
   from `$RUNLOOM_CRASH`, then app code calls `install_crash_handler()` to set
   a level/file — the second install saved *its own* `crash_handler` as the
   "previous" disposition. On a real fault the chain-out then restored
   `crash_handler` and re-faulted straight into its own re-entrancy `pause()`
   guard → permanent wedge. Fix: a re-install preserves the dispositions
   captured by the first install.

2. **The handoff rescue pool adopted a crash-dumping hub** (`mn_sched*`,
   `runloom_crash.c`). While the crash owner dumps, sysmon could flag its hub
   "wedged" and the handoff pool steal the faulting goroutine away before the
   chain-out re-faulted. Fix: the crash handler calls
   `runloom_sched_freeze_for_crash()` on owner-entry to halt the sysmon +
   handoff watchdogs (async-signal-safe atomic stores).
