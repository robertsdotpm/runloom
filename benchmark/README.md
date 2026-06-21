# benchmark/

- **Throughput / speed / memory suite** → [`suite/`](suite/README.md); consolidated
  results in [`report.html`](report.html). Compares runloom vs Go/asyncio/uvloop/
  gevent/greenlet on req/s, bandwidth, spawn, context-switch and RTT. **Memory
  (RSS/fiber, 1M fibers) covers only the two stackful runtimes, runloom and Go** —
  stackless asyncio/uvloop/greenlet are not in the memory matrix.
- **Backend syscall profiles** (this file, below) — per-OS syscall traces the
  report links to.

---

# big_100 syscall profiles, by netpoll backend

Per-syscall profiles of the `big_100` stress programs on the latest `origin/main`,
captured on three platforms / netpoll backends, each with that OS's native tracer.
Same workload everywhere (`--hubs 2 --funcs 150 --seed 1234 --duration 3`; Windows
used `--funcs 60 --duration 2` to keep the system-wide trace volume manageable).

## Reports (open in a browser)
- **big100_syscall_backends.html** — cross-backend comparison (epoll vs kqueue vs
  iocp-afd); syscalls bucketed into shared categories so the backends line up.
- **big100_syscall_profile_linux.html** — Linux / epoll, per-program detail.
- **big100_syscall_profile_mac.html** — macOS / kqueue, per-program detail.
- **big100_syscall_profile_win.html** — Windows / iocp-afd, per-program detail.

Regenerate from the committed data dirs:

    python gen_syscall_backends_html.py     # the comparison
    python gen_syscall_profile.py           # the 3 per-platform profiles

## How each was captured
- **Linux (epoll)** — `strace -f -c` (per-process) → `lin_sys/*.strace`.
- **macOS (kqueue)** — `ktrace` / KDEBUG (per-process) → `mac_sys/*.counts`
  (`BSC_`/`MSC_` names). dtrace's `syscall` provider is SIP-blocked; KDEBUG is not.
- **Windows (iocp-afd)** — `xperf` NT-kernel `SYSCALL` flag + MS symbols, aggregated
  by resolved `NtXxx` name → `win_sys/*.counts`. Windows syscall ETW events carry no
  pid, so this is **system-wide**: captured at a scale where python dominates, with an
  idle `BASELINE.counts` subtracted and the tracer's own `NtTraceControl` excluded.

## Caveat
Each tracer perturbs timing differently, and Windows is system-wide-minus-baseline,
so compare the **mix** across backends — not raw cross-OS magnitudes.

## Headline
The three backends idle/wake completely differently, visible in the syscalls:
- **epoll** spins on `sched_yield`;
- **kqueue** polls `kevent` (+ pipe `write` wakes, `psynch` locks);
- **iocp-afd** sleeps on `NtDelayExecution`, does sockets via `NtDeviceIoControlFile`
  (AFD), and wakes cross-hub via `NtSetIoCompletion` / completion-port waits.

io_uring is confirmed gated **off** on Linux (that gate landed this session).
