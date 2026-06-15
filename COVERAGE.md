# runloom_c — C coverage

C-line coverage of the `runloom_c` extension, measured with **gcov** on a
free-threaded CPython 3.13t build, driven by the whole isolated test corpus.

> Numbers are the **coverable surface**: `covered / (executable − excluded)`.
> The excluded set (`tools/coverage_exclusions.txt`) is **only** lines a
> clean-exiting test genuinely cannot reach under this project's rules — never
> lines we simply chose not to test. Every exclusion was adversarially verified
> (see *Verification*). Reproduce with `tools/cov_measure.sh`.

## Per-file (translation unit) coverable coverage

Gate: **every C file ≥ 95%**; highest-bug files (netpoll, mn_sched, sched) → ~100%.
**Result: all 17 TUs ≥ 95%; whole extension 98.2%.**

| Translation unit | coverable | covered | %  | excl |
|------------------|----------:|--------:|---:|-----:|
| coro.c — coroutine/stack engine            | 514  | 514  | 100.0% | 24 |
| runloom_gstate.c — goroutine state         | 10   | 10   | 100.0% | 21 |
| runloom_stackadvice.c — stack autosizer    | 209  | 209  | 100.0% | 4  |
| cldeque.c — Chase-Lev work deque           | 35   | 35   | 100.0% | 6  |
| fcontext.c — context-switch trampoline     | 21   | 21   | 100.0% | 0  |
| netpoll.c — epoll default backend          | 953  | 949  | 99.6%  | 76 |
| runloom_diag.c — diagnostics/event ring    | 180  | 179  | 99.4%  | 106|
| mn_sched.c — M:N scheduler                 | 1428 | 1418 | 99.3%  | 60 |
| runloom_sched.c — single-thread scheduler  | 1083 | 1070 | 98.8%  | 14 |
| runloom_blockpool.c — blocking offload     | 83   | 82   | 98.8%  | 5  |
| chan.c — channels + select                 | 355  | 350  | 98.6%  | 6  |
| runloom_crash.c — crash handler            | 111  | 109  | 98.2%  | ~125 |
| runloom_tcp.c — TCP/conn layer             | 384  | 377  | 98.2%  | 5  |
| runloom_iframe.c — interp-frame helpers    | 35   | 34   | 97.1%  | 1  |
| io_uring.c — io_uring backend              | 918  | 891  | 97.1%  | 25 |
| runloom_introspect.c — introspection       | 308  | 298  | 96.8%  | 0  |
| module.c — Python module surface           | 1387 | 1322 | 95.3%  | 0  |
| **WHOLE EXTENSION** | **8014** | **7868** | **98.2%** | **211** |

`netpoll_iocp.c` (Windows IOCP) and the kqueue-only paths are `#ifdef`-out on the
Linux epoll build and emit no gcov.

## How it's measured

1. **Instrumented build** — `setup.py build_ext` with `-fprofile-arcs
   -ftest-coverage -O0` (exact line mapping).
2. **Drive** — `tests/run_isolated.py -j1` runs the **whole corpus** one file per
   subprocess, serially (clean per-process `.gcda` flush + merge; parallel would
   race the shared `.gcda`), plus `tools/mn_stress.py` for contended
   scheduler/netpoll paths. (`test_soak.py` skipped — pure repetition, no new
   lines. A global `RUNLOOM_TCPCONN_IOURING`/`RUNLOOM_IOURING_LOOP` re-drive was
   tried and reverted — see *io_uring*.)
3. **Aggregate** — `tools/cov_subsystem.py` sums gcov across each `.c` TU **and
   its `.c.inc` fragments** (gcov emits one report per source file; the real code
   lives in the fragments), subtracts the exclusion manifest from both numerator
   and denominator, and reports per-TU + whole-extension with a ≥95% gate.

```
tools/cov_measure.sh                            # build + drive + gcov + report + restore normal .so
python tools/cov_subsystem.py build/coverage    # re-report from existing gcov
```

## Exclusion categories (`tools/coverage_exclusions.txt`, 211 entries)

A line is excluded only if a clean-exiting test cannot reach it (gcov flushes
only on clean process exit). Each entry carries fragment, line range, category,
and a concrete reason.

| Category | n | meaning |
|----------|--:|---------|
| DEFENSIVE | 60 | "can't happen" corruption/invariant guards with no forge path |
| OOM | 51 | alloc-failure cleanup unreachable even via the `faultinj` LD_PRELOAD / `strace -e inject` harnesses (the failure path then crashes/aborts before gcov flushes) |
| RACE | 32 | a free-threaded interleaving with no deterministic trigger; a `for(;;)` commit-CAS retry latch gcov counts only under contention (enclosing function fully covered); or a non-atomic `-O0` gcov line-counter race on a line **proven to execute** (cldeque steal/pop tails; crash disarm body) |
| DEAD | 19 | defined/exported but zero callers (proven by grep + `nm`) |
| MIGRATION | 17 | gated on `RUNLOOM_ALLOW_UNSAFE_MIGRATION` / `per_g_tstate` mode — a known-crash mode this project forbids enabling |
| CRASHONLY | 12 | runs only in the fatal-signal handler, which re-raises and dies before gcov flushes |
| PLATFORM | 11 | `#ifdef`-out on Linux epoll, or needs an absent kernel/rlimit feature (pre-4.5 EPOLLEXCLUSIVE / MADV_FREE) |
| SPAWNFAIL | 8 | OS-thread / `PyThreadState_New` failure cleanup; no fault hook |
| BLOCKED | 1 | coverable in principle but blocked by the io_uring-recv deadlock (below) |

**Verification.** The manifest was built in two adversarial passes: every entry
(extracted from a test docstring, or classified from an uncovered line) was
re-checked by an **independent skeptic** instructed to refute it — find a
clean-exit test that *does* reach it, using `faultinj`/`strace`/fork — upholding
the exclusion only when refutation failed. 12 originally-claimed exclusions were
**rejected** as actually-coverable and turned into gap-fill tests
(`tests/test_cov95_gap_*.py`, 34 tests) instead.

## Known limitation — io_uring recv backpressure deadlock

While driving io_uring coverage we found a real bug: forcing recv through the
opt-in io_uring backend (`RUNLOOM_TCPCONN_IOURING=1`) **deadlocks a backpressured
loopback transfer** — the receiver parks in `recv()` mid-transfer and is never
woken; the default epoll backend is unaffected. Reproducer + analysis:
`tests/regressions/iouring_recv_backpressure_deadlock.py`. Consequently a few
io_uring recv/eventfd cleanup lines reachable only by failing a syscall *during an
active recv* cannot be driven by a clean-exit test and are left uncovered
(io_uring.c 97.1%) pending a fix. Latent — io_uring recv is opt-in per connection
and the suite runs default mode.

## Notes

- **cldeque.c** (lock-free Chase-Lev deque) and **runloom_crash.c**'s disarm body
  have lines that execute under contention (the gap-fill tests drive `steal`
  891,830× / `pop` 146,611× with "blocks executed 100%", and the disarm body to a
  proven count) but whose non-atomic `-O0` gcov *line* counters race to `#####`.
  Excluded as RACE (execution proven); cldeque is additionally model/sanitizer-
  checked by `tests_c/test_cldeque.c` (ASan/TSan/UBSan).
- **High-bug files** (netpoll 99.6%, mn_sched 99.3%, runloom_sched 98.8%) sit just
  under 100%; the residual is genuine MIGRATION/DEFENSIVE/RACE that cannot be
  driven without the forbidden migration mode or a deterministic race trigger.
