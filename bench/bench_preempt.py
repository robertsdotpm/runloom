"""Demonstrate Go-1.14-style preemption: a CPU-bound goroutine that
never calls sched_yield() still lets other goroutines make progress
because the runtime preempts it via Py_AddPendingCall + eval_breaker.

Two goroutines are spawned in order: a busy CPU loop first, then a
reader.  In the cooperative-only model the busy loop runs to
completion before the reader gets a turn -- so the reader's FIRST
tick happens AFTER busy_done_ts.  With preemption the reader gets
periodic turns inside the busy loop -- so its FIRST tick happens
BEFORE busy_done_ts.

Run with `~/.pyenv/versions/3.13.13t/bin/python3.13t bench/bench_preempt.py`.
preempt_init is 3.13t-only.
"""
import sys
import time

sys.path.insert(0, "src")
import pygo_core


def busy_loop(n_iters, marker):
    """Pure-CPU loop with NO sched_yield() calls."""
    total = 0
    for i in range(n_iters):
        total += i * i
    marker["busy_done_ts"] = time.perf_counter()
    marker["busy_total"] = total


def reader(marker, n_target):
    """Cooperative goroutine; records its first-tick time."""
    marker["reader_first_ts"] = time.perf_counter()
    for _ in range(n_target):
        pygo_core.sched_yield()
    marker["reader_done_ts"] = time.perf_counter()


def run(quantum_us, label):
    marker = {"start_ts": time.perf_counter()}
    if quantum_us > 0:
        pygo_core.preempt_init(quantum_us)
    try:
        pygo_core.go(lambda: busy_loop(8_000_000, marker))
        pygo_core.go(lambda: reader(marker, 10))
        pygo_core.run()
    finally:
        if quantum_us > 0:
            pygo_core.preempt_fini()
    start = marker["start_ts"]
    busy_end = marker["busy_done_ts"] - start
    reader_first = marker["reader_first_ts"] - start
    reader_end = marker["reader_done_ts"] - start
    preempted = reader_first < busy_end
    print("  {:<22}  busy_done={:>5.0f}ms  reader_first={:>5.0f}ms  "
          "{}".format(label, busy_end * 1000.0, reader_first * 1000.0,
                      "[PREEMPTED]" if preempted else "[serialized]"))


def main():
    print("pygo time-sliced preemption demo (3.13t)")
    print()
    run(0,      "no preemption")
    run(10_000, "preempt every 10 ms")
    run(1_000,  "preempt every 1 ms")
    run(100,    "preempt every 100 us")


if __name__ == "__main__":
    main()
