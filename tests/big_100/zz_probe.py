"""Scheduler-only 1M-goroutine probe (NOT one of the 100 programs).

The thinnest possible workload: each goroutine parks in a cooperative sleep
(so ~N are alive AT ONCE during the window), bumps a metric, returns.  Isolates
the M:N scheduler's capacity to field N goroutines from any per-program I/O.
Default backend (no io_uring loop).
"""
import harness


def worker(H, wid, rng):
    # Park so all N goroutines are simultaneously alive ("at once") before any
    # of them return; then complete.
    H.sleep(1.0)
    H.op(wid)
    H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker)


if __name__ == "__main__":
    harness.main("zz_probe", body, default_funcs=10000,
                 describe="scheduler-only N-goroutine probe")
