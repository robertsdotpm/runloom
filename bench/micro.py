"""Core single-hub scheduler microbenchmarks.

These isolate the cost of runloom's scheduler primitives with no I/O:

  * spawn+run   -- dispatch N no-op goroutines and drain to empty
                   (go() + run() round-trip per goroutine)
  * yield       -- pure cooperative context-switch cost (sched_yield)
  * chan ping-pong (unbuffered) -- the classic two-goroutine round-trip,
                   directly comparable to Go's BenchmarkPingPong
  * chan buffered send/recv     -- the no-park buffered fast path

All run on ONE hub (no M:N parallelism) so the numbers reflect raw
per-operation overhead, not core scaling.  M:N scaling lives in bench/mn.py.

Run:
    PYTHONPATH=src ~/.pyenv/versions/3.13.13t/bin/python -m bench.micro
"""
import runloom_c

from bench.gil import ensure_nogil
from bench.harness import Suite


def make_spawn(n):
    """Spawn n no-op goroutines, run to empty.  inner = n goroutines."""
    go = runloom_c.fiber
    run = runloom_c.run

    def noop():
        pass

    def once():
        for _ in range(n):
            go(noop)
        run()

    return once


def make_yield(n_coros, m_yields):
    """n_coros goroutines each yielding m_yields times.

    inner = n_coros * m_yields cooperative context switches.  Two shapes
    (few-long vs many-short) expose ready-queue vs per-goroutine cost.

    NB: m_yields is captured via the closure, NOT passed as a second
    positional to go() -- `runloom_c.fiber(fn, stack_size=None)` reads its
    second positional as the stack size, so `go(worker, m)` would set the
    stack to m bytes and call worker() with no args (silent TypeError,
    swallowed by the scheduler -> zero work measured).
    """
    go = runloom_c.fiber
    run = runloom_c.run
    sched_yield = runloom_c.sched_yield

    def worker():
        for _ in range(m_yields):
            sched_yield()

    def once():
        for _ in range(n_coros):
            go(worker)
        run()

    return once


def make_pingpong(n):
    """Unbuffered two-goroutine ping-pong.  inner = n round-trips.

    Each round-trip = a.send + a.recv + b.send + b.recv = 4 chan ops and
    2 cross-goroutine wakeups -- the unbuffered (always-park) hot path.
    """
    go = runloom_c.fiber
    run = runloom_c.run

    def once():
        a = runloom_c.Chan()
        b = runloom_c.Chan()

        def pinger():
            for i in range(n):
                a.send(i)
                b.recv()

        def ponger():
            for _ in range(n):
                v, _ = a.recv()
                b.send(v)

        go(pinger)
        go(ponger)
        run()

    return once


def make_buffered(n, cap):
    """Buffered single-producer/single-consumer.  inner = n sends.

    With a cap-deep buffer most sends don't park, so this measures the
    buffered fast path rather than the wakeup machinery.
    """
    go = runloom_c.fiber
    run = runloom_c.run

    def once():
        ch = runloom_c.Chan(cap)

        def producer():
            for i in range(n):
                ch.send(i)

        def consumer():
            for _ in range(n):
                ch.recv()

        go(producer)
        go(consumer)
        run()

    return once


def main():
    # Run-as-script entry point: force the GIL off (re-exec with -X gil=0 if
    # needed) before any measurement.  Done here, not at import time, so that
    # `import bench.micro` from pytest stays side-effect-free.
    ensure_nogil()
    s = Suite("micro", samples=20, warmup=5)
    s.banner()
    # Inner counts are sized so each sample is >~25 ms: on a shared, noisy
    # VM a sub-millisecond sample's dispersion swamps any real delta, which
    # makes the regression gate useless. Longer samples -> stable min_s.
    s.bench("spawn+run noop x10k", make_spawn(10_000), inner=10_000,
            note="go()+run() round-trip per goroutine")
    s.bench("yield 100coro x2000", make_yield(100, 2_000), inner=200_000,
            note="context-switch, few long-lived goroutines (cache-hot)")
    s.bench("yield 1000coro x200", make_yield(1_000, 200), inner=200_000,
            note="context-switch, many goroutines (cache-cold, cf. F1)")
    s.bench("chan unbuf ping-pong x100k", make_pingpong(100_000), inner=100_000,
            note="always-park round-trip; comparable to Go BenchmarkPingPong")
    s.bench("chan buf64 send/recv x500k", make_buffered(500_000, 64),
            inner=500_000, note="buffered no-park fast path")
    s.write()


if __name__ == "__main__":
    main()
