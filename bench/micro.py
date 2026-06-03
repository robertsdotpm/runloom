"""Core single-hub scheduler microbenchmarks.

These isolate the cost of pygo's scheduler primitives with no I/O:

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
import pygo_core

from bench.harness import Suite


def make_spawn(n):
    """Spawn n no-op goroutines, run to empty.  inner = n goroutines."""
    go = pygo_core.go
    run = pygo_core.run

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
    """
    go = pygo_core.go
    run = pygo_core.run
    sched_yield = pygo_core.sched_yield

    def worker(m):
        for _ in range(m):
            sched_yield()

    def once():
        for _ in range(n_coros):
            go(worker, m_yields)
        run()

    return once


def make_pingpong(n):
    """Unbuffered two-goroutine ping-pong.  inner = n round-trips.

    Each round-trip = a.send + a.recv + b.send + b.recv = 4 chan ops and
    2 cross-goroutine wakeups -- the unbuffered (always-park) hot path.
    """
    go = pygo_core.go
    run = pygo_core.run

    def once():
        a = pygo_core.Chan()
        b = pygo_core.Chan()

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
    go = pygo_core.go
    run = pygo_core.run

    def once():
        ch = pygo_core.Chan(cap)

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
    s = Suite("micro", samples=20, warmup=5)
    s.banner()
    s.bench("spawn+run noop x10k", make_spawn(10_000), inner=10_000,
            note="go()+run() round-trip per goroutine")
    s.bench("yield 100coro x1000", make_yield(100, 1_000), inner=100_000,
            note="context-switch, few long-lived goroutines")
    s.bench("yield 1000coro x100", make_yield(1_000, 100), inner=100_000,
            note="context-switch, many short goroutines")
    s.bench("chan unbuf ping-pong x100k", make_pingpong(100_000), inner=100_000,
            note="always-park round-trip; comparable to Go BenchmarkPingPong")
    s.bench("chan buf64 send/recv x500k", make_buffered(500_000, 64),
            inner=500_000, note="buffered no-park fast path")
    s.write()


if __name__ == "__main__":
    main()
