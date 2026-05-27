"""pygo channel microbench -- send/recv throughput vs Go.

Two goroutines bouncing a value through an unbuffered + a buffered
channel.  Equivalent to Go's `BenchmarkPingPong` with `make(chan int)`.

Run alongside the Go bench in examples/bench_chan_go.go (same machine).
"""
import sys
import time

sys.path.insert(0, "src")
import pygo_core


def bench_unbuffered_ping_pong(N=200_000):
    a = pygo_core.Chan()
    b = pygo_core.Chan()

    def pinger():
        for i in range(N):
            a.send(i)
            b.recv()

    def ponger():
        for _ in range(N):
            v, _ = a.recv()
            b.send(v)

    pygo_core.go(pinger)
    pygo_core.go(ponger)
    t0 = time.perf_counter()
    pygo_core.run()
    dt = time.perf_counter() - t0
    # Each ping = a.send + a.recv + b.send + b.recv = 4 chan ops, 2 yields.
    # Report per-roundtrip cost (one full ping-pong).
    rt_per_s = N / dt
    ns_per_rt = dt * 1e9 / N
    print("  unbuffered ping-pong   {:>7.2f} M rt/s   {:>4.0f} ns/rt".format(
        rt_per_s / 1e6, ns_per_rt))


def bench_buffered(N=500_000, cap=64):
    """Buffered channel, single producer + consumer.  The buffer means
    most sends don't park, so this measures the buffered fast path."""
    ch = pygo_core.Chan(cap)

    def producer():
        for i in range(N):
            ch.send(i)

    def consumer():
        for _ in range(N):
            ch.recv()

    pygo_core.go(producer)
    pygo_core.go(consumer)
    t0 = time.perf_counter()
    pygo_core.run()
    dt = time.perf_counter() - t0
    ops_per_s = N / dt
    ns_per_op = dt * 1e9 / N
    print("  buffered cap={:<3d}        {:>7.2f} M send/s  {:>4.0f} ns/op".format(
        cap, ops_per_s / 1e6, ns_per_op))


def main():
    print("pygo channel microbench")
    print("backend:", pygo_core.backend())
    print()
    bench_unbuffered_ping_pong()
    bench_buffered(cap=1)
    bench_buffered(cap=64)


if __name__ == "__main__":
    main()
