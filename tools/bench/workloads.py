"""workloads.py -- canonical runloom microbenchmarks for the rigor harness.

Each workload is a plain function returning ``(ops, seconds)`` for ONE
in-process iteration:

  * ``ops``     -- number of logical operations performed (roundtrips,
                   spawns, context switches, ...).
  * ``seconds`` -- wall time the harness should attribute to those ops.

The harness (``rigor.py``) handles warmup, repetition, layout-bias, and
statistics; a workload just does work and times it.  Keep them allocation-
light and steady-state: the stack pool is pre-warmed by the harness, so a
workload measures the hot path, not first-spawn mmap latency.

House style: ``.format()`` / ``%`` only -- no f-strings.
"""
import time

# Guarded so the parent harness can import this module just to read workload
# names (``rigor.py list``) without a built extension on its path.  Children
# run with PYTHONPATH=src, where the import succeeds.
try:
    import runloom_c
except ImportError:
    runloom_c = None


def spawn(scale=100000):
    """Spawn ``scale`` no-op goroutines and drain them once.

    Exercises runloom_g_t alloc + coro_new (stack-pool pop) + capsule handle.
    ops = goroutines spawned-and-run.
    """
    def noop():
        pass

    t0 = time.perf_counter()
    for _ in range(scale):
        runloom_c.go(noop)
    runloom_c.run()
    return scale, time.perf_counter() - t0


def chan_pingpong(scale=200000):
    """Two goroutines bouncing a value through unbuffered channels.

    The classic ``BenchmarkPingPong`` shape: every roundtrip is 4 channel
    ops + 2 goroutine switches, all parking.  ops = roundtrips.
    """
    a = runloom_c.Chan()
    b = runloom_c.Chan()

    def pinger():
        for i in range(scale):
            a.send(i)
            b.recv()

    def ponger():
        for _ in range(scale):
            v, _ = a.recv()
            b.send(v)

    runloom_c.go(pinger)
    runloom_c.go(ponger)
    t0 = time.perf_counter()
    runloom_c.run()
    return scale, time.perf_counter() - t0


def chan_buffered(scale=500000, cap=64):
    """Single producer/consumer over a buffered channel.

    Most sends don't park (buffer absorbs them): measures the buffered
    fast path rather than the park/wake path.  ops = items moved.
    """
    ch = runloom_c.Chan(cap)

    def producer():
        for i in range(scale):
            ch.send(i)
        ch.close()

    def consumer():
        while True:
            _, ok = ch.recv()
            if not ok:
                break

    runloom_c.go(producer)
    runloom_c.go(consumer)
    t0 = time.perf_counter()
    runloom_c.run()
    return scale, time.perf_counter() - t0


def yield_storm(gs=200, k=2000):
    """``gs`` goroutines each yielding ``k`` times.

    Pure scheduler context-switch throughput, no channels.
    ops = context switches = gs * k.
    """
    def spinner():
        for _ in range(k):
            runloom_c.yield_()

    for _ in range(gs):
        runloom_c.go(spinner)
    t0 = time.perf_counter()
    runloom_c.run()
    return gs * k, time.perf_counter() - t0


# name -> (callable, unit label for ops)
WORKLOADS = {
    "spawn":         (spawn,         "spawn+run"),
    "chan_pingpong": (chan_pingpong, "roundtrip"),
    "chan_buffered": (chan_buffered, "item"),
    "yield_storm":   (yield_storm,   "ctxsw"),
}
