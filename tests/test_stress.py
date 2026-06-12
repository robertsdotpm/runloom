"""Stress tests for runloom.  High volume, long-running, memory soak.

Run a subset by default; the full suite (RUNLOOM_RUN_STRESS=1) exercises
patterns that take seconds to minutes and are kept out of the normal
unit run so CI stays fast.  These tests are how we catch:

  * fiber / coro / chan / Future leaks (RSS climbs)
  * scheduler starvation under load
  * channel send/recv head-tail desync at high throughput
  * use-after-free across yield boundaries at scale
  * deadlocks that only appear when N waiters are in flight
"""
import asyncio
import gc
import os
import sys
import time
import unittest

import runloom_c
import runloom.aio as paio
import runloom.sync as ps


_FULL = os.environ.get("RUNLOOM_RUN_STRESS", "").strip() not in ("", "0", "no", "false")


def _rss_mb():
    """Best-effort RSS in MiB.  Returns -1 if /proc isn't available."""
    try:
        with open("/proc/self/statm") as f:
            return int(f.read().split()[1]) * (os.sysconf("SC_PAGE_SIZE") / (1 << 20))
    except Exception:
        return -1.0


# ====================================================================
# Section 1: scheduler stress
# ====================================================================
class TestSchedulerStress(unittest.TestCase):
    def test_spawn_drain_100k(self):
        """100,000 spawn/drain cycles (10 batches of 10k).  Catches
        fiber slab / stack pool leaks."""
        for batch in range(10):
            for _ in range(10_000):
                runloom_c.go(lambda: None)
            runloom_c.run()
        gc.collect()
        stats = runloom_c.stats()
        self.assertEqual(stats["ready"], 0)
        self.assertEqual(stats["sleeping"], 0)
        # Use the PER-SCHED parked count (this thread's sched), not the global
        # one: in an in-process suite a parker can be stranded on another (or a
        # since-exited) thread's sched -- e.g. test_leaked_parker_does_not_wedge
        # deliberately leaks one on a dead thread -- and the global count then
        # flakes this assertion even though THIS workload (100k spawn/drain on
        # the main sched) leaked nothing.
        self.assertEqual(stats.get("netpoll_parked_self",
                                   stats["netpoll_parked"]), 0)

    def test_deep_yield_chain(self):
        """One fiber doing 100k yields.  Catches snap/load drift
        in the loop body."""
        counter = [0]
        def w():
            for _ in range(100_000):
                counter[0] += 1
                runloom_c.sched_yield_classic()
        runloom_c.go(w)
        runloom_c.run()
        self.assertEqual(counter[0], 100_000)

    def test_n_yielding_workers(self):
        """N=500 workers each yielding K=200 times -- 100k total
        cooperative switches.  Catches ready-ring desync."""
        N, K = 500, 200
        counters = [0] * N
        def w(i):
            for _ in range(K):
                counters[i] += 1
                runloom_c.sched_yield_classic()
        for i in range(N):
            runloom_c.go(lambda i=i: w(i))
        runloom_c.run()
        self.assertEqual(sum(counters), N * K)
        self.assertTrue(all(c == K for c in counters))

    def test_sleeper_storm(self):
        """1000 fibers all sleeping with staggered wake times.
        Catches sleep-heap heap-invariant bugs."""
        N = 1000
        wakes = [None] * N
        t0 = time.monotonic()
        def w(i):
            runloom_c.sched_sleep(0.001 + i * 0.00001)
            wakes[i] = time.monotonic() - t0
        for i in range(N):
            runloom_c.go(lambda i=i: w(i))
        runloom_c.run()
        # Every sleep completed.
        self.assertTrue(all(w is not None for w in wakes))
        # Wake times are roughly ascending (matches sleep deadlines).
        out_of_order = 0
        for i in range(1, N):
            if wakes[i] < wakes[i - 1] - 0.005:  # allow 5ms slop
                out_of_order += 1
        self.assertLess(out_of_order, 50)  # <5% wake-order violations


# ====================================================================
# Section 2: channel stress
# ====================================================================
class TestChannelStress(unittest.TestCase):
    def test_pingpong_1m(self):
        """1M ping-pong rounds through a buffered channel."""
        ch_a = runloom_c.Chan(0)
        ch_b = runloom_c.Chan(0)
        N = 1_000_000 if _FULL else 100_000
        def pinger():
            for _ in range(N):
                ch_a.send(1)
                ch_b.recv()
        def ponger():
            for _ in range(N):
                ch_a.recv()
                ch_b.send(1)
        runloom_c.go(pinger)
        runloom_c.go(ponger)
        t0 = time.monotonic()
        runloom_c.run()
        elapsed = time.monotonic() - t0
        print("\n  pingpong %d rounds in %.2fs (%.1f ns/round)"
              % (N, elapsed, elapsed * 1e9 / N))

    def test_fan_in(self):
        """N producers, 1 consumer.  Catches lost-wakeup on multi-
        sender queues."""
        N = 100
        K = 1000
        ch = runloom_c.Chan(16)
        results = []

        def producer(pid):
            for i in range(K):
                ch.send((pid, i))

        def consumer():
            for _ in range(N * K):
                results.append(ch.recv())

        for p in range(N):
            runloom_c.go(lambda p=p: producer(p))
        runloom_c.go(consumer)
        runloom_c.run()

        self.assertEqual(len(results), N * K)
        # Each producer's K messages all arrive (order may interleave).
        per_producer = {}
        for v in results:
            pid, _ = v[0]
            per_producer[pid] = per_producer.get(pid, 0) + 1
        self.assertEqual(set(per_producer.values()), {K})

    def test_fan_out(self):
        """1 producer, N consumers."""
        N = 50
        K = 2000
        ch = runloom_c.Chan(8)
        per_consumer = [0] * N

        def producer():
            for i in range(N * K):
                ch.send(i)
            ch.close()

        def consumer(idx):
            for _v in ch:
                per_consumer[idx] += 1

        runloom_c.go(producer)
        for i in range(N):
            runloom_c.go(lambda i=i: consumer(i))
        runloom_c.run()

        self.assertEqual(sum(per_consumer), N * K)

    def test_select_under_load(self):
        """Many select() calls choosing across hot channels."""
        ch_a = runloom_c.Chan(0)
        ch_b = runloom_c.Chan(0)
        ch_done = runloom_c.Chan(1)
        K = 5000
        counts = {"a": 0, "b": 0}

        def selector():
            for _ in range(K * 2):
                idx, val = runloom_c.select([
                    ("recv", ch_a),
                    ("recv", ch_b),
                ])
                if idx == 0:
                    counts["a"] += 1
                else:
                    counts["b"] += 1
            ch_done.send(None)

        def feeder(ch, key):
            for i in range(K):
                ch.send((key, i))

        runloom_c.go(selector)
        runloom_c.go(lambda: feeder(ch_a, "a"))
        runloom_c.go(lambda: feeder(ch_b, "b"))
        runloom_c.go(lambda: ch_done.recv())
        runloom_c.run()

        self.assertEqual(counts["a"], K)
        self.assertEqual(counts["b"], K)


# ====================================================================
# Section 3: memory soak (gated)
# ====================================================================
@unittest.skipUnless(_FULL, "set RUNLOOM_RUN_STRESS=1 to enable")
class TestMemorySoak(unittest.TestCase):
    def test_no_leak_spawn_drain(self):
        """1M spawn/drain cycles, measure RSS growth post-warmup."""
        for _ in range(10_000):
            runloom_c.go(lambda: None)
        runloom_c.run()
        gc.collect()
        baseline = _rss_mb()

        for _ in range(99):
            for _ in range(10_000):
                runloom_c.go(lambda: None)
            runloom_c.run()

        gc.collect()
        growth = _rss_mb() - baseline
        print("\n  spawn-drain leak: %.1f MiB after 990k extra gs" % growth)
        self.assertLess(growth, 10.0)

    def test_no_leak_aio_run(self):
        """Repeated paio.run cycles with full async surface."""
        async def main():
            await asyncio.sleep(0)
            return 1
        # Warm
        for _ in range(50):
            paio.run(main())
        gc.collect()
        baseline = _rss_mb()
        for _ in range(500):
            paio.run(main())
        gc.collect()
        growth = _rss_mb() - baseline
        print("\n  aio.run leak: %.1f MiB after 500 cycles" % growth)
        self.assertLess(growth, 5.0)

    def test_no_leak_channel_pingpong(self):
        """Long-running channel ping-pong with periodic memory checks."""
        # Warm
        ch_a = runloom_c.Chan(0)
        ch_b = runloom_c.Chan(0)
        def pinger(N):
            for _ in range(N): ch_a.send(1); ch_b.recv()
        def ponger(N):
            for _ in range(N): ch_a.recv(); ch_b.send(1)
        runloom_c.go(lambda: pinger(10_000))
        runloom_c.go(lambda: ponger(10_000))
        runloom_c.run()
        gc.collect()
        baseline = _rss_mb()
        # Long run with fresh channels each batch
        for _ in range(50):
            ch_a = runloom_c.Chan(0)
            ch_b = runloom_c.Chan(0)
            runloom_c.go(lambda: pinger(10_000))
            runloom_c.go(lambda: ponger(10_000))
            runloom_c.run()
        gc.collect()
        growth = _rss_mb() - baseline
        print("\n  channel pingpong leak: %.1f MiB after 500k extra ops" % growth)
        self.assertLess(growth, 5.0)


# ====================================================================
# Section 4: asyncio bridge stress
# ====================================================================
class TestAioStress(unittest.TestCase):
    def test_5000_concurrent_tasks(self):
        """5000 RunloomTasks each doing one short sleep."""
        async def w():
            await asyncio.sleep(0.001)
            return 1
        async def main():
            return sum(await asyncio.gather(*[w() for _ in range(5000)]))
        self.assertEqual(paio.run(main()), 5000)

    def test_deep_await_chain(self):
        """One task doing K=10000 awaits on already-done futures.
        Catches RunloomTask driver loop bugs."""
        async def main():
            loop = asyncio.get_running_loop()
            for _ in range(10_000):
                fut = loop.create_future()
                fut.set_result(None)
                await fut
            return "done"
        self.assertEqual(paio.run(main()), "done")

    def test_recursive_gather(self):
        """Nested gather to depth 100, fanning to 10 at each level.
        10 ** 100 sounds bad but we cap depth at 5 -> 100k tasks max."""
        DEPTH = 5
        FAN   = 10
        async def w(d):
            if d == 0:
                await asyncio.sleep(0)
                return 1
            return sum(await asyncio.gather(*[w(d - 1) for _ in range(FAN)]))
        async def main():
            return await w(DEPTH)
        self.assertEqual(paio.run(main()), FAN ** DEPTH)

    def test_cancellation_storm(self):
        """1000 long-sleeping tasks all cancelled.  Catches cancel
        plumbing bugs at scale."""
        async def slow():
            try:
                await asyncio.sleep(60.0)
                return "never"
            except asyncio.CancelledError:
                return "cancelled"
        async def main():
            tasks = [asyncio.create_task(slow()) for _ in range(1000)]
            await asyncio.sleep(0.005)
            for t in tasks:
                t.cancel()
            return [await t for t in tasks]
        results = paio.run(main())
        self.assertEqual(results, ["cancelled"] * 1000)


# ====================================================================
# Section 5: network stress
# ====================================================================
class TestNetworkStress(unittest.TestCase):
    def test_500_tcp_clients(self):
        """500 concurrent TCP clients to one server -- pushes the
        accept loop + per-conn task path."""
        async def handler(reader, writer):
            data = await reader.readline()
            writer.write(b"echo:" + data)
            await writer.drain()
            writer.close()

        async def client(host, port, msg):
            r, w = await paio.open_connection(host, port)
            w.write(msg + b"\n")
            await w.drain()
            data = await r.readline()
            w.close()
            return data

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            payloads = [b"msg-%05d" % i for i in range(500)]
            # Bound in-flight connects.  500 simultaneous SYNs overrun the
            # listen accept queue (min(backlog, kern.ipc.somaxconn) ~= 128):
            # BSD/macOS answer the overflow with RST -> ConnectionResetError,
            # while Linux silently drops the SYN and the client retransmits.
            # Not runloom-specific (stdlib asyncio fails the same unbounded storm
            # on FreeBSD).  Gate concurrency so the queue never overflows while
            # still driving 500 concurrent round-trips.
            sem = asyncio.Semaphore(64)

            async def bounded(p):
                async with sem:
                    return await client(host, port, p)

            results = await asyncio.gather(
                *[bounded(p) for p in payloads])
            server.close()
            return results

        results = paio.run(main())
        self.assertEqual(len(results), 500)
        for r in results:
            self.assertTrue(r.startswith(b"echo:msg-"))
            self.assertTrue(r.endswith(b"\n"))

    def test_large_payload_round_trip(self):
        """1 MiB payload round-trip -- catches buffer/drain bugs."""
        payload = b"X" * (1 << 20)

        async def handler(reader, writer):
            data = await reader.readexactly(len(payload))
            writer.write(data)
            await writer.drain()
            writer.close()

        async def main():
            server = await paio.start_server(handler, "127.0.0.1", 0)
            host, port = server.sockets[0].getsockname()[:2]
            r, w = await paio.open_connection(host, port)
            w.write(payload)
            await w.drain()
            data = await r.readexactly(len(payload))
            w.close()
            server.close()
            return data

        self.assertEqual(paio.run(main()), payload)


if __name__ == "__main__":
    unittest.main()
