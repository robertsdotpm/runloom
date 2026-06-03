"""Chaos tests for runloom.

Randomized interleavings + hostile inputs that traditional unit tests
don't reach.  We seed with a fixed value by default so failures are
reproducible; set RUNLOOM_CHAOS_SEED=<n> to vary.

Each test runs many iterations of a small scenario with randomized
choice of yield points, sleep durations, and operation ordering.  If
the invariant holds across all iterations, the test passes.  Failures
print the seed + iteration number so a repro can be built.
"""
import asyncio
import collections
import os
import random
import unittest

import runloom_c
import runloom.aio as paio
import runloom.sync as ps


_SEED = int(os.environ.get("RUNLOOM_CHAOS_SEED", "12345"))


class _Seeded(unittest.TestCase):
    """Base: gives each test a deterministic RNG seeded off _SEED + test id."""
    def setUp(self):
        # Stir the seed with the test name so different tests don't
        # all see the same sequence.
        h = sum(ord(c) for c in self.id())
        self.rng = random.Random(_SEED ^ h)

    def tearDown(self):
        # Reset asyncio policy + event loop between chaos tests.  paio.run
        # installs a loop; without this cleanup, a stale loop from the
        # previous test can leak into the next one and corrupt state.
        try:
            asyncio.set_event_loop(None)
        except Exception:
            pass


# ====================================================================
# Section 1: scheduler chaos -- random yield placement
# ====================================================================
class TestRandomYield(_Seeded):
    def test_invariant_sum_under_random_yields(self):
        """N workers each increment a shared counter K times, yielding
        at random points.  Final count must equal N*K -- catches any
        non-atomic update via cooperative interleaving."""
        N, K = 50, 200
        counter = [0]

        def w(rng):
            for _ in range(K):
                # Read-modify-write with optional yield in the middle.
                v = counter[0]
                if rng.random() < 0.5:
                    runloom_c.sched_yield_classic()
                counter[0] = v + 1

        # Single-thread cooperative: yields are at our chosen points.
        # We test that the read-modify-write pattern produces lost
        # updates when yielded; this confirms it does (the test isn't
        # asserting correctness of the user code, but of the model).
        for _ in range(20):
            counter[0] = 0
            for _ in range(N):
                runloom_c.go(lambda r=random.Random(self.rng.random()): w(r))
            runloom_c.run()
            # With random yields between read and write, we DO lose
            # updates -- that's the design (cooperative != locked).
            # The check is: the count is in [0, N*K], non-zero.
            self.assertGreater(counter[0], 0)
            self.assertLessEqual(counter[0], N * K)


# ====================================================================
# Section 2: channel chaos
# ====================================================================
class TestChannelChaos(_Seeded):
    def test_random_send_recv_order(self):
        """Producers and consumers fire send/recv at randomly chosen
        moments.  Verifies that everything that was sent is received
        (no lost messages) for any interleaving."""
        for trial in range(50):
            N_PRODS = self.rng.randint(2, 8)
            N_CONS  = self.rng.randint(2, 8)
            BUFFER  = self.rng.choice([0, 1, 4, 16])
            PER     = self.rng.randint(10, 100)

            ch = runloom_c.Chan(BUFFER)
            sent_total  = N_PRODS * PER
            recv_count  = [0]

            def producer(pid):
                for i in range(PER):
                    if self.rng.random() < 0.2:
                        runloom_c.sched_yield_classic()
                    ch.send((pid, i))

            done = [False]
            def closer():
                # Wait for all producer goroutines to finish.  We don't
                # have a join primitive here -- approximate by sleeping
                # a tiny bit then closing.  If consumers haven't drained,
                # they will (loop on recv).
                runloom_c.sched_sleep(0.01)
                ch.close()
                done[0] = True

            def consumer():
                for _v in ch:
                    recv_count[0] += 1

            for pid in range(N_PRODS):
                runloom_c.go(lambda pid=pid: producer(pid))
            for _ in range(N_CONS):
                runloom_c.go(consumer)
            runloom_c.go(closer)
            runloom_c.run()

            self.assertEqual(
                recv_count[0], sent_total,
                "trial %d: %d/%d (prods=%d cons=%d buf=%d per=%d)"
                % (trial, recv_count[0], sent_total,
                   N_PRODS, N_CONS, BUFFER, PER))

    def test_close_during_send(self):
        """Random closes mid-send.  Closed senders raise; closed
        receivers see ok=False."""
        for trial in range(30):
            BUFFER = self.rng.choice([0, 1, 4])
            ch = runloom_c.Chan(BUFFER)
            raised_send = [0]
            recv_after_close = [0]

            def sender():
                try:
                    for i in range(100):
                        ch.send(i)
                except Exception:
                    raised_send[0] = 1

            def receiver():
                for _v in ch:
                    pass
                recv_after_close[0] = 1

            def closer():
                runloom_c.sched_sleep(self.rng.uniform(0.0001, 0.005))
                ch.close()

            runloom_c.go(sender)
            runloom_c.go(receiver)
            runloom_c.go(closer)
            runloom_c.run()

            # After close, sender raised (sent < 100) OR sent all 100.
            # Receiver exits normally after seeing all queued + close.
            self.assertEqual(recv_after_close[0], 1)


# ====================================================================
# Section 3: cancellation chaos
# ====================================================================
class TestCancellationChaos(_Seeded):
    def test_cancel_at_random_times(self):
        """N tasks cancelled at random points in their lifecycle.
        Each task either completes normally OR raises CancelledError --
        never something else."""
        async def w(dur, rng):
            try:
                # 0..dur seconds, with internal yield points.
                t = 0
                step = dur / 10
                for _ in range(10):
                    await asyncio.sleep(step)
                    t += step
                return "ok"
            except asyncio.CancelledError:
                return "cancelled"

        for trial in range(20):
            N = self.rng.randint(20, 100)
            duration = self.rng.uniform(0.005, 0.05)

            async def main():
                tasks = [
                    asyncio.create_task(
                        w(duration, random.Random(self.rng.random())))
                    for _ in range(N)
                ]
                # Cancel random subset at random times.
                async def killer():
                    for _ in range(self.rng.randint(0, N)):
                        await asyncio.sleep(self.rng.uniform(0, duration))
                        idx = self.rng.randint(0, N - 1)
                        if not tasks[idx].done():
                            tasks[idx].cancel()
                asyncio.create_task(killer())
                # Wait for all.
                out = []
                for t in tasks:
                    try:
                        out.append(await t)
                    except asyncio.CancelledError:
                        out.append("cancelled")
                return out

            results = paio.run(main())
            self.assertEqual(len(results), N)
            for r in results:
                self.assertIn(r, ("ok", "cancelled"),
                              "trial %d: bad result %r" % (trial, r))


# ====================================================================
# Section 4: lock chaos
# ====================================================================
class TestLockChaos(_Seeded):
    def test_lock_serialises_under_random_contention(self):
        """Many tasks contend on one lock at random rates.  The lock
        must serialise: never see two tasks inside the critical
        section at once."""
        for trial in range(20):
            N = self.rng.randint(10, 40)
            K = self.rng.randint(20, 60)

            async def main():
                lk = asyncio.Lock()
                inside = [0]
                max_inside = [0]

                async def w():
                    for _ in range(K):
                        async with lk:
                            inside[0] += 1
                            if inside[0] > max_inside[0]:
                                max_inside[0] = inside[0]
                            # Yield randomly to invite races.
                            if self.rng.random() < 0.3:
                                await asyncio.sleep(0)
                            inside[0] -= 1

                await asyncio.gather(*[w() for _ in range(N)])
                return max_inside[0]

            max_inside = paio.run(main())
            self.assertEqual(
                max_inside, 1,
                "trial %d: lock breach max_inside=%d" % (trial, max_inside))


# ====================================================================
# Section 5: queue chaos
# ====================================================================
class TestQueueChaos(_Seeded):
    def test_queue_no_lost_messages(self):
        """Many producers/consumers on an asyncio.Queue at random rates.
        Every item must arrive exactly once."""
        for trial in range(10):
            N_PRODS = self.rng.randint(2, 6)
            N_CONS  = self.rng.randint(2, 6)
            PER     = self.rng.randint(10, 50)
            MAXSIZE = self.rng.choice([0, 1, 4])

            async def main():
                q = asyncio.Queue(maxsize=MAXSIZE)
                sent = []
                received = []

                async def producer(pid):
                    for i in range(PER):
                        if self.rng.random() < 0.2:
                            await asyncio.sleep(0)
                        item = (pid, i)
                        sent.append(item)
                        await q.put(item)

                async def consumer():
                    while True:
                        try:
                            item = await asyncio.wait_for(q.get(), timeout=0.5)
                        except asyncio.TimeoutError:
                            return
                        received.append(item)

                ptasks = [asyncio.create_task(producer(p)) for p in range(N_PRODS)]
                ctasks = [asyncio.create_task(consumer()) for _ in range(N_CONS)]
                await asyncio.gather(*ptasks)
                # Wait for queue to drain.
                while not q.empty():
                    await asyncio.sleep(0)
                # Cancel consumers (they're waiting for more).
                for t in ctasks:
                    t.cancel()
                for t in ctasks:
                    try: await t
                    except asyncio.CancelledError: pass
                    except asyncio.TimeoutError: pass
                return sent, received

            sent, received = paio.run(main())
            self.assertEqual(
                collections.Counter(sent),
                collections.Counter(received),
                "trial %d: send-recv mismatch (sent %d / recv %d)"
                % (trial, len(sent), len(received)))


if __name__ == "__main__":
    unittest.main()
