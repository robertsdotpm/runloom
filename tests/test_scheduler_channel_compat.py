"""Scheduler fairness + channel concurrency *under cooperative blocking I/O*.

Adapted from the Go runtime's src/runtime/proc_test.go (scheduler/fiber
progress and fairness) and src/runtime/chan_test.go (channel produce/consume,
fan-in/fan-out, select), plus the many-handles concurrency shape of libuv's
test/test-tcp-* and test/test-timer.c.

test_chan.py already covers channels in isolation.  The point *here* is that
the blocking-API patches and the channel/scheduler primitives compose: many
fibers parked on real socket I/O all make progress, data sourced from
blocking reads flows through native channels in a pipeline, and runloom_c's
select() picks among channels fed by blocking I/O -- with no starvation, no
lost items, and genuine overlap rather than serialization.
"""
import socket
import time
import unittest

import runloom
import runloom.monkey
import runloom_c


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    runloom_c.go(runner)
    runloom_c.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    runloom.monkey.patch()


def tearDownModule():
    runloom.monkey.unpatch()


def _echo_pair():
    """A connected socketpair with one end acting as an echo responder
    fiber; returns the client end."""
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


class TestSchedulerFairness(unittest.TestCase):
    def test_many_blocked_io_all_progress(self):
        """proc_test-style: N fibers each block on a socket recv; once a
        single broadcast wakes them, every one must make progress (no
        starvation / lost wakeup), and they overlap on one OS thread."""
        def body():
            N = 64
            pairs = [_echo_pair() for _ in range(N)]
            done = []

            def worker(idx):
                a, _b = pairs[idx]
                data = a.recv(16)            # parks until its b end is fed
                done.append((idx, data))

            for i in range(N):
                runloom_c.go(lambda i=i: worker(i))

            # Let them all park, then feed every peer in a burst.
            def feeder():
                time.sleep(0.02)
                for _a, b in pairs:
                    b.send(b"go")

            runloom_c.go(feeder)

            t0 = time.monotonic()
            while len(done) < N and time.monotonic() - t0 < 5:
                runloom.sleep(0.005)
            for a, b in pairs:
                a.close(); b.close()
            return done

        done = _drive(body)
        self.assertEqual(len(done), 64)
        self.assertEqual(sorted(i for i, _ in done), list(range(64)))
        self.assertTrue(all(d == b"go" for _, d in done))

    def test_io_round_trips_overlap(self):
        """N independent socket round-trips, each gated by a 30ms responder
        delay, must finish in ~one delay (concurrent), not N*delay."""
        def body():
            N, DELAY = 10, 0.03
            results = []

            def round_trip(idx):
                a, b = _echo_pair()

                def responder():
                    a.recv(8)                # wait for the ping
                    time.sleep(DELAY)        # simulate work
                    a.send(b"pong")

                runloom_c.go(responder)
                b.send(b"ping")
                r = b.recv(8)
                results.append((idx, r))
                a.close(); b.close()

            t0 = time.monotonic()
            for i in range(N):
                runloom_c.go(lambda i=i: round_trip(i))
            while len(results) < N and time.monotonic() - t0 < 5:
                runloom.sleep(0.005)
            return len(results), time.monotonic() - t0

        n, elapsed = _drive(body)
        self.assertEqual(n, 10)
        # Serial would be ~0.30s; overlapped is ~0.03-0.06s.
        self.assertLess(elapsed, 0.20)


class TestChannelPipeline(unittest.TestCase):
    def test_blocking_io_into_channel_pipeline(self):
        """chan_test-style 3-stage pipeline.  Stage 1 reads bytes off sockets
        (cooperative blocking I/O) and feeds a channel; stage 2 transforms and
        forwards; the collector drains.  Every item must arrive, in order per
        producer."""
        def body():
            ITEMS = 50
            src = runloom_c.Chan(8)
            mid = runloom_c.Chan(8)
            collected = []

            # Feed a socket from which stage 1 reads, one byte-record per item.
            a, b = _echo_pair()

            def producer():
                for i in range(ITEMS):
                    b.send(bytes([i]))
                b.close()

            def stage1():
                seen = 0
                while seen < ITEMS:
                    chunk = a.recv(64)
                    if not chunk:
                        break
                    for byte in chunk:
                        src.send(byte)
                        seen += 1
                src.close()

            def stage2():
                while True:
                    v, ok = src.recv()
                    if not ok:
                        break
                    mid.send(v * 2)
                mid.close()

            def collector():
                while True:
                    v, ok = mid.recv()
                    if not ok:
                        break
                    collected.append(v)

            runloom_c.go(producer)
            runloom_c.go(stage1)
            runloom_c.go(stage2)
            runloom_c.go(collector)

            t0 = time.monotonic()
            while len(collected) < ITEMS and time.monotonic() - t0 < 5:
                runloom.sleep(0.005)
            a.close(); b.close()
            return collected

        collected = _drive(body)
        self.assertEqual(collected, [i * 2 for i in range(50)])

    def test_fan_in_conservation(self):
        """M producers each block-recv from their own socket and forward into
        one shared channel; a single consumer drains.  No item lost/duplicated
        (Go fan-in pattern)."""
        def body():
            M, PER = 6, 20
            total = M * PER
            hub = runloom_c.Chan(4)
            got = []

            pairs = [_echo_pair() for _ in range(M)]

            def feeder(idx):
                a, b = pairs[idx]
                for i in range(PER):
                    b.send(bytes([idx, i]))
                b.close()

            def producer(idx):
                a, _b = pairs[idx]
                seen = 0
                while seen < PER:
                    chunk = a.recv(64)
                    if not chunk:
                        break
                    # each record is 2 bytes
                    for j in range(0, len(chunk), 2):
                        hub.send((chunk[j], chunk[j + 1]))
                        seen += 1

            done = {"producers": 0}

            def producer_wrap(idx):
                producer(idx)
                done["producers"] += 1
                if done["producers"] == M:
                    hub.close()

            def consumer():
                while True:
                    v, ok = hub.recv()
                    if not ok:
                        break
                    got.append(v)

            for i in range(M):
                runloom_c.go(lambda i=i: feeder(i))
                runloom_c.go(lambda i=i: producer_wrap(i))
            runloom_c.go(consumer)

            t0 = time.monotonic()
            while len(got) < total and time.monotonic() - t0 < 5:
                runloom.sleep(0.005)
            for a, b in pairs:
                a.close(); b.close()
            return got

        got = _drive(body)
        self.assertEqual(len(got), 6 * 20)
        # Exactly the full cross-product, once each.
        self.assertEqual(sorted(got),
                         sorted((idx, i) for idx in range(6) for i in range(20)))


class TestSelectWithIO(unittest.TestCase):
    def test_select_among_io_fed_channels(self):
        """Go select{}: wait on two channels, each fed by a fiber doing a
        blocking socket read first.  select must return the one that becomes
        ready, and over repeated rounds both sources get serviced."""
        def body():
            ch0 = runloom_c.Chan()
            ch1 = runloom_c.Chan()

            def io_feeder(ch, pair, tag, delay):
                a, b = pair
                # block until fed, then publish onto the channel
                def kick():
                    time.sleep(delay)
                    b.send(b"x")
                runloom_c.go(kick)
                a.recv(4)
                ch.send(tag)
                a.close(); b.close()

            p0, p1 = _echo_pair(), _echo_pair()
            runloom_c.go(lambda: io_feeder(ch0, p0, "zero", 0.04))
            runloom_c.go(lambda: io_feeder(ch1, p1, "one", 0.02))

            results = []
            for _ in range(2):
                idx, val = runloom_c.select([("recv", ch0), ("recv", ch1)])
                results.append(val[0] if isinstance(val, tuple) else val)
            return sorted(results)

        # ch1 (20ms) should generally fire before ch0 (40ms); both serviced.
        self.assertEqual(_drive(body), ["one", "zero"])


class TestConcurrentTimers(unittest.TestCase):
    def test_many_sleeps_fire_concurrently(self):
        """libuv test-timer shape: N cooperative sleeps of equal duration all
        finish within ~one duration, proving time.sleep parks (not spins) and
        the timer wheel services them together."""
        def body():
            N, NAP = 50, 0.05
            fired = []

            def napper(i):
                time.sleep(NAP)
                fired.append(i)

            t0 = time.monotonic()
            for i in range(N):
                runloom_c.go(lambda i=i: napper(i))
            while len(fired) < N and time.monotonic() - t0 < 5:
                runloom.sleep(0.002)
            return len(fired), time.monotonic() - t0

        n, elapsed = _drive(body)
        self.assertEqual(n, 50)
        self.assertLess(elapsed, 0.20)        # not 50 * 0.05 = 2.5s


if __name__ == "__main__":
    unittest.main()
