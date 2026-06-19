"""Cooperative multiprocessing: Connection (Pipe) recv / send / poll.

Nothing in runloom.monkey reimplements multiprocessing -- it cooperates because
the primitives it is built on are cooperative.  On POSIX
multiprocessing.connection.Connection reads its pipe with os.read and waits
with select/poll, all patched, so Connection.recv parks on wait_fd and
Process.join / Queue.get / Pool (built on Connection) come along.

The one thing runloom.monkey *does* patch: Connection._recv/_send/_close capture
os.read/os.write/os.close as DEFAULT ARGUMENTS at import time

    _read = os.read
    def _recv(self, size, read=_read): ...

so if multiprocessing was imported before patch() those defaults are the
original *blocking* os.read/os.write (and an os.close that doesn't clear the
netpoll registration -> fd-reuse hangs).  The "multiprocessing" patch rebinds
them to the cooperative versions.  This file imports multiprocessing at the top
(before setUpModule patches) on purpose, to exercise that import-before-patch
fix.

Coverage is intentionally IN-PROCESS -- a Pipe's two ends used by two
fibers -- because that isolates exactly what runloom is responsible for (a
blocked recv parks on the cooperative os.read and yields), with no fork.

CAVEAT (not tested here): cross-process multiprocessing works, but only with
the "forkserver" or "spawn" start methods.  The "fork" start method inherits
runloom's background threads, and a fork of a multi-threaded process can deadlock
the child (Python warns: "use of fork() may lead to deadlocks in the child").
Single short-lived forks usually survive, but a long-lived runloom process doing
several fork-based multiprocessing operations reliably wedges.  Use forkserver
or spawn under runloom.

Adapted from CPython Lib/test/_test_multiprocessing (_TestConnection) and the
pipe round-trip patterns in libuv test/test-pipe-*.c.
"""
import multiprocessing            # imported before patch() on purpose
import multiprocessing.connection
import os
import platform
import unittest

import runloom
import runloom.monkey
import runloom_c

_IS_WINDOWS = platform.system() == "Windows"
_Connection = multiprocessing.connection.Connection


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    runloom_c.fiber(runner)
    runloom_c.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    runloom.monkey.patch()


def tearDownModule():
    runloom.monkey.unpatch()


@unittest.skipIf(_IS_WINDOWS, "POSIX Connection (os.read) path")
class TestDefaultArgRebind(unittest.TestCase):
    """The import-before-patch fix: Connection._recv/_send/_close must end up
    bound to the cooperative os.read/os.write/os.close after patch()."""

    def test_recv_default_is_cooperative_os_read(self):
        recv = _Connection.__dict__.get("_recv")
        self.assertIsNotNone(recv)
        self.assertIs(recv.__defaults__[0], os.read)

    def test_send_default_is_cooperative_os_write(self):
        send = _Connection.__dict__.get("_send")
        self.assertIsNotNone(send)
        self.assertIs(send.__defaults__[0], os.write)

    def test_close_default_is_cooperative_os_close(self):
        close = _Connection.__dict__.get("_close")
        self.assertIsNotNone(close)
        # os.close here is the patched one that clears the netpoll bit.
        self.assertIs(close.__defaults__[0], os.close)


@unittest.skipIf(_IS_WINDOWS, "POSIX Connection (os.read) path")
class TestConnectionInProcess(unittest.TestCase):
    """A Pipe's two ends, driven by two fibers in one process.  Exercises
    the real Connection.recv/send/poll code path with no fork."""

    def test_send_recv_roundtrip(self):
        def body():
            a, b = multiprocessing.Pipe()
            a.send(("hello", [1, 2, 3]))
            got = b.recv()
            a.close(); b.close()
            return got
        self.assertEqual(_drive(body), ("hello", [1, 2, 3]))

    def test_recv_blocks_then_yields(self):
        """A fiber blocked in Connection.recv must let a sibling run while
        another fiber prepares the message -- proves recv parks on the
        cooperative os.read, not a blocking read that freezes the scheduler."""
        def body():
            a, b = multiprocessing.Pipe()
            ticks, stop = [], {"v": False}

            def ticker():
                while not stop["v"]:
                    ticks.append(1)
                    runloom.sleep(0.003)

            def sender():
                for _ in range(8):
                    runloom.sleep(0.004)       # ~32 ms before the message lands
                b.send(("payload", 99))

            runloom_c.fiber(ticker)
            runloom_c.fiber(sender)
            got = a.recv()                  # blocks until the sender sends
            stop["v"] = True
            a.close(); b.close()
            return got, len(ticks)
        got, ticks = _drive(body)
        self.assertEqual(got, ("payload", 99))
        self.assertGreaterEqual(ticks, 1)

    def test_poll_timeout_then_ready(self):
        def body():
            a, b = multiprocessing.Pipe()
            before = a.poll(0.02)           # nothing pending -> False (coop wait)
            b.send("x")
            after = a.poll(1.0)             # ready -> True
            val = a.recv()
            a.close(); b.close()
            return before, after, val
        self.assertEqual(_drive(body), (False, True, "x"))

    def test_recv_eof_raises(self):
        """recv() on a pipe whose write end is closed must raise EOFError, not
        hang."""
        def body():
            a, b = multiprocessing.Pipe()
            b.close()
            raised = False
            try:
                a.recv()
            except EOFError:
                raised = True
            a.close()
            return raised
        self.assertTrue(_drive(body))

    def test_send_bytes_recv_bytes(self):
        def body():
            a, b = multiprocessing.Pipe()
            payload = b"\x00\x01\x02" * 10000     # bigger than a pipe buffer
            ticks, stop = [], {"v": False}

            def ticker():
                while not stop["v"]:
                    ticks.append(1)
                    runloom.sleep(0.003)

            def reader(out):
                out.append(b.recv_bytes())

            out = []
            runloom_c.fiber(ticker)
            runloom_c.fiber(lambda: reader(out))
            a.send_bytes(payload)
            # let the reader drain
            import time
            t0 = time.monotonic()
            while not out and time.monotonic() - t0 < 5:
                runloom.sleep(0.003)
            stop["v"] = True
            a.close(); b.close()
            return (out[0] if out else None), len(ticks)
        data, ticks = _drive(body)
        self.assertEqual(data, b"\x00\x01\x02" * 10000)


@unittest.skipIf(_IS_WINDOWS, "POSIX SemLock path")
class TestSyncPrimitives(unittest.TestCase):
    """multiprocessing.Lock/Semaphore are POSIX semaphores (sem_wait blocks the
    OS thread).  The cooperative SemLock.acquire does sem_trywait + backoff, so
    a contended acquire yields.  A process-shared semaphore contends within one
    process too, so this is exercised in-process (no fork).  Adapted from
    CPython Lib/test/_test_multiprocessing (_TestLock / _TestSemaphore).
    """

    def test_lock_acquire_is_cooperative(self):
        self.assertIn("_co_semlock_acquire",
                      multiprocessing.Lock().acquire.__qualname__)

    def test_lock_mutual_exclusion_and_yield(self):
        def body():
            lock = multiprocessing.Lock()
            order, ticks, stop = [], [], {"v": False}

            def holder():
                lock.acquire()
                order.append("A-lock")
                for _ in range(6):
                    runloom.sleep(0.004)
                order.append("A-unlock")
                lock.release()

            def waiter():
                runloom.sleep(0.002)
                lock.acquire()            # blocks until holder releases
                order.append("B-lock")
                lock.release()

            def ticker():
                while "B-lock" not in order:
                    ticks.append(1)
                    runloom.sleep(0.003)

            runloom_c.fiber(holder)
            runloom_c.fiber(waiter)
            runloom_c.fiber(ticker)
            import time
            t0 = time.monotonic()
            while "B-lock" not in order and time.monotonic() - t0 < 5:
                runloom.sleep(0.005)
            return order, len(ticks)
        order, ticks = _drive(body)
        self.assertIn("B-lock", order)
        # B got the lock only after A released it (real mutual exclusion)
        self.assertLess(order.index("A-unlock"), order.index("B-lock"))
        # ... and the scheduler kept running while B was blocked
        self.assertGreaterEqual(ticks, 1)

    def test_semaphore_nonblocking_passthrough(self):
        def body():
            s = multiprocessing.Semaphore(1)
            return s.acquire(), s.acquire(False)   # True, then False (empty)
        first, second = _drive(body)
        self.assertTrue(first)
        self.assertFalse(second)

    def test_bounded_semaphore_count(self):
        def body():
            s = multiprocessing.BoundedSemaphore(2)
            got = [s.acquire(False), s.acquire(False), s.acquire(False)]
            s.release(); s.release()
            return got
        self.assertEqual(_drive(body), [True, True, False])


if __name__ == "__main__":
    unittest.main()
