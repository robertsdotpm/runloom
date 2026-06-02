"""Cooperative advisory file locks: fcntl.flock / fcntl.lockf.

Adapted from CPython Lib/test/test_fcntl.py (TestFcntl.test_flock,
test_lockf_exclusive / test_lockf_share) and the lock-ownership semantics in
the flock(2) and fcntl(2) F_SETLKW man pages:

  * flock locks are tied to the *open file description*, so two independent
    open()s in the same process contend -- exercised with two goroutines;
  * POSIX fcntl/lockf locks are owned by the *process*, so contention needs a
    second process -- exercised with a forked child;
  * a blocked acquire must yield the scheduler (a sibling keeps ticking),
    never spin the whole runtime;
  * LOCK_NB must pass straight through and raise immediately (no coop loop);
  * the lock is only granted after the holder releases it.

There is no readiness fd for a file lock, so the cooperative form is a
non-blocking-acquire + backoff park (not a netpoll wait); these tests pin that
behaviour down.
"""
import os
import platform
import tempfile
import time
import unittest

import pygo
import pygo.monkey
import pygo_core

try:
    import fcntl
except ImportError:
    fcntl = None

_IS_WINDOWS = platform.system() == "Windows"


def _drive(fn):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    pygo_core.go(runner)
    pygo_core.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


def setUpModule():
    pygo.monkey.patch()


def tearDownModule():
    pygo.monkey.unpatch()


def _tmpfile():
    fd, path = tempfile.mkstemp(prefix="pygo_lock_")
    os.write(fd, b"lockfile-contents")
    os.close(fd)
    return path


@unittest.skipIf(fcntl is None, "no fcntl module")
@unittest.skipUnless(hasattr(fcntl, "flock"), "no fcntl.flock")
class TestFlock(unittest.TestCase):
    def setUp(self):
        self.path = _tmpfile()

    def tearDown(self):
        os.unlink(self.path)

    def test_flock_acquire_release(self):
        def body():
            fd = os.open(self.path, os.O_RDWR)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                fcntl.flock(fd, fcntl.LOCK_UN)
                return True
            finally:
                os.close(fd)
        self.assertTrue(_drive(body))

    def test_flock_contention_blocks_until_release_and_yields(self):
        """Two open descriptions contend.  The waiter must block until the
        holder unlocks, and a ticker goroutine must run meanwhile."""
        def body():
            order = []
            ticks = []
            fd_a = os.open(self.path, os.O_RDWR)
            fd_b = os.open(self.path, os.O_RDWR)

            def holder():
                fcntl.flock(fd_a, fcntl.LOCK_EX)
                order.append("a-locked")
                for _ in range(5):
                    pygo.sleep(0.005)       # hold, cooperatively
                order.append("a-unlock")
                fcntl.flock(fd_a, fcntl.LOCK_UN)

            def waiter():
                pygo.sleep(0.002)           # let the holder grab it first
                order.append("b-try")
                fcntl.flock(fd_b, fcntl.LOCK_EX)   # blocks until a unlocks
                order.append("b-locked")
                fcntl.flock(fd_b, fcntl.LOCK_UN)

            def ticker():
                while "b-locked" not in order:
                    ticks.append(1)
                    pygo.sleep(0.003)

            pygo_core.go(holder)
            pygo_core.go(waiter)
            pygo_core.go(ticker)
            t0 = time.monotonic()
            while "b-locked" not in order and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            os.close(fd_a); os.close(fd_b)
            return order, len(ticks)

        order, ticks = _drive(body)
        self.assertIn("b-locked", order)
        # the waiter could not take the lock until the holder released it
        self.assertLess(order.index("a-unlock"), order.index("b-locked"))
        # ... and the runtime kept scheduling while the waiter was blocked
        self.assertGreaterEqual(ticks, 1)

    def test_flock_nb_passthrough_raises(self):
        """LOCK_NB must not enter the cooperative loop -- it raises at once."""
        def body():
            fd_a = os.open(self.path, os.O_RDWR)
            fd_b = os.open(self.path, os.O_RDWR)
            fcntl.flock(fd_a, fcntl.LOCK_EX)
            raised = False
            try:
                fcntl.flock(fd_b, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raised = True
            fcntl.flock(fd_a, fcntl.LOCK_UN)
            os.close(fd_a); os.close(fd_b)
            return raised
        self.assertTrue(_drive(body))


@unittest.skipIf(fcntl is None, "no fcntl module")
@unittest.skipUnless(hasattr(fcntl, "lockf"), "no fcntl.lockf")
class TestLockf(unittest.TestCase):
    def setUp(self):
        self.path = _tmpfile()

    def tearDown(self):
        os.unlink(self.path)

    def test_lockf_acquire_release(self):
        def body():
            fd = os.open(self.path, os.O_RDWR)
            try:
                fcntl.lockf(fd, fcntl.LOCK_EX)
                fcntl.lockf(fd, fcntl.LOCK_UN)
                return True
            finally:
                os.close(fd)
        self.assertTrue(_drive(body))

    @unittest.skipUnless(hasattr(os, "fork"), "no os.fork")
    def test_lockf_cross_process_contention_yields(self):
        """POSIX locks are per-process: a forked child holds the lock, the
        parent's lockf blocks until the child exits, and a sibling goroutine
        keeps running while the parent is blocked."""
        def body():
            fd = os.open(self.path, os.O_RDWR)
            pid = os.fork()
            if pid == 0:
                cfd = os.open(self.path, os.O_RDWR)
                try:
                    fcntl.lockf(cfd, fcntl.LOCK_EX)
                    time.sleep(0.05)
                finally:
                    os._exit(0)
            time.sleep(0.02)                # let the child take the lock
            ticks = []

            def ticker():
                for _ in range(20):
                    ticks.append(1)
                    pygo.sleep(0.004)

            pygo_core.go(ticker)
            fcntl.lockf(fd, fcntl.LOCK_EX)  # blocks until the child releases
            acquired = True
            fcntl.lockf(fd, fcntl.LOCK_UN)
            os.close(fd)
            os.waitpid(pid, 0)
            return acquired, len(ticks)
        acquired, ticks = _drive(body)
        self.assertTrue(acquired)
        self.assertGreaterEqual(ticks, 1)

    def test_lockf_nb_passthrough_raises(self):
        """A second process holds the lock; LOCK_NB must raise immediately."""
        if not hasattr(os, "fork"):
            self.skipTest("no os.fork")

        def body():
            fd = os.open(self.path, os.O_RDWR)
            pid = os.fork()
            if pid == 0:
                cfd = os.open(self.path, os.O_RDWR)
                try:
                    fcntl.lockf(cfd, fcntl.LOCK_EX)
                    time.sleep(0.1)
                finally:
                    os._exit(0)
            time.sleep(0.02)
            raised = False
            try:
                fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raised = True
            os.close(fd)
            os.waitpid(pid, 0)
            return raised
        self.assertTrue(_drive(body))


if __name__ == "__main__":
    unittest.main()
