"""Resource-balance for the blocking shims that manage fds/parkers.

The cooperative shims open and close things on every call -- pidfds
(os.pidfd_open/os.close), self-pipe parkers (offload / the backend pool),
sockets and temp fds.  A miss (a pidfd never closed, a parker never recycled)
is silent and accumulates until the process runs out of fds.  These tests run
each op in a tight loop and assert the fd count + the parker pool return to
baseline -- i.e. balanced open/close, no per-iteration growth.

Companion to the project's test_monkey_leak.py, scoped to the new ops.
"""
import os
import time
import unittest

import pygo
import pygo.monkey
import pygo_core

_HAVE_FORK = hasattr(os, "fork")


def _fd_count():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None        # non-Linux: caller skips


_HAVE_PROCFD = _fd_count() is not None


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


@unittest.skipUnless(_HAVE_PROCFD, "needs /proc/self/fd")
class TestOffloadBalance(unittest.TestCase):
    def test_offload_no_fd_or_parker_leak(self):
        def body():
            # Warm up: prime the backend pool + the parker free-list so the
            # measured window only sees steady-state churn.
            for _ in range(25):
                pygo.monkey.offload(lambda: 1)
            base_fd = _fd_count()
            for _ in range(500):
                pygo.monkey.offload(lambda: 1)
            return base_fd, _fd_count(), len(pygo.monkey._Parker._pool)
        base_fd, after_fd, pool = _drive(body)
        self.assertLessEqual(after_fd - base_fd, 4)   # no per-iter fd growth
        self.assertLessEqual(pool, 64)                # parker free-list is capped


@unittest.skipUnless(_HAVE_PROCFD, "needs /proc/self/fd")
@unittest.skipUnless(_HAVE_FORK, "no os.fork")
class TestPidfdBalance(unittest.TestCase):
    def test_waitpid_balanced(self):
        """Each waitpid opens a pidfd and must close it -- fd count stays flat
        across many reaps."""
        def reap():
            pid = os.fork()
            if pid == 0:
                os._exit(0)
            os.waitpid(pid, 0)

        def body():
            for _ in range(5):       # warmup
                reap()
            base = _fd_count()
            for _ in range(40):
                reap()
            return base, _fd_count()
        base, after = _drive(body)
        self.assertLessEqual(after - base, 4)


@unittest.skipUnless(_HAVE_PROCFD, "needs /proc/self/fd")
class TestSocketBalance(unittest.TestCase):
    def test_socketpair_recv_balanced(self):
        import socket
        def body():
            def once():
                a, b = socket.socketpair()
                a.setblocking(False); b.setblocking(False)
                b.send(b"ping")
                a.recv(16)
                a.close(); b.close()
            for _ in range(10):
                once()
            base = _fd_count()
            for _ in range(300):
                once()
            return base, _fd_count()
        base, after = _drive(body)
        self.assertLessEqual(after - base, 4)


@unittest.skipUnless(_HAVE_PROCFD, "needs /proc/self/fd")
class TestSyscallBalance(unittest.TestCase):
    def test_readv_pipe_balanced(self):
        def body():
            def once():
                r, w = os.pipe()
                os.set_blocking(r, False); os.set_blocking(w, False)
                os.write(w, b"abcd")
                os.readv(r, [bytearray(2), bytearray(2)])
                os.close(r); os.close(w)
            for _ in range(10):
                once()
            base = _fd_count()
            for _ in range(300):
                once()
            return base, _fd_count()
        base, after = _drive(body)
        self.assertLessEqual(after - base, 4)


if __name__ == "__main__":
    unittest.main()
