"""Errno fault-injection for the cooperative blocking shims.

The shims have error paths the "result is correct" tests never exercise: EINTR
retry, contention retry, the pidfd-open ESRCH race -> busy-poll fallback, and
real-error propagation.  Here we inject those at the shim's captured original
(e.g. pygo.monkey._raw_os_sendfile / _orig_flock / os.pidfd_open) and assert
the wrapper retries, falls back, or raises -- and never hangs (a hang shows up
as a run_isolated TIMEOUT).

Mirrors the project's *_faultinject suites (netpoll/tcp/iouring) but at the
Python shim layer the C harness doesn't reach.
"""
import contextlib
import errno
import os
import socket
import tempfile
import time
import unittest

import pygo
import pygo.monkey
import pygo_core

_HAVE_FORK = hasattr(os, "fork")
_HAVE_PIDFD = hasattr(os, "pidfd_open")
try:
    import fcntl
except ImportError:
    fcntl = None


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


@contextlib.contextmanager
def inject(holder, attr, exc_factory, n=1):
    """Replace holder.attr with a stub that raises exc_factory() for the first
    n calls, then delegates to the real callable.  Restores on exit."""
    real = getattr(holder, attr)
    state = {"n": 0}

    def stub(*a, **k):
        if state["n"] < n:
            state["n"] += 1
            raise exc_factory()
        return real(*a, **k)

    setattr(holder, attr, stub)
    try:
        yield state
    finally:
        setattr(holder, attr, real)


def _oserr(num):
    return lambda: OSError(num, os.strerror(num))


@unittest.skipIf(fcntl is None or not hasattr(fcntl, "flock"), "no flock")
class TestFlockFaults(unittest.TestCase):
    def setUp(self):
        fd, self.path = tempfile.mkstemp(prefix="pygo_fi_")
        os.close(fd)

    def tearDown(self):
        os.unlink(self.path)

    def test_eintr_is_retried(self):
        def body():
            fd = os.open(self.path, os.O_RDWR)
            fd2 = os.open(self.path, os.O_RDWR)
            try:
                with inject(pygo.monkey.files, "_orig_flock", InterruptedError, n=1):
                    fcntl.flock(fd, fcntl.LOCK_EX)   # EINTR once -> retried
                # Verify the lock is ACTUALLY held (not silently dropped on the
                # EINTR): a second open description must fail to take it.
                held = False
                try:
                    fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(fd2, fcntl.LOCK_UN)
                except BlockingIOError:
                    held = True
                fcntl.flock(fd, fcntl.LOCK_UN)
                return held
            finally:
                os.close(fd); os.close(fd2)
        self.assertTrue(_drive(body))

    def test_contention_errno_then_acquire(self):
        """A few EWOULDBLOCK then success: the backoff loop must keep trying."""
        def body():
            fd = os.open(self.path, os.O_RDWR)
            try:
                with inject(pygo.monkey.files, "_orig_flock",
                            _oserr(errno.EWOULDBLOCK), n=3) as st:
                    fcntl.flock(fd, fcntl.LOCK_EX)
                fcntl.flock(fd, fcntl.LOCK_UN)
                return st["n"]
            finally:
                os.close(fd)
        self.assertEqual(_drive(body), 3)   # all 3 contended tries consumed

    def test_real_error_propagates(self):
        def body():
            fd = os.open(self.path, os.O_RDWR)
            try:
                with inject(pygo.monkey.files, "_orig_flock", _oserr(errno.EBADF), n=99):
                    with self.assertRaises(OSError) as cm:
                        fcntl.flock(fd, fcntl.LOCK_EX)
                    return cm.exception.errno
            finally:
                os.close(fd)
        self.assertEqual(_drive(body), errno.EBADF)


class TestReadvFaults(unittest.TestCase):
    @unittest.skipUnless(hasattr(os, "readv"), "no os.readv")
    def test_eintr_is_retried(self):
        def body():
            r, w = os.pipe()
            try:
                os.set_blocking(r, False); os.set_blocking(w, False)
                os.write(w, b"abcd")
                with inject(pygo.monkey.osio, "_orig_os_readv", InterruptedError, n=1):
                    bufs = [bytearray(2), bytearray(2)]
                    n = os.readv(r, bufs)
                return n, bytes(bufs[0]) + bytes(bufs[1])
            finally:
                os.close(r); os.close(w)
        n, data = _drive(body)
        self.assertEqual((n, data), (4, b"abcd"))


@unittest.skipUnless(_HAVE_FORK, "no os.fork")
class TestPidfdFaults(unittest.TestCase):
    @unittest.skipUnless(_HAVE_PIDFD, "no pidfd_open")
    def test_pidfd_open_esrch_falls_back_to_poll(self):
        """If pidfd_open races a reap (ESRCH), _pidfd_open returns None and the
        wait must fall back to the WNOHANG busy-poll and still reap correctly."""
        def body():
            pid = os.fork()
            if pid == 0:
                time.sleep(0.03)
                os._exit(17)
            with inject(os, "pidfd_open", _oserr(errno.ESRCH), n=99):
                wpid, status = os.waitpid(pid, 0)   # no pidfd -> poll path
            return wpid == pid, os.WEXITSTATUS(status)
        ok, code = _drive(body)
        self.assertTrue(ok)
        self.assertEqual(code, 17)

    @unittest.skipUnless(_HAVE_PIDFD, "no pidfd_open")
    def test_pidfd_open_einval_falls_back(self):
        def body():
            pid = os.fork()
            if pid == 0:
                os._exit(5)
            with inject(os, "pidfd_open", _oserr(errno.EINVAL), n=99):
                wpid, status = os.waitpid(pid, 0)
            return wpid == pid, os.WEXITSTATUS(status)
        ok, code = _drive(body)
        self.assertTrue(ok)
        self.assertEqual(code, 5)


# NOTE: sigwait/sigtimedwait EINTR fault-injection lives in
# test_signal_compat.py -- a forced SIGUSR1 is unreliable once this file's
# fork/pool tests have spun up a background thread that can catch it (the
# signal terminates the process instead of staying pending for sigwait).


class TestSendfileFaults(unittest.TestCase):
    def _server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0)); srv.listen(8)
        return srv, srv.getsockname()

    @unittest.skipUnless(hasattr(os, "sendfile"), "no os.sendfile")
    def test_sendfile_eagain_then_completes(self):
        """os.sendfile raising EAGAIN must park on wait_fd and resume, not fail."""
        data = bytes(range(256)) * 1024
        fd, path = tempfile.mkstemp(prefix="pygo_fi_sf_")
        os.write(fd, data); os.close(fd)

        def body():
            srv, addr = self._server()
            got = {"buf": b""}

            def server():
                conn, _ = srv.accept()
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    got["buf"] += chunk
                conn.close()

            pygo_core.go(server)
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect(addr)
            with inject(pygo.monkey.sockets, "_raw_os_sendfile",
                        lambda: BlockingIOError(errno.EAGAIN, "EAGAIN"), n=2):
                with open(path, "rb") as f:
                    sent = cli.sendfile(f)
            cli.shutdown(socket.SHUT_WR)
            t0 = time.monotonic()
            while len(got["buf"]) < len(data) and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            cli.close(); srv.close()
            return sent, got["buf"]
        try:
            sent, buf = _drive(body)
        finally:
            os.unlink(path)
        self.assertEqual(sent, len(data))
        self.assertEqual(buf, data)


if __name__ == "__main__":
    unittest.main()
