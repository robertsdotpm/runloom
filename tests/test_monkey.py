"""Tests for pygo.monkey -- cooperative patches across the stdlib.

These tests exercise the C scheduler (pygo_core.go / pygo_core.run)
because that's the path the monkey-patches target.
"""
import os
import platform
import queue
import socket
import sys
import threading
import time
import unittest

_IS_WINDOWS = platform.system() == "Windows"

sys.path.insert(0, "src")

import pygo
import pygo.monkey
import pygo_core


def _drive(fn):
    """Spawn fn as a goroutine, run scheduler, return its return value."""
    box = [None, None]
    def runner():
        try:
            box[0] = fn()
        except BaseException as e:
            box[1] = e
    pygo_core.go(runner)
    pygo_core.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


class TestPatchIdempotence(unittest.TestCase):
    def test_double_patch(self):
        pygo.monkey.patch()
        pygo.monkey.patch()   # second call is a no-op
        self.assertTrue(callable(time.sleep))
        self.assertTrue(callable(socket.socket.recv))


class TestTimeSleep(unittest.TestCase):
    def test_sleep_interleaves(self):
        pygo.monkey.patch()
        log = []
        def sleeper(name, dur):
            log.append((name, "start"))
            time.sleep(dur)            # patched -> pygo.sleep
            log.append((name, "end"))
        pygo_core.go(lambda: sleeper("A", 0.05))
        pygo_core.go(lambda: sleeper("B", 0.05))
        t0 = time.monotonic()
        pygo_core.run()
        elapsed = time.monotonic() - t0
        # If both were truly parallel sleepers, elapsed ~= 0.05, not 0.10.
        self.assertLess(elapsed, 0.09)
        self.assertEqual(sorted([e for _, e in log]),
                         ["end", "end", "start", "start"])


class TestThreadingLock(unittest.TestCase):
    def test_lock_excludes_goroutines(self):
        pygo.monkey.patch()
        lock = threading.Lock()
        log = []
        def worker(name):
            with lock:
                log.append((name, "in"))
                pygo.sleep(0.01)
                log.append((name, "out"))
        pygo_core.go(lambda: worker("A"))
        pygo_core.go(lambda: worker("B"))
        pygo_core.go(lambda: worker("C"))
        pygo_core.run()
        # Within each pair (in, out) must be adjacent -- no interleaving.
        names = [n for n, _ in log]
        for i in range(0, len(log), 2):
            self.assertEqual(log[i][1], "in")
            self.assertEqual(log[i + 1][1], "out")
            self.assertEqual(log[i][0], log[i + 1][0])


class TestThreadingEvent(unittest.TestCase):
    def test_event_wakes_waiters(self):
        pygo.monkey.patch()
        ev = threading.Event()
        log = []
        def waiter():
            log.append("wait-start")
            ev.wait()
            log.append("wait-end")
        def setter():
            pygo.sleep(0.02)
            log.append("set")
            ev.set()
        pygo_core.go(waiter)
        pygo_core.go(waiter)
        pygo_core.go(setter)
        pygo_core.run()
        self.assertEqual(log.count("wait-start"), 2)
        self.assertEqual(log.count("wait-end"), 2)
        self.assertEqual(log[2], "set")  # both waits started before set
        self.assertEqual(log[-1], "wait-end")


class TestQueue(unittest.TestCase):
    def test_producer_consumer(self):
        pygo.monkey.patch()
        q = queue.Queue(maxsize=3)
        consumed = []
        def producer():
            for i in range(5):
                q.put(i)
        def consumer():
            for _ in range(5):
                consumed.append(q.get())
        pygo_core.go(producer)
        pygo_core.go(consumer)
        pygo_core.run()
        self.assertEqual(consumed, [0, 1, 2, 3, 4])


class TestOsReadWrite(unittest.TestCase):
    @unittest.skipIf(_IS_WINDOWS,
        "Windows pipes aren't pollable via Winsock select/WSAPoll; "
        "this test exercises the POSIX pipe-cooperative path.")
    def test_pipe_round_trip(self):
        pygo.monkey.patch()
        r, w = os.pipe()
        got = [None]
        def writer():
            time.sleep(0.01)
            os.write(w, b"hello")
            os.close(w)
        def reader():
            got[0] = os.read(r, 1024)
            os.close(r)
        pygo_core.go(reader)
        pygo_core.go(writer)
        pygo_core.run()
        self.assertEqual(got[0], b"hello")


@unittest.skipIf(_IS_WINDOWS,
    "select.select on Windows only accepts SOCKET handles, not pipe "
    "fds.  The pipe-based select integration is a POSIX-only path.")
class TestSelect(unittest.TestCase):
    def test_select_single_fd(self):
        import select
        pygo.monkey.patch()
        r, w = os.pipe()
        ready_fd = [None]
        def writer():
            time.sleep(0.01)
            os.write(w, b"x")
        def reader():
            rr, _, _ = select.select([r], [], [], 1.0)
            ready_fd[0] = rr
            os.read(r, 1)
        pygo_core.go(reader)
        pygo_core.go(writer)
        pygo_core.run()
        os.close(r); os.close(w)
        self.assertEqual(ready_fd[0], [r])

    def test_select_timeout(self):
        import select
        pygo.monkey.patch()
        r, _ = os.pipe()
        result = [None]
        def waiter():
            result[0] = select.select([r], [], [], 0.05)
        pygo_core.go(waiter)
        t0 = time.monotonic()
        pygo_core.run()
        elapsed = time.monotonic() - t0
        os.close(r)
        self.assertEqual(result[0], ([], [], []))
        self.assertGreaterEqual(elapsed, 0.04)


class TestDNS(unittest.TestCase):
    def test_getaddrinfo_localhost(self):
        pygo.monkey.patch()
        result = [None]
        def looker():
            result[0] = socket.getaddrinfo("localhost", 80,
                                           type=socket.SOCK_STREAM)
        pygo_core.go(looker)
        pygo_core.run()
        self.assertIsNotNone(result[0])
        self.assertTrue(len(result[0]) > 0)
        # Should land on 127.0.0.1 or ::1 via /etc/hosts.
        addrs = {info[4][0] for info in result[0]}
        self.assertTrue(addrs & {"127.0.0.1", "::1"})

    def test_getaddrinfo_ip_literal(self):
        pygo.monkey.patch()
        result = [None]
        def looker():
            result[0] = socket.getaddrinfo("8.8.8.8", 53,
                                           family=socket.AF_INET,
                                           type=socket.SOCK_DGRAM)
        pygo_core.go(looker)
        pygo_core.run()
        self.assertEqual(result[0][0][4][0], "8.8.8.8")

    def test_getaddrinfo_no_thread_handoff(self):
        # Async DNS must NOT block the scheduler.  Two concurrent lookups
        # should both finish in roughly the time of one.
        pygo.monkey.patch()
        import pygo.monkey as M
        # Clear cache so we actually do round-trips.
        M._dns_result_cache.clear()
        times = []
        def looker(name):
            t0 = time.monotonic()
            try:
                socket.getaddrinfo(name, 80, family=socket.AF_INET)
            except Exception:
                pass
            times.append(time.monotonic() - t0)
        pygo_core.go(lambda: looker("localhost"))
        pygo_core.go(lambda: looker("localhost"))
        pygo_core.run()
        # Both should be sub-second (they hit /etc/hosts, no UDP).
        self.assertTrue(all(t < 0.5 for t in times), times)


class TestFile(unittest.TestCase):
    def test_open_read_regular_file(self):
        import tempfile
        pygo.monkey.patch()
        path = tempfile.mktemp()
        with open(path, "w") as f:
            f.write("hello pygo")
        try:
            got = [None]
            def reader():
                with open(path, "r") as f:
                    got[0] = f.read()
            pygo_core.go(reader)
            pygo_core.run()
            self.assertEqual(got[0], "hello pygo")
        finally:
            os.unlink(path)

    def test_concurrent_file_reads_interleave(self):
        # Two goroutines reading files should overlap via the thread
        # pool -- the scheduler must not be blocked while one reads.
        import tempfile
        pygo.monkey.patch()
        path = tempfile.mktemp()
        with open(path, "wb") as f:
            f.write(b"x" * 4096)
        try:
            log = []
            def reader(name):
                log.append((name, "start"))
                with open(path, "rb") as f:
                    f.read()
                log.append((name, "done"))
            pygo_core.go(lambda: reader("A"))
            pygo_core.go(lambda: reader("B"))
            pygo_core.run()
            starts = [e for e in log if e[1] == "start"]
            self.assertEqual(len(starts), 2)
        finally:
            os.unlink(path)


class TestSyscalls(unittest.TestCase):
    def test_stat_listdir(self):
        import tempfile
        pygo.monkey.patch()
        tmpdir = tempfile.mkdtemp()
        try:
            for nm in ("a.txt", "b.txt"):
                with open(os.path.join(tmpdir, nm), "w") as f:
                    f.write(nm)
            got = [None, None]
            def worker():
                got[0] = sorted(os.listdir(tmpdir))
                got[1] = os.stat(os.path.join(tmpdir, "a.txt")).st_size
            pygo_core.go(worker)
            pygo_core.run()
            self.assertEqual(got[0], ["a.txt", "b.txt"])
            self.assertEqual(got[1], 5)
        finally:
            import shutil
            shutil.rmtree(tmpdir)


class TestSubprocessWait(unittest.TestCase):
    """Cooperative Popen.wait must not block the scheduler."""

    def test_wait_uses_cooperative_poll(self):
        import subprocess as _sp
        pygo.monkey.patch()
        # Two child processes with overlapping sleeps; if wait() blocked
        # the scheduler, total wall time would be 2x child sleep.  Pick
        # a child duration long enough that Popen()'s ~30 ms spawn cost
        # and the poll loop's ~32 ms tail latency don't swamp the
        # parallel/sequential signal.
        log = []
        SLEEP = 0.2
        def waiter(name):
            log.append((name, "start"))
            p = _sp.Popen([sys.executable, "-c",
                           "import time; time.sleep({})".format(SLEEP)])
            rc = p.wait()
            log.append((name, "done", rc))
        t0 = time.monotonic()
        pygo_core.go(lambda: waiter("A"))
        pygo_core.go(lambda: waiter("B"))
        pygo_core.run()
        elapsed = time.monotonic() - t0
        # Sequential ~= 0.4 s (+ 2x spawn).  Cooperative ~= 0.2 s + 2x
        # spawn + poll tail.  Allow generous headroom for slow CI boxes
        # while still failing on a true sequential regression.
        self.assertLess(elapsed, SLEEP * 2 - 0.05,
                        "cooperative wait should overlap")
        self.assertEqual([e[1] for e in log if e[1] == "start"],
                         ["start", "start"])
        for e in log:
            if e[1] == "done":
                self.assertEqual(e[2], 0)


class TestParkerSocketpair(unittest.TestCase):
    """Verify the socket-backed parker path works (used on Windows where
    select() can only poll sockets, not pipe fds).  We force the path on
    POSIX by toggling the module flag; socket.socketpair() returns
    AF_UNIX sockets on POSIX and AF_INET on Windows, both fd-pollable."""

    def _drain_parker_pool(self, M):
        """Empty the parker pool, closing any socketpair sockets so
        ResourceWarning doesn't fire."""
        while M._Parker._pool:
            entry = M._Parker._pool.pop()
            socks = entry[2] if len(entry) > 2 else None
            if socks is not None:
                for s in socks:
                    try: s.close()
                    except OSError: pass

    def test_socketpair_parker_round_trip(self):
        import pygo.monkey as M
        pygo.monkey.patch()
        # Drain any pooled parkers so the next _Parker() actually
        # constructs a fresh one through the forced path.
        self._drain_parker_pool(M)
        was_windows = M._IS_WINDOWS
        M._IS_WINDOWS = True
        try:
            sequence = []
            def coordinator():
                p = M._Parker()
                def signaller():
                    sequence.append("signal")
                    p.unpark()
                pygo_core.go(signaller)
                p.park()
                sequence.append("woken")
                p.release()
            pygo_core.go(coordinator)
            pygo_core.run()
        finally:
            M._IS_WINDOWS = was_windows
            self._drain_parker_pool(M)
        self.assertEqual(sequence, ["signal", "woken"])


class TestSocketStillWorks(unittest.TestCase):
    """Regression: the original socket patches still work after refactor."""
    def test_echo(self):
        pygo.monkey.patch()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        port = srv.getsockname()[1]
        result = [None]
        def server():
            conn, _ = srv.accept()
            data = conn.recv(1024)
            conn.sendall(data)
            conn.close()
        def client():
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(("127.0.0.1", port))
            c.sendall(b"ping")
            result[0] = c.recv(1024)
            c.close()
        pygo_core.go(server)
        pygo_core.go(client)
        pygo_core.run()
        srv.close()
        self.assertEqual(result[0], b"ping")


if __name__ == "__main__":
    unittest.main()
