"""Tests for runloom_c.TCPConn: connect/listen/accept/recv/send/close."""
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, "src")

import runloom
import runloom_c


def _bound_port(listener):
    """Read the bound port back via a dup'd socket."""
    fd = listener.fileno()
    sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]
    sk.close()
    return port


def _drive(*fibers):
    """Spawn each callable as a fiber, run scheduler."""
    box = []
    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:
                box.append(e)
        return runner
    for g in fibers:
        runloom_c.fiber(wrap(g))
    runloom_c.run()
    if box:
        raise box[0]


class TestBasicEcho(unittest.TestCase):
    """The canonical client/server echo through TCPConn."""

    def test_echo_round_trip(self):
        port_holder = [None]
        result = [None]

        def server():
            listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            data = conn.recv(1024)
            conn.send_all(data)
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                runloom_c.sched_yield()
            c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
            c.send_all(b"ping")
            result[0] = c.recv(1024)
            c.close()

        _drive(server, client)
        self.assertEqual(result[0], b"ping")


class TestRecvInto(unittest.TestCase):
    """recv_into avoids the bytes-object allocation; verify the same
    data lands in the caller's buffer."""

    def test_recv_into(self):
        port_holder = [None]
        result = [None]

        def server():
            listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            conn.send_all(b"abcdef")
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                runloom_c.sched_yield()
            c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
            buf = bytearray(16)
            n = c.recv_into(buf)
            result[0] = (n, bytes(buf[:n]))
            c.close()

        _drive(server, client)
        n, data = result[0]
        self.assertEqual(n, 6)
        self.assertEqual(data, b"abcdef")


class TestRecvIntoSizeLimit(unittest.TestCase):
    """recv_into(buf, n) caps the read at n bytes even if buf is larger."""

    def test_size_limit(self):
        port_holder = [None]
        result = [None]

        def server():
            listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            conn.send_all(b"abcdef")
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                runloom_c.sched_yield()
            c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
            buf = bytearray(16)
            n = c.recv_into(buf, 3)
            result[0] = (n, bytes(buf[:n]))
            # remaining 3 bytes should still arrive
            n2 = c.recv_into(buf)
            result.append((n2, bytes(buf[:n2])))
            c.close()

        _drive(server, client)
        self.assertEqual(result[0], (3, b"abc"))
        self.assertEqual(result[1], (3, b"def"))


class TestOrderlyShutdown(unittest.TestCase):
    """recv on a closed peer returns b'' (or 0 for recv_into)."""

    def test_eof_recv(self):
        port_holder = [None]
        got = [None]

        def server():
            listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                runloom_c.sched_yield()
            c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
            got[0] = c.recv(64)
            c.close()

        _drive(server, client)
        self.assertEqual(got[0], b"")

    def test_eof_recv_into(self):
        port_holder = [None]
        got = [None]

        def server():
            listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                runloom_c.sched_yield()
            c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
            buf = bytearray(64)
            got[0] = c.recv_into(buf)
            c.close()

        _drive(server, client)
        self.assertEqual(got[0], 0)


class TestCloseSemantics(unittest.TestCase):
    """close() is idempotent; .closed flips True; fileno reports -1."""

    def test_close_idempotent(self):
        seen = []

        def coro():
            c = runloom_c.TCPConn.listen("127.0.0.1", 0)
            seen.append(("before", c.closed, c.fileno() >= 0))
            c.close()
            seen.append(("after", c.closed, c.fileno()))
            c.close()  # second close is no-op
            seen.append(("after2", c.closed, c.fileno()))

        _drive(coro)
        self.assertEqual(seen[0][1:], (False, True))
        self.assertEqual(seen[1][1:], (True, -1))
        self.assertEqual(seen[2][1:], (True, -1))


class TestRecvBlocksUntilData(unittest.TestCase):
    """A TCPConn.recv parks the fiber and resumes when data arrives.
    Concurrent fibers making progress prove the recv didn't busy-wait."""

    def test_recv_yields(self):
        port_holder = [None]
        ticks = [0]
        result = [None]

        def server():
            listener = runloom_c.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            # Sleep briefly so client.recv has to actually park.
            runloom_c.sched_sleep(0.05)
            conn.send_all(b"slow data")
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                runloom_c.sched_yield()
            c = runloom_c.TCPConn.connect("127.0.0.1", port_holder[0])
            result[0] = c.recv(64)
            c.close()

        def ticker():
            # If the scheduler is yielding cooperatively while client.recv
            # is parked, this ticker should run many times.
            for _ in range(20):
                ticks[0] += 1
                runloom_c.sched_sleep(0.005)

        _drive(server, client, ticker)
        self.assertEqual(result[0], b"slow data")
        # Ticker had ~50ms in which to run while client.recv was parked.
        self.assertGreaterEqual(ticks[0], 5)


class TestInteropWithStdlibSocket(unittest.TestCase):
    """TCPConn.connect talks to a stdlib socket listener and vice versa.
    Validates the wire format and getaddrinfo path are compatible."""

    def test_connect_to_stdlib_server(self):
        # Use a stdlib socket on a thread for the server side -- proves
        # TCPConn doesn't depend on the other side being a TCPConn.
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        got = [None]

        def stdlib_server():
            conn, _ = srv.accept()
            conn.sendall(b"hello stdlib")
            data = b""
            while True:
                chunk = conn.recv(1024)
                if not chunk: break
                data += chunk
            conn.close()
            got[0] = data

        th = threading.Thread(target=stdlib_server)
        th.start()

        def client():
            c = runloom_c.TCPConn.connect("127.0.0.1", port)
            data = c.recv(1024)
            c.send_all(b"back from tcpconn")
            c.close()
            got.append(data)

        _drive(client)
        th.join(timeout=5)
        srv.close()
        self.assertEqual(got[0], b"back from tcpconn")
        self.assertEqual(got[1], b"hello stdlib")


if __name__ == "__main__":
    unittest.main()
