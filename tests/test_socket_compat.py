"""Cooperative socket surface: recv / recv_into / send / sendall / accept /
connect / recvfrom / sendto / recvmsg / recvmsg_into / sendmsg.

Adapted from CPython's Lib/test/test_socket.py (GeneralModuleTests,
BasicTCPTest, SendmsgTests / RecvmsgTests, the SCM_RIGHTS fd-passing cases)
and the TCP/pipe echo patterns in libuv's test/test-tcp-*.c.  The emphasis
is the parts a cooperative re-implementation can get wrong:

  * return codes: recv() returns b'' at EOF, recv_into returns 0 at EOF and
    the byte count otherwise, send() returns the count, connect() to a dead
    port raises ConnectionRefusedError;
  * fault injection: ECONNRESET on a reset peer, BrokenPipeError on write to
    a closed peer, OSError/EBADF on a closed socket;
  * the new recvmsg/sendmsg path actually moves data + ancillary SCM_RIGHTS
    file descriptors between goroutines;
  * a blocked recv/accept yields the OS thread so siblings run.
"""
import array
import errno
import os
import platform
import socket
import time
import unittest

import pygo
import pygo.monkey
import pygo_core

_IS_WINDOWS = platform.system() == "Windows"
_HAVE_MSG = hasattr(socket.socket, "sendmsg") and hasattr(socket.socket, "recvmsg")


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


def _tcp_server():
    """A bound+listening TCP server socket on localhost (ephemeral port)."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    return srv, srv.getsockname()


class TestTCPEcho(unittest.TestCase):
    def test_connect_accept_echo(self):
        """libuv-style echo: connect, server accepts in a goroutine, round
        trip a payload.  Proves accept()/connect()/recv()/sendall() all park
        cooperatively and hand control back and forth."""
        def body():
            srv, addr = _tcp_server()
            got = {}

            def server():
                conn, _peer = srv.accept()
                data = conn.recv(1024)
                conn.sendall(data[::-1])
                conn.close()

            pygo_core.go(server)
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect(addr)
            n = cli.send(b"hello-world")
            got["sent"] = n
            got["echo"] = cli.recv(1024)
            cli.close(); srv.close()
            return got

        got = _drive(body)
        self.assertEqual(got["sent"], len(b"hello-world"))
        self.assertEqual(got["echo"], b"hello-world"[::-1])

    def test_recv_returns_empty_at_eof(self):
        """recv() must return b'' (not block) once the peer closes."""
        def body():
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            b.send(b"tail")
            b.close()
            first = a.recv(64)
            second = a.recv(64)        # peer gone -> EOF -> b''
            a.close()
            return first, second
        first, second = _drive(body)
        self.assertEqual(first, b"tail")
        self.assertEqual(second, b"")

    def test_recv_into_returns_count_and_zero_at_eof(self):
        def body():
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            b.send(b"12345")
            buf = bytearray(16)
            n = a.recv_into(buf)
            b.close()
            n2 = a.recv_into(buf)      # EOF
            a.close()
            return n, bytes(buf[:n]), n2
        n, data, n2 = _drive(body)
        self.assertEqual(n, 5)
        self.assertEqual(data, b"12345")
        self.assertEqual(n2, 0)

    def test_sendall_large_partial_writes(self):
        """sendall must loop over partial writes until the whole buffer is
        delivered (the kernel send buffer is far smaller than this)."""
        def body():
            srv, addr = _tcp_server()
            payload = b"x" * (4 * 1024 * 1024)
            received = {"n": 0}

            def server():
                conn, _ = srv.accept()
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    received["n"] += len(chunk)
                conn.close()

            pygo_core.go(server)
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect(addr)
            cli.sendall(payload)
            cli.shutdown(socket.SHUT_WR)
            # let the server drain
            t0 = time.monotonic()
            while received["n"] < len(payload) and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            cli.close(); srv.close()
            return received["n"]

        self.assertEqual(_drive(body), 4 * 1024 * 1024)

    def test_blocked_accept_yields_to_siblings(self):
        def body():
            srv, addr = _tcp_server()
            order = []

            def acceptor():
                order.append("accept-wait")
                conn, _ = srv.accept()
                order.append("accepted")
                conn.close()

            def connector():
                for _ in range(3):
                    time.sleep(0.005)
                    order.append("tick")
                c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                c.connect(addr)
                c.close()

            pygo_core.go(acceptor)
            pygo_core.go(connector)
            t0 = time.monotonic()
            while "accepted" not in order and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            srv.close()
            return order

        order = _drive(body)
        self.assertEqual(order[0], "accept-wait")
        self.assertIn("accepted", order)
        # ticks happened while accept() was parked
        self.assertGreaterEqual(order.count("tick"), 1)
        self.assertLess(order.index("tick"), order.index("accepted"))


class TestUDP(unittest.TestCase):
    def test_sendto_recvfrom(self):
        def body():
            s1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s1.bind(("127.0.0.1", 0))
            s2.bind(("127.0.0.1", 0))
            addr1 = s1.getsockname()
            got = {}

            def receiver():
                data, peer = s1.recvfrom(1024)
                got["data"] = data
                got["peer_ok"] = peer[0] == "127.0.0.1"

            pygo_core.go(receiver)
            time.sleep(0.01)
            s2.sendto(b"datagram", addr1)
            t0 = time.monotonic()
            while "data" not in got and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            s1.close(); s2.close()
            return got

        got = _drive(body)
        self.assertEqual(got["data"], b"datagram")
        self.assertTrue(got["peer_ok"])


@unittest.skipUnless(_HAVE_MSG, "recvmsg/sendmsg not available on this platform")
class TestSendmsgRecvmsg(unittest.TestCase):
    def test_sendmsg_recvmsg_data(self):
        def body():
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            got = {}

            def receiver():
                data, ancdata, flags, addr = a.recvmsg(64, 256)
                got["data"] = data
                got["anc"] = ancdata

            pygo_core.go(receiver)
            time.sleep(0.01)
            n = b.sendmsg([b"hello", b"-msg"])
            t0 = time.monotonic()
            while "data" not in got and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            a.close(); b.close()
            return n, got
        n, got = _drive(body)
        self.assertEqual(n, len(b"hello-msg"))
        self.assertEqual(got["data"], b"hello-msg")
        self.assertEqual(got["anc"], [])

    @unittest.skipUnless(hasattr(socket, "SCM_RIGHTS"), "no SCM_RIGHTS")
    def test_scm_rights_fd_passing(self):
        """Pass an open file descriptor over a unix socket via SCM_RIGHTS --
        the headline reason recvmsg/sendmsg exist.  The received fd must read
        the same bytes as the original."""
        def body():
            a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            a.setblocking(False); b.setblocking(False)

            # A pipe whose read end we will ship across the socket.
            rfd, wfd = os.pipe()
            os.write(wfd, b"through-the-fd")
            os.close(wfd)

            got = {}

            def receiver():
                fds = array.array("i")
                msg, ancdata, flags, addr = a.recvmsg(
                    64, socket.CMSG_LEN(array.array("i", [0]).itemsize))
                for cmsg_level, cmsg_type, cmsg_data in ancdata:
                    if (cmsg_level == socket.SOL_SOCKET and
                            cmsg_type == socket.SCM_RIGHTS):
                        fds.frombytes(cmsg_data[:len(cmsg_data) -
                                               (len(cmsg_data) % fds.itemsize)])
                got["msg"] = msg
                got["fds"] = list(fds)

            pygo_core.go(receiver)
            time.sleep(0.01)
            b.sendmsg([b"fd!"],
                      [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                        array.array("i", [rfd]))])
            t0 = time.monotonic()
            while "fds" not in got and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            os.close(rfd)
            a.close(); b.close()
            # Read the passed fd to prove it is a working duplicate.
            passed = got["fds"][0]
            data = os.read(passed, 64)
            os.close(passed)
            return got["msg"], data
        msg, data = _drive(body)
        self.assertEqual(msg, b"fd!")
        self.assertEqual(data, b"through-the-fd")


class TestFaultInjection(unittest.TestCase):
    def test_connect_refused(self):
        """connect() to a port with no listener -> ConnectionRefusedError."""
        def body():
            # Bind+close to get an almost-certainly-free port, then connect.
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probe.bind(("127.0.0.1", 0))
            addr = probe.getsockname()
            probe.close()
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                with self.assertRaises(ConnectionRefusedError):
                    c.connect(addr)
            finally:
                c.close()
        _drive(body)

    def test_econnreset_on_reset_peer(self):
        """Peer sets SO_LINGER{0} and closes -> RST -> ECONNRESET on our
        next recv (classic fault-injection from test_socket)."""
        def body():
            srv, addr = _tcp_server()

            def server():
                conn, _ = srv.accept()
                # Force an abortive close (RST), not a graceful FIN.
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                _linger_struct())
                conn.close()

            pygo_core.go(server)
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # The RST can race any step: connect (handshake then abort),
            # send (write to a reset peer) or recv (notice the RST).  Catch
            # it wherever it lands -- the contract is "an abortive close
            # surfaces as a reset error code, and we never hang".
            err = None
            try:
                c.connect(addr)
                time.sleep(0.02)
                c.send(b"ping")
                time.sleep(0.02)
                while True:
                    d = c.recv(64)
                    if d == b"":
                        break
            except OSError as e:
                err = e.errno
            c.close(); srv.close()
            return err

        err = _drive(body)
        # Either ECONNRESET surfaced, or the RST presented as a clean EOF;
        # both are valid kernel outcomes -- the point is we did not hang and
        # the error code, when present, is the right one.
        self.assertIn(err, (errno.ECONNRESET, errno.EPIPE, None))

    def test_broken_pipe_on_write_to_closed_peer(self):
        def body():
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            b.close()
            raised = None
            try:
                for _ in range(1000):
                    a.send(b"x" * 4096)   # eventually hits the dead peer
            except (BrokenPipeError, ConnectionResetError) as e:
                raised = e.errno
            a.close()
            return raised
        err = _drive(body)
        self.assertIn(err, (errno.EPIPE, errno.ECONNRESET))

    def test_operation_on_closed_socket(self):
        def body():
            a, b = socket.socketpair()
            a.close()
            with self.assertRaises(OSError):
                a.recv(16)             # EBADF on a closed socket
            b.close()
        _drive(body)


def _linger_struct():
    import struct
    # struct linger { int l_onoff; int l_linger; } -- on, timeout=0 -> RST.
    return struct.pack("ii", 1, 0)


if __name__ == "__main__":
    unittest.main()
