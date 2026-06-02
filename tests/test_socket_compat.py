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
import io
import os
import platform
import socket
import tempfile
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


def _write_temp(data):
    """Write `data` to a fresh temp file, return its path.  Caller unlinks."""
    fd, path = tempfile.mkstemp(prefix="pygo_sendfile_")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
    except BaseException:
        os.unlink(path)
        raise
    return path


class TestSendfile(unittest.TestCase):
    """Cooperative socket.sendfile.

    Adapted from CPython Lib/test/test_socket.py SendfileUsingSendfileTest
    and SendfileUsingSendTest.  Stock sendfile refuses non-blocking sockets
    and drives os.sendfile with its own selector; the cooperative version
    must move the whole file (zero-copy os.sendfile fast path), honour
    offset/count, fall back to read()+send() for non-regular files, and park
    on wait_fd throughout so a sibling goroutine keeps running.
    """

    # 256 KiB -- larger than the kernel send buffer, so the transfer is
    # guaranteed to hit EAGAIN and exercise the wait_fd park/resume path.
    DATA = bytes(range(256)) * 1024

    def _run_sendfile(self, make_file, offset=0, count=None):
        def body():
            srv, addr = _tcp_server()
            received = {"buf": b""}

            def server():
                conn, _ = srv.accept()
                while True:
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    received["buf"] += chunk
                conn.close()

            pygo_core.go(server)
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect(addr)
            f = make_file()
            try:
                sent = cli.sendfile(f, offset, count)
            finally:
                f.close()
            cli.shutdown(socket.SHUT_WR)
            want = (len(self.DATA) - offset) if count is None else count
            t0 = time.monotonic()
            while len(received["buf"]) < want and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            cli.close(); srv.close()
            return sent, received["buf"]
        return _drive(body)

    def test_sendfile_regular_file_zero_copy(self):
        path = _write_temp(self.DATA)
        try:
            sent, buf = self._run_sendfile(lambda: open(path, "rb"))
        finally:
            os.unlink(path)
        self.assertEqual(sent, len(self.DATA))
        self.assertEqual(buf, self.DATA)

    def test_sendfile_offset_and_count(self):
        path = _write_temp(self.DATA)
        try:
            sent, buf = self._run_sendfile(
                lambda: open(path, "rb"), offset=100, count=5000)
        finally:
            os.unlink(path)
        self.assertEqual(sent, 5000)
        self.assertEqual(buf, self.DATA[100:5100])

    def test_sendfile_fallback_nonregular_file(self):
        # BytesIO has no fileno() -> _GiveupOnSendfile -> read()+send() path.
        sent, buf = self._run_sendfile(lambda: io.BytesIO(self.DATA))
        self.assertEqual(sent, len(self.DATA))
        self.assertEqual(buf, self.DATA)

    def test_sendfile_empty_file(self):
        path = _write_temp(b"")
        try:
            sent, buf = self._run_sendfile(lambda: open(path, "rb"))
        finally:
            os.unlink(path)
        self.assertEqual(sent, 0)
        self.assertEqual(buf, b"")

    def test_sendfile_text_mode_rejected(self):
        """_check_sendfile_params must still reject text-mode files."""
        path = _write_temp(b"abc")
        try:
            def body():
                cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                with self.assertRaises(ValueError):
                    cli.sendfile(open(path, "r"))
                cli.close()
            _drive(body)
        finally:
            os.unlink(path)

    def test_sendfile_blocked_yields_to_sibling(self):
        """A goroutine pushing a large file must let a sibling make progress
        while the send buffer is full (proves wait_fd parking, not spinning)."""
        path = _write_temp(self.DATA)
        ticks = {"n": 0}

        def body():
            srv, addr = _tcp_server()
            done = {"v": False}

            def server():
                conn, _ = srv.accept()
                total = 0
                while total < len(self.DATA):
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    total += len(chunk)
                    pygo.sleep(0.002)   # drain slowly so the sender blocks
                conn.close()

            def ticker():
                while not done["v"]:
                    ticks["n"] += 1
                    pygo.sleep(0.002)

            pygo_core.go(server)
            pygo_core.go(ticker)
            cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cli.connect(addr)
            with open(path, "rb") as f:
                sent = cli.sendfile(f)
            cli.shutdown(socket.SHUT_WR)
            cli.close(); srv.close()
            done["v"] = True
            return sent

        try:
            sent = _drive(body)
        finally:
            os.unlink(path)
        self.assertEqual(sent, len(self.DATA))
        # the ticker ran concurrently while sendfile was blocked
        self.assertGreater(ticks["n"], 0)


class TestRecvfromInto(unittest.TestCase):
    """Cooperative socket.recvfrom_into (zero-alloc datagram receive).

    Adapted from CPython Lib/test/test_socket.py BasicUDPTest /
    RecvIntoTests.  Fills a caller-owned buffer and returns (nbytes, addr);
    a blocked recvfrom_into must yield to siblings.
    """

    def test_udp_recvfrom_into(self):
        def body():
            rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rx.bind(("127.0.0.1", 0))
            addr = rx.getsockname()

            def sender():
                tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                tx.sendto(b"datagram-payload", addr)
                tx.close()

            pygo_core.go(sender)
            buf = bytearray(64)
            n, peer = rx.recvfrom_into(buf)
            rx.close()
            return n, bytes(buf[:n]), peer
        n, data, peer = _drive(body)
        self.assertEqual(n, len(b"datagram-payload"))
        self.assertEqual(data, b"datagram-payload")
        self.assertEqual(peer[0], "127.0.0.1")

    def test_recvfrom_into_nbytes_cap(self):
        """recvfrom_into with an explicit nbytes reads at most that many."""
        def body():
            rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rx.bind(("127.0.0.1", 0))
            addr = rx.getsockname()

            def sender():
                tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                tx.sendto(b"0123456789", addr)
                tx.close()

            pygo_core.go(sender)
            buf = bytearray(64)
            n, _ = rx.recvfrom_into(buf, 4)
            rx.close()
            return n, bytes(buf[:4])
        n, data = _drive(body)
        self.assertEqual(n, 4)
        self.assertEqual(data, b"0123")

    def test_recvfrom_into_blocks_then_yields(self):
        def body():
            rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rx.bind(("127.0.0.1", 0))
            addr = rx.getsockname()
            order = []

            def receiver():
                order.append("recv-wait")
                buf = bytearray(32)
                rx.recvfrom_into(buf)
                order.append("got")

            def sender():
                for _ in range(3):
                    time.sleep(0.005)
                    order.append("tick")
                tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                tx.sendto(b"ping", addr)
                tx.close()

            pygo_core.go(receiver)
            pygo_core.go(sender)
            t0 = time.monotonic()
            while "got" not in order and time.monotonic() - t0 < 5:
                pygo.sleep(0.005)
            rx.close()
            return order
        order = _drive(body)
        self.assertIn("got", order)
        self.assertIn("tick", order)
        self.assertLess(order.index("recv-wait"), order.index("got"))


def _linger_struct():
    import struct
    # struct linger { int l_onoff; int l_linger; } -- on, timeout=0 -> RST.
    return struct.pack("ii", 1, 0)


if __name__ == "__main__":
    unittest.main()
