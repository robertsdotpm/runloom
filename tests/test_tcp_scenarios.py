"""TCP I/O scenarios re-authored from libuv's test suite.

libuv's tests are C against its own loop API, so they can't be ported
mechanically -- but the *scenarios* are the distilled hard cases of async TCP
(backpressure, half-close, close-mid-write, many concurrent connections,
stream-order integrity), and they map directly onto pygo's TCPConn (the
io_uring/netpoll recv/send paths just hardened) and pygo.sync.Socket.

These run in-process on the single-thread scheduler (the test_tcpconn.py idiom:
server + client goroutines cooperate, recv/send park on netpoll readiness).
The conftest invariant fixture then checks self_check + parker leak after each.

libuv originals (libuv/libuv, test/):
  test-tcp-writealot.c          -> backpressure on a payload > socket buffers
  test-tcp-close-while-writing  -> peer closes mid-write, no crash
  test-tcp-shutdown-after-write -> half-close (SHUT_WR), peer drains then EOF
  test-tcp-many-accepts.c       -> many concurrent connections, all serviced
  test-tcp-write-queue / order  -> a long run of writes arrives byte-exact
"""
import os
import socket
import sys
import unittest
import zlib

sys.path.insert(0, "src")

import pygo_core
import pygo.sync as psync

# Prewarm getaddrinfo's deep, non-yielding lazy import chain
# (encodings.idna -> stringprep -> unicodedata) on the MAIN thread, with its
# full C stack.  pygo.sync.run()/pygo.runtime.run() do this for you; these
# tests drive the low-level pygo_core.go/run() path directly, which does not,
# so the first getaddrinfo inside a goroutine would otherwise overflow the
# (small) goroutine stack and crash.  See pygo/sync.py prewarm comment.
socket.getaddrinfo("127.0.0.1", 0, socket.AF_INET, socket.SOCK_STREAM)


def _bound_port(listener):
    """Read the bound port of a TCPConn listener via a dup'd socket."""
    fd = listener.fileno()
    sk = socket.socket(fileno=socket.dup(fd))
    port = sk.getsockname()[1]
    sk.close()
    return port


def _drive(*goroutines):
    """Spawn each callable as a goroutine, run the scheduler, re-raise the
    first exception any goroutine hit."""
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:  # noqa: BLE001 - surfaced to the test
                box.append(e)
        return runner

    for g in goroutines:
        pygo_core.go(wrap(g))
    pygo_core.run()
    if box:
        raise box[0]


def _pattern(nbytes):
    """A non-trivial, position-sensitive byte pattern (reordering or loss
    changes its CRC), sized to nbytes."""
    block = bytes(range(256))
    reps = (nbytes + len(block) - 1) // len(block)
    return (block * reps)[:nbytes]


class TestBackpressure(unittest.TestCase):
    """libuv test-tcp-writealot: a payload far larger than the kernel socket
    buffers must transfer completely -- the sender parks on write-readiness
    while the receiver drains, then resumes, until every byte is delivered."""

    def test_writealot_backpressure(self):
        SIZE = 4 * 1024 * 1024          # 4 MiB: well past socket buffers
        payload = _pattern(SIZE)
        want_crc = zlib.crc32(payload)
        port_holder = [None]
        got = [None]

        def server():
            listener = pygo_core.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            crc = 0
            total = 0
            while True:
                chunk = conn.recv(65536)
                if not chunk:           # EOF: peer closed after send_all
                    break
                crc = zlib.crc32(chunk, crc)
                total += len(chunk)
            got[0] = (total, crc)
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                pygo_core.sched_yield()
            c = pygo_core.TCPConn.connect("127.0.0.1", port_holder[0])
            c.send_all(payload)         # blocks/parks repeatedly: backpressure
            c.close()                   # FIN after all buffered bytes flushed

        _drive(server, client)
        self.assertEqual(got[0], (SIZE, want_crc))


class TestStreamIntegrity(unittest.TestCase):
    """libuv write-queue / ordering: a long run of independent writes must
    arrive as one byte-exact, correctly-ordered stream (no loss, no
    reordering, no interleave) -- TCP stream semantics through TCPConn."""

    def test_many_writes_arrive_byte_exact(self):
        NCHUNKS = 600
        # variable-size chunks carved IN ORDER from one known buffer
        sizes = [1 + (i * 37) % 9000 for i in range(NCHUNKS)]
        total = sum(sizes)
        whole = _pattern(total)
        want_crc = zlib.crc32(whole)
        port_holder = [None]
        got = [None]

        def server():
            listener = pygo_core.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            crc = 0
            n = 0
            while n < total:
                chunk = conn.recv(65536)
                if not chunk:
                    break
                crc = zlib.crc32(chunk, crc)
                n += len(chunk)
            got[0] = (n, crc)
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                pygo_core.sched_yield()
            c = pygo_core.TCPConn.connect("127.0.0.1", port_holder[0])
            off = 0
            for sz in sizes:
                c.send_all(whole[off:off + sz])
                off += sz
            c.close()

        _drive(server, client)
        self.assertEqual(got[0], (total, want_crc))


class TestCloseWhileWriting(unittest.TestCase):
    """libuv test-tcp-close-while-writing: the peer closes while the local
    side is mid-write.  The invariant is robustness, not a particular outcome:
    the writer either completes (it all fit in buffers before the close) or
    raises an OSError (broken pipe / connection reset) -- never a crash, never
    a hang, and the runtime self-check stays clean."""

    def test_peer_close_mid_write_no_crash(self):
        SIZE = 8 * 1024 * 1024          # big enough that the writer is mid-flight
        payload = _pattern(SIZE)
        port_holder = [None]
        outcome = [None]

        def server():
            listener = pygo_core.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            conn = listener.accept()
            conn.recv(4096)             # take a little, then abandon the peer
            conn.close()
            listener.close()

        def client():
            while port_holder[0] is None:
                pygo_core.sched_yield()
            c = pygo_core.TCPConn.connect("127.0.0.1", port_holder[0])
            try:
                c.send_all(payload)
                outcome[0] = "completed"
            except OSError as e:
                outcome[0] = ("oserror", type(e).__name__)
            finally:
                c.close()

        _drive(server, client)
        # Either outcome is acceptable; the point is no crash/hang above.
        self.assertIn(
            outcome[0] if isinstance(outcome[0], str) else outcome[0][0],
            ("completed", "oserror"))
        self.assertEqual(pygo_core._self_check(0), 0)


class TestHalfClose(unittest.TestCase):
    """libuv test-tcp-shutdown-after-write: the client writes its request and
    shuts down its WRITE half; the server reads to EOF, then -- the connection
    still being open the other direction -- sends a reply the client reads.
    Uses pygo.sync.Socket (TCPConn has no shutdown())."""

    def test_shutdown_wr_then_read_reply(self):
        port_holder = [None]
        reply_holder = [None]
        req = b"REQUEST-" + _pattern(20000)
        rep = b"REPLY-" + _pattern(8000)
        want_req_crc = zlib.crc32(req)
        server_saw = [None]

        def server():
            listen = psync.tcp_listen("127.0.0.1", 0)
            port_holder[0] = listen.getsockname()[1]
            conn, _addr = listen.accept()
            buf = bytearray()
            while True:
                data = conn.recv(65536)
                if not data:            # client did shutdown(SHUT_WR) -> EOF
                    break
                buf += data
            server_saw[0] = (len(buf), zlib.crc32(bytes(buf)))
            conn.sendall(rep)           # reply on the still-open write half
            conn.close()
            listen.close()

        def client():
            while port_holder[0] is None:
                pygo_core.sched_yield()
            s = psync.tcp_connect("127.0.0.1", port_holder[0])
            s.sendall(req)
            s.shutdown(socket.SHUT_WR)  # half-close: signals EOF to the server
            chunks = []
            while True:
                d = s.recv(65536)
                if not d:
                    break
                chunks.append(d)
            reply_holder[0] = b"".join(chunks)
            s.close()

        _drive(server, client)
        self.assertEqual(server_saw[0], (len(req), want_req_crc))
        self.assertEqual(reply_holder[0], rep)


class TestManyConcurrentConnections(unittest.TestCase):
    """libuv test-tcp-many-accepts: a server accepts many connections that are
    all live at once, each handled by its own goroutine, and every client gets
    its own correct echo.  Conservation across all connections."""

    def test_many_concurrent_echo(self):
        N = 40
        port_holder = [None]
        results = []

        def handler(conn):
            def run():
                data = conn.recv(4096)
                conn.send_all(data)     # echo back exactly what we got
                conn.close()
            return run

        def server():
            listener = pygo_core.TCPConn.listen("127.0.0.1", 0)
            port_holder[0] = _bound_port(listener)
            for _ in range(N):
                conn = listener.accept()
                pygo_core.go(handler(conn))   # one goroutine per connection
            listener.close()

        def make_client(i):
            payload = ("client-%d-" % i).encode() + _pattern(100 + i)

            def run():
                while port_holder[0] is None:
                    pygo_core.sched_yield()
                c = pygo_core.TCPConn.connect("127.0.0.1", port_holder[0])
                c.send_all(payload)
                buf = bytearray()
                while len(buf) < len(payload):
                    d = c.recv(4096)
                    if not d:
                        break
                    buf += d
                results.append(bytes(buf) == payload)
                c.close()
            return run

        _drive(server, *(make_client(i) for i in range(N)))
        self.assertEqual(len(results), N)
        self.assertTrue(all(results), "some connection got a wrong echo")


if __name__ == "__main__":
    unittest.main()
