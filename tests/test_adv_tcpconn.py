"""Adversarial QA: the C-level TCPConn (runloom_c.TCPConn).

A thin cooperative-I/O socket wrapper: TCPConn.listen / .connect (classmethods),
.accept (-> a conn, NO addr tuple), .recv / .recv_into / .send / .send_all /
.close / .fileno.  We probe the real-network edge cases: connection refused,
EOF on peer close, large framed transfers (partial send/recv loops), and many
concurrent connections under both schedulers.
"""
import os
import socket
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()


def _listener_port(lst):
    # TCPConn has no getsockname(); read it off a dup'd fd.
    s = socket.socket(fileno=os.dup(lst.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach()


def _recv_exactly(conn, n):
    chunks = []
    got = 0
    while got < n:
        b = conn.recv(n - got)
        if not b:
            break
        chunks.append(b)
        got += len(b)
    return b"".join(chunks)


# --------------------------------------------------------------------------
# basics
# --------------------------------------------------------------------------
def test_tcpconn_echo_roundtrip():
    out = {}
    def main():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        port = _listener_port(lst)
        def server():
            conn = lst.accept()
            assert isinstance(conn, rc.TCPConn)
            data = conn.recv(64)
            conn.send_all(b"echo:" + data)
            conn.close()
        def client():
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"hi")
            out["reply"] = c.recv(64)
            c.close()
            lst.close()
        rc.go(server); rc.go(client)
    with hang_guard(15, "tcpconn echo"):
        rc.go(main); rc.run()
    assert out.get("reply") == b"echo:hi"


def test_tcpconn_connect_refused_raises():
    out = {}
    def f():
        s = socket.socket(); s.bind(("127.0.0.1", 0))
        p = s.getsockname()[1]; s.close()          # now-free port, nothing listening
        try:
            rc.TCPConn.connect("127.0.0.1", p)
            out["r"] = "connected"
        except OSError as e:
            out["r"] = type(e).__name__
    with hang_guard(15, "tcpconn refused"):
        rc.go(f); rc.run()
    assert out.get("r") in ("ConnectionRefusedError", "OSError"), out


def test_tcpconn_recv_after_peer_close_is_eof():
    out = {}
    def main():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        port = _listener_port(lst)
        def server():
            conn = lst.accept()
            conn.close()                            # close immediately, no data
        def client():
            c = rc.TCPConn.connect("127.0.0.1", port)
            out["recv"] = c.recv(64)                # peer closed -> b""
            c.close(); lst.close()
        rc.go(server); rc.go(client)
    with hang_guard(15, "tcpconn eof"):
        rc.go(main); rc.run()
    assert out.get("recv") == b""


def test_tcpconn_large_framed_transfer():
    SIZE = 512 * 1024
    payload = bytes((i & 0xFF) for i in range(256)) * (SIZE // 256)
    out = {}
    def main():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        port = _listener_port(lst)
        def server():
            conn = lst.accept()
            data = _recv_exactly(conn, SIZE)        # loop partial recvs
            conn.send_all(data)                     # loop partial sends
            conn.close()
        def client():
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(payload)
            out["echo"] = _recv_exactly(c, SIZE)
            c.close(); lst.close()
        rc.go(server); rc.go(client)
    with hang_guard(30, "tcpconn large"):
        rc.go(main); rc.run()
    assert out.get("echo") == payload, "large transfer corrupted/truncated"


def test_tcpconn_recv_into_buffer():
    out = {}
    def main():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        port = _listener_port(lst)
        def server():
            conn = lst.accept()
            conn.send_all(b"ABCDE")
            conn.close()
        def client():
            c = rc.TCPConn.connect("127.0.0.1", port)
            buf = bytearray(5)
            n = c.recv_into(buf, 5)
            out["n"] = n; out["buf"] = bytes(buf[:n])
            c.close(); lst.close()
        rc.go(server); rc.go(client)
    with hang_guard(15, "tcpconn recv_into"):
        rc.go(main); rc.run()
    assert out.get("buf") == b"ABCDE"


# --------------------------------------------------------------------------
# concurrency
# --------------------------------------------------------------------------
def test_tcpconn_many_concurrent_connections():
    N = 40
    ok = bytearray(N)
    def main():
        from runloom.sync import WaitGroup
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        port = _listener_port(lst)
        wg = WaitGroup(); wg.add(N)

        def acceptor():
            for _ in range(N):
                conn = lst.accept()
                def handle(conn=conn):
                    data = conn.recv(64)
                    conn.send_all(b"r:" + data)
                    conn.close()
                rc.go(handle)

        def client(i):
            try:
                c = rc.TCPConn.connect("127.0.0.1", port)
                msg = ("m%d" % i).encode()
                c.send_all(msg)
                if c.recv(64) == b"r:" + msg:
                    ok[i] = 1
                c.close()
            finally:
                wg.done()

        rc.go(acceptor)
        for i in range(N):
            rc.go(lambda i=i: client(i))
        wg.wait()
        lst.close()
    with hang_guard(40, "tcpconn concurrent"):
        rc.go(main); rc.run()
    assert sum(ok) == N, "%d/%d concurrent TCPConn echoes ok" % (sum(ok), N)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_tcpconn_echo_under_mn():
    N = 60
    ok = bytearray(N)
    def main():
        from runloom.sync import WaitGroup
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        port = _listener_port(lst)
        wg = WaitGroup(); wg.add(N)

        def acceptor():
            for _ in range(N):
                conn = lst.accept()
                rc.mn_go(lambda conn=conn: (conn.send_all(b"x:" + conn.recv(64)), conn.close()))

        def client(i):
            try:
                c = rc.TCPConn.connect("127.0.0.1", port)
                msg = ("c%d" % i).encode()
                c.send_all(msg)
                if c.recv(64) == b"x:" + msg:
                    ok[i] = 1
                c.close()
            finally:
                wg.done()

        rc.mn_go(acceptor)
        for i in range(N):
            rc.mn_go(lambda i=i: client(i))
        wg.wait()
        lst.close()
    with hang_guard(60, "tcpconn M:N"):
        runloom.run(4, main)
    assert sum(ok) == N, "%d/%d M:N TCPConn echoes ok" % (sum(ok), N)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
