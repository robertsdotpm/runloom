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
        rc.fiber(server); rc.fiber(client)
    with hang_guard(15, "tcpconn echo"):
        rc.fiber(main); rc.run()
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
        rc.fiber(f); rc.run()
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
        rc.fiber(server); rc.fiber(client)
    with hang_guard(15, "tcpconn eof"):
        rc.fiber(main); rc.run()
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
        rc.fiber(server); rc.fiber(client)
    with hang_guard(30, "tcpconn large"):
        rc.fiber(main); rc.run()
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
        rc.fiber(server); rc.fiber(client)
    with hang_guard(15, "tcpconn recv_into"):
        rc.fiber(main); rc.run()
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
                rc.fiber(handle)

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

        rc.fiber(acceptor)
        for i in range(N):
            rc.fiber(lambda i=i: client(i))
        wg.wait()
        lst.close()
    with hang_guard(40, "tcpconn concurrent"):
        rc.fiber(main); rc.run()
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
                rc.mn_fiber(lambda conn=conn: (conn.send_all(b"x:" + conn.recv(64)), conn.close()))

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

        rc.mn_fiber(acceptor)
        for i in range(N):
            rc.mn_fiber(lambda i=i: client(i))
        wg.wait()
        lst.close()
    with hang_guard(60, "tcpconn M:N"):
        runloom.run(4, main)
    assert sum(ok) == N, "%d/%d M:N TCPConn echoes ok" % (sum(ok), N)


# --------------------------------------------------------------------------
# io_uring single-shot close-cancel (R7 item 1): closing a TCPConn while a
# SINGLE-SHOT io_uring recv is parked on it must WAKE the recv, not hang.
# io_uring pins the file, so a plain close does not complete the parked op --
# RunloomTCPConn_close submits an ASYNC_CANCEL_FD (runloom_iouring_cancel_fd).
#
# SUBPROCESS + os._exit-on-wake: the failure mode is an io_uring D-state wedge
# (an unkillable process whose ring teardown waits on the stuck op), so we run
# it isolated with a hard timeout and hard-exit the instant the recv wakes,
# before any teardown path.  Single-thread scheduler on purpose: that routes
# the single-shot recv to the GLOBAL ring, which the cancel covers (the M:N
# per-hub-ring case is a documented follow-up, RELIABILITY_PROGRAM.md R7).
# --------------------------------------------------------------------------
_IOU_CLOSE_CANCEL = r'''
import sys, os, socket
sys.path.insert(0, "src")
import runloom_c as rc
FLAGS = socket.MSG_WAITALL          # non-zero flags -> single-shot IORING_OP_RECV
out = {}
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(lst.fileno())); port = s.getsockname()[1]; s.detach()
    box = {}
    rc.fiber(lambda: box.__setitem__("conn", lst.accept()))
    c = rc.TCPConn.connect("127.0.0.1", port)
    for _ in range(5): rc.sched_yield()
    def parker():
        try:
            r = c.recv(64, FLAGS)       # parks: peer never sends
            out["r"] = ("ret", r)
        except OSError as e:
            out["r"] = ("err", e.errno)
        sys.stdout.write("WOKE\n"); sys.stdout.flush()
        os._exit(0)                     # hard-exit before any teardown can wedge
    rc.fiber(parker)
    def closer():
        for _ in range(6): rc.sched_yield()
        c.close()                       # -> runloom_iouring_cancel_fd(fd)
    rc.fiber(closer)
rc.fiber(main); rc.run()
sys.stdout.write("HUNG\n"); sys.stdout.flush(); os._exit(2)
'''


@pytest.mark.skipif(not rc.iouring_available(), reason="io_uring unavailable")
def test_tcpconn_iouring_close_cancels_parked_single_shot_recv():
    import subprocess
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, RUNLOOM_TCPCONN_IOURING="1",
               PYTHON_GIL="0", PYTHONPATH="src")
    # -s KILL bounds a regression (a stuck op) so the suite never hangs; on a
    # working build the child exits in well under a second.
    p = subprocess.run(["timeout", "-s", "KILL", "15", sys.executable,
                        "-c", _IOU_CLOSE_CANCEL],
                       cwd=repo, env=env, capture_output=True, text=True)
    assert "WOKE" in p.stdout, (
        "parked io_uring single-shot recv did not wake on close (got %r / rc=%d)"
        % (p.stdout, p.returncode))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
