"""Behavioral torture test for pygo_tcp (TCPConn) error handling.

Same approach as the netpoll/io_uring arming tests, applied to the socket
error contract of connect/accept/recv/send.  pygo_tcp runs the classic
non-blocking loop: try the syscall; on EAGAIN/EWOULDBLOCK/EINTR park on netpoll
and retry; on any other errno raise OSError.  These tests pin the observable
end of that contract over real loopback sockets, driven as cooperative
goroutines on the single-thread scheduler.

Deterministic error-CODE coverage (ECONNRESET/EPIPE/EINTR retries) lives in
test_tcp_faultinject.py via strace injection; here we cover the real-socket
behaviours that are reliable without injection: a full echo round-trip, EOF on
a clean peer close, and ECONNREFUSED to a dead port.
"""
import errno
import os
import socket

import pytest

import pygo_core


def _drive(*goroutines):
    """Run callables as goroutines; re-raise the first exception they hit."""
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:   # noqa: BLE001
                box.append(e)
        return runner

    for g in goroutines:
        pygo_core.go(wrap(g))
    pygo_core.run()
    if box:
        raise box[0]


def _port(listener):
    # socket.dup (WSADuplicateSocket on Windows), NOT os.dup: os.dup is a CRT
    # fd op and corrupts a raw WinSock socket handle on Windows (access
    # violation).  Matches test_tcpconn / test_tcp_scenarios.
    s = socket.socket(fileno=socket.dup(listener.fileno()))
    try:
        return s.getsockname()[1]
    finally:
        s.detach()
        s.close()


def _free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def test_echo_round_trip():
    port = [None]
    result = [None]

    def server():
        ln = pygo_core.TCPConn.listen("127.0.0.1", 0)
        port[0] = _port(ln)
        conn = ln.accept()
        result[0] = conn.recv(1024)
        conn.send_all(result[0])
        conn.close()
        ln.close()

    def client():
        while port[0] is None:
            pygo_core.sched_yield()
        c = pygo_core.TCPConn.connect("127.0.0.1", port[0])
        c.send_all(b"ping")
        echo = c.recv(1024)
        c.close()
        assert echo == b"ping", echo

    _drive(server, client)
    assert result[0] == b"ping"


def test_recv_returns_empty_on_clean_peer_close():
    """A peer that closes without sending is EOF: recv must return b'', not
    block forever and not raise."""
    port = [None]
    got = [None]

    def server():
        ln = pygo_core.TCPConn.listen("127.0.0.1", 0)
        port[0] = _port(ln)
        conn = ln.accept()
        conn.close()          # close immediately, no data -> client sees EOF
        ln.close()

    def client():
        while port[0] is None:
            pygo_core.sched_yield()
        c = pygo_core.TCPConn.connect("127.0.0.1", port[0])
        got[0] = c.recv(1024)
        c.close()

    _drive(server, client)
    assert got[0] == b"", "expected EOF (b''), got %r" % (got[0],)


def test_connect_refused_raises_oserror():
    """connect() to a port with no listener must surface OSError(ECONNREFUSED),
    not hang or crash."""
    dead = _free_port()
    box = {}

    def client():
        try:
            pygo_core.TCPConn.connect("127.0.0.1", dead)
        except OSError as e:
            box["errno"] = e.errno

    _drive(client)
    assert box.get("errno") == errno.ECONNREFUSED, box


def test_recv_into_round_trip():
    port = [None]
    out = [None]

    def server():
        ln = pygo_core.TCPConn.listen("127.0.0.1", 0)
        port[0] = _port(ln)
        conn = ln.accept()
        conn.send_all(b"abcdef")
        conn.close()
        ln.close()

    def client():
        while port[0] is None:
            pygo_core.sched_yield()
        c = pygo_core.TCPConn.connect("127.0.0.1", port[0])
        buf = bytearray(16)
        n = c.recv_into(buf)
        out[0] = (n, bytes(buf[:n]))
        c.close()

    _drive(server, client)
    assert out[0] == (6, b"abcdef"), out[0]
