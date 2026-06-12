"""Bridge low-level loop.sock_* must not hang on fd-number reuse.

A user socket used via loop.sock_* and closed with a plain socket.close()
bypasses the bridge's _close_sock netpoll-unregister hook (only sockets the
bridge itself owns go through that).  Before the fix that left a stale
single-thread netpoll LEVEL arm cache for the closed fd; the next socket reusing
that fd NUMBER inherited the stale "armed" mask, netpoll's register-once skip
never re-armed it in the kernel, and wait_fd parked forever -- the long-standing
test_asyncio_conformance::test_recvfrom flake (and a hang for any asyncio program
doing low-level sock_* + close + fd reuse).

The fix: loop.sock_* call runloom_c.netpoll_release_if_idle(fd) on completion,
which DELs + clears the arm for an fd no fiber is parked on, so a later raw
close + reuse re-registers cleanly.  A regression HANGS here (caught by the suite
timeout); a clean finish IS the assertion.  Mirrors the deterministic repro: each
round closes its sockets and the next round reuses the fd numbers.
"""
import socket

import pytest

import runloom.aio as paio


def _roundtrip(loop):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    srv.setblocking(False)
    addr = srv.getsockname()
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    c.setblocking(False)
    try:
        async def drive():
            await loop.sock_connect(c, addr)
            conn, _ = await loop.sock_accept(srv)
            await loop.sock_sendall(conn, b"x" * 64)
            data = await loop.sock_recv(c, 64)
            conn.close()
            return data

        return loop.run_until_complete(drive())
    finally:
        c.close()
        srv.close()


def test_lowlevel_sock_fd_reuse_no_hang():
    # Many round-trips, each closing its sockets so the next reuses the fd
    # numbers via low-level sock_* (the path the bridge can't close-hook).
    loop = paio.RunloomEventLoop()
    try:
        for i in range(30):
            assert _roundtrip(loop) == b"x" * 64, i
    finally:
        loop.close()


def test_udp_recvfrom_after_tcp_churn_no_hang():
    # The exact conformance shape: churn TCP fds, then a UDP sendto/recvfrom on a
    # reused fd number.  A stale arm from the TCP fds would hang the UDP recvfrom.
    def echo_server():
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("127.0.0.1", 0))
        return s, s.getsockname()

    loop = paio.RunloomEventLoop()
    try:
        # churn: open/close several TCP client+server pairs to register+close fds
        for _ in range(6):
            _roundtrip(loop)

        srv, srv_addr = echo_server()
        import threading
        stop = {"v": False}

        def serve():
            srv.settimeout(2.0)
            try:
                while not stop["v"]:
                    try:
                        data, who = srv.recvfrom(4096)
                    except socket.timeout:
                        return
                    if data == b"STOP":
                        return
                    srv.sendto(data, who)
            finally:
                pass

        t = threading.Thread(target=serve)
        t.start()
        try:
            async def probe():
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setblocking(False)
                try:
                    await loop.sock_sendto(sock, b"\x01" * 4096, srv_addr)
                    data, _ = await loop.sock_recvfrom(sock, 4096)
                    return data
                finally:
                    sock.close()
            got = loop.run_until_complete(probe())
            assert got == b"\x01" * 4096
        finally:
            stop["v"] = True
            stopper = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            stopper.sendto(b"STOP", srv_addr)
            stopper.close()
            t.join(timeout=3.0)
            srv.close()
    finally:
        loop.close()
