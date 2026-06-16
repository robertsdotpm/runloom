"""big_100 / 07 -- UDP packet storm.

A local UDP echo server.  Tens of thousands of goroutines fire random
datagrams and verify the reply, with a real timeout so a dropped datagram
(UDP is lossy, even on loopback under load) is retried rather than hanging the
goroutine forever.

Stresses: recvfrom/sendto, timeouts via wait_fd, packet loss handling.
"""
import socket
import sys

import harness
import netutil

# Windows' UDP socket ceiling is the non-paged pool (~16k live sockets on the
# test box before WSAENOBUFS / WinError 10055), NOT an fd rlimit.  Each worker
# holds its socket for its whole life, so the sweep's --funcs 100000 would open
# 100k at once and exhaust it.  Cap LIVE workers on Windows to the designed
# scale; mac/Linux open them all (1M verified), absorbing the over-scale via
# memory compression / larger buffers.  One socket per worker here.
_WIN_MAX_LIVE = 8000


def setup(H):
    srv = netutil.udp_socket()
    srv.setblocking(False)
    H.state = {"addr": srv.getsockname()}

    def echo_server():
        # Timeout-recv loop so the server re-checks running() and self-
        # terminates at the deadline -- a UDP socket parked in recvfrom is not
        # woken by a cross-goroutine close, so don't rely on register_close.
        try:
            while H.running():
                data, addr = netutil.udp_recvfrom_timeout(srv, 2048, 300)
                if data is None:
                    continue
                try:
                    srv.sendto(data, addr)
                except OSError:
                    pass
        finally:
            netutil.close_quiet(srv)

    H.go(echo_server)


def client(H, wid, rng, state):
    addr = state["addr"]
    sock = netutil.udp_socket()
    sock.setblocking(False)
    try:
        H.sleep(rng.random() * 0.5)
        while H.running():
            # Unique nonce per datagram so a delayed/duplicated reply from an
            # earlier iteration is recognised as stale and skipped, not
            # mistaken for a corrupt echo (UDP reorders/duplicates freely).
            payload = (rng.getrandbits(64).to_bytes(8, "big")
                       + rng.randbytes(rng.randint(0, 504)))
            matched = False
            for _attempt in range(4):           # retry across loss
                if not H.running():
                    break
                try:
                    sock.sendto(payload, addr)
                except OSError:
                    break
                # Drain replies until we see THIS datagram echoed or we time
                # out; stale echoes (!= payload) are ignored.
                while True:
                    data, _ = netutil.udp_recvfrom_timeout(sock, 2048, 200)
                    if data is None:
                        break                   # timeout -> resend
                    if data == payload:
                        matched = True
                        break
                if matched:
                    break
            if matched:
                H.op(wid)
                H.task_done(wid)
    finally:
        netutil.close_quiet(sock)


def body(H):
    mc = _WIN_MAX_LIVE if sys.platform == "win32" else H.max_concurrent
    H.run_pool(H.funcs, client, H.state, max_concurrent=mc)


if __name__ == "__main__":
    harness.main("p07_udp_storm", body, setup=setup, default_funcs=8000,
                 describe="UDP echo storm with timeout/retry on loss")
