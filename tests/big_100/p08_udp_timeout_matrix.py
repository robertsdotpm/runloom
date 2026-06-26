"""big_100 / 08 -- UDP timeout matrix.

Some goroutines talk to a live UDP echo server (replies arrive); others fire at
a dead port (nothing listens, so every send times out, possibly via an ICMP
port-unreachable).  The point: a goroutine stuck waiting out a timeout on the
dead path must not block the goroutines making progress on the live path.

Stresses: timeout handling, scheduler wakeups, ICMP-unreachable error delivery.
"""
import errno
import sys

import harness
import netutil

# Windows' UDP socket ceiling is the non-paged pool (~16k live sockets on the
# test box before WSAENOBUFS / WinError 10055), NOT an fd rlimit.  This worker
# opens TWO sockets (live + dead), so the sweep's --funcs 100000 would open 200k
# at once and exhaust it.  Cap LIVE workers on Windows so live-socket count
# stays well under the ceiling; mac/Linux open them all (1M verified).
_WIN_MAX_LIVE = 5000

# At 100k goroutines each worker opens 2 UDP sockets, producing 200k sockets
# all binding to 127.0.0.1.  The default ephemeral port range is only ~28k
# ports, so the kernel runs out and starts reusing the dead_addr port, causing
# phantom replies.  Use 127.0.0.2-9 (8 separate loopback addresses each with
# their own 28k-port pool) so workers never consume 127.0.0.1's port space and
# dead_addr (on 127.0.0.1) can never be accidentally auto-assigned to a worker.
_CLIENT_BIND_ADDRS = ["127.0.0.{0}".format(i) for i in range(2, 10)]


def setup(H):
    live = netutil.udp_socket()
    live.setblocking(False)
    live_addr = live.getsockname()

    # A dead address: bind a socket to grab a port on 127.0.0.1, then close it.
    # Workers bind to 127.0.0.2-9, so this port is never auto-recycled to them.
    dead = netutil.udp_socket()
    dead_addr = dead.getsockname()
    dead.close()

    H.state = {"live": live_addr, "dead": dead_addr}

    def echo_server():
        try:
            while H.running():
                data, addr = netutil.udp_recvfrom_timeout(live, 2048, 300)
                if data is None:
                    continue
                try:
                    live.sendto(data, addr)
                except OSError:
                    pass
        finally:
            netutil.close_quiet(live)

    H.fiber(echo_server)


def client(H, wid, rng, state):
    # Connected UDP sockets: each only ever receives from its own peer, so a
    # delayed live echo can never pollute a dead-port read (and the dead socket
    # surfaces the ICMP unreachable as ECONNREFUSED on recv, never as bytes).
    # Bind to a distributed loopback address (127.0.0.2-9) so workers don't
    # deplete 127.0.0.1's port pool — see _CLIENT_BIND_ADDRS comment above.
    bind_host = _CLIENT_BIND_ADDRS[wid % len(_CLIENT_BIND_ADDRS)]
    # Socket SETUP can hit a benign HOST resource limit at high concurrency that
    # is orthogonal to this test's timeout-isolation invariant: e.g. on macOS a
    # bind/connect to a per-worker loopback addr can momentarily return
    # EADDRNOTAVAIL (ephemeral-port pool churn), the *NIX cousin of the Windows
    # WSAENOBUFS socket ceiling this test already documents.  Treat such a setup
    # failure as a skipped worker (benign), not an invariant failure -- mirrors
    # the send/recv OSError tolerance below.
    live = dead = None
    try:
        live = netutil.udp_socket(host=bind_host)
        live.setblocking(False)
        live.connect(state["live"])
        dead = netutil.udp_socket(host=bind_host)
        dead.setblocking(False)
        dead.connect(state["dead"])
    except OSError as e:
        netutil.close_quiet(live)
        netutil.close_quiet(dead)
        if e.errno in (errno.EADDRNOTAVAIL, errno.EADDRINUSE,
                       errno.ENOBUFS, errno.EMFILE, errno.ENFILE):
            H.op(wid)               # benign host limit -> count + skip this worker
            return
        raise
    try:
        H.sleep(rng.random() * 0.5)
        while H.running():
            to_dead = (rng.random() < 0.5)
            sock = dead if to_dead else live
            nonce = rng.getrandbits(64).to_bytes(8, "big")
            try:
                sock.send(nonce)
            except OSError:
                H.op(wid)
                continue
            # Drain until we see OUR nonce echoed (live) or time out; a stale
            # echo of an earlier datagram is skipped, not mistaken for a reply.
            matched = False
            while True:
                try:
                    data, _ = netutil.udp_recvfrom_timeout(sock, 2048, 150)
                except OSError:
                    data = None     # ECONNREFUSED on the dead path
                if data is None:
                    break
                if data == nonce:
                    matched = True
                    break
            if to_dead:
                H.check(not matched,
                        "phantom reply from dead port wid={0}".format(wid))
            H.op(wid)
            H.task_done(wid)
    finally:
        netutil.close_quiet(live)
        netutil.close_quiet(dead)


def body(H):
    mc = _WIN_MAX_LIVE if sys.platform == "win32" else H.max_concurrent
    H.run_pool(H.funcs, client, H.state, max_concurrent=mc)


if __name__ == "__main__":
    harness.main("p08_udp_timeout_matrix", body, setup=setup, default_funcs=8000,
                 describe="live vs dead UDP ports; dead-path timeouts isolate")
