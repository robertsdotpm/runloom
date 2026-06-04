"""big_100 / 08 -- UDP timeout matrix.

Some goroutines talk to a live UDP echo server (replies arrive); others fire at
a dead port (nothing listens, so every send times out, possibly via an ICMP
port-unreachable).  The point: a goroutine stuck waiting out a timeout on the
dead path must not block the goroutines making progress on the live path.

Stresses: timeout handling, scheduler wakeups, ICMP-unreachable error delivery.
"""
import harness
import netutil


def setup(H):
    live = netutil.udp_socket()
    live.setblocking(False)
    live_addr = live.getsockname()

    # A dead address: bind a socket to grab a port, then close it.  Nothing
    # listens there now, so datagrams sent to it never get a reply.
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

    H.go(echo_server)


def client(H, wid, rng, state):
    # Connected UDP sockets: each only ever receives from its own peer, so a
    # delayed live echo can never pollute a dead-port read (and the dead socket
    # surfaces the ICMP unreachable as ECONNREFUSED on recv, never as bytes).
    live = netutil.udp_socket()
    live.setblocking(False)
    live.connect(state["live"])
    dead = netutil.udp_socket()
    dead.setblocking(False)
    dead.connect(state["dead"])
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
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p08_udp_timeout_matrix", body, setup=setup, default_funcs=8000,
                 describe="live vs dead UDP ports; dead-path timeouts isolate")
