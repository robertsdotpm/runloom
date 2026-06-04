"""big_100 / 14 -- random TCP chaos monkey.

Clients pick a random nasty behaviour each connection: a clean length-prefixed
request/response, a partial write then abrupt close, a half-close (shutdown
write) then read, or connect-and-stall.  A fraction always behave well; the
server must keep serving those correctly no matter what the chaos clients do.

Stresses: edge-case socket state transitions, server robustness.
"""
import socket

import harness
import netutil


def server_handler(conn):
    """Length-prefixed echo: 4-byte big-endian length, then that many bytes."""
    try:
        while True:
            hdr = netutil.recv_exact(conn, 4)
            n = int.from_bytes(hdr, "big")
            if n == 0 or n > 1 << 20:
                break
            body = netutil.recv_exact(conn, n)
            conn.sendall(hdr + body)
    except OSError:
        pass
    finally:
        netutil.close_quiet(conn)


def setup(H):
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1]}
    H.register_close(srv)

    def accept_loop():
        while H.running():
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            H.go(server_handler, conn)

    H.go(accept_loop)


def well_behaved(H, sock, rng, wid):
    for _ in range(rng.randint(1, 6)):
        if not H.running():
            return
        payload = rng.randbytes(rng.randint(1, 300))
        sock.sendall(len(payload).to_bytes(4, "big") + payload)
        hdr = netutil.recv_exact(sock, 4)
        got = netutil.recv_exact(sock, int.from_bytes(hdr, "big"))
        if not H.check(got == payload,
                       "clean request corrupted wid={0}".format(wid)):
            return
        H.op(wid)
    H.task_done(wid)


def chaos(H, sock, rng):
    pick = rng.random()
    if pick < 0.3:
        sock.sendall(b"\x00\x00\x01")          # partial length prefix
    elif pick < 0.6:
        sock.sendall((50).to_bytes(4, "big") + rng.randbytes(10))  # short body
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        try:
            sock.recv(64)
        except OSError:
            pass
    else:
        H.sleep(rng.random() * 0.5)             # connect-and-stall


def client(H, wid, rng, state):
    port = state["port"]
    # ~40% of clients are always well-behaved -> their requests must succeed.
    good = (wid % 5) < 2
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            if good:
                well_behaved(H, sock, rng, wid)
            else:
                chaos(H, sock, rng)
        except OSError:
            if not H.running():
                break
            H.sleep(0.003)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p14_tcp_chaos", body, setup=setup, default_funcs=8000,
                 describe="random nasty socket behaviours; well-behaved clients still served")
