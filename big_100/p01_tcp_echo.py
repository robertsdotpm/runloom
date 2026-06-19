"""big_100 / 01 -- 10k TCP echo clients.

One local TCP echo server.  Tens of thousands of lightweight goroutines each
connect, send random payloads, verify the echo byte-for-byte, disconnect, and
repeat for the whole duration.

Stresses: sockets, connect, recv, send, fd churn, scheduler fairness under
many short-lived connections.
"""
import socket

import harness
import netutil

recv_exact = netutil.recv_exact


def echo_handler(H, conn):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)
    except OSError:
        pass
    finally:
        netutil.close_quiet(conn)


def setup(H):
    servers = []
    for ip in H.net_ips:
        srv = netutil.listen_tcp(host=ip)
        H.register_close(srv)
        H.fiber(netutil.serve_forever, H, srv,
             lambda conn, addr: H.fiber(echo_handler, H, conn))
        servers.append((ip, srv.getsockname()[1]))
    H.state = {"servers": servers}


def client(H, wid, rng, state):
    servers = state["servers"]
    # Spread the initial connect storm deterministically.
    H.sleep(rng.random() * 0.5)
    for _ in H.round_range():
        sock = None
        did = 0
        host, port = servers[rng.randrange(len(servers))]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            n = rng.randint(1, 8)
            for _ in range(n):
                if not H.running():
                    break
                payload = rng.randbytes(rng.randint(1, 512))
                sock.sendall(payload)
                got = recv_exact(sock, len(payload))
                if not H.check(got == payload,
                               "echo mismatch wid={0} ({1} != {2})".format(
                                   wid, len(got), len(payload))):
                    return
                H.op(wid)
                did += 1
            if did:
                H.task_done(wid)
        except OSError:
            if not H.running():
                break
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p01_tcp_echo", body, setup=setup, default_funcs=10000,
                 describe="10k TCP echo clients against one local echo server")
