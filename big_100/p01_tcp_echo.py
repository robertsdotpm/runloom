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
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1]}
    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(echo_handler, H, conn))


def client(H, wid, rng, state):
    port = state["port"]
    # Spread the initial connect storm deterministically so 10k clients don't
    # all SYN in the same instant (still a storm, just not a thundering one).
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        did = 0
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            rounds = rng.randint(1, 8)
            for _ in range(rounds):
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
            # Connect storms hit backlog limits -> brief backoff and retry.
            H.sleep(0.005)
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
