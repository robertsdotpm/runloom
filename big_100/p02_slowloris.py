"""big_100 / 02 -- TCP slowloris simulator.

A local request/reply server.  Most goroutines are "fast" clients doing quick
request/response round-trips; a large fraction are "slow" clients that hold a
connection open and dribble one byte every few seconds without ever completing
a request.  The server must keep serving the fast clients -- a fast round-trip
that ever exceeds the fairness bound means the slow clients starved everyone.

Stresses: blocking reads, scheduler fairness, idle sockets, timers.
"""
import socket
import time

import harness
import netutil

FAIRNESS_BOUND = 10.0   # seconds; a fast round-trip slower than this = starvation


def server_handler(conn):
    try:
        while True:
            line = netutil.recv_until(conn, b"\n")
            # One reply per newline-terminated request.
            conn.sendall(b"OK " + line)
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


def fast_client(H, wid, rng, state):
    port = state["port"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            for _ in range(rng.randint(1, 20)):
                if not H.running():
                    break
                t0 = time.monotonic()
                sock.sendall(b"ping\n")
                reply = netutil.recv_until(sock, b"\n")
                dt = time.monotonic() - t0
                H.check(reply.startswith(b"OK "),
                        "bad reply wid={0}: {1!r}".format(wid, reply[:32]))
                H.check(dt < FAIRNESS_BOUND,
                        "starvation: fast round-trip took {0:.1f}s "
                        "(wid={1})".format(dt, wid))
                H.op(wid)
                H.sleep(rng.random() * 0.05)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(sock)


def slow_client(H, wid, rng, state):
    port = state["port"]
    H.sleep(rng.random() * 1.0)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            # Dribble a never-completed request: one byte every few seconds.
            for _ in range(rng.randint(20, 60)):
                if not H.running():
                    break
                sock.sendall(b"x")          # no newline -> server keeps waiting
                H.op(wid)                    # progress is "bytes dribbled"
                H.sleep(2.0 + rng.random() * 3.0)
        except OSError:
            pass
        finally:
            netutil.close_quiet(sock)
        H.sleep(0.05)


def body(H):
    slow = H.funcs // 2
    fast = H.funcs - slow
    H.run_pool(slow, slow_client, H.state)
    H.run_pool(fast, fast_client, H.state)


if __name__ == "__main__":
    harness.main("p02_slowloris", body, setup=setup, default_funcs=8000,
                 describe="slowloris: dribbling clients must not starve fast ones")
