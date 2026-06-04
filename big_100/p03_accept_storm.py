"""big_100 / 03 -- TCP accept storm.

One listener.  Tens of thousands of goroutines connect and disconnect as fast
as they can.  The server accepts every connection; we track that the number of
connections accepted by the server tracks the number completed by clients (no
accepted connection is silently dropped, and no handler is leaked).

Stresses: accept wakeups, fd lifetime, teardown churn.
"""
import socket
import threading

import harness
import netutil


def setup(H):
    srv = netutil.listen_tcp(backlog=8192)
    H.state = {"port": srv.getsockname()[1]}
    H.counters = {"accepted": 0, "closed": 0, "live": 0, "max_live": 0}
    H.clock = threading.Lock()

    def bump(key, delta):
        with H.clock:
            H.counters[key] += delta
            if H.counters["live"] > H.counters["max_live"]:
                H.counters["max_live"] = H.counters["live"]

    H.bump = bump

    def handler(conn):
        bump("live", 1)
        try:
            # Drain whatever the client sends, then it closes -> EOF.
            while True:
                data = conn.recv(256)
                if not data:
                    break
        except OSError:
            pass
        finally:
            netutil.close_quiet(conn)
            bump("live", -1)
            bump("closed", 1)

    def on_conn(conn, addr):
        bump("accepted", 1)
        H.go(handler, conn)

    H.go(netutil.serve_forever, H, srv, on_conn)


def client(H, wid, rng, state):
    port = state["port"]
    H.sleep(rng.random() * 0.3)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            if rng.random() < 0.5:
                sock.sendall(b"hi")
            H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)

    def auditor():
        # Live handler count must stay bounded (no handler leak): it should
        # track in-flight connections, never grow without bound.
        while H.running():
            live = H.counters["live"]
            H.check(live <= H.funcs + 1000,
                    "handler leak: {0} live handlers (funcs={1})".format(
                        live, H.funcs))
            H.sleep(1.0)
        acc = H.counters["accepted"]
        clo = H.counters["closed"]
        H.log("accepted={0} closed={1} max_live={2} diff={3}".format(
            acc, clo, H.counters["max_live"], acc - clo))

    H.go(auditor)


if __name__ == "__main__":
    harness.main("p03_accept_storm", body, setup=setup, default_funcs=12000,
                 describe="rapid connect/disconnect; accepted tracks completed")
