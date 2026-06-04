"""big_100 / 06 -- HTTP keep-alive torture.

A keep-alive HTTP server.  Each client goroutine reuses ONE connection for
many requests, and randomly slams the socket shut mid-conversation (sometimes
after sending only a partial request).  The server must cope with partial
reads and abrupt EOF without wedging other connections.

Stresses: partial reads, EOF mid-request, connection pooling/reuse.
"""
import socket

import harness
import httputil
import netutil


def setup(H):
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1]}
    H.register_close(srv)

    def handler(conn):
        served = 0
        try:
            while True:
                method, path, headers, keep_alive = httputil.read_request(conn)
                served += 1
                httputil.send_response(
                    conn, "req {0} path {1}".format(served, path),
                    keep_alive=keep_alive)
                if not keep_alive:
                    break
        except OSError:
            pass            # partial request / client vanished -- expected
        finally:
            netutil.close_quiet(conn)

    def accept_loop():
        while H.running():
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            H.go(handler, conn)

    H.go(accept_loop)


def client(H, wid, rng, state):
    port = state["port"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            reqs = rng.randint(5, 50)
            for i in range(reqs):
                if not H.running():
                    break
                # Occasionally abandon mid-request: send a partial request line
                # and bail, exercising the server's partial-read/EOF path.
                if rng.random() < 0.04:
                    sock.sendall(b"GET /part")     # no terminator
                    break
                status, bodydata = httputil.get(
                    sock, "/r{0}".format(i), keep_alive=True)
                if not H.check(status == 200,
                               "status {0} wid={1}".format(status, wid)):
                    return
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


if __name__ == "__main__":
    harness.main("p06_http_keepalive", body, setup=setup, default_funcs=8000,
                 describe="keep-alive reuse with random mid-request closes")
