"""big_100 / 88 -- local package mirror simulator.

An HTTP server serves package metadata and tarballs but, like a flaky mirror,
randomly DELAYS a response or DROPS the connection before answering.  Clients
fetch metadata then the tarball, retrying on failure, and verify the tarball's
content -- so the retry path must be robust and the eventually-delivered bytes
correct.

Stresses: realistic HTTP-ish behaviour, dropped connections, client retries.
"""
import hashlib
import socket

import harness
import httputil
import netutil
import runloom

NPKGS = 64


def tarball(idx):
    base = (idx * 97) & 0xFF
    return bytes(((base + j * 13) % 251) for j in range(2048 + (idx % 7) * 512))


def setup(H):
    digests = {i: hashlib.sha256(tarball(i)).hexdigest() for i in range(NPKGS)}

    def handle(conn):
        import random
        r = random.Random(conn.fileno() * 2654435761)
        try:
            while True:
                method, path, headers, keep_alive = httputil.read_request(conn)
                # Flaky behaviour: sometimes drop, sometimes delay.
                roll = r.random()
                if roll < 0.2:
                    return                      # drop: close without replying
                if roll < 0.35:
                    runloom.sleep(r.uniform(0.005, 0.05))
                if path.startswith("/meta/"):
                    idx = int(path[6:])
                    httputil.send_response(
                        conn, '{{"id":{0},"size":{1}}}'.format(
                            idx, len(tarball(idx))),
                        content_type="application/json", keep_alive=keep_alive)
                elif path.startswith("/tarball/"):
                    idx = int(path[9:])
                    httputil.send_response(
                        conn, tarball(idx),
                        content_type="application/octet-stream",
                        keep_alive=keep_alive)
                else:
                    httputil.send_response(conn, "nf", status="404 Not Found",
                                           keep_alive=keep_alive)
                if not keep_alive:
                    break
        except (OSError, ValueError):
            pass
        finally:
            netutil.close_quiet(conn)

    servers = netutil.listen_all(H, lambda conn, addr: H.fiber(handle, conn))
    H.state = {"servers": servers, "digests": digests, "drops": [0] * 1024}


def fetch(H, host, port, path):
    """GET path with retries; returns (status, body) or None after giving up."""
    for _attempt in range(8):
        if not H.running():
            return None
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            return httputil.get(sock, path, keep_alive=False)
        except OSError:
            runloom.sleep(0.003)        # dropped/flaky -> retry
        finally:
            netutil.close_quiet(sock)
    return None


def client(H, wid, rng, state):
    servers = state["servers"]
    digests = state["digests"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        idx = rng.randrange(NPKGS)
        host, port = netutil.pick_server(servers, rng)
        meta = fetch(H, host, port, "/meta/{0}".format(idx))
        if meta is None:
            continue
        if not H.check(meta[0] == 200 and str(idx).encode() in meta[1],
                       "bad metadata wid={0} idx={1}".format(wid, idx)):
            return
        host, port = netutil.pick_server(servers, rng)
        tb = fetch(H, host, port, "/tarball/{0}".format(idx))
        if tb is None:
            continue
        if not H.check(
                tb[0] == 200
                and hashlib.sha256(tb[1]).hexdigest() == digests[idx],
                "tarball corrupt wid={0} idx={1} ({2} bytes)".format(
                    wid, idx, len(tb[1]))):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p88_package_mirror", body, setup=setup, default_funcs=3000,
                 describe="flaky mirror (delays/drops); clients retry + verify")
