"""big_100 / 82 -- mini web server with range requests.

A static-file HTTP server backed by a temp dir of known-content files, with
HTTP Range support.  Many clients download whole files or random byte ranges
and verify the bytes against the deterministic content.

Stresses: file + socket I/O together, Range handling, partial responses.
"""
import socket
import threading

import harness
import httputil
import netutil

NFILES = 16

def content(idx, size):
    # Deterministic bytes: byte j of file idx = (idx*131 + j) % 251
    base = (idx * 131) & 0xFF
    return bytes(((base + j) % 251) for j in range(size))


def setup(H):
    base = H.make_tmpdir("big100_web_")
    files = {}
    for k in range(NFILES):
        size = 4096 + k * 4096
        data = content(k, size)
        files[k] = (size, data)
    host = H.net_ips[0]
    srv = netutil.listen_tcp(host=host)
    H.state = {"port": srv.getsockname()[1], "host": host, "files": files}

    def handle(conn):
        try:
            while True:
                method, path, headers, keep_alive = httputil.read_request(conn)
                try:
                    idx = int(path.lstrip("/").split(".")[0].lstrip("f"))
                    size, full = files[idx]
                except (ValueError, KeyError):
                    httputil.send_response(conn, "no", status="404 Not Found",
                                           keep_alive=keep_alive)
                    if not keep_alive:
                        break
                    continue
                rng = headers.get("range")
                if rng and rng.startswith("bytes="):
                    a, _, b = rng[6:].partition("-")
                    start = int(a)
                    end = int(b) if b else size - 1
                    body = full[start:end + 1]
                    hdr = ("HTTP/1.1 206 Partial Content\r\n"
                           "Content-Range: bytes {0}-{1}/{2}\r\n"
                           "Content-Length: {3}\r\n"
                           "Connection: {4}\r\n\r\n").format(
                               start, end, size, len(body),
                               "keep-alive" if keep_alive else "close")
                    conn.sendall(hdr.encode("latin-1") + body)
                else:
                    httputil.send_response(conn, full,
                                           content_type="application/octet-stream",
                                           keep_alive=keep_alive)
                if not keep_alive:
                    break
        except (OSError, ValueError):
            pass
        finally:
            netutil.close_quiet(conn)

    H.fiber(netutil.serve_forever, H, srv,
         lambda conn, addr: H.fiber(handle, conn))


def read_response(sock):
    raw = netutil.recv_until(sock, b"\r\n\r\n")
    head, rest = raw.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    status = int(lines[0].split(" ")[1])
    clen = 0
    for ln in lines[1:]:
        if ln.lower().startswith("content-length:"):
            clen = int(ln.split(":", 1)[1])
    body = bytearray(rest)
    while len(body) < clen:
        chunk = sock.recv(clen - len(body))
        if not chunk:
            raise OSError("eof")
        body += chunk
    return status, bytes(body)


def client(H, wid, rng, state):
    port = state["port"]
    host = state["host"]
    files = state["files"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            idx = rng.randrange(NFILES)
            size, full = files[idx]
            ranged = rng.random() < 0.5
            req = "GET /f{0}.bin HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
            if ranged:
                start = rng.randrange(0, size - 1)
                end = min(size - 1, start + rng.randrange(1, 2048))
                req += "Range: bytes={0}-{1}\r\n".format(start, end)
                expect = full[start:end + 1]
                expect_status = 206
            else:
                expect = full
                expect_status = 200
            sock.sendall((req.format(idx) + "\r\n").encode("latin-1"))
            status, body = read_response(sock)
            if not H.check(status == expect_status and body == expect,
                           "web download mismatch wid={0} idx={1} ranged={2} "
                           "({3} vs {4} bytes, status {5})".format(
                               wid, idx, ranged, len(body), len(expect),
                               status)):
                return
            H.op(wid)
            H.task_done(wid)
        except (OSError, ValueError):
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p82_web_server", body, setup=setup, default_funcs=4000,
                 describe="static web server with range requests; verify bytes")
