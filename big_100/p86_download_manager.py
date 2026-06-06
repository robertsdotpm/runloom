"""big_100 / 86 -- download manager.

A local HTTP server (with Range support) serves known-content files.  Each
client downloads a file by fetching several byte ranges IN PARALLEL (one
goroutine per range), reassembles them in order, and verifies the SHA-256 of
the whole file.  Failed range fetches are retried.

Stresses: parallel ranged I/O, reassembly, error recovery, hashing.
"""
import hashlib
import os
import socket

import harness
import httputil
import netutil
import runloom

NFILES = 12


def content(idx, size):
    base = (idx * 167) & 0xFF
    return bytes(((base + j * 7) % 251) for j in range(size))


def setup(H):
    base = H.make_tmpdir("big100_dl_")
    files = {}
    for k in range(NFILES):
        size = 16384 + k * 8192
        data = content(k, size)
        path = os.path.join(base, "f{0}".format(k))
        with open(path, "wb") as f:
            f.write(data)
        files[k] = (path, size, hashlib.sha256(data).hexdigest())
    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1], "host": srv.getsockname()[0], "files": files}

    def handle(conn):
        try:
            while True:
                method, path, headers, keep_alive = httputil.read_request(conn)
                try:
                    idx = int(path.lstrip("/f"))
                    fpath, size, _h = files[idx]
                except (ValueError, KeyError):
                    httputil.send_response(conn, "no", status="404 Not Found",
                                           keep_alive=keep_alive)
                    if not keep_alive:
                        break
                    continue
                rng = headers.get("range", "")
                with open(fpath, "rb") as fh:
                    if rng.startswith("bytes="):
                        a, _, b = rng[6:].partition("-")
                        start, end = int(a), int(b)
                        fh.seek(start)
                        chunk = fh.read(end - start + 1)
                    else:
                        chunk = fh.read()
                httputil.send_response(conn, chunk,
                                       content_type="application/octet-stream",
                                       keep_alive=keep_alive)
                if not keep_alive:
                    break
        except (OSError, ValueError):
            pass
        finally:
            netutil.close_quiet(conn)

    H.go(netutil.serve_forever, H, srv,
         lambda conn, addr: H.go(handle, conn))


def fetch_range(H, host, port, idx, start, end, result_idx, out):
    data = None
    for _attempt in range(4):
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            req = ("GET /f{0} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n"
                   "Range: bytes={1}-{2}\r\n\r\n").format(idx, start, end)
            sock.sendall(req.encode("latin-1"))
            _status, body = httputil.read_response(sock)
            data = body
            break
        except OSError:
            if not H.running():
                break
        finally:
            netutil.close_quiet(sock)
    out.send((result_idx, data))


def client(H, wid, rng, state):
    port = state["port"]
    host = state["host"]
    files = state["files"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        idx = rng.randrange(NFILES)
        _path, size, expect_hash = files[idx]
        nparts = rng.randint(2, 6)
        step = (size + nparts - 1) // nparts
        ranges = []
        for p in range(nparts):
            start = p * step
            end = min(size - 1, start + step - 1)
            if start <= end:
                ranges.append((start, end))
        out = runloom.Chan(len(ranges))
        for ri, (s, e) in enumerate(ranges):
            H.go(fetch_range, H, host, port, idx, s, e, ri, out)
        parts = [None] * len(ranges)
        ok = True
        for _ in range(len(ranges)):
            ri, data = out.recv()[0]
            if data is None:
                ok = False
            parts[ri] = data
        if not ok:
            if not H.running():
                break
            continue
        full = b"".join(parts)
        if not H.check(hashlib.sha256(full).hexdigest() == expect_hash,
                       "download hash mismatch wid={0} idx={1} ({2} bytes)"
                       .format(wid, idx, len(full))):
            return
        H.op(wid, len(ranges))
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p86_download_manager", body, setup=setup, default_funcs=2000,
                 describe="parallel ranged downloads, reassemble + verify hash")
