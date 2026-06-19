"""The [greenlet] column of the speed benchmark (GIL build, single-threaded).

Raw `greenlet` has no scheduler or I/O loop, so:
  * spawn / ctxswitch use raw greenlet (the purest cooperative switch);
  * rtt / http use gevent (greenlet + libev), the greenlet ecosystem's I/O layer.
The report labels these "greenlet (gevent for I/O)".

  spawn      : create N greenlets, switch into each  -> seconds
  ctxswitch  : round-robin hub over G greenlets, K rounds (loaded-yield) -> seconds
  rtt        : gevent socket, N sequential round-trips to a Go echo server
  http       : H greenlets, keepalive HTTP/1.1 GET vs a Go httpd, windowed req/s
"""
import argparse
import json
import time

import greenlet

HTTP_REQ = b"GET / HTTP/1.1\r\nHost: b\r\nConnection: keep-alive\r\n\r\n"


def m_spawn(n):
    def noop():
        pass
    t0 = time.perf_counter()
    gs = [greenlet.greenlet(noop) for _ in range(n)]
    for g in gs:
        g.switch()
    return {"seconds": time.perf_counter() - t0, "n": n, "cores": 1}


def m_ctxswitch(n):
    G = 64
    K = max(1, n // (2 * G))
    hub = greenlet.getcurrent()

    def worker():
        for _ in range(K):
            hub.switch()
    workers = [greenlet.greenlet(worker) for _ in range(G)]
    t0 = time.perf_counter()
    for _ in range(K):
        for w in workers:
            w.switch()
    dt = time.perf_counter() - t0
    return {"seconds": dt, "switches": 2 * G * K, "fibers": G, "rounds": K,
            "cores": 1}


def m_rtt(host, port, n, payload):
    import socket
    from gevent import socket as gsocket
    s = gsocket.create_connection((host, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    msg = b"\xab" * payload

    def recvn(k):
        got = 0
        while got < k:
            b = s.recv(k - got)
            if not b:
                return False
            got += len(b)
        return True

    for _ in range(1000):
        s.sendall(msg)
        recvn(payload)
    t0 = time.perf_counter()
    for _ in range(n):
        s.sendall(msg)
        recvn(payload)
    dt = time.perf_counter() - t0
    s.close()
    return {"ns_per_rtt": dt * 1e9 / n, "n": n, "payload": payload, "cores": 1}


def _content_length(header):
    for line in header.split(b"\r\n"):
        if line[:15].lower() == b"content-length:":
            return int(line.split(b":", 1)[1])
    return 0


def m_http(host, port, conns, ramp, measure):
    import socket
    import gevent
    from gevent import socket as gsocket
    state = {"measuring": False, "stop": False}
    counters = [0] * conns

    def worker(idx):
        try:
            s = gsocket.create_connection((host, port))
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            return
        buf = b""
        while not state["stop"]:
            s.sendall(HTTP_REQ)
            buf = b""
            while b"\r\n\r\n" not in buf:
                d = s.recv(4096)
                if not d:
                    return
                buf += d
            header, _, rest = buf.partition(b"\r\n\r\n")
            cl = _content_length(header)
            body = rest
            while len(body) < cl:
                d = s.recv(4096)
                if not d:
                    return
                body += d
            if state["measuring"]:
                counters[idx] += 1
        s.close()

    greenlets = [gevent.spawn(worker, i) for i in range(conns)]
    gevent.sleep(ramp)
    state["measuring"] = True
    t0 = time.perf_counter()
    gevent.sleep(measure)
    state["measuring"] = False
    state["stop"] = True
    elapsed = time.perf_counter() - t0
    gevent.killall(greenlets, block=True, timeout=2)
    total = sum(counters)
    return {"rps": total / elapsed, "reqs": total, "measure_s": elapsed,
            "conns": conns, "cores": 1}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metric", required=True,
                    choices=["spawn", "ctxswitch", "rtt", "http"])
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--payload", type=int, default=64)
    ap.add_argument("--conns", type=int, default=64)
    ap.add_argument("--ramp", type=float, default=1.0)
    ap.add_argument("--measure", type=float, default=3.0)
    args = ap.parse_args()

    if args.metric == "spawn":
        res = m_spawn(args.n)
    elif args.metric == "ctxswitch":
        res = m_ctxswitch(args.n)
    elif args.metric == "rtt":
        res = m_rtt(args.host, args.port, args.n, args.payload)
    else:
        res = m_http(args.host, args.port, args.conns, args.ramp, args.measure)
    res.update({"runtime": "greenlet", "metric": args.metric})
    print(json.dumps(res))


if __name__ == "__main__":
    main()
