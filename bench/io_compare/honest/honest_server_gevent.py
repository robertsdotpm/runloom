#!/usr/bin/env python3
"""HONEST_BENCH gevent server -- one greenlet per conn; the 100ms CPU tier
blocks the single OS thread (no preemption). Usage: host port [io_unused]"""
from gevent import monkey; monkey.patch_all()
import os, random, socket, sys
import gevent
from gevent.server import StreamServer
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import workload as w
REQ_LEN = 10; RESP = b"200 " + b"x" * 1024 + b"\n"


def recv_exactly(sock, n):
    out = bytearray()
    while len(out) < n:
        c = sock.recv(n - len(out))
        if not c: return b""
        out += c
    return bytes(out)


def handle(sock, addr):
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    rng = random.Random()
    try:
        while True:
            if not recv_exactly(sock, REQ_LEN): break
            kind, dur = w.tier(rng.random())
            if kind == "io":
                gevent.sleep(dur)
            else:
                w.burn_cpu(100)               # blocks the hub (pathological)
            sock.sendall(RESP)
    except OSError:
        pass
    finally:
        try: sock.close()
        except OSError: pass

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    srv = StreamServer((host, port), handle, backlog=65535); srv.init_socket()
    print("honest-gevent listening", (host, port), flush=True)
    srv.serve_forever()
