"""Baseline server: gevent StreamServer echo (greenlet + libev).

Single-threaded (decision #4: GIL build, best case). One greenlet per connection.

--work N applies the SAME FNV-1a byte hash as the runloom work curve (py_fnv,
identical constants) N times over each chunk before echoing, folded into byte 0.
work=0 is the plain echo. Interpreted-Python reference (1 core) for the
cross-runtime handler work curve.
"""
import argparse
import socket

from gevent.server import StreamServer

FNV_OFF = 2166136261        # 0x811c9dc5
FNV_PRIME = 16777619        # 0x01000193
WORK = 0                    # set from --work in main()


def py_fnv(buf, n, passes):
    """Identical to srv_runloom_work.py:py_fnv -- pure inline arithmetic."""
    h = FNV_OFF
    for _ in range(passes):
        for i in range(n):
            h = ((h ^ buf[i]) * FNV_PRIME) & 0xffffffff
    return h


def handle(sock, addr):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass
    while True:
        data = sock.recv(65536)
        if not data:
            break
        if WORK:
            b = bytearray(data)
            h = py_fnv(b, len(b), WORK)
            b[0] = (b[0] ^ (h & 0xff)) & 0xff   # fold in -> no elision
            sock.sendall(b)
        else:
            sock.sendall(data)   # echo (work=0)


def main():
    global WORK
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--work", type=int, default=0, help="FNV passes per chunk (0 = echo)")
    ap.add_argument("--token", default="")
    args = ap.parse_args()
    WORK = args.work
    server = StreamServer((args.host, args.port), handle, backlog=4096)
    server.init_socket()
    server.start()
    print("LISTENING %d" % server.address[1], flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
