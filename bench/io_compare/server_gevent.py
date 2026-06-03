#!/usr/bin/env python3
"""gevent baseline -- greenlet + libev, pygo's DIRECT competitor: blocking-
style code, one greenlet per connection, single-core. Same wire protocol.
Usage: server_gevent.py [host] [port] [io_ms]"""
from gevent import monkey
monkey.patch_all()
import socket
import sys
import gevent
from gevent.server import StreamServer

REQ_LEN = 10
RESP = b"200 " + b"x" * 1024 + b"\n"
IO_S = 0.0


def recv_exactly(sock, n):
    out = bytearray()
    while len(out) < n:
        chunk = sock.recv(n - len(out))
        if not chunk:
            return b""
        out += chunk
    return bytes(out)


def handle(sock, addr):
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        while True:
            data = recv_exactly(sock, REQ_LEN)
            if not data:
                break
            if IO_S > 0:
                gevent.sleep(IO_S)
            sock.sendall(RESP)
    except OSError:
        pass
    finally:
        try:
            sock.close()
        except OSError:
            pass


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    IO_S = (float(sys.argv[3]) if len(sys.argv) > 3 else 0.0) / 1000.0
    server = StreamServer((host, port), handle, backlog=65535)
    server.init_socket()
    print("gevent-server listening on %s io=%sms" % ((host, port), IO_S * 1000), flush=True)
    server.serve_forever()
