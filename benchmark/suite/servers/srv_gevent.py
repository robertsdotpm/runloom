"""Baseline server: gevent StreamServer echo (greenlet + libev).

Single-threaded (decision #4: GIL build, best case). One greenlet per connection.
"""
import argparse
import socket

from gevent.server import StreamServer


def handle(sock, addr):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass
    while True:
        data = sock.recv(65536)
        if not data:
            break
        sock.sendall(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="10.99.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--token", default="")
    args = ap.parse_args()
    server = StreamServer((args.host, args.port), handle, backlog=4096)
    server.init_socket()
    server.start()
    print("LISTENING %d" % server.address[1], flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
