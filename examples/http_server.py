"""Tiny HTTP server — and stdlib clients that "just work".

The selling point of pygo.monkey.patch(): code that was written to
block runs cooperatively, unchanged.  Here a hand-rolled HTTP/1.0
server accepts connections in blocking style, while several
urllib.request.urlopen() clients fetch from it concurrently — all on
one OS thread, all parking on netpoll under the hood.

Run:
    python3 examples/http_server.py
"""
import os
import socket
import sys
from urllib.request import urlopen

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo.monkey
import pygo_core

pygo.monkey.patch()

BODY = b"Hello from pygo!\n"
NUM_CLIENTS = 5


def serve_one(conn):
    try:
        conn.recv(4096)                   # read (and ignore) the request
        head = b"HTTP/1.0 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n"
        conn.sendall(head % len(BODY) + BODY)
    finally:
        conn.close()


def http_server(ready, n_requests):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    s.listen(16)
    ready.send(s.getsockname()[1])        # tell main which port we got
    for _ in range(n_requests):
        conn, _ = s.accept()
        pygo.go(serve_one, conn)          # one goroutine per connection
    s.close()


def fetcher(fid, port, results):
    body = urlopen("http://127.0.0.1:{0}/".format(port)).read()
    results.send((fid, body))


def main():
    ready = pygo_core.Chan(1)
    pygo.go(http_server, ready, NUM_CLIENTS)
    port = ready.recv()[0]

    results = pygo_core.Chan(NUM_CLIENTS)
    for fid in range(NUM_CLIENTS):
        pygo.go(fetcher, fid, port, results)

    for _ in range(NUM_CLIENTS):
        fid, body = results.recv()[0]
        print("fetcher {0} got: {1}".format(fid, body.decode().strip()))


if __name__ == "__main__":
    pygo.run(main)
