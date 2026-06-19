"""Tiny HTTP server — and stdlib clients that "just work".

The selling point of runloom.monkey.patch(): code that was written to
block runs cooperatively, unchanged.  Here a hand-rolled HTTP/1.0
server accepts connections in blocking style, while several
urllib.request.urlopen() clients fetch from it concurrently — all on
one OS thread, all parking on netpoll under the hood.

Run:
    python3 examples/http_server.py
"""
import socket
from urllib.request import urlopen

import os

import runloom

# Free-threaded build: fan fibers across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

runloom.monkey.patch()

BODY = b"Hello from runloom!\n"
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
        runloom.fiber(serve_one, conn)          # one fiber per connection
    s.close()

def fetcher(fid, port, results):
    body = urlopen("http://127.0.0.1:{0}/".format(port)).read()
    results.send((fid, body))

def main():
    ready = runloom.Chan(1)
    runloom.fiber(http_server, ready, NUM_CLIENTS)
    port = ready.recv()[0]

    results = runloom.Chan(NUM_CLIENTS)
    for fid in range(NUM_CLIENTS):
        runloom.fiber(fetcher, fid, port, results)

    for _ in range(NUM_CLIENTS):
        fid, body = results.recv()[0]
        print("fetcher {0} got: {1}".format(fid, body.decode().strip()))

if __name__ == "__main__":
    runloom.run(HUBS, main)
