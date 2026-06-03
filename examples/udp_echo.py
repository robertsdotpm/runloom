"""UDP echo — cooperative datagrams with the pygo.sync front-end.

pygo.sync gives you blocking-style sockets without monkey-patching the
stdlib and without async/await: pygo.sync.udp_endpoint returns a
cooperative Socket whose recvfrom/sendto/recv/send park the goroutine
on netpoll.  Here a server and a client run as two goroutines in one
process and exchange a few datagrams over loopback.

Run:
    python3 examples/udp_echo.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo.sync
import pygo_core

ROUNDS = 3


def server(ready):
    sock = pygo.sync.udp_endpoint(local_addr=("127.0.0.1", 0))
    ready.send(sock.getsockname())        # hand the bound address to the client
    for _ in range(ROUNDS):
        data, addr = sock.recvfrom(1024)
        sock.sendto(b"echo:" + data, addr)
    sock.close()


def client(ready):
    addr = ready.recv()[0]
    sock = pygo.sync.udp_endpoint(remote_addr=addr)   # connected UDP
    for i in range(ROUNDS):
        sock.send("msg{0}".format(i).encode())
        reply = sock.recv(1024)
        print("client got:", reply.decode())
    sock.close()


def main():
    ready = pygo_core.Chan(1)
    pygo.go(server, ready)
    pygo.go(client, ready)


if __name__ == "__main__":
    pygo.run(main)
