"""TCP echo server — the Go-feel proof.

Reads exactly like blocking code.  No async, no await.  Each client gets
its own goroutine; the goroutines park transparently on socket I/O via
the monkey-patch + netpoll.

Usage:
    python3 examples/echo_server.py          # starts server
    nc localhost 9000                         # client

To bench:
    python3 examples/echo_server.py          # starts server
    python3 examples/echo_client.py 100      # 100 concurrent clients
"""
import socket

import runloom

HOST = "127.0.0.1"
PORT = 9000

def handle(conn, addr):
    print("conn from", addr)
    buf = bytearray(4096)
    view = memoryview(buf)
    try:
        while True:
            n = conn.recv_into(buf)
            if not n:
                break
            conn.sendall(view[:n])
    finally:
        conn.close()
        print("closed", addr)

def main():
    runloom.monkey.patch()
    print("backend:", runloom.backend(), "netpoll:", runloom.netpoll_backend())
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(128)
    print("echo server on", HOST, PORT)

    def accept_loop():
        while True:
            conn, addr = listener.accept()
            runloom.go(lambda c=conn, a=addr: handle(c, a))

    runloom.go(accept_loop)
    runloom.run()

if __name__ == "__main__":
    main()
