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
import sys

sys.path.insert(0, "src")
import pygo
import pygo.monkey
import pygo_core


HOST = "127.0.0.1"
PORT = 9000


def handle(conn, addr):
    print("conn from", addr)
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)
    finally:
        conn.close()
        print("closed", addr)


def main():
    pygo.monkey.patch()
    print("backend:", pygo_core.backend(), "netpoll:", pygo_core.netpoll_backend())
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((HOST, PORT))
    listener.listen(128)
    print("echo server on", HOST, PORT)

    def accept_loop():
        while True:
            conn, addr = listener.accept()
            pygo_core.go(lambda c=conn, a=addr: handle(c, a))

    pygo_core.go(accept_loop)
    pygo_core.run()


if __name__ == "__main__":
    main()
