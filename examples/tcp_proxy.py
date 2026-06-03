"""TCP proxy — pump bytes both ways with two goroutines per connection.

A port forwarder is the textbook two-goroutines-per-connection job: one
goroutine copies client->upstream, another copies upstream->client, and
each blocks on recv without holding up the other.  Under
runloom.monkey.patch() these are plain blocking sockets.

Self-contained: it stands up an echo upstream, the proxy, and a client
as three goroutines and runs the traffic through end to end.

Run:
    python3 examples/tcp_proxy.py
"""
import socket

import runloom

runloom.monkey.patch()

def pump(src, dst):
    """Copy src -> dst until src reaches EOF, then half-close dst."""
    try:
        while True:
            data = src.recv(4096)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except OSError:
            pass

def handle(client, upstream_addr):
    upstream = socket.socket()
    upstream.connect(upstream_addr)
    # One goroutine each way; this goroutine handles the return path.
    runloom.go(pump, client, upstream)
    pump(upstream, client)
    client.close()
    upstream.close()

def proxy(ready, upstream_addr):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    s.listen(8)
    ready.send(s.getsockname())
    conn, _ = s.accept()                   # serve a single connection for the demo
    s.close()
    handle(conn, upstream_addr)

def echo_upstream(ready):
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    s.listen(8)
    ready.send(s.getsockname())
    conn, _ = s.accept()
    s.close()
    while True:
        data = conn.recv(4096)
        if not data:
            break
        conn.sendall(data)
    conn.close()

def client(proxy_addr):
    s = socket.socket()
    s.connect(proxy_addr)
    for i in range(3):
        s.sendall("line {0}\n".format(i).encode())
        print("client got:", s.recv(4096).decode().strip())
    s.shutdown(socket.SHUT_WR)             # EOF tears the whole chain down
    s.close()

def main():
    up_ready = runloom.Chan(1)
    proxy_ready = runloom.Chan(1)

    runloom.go(echo_upstream, up_ready)
    upstream_addr = up_ready.recv()[0]

    runloom.go(proxy, proxy_ready, upstream_addr)
    proxy_addr = proxy_ready.recv()[0]

    runloom.go(client, proxy_addr)

if __name__ == "__main__":
    runloom.run(main)
