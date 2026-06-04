"""big_100 / 04 -- bidirectional TCP proxy.

A local proxy sits between clients and a backend echo server.  For each client
connection the proxy opens a backend connection and copies bytes both ways
with two goroutines.  Clients pipeline many messages (data in flight in both
directions at once) and verify every echo comes back in order.

Stresses: simultaneous recv/send, half-close, backpressure, 3 hops of sockets.
"""
import socket

import harness          # sets up sys.path so `runloom` imports
import netutil
import runloom


def pipe(src, dst, done):
    """Copy src -> dst until src EOF, then half-close dst's WRITE side (a
    network FIN, which wakes whoever is reading dst).  Never closes a socket
    another goroutine may be parked on -- proxy_conn closes both fds only
    after BOTH halves have signalled `done`."""
    try:
        while True:
            data = src.recv(65536)
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
        done.send(1)


def proxy_conn(H, client_sock, backend_addr):
    backend = None
    try:
        backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend.connect(backend_addr)
    except OSError:
        netutil.close_quiet(client_sock)
        netutil.close_quiet(backend)
        return
    done = runloom.Chan(2)
    H.go(pipe, client_sock, backend, done)
    H.go(pipe, backend, client_sock, done)
    # Join both halves (each ends on a network FIN, never a forced close)
    # before tearing the fds down, so neither half is parked at close time.
    done.recv()
    done.recv()
    netutil.close_quiet(client_sock)
    netutil.close_quiet(backend)


def setup(H):
    backend_port = netutil.start_echo_server(H)
    backend_addr = ("127.0.0.1", backend_port)

    pxy = netutil.listen_tcp()
    proxy_port = pxy.getsockname()[1]
    H.state = {"proxy_port": proxy_port}

    H.go(netutil.serve_forever, H, pxy,
         lambda conn, addr: H.go(proxy_conn, H, conn, backend_addr))


def client(H, wid, rng, state):
    port = state["proxy_port"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("127.0.0.1", port))
            msgs = [rng.randbytes(rng.randint(4, 200)) for _ in
                    range(rng.randint(2, 8))]
            framed = b"".join(
                len(m).to_bytes(4, "big") + m for m in msgs)
            sock.sendall(framed)           # pipeline all at once
            for m in msgs:                  # read echoes back in order
                hdr = netutil.recv_exact(sock, 4)
                ln = int.from_bytes(hdr, "big")
                got = netutil.recv_exact(sock, ln)
                if not H.check(got == m,
                               "proxy corrupted frame wid={0}".format(wid)):
                    return
                H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
            H.sleep(0.005)
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p04_tcp_proxy", body, setup=setup, default_funcs=6000,
                 describe="client<->proxy<->backend full-duplex pipelining")
