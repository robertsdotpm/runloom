"""big_100 / 15 -- SOCKS-like CONNECT proxy.

A minimal HTTP-CONNECT tunnel proxy.  Clients send `CONNECT host:port` and,
once the proxy answers 200, get a raw byte tunnel to a backend echo server.
Many clients tunnel concurrently and verify their echoes.

Stresses: layered protocols, bidirectional copy, connection setup teardown.
"""
import socket

import harness
import netutil
import runloom


def tunnel_half(src, dst, done):
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


def proxy_conn(H, client_sock):
    backend = None
    try:
        req = netutil.recv_until(client_sock, b"\r\n\r\n")
        line = req.split(b"\r\n", 1)[0].decode("latin-1")
        parts = line.split(" ")
        if len(parts) < 2 or parts[0] != "CONNECT":
            client_sock.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        host, _, port = parts[1].partition(":")
        backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        backend.connect((host, int(port)))
        client_sock.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        done = runloom.Chan(2)
        H.go(tunnel_half, client_sock, backend, done)
        H.go(tunnel_half, backend, client_sock, done)
        done.recv()
        done.recv()
    except OSError:
        pass
    finally:
        netutil.close_quiet(client_sock)
        netutil.close_quiet(backend)


def setup(H):
    host = H.net_ips[0]
    backend_port = netutil.start_echo_server(H, host=host)
    pxy = netutil.listen_tcp(host=host)
    H.state = {"host": host, "proxy_port": pxy.getsockname()[1],
               "backend_port": backend_port}

    H.go(netutil.serve_forever, H, pxy,
         lambda conn, addr: H.go(proxy_conn, H, conn))


def client(H, wid, rng, state):
    host = state["host"]
    pport = state["proxy_port"]
    bport = state["backend_port"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind((host, 0))
            sock.connect((host, pport))
            sock.sendall(
                "CONNECT {0}:{1} HTTP/1.1\r\n\r\n".format(host, bport)
                .encode("latin-1"))
            resp = netutil.recv_until(sock, b"\r\n\r\n")
            if not H.check(b" 200 " in resp,
                           "proxy did not establish tunnel wid={0}".format(wid)):
                return
            for _ in range(rng.randint(1, 5)):
                if not H.running():
                    break
                payload = rng.randbytes(rng.randint(1, 256))
                sock.sendall(payload)
                got = netutil.recv_exact(sock, len(payload))
                if not H.check(got == payload,
                               "tunnel echo mismatch wid={0}".format(wid)):
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
    harness.main("p15_socks_proxy", body, setup=setup, default_funcs=6000,
                 describe="HTTP CONNECT tunnel proxy to a backend echo server")
