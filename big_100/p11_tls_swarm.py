"""big_100 / 11 -- TLS handshake swarm.

A local TLS echo server with a self-signed cert (generated once via the
openssl CLI into a temp dir).  Thousands of goroutines connect, complete a
full TLS handshake, exchange an encrypted echo, and disconnect.

Stresses: the OpenSSL blocking paths (do_handshake / SSL_read / SSL_write made
cooperative by monkey), socket+TLS integration, a CPU/I-O mix.
"""
import os
import socket
import ssl
import subprocess
import tempfile

import harness
import netutil


def make_cert():
    d = tempfile.mkdtemp(prefix="big100_tls_")
    cert = os.path.join(d, "cert.pem")
    key = os.path.join(d, "key.pem")
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", key, "-out", cert, "-days", "1",
         "-subj", "/CN=localhost"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return cert, key


def setup(H):
    cert, key = make_cert()
    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sctx.load_cert_chain(cert, key)
    cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    cctx.check_hostname = False
    cctx.verify_mode = ssl.CERT_NONE

    srv = netutil.listen_tcp()
    H.state = {"port": srv.getsockname()[1], "host": srv.getsockname()[0], "cctx": cctx}
    H.register_close(srv)

    def handler(raw):
        tls = None
        try:
            tls = sctx.wrap_socket(raw, server_side=True)
            while True:
                data = tls.recv(4096)
                if not data:
                    break
                tls.sendall(data)
        except (OSError, ssl.SSLError):
            pass
        finally:
            try:
                (tls or raw).close()
            except OSError:
                pass

    def accept_loop():
        while H.running():
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            H.go(handler, conn)

    H.go(accept_loop)


def client(H, wid, rng, state):
    port = state["port"]

    host = state["host"]
    cctx = state["cctx"]
    H.sleep(rng.random() * 0.8)
    while H.running():
        tls = None
        try:
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw.connect((host, port))
            tls = cctx.wrap_socket(raw, server_hostname="localhost")
            for _ in range(rng.randint(1, 4)):
                if not H.running():
                    break
                payload = rng.randbytes(rng.randint(1, 400))
                tls.sendall(payload)
                got = netutil.recv_exact(tls, len(payload))
                if not H.check(got == payload,
                               "tls echo mismatch wid={0}".format(wid)):
                    return
                H.op(wid)
            H.task_done(wid)
        except (OSError, ssl.SSLError):
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(tls)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main("p11_tls_swarm", body, setup=setup, default_funcs=4000,
                 describe="self-signed TLS handshake swarm + encrypted echo")
