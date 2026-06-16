"""big_100 / 11 -- TLS handshake swarm.

A local TLS echo server with a self-signed cert (generated once via the
openssl CLI into a temp dir).  Thousands of goroutines connect, complete a
full TLS handshake, exchange an encrypted echo, and disconnect.

Stresses: the OpenSSL blocking paths (do_handshake / SSL_read / SSL_write made
cooperative by monkey), socket+TLS integration, a CPU/I-O mix.
"""
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile

import harness
import netutil


def find_openssl():
    """Locate the openssl CLI.  It is on PATH on mac/Linux; on Windows it is
    usually present only under Git-for-Windows (shipped, but NOT on PATH), so
    probe there before giving up.  Returns the executable path or None.

    Both Git binaries mint a valid '/CN=localhost' cert with the args below
    (verified on the Win11 test box); the mingw64 build is the native one and
    is preferred over the MSYS usr/bin build, whose runtime can rewrite a
    leading-slash argument via path-conversion."""
    exe = shutil.which("openssl")
    if exe:
        return exe
    if sys.platform == "win32":
        for cand in (
                r"C:\Program Files\Git\mingw64\bin\openssl.exe",
                r"C:\Program Files\Git\usr\bin\openssl.exe",
                r"C:\Program Files (x86)\Git\mingw64\bin\openssl.exe",
                r"C:\Program Files (x86)\Git\usr\bin\openssl.exe"):
            if os.path.exists(cand):
                return cand
    return None


def make_cert():
    d = tempfile.mkdtemp(prefix="big100_tls_")
    cert = os.path.join(d, "cert.pem")
    key = os.path.join(d, "key.pem")
    exe = find_openssl()
    if exe is None:
        raise RuntimeError(
            "openssl CLI not found on PATH or under Git-for-Windows; it is "
            "needed to mint the self-signed test cert for p11_tls_swarm")
    subprocess.run(
        [exe, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
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

    servers = netutil.listen_all(H, lambda conn, addr: H.go(handler, conn))
    H.state = {"servers": servers, "cctx": cctx}


def client(H, wid, rng, state):
    cctx = state["cctx"]
    H.sleep(rng.random() * 0.8)
    while H.running():
        tls = None
        try:
            host, port = netutil.pick_server(state["servers"], rng)
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
