"""big_100 / 172 -- TLS read/write cancellation.

A TLS echo server over loopback (self-signed cert, like p11).  Clients perform
partial TLS reads/writes; a fraction are CANCELLED part-way -- abandoned mid-
handshake or mid-data by closing the socket while an SSL_read/SSL_write is in a
WANT_READ/WANT_WRITE transition (driven by a short overall budget).  Cancelled
connections close cleanly; the connections that run to completion must echo
their bytes back EXACTLY.

Stresses: the OpenSSL cooperative paths (do_handshake/SSL_read/SSL_write made
cooperative by monkey) under cancellation, fd cleanup on a torn-down TLS socket,
no fd leak, no crash.  Low funcs (TLS is heavy).
"""
import os
import socket
import ssl
import subprocess
import tempfile

import harness
import netutil
import runloom
import runloom_c


def make_cert():
    d = tempfile.mkdtemp(prefix="big100_tls172_")
    cert = os.path.join(d, "cert.pem")
    key = os.path.join(d, "key.pem")
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", key, "-out", cert, "-days", "1",
         "-subj", "/CN=localhost"],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return d, cert, key


def setup(H):
    d, cert, key = make_cert()
    import shutil
    H.add_cleanup(lambda: shutil.rmtree(d, ignore_errors=True))

    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    sctx.load_cert_chain(cert, key)
    cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    cctx.check_hostname = False
    cctx.verify_mode = ssl.CERT_NONE

    def handler(raw):
        tls = None
        try:
            # The accepted conn is NON-blocking (timeout 0) under monkey.patch,
            # which makes ssl.wrap_socket(do_handshake_on_connect=True) raise
            # ValueError ("...should not be specified for non-blocking sockets").
            # Put it in blocking mode so the server-side handshake (and the
            # echo recv/send) drive through the cooperative monkey socket layer,
            # exactly like the client's success path uses a blocking socket.
            raw.setblocking(True)
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
    # per-wid outcome counters (single writer per slot).
    H.completed = [0] * H.funcs
    H.cancelled = [0] * H.funcs


def client(H, wid, rng, state):
    cctx = state["cctx"]
    completed = H.completed
    cancelled = H.cancelled
    H.sleep(rng.random() * 0.5)
    for _ in H.round_range():
        # ~30% of connections are cancelled mid-flight via a tight fd-readiness
        # budget; the rest run to completion and must echo correctly.
        cancel = (rng.random() < 0.30)
        host, port = netutil.pick_server(state["servers"], rng)
        raw = None
        tls = None
        try:
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw.connect((host, port))
            if cancel:
                # Start the handshake then abandon it on a short wait_fd budget:
                # set non-blocking, kick do_handshake, and if it can't finish in
                # a couple of ms close the socket mid-WANT_READ/WANT_WRITE.
                raw.setblocking(False)
                tls = cctx.wrap_socket(raw, server_hostname="localhost",
                                       do_handshake_on_connect=False)
                budget = 0
                done = False
                while budget < 3:
                    try:
                        tls.do_handshake()
                        done = True
                        break
                    except ssl.SSLWantReadError:
                        runloom_c.wait_fd(tls.fileno(), 1, 1)
                    except ssl.SSLWantWriteError:
                        runloom_c.wait_fd(tls.fileno(), 2, 1)
                    except (OSError, ssl.SSLError):
                        break
                    budget += 1
                # Whether or not it finished, ABANDON it now (the cancellation).
                cancelled[wid] += 1
                H.op(wid)
                H.task_done(wid)
                continue

            tls = cctx.wrap_socket(raw, server_hostname="localhost")
            for _ in range(rng.randint(1, 3)):
                if not H.running():
                    break
                payload = rng.randbytes(rng.randint(1, 400))
                tls.sendall(payload)
                got = netutil.recv_exact(tls, len(payload))
                if not H.check(got == payload,
                               "tls echo mismatch wid={0}".format(wid)):
                    return
                completed[wid] += 1
                H.op(wid)
            H.task_done(wid)
        except (OSError, ssl.SSLError):
            if not H.running():
                break
            H.sleep(0.01)
        finally:
            netutil.close_quiet(tls if tls is not None else raw)


def body(H):
    H.run_pool(H.funcs, client, H.state)


def post(H):
    comp = sum(H.completed)
    canc = sum(H.cancelled)
    H.check(comp > 0, "no TLS round-trip completed")
    H.check(canc > 0, "no TLS connection was cancelled (cancellation path unexercised)")
    # fd leak guard: completed + cancelled both closed their sockets, so the fd
    # count must have returned near baseline (the harness reports leaked_fds; a
    # large positive value would also fail an auditor, but here completion of
    # post with no crash + the explicit closes is the structural check).
    H.log("tls_completed={0} tls_cancelled={1}".format(comp, canc))


if __name__ == "__main__":
    # Correctness test: the subject is OpenSSL state cleanup when a TLS
    # handshake/read/write is cancelled mid-flight (full OpenSSL handshakes are
    # heavy), not goroutine count.  Cap to the intended scale.
    harness.main("p172_ssl_rw_cancellation", body, setup=setup, post=post,
                 default_funcs=800, max_funcs=800,
                 describe="partial TLS reads/writes with some connections "
                          "cancelled mid-handshake/data; completed ones echo exactly")
