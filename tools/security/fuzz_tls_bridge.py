"""S7 -- adversarial TLS fuzzing of the runloom.aio bridge transport.

Extends S6 (plaintext transport fuzz) to the TLS path, the highest-value REMOTE
attack surface: a real `runloom.aio` TLS echo server is hit with malformed TLS
records (garbage, truncated/oversized handshakes, RST mid-handshake, valid
handshake then post-handshake garbage, slow-drip, connect storms) while we watch
for a crash, a hang (lost wakeup), an ASan error, or unresponsiveness.  This is
where a remote memory bug in the bridge's TLS handling would live -- and it is
exactly the path the S1 info-leak finding cared about (OpenSSL handshakes run on
the recycled goroutine stacks, so a TLS bug here is the sensitive one).

Contract: every malformed input must be rejected (SSL error -> connection
dropped), the server must stay ALIVE + RESPONSIVE to a legitimate TLS client,
and (under ASan) there must be no memory error.  Run under ASan to also catch a
non-crashing transport OOB / an S5 stack-pool use-after-recycle.

    PYTHON_GIL=0 PYTHONPATH=src python tools/security/fuzz_tls_bridge.py --iters 800
"""
import argparse
import os
import random
import socket
import ssl
import sys
import tempfile
import time

HOST = "127.0.0.1"


def gen_cert():
    """Self-signed cert+key -> (certfile, keyfile).  Uses cryptography."""
    import datetime
    from cryptography import x509
    from cryptography.x509.oid import NameOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, u"localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .sign(key, hashes.SHA256()))
    cd, certfile = tempfile.mkstemp(suffix=".pem"); os.close(cd)
    kd, keyfile = tempfile.mkstemp(suffix=".pem"); os.close(kd)
    with open(certfile, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(keyfile, "wb") as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.TraditionalOpenSSL,
                                  serialization.NoEncryption()))
    return certfile, keyfile


def run_server(port_file, certfile, keyfile):
    import runloom.aio as paio

    async def handle(reader, writer):
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except Exception:        # noqa: BLE001  (a reset/SSL error is expected)
            pass
        finally:
            try:
                writer.close()
            except Exception:    # noqa: BLE001
                pass

    async def main():
        import asyncio
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile, keyfile)
        # start_server returns an ALREADY-serving server (its accept loop is
        # running); just publish the port and keep the loop alive until the
        # parent terminates us.
        server = await paio.start_server(handle, HOST, 0, ssl=ctx,
                                         ssl_handshake_timeout=2.0)
        port = server.sockets[0].getsockname()[1]
        with open(port_file, "w") as f:
            f.write(str(port))
        await asyncio.Event().wait()

    paio.run(main())


# ---- malformed-TLS attacks (raw socket; OpenSSL must reject, server survives) --

def _raw(port, timeout=2.0):
    s = socket.socket()
    s.settimeout(timeout)
    s.connect((HOST, port))
    return s


def tls_chaos(port, rng):
    kind = rng.randrange(9)
    try:
        s = _raw(port)
    except OSError:
        return
    try:
        if kind == 0:                                   # pure garbage (not TLS)
            s.sendall(os.urandom(rng.randint(1, 4096)))
        elif kind == 1:                                 # TLS record header + garbage body
            s.sendall(b"\x16\x03\x01" + os.urandom(rng.randint(2, 600)))
        elif kind == 2:                                 # oversized record length, truncated body
            s.sendall(b"\x16\x03\x01\xff\xff" + os.urandom(rng.randint(0, 40)))
        elif kind == 3:                                 # plausible ClientHello prefix, truncated
            s.sendall(b"\x16\x03\x01\x00\x2e\x01\x00\x00\x2a\x03\x03" + os.urandom(8))
        elif kind == 4:                                 # RST mid-handshake
            s.sendall(b"\x16\x03\x01\x00\x10" + os.urandom(4))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                         __import__("struct").pack("ii", 1, 0))
            s.close(); return
        elif kind == 5:                                 # slow-drip a handshake byte at a time
            for b in (b"\x16\x03\x01\x00\x05" + os.urandom(5)):
                s.sendall(bytes([b])); time.sleep(0.001)
        elif kind == 6:                                 # half-close then dangle
            s.sendall(b"\x16\x03\x01\x00\x08" + os.urandom(2))
            s.shutdown(socket.SHUT_WR)
        elif kind == 7:                                 # wrong content-type bytes
            s.sendall(bytes([rng.randint(0, 255)]) + b"\x03\x03" + os.urandom(20))
        else:                                           # connect + immediate close (storm)
            pass
        try:
            s.recv(64)
        except OSError:
            pass
    except OSError:
        pass
    finally:
        try:
            s.close()
        except OSError:
            pass


def clean_tls_echo(port, certfile):
    """A LEGIT TLS client: full handshake + echo round-trip -> server is alive."""
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(certfile)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    raw = socket.create_connection((HOST, port), timeout=4.0)
    try:
        s = ctx.wrap_socket(raw, server_hostname="localhost")
        msg = b"PING-" + os.urandom(8).hex().encode()
        s.sendall(msg)
        got = s.recv(len(msg) + 16)
        s.close()
        return got == msg
    except Exception:            # noqa: BLE001
        try:
            raw.close()
        except OSError:
            pass
        return False


def main():
    import subprocess
    p = argparse.ArgumentParser()
    p.add_argument("--iters", type=int, default=800)
    p.add_argument("--seed", type=int, default=None)
    a = p.parse_args()
    seed = a.seed if a.seed is not None else int.from_bytes(os.urandom(4), "little")
    rng = random.Random(seed)
    print("fuzz_tls_bridge seed=%d iters=%d" % (seed, a.iters))

    certfile, keyfile = gen_cert()
    pf = tempfile.mktemp(suffix=".port")
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    srv = subprocess.Popen(
        [sys.executable, "-c",
         "import sys; sys.path.insert(0,'src');"
         "from tools.security.fuzz_tls_bridge import run_server;"
         "run_server(%r, %r, %r)" % (pf, certfile, keyfile)],
        env=env, cwd=os.getcwd())
    try:
        # wait for the server to bind
        for _ in range(100):
            if os.path.exists(pf) and open(pf).read().strip():
                break
            if srv.poll() is not None:
                print("FAIL: server died at startup"); sys.exit(1)
            time.sleep(0.1)
        port = int(open(pf).read().strip())
        if not clean_tls_echo(port, certfile):
            print("FAIL: server not responsive to a clean TLS client at startup"); sys.exit(1)

        for i in range(a.iters):
            tls_chaos(port, rng)
            if srv.poll() is not None:
                print("FAIL: server CRASHED after %d malformed iters (rc=%s)"
                      % (i, srv.returncode)); sys.exit(1)
            if i % 100 == 99:                # periodic liveness (hang/lost-wakeup check)
                if not clean_tls_echo(port, certfile):
                    print("FAIL: server UNRESPONSIVE after %d iters (hang/leak?)" % (i + 1))
                    sys.exit(1)
        if not clean_tls_echo(port, certfile):
            print("FAIL: server unresponsive at end"); sys.exit(1)
        print("TLS_FUZZ_OK %d iters, server alive+responsive, seed=%d" % (a.iters, seed))
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=5)
        except Exception:        # noqa: BLE001
            srv.kill()
        for f in (certfile, keyfile, pf):
            try:
                os.unlink(f)
            except OSError:
                pass


if __name__ == "__main__":
    main()
