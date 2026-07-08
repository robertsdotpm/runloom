"""Edge coverage for the monkey-patched ssl.SSLSocket cooperative paths.

runloom.monkey makes SSLSocket.recv/recv_into/send/sendall/do_handshake/unwrap
and SSLContext.wrap_socket cooperative: a WANT_READ/WANT_WRITE from OpenSSL parks
the fiber on wait_fd instead of spinning or wedging the hub (src/runloom/monkey/
tls.py).  test_swarm_monkey.py covers the headline handshake; this file drills
three under-covered edges:

  (1) SSLSocket.recv_into into a caller-supplied bytearray -- both the
      nbytes=None branch (fill available) and the explicit-nbytes branch (read at
      most nbytes) -- must PARK on WANT_READ until the peer sends, then return the
      right count with the right bytes.
  (2) SSLSocket.sendall of a multi-MB payload to a deliberately slow reader --
      the underlying socket buffer fills, OpenSSL returns WANT_WRITE, the sender
      parks (partial-send loop in _patched_ssl_sendall) -- must deliver every byte
      intact AND let a sibling fiber advance while it is parked (real overlap, not
      a hub stall).
  (3) SSLContext.wrap_socket with CERT_REQUIRED against a self-signed cert must
      raise ssl.SSLCertVerificationError PROMPTLY out of the cooperative
      do_handshake -- a verification failure is an SSLError, not a WANT_READ, so
      it must propagate, never hang the fiber on wait_fd.

Convention (mirrors test_swarm_monkey.py::test_ssl_cooperative_handshake_over_
socketpair): each scenario runs in a SUBPROCESS.  The SSLSocket close path does
not run monkey's socket.close() netpoll-unregister, so an in-process SSL run
leaves the socketpair fd NUMBERS arm-poisoned and a sibling test that reuses the
fd number hangs; a fresh child contains that residue and also contains any crash
as a negative return code.  The parent wraps subprocess.run in hang_guard so a
true wedge dumps stacks and _exits instead of blocking the suite.
"""
import ast
import atexit
import os
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))          # adv_util
from adv_util import hang_guard                                          # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(REPO, "src")


# --------------------------------------------------------------------------
# Self-signed cert, minted ONCE in the parent (cryptography preferred, openssl
# CLI fallback).  If neither can produce one, the whole module SKIPs cleanly.
# --------------------------------------------------------------------------
def _mint_cert():
    """Return (certfile, keyfile) temp paths for a self-signed localhost cert,
    or None if no minting path is available on this interpreter/host."""
    d = tempfile.mkdtemp(prefix="cov_ssl_edge_")
    cert = os.path.join(d, "cert.pem")
    key = os.path.join(d, "key.pem")
    # Path A: cryptography (same shape test_swarm_monkey.py uses).
    try:
        from datetime import datetime, timedelta, timezone
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        pk = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
        now = datetime.now(timezone.utc)
        crt = (x509.CertificateBuilder()
               .subject_name(name).issuer_name(name)
               .public_key(pk.public_key())
               .serial_number(x509.random_serial_number())
               .not_valid_before(now - timedelta(days=1))
               .not_valid_after(now + timedelta(days=1))
               .sign(pk, hashes.SHA256()))
        with open(cert, "wb") as f:
            f.write(crt.public_bytes(serialization.Encoding.PEM))
        with open(key, "wb") as f:
            f.write(pk.private_bytes(serialization.Encoding.PEM,
                                     serialization.PrivateFormat.TraditionalOpenSSL,
                                     serialization.NoEncryption()))
        return cert, key
    except Exception:
        pass
    # Path B: openssl CLI.
    try:
        subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
             "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=30)
        if os.path.exists(cert) and os.path.exists(key):
            return cert, key
    except Exception:
        pass
    return None


_CERT = _mint_cert()
if _CERT is not None:
    @atexit.register
    def _cleanup_cert():
        import shutil
        shutil.rmtree(os.path.dirname(_CERT[0]), ignore_errors=True)

pytestmark = pytest.mark.skipif(
    _CERT is None,
    reason="cannot mint a self-signed cert (no cryptography and no openssl CLI)")


# --------------------------------------------------------------------------
# Child driver: write `body` to a temp .py, run it patched under a fresh
# interpreter, return the RESULT dict the child printed (or fail loudly).
# --------------------------------------------------------------------------
def _run_child(body, label, timeout, guard):
    fd, path = tempfile.mkstemp(suffix="_ssl_edge.py")
    with os.fdopen(fd, "w") as f:
        f.write(body)
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = _SRC
    env["RL_SRC"] = _SRC
    env["RL_CERT"] = _CERT[0]
    env["RL_KEY"] = _CERT[1]
    env["RUNLOOM_GOROUTINE_PANIC"] = "silent"
    try:
        with hang_guard(guard, label):
            try:
                p = subprocess.run([sys.executable, path], capture_output=True,
                                   text=True, env=env, timeout=timeout)
            except subprocess.TimeoutExpired as e:
                # A child that never returns is exactly the WANT_READ/lost-wake
                # wedge these tests guard against: surface it as a failure, not a
                # silent hang (hang_guard would otherwise _exit the whole suite).
                pytest.fail("{0}: child HUNG > {1}s (cooperative wedge / "
                            "WANT_READ hang)\n{2}".format(
                                label, timeout,
                                (e.output or b"")[-2000:] if isinstance(e.output, bytes)
                                else (e.output or "")[-2000:]))
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    out = (p.stdout or "") + (p.stderr or "")
    # A negative return code == death by signal (SIGSEGV/SIGABRT) -> a crash, the
    # worst outcome; a clean Python error still exits >= 0.
    assert p.returncode is None or p.returncode >= 0, (
        "{0}: child died by signal {1} (crash, not a clean error)\n{2}".format(
            label, -p.returncode, out[-3000:]))
    line = None
    for ln in out.splitlines():
        if ln.startswith("RESULT "):
            line = ln[len("RESULT "):]
    assert line is not None, (
        "{0}: child produced no RESULT line (rc={1})\n{2}".format(
            label, p.returncode, out[-3000:]))
    return ast.literal_eval(line), out


# Common preamble every child shares: patch, import the (now cooperative) stdlib,
# build the two SSLContexts off the parent-minted cert.
_PREAMBLE = r'''
import os, sys
sys.path.insert(0, os.environ["RL_SRC"])
import runloom.monkey as monkey
monkey.patch()
import time
import runloom, runloom_c as rc, socket, ssl

CERT = os.environ["RL_CERT"]; KEY = os.environ["RL_KEY"]
out = {}

def _server_ctx():
    c = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    c.load_cert_chain(CERT, KEY)
    return c

def _client_ctx_insecure():
    c = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c
'''


# ==========================================================================
# (1) recv_into into a caller bytearray: nbytes=None and explicit-nbytes,
#     each parking on WANT_READ until data arrives.
# ==========================================================================
_SCRIPT_RECV_INTO = _PREAMBLE + r'''
# Two known payloads: 1024 bytes for the nbytes=None fill, 300 for explicit-nbytes.
MSG1 = bytes(range(256)) * 4
MSG2 = bytes((i * 37 + 11) & 0xFF for i in range(300))

def scenario():
    sp_a, sp_b = socket.socketpair()
    sctx = _server_ctx(); cctx = _client_ctx_insecure()

    def server():
        try:
            ss = sctx.wrap_socket(sp_a, server_side=True, do_handshake_on_connect=False)
            ss.do_handshake()
            # Sleep so the client's recv_into is already PARKED on WANT_READ before
            # any byte exists -> proves the cooperative park, not a lucky read.
            time.sleep(0.05); ss.sendall(MSG1)
            time.sleep(0.05); ss.sendall(MSG2)
            time.sleep(0.05)
        except Exception as e:
            out["server_err"] = type(e).__name__ + ":" + str(e)[:80]
        finally:
            try: sp_a.close()
            except OSError: pass

    def client():
        try:
            cs = cctx.wrap_socket(sp_b, server_hostname="localhost",
                                  do_handshake_on_connect=False)
            cs.do_handshake()
            # (a) nbytes=None: fill a caller bytearray exactly len(MSG1).
            buf1 = bytearray(len(MSG1))
            n1 = cs.recv_into(buf1)                 # parks until server sends MSG1
            out["n1_first"] = n1
            got = n1
            while got < len(MSG1):
                got += cs.recv_into(memoryview(buf1)[got:])
            out["got1"] = got
            out["buf1_ok"] = (bytes(buf1) == MSG1)
            # (b) explicit nbytes: a big buffer but read AT MOST 100 bytes.
            buf2 = bytearray(4096)
            n2 = cs.recv_into(buf2, 100)            # parks until server sends MSG2
            out["n2"] = n2
            out["n2_le_100"] = (0 < n2 <= 100)
            collected = bytes(buf2[:n2])
            while len(collected) < len(MSG2):
                b = bytearray(4096)
                k = cs.recv_into(b, len(MSG2) - len(collected))
                if k == 0:
                    break
                collected += bytes(b[:k])
            out["buf2_ok"] = (collected == MSG2)
            out["got2"] = len(collected)
        except Exception as e:
            out["client_err"] = type(e).__name__ + ":" + str(e)[:80]
        finally:
            try: sp_b.close()
            except OSError: pass

    rc.fiber(server); rc.fiber(client)

rc.fiber(scenario); rc.run()
print("RESULT", repr(out))
'''


def test_ssl_recv_into_parks_until_data_nbytes_and_explicit():
    res, out = _run_child(_SCRIPT_RECV_INTO, "ssl-recv_into", timeout=45, guard=60)
    assert "client_err" not in res, "client raised: {0}\n{1}".format(res, out)
    assert "server_err" not in res, "server raised: {0}\n{1}".format(res, out)
    # nbytes=None branch: parked, then returned the full known payload intact.
    assert res.get("n1_first", 0) > 0, res
    assert res.get("got1") == 1024, res
    assert res.get("buf1_ok") is True, res
    # explicit-nbytes branch: honoured the cap AND the bytes are correct.
    assert res.get("n2_le_100") is True, res
    assert res.get("got2") == 300, res
    assert res.get("buf2_ok") is True, res


# ==========================================================================
# (2) sendall of a multi-MB payload to a slow reader: partial-send / WANT_WRITE
#     park.  Every byte must arrive AND a sibling fiber must advance meanwhile.
# ==========================================================================
_SCRIPT_SENDALL = _PREAMBLE + r'''
N = 4 * 1024 * 1024
PAYLOAD = os.urandom(N)          # shared in-process by sender + reader
done = {"recv": False}
tick = {"n": 0}

def scenario():
    sp_a, sp_b = socket.socketpair()
    # Small buffers => the sender fills them fast => WANT_WRITE park fires often.
    for s in (sp_a, sp_b):
        try: s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        except OSError: pass
        try: s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        except OSError: pass
    sctx = _server_ctx(); cctx = _client_ctx_insecure()

    def sibling():
        # A pure cooperative ticker: it can ONLY advance if the sender yields the
        # hub while parked on WANT_WRITE (single-thread run, so no parallelism).
        while not done["recv"]:
            tick["n"] += 1
            time.sleep(0.001)

    def reader():                # deliberately slow -> keeps the pipe full
        try:
            ss = sctx.wrap_socket(sp_a, server_side=True, do_handshake_on_connect=False)
            ss.do_handshake()
            buf = bytearray()
            while len(buf) < N:
                chunk = ss.recv(16384)
                if not chunk:
                    break
                buf.extend(chunk)
                time.sleep(0.001)
            out["recv_len"] = len(buf)
            out["recv_ok"] = (bytes(buf) == PAYLOAD)
        except Exception as e:
            out["reader_err"] = type(e).__name__ + ":" + str(e)[:80]
        finally:
            done["recv"] = True
            try: sp_a.close()
            except OSError: pass

    def sender():
        try:
            cs = cctx.wrap_socket(sp_b, server_hostname="localhost",
                                  do_handshake_on_connect=False)
            cs.do_handshake()
            before = tick["n"]
            cs.sendall(PAYLOAD)              # partial-send / WANT_WRITE park loop
            out["sib_delta"] = tick["n"] - before
            out["sent"] = True
            while not done["recv"]:          # let the slow reader drain the tail
                time.sleep(0.002)
        except Exception as e:
            out["sender_err"] = type(e).__name__ + ":" + str(e)[:80]
        finally:
            try: sp_b.close()
            except OSError: pass

    rc.fiber(sibling); rc.fiber(reader); rc.fiber(sender)

rc.fiber(scenario); rc.run()
print("RESULT", repr(out))
'''


def test_ssl_sendall_multi_mb_want_write_park_with_sibling_overlap():
    res, out = _run_child(_SCRIPT_SENDALL, "ssl-sendall", timeout=90, guard=120)
    assert "sender_err" not in res, "sender raised: {0}\n{1}".format(res, out)
    assert "reader_err" not in res, "reader raised: {0}\n{1}".format(res, out)
    # Every byte of the 4 MB payload arrived, intact.
    assert res.get("sent") is True, res
    assert res.get("recv_len") == 4 * 1024 * 1024, res
    assert res.get("recv_ok") is True, res
    # The sender parked (WANT_WRITE) and a sibling fiber made progress meanwhile:
    # a hub stall would leave sib_delta == 0.
    assert res.get("sib_delta", 0) > 10, (
        "sibling did not advance during sendall (delta={0}) -> the multi-MB "
        "send monopolised the hub instead of parking cooperatively\n{1}".format(
            res.get("sib_delta"), out))


# ==========================================================================
# (3) wrap_socket CERT_REQUIRED vs a self-signed cert -> SSLCertVerificationError
#     PROMPTLY (out of the cooperative do_handshake), never a WANT_READ hang.
# ==========================================================================
_SCRIPT_CERT_REQUIRED = _PREAMBLE + r'''
def scenario():
    sp_a, sp_b = socket.socketpair()
    sctx = _server_ctx()
    # A verifying client with an EMPTY trust store: our self-signed cert can never
    # chain to a trusted root -> verification MUST fail.
    cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)   # defaults: verify + hostname
    cctx.check_hostname = True
    cctx.verify_mode = ssl.CERT_REQUIRED

    def server():
        try:
            ss = sctx.wrap_socket(sp_a, server_side=True, do_handshake_on_connect=False)
            ss.do_handshake()
            out["server_ok"] = True
        except Exception as e:
            out["server_err"] = type(e).__name__       # client-abort is expected
        finally:
            try: sp_a.close()
            except OSError: pass

    def client():
        try:
            # Connected client socket -> _patched_wrap_socket runs the cooperative
            # do_handshake, which is where OpenSSL performs verification.
            cs = cctx.wrap_socket(sp_b, server_hostname="localhost")
            out["client_ok"] = True                    # must NOT reach here
            try: cs.close()
            except OSError: pass
        except ssl.SSLCertVerificationError as e:
            out["client_err"] = "SSLCertVerificationError"
            out["verify_code"] = getattr(e, "verify_code", None)
            out["client_msg"] = str(e)[:100]
        except ssl.SSLError as e:
            out["client_err"] = "SSLError:" + type(e).__name__
            out["client_msg"] = str(e)[:100]
        except Exception as e:
            out["client_err"] = "OTHER:" + type(e).__name__
            out["client_msg"] = str(e)[:100]
        finally:
            try: sp_b.close()
            except OSError: pass

    rc.fiber(server); rc.fiber(client)

rc.fiber(scenario); rc.run()
print("RESULT", repr(out))
'''


def test_ssl_wrap_socket_cert_required_raises_verification_error_promptly():
    # A short timeout: the whole point is that verification FAILS FAST.  If it
    # hangs (WANT_READ loop), _run_child converts the child timeout into a test
    # failure rather than letting it wedge.
    res, out = _run_child(_SCRIPT_CERT_REQUIRED, "ssl-cert-required",
                          timeout=30, guard=45)
    assert res.get("client_ok") is not True, (
        "handshake unexpectedly SUCCEEDED against a self-signed cert with "
        "CERT_REQUIRED\n{0}".format(out))
    assert res.get("client_err") == "SSLCertVerificationError", (
        "expected SSLCertVerificationError, got {0!r} ({1})\n{2}".format(
            res.get("client_err"), res.get("client_msg"), out))
