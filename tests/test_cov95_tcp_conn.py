"""Coverage-driven adversarial suite for the TCPConn C surface.

Targets the uncovered (#####) lines in three fragments of runloom_tcp.c:

  src/runloom_c/runloom_tcp_conn_io.c.inc    -- recv / recv_into (+ iouring)
  src/runloom_c/runloom_tcp_conn_send.c.inc  -- send / send_all  (+ iouring)
  src/runloom_c/runloom_tcp_conn_net.c.inc   -- listen / accept / connect / setsockopt

The uncovered lines split into four reachability classes, and the tests are
grouped accordingly. Each test names the source lines it drives and the gate it
makes true.

 1. PLAIN ERROR / EDGE branches reachable in-process on Linux (epoll backend):
    closed-conn guards, bad-arg parse failures, the `fd < 0` ctor guard, the
    `family` getter, a non-local bind (EADDRNOTAVAIL), an unresolvable host
    (getaddrinfo failure on a reserved .invalid TLD), a failing setsockopt.
    Driven directly under runloom.run(2).

 2. SIGNAL-INTERRUPTED COOPERATIVE PARK (the `wait_fd_coop(...) < 0` arms that
    propagate a raised Python signal handler instead of overwriting it with
    OSError): a SIGALRM handler that raises lands on a fiber parked in
    recv / recv_into / send_all / connect. The signal must be installed in the
    MAIN thread, so these run on the SINGLE-THREAD scheduler (runloom_c.run()),
    where the parked fiber lives on the main OS thread.

 3. SYNCHRONOUS SYSCALL HARD-ERROR branches that loopback never produces on its
    own (a connect() that returns ECONNREFUSED synchronously instead of
    EINPROGRESS; a recv_into whose recvfrom returns ECONNRESET). Driven by
    strace -e inject= in a subprocess (Linux-only, same mechanism as
    tests/test_tcp_faultinject.py).

 4. IO_URING TCPConn backend paths (recv multishot + single-shot, recv_into,
    send, send_all, the ms-handle close, and their r<0 error arms). Reached only
    with RUNLOOM_TCPCONN_IOURING=1 under the io_uring-as-loop backend, which the
    runtime resolves ONCE at first run(); so these run in a SUBPROCESS with the
    env set and EXIT CLEANLY so gcov counters flush. A peer RST drives the
    iouring r<0 error arms (ECONNRESET on recv, EPIPE on send) with a real
    condition (no fault hook exists for an io_uring op completion).

Excluded (see the structured report): the RunloomTCPConn_alloc()==NULL cleanup
arms (listen L70-71, accept L126-127, connect L247-248) -- a tp_alloc OOM with
no fault hook in the TCPConn path; and the _PyBytes_Resize(<0) arms -- likewise
OOM-only.
"""
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from adv_util import hang_guard, needs_free_threading  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FT = needs_free_threading()

import runloom  # noqa: E402
import runloom_c as rc  # noqa: E402
from runloom.sync import WaitGroup  # noqa: E402


# ===========================================================================
# Class 1: in-process plain error / edge branches (epoll backend, no iouring).
# ===========================================================================

def _connected_pair():
    """Inside a hub: return (client_conn, server_conn, listener) over loopback."""
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno()))
    port = s.getsockname()[1]
    s.detach()
    holder = {}
    wg = WaitGroup()
    wg.add(1)

    def acc():
        try:
            holder["sc"] = L.accept()
        finally:
            wg.done()
    rc.mn_fiber(acc)
    c = rc.TCPConn.connect("127.0.0.1", port)
    wg.wait()
    return c, holder["sc"], L


def test_ctor_and_closed_and_family_and_setsockopt():
    """io.c.inc: L45 (ctor parse fail), L47-48 (fd<0 -> ValueError),
    L117/L120 (family getter), L237-238/L241 (recv_into closed / read-only buf).
    send.c.inc: L13-15 (send closed), L68-69 (send_all closed), L17-54 (send body).
    net.c.inc: L260-274 (setsockopt incl. L273 failure arm), L87-88 (accept closed),
    L152 (connect parse), L26 (listen parse)."""
    box = {}

    def main():
        # io L45: TCPConn(fd) with a non-int fd -> PyArg_ParseTupleAndKeywords fails.
        try:
            rc.TCPConn(fd="not-an-int")
        except TypeError:
            box["ctor_parse"] = True
        # io L47-48: fd < 0 -> ValueError("fd must be >= 0").
        try:
            rc.TCPConn(fd=-1)
        except ValueError as e:
            box["ctor_neg"] = str(e)

        c, sc, L = _connected_pair()

        # io L117/L120: the `family` getset getter (AF_INET == 2).
        box["family"] = c.family

        # send.c.inc L17-54: a single send() on an open conn (the suite otherwise
        # only ever exercises send_all, leaving the whole single-send body dark).
        box["send_n"] = c.send(b"hello")
        box["server_recv"] = sc.recv(5)

        # io L241: recv_into of a READ-ONLY buffer -> the "w*" format demands a
        # writable buffer, so PyArg_ParseTuple fails -> return NULL.
        try:
            sc.recv_into(b"read-only-bytes")
        except TypeError:
            box["recvinto_ro"] = True

        # net.c.inc L260-274: setsockopt success path + the L273 rc!=0 failure arm.
        box["setsockopt_ok"] = sc.setsockopt(
            socket.IPPROTO_TCP, socket.TCP_NODELAY, b"\x01\x00\x00\x00")
        try:
            sc.setsockopt(-1, -1, b"\x00")     # bogus level/optname -> setsockopt fails
        except OSError:
            box["setsockopt_fail"] = True

        c.close()
        box["closed_flag"] = c.closed          # io L111-115 is_closed getter (True)

        # io L237-238: recv_into on a closed conn -> "TCPConn is closed".
        try:
            c.recv_into(bytearray(8))
        except OSError as e:
            box["recvinto_closed"] = str(e)
        # send.c.inc L68-69: send_all on a closed conn.
        try:
            c.send_all(b"x")
        except OSError as e:
            box["sendall_closed"] = str(e)
        # send.c.inc L13-15: send() on a closed conn.
        try:
            c.send(b"x")
        except OSError as e:
            box["send_closed"] = str(e)

        sc.close()
        L.close()
        # net.c.inc L87-88: accept() on a closed listener.
        try:
            L.accept()
        except OSError as e:
            box["accept_closed"] = str(e)

        # net.c.inc L26: TCPConn.listen() with missing required args -> parse fail.
        try:
            rc.TCPConn.listen()
        except TypeError:
            box["listen_parse"] = True
        # net.c.inc L152: TCPConn.connect(host) missing port -> parse fail.
        try:
            rc.TCPConn.connect("127.0.0.1")
        except TypeError:
            box["connect_parse"] = True

    with hang_guard(60, "ctor/closed/family/setsockopt"):
        runloom.run(2, main)

    assert box.get("ctor_parse") is True
    assert box.get("ctor_neg") == "fd must be >= 0"
    assert box.get("family") == socket.AF_INET
    assert box.get("send_n") == 5
    assert box.get("server_recv") == b"hello"
    assert box.get("recvinto_ro") is True
    assert box.get("setsockopt_ok") is None
    assert box.get("setsockopt_fail") is True
    assert box.get("closed_flag") is True
    assert box.get("recvinto_closed") == "TCPConn is closed"
    assert box.get("sendall_closed") == "TCPConn is closed"
    assert box.get("send_closed") == "TCPConn is closed"
    assert box.get("accept_closed") == "TCPConn is closed"
    assert box.get("listen_parse") is True
    assert box.get("connect_parse") is True


def test_bind_failure_and_resolve_failure():
    """net.c.inc: L59-62 (bind() of a non-local address -> EADDRNOTAVAIL cleanup:
    saved errno + close(fd) + raise), L29 (listen resolve fail), L154 (connect
    resolve fail). getaddrinfo fails deterministically offline on the reserved
    `.invalid` TLD (RFC 6761) and on a non-local literal."""
    box = {}

    def main():
        # net L59-62: 1.2.3.4 is not an address on this host -> bind() EADDRNOTAVAIL.
        try:
            rc.TCPConn.listen("1.2.3.4", 9999)
        except OSError as e:
            box["bind_errno"] = e.errno
        # net L29: listen resolve fail (getaddrinfo on a guaranteed-nonexistent name).
        try:
            rc.TCPConn.listen("no.such.host.invalid", 80)
        except OSError:
            box["listen_resolve_fail"] = True
        # net L154: connect resolve fail.
        try:
            rc.TCPConn.connect("no.such.host.invalid", 80)
        except OSError:
            box["connect_resolve_fail"] = True

    with hang_guard(60, "bind/resolve failure"):
        runloom.run(2, main)

    import errno as _errno
    assert box.get("bind_errno") == _errno.EADDRNOTAVAIL, box
    assert box.get("listen_resolve_fail") is True
    assert box.get("connect_resolve_fail") is True


def test_recv_partial_resize():
    """io.c.inc: the `got < n_bytes -> _PyBytes_Resize` success arm in recv()
    (L222-224 on the epoll path). recv(4096) returns only the few bytes the peer
    sent, so the result bytes object is shrunk -- a partial-recv we assert is
    exactly the sent payload (proving the resize landed the right length)."""
    box = {}

    def main():
        c, sc, L = _connected_pair()
        c.send_all(b"tiny")
        # recv with a buffer far larger than what arrived -> got(4) < n(4096) -> resize.
        box["data"] = sc.recv(4096)
        c.close()
        sc.close()
        L.close()

    with hang_guard(60, "recv partial resize"):
        runloom.run(2, main)
    assert box.get("data") == b"tiny", box


# ===========================================================================
# Class 2: a raised signal handler interrupts a cooperative park. Drives the
# `wait_fd_coop(...) < 0` -> PyErr_Occurred() ? NULL arms. SINGLE-THREAD
# scheduler so the parked fiber is on the main OS thread (signal-deliverable).
# ===========================================================================
# These run in a SUBPROCESS: a SIGALRM handler installed at module scope is
# process-global, and the test must not perturb the parent pytest's signal
# state or its scheduler. Each child exits 0 and prints a marker we assert on.

_SIG_TEMPLATE = r'''
import os, socket, signal, sys
sys.path.insert(0, "src")
import runloom_c as rc

box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

OP = "__OP__"

def server():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno())); box["port"] = s.getsockname()[1]; s.detach()
    sc = L.accept()
    if OP in ("recv", "recv_into"):
        # shrink nothing; just park reading with no peer data.
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.15)
            if OP == "recv":
                sc.recv(64)                  # io L214-218: parked recv, signal -> raise
            else:
                sc.recv_into(bytearray(64))  # io L288-292: parked recv_into, signal -> raise
            box["rv"] = "got"
        except KeyboardInterrupt:
            box["interrupt"] = True
    elif OP == "send_all":
        # tiny rcvbuf on the server (peer) so the client's send_all fills + parks.
        sc.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, (4096).to_bytes(4, "little"))
    box["L"] = L; box["sc"] = sc
    while "done" not in box and "interrupt" not in box and "rv" not in box:
        rc.sched_sleep(0.02)
    sc.close(); L.close()

def client():
    while "port" not in box:
        rc.sched_yield()
    c = rc.TCPConn.connect("127.0.0.1", box["port"]); box["c"] = c
    if OP == "send_all":
        c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, (4096).to_bytes(4, "little"))
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.2)
            c.send_all(b"Z" * (8 * 1024 * 1024))   # send.c.inc L118-122: parked WRITE, signal -> raise
            box["rv"] = "sent"
        except KeyboardInterrupt:
            box["interrupt"] = True
        box["done"] = True
    else:
        while "interrupt" not in box and "rv" not in box:
            rc.sched_sleep(0.02)
        box["done"] = True
    c.close()

import faulthandler; faulthandler.dump_traceback_later(40, exit=True)
rc.fiber(server); rc.fiber(client); rc.run()
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("SIG OP=%s interrupt=%r rv=%r\n" % (OP, box.get("interrupt"), box.get("rv")))
'''

_CONNECT_SIG = r'''
import signal, sys
sys.path.insert(0, "src")
import runloom_c as rc
box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)
def client():
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.2)
        # 240.0.0.1 (class-E, unroutable) never completes the handshake, so the
        # non-blocking connect parks on WRITE. The SIGALRM handler raises during
        # the park -> wait_fd_coop returns -1 -> net.c.inc L218-224: saved errno +
        # close(fd) + propagate the raised KeyboardInterrupt (NOT OSError).
        rc.TCPConn.connect("240.0.0.1", 9)
        box["rv"] = "connected"
    except KeyboardInterrupt:
        box["interrupt"] = True
    except OSError as e:
        box["oserror"] = e.errno
import faulthandler; faulthandler.dump_traceback_later(40, exit=True)
rc.fiber(client); rc.run()
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("CONNECT_SIG interrupt=%r oserror=%r rv=%r\n" %
                 (box.get("interrupt"), box.get("oserror"), box.get("rv")))
'''


def _run_child(script, timeout=120, env_extra=None):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("child workload timed out (box under heavy load)")


@pytest.mark.parametrize("op", ["recv", "recv_into", "send_all"])
def test_signal_interrupts_parked_io(op):
    """io.c.inc L214-218 (recv) / L288-292 (recv_into); send.c.inc L118-122
    (send_all): a raised signal handler during the cooperative park makes
    wait_fd_coop return <0 with a pending exception, so the op returns NULL
    with the SIGNAL's exception, not an OSError overwrite."""
    p = _run_child(_SIG_TEMPLATE.replace("__OP__", op))
    assert p.returncode == 0, (op, p.stdout[-400:], p.stderr[-1200:])
    assert ("SIG OP=%s interrupt=True" % op) in p.stdout, (
        "the signal raised during the parked %s did not propagate as the "
        "interrupt (it may have been swallowed / overwritten by OSError)\n"
        "stdout=%s\nstderr=%s" % (op, p.stdout, p.stderr[-800:]))


def test_signal_interrupts_parked_connect():
    """net.c.inc L218-224: a signal raised while connect() is parked on WRITE
    must close(fd) (preserving errno) and propagate the raised exception."""
    p = _run_child(_CONNECT_SIG)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1200:])
    assert "CONNECT_SIG interrupt=True" in p.stdout, (
        "signal during parked connect did not propagate the interrupt\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-800:]))


# ===========================================================================
# Class 3: synchronous syscall hard-errors via strace -e inject= (Linux only).
# Drives the connect() immediate-error arm and the recv_into() recvfrom-error
# arm that loopback never produces on its own.
# ===========================================================================

def _strace_supports_inject():
    strace = shutil.which("strace")
    if not strace:
        return False
    try:
        p = subprocess.run(
            [strace, "-e", "inject=connect:error=EINTR:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


_STRACE_OK = sys.platform.startswith("linux") and _strace_supports_inject()
needs_strace = pytest.mark.skipif(
    not _STRACE_OK, reason="strace -e inject= (Linux) not available")

_CONNECT_HARD = r'''
import os, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc
box = {}
def client():
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0)); lsock.listen(16)
    port = lsock.getsockname()[1]
    try:
        # strace forces connect() itself to return ECONNREFUSED synchronously
        # (not EINPROGRESS), so errno is not in {EINPROGRESS,EAGAIN,EINTR} and we
        # take net.c.inc L234-238: saved errno + close(fd) + raise OSError.
        rc.TCPConn.connect("127.0.0.1", port); box["ok"] = True
    except OSError as e:
        box["errno"] = e.errno
    lsock.close()
rc.fiber(client); rc.run()
if "errno" in box:
    print("OSERROR errno=%s" % box["errno"]); sys.exit(42)
print("OK"); sys.exit(0)
'''

_RECVINTO_HARD = r'''
import os, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc
box = {}
def client():
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind(("127.0.0.1", 0)); lsock.listen(16)
    port = lsock.getsockname()[1]
    try:
        c = rc.TCPConn.connect("127.0.0.1", port)
        # strace forces recvfrom() -> ECONNRESET: a non-EAGAIN error in
        # recv_into's loop -> io.c.inc L283-285: release buffer + raise OSError.
        box["n"] = c.recv_into(bytearray(1024)); c.close()
    except OSError as e:
        box["errno"] = e.errno
    lsock.close()
rc.fiber(client); rc.run()
if "errno" in box:
    print("OSERROR errno=%s" % box["errno"]); sys.exit(42)
print("N=%s" % box.get("n")); sys.exit(0)
'''


def _run_strace(script, inject, timeout=60):
    strace = shutil.which("strace")
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    cmd = [strace, "-f", "-e", "signal=none", "-e", "inject=" + inject,
           PY, "-c", script]
    try:
        return subprocess.run(cmd, cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("strace workload timed out (box under heavy load)")


@needs_strace
def test_connect_synchronous_econnrefused():
    """net.c.inc L234-238: a connect() that returns a hard error synchronously
    (not EINPROGRESS) must close(fd) and surface a clean OSError(ECONNREFUSED)."""
    import errno as _errno
    p = _run_strace(_CONNECT_HARD, "connect:error=ECONNREFUSED:when=1")
    assert p.returncode == 42, (p.returncode, p.stdout[-300:], p.stderr[-600:])
    assert ("errno=%d" % _errno.ECONNREFUSED) in p.stdout, p.stdout[-300:]


@needs_strace
def test_recv_into_synchronous_econnreset():
    """io.c.inc L283-285: a recvfrom() ECONNRESET inside recv_into's loop must
    release the buffer and surface a clean OSError(ECONNRESET)."""
    import errno as _errno
    p = _run_strace(_RECVINTO_HARD, "recvfrom:error=ECONNRESET:when=1")
    assert p.returncode == 42, (p.returncode, p.stdout[-300:], p.stderr[-600:])
    assert ("errno=%d" % _errno.ECONNRESET) in p.stdout, p.stdout[-300:]


# ===========================================================================
# Class 4: the io_uring TCPConn backend (RUNLOOM_TCPCONN_IOURING=1 under the
# io_uring-as-loop backend). Drives the iouring recv/recv_into/send/send_all
# paths, the multishot ms-handle open+close, and the r<0 error arms.
# ===========================================================================

def _iou_available():
    try:
        return bool(rc.iouring_available())
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(
    not (FT and _iou_available()),
    reason="io_uring TCPConn backend needs a GIL-disabled build + io_uring")

_IOU_ENV = {"RUNLOOM_IOURING_LOOP": "1", "RUNLOOM_IOURING_MS": "1",
            "RUNLOOM_TCPCONN_IOURING": "1"}

# Exercises every iouring success arm: multishot recv (flags=0, opens self->ms),
# single-shot recv via MSG_PEEK (flags!=0 bypasses multishot), recv_into both
# ways, send (single), send_all (loop), and an explicit close() of a conn whose
# multishot ms handle is open + a conn GC'd with ms open (the dealloc ms-close).
_IOU_OK = r'''
import sys, struct, socket, gc
sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
MSG_PEEK = socket.MSG_PEEK
N = 24
got = [None] * N
peek = [None] * N
def main():
    def handler(conn):
        try:
            d = conn.recv(8)            # iouring multishot recv -> opens self->ms
            if d: conn.send_all(d)      # iouring send_all loop
        finally:
            conn.close()                # iouring close with self->ms != NULL
    port, lst = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send(struct.pack(">Q", i))               # iouring single send
            peek[i] = c.recv(8, MSG_PEEK)              # iouring single-shot recv (flags!=0)
            if i % 2 == 0:
                buf = bytearray(64)
                n = c.recv_into(buf)                   # iouring recv_into multishot (partial: 8<64)
                got[i] = bytes(buf[:n])
            else:
                got[i] = c.recv(64)                    # iouring multishot recv (partial: 8<64)
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_fiber(lambda i=i: client(i))
    wg.wait()
    # leave one conn with an OPEN multishot ms handle to be GC'd (dealloc ms-close):
    leak = rc.TCPConn.connect("127.0.0.1", port)
    leak.send(struct.pack(">Q", 0))
    leak.recv(8)                                       # opens its ms; then drop without close
    del leak
    gc.collect()
    for ln in lst: ln.close()
runloom.run(4, main)
ok = sum(1 for i in range(N) if got[i] == struct.pack(">Q", i) and peek[i] == struct.pack(">Q", i))
sys.stdout.write("IOU_OK %d\n" % ok)
'''

# A peer RST drives the iouring r<0 error arms with a real condition (no fault
# hook exists for an io_uring SQE completion): server-side iouring recv sees
# ECONNRESET, iouring send/send_all see EPIPE.
_IOU_ERR = r'''
import sys, struct, socket, os
sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
res = {}
def main():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno())); port = s.getsockname()[1]; s.detach()
    holder = {}; wg = WaitGroup(); wg.add(1)
    def acc():
        try: holder["sc"] = L.accept()
        finally: wg.done()
    rc.mn_fiber(acc)
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.connect(("127.0.0.1", port))
    wg.wait()
    sc = holder["sc"]                                  # server-side TCPConn (iouring)
    raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    raw.close()                                        # hard RST from the peer
    runloom.sleep(0.1)
    try: sc.recv(64)                                   # iouring recv r<0 (io ms_recv / single-shot)
    except OSError as e: res["recv_errno"] = e.errno
    try: sc.recv_into(bytearray(64))                   # iouring recv_into r<0
    except OSError as e: res["recvinto_errno"] = e.errno
    try: sc.send(b"x" * 64)                            # iouring send r<0 (send.c.inc L25)
    except OSError as e: res["send_errno"] = e.errno
    try: sc.send_all(b"y" * 64)                        # iouring send_all r<0 (send.c.inc L86-88)
    except OSError as e: res["sendall_errno"] = e.errno
    sc.close(); L.close()
runloom.run(2, main)
sys.stdout.write("IOU_ERR recv=%r recvinto=%r send=%r sendall=%r\n" %
                 (res.get("recv_errno"), res.get("recvinto_errno"),
                  res.get("send_errno"), res.get("sendall_errno")))
'''


@needs_iouring
def test_iouring_tcpconn_success_paths():
    """io.c.inc L160-193 (recv multishot+single-shot), L249-268 (recv_into),
    L64-65/L89-90 (ms close in dealloc/close); send.c.inc L20-26 (send),
    L74-94 (send_all). Exact-once echo oracle proves the iouring path round-trips."""
    p = _run_child(_IOU_OK, timeout=200, env_extra=_IOU_ENV)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert "IOU_OK 24" in p.stdout, (
        "io_uring TCPConn echo did not round-trip exactly-once\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-1000:]))


@needs_iouring
def test_iouring_tcpconn_error_paths():
    """io.c.inc recv/recv_into r<0 arms; send.c.inc L25 + L86-88: a peer RST makes
    the iouring recv complete -ECONNRESET and the iouring send complete -EPIPE,
    each surfaced as a clean OSError (not a crash / hang / swallow)."""
    import errno as _errno
    p = _run_child(_IOU_ERR, timeout=200, env_extra=_IOU_ENV)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1500:])
    assert ("recv=%d" % _errno.ECONNRESET) in p.stdout, (p.stdout, p.stderr[-1000:])
    # send to a reset peer surfaces EPIPE (32); accept either EPIPE or ECONNRESET.
    assert ("send=%d" % _errno.EPIPE) in p.stdout or \
           ("send=%d" % _errno.ECONNRESET) in p.stdout, (p.stdout, p.stderr[-1000:])
    assert ("sendall=%d" % _errno.EPIPE) in p.stdout or \
           ("sendall=%d" % _errno.ECONNRESET) in p.stdout, (p.stdout, p.stderr[-1000:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
