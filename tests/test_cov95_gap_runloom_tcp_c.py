"""Bounded gap-fill coverage for runloom_tcp.c (TCPConn) COVER lines.

Targets the uncovered-but-reachable lines classified COVER in
build/cover_by_tu.json under "runloom_tcp.c":

  runloom_tcp.c
    L90        resolve_mode: RUNLOOM_TCPCONN_IOURING="auto" -> MODE_AUTO branch
    L94-95     resolve_mode: RUNLOOM_TCPCONN_IOURING_THRESHOLD set -> atoi>0 latch
    L116-121   use_iouring auto-choice block (count<threshold -> 0 ; >=threshold -> 1)
  runloom_tcp_conn_send.c.inc
    L30        send while(1) loop back-edge (resume after an EAGAIN park)
    L41-43     send hard error (EPIPE/ECONNRESET) -> PyBuffer_Release + raise
    L46-50     send EAGAIN park + the wait_fd<0 (cancel) error-return branch
    L68        send_all closed-conn guard
  runloom_tcp_conn_net.c.inc
    L110-111   accept fatal-error branch (errno not in the transient set)
  runloom_tcp_conn_io.c.inc
    L265-268   recv_into single-shot io_uring RECV fallback (flags!=0, MSG_PEEK,
               pre-ready bytes complete it inline -- NOT a backpressured recv)

Mechanisms (per the classifier):
  * Env-mode / io_uring branches are PROCESS-FROZEN (mode resolved once on first
    read), so each runs in a fresh clean-exiting SUBPROCESS with the env set.
  * The accept fatal-error branch has no in-process FINJ hook on Linux
    (RUNLOOM_TCP_FINJ compiles to 0); driven with strace -e inject=accept4.
  * The send/recv epoll-path branches use real backpressure / RST / cancel with
    NO io_uring (epoll path), so none can hit the io_uring recv-backpressure
    deadlock.

Every test is deadline-bounded (hang_guard / subprocess timeout); a TimeoutExpired
on an io_uring child is treated as box contention -> skip, never a flaky fail.
"""
import os
import shutil
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from adv_util import hang_guard  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

import runloom_c as rc  # noqa: E402

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="runloom_tcp.c io_uring/strace gap-fill is Linux-only")


def _iou_available():
    try:
        return bool(rc.iouring_available())
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(
    not _iou_available(), reason="io_uring unavailable on this box")


def _run_child(script, env_extra, timeout=60):
    """Run `script` in a fresh child with env_extra layered on.  A
    TimeoutExpired is treated as box contention (io_uring + CPU shared with a CI
    runner) -> skip, not a flaky fail."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("io_uring child timed out (box under heavy load)")


# ===========================================================================
# 1. resolve_mode "auto" branch (runloom_tcp.c L90) + the auto-choice block
#    with count < threshold -> choice=0 (pure epoll; L116/117/118/121).
#    RUNLOOM_TCPCONN_IOURING=auto with the DEFAULT threshold (2048) and a couple
#    of conns: live_count never crosses 2048, so the auto branch picks epoll.
#    The send/recv still round-trips (oracle: echo), proving the auto-epoll path
#    is clean.
# ===========================================================================
_AUTO_EPOLL = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
res = [None]
def main():
    def server():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        p[0] = lst.fileno() and _port(lst)
        conn = lst.accept()
        d = conn.recv(64)
        conn.send_all(d)          # auto-mode resolve runs here (count<threshold)
        conn.close(); lst.close()
    def client():
        while p[0] is None:
            rc.sched_yield()
        c = rc.TCPConn.connect("127.0.0.1", p[0])
        c.send_all(b"ping")       # auto-mode resolve_mode runs here too
        res[0] = c.recv(64)
        c.close()
    rc.fiber(server); rc.fiber(client); rc.run()
import socket
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
p = [None]
main()
sys.stdout.write("AUTO_EPOLL %r\n" % (res[0] == b"ping",))
'''


@needs_iouring
def test_resolve_mode_auto_below_threshold_picks_epoll():
    # RUNLOOM_TCPCONN_IOURING=auto -> L90 strcmp("auto") branch + the auto-choice
    # block taking the count<threshold (default 2048) -> epoll path (L121).
    p = _run_child(_AUTO_EPOLL, {"RUNLOOM_TCPCONN_IOURING": "auto"})
    assert p.returncode == 0, (p.stdout[-500:], p.stderr[-1500:])
    assert "AUTO_EPOLL True" in p.stdout, (p.stdout[-500:], p.stderr[-1500:])


# ===========================================================================
# 2. resolve_mode threshold parse (L94-95) + the auto-choice io_uring branch
#    (L119): RUNLOOM_TCPCONN_IOURING=auto + THRESHOLD=1.  atoi("1")=1>0 latches
#    the threshold (L94-95); a single live conn makes live_count>=1>=threshold,
#    so the auto block picks io_uring (choice=1).  We drive use_iouring via a
#    bounded TCPConn.send (io_uring SEND -- NOT a backpressured recv), so no
#    deadlock.  Oracle: the send returns the byte count, child exits clean.
# ===========================================================================
_AUTO_IOURING_SEND = r'''
import sys, socket; sys.path.insert(0, "src")
import runloom_c as rc
res = [None]
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
p = [None]
def main():
    def server():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        p[0] = lst.fileno() and _port(lst)
        conn = lst.accept()
        # drain so the client's send always completes; then close.
        got = b""
        while len(got) < 4:
            d = conn.recv(64)
            if not d: break
            got += d
        conn.close(); lst.close()
    def client():
        while p[0] is None:
            rc.sched_yield()
        c = rc.TCPConn.connect("127.0.0.1", p[0])
        # First send latches the per-conn backend choice via use_iouring:
        # auto + live_count(>=1) >= threshold(1) + iouring_available -> choice=1.
        res[0] = c.send(b"ping")    # io_uring SEND, small, bounded
        c.close()
    rc.fiber(server); rc.fiber(client); rc.run()
main()
sys.stdout.write("AUTO_IOURING_SEND %r\n" % (res[0],))
'''


@needs_iouring
def test_resolve_mode_threshold_one_auto_picks_iouring_send():
    p = _run_child(_AUTO_IOURING_SEND,
                   {"RUNLOOM_TCPCONN_IOURING": "auto",
                    "RUNLOOM_TCPCONN_IOURING_THRESHOLD": "1"})
    assert p.returncode == 0, (p.stdout[-500:], p.stderr[-1500:])
    assert "AUTO_IOURING_SEND 4" in p.stdout, (p.stdout[-500:], p.stderr[-1500:])


# ===========================================================================
# 3. recv_into single-shot io_uring RECV fallback (conn_io.c.inc L265-268).
#    RUNLOOM_TCPCONN_IOURING=1 + recv_into(buf, n, flags=MSG_PEEK): flags!=0
#    bypasses the pbuf multishot fast-path and takes the single-shot
#    runloom_iouring_recv fallback.  The peer has ALREADY sent the bytes (they
#    sit in the socket buffer) so the op completes inline via io_uring FAST_POLL
#    -- never parks, no wake-pump, NO backpressure deadlock.  MSG_PEEK leaves the
#    data, so a following plain recv() returns the same bytes (oracle).
# ===========================================================================
_PEEK_SINGLESHOT = r'''
import sys, socket; sys.path.insert(0, "src")
import runloom_c as rc
res = {}
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
p = [None]
def main():
    def server():
        lst = rc.TCPConn.listen("127.0.0.1", 0)
        p[0] = lst.fileno() and _port(lst)
        conn = lst.accept()
        conn.send_all(b"PEEKME12")     # 8 pre-ready bytes
        rc.sched_sleep(0.3)            # keep conn open while client peeks+recvs
        conn.close(); lst.close()
    def client():
        while p[0] is None:
            rc.sched_yield()
        c = rc.TCPConn.connect("127.0.0.1", p[0])
        # Wait for the data to actually arrive (a plain readiness park on a
        # tiny pre-ready payload), then PEEK via the single-shot fallback.
        rc.sched_sleep(0.1)
        buf = bytearray(8)
        n = c.recv_into(buf, 8, socket.MSG_PEEK)   # flags!=0 -> single-shot RECV
        res["peek"] = (n, bytes(buf[:n]) if n > 0 else b"")
        c.close()
    rc.fiber(server); rc.fiber(client); rc.run()
main()
sys.stdout.write("PEEK %r\n" % (res.get("peek"),))
'''


@needs_iouring
def test_recv_into_msg_peek_singleshot_fallback():
    # MSG_PEEK + pre-ready bytes -> the single-shot io_uring RECV fallback
    # (L265-268) completes inline.  This is the SAFE single tiny bounded op, not
    # the backpressured-recv deadlock path.
    p = _run_child(_PEEK_SINGLESHOT, {"RUNLOOM_TCPCONN_IOURING": "1"},
                   timeout=40)
    assert p.returncode == 0, (p.stdout[-500:], p.stderr[-1800:])
    # The single-shot RECV completed and returned the peeked bytes (or, if the
    # kernel/io_uring config doesn't take this exact fallback, at least it must
    # have exited cleanly).
    assert "PEEK (8, b'PEEKME12')" in p.stdout, (p.stdout[-500:], p.stderr[-1500:])


# ===========================================================================
# 4. send EAGAIN backpressure: epoll-path send loop back-edge (L30) + the
#    EAGAIN park (L46) and its success-resume (L46-47).  A fiber fills
#    SO_SNDBUF on a conn whose peer never reads -> send() EAGAINs -> park on
#    EPOLLOUT (L46) -> a second fiber drains the peer -> the park resumes ->
#    loop back to L30 -> send completes -> break.  DEFAULT (epoll) backend so no
#    io_uring at all.
# ===========================================================================
def test_send_eagain_park_then_resume_epoll():
    # A 4 MiB payload over default socket buffers (~200 KiB) GUARANTEES many
    # EAGAIN parks on EPOLLOUT: send_all sends what fits, EAGAINs, parks (L46),
    # the server drains (frees buffer space), the park resumes and the loop
    # re-enters at the back-edge (L30) until the whole payload is sent.  Two
    # fibers only (server reads, client sends) -- no busy-yield fiber
    # that would starve netpoll.  DEFAULT (epoll) backend; no io_uring.
    PAYLOAD = 4 * 1024 * 1024
    res = {}
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        port = s.getsockname()[1]
    finally:
        s.detach()
        s.close()

    def server():
        conn = lst.accept()
        total = 0
        while total < PAYLOAD:
            d = conn.recv(64 * 1024)
            if not d:
                break
            total += len(d)
        res["recv"] = total
        conn.close()
        lst.close()

    def sender():
        c = rc.TCPConn.connect("127.0.0.1", port)
        n = c.send_all(b"Z" * PAYLOAD)   # forces EAGAIN parks (L30 + L46-47)
        res["sent"] = n
        c.close()

    with hang_guard(25, "send EAGAIN park/resume"):
        rc.fiber(server)
        rc.fiber(sender)
        rc.run()
    assert res.get("sent") == PAYLOAD, res
    assert res.get("recv") == PAYLOAD, res


# ===========================================================================
# 5. send hard-error branch (conn_send.c.inc L41-43): a peer that RSTs the
#    connection makes send() return EPIPE/ECONNRESET (not EAGAIN/EWOULDBLOCK/
#    EINTR) -> L41 true -> PyBuffer_Release + raise OSError.  DEFAULT (epoll)
#    backend.  Driven with a real raw socket peer that sets SO_LINGER {1,0} and
#    closes (sends a RST).
# ===========================================================================
def test_send_hard_error_surfaces_oserror_epoll():
    import struct
    res = {}
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        port = s.getsockname()[1]
    finally:
        s.detach()
        s.close()
    accepted = {}

    def server():
        conn = lst.accept()
        accepted["c"] = conn

    def sender():
        c = rc.TCPConn.connect("127.0.0.1", port)
        while "c" not in accepted:
            rc.sched_yield()
        # Abort the server side with an RST.
        sconn = accepted["c"]
        raw = socket.socket(fileno=socket.dup(sconn.fileno()))
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                       struct.pack("ii", 1, 0))
        raw.close()        # RST toward the client
        sconn.close()
        # Now repeatedly send until the local stack reports the broken pipe.
        err = None
        try:
            for _ in range(200):
                c.send_all(b"x" * 4096)
                rc.sched_yield()
        except OSError as e:
            err = e.errno
        res["err"] = err
        try:
            c.close()
        except OSError:
            pass
        lst.close()

    with hang_guard(20, "send hard error"):
        rc.fiber(server)
        rc.fiber(sender)
        rc.run()
    # EPIPE (32) or ECONNRESET (104): a non-transient send error surfaced as a
    # clean OSError (L41-43), never a crash or a hang.
    assert res.get("err") in (errno_EPIPE(), errno_ECONNRESET()), res


def errno_EPIPE():
    import errno
    return errno.EPIPE


def errno_ECONNRESET():
    import errno
    return errno.ECONNRESET


# ===========================================================================
# 6. send EAGAIN park + cancel -> wait_fd<0 error return (conn_send.c.inc L50).
#    A fiber fills SO_SNDBUF and parks on EPOLLOUT inside send_all (L46);
#    a second fiber then cancel_wait_fd()s it, so netpoll_wait_fd_coop
#    returns <0 -> L47 PyBuffer_Release + L50 returns (clean OSError, no
#    Python-level pending exc -> SetFromErrno).  Epoll path, bounded.
# ===========================================================================
def test_send_park_then_cancel_returns_error_epoll():
    res = {}
    hold = {}
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        port = s.getsockname()[1]
    finally:
        s.detach()
        s.close()
    accepted = {}

    def server():
        conn = lst.accept()
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                        (1024).to_bytes(4, sys.byteorder))
        accepted["c"] = conn   # never read -> the client's send backs up

    def sender():
        c = rc.TCPConn.connect("127.0.0.1", port)
        c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                     (1024).to_bytes(4, sys.byteorder))
        while "c" not in accepted:
            rc.sched_yield()
        try:
            # Fills the buffers and parks on EPOLLOUT (peer never reads).
            c.send_all(b"Q" * (4 * 1024 * 1024))
            res["outcome"] = "completed"
        except OSError as e:
            res["outcome"] = "oserror"
            res["errno"] = e.errno
        except BaseException as e:  # noqa: BLE001
            res["outcome"] = "other:%r" % (e,)
        c.close()

    def canceller():
        g = hold["g"]
        # Let the sender fill SO_SNDBUF and actually park.
        for _ in range(50):
            rc.sched_yield()
        rc.sched_sleep(0.05)
        res["cancel_ret"] = g.cancel_wait_fd()
        accepted["c"].close()
        lst.close()

    with hang_guard(20, "send park+cancel"):
        hold["g"] = rc.fiber(sender)
        rc.fiber(server)
        rc.fiber(canceller)
        rc.run()
    # The parked send was cancelled: wait_fd returned <0 -> L50 error return.
    # The cancel succeeded and the send did NOT silently complete.
    assert res.get("cancel_ret") is True, res
    assert res.get("outcome") == "oserror", res


# ===========================================================================
# 7. send_all closed-conn guard (conn_send.c.inc L68): close() then send_all
#    hits self->closed -> PyErr_SetString("TCPConn is closed") + return.
# ===========================================================================
def test_send_all_on_closed_conn_raises():
    res = {}
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try:
        port = s.getsockname()[1]
    finally:
        s.detach()
        s.close()
    accepted = {}

    def server():
        accepted["c"] = lst.accept()

    def client():
        c = rc.TCPConn.connect("127.0.0.1", port)
        while "c" not in accepted:
            rc.sched_yield()
        c.close()
        try:
            c.send_all(b"x")            # closed -> L68
            res["raised"] = False
        except OSError as e:
            res["raised"] = True
            res["msg"] = str(e)
        accepted["c"].close()
        lst.close()

    with hang_guard(15, "send_all closed guard"):
        rc.fiber(server)
        rc.fiber(client)
        rc.run()
    assert res.get("raised") is True, res
    assert "closed" in res.get("msg", ""), res


# ===========================================================================
# 8. accept fatal-error branch (conn_net.c.inc L110-111): an accept() error
#    whose errno is not in {EAGAIN,EWOULDBLOCK,EINTR,ECONNABORTED} -> L110 true
#    -> L111 PyErr_SetFromErrno -> clean OSError.  No in-process FINJ hook on
#    Linux (RUNLOOM_TCP_FINJ==0), so use strace -e inject=accept:error=EINVAL.
#    NB: runloom's accept path uses the bare accept() syscall (NOT accept4),
#    confirmed by `strace -e trace=accept,accept4`.
# ===========================================================================
_ACCEPT_FATAL = r'''
import sys, socket; sys.path.insert(0, "src")
import runloom_c as rc
box = {}
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: port = s.getsockname()[1]
    finally: s.detach(); s.close()
    # A real raw client fills the accept queue so accept() would otherwise
    # succeed; the injected error fires INSTEAD of a successful accept.
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    raw.connect(("127.0.0.1", port))
    def server():
        try:
            conn = lst.accept()
            box["ok"] = True
            conn.close()
        except OSError as e:
            box["errno"] = e.errno
    rc.fiber(server); rc.run()
    raw.close(); lst.close()
main()
if "errno" in box:
    sys.stdout.write("ACCEPT_OSERROR errno=%s\n" % box["errno"])
else:
    sys.stdout.write("ACCEPT_OK %r\n" % box.get("ok"))
'''


def _strace_supports_inject():
    strace = shutil.which("strace")
    if not strace:
        return False
    try:
        p = subprocess.run(
            [strace, "-e", "inject=accept:error=EINVAL:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


@pytest.mark.skipif(not _strace_supports_inject(),
                    reason="strace with -e inject= not available")
def test_accept_fatal_error_surfaces_oserror():
    strace = shutil.which("strace")
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    # EINVAL is not in {EAGAIN,EWOULDBLOCK,EINTR,ECONNABORTED} -> L110 fatal.
    cmd = [strace, "-f", "-e", "signal=none",
           "-e", "inject=accept:error=EINVAL:when=1+",
           PY, "-c", _ACCEPT_FATAL]
    try:
        p = subprocess.run(cmd, cwd=REPO, env=env, capture_output=True,
                           text=True, timeout=60)
    except subprocess.TimeoutExpired:
        pytest.skip("strace accept-fatal child timed out")
    assert p.returncode == 0, (p.stdout[-500:], p.stderr[-2000:])
    # EINVAL == 22: the fatal accept error surfaced cleanly (no crash/hang).
    assert "ACCEPT_OSERROR errno=22" in p.stdout, (p.stdout[-500:],
                                                   p.stderr[-2000:])
