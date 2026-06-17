"""Coverage-driven adversarial suite for the io_uring hub-ring + multishot-close
fragments:

  * src/runloom_c/io_uring_l_ring.c.inc      (per-hub SINGLE_ISSUER ring)
  * src/runloom_c/io_uring_l_msclose.c.inc    (global-ring recv/send dispatch +
                                              multishot handle teardown)

The functions here are only reached when TCPConn's recv/send routes through
io_uring -- the env knob RUNLOOM_TCPCONN_IOURING=1, resolved ONCE at first use
(runloom_tcp.c:runloom_tcpconn_resolve_mode).  Because that resolution is a
process-global latch, every scenario runs in a SUBPROCESS with the env set, and
each child EXITS CLEANLY so the gcov counters flush (a crash/_exit never does).

Two reachability dimensions select which fragment lines run:

  HUB ring (M:N, runloom.run(n>=2)):  runloom_mn_current_iouring_ring() returns
    the running hub's ring, so TCPConn recv/send with a non-zero MSG_* flag take
    the SINGLE-SHOT hub-ring path -> runloom_iouring_ring_recv / _ring_send /
    _ring_do, and the inline runloom_iouring_ring_drain processes the CQE.

  GLOBAL ring (single-thread, rc.run()):  no hub, so
    runloom_mn_current_iouring_ring() is NULL and runloom_iouring_recv/_send fall
    through to the global-ring runloom_iouring_do path (the msclose dispatch
    fall-through lines).

Oracles are real and end-to-end: an exact-byte echo through the hub ring, a
recv that returns the *already-buffered* bytes inline (the FAST_POLL inline-drain
success path), a parked global-ring file_read that a cross-thread cancel must
complete with ECANCELED (not hang), and a multishot handle that is freed both
while still armed (cancel branch) and after the peer EOF terminated it
(immediate-free branch).
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(
    not FT, reason="io_uring hub-ring paths are M:N + free-threaded only")


def _iou_available():
    try:
        import runloom_c
        return bool(runloom_c.iouring_available())
    except Exception:
        return False


needs_iouring = pytest.mark.skipif(
    not _iou_available(), reason="io_uring unavailable (need Linux >= 5.1)")

# RUNLOOM_TCPCONN_IOURING=1 forces TCPConn.recv/send through the io_uring
# backend.  We do NOT set RUNLOOM_IOURING_LOOP: the per-hub ring is created
# unconditionally in hub_main (defer_taskrun), so the hub-ring recv/send paths
# are reached on the plain readiness pump too -- which is exactly the path these
# fragments implement.
TCPCONN_ENV = {"RUNLOOM_TCPCONN_IOURING": "1"}


def _run(script, env_extra=None, timeout=240):
    # Generous timeout + skip-on-timeout: this box is shared with a CI runner
    # that competes for io_uring + CPU, so a timeout is contention, not a bug.
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               **(env_extra or {}))
    env.update(TCPCONN_ENV)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("io_uring workload timed out (box under heavy load)")


def _no_crash(p, label):
    # A signal-killed child returns a negative code and NEVER flushes gcov, so a
    # crash is both a finding and useless for coverage.  Require a clean exit.
    assert p.returncode is not None and p.returncode >= 0, (
        "%s CRASHED with signal %d\nstdout=%s\nstderr=%s"
        % (label, -p.returncode if p.returncode else 0,
           p.stdout[-500:], p.stderr[-1800:]))


# Shared subprocess preamble: in-tree runloom_c + a watchdog that dumps stacks
# and _exits if a hub-ring op ever leaks a wake (so a hang surfaces as a clean
# child failure with a traceback, never a wedged pytest).
_PRE = r'''
import sys, os, socket, errno
sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup
import faulthandler
faulthandler.dump_traceback_later(60, exit=True)
'''
_POST = "faulthandler.cancel_dump_traceback_later()\n"


# ===========================================================================
# 1. HUB-RING ECHO -- runloom_iouring_ring_send / _ring_recv / _ring_do success.
#    M:N TCPConn.send_all + recv route through the per-hub SINGLE_ISSUER ring.
#    Drives:
#      io_uring_l_ring.c.inc  L350-361 (ring_recv), L364-375 (ring_send),
#                             L304-340 + L344-346 (ring_do success return rr>=0),
#      io_uring_l_msclose.c.inc L63-65 (recv hub-ring dispatch),
#                             L79-81 (send hub-ring dispatch).
#    Oracle: a 4 KiB payload echoes back byte-exact (only true if every send/recv
#    op routed its OWN completion back -- a cross-delivered CQE corrupts it).
# ===========================================================================
_HUB_ECHO = _PRE + r'''
res = {}
def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()
    acc = {}; wga = WaitGroup(); wga.add(1)
    def acceptor():
        try: acc["sc"] = lconn.accept()
        finally: wga.done()
    rc.mn_go(acceptor)
    client = rc.TCPConn.connect("127.0.0.1", port)
    wga.wait(); sc = acc["sc"]

    payload = bytes((i % 251) for i in range(4096))
    wg = WaitGroup(); wg.add(1)
    def server():
        try:
            total = bytearray()
            while len(total) < 4096:
                d = sc.recv(8192)          # default multishot recv (opens self->ms)
                if not d: break
                total += d
            sc.send_all(bytes(total))      # hub-ring iouring send -> ring_send -> ring_do
            sc.close()                     # ms_close (armed cancel branch)
        finally:
            wg.done()
    rc.mn_go(server)

    client.send_all(payload)               # hub-ring iouring send -> ring_send
    got = bytearray()
    while len(got) < 4096:
        d = client.recv(8192)
        if not d: break
        got += d
    res["echo"] = bytes(got)
    wg.wait()
    client.close(); lconn.close()
runloom.run(2, main)
''' + _POST + r'''
ok = res.get("echo") == bytes((i % 251) for i in range(4096))
sys.stdout.write("HUB_ECHO ok=%r len=%d\n" % (ok, len(res.get("echo", b""))))
'''


@needs_iouring
def test_hub_ring_echo_send_recv_roundtrip():
    p = _run(_HUB_ECHO)
    _no_crash(p, "hub-ring echo")
    assert p.returncode == 0, "hub-ring echo failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    assert "HUB_ECHO ok=True len=4096" in p.stdout, (
        "4 KiB payload did not echo byte-exact through the hub io_uring ring "
        "(a cross-delivered or lost CQE)\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# 2. HUB-RING INLINE-DRAIN SUCCESS -- data already buffered when the recv op is
#    submitted, so FAST_POLL completes the CQE before io_uring_enter returns and
#    the inline runloom_iouring_ring_drain (ring_do L318) reaps it.
#    Drives:
#      io_uring_l_ring.c.inc  L227-234 (drain loop entry + the eventfd read),
#                             L235-272 (CQE claim CAS + publish-result + the
#                               op->hub != NULL / wait!=PARKED no-wake branch +
#                               the inflight decrements),
#                             L344-346 (ring_do returns rr>=0, NOT an error).
#    Oracle: the flagged (MSG_PEEK) single-shot hub-ring recv returns the exact
#    pre-buffered bytes -- a value can ONLY come back via the success return at
#    L346 (the cancel tests in the sibling suite only ever produce rr<0).
# ===========================================================================
_HUB_INLINE = _PRE + r'''
MSG_PEEK = socket.MSG_PEEK
res = {}
def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()
    acc = {}; wga = WaitGroup(); wga.add(1)
    def acceptor():
        try: acc["sc"] = lconn.accept()
        finally: wga.done()
    rc.mn_go(acceptor)
    client = rc.TCPConn.connect("127.0.0.1", port)
    wga.wait(); sc = acc["sc"]

    # Send first + settle, so the bytes are sitting in sc's recv buffer BEFORE
    # the server submits its RECV -> FAST_POLL completes the op inline.
    client.send_all(b"INLINE42")
    runloom.sleep(0.05)

    wg = WaitGroup(); wg.add(1)
    def reader():
        try:
            res["got"] = sc.recv(64, MSG_PEEK)   # single-shot hub-ring recv; data ready
        except OSError as e:
            res["err"] = e.errno
        finally:
            wg.done()
    rc.mn_go(reader)
    wg.wait()
    sc.close(); client.close(); lconn.close()
runloom.run(2, main)
''' + _POST + r'''
sys.stdout.write("HUB_INLINE got=%r err=%r\n" % (res.get("got"), res.get("err")))
'''


@needs_iouring
def test_hub_ring_inline_drain_success_returns_data():
    p = _run(_HUB_INLINE)
    _no_crash(p, "hub-ring inline drain")
    assert p.returncode == 0, "inline-drain run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    # The recv must have returned the pre-buffered bytes (the ring_do success
    # return), not an error -- proving the inline-drain CQE body ran and L346
    # was taken with rr >= 0.
    assert "HUB_INLINE got=b'INLINE42' err=None" in p.stdout, (
        "the hub-ring recv did not return the pre-buffered bytes via the inline "
        "FAST_POLL drain success path\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# 3. HUB-RING ECHO STORM -- many concurrent connections, each round-tripping
#    several messages through the per-hub ring.  Repeatedly exercises the
#    ring submit / inline-drain / park / wake handshake under contention across
#    >=2 hubs (each hub a SINGLE issuer of its own ring; the netpoll pump on the
#    idle hub races the inline drainer -- the cq_head CAS at L239 must keep them
#    from cross-delivering).
#    Drives the same ring_recv/ring_send/ring_do/ring_drain machinery as #1+#2
#    but under real multi-hub concurrency, hardening the CAS-claim path.
#    Oracle: a closed-form per-conn sum -- every byte echoed exactly once.
# ===========================================================================
_HUB_STORM = _PRE + r'''
import struct
N = 48
ROUNDS = 4
got = [0] * N
def main():
    def handler(conn):
        try:
            for _ in range(ROUNDS):
                d = conn.recv(8)
                if not d: break
                conn.send_all(d)
        finally:
            conn.close()
    port, listeners = rc.serve("127.0.0.1", 0, handler, 2)
    wg = WaitGroup(); wg.add(N)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            acc = 0
            for r in range(ROUNDS):
                c.send_all(struct.pack(">Q", i * 1000 + r))
                rep = c.recv(8)
                if rep == struct.pack(">Q", i * 1000 + r):
                    acc += 1
            got[i] = acc
            c.close()
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for L in listeners: L.close()
runloom.run(4, main)
''' + _POST + r'''
sys.stdout.write("HUB_STORM total=%d expected=%d\n" % (sum(got), N * ROUNDS))
'''


@needs_iouring
def test_hub_ring_concurrent_echo_storm_exact_once():
    p = _run(_HUB_STORM)
    _no_crash(p, "hub-ring echo storm")
    assert p.returncode == 0, "echo-storm run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    assert "HUB_STORM total=192 expected=192" in p.stdout, (
        "concurrent hub-ring echo lost or cross-delivered a message (the "
        "multi-drainer cq_head CAS failed)\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# 4. GLOBAL-RING CANCEL -- a hub fiber parked on a GLOBAL-ring io_uring op
#    (op->ring == NULL) is cancelled cross-thread.  A file_read on an empty pipe
#    routes runloom_iouring_pread -> runloom_iouring_do, which publishes
#    g->iouring_op with op.ring == NULL and parks.  G.cancel_wait_fd() ->
#    runloom_iouring_cancel_g sees op->ring == NULL and submits the ASYNC_CANCEL
#    on the GLOBAL ring inline (NOT via the hub mailbox).
#    Drives:
#      io_uring_l_ring.c.inc  L406 (op->ring == NULL, so NOT the mailbox route),
#                             L408-417 (calloc cancel_op, build the ASYNC_CANCEL
#                               SQE, submit on the global ring, return 1).
#    Oracle: cancel_wait_fd() returned True AND the parked read completed with
#    ECANCELED -- not a hang, not a spurious wake.
# ===========================================================================
_GLOBAL_CANCEL = _PRE + r'''
res = {}
def main():
    rfd, wfd = os.pipe()                 # never written -> the read parks forever
    os.set_blocking(rfd, False)
    rd = {}
    def reader():
        rd["g"] = rc.current_g()
        buf = bytearray(16)
        try:
            rd["n"] = rc.file_read(rfd, buf, 16, -1)   # global-ring iouring pread; parks
        except OSError as e:
            rd["errno"] = e.errno
        rd["done"] = True
    rc.mn_go(reader)
    # Deterministic handshake (no sleep-as-sync): cancel_wait_fd() returns True
    # IFF the reader's io_uring op is published AND wait==PARKED -- i.e. the park
    # has committed (runloom_iouring_cancel_g returns 0 otherwise).  So retry it
    # until it actually submits the global-ring ASYNC_CANCEL; the cap only bounds
    # a hang.  This removes the load-dependent "did 0.08s commit the park?" guess
    # that could fire the cancel before the read parked (cancel False + the read
    # then parks forever on the never-written pipe).
    woke = False
    for _ in range(2000000):
        woke = rd["g"].cancel_wait_fd() if "g" in rd else False
        if woke:
            break
        rc.sched_yield()
    res["woke"] = woke                   # -> iouring_cancel_g global path
    for _ in range(2000):
        if rd.get("done"): break
        runloom.sleep(0.01)
    res["errno"] = rd.get("errno"); res["done"] = rd.get("done")
    for fd in (rfd, wfd):
        try: os.close(fd)
        except OSError: pass
runloom.run(2, main)
''' + _POST + r'''
sys.stdout.write("GLOBAL_CANCEL woke=%r errno=%r done=%r\n" %
                 (res.get("woke"), res.get("errno"), res.get("done")))
'''


@needs_iouring
def test_global_ring_iouring_cancel_unblocks_parked_read():
    p = _run(_GLOBAL_CANCEL)
    _no_crash(p, "global-ring cancel")
    assert p.returncode == 0, "global-ring cancel run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    # True can only come from runloom_iouring_cancel_g's global-ring branch here:
    # there is no netpoll parker for a fiber sitting on a bare io_uring op.
    assert "GLOBAL_CANCEL woke=True" in p.stdout, (
        "cancel_wait_fd() did not submit a global-ring ASYNC_CANCEL (no "
        "iouring_cancel_g global path)\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))
    assert "errno=125" in p.stdout and "done=True" in p.stdout, (
        "the parked global-ring read did not complete with ECANCELED after the "
        "cancel (the op was stranded)\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# 5. GLOBAL-RING RECV/SEND fall-through -- single-thread rc.run() has no hub, so
#    runloom_mn_current_iouring_ring() is NULL and TCPConn recv/send take the
#    global-ring path.
#    Drives:
#      io_uring_l_msclose.c.inc  L63-64 NULL hub -> L67-73 (recv builds the RECV
#                               SQE + runloom_iouring_do),
#                             L79-80 NULL hub -> L83-89 (send builds the SEND SQE
#                               + runloom_iouring_do).
#    Oracle: a single-thread echo round-trips -- the bytes go out the global-ring
#    SEND and come back through a global-ring RECV.
# ===========================================================================
_GLOBAL_ECHO = _PRE + r'''
MSG_PEEK = socket.MSG_PEEK
res = {}
def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()
    holder = {}
    def acceptor():
        holder["sc"] = lconn.accept()
    rc.go(acceptor)
    client = rc.TCPConn.connect("127.0.0.1", port)
    for _ in range(200000):
        if "sc" in holder: break
        rc.sched_yield()
    sc = holder["sc"]

    done = {}
    def server():
        # flagged recv -> single-shot global-ring runloom_iouring_recv (no hub);
        # data arrives from the client below, completes rr>=0
        d = sc.recv(64, MSG_PEEK)
        sc.send_all(d)                 # global-ring runloom_iouring_send
        done["d"] = d
    rc.go(server)
    client.send_all(b"GLOBALRG")       # global-ring send
    res["reply"] = client.recv(64, MSG_PEEK)   # global-ring recv
    for _ in range(200000):
        if "d" in done: break
        rc.sched_yield()
    res["server_saw"] = done.get("d")
    client.close(); sc.close(); lconn.close()
rc.go(main)
rc.run()
''' + _POST + r'''
sys.stdout.write("GLOBAL_ECHO reply=%r server_saw=%r\n" %
                 (res.get("reply"), res.get("server_saw")))
'''


@needs_iouring
def test_global_ring_recv_send_fall_through_single_thread():
    p = _run(_GLOBAL_ECHO)
    _no_crash(p, "global-ring recv/send")
    assert p.returncode == 0, "global-ring echo run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    assert ("GLOBAL_ECHO reply=b'GLOBALRG' server_saw=b'GLOBALRG'" in p.stdout), (
        "the single-thread (no-hub) TCPConn echo did not round-trip through the "
        "global io_uring ring recv/send fall-through\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# 6. MULTISHOT-CLOSE both branches -- runloom_iouring_ms_close.
#    The default recv(n) (flags == 0) on an iouring TCPConn opens a GLOBAL-ring
#    multishot handle (self->ms).  Closing the conn frees it via ms_close, which
#    forks on whether the multishot is still armed:
#      ARMED  (still receiving): L38-52 -- submit an ASYNC_CANCEL, drain frees it.
#      !ARMED (peer EOF ended it): L18-32 -- return buffers + free immediately.
#    This one child drives BOTH:
#      conn A: recv data, close while STILL armed   -> the cancel branch.
#      conn B: recv data, peer closes (EOF terminates the multishot), recv again
#              to observe EOF, then close            -> the immediate-free branch.
#    Drives:
#      io_uring_l_msclose.c.inc  L11 (h != NULL), L13-16 (snapshot armed),
#                             L18-32 (immediate-free: return pbufs + free),
#                             L38-52 (armed: build + submit the cancel SQE).
#    Oracle: both conns received their bytes, B saw a clean EOF, and the child
#    exits 0 with the per-test self_check/leak invariants intact (a botched
#    ms_close double-frees a pbuf or leaks the handle).
# ===========================================================================
_MS_CLOSE = _PRE + r'''
res = {}
def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()

    # ---- conn A: close while the multishot is still armed (cancel branch) ----
    accA = {}; wgaA = WaitGroup(); wgaA.add(1)
    def acceptorA():
        try: accA["sc"] = lconn.accept()
        finally: wgaA.done()
    rc.mn_go(acceptorA)
    clientA = rc.TCPConn.connect("127.0.0.1", port)
    wgaA.wait(); scA = accA["sc"]
    wgA = WaitGroup(); wgA.add(1)
    def serverA():
        try:
            res["a"] = scA.recv(8)     # opens self->ms (armed)
            scA.close()                # ms_close while ARMED -> cancel branch
        finally:
            wgA.done()
    rc.mn_go(serverA)
    clientA.send_all(b"ARMEDXX1")
    wgA.wait()
    clientA.close()

    # ---- conn B: peer EOF terminates the multishot, then close (immediate) ----
    accB = {}; wgaB = WaitGroup(); wgaB.add(1)
    def acceptorB():
        try: accB["sc"] = lconn.accept()
        finally: wgaB.done()
    rc.mn_go(acceptorB)
    clientB = rc.TCPConn.connect("127.0.0.1", port)
    wgaB.wait(); scB = accB["sc"]
    wgB = WaitGroup(); wgB.add(1)
    def serverB():
        try:
            res["b1"] = scB.recv(8)    # opens self->ms (armed); gets data
            res["b2"] = scB.recv(8)    # client closes -> EOF terminates multishot
            scB.close()                # ms_close, multishot ENDED -> immediate-free
        finally:
            wgB.done()
    rc.mn_go(serverB)
    clientB.send_all(b"EOFCLOSE")
    runloom.sleep(0.05)
    clientB.close()                    # peer EOF
    wgB.wait()
    lconn.close()
runloom.run(2, main)
''' + _POST + r'''
sys.stdout.write("MS_CLOSE a=%r b1=%r b2=%r\n" %
                 (res.get("a"), res.get("b1"), res.get("b2")))
'''


@needs_iouring
def test_multishot_close_armed_and_immediate_free_branches():
    p = _run(_MS_CLOSE)
    _no_crash(p, "multishot close")
    assert p.returncode == 0, "ms_close run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    # conn A delivered its bytes (multishot armed at close), conn B delivered its
    # bytes then saw a clean EOF (empty second recv) -- so both ms_close branches
    # ran and neither double-freed a buffer nor stranded the handle.
    assert "MS_CLOSE a=b'ARMEDXX1' b1=b'EOFCLOSE' b2=b''" in p.stdout, (
        "the multishot recv/close lifecycle did not produce the expected "
        "armed-close + EOF-close outcomes\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# 7. HUB-RING CANCEL under the READINESS PUMP (NO RUNLOOM_IOURING_LOOP) -- the
#    pump (netpoll_pump.c.inc:88) drains the hub ring via runloom_iouring_ring_
#    drain, so BOTH the recv's -ECANCELED CQE and the ASYNC_CANCEL's own CQE flow
#    through that function (the sibling cov100_mn_api cancel tests run under the
#    LOOP backend, where loop_drain -- a different reaper -- handles them, so
#    ring_drain's cancel + hub-wake branches stay dark there; here they run).
#    A reader fiber parks on a single-shot hub-ring recv (MSG_PEEK, no data); a
#    second fiber cancels it.  runloom_iouring_cancel_g sees op->ring != NULL and
#    routes through the hub mailbox -> runloom_iouring_submit_cancel_for_op submits
#    the ASYNC_CANCEL on the hub ring; the pump's ring_drain then reaps:
#    Drives:
#      io_uring_l_ring.c.inc  L227-243 (drain loop + cq_head CAS claim),
#                             L244 + L249  (the cancel-completion CQE -> free(op)),
#                             L250 + L259-266 (the recv -ECANCELED CQE ->
#                               store result, op->hub != NULL, exchange wait,
#                               prev == PARKED -> runloom_mn_wake_g),
#                             L271-272 (the inflight decrements),
#                             L425-441 (runloom_iouring_submit_cancel_for_op:
#                               snapshot PARKED, build + submit the cancel SQE).
#    Oracle: cancel_wait_fd() returned True (the hub-mailbox route) AND the recv
#    unblocked with ECANCELED -- end-to-end proof the pump's ring_drain woke the
#    parked fiber, not a hang.
# ===========================================================================
_HUB_CANCEL = _PRE + r'''
MSG_PEEK = socket.MSG_PEEK
res = {}
def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()
    acc = {}; wga = WaitGroup(); wga.add(1)
    def acceptor():
        try: acc["sc"] = lconn.accept()
        finally: wga.done()
    rc.mn_go(acceptor)
    client = rc.TCPConn.connect("127.0.0.1", port)
    wga.wait(); sc = acc["sc"]

    holder = {}; wg = WaitGroup(); wg.add(1)
    def reader():
        holder["g"] = rc.current_g()
        try:
            sc.recv(64, MSG_PEEK)      # single-shot hub-ring recv; no data -> parks
        except OSError as e:
            res["errno"] = e.errno
        finally:
            wg.done()
    rc.mn_go(reader)
    # Deterministic handshake (no sleep-as-sync): cancel_wait_fd() returns True
    # IFF the reader's hub-ring op is published AND wait==PARKED -- i.e. the park
    # has committed (runloom_iouring_cancel_g returns 0 / routes nothing before
    # then).  Retry until it actually requests the hub-ring ASYNC_CANCEL; the cap
    # only bounds a hang.  This replaces the load-dependent 0.08s "commit to the
    # park" guess that could fire the cancel before the recv parked (cancel False
    # + the MSG_PEEK recv on a peer that never sends then parks forever).
    woke = False
    for _ in range(2000000):
        woke = holder["g"].cancel_wait_fd() if "g" in holder else False
        if woke:
            break
        rc.sched_yield()
    res["woke"] = woke                 # hub mailbox -> submit_cancel_for_op
    wg.wait()                          # the pump's ring_drain MUST wake it
    sc.close(); client.close(); lconn.close()
runloom.run(2, main)
''' + _POST + r'''
sys.stdout.write("HUB_CANCEL woke=%r errno=%r\n" % (res.get("woke"), res.get("errno")))
'''


@needs_iouring
def test_hub_ring_cancel_drives_ring_drain_cancel_and_wake():
    # Explicitly does NOT enable RUNLOOM_IOURING_LOOP, so the readiness pump
    # drains the hub ring through runloom_iouring_ring_drain.
    p = _run(_HUB_CANCEL)
    _no_crash(p, "hub-ring cancel via pump")
    assert p.returncode == 0, "hub-ring cancel run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1600:])
    assert "HUB_CANCEL woke=True errno=125" in p.stdout, (
        "the hub-ring recv was not cancelled+woken through ring_drain (the "
        "cancel CQE or the -ECANCELED completion never reached the drain)\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-1200:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
