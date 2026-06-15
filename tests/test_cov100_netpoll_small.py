"""Adversarial coverage suite for the small netpoll fragments.

Targets the never-executed lines of:
  - netpoll_init.c.inc        (L74 lock-init spin, L184 malformed-fault else,
                               L394/L413 reset_after_fork memsets)
  - netpoll_register.c.inc     (L122-124 armed_set ENOMEM register failure)
  - netpoll_pump.c.inc         (L83-89 per-hub io_uring ring eventfd match)
  - netpoll_pump_helpers.c.inc (L22 pump_claim CAS load, L108 drain_expired CAS load)
  - netpoll_parker_link.c.inc  (L31-38 ghost self-reference detach -- best-effort)

Each test names the uncovered region it drives and HOW.  Env-gated / first-touch
paths (small RUNLOOM_NETPOLL_MAXFD, malformed RUNLOOM_FAULT_FD_READ, the
first-ever default-pool park) are reached in SUBPROCESSES that exit cleanly with a
stdout marker, because the parent pytest process imports runloom_c once and those
values are read+cached at first use.  In-process tests cover the
pump-claim/drain/io_uring paths that need no special env.

NB: register_at_fork wires runloom_netpoll_reset_after_fork as an
after-in-child handler, so os.fork() (after prior park activity) drives the
reset_after_fork memsets in the child.
"""
import errno
import os
import socket
import subprocess
import sys
import textwrap
import time

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


def _run_py(src, env_extra=None, timeout=60):
    """Run a python snippet in a clean subprocess with the runloom env set.

    Returns the CompletedProcess.  The snippet must print its own success
    marker and exit 0 -- a crash/_exit does NOT flush gcov, so we always
    assert returncode==0 + the marker at the call site.
    """
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", textwrap.dedent(src)],
                          cwd=REPO, env=env, capture_output=True, text=True,
                          timeout=timeout)


# ---------------------------------------------------------------------------
# netpoll_register.c.inc L121-124: runloom_fd_armed_set() returns -1 -> the
# register ENOMEM branch (RUNLOOM_RUNLOCK; errno=ENOMEM; return -1).
#
# armed_set fails iff fd >= runloom_fd_pending_wake_cap.  That cap is sized once
# (runloom_fd_arrays_init) from RUNLOOM_NETPOLL_MAXFD (clamped to >=1024).  In a
# subprocess we pin the cap to 1024, then wait_fd() on a real fd whose NUMBER is
# >= 1024 (dup2 to 2000, still < the rlimit hard ceiling so wait_fd's own
# max-fd guard passes).  register's armed_get returns 0 (out of range) so cur==0,
# then armed_set(fd,target) returns -1 -> L122-124 -> wait_fd returns -1 ->
# OSError(ENOMEM).  We assert exactly that errno, proving the branch ran.
# ---------------------------------------------------------------------------
def test_register_armed_set_enomem_high_fd():
    p = _run_py(r"""
        import os, errno, socket, sys
        import runloom_c as rc
        s = socket.socketpair()[0]
        HI = 2000                       # > MAXFD(1024), < rlimit hard
        os.dup2(s.fileno(), HI)
        try:
            rc.wait_fd(HI, 1, 0)        # register armed_set fails BEFORE any park
            sys.stdout.write("NO_ERROR\n")
        except OSError as e:
            sys.stdout.write("ENOMEM\n" if e.errno == errno.ENOMEM
                             else "OTHER:%d\n" % e.errno)
        finally:
            os.close(HI)
    """, env_extra={"RUNLOOM_NETPOLL_MAXFD": "1024"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "ENOMEM" in p.stdout, (p.stdout, p.stderr[-800:])
    # And it must NOT have silently swallowed the error.
    assert "NO_ERROR" not in p.stdout and "OTHER" not in p.stdout, p.stdout


# ---------------------------------------------------------------------------
# netpoll_init.c.inc L184: runloom_fault_inject's `else return 0` -- a fault
# spec that is neither "once:<n>" nor "always:<n>".  Armed (env present, so the
# fdio fault gate is true) but MALFORMED, so fault_inject returns 0 == no
# injection.  We then read real data from a pipe via rc.fd_read and assert it
# comes back intact: a spurious injection would have raised OSError instead.
# (The cached fdio-fault-armed flag and the malformed spec are both first-touch,
# hence the subprocess.)
# ---------------------------------------------------------------------------
def test_fault_inject_malformed_spec_is_noop():
    p = _run_py(r"""
        import os, sys
        import runloom_c as rc
        r, w = os.pipe()
        os.write(w, b"realbytes")        # ready immediately, no park needed
        buf = bytearray(9)
        n = rc.fd_read(r, buf, 9)        # hits FD_READ fault site -> malformed -> L184 return 0
        os.close(r); os.close(w)
        sys.stdout.write("READ:%d:%s\n" % (n, bytes(buf[:n]).decode()))
        # prove the fault site was actually consulted but injected nothing.
        sys.stdout.write("FIRED:%d\n" % rc._fault_count("FD_READ"))
    """, env_extra={"RUNLOOM_FAULT_FD_READ": "garbage"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "READ:9:realbytes" in p.stdout, (p.stdout, p.stderr[-800:])
    # Malformed spec -> never counted as fired.
    assert "FIRED:0" in p.stdout, p.stdout


def test_fault_inject_wellformed_spec_does_inject_contrast():
    # Contrast probe: a WELL-FORMED once:<errno> spec DOES inject at the same
    # FD_READ site (taking the once-CAS branch, not L184) -- proving the malformed
    # test above isolates L184 specifically and not a dead site.  EAGAIN(11) is
    # injected once: fd_read treats it as "park", and with data already present
    # the post-park read still returns the bytes, so the call still succeeds but
    # the fault counter increments.
    p = _run_py(r"""
        import os, sys
        import runloom, runloom_c as rc
        def main():
            r, w = os.pipe()
            os.write(w, b"abcd")
            buf = bytearray(4)
            n = rc.fd_read(r, buf, 4)     # once:11 -> EAGAIN -> park -> wake -> read
            os.close(r); os.close(w)
            sys.stdout.write("READ:%d:%s\n" % (n, bytes(buf[:n]).decode()))
            sys.stdout.write("FIRED:%d\n" % rc._fault_count("FD_READ"))
        def driver(): rc.mn_go(main)
        runloom.run(2, driver)
    """, env_extra={"RUNLOOM_FAULT_FD_READ": "once:11"})
    assert p.returncode == 0, p.stderr[-1500:]
    assert "READ:4:abcd" in p.stdout, (p.stdout, p.stderr[-800:])
    assert "FIRED:1" in p.stdout, p.stdout   # the well-formed once: path DID fire


# ---------------------------------------------------------------------------
# netpoll_init.c.inc L74-75: the lock-init loser-spin
#     while (__atomic_load_n(&pool->lock_inited) != 2) { /* spin */ }
# Reached when two+ threads call runloom_parker_pool_lock_ensure_inited for the
# SAME pool concurrently and a thread loses the 0->1 CAS: it spins until the
# winner finishes runloom_mutex_init + runloom_pool_backend_create and stores 2.
# Per-hub pools each init in isolation, so we target the DEFAULT pool (index 64),
# which every NON-hub thread shares: many raw OS threads all doing their FIRST
# wait_fd simultaneously behind a barrier race that one lazy init.  In a fresh
# subprocess this is the first-ever default-pool park (in-process the lock is
# long since inited).  Backend_create issues epoll syscalls, widening the loser
# window.  Asserts every racer's short-timeout park completed cleanly.
# ---------------------------------------------------------------------------
def test_lock_init_loser_spin_race():
    p = _run_py(r"""
        import os, socket, sys, threading
        sys.path.insert(0, "src")
        import runloom_c as rc
        N = 96
        ready = threading.Barrier(N)
        errs = []
        def worker():
            a, b = socket.socketpair()
            a.setblocking(False)
            try:
                ready.wait()
                rc.wait_fd(a.fileno(), 1, 40)   # times out; fd never readable
            except Exception as e:
                errs.append(repr(e))
            finally:
                try: rc.netpoll_unregister(a.fileno())
                except Exception: pass
                a.close(); b.close()
        ts = [threading.Thread(target=worker) for _ in range(N)]
        for t in ts: t.start()
        for t in ts: t.join()
        assert not errs, errs
        # structural integrity after the concurrent first-init.
        assert rc._self_check(0) == 0
        sys.stdout.write("SPIN_OK\n")
    """, timeout=45)
    assert p.returncode == 0, p.stderr[-1500:]
    assert "SPIN_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# ---------------------------------------------------------------------------
# netpoll_init.c.inc L394 + L413: runloom_netpoll_reset_after_fork's two memsets
#   L394  memset(p->by_fd, 0, p->by_fd_cap * sizeof p->by_fd[0])   (per pool, if by_fd!=NULL)
#   L413  memset(runloom_fd_registered_bm, 0, runloom_fd_registered_cap_bytes)  (if bm!=NULL)
# Both guards are non-NULL only AFTER real park activity: a parker linked into a
# pool allocates that pool's by_fd[]; an epoll-registered fd sets the registered
# bitmap.  We do a genuine fd park in the PARENT (allocating both), then os.fork().
# register_at_fork(after_in_child) runs reset_after_fork in the child, taking
# both memsets.  The child then re-parks to prove the reset left a working
# runtime (a botched reset would hang or lose the wake).  Child exit 0 == both.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_reset_after_fork_memsets():
    p = _run_py(r"""
        import os, socket, sys
        import runloom, runloom_c as rc

        def park_once(tag):
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            def w():
                rc.sched_yield()
                rc.tcp_send(b.fileno(), tag)
            rc.mn_go(w)
            r = rc.wait_fd(a.fileno(), 1, 5000)   # real epoll park+wake: alloc by_fd + reg bitmap
            assert r == 1, (tag, r)
            buf = bytearray(1); rc.tcp_recv(a.fileno(), buf, 1)
            rc.netpoll_unregister(a.fileno()); a.close()
            rc.netpoll_unregister(b.fileno()); b.close()

        def driver(): rc.mn_go(lambda: park_once(b"P"))
        runloom.run(2, driver)            # parent: by_fd[] + registered_bm now non-NULL

        pid = os.fork()
        if pid == 0:
            try:
                def cd(): rc.mn_go(lambda: park_once(b"C"))   # reset ran at fork; re-park
                runloom.run(2, cd)
                os._exit(0)
            except BaseException as e:
                sys.stderr.write("child: %r\n" % e); os._exit(7)
        else:
            _, st = os.waitpid(pid, 0)
            code = os.waitstatus_to_exitcode(st)
            sys.stdout.write("CHILD:%d\n" % code)
    """, timeout=45)
    assert p.returncode == 0, p.stderr[-1500:]
    assert "CHILD:0" in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# netpoll_pump_helpers.c.inc L22: the CAS load inside runloom_pump_claim, reached
# via runloom_pump_dispatch_event when the SHARED EPOLL PUMP wakes a fd-parker
# on a data event (not io_uring, not timeout).  A raw wait_fd(READ) parks; a
# sibling fiber writes; epoll fires; the pump dispatches -> pump_claim CASes the
# parker PARKED->WOKEN.  wait_fd returns 1 (READ-ready) iff the claim+wake ran.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_pump_claim_via_epoll_data_event():
    res = {}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        def writer():
            rc.sched_yield(); rc.sched_yield()
            rc.tcp_send(b.fileno(), b"X")     # make 'a' readable -> epoll pump dispatch
        rc.mn_go(writer)
        res["r"] = rc.wait_fd(a.fileno(), 1, 5000)   # park READ; pump_claim wakes it
        buf = bytearray(1); rc.tcp_recv(a.fileno(), buf, 1)
        res["data"] = bytes(buf)
        rc.netpoll_unregister(a.fileno()); a.close()
        rc.netpoll_unregister(b.fileno()); b.close()
    def driver(): rc.mn_go(main)
    with hang_guard(30, "pump_claim epoll"):
        runloom.run(2, driver)
    assert res["r"] == 1, res          # READ readiness delivered by the pump claim
    assert res["data"] == b"X"


# ---------------------------------------------------------------------------
# netpoll_pump_helpers.c.inc L22 again, but via an ERROR event (peer RST):
# a half-open peer reset folds EPOLLHUP/EPOLLERR into BOTH directions in the
# pump, still routing through pump_dispatch_event -> pump_claim.  Proves the
# claim path runs on the error-fold branch too, not just clean readability.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_pump_claim_on_peer_reset():
    res = {}
    def main():
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0)); srv.listen(8)
        port = srv.getsockname()[1]

        cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cli.connect(("127.0.0.1", port))
        conn, _ = srv.accept()
        conn.setblocking(False)

        def resetter():
            rc.sched_yield(); rc.sched_yield()
            # SO_LINGER 0 -> close sends RST, surfacing as EPOLLHUP/ERR on conn.
            import struct
            cli.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                           struct.pack("ii", 1, 0))
            cli.close()
        rc.mn_go(resetter)
        # park READ; the RST wakes us through the pump's error-fold -> pump_claim.
        res["r"] = rc.wait_fd(conn.fileno(), 1, 5000)
        rc.netpoll_unregister(conn.fileno()); conn.close(); srv.close()
    def driver(): rc.mn_go(main)
    with hang_guard(30, "pump_claim RST"):
        runloom.run(2, driver)
    # A reset must wake the reader (READ bit set by the error fold), never hang.
    assert res["r"] != 0, res


# ---------------------------------------------------------------------------
# netpoll_pump_helpers.c.inc L107-108: the CAS load inside
# runloom_pump_drain_expired.  A timed wait_fd on a never-ready fd expires; the
# pump's deadline-heap drain claims the parker (PARKED->WOKEN), unlinks it, and
# sets ready_out=0 (timeout).  wait_fd returns 0 (timed out) after ~the timeout,
# which is observable proof the drain_expired claim loop ran.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_drain_expired_timeout_claim():
    res = {}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False)
        t0 = time.monotonic()
        res["r"] = rc.wait_fd(a.fileno(), 1, 40)   # 40ms; nobody writes -> drain_expired
        res["dt"] = time.monotonic() - t0
        rc.netpoll_unregister(a.fileno()); a.close(); b.close()
    def driver(): rc.mn_go(main)
    with hang_guard(30, "drain_expired"):
        runloom.run(2, driver)
    assert res["r"] == 0, res                  # 0 == timeout (drain_expired set ready=0)
    assert 0.02 <= res["dt"] < 2.0, res        # waited ~40ms, not instant and not hung


# ---------------------------------------------------------------------------
# netpoll_pump.c.inc L82-89: the per-hub io_uring ring eventfd match in the
# shared epoll pump.  In DEFAULT (non-loop) io_uring mode each M:N hub creates
# its own ring and registers its CQE eventfd into the shared epoll set
# (mn_sched_hub_main add_iouring_ring).  When a hub is idle in
# runloom_netpoll_pump and a hub-bound io_uring recv/send op completes, that
# ring's eventfd fires in epoll_wait; the pump scans runloom_iouring_ring_efds,
# matches (L82->L83 match=ptrs[ri]; L84 break), and L88 runloom_iouring_ring_drain
# / L89 continue.  Drive a real TCPConn echo (TCPConn uses io_uring on this box)
# with deliberate idle gaps between client sends so the server hub is parked in
# the pump when the recv CQE eventfd fires.  io_uring availability is asserted so
# the test self-skips if a kernel ever lacks it.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.skipif(not rc.iouring_available(), reason="needs io_uring")
def test_pump_iouring_ring_eventfd_match():
    p = _run_py(r"""
        import sys
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        assert rc.iouring_available()
        def main():
            def handler(conn):
                while True:
                    d = conn.recv(64)
                    if not d: break
                    conn.send_all(d)
                conn.close()
            port, listeners = rc.serve("127.0.0.1", 0, handler, 2, 128)
            res = {}
            N = 12
            wg = WaitGroup(); wg.add(N)
            def client(cid):
                try:
                    c = rc.TCPConn.connect("127.0.0.1", port)
                    for i in range(15):
                        msg = b"m%02d-%03d" % (cid, i)
                        c.send_all(msg)
                        got = c.recv(64)
                        assert got == msg, (got, msg)
                        # idle gap: lets the server hub fall into the epoll pump
                        # so the NEXT recv CQE eventfd fires while it's parked.
                        runloom.sleep(0.003)
                    c.close()
                    res[cid] = 1
                finally:
                    wg.done()
            for cid in range(N):
                rc.mn_go(lambda cid=cid: client(cid))
            wg.wait()
            for L in listeners: L.close()
            assert sum(res.values()) == N, res
        runloom.run(4, main)
        sys.stdout.write("IOURING_ECHO_OK\n")
    """, timeout=60)
    assert p.returncode == 0, p.stderr[-1500:]
    assert "IOURING_ECHO_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# ---------------------------------------------------------------------------
# netpoll_parker_link.c.inc L31-38: the ghost self-reference detach
#   if (pool->head == p) { pool->head = NULL; EVT(...) }
#   if (pool->by_fd[p->fd] == p) { pool->by_fd[p->fd] = NULL; EVT(...) }
# This self-heal fires ONLY when stack-pool reuse hands a new fiber's parker the
# byte-identical address of a prior occupant that an unlink MISSED -- a residual
# M:N + free-threaded race the source comment says is "not yet fully isolated
# upstream".  There is no deterministic trigger and no Python hook to plant a
# ghost reference, so this is a BEST-EFFORT provocation: maximal stack/parker-
# address reuse via a long run of short-lived fibers that each park+wake on the
# SAME fd, then assert structural integrity held throughout (no list cycle, which
# is exactly what the detach prevents).  If the race never fires here the lines
# stay uncovered; see the report's `unreachable` note.
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_parker_link_ghost_churn_beststeffort():
    res = {"ok": 0}
    def main():
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        ROUNDS = 400
        for _ in range(ROUNDS):
            # one short-lived parker on fd(a); a writer makes it ready, the
            # parker wakes + unlinks, the stack returns to the pool, and the
            # NEXT round's parker is re-issued at the same offset.
            done = bytearray(1)
            def writer():
                rc.sched_yield()
                rc.tcp_send(b.fileno(), b"x")
            rc.mn_go(writer)
            r = rc.wait_fd(a.fileno(), 1, 2000)
            assert r == 1, r
            buf = bytearray(1); rc.tcp_recv(a.fileno(), buf, 1)
            done[0] = 1
        rc.netpoll_unregister(a.fileno()); a.close()
        rc.netpoll_unregister(b.fileno()); b.close()
        res["ok"] = 1
    def driver(): rc.mn_go(main)
    with hang_guard(60, "parker ghost churn"):
        runloom.run(4, driver)
    assert res["ok"] == 1
    # The detach's whole purpose is to keep the lists acyclic; assert it.
    assert rc._self_check(0) == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
