"""Coverage-driven adversarial tests for src/runloom_c/mn_sched_mn_api.c.inc.

The uncovered lines in this fragment fall into two reachability classes, and
this suite is split accordingly:

  REACHABLE (driven here, via the io_uring-as-loop backend):
    * L39-42  runloom_mn_current_iouring_ring(): returns the running hub's
              per-hub io_uring ring.  Only non-NULL -- and only *called* -- under
              the io_uring-as-loop backend (RUNLOOM_IOURING_LOOP=1), where each
              hub thread creates its own ring (mn_sched_hub_main.c.inc:204) and
              the C echo handler / TCPConn iouring recv resolve the ring through
              this accessor (module_io.c.inc:158, io_uring_l_msclose.c.inc:63).
    * L257-273 runloom_mn_hub_request_iouring_cancel(): cross-thread cancel of a
              fiber parked on a *hub-ring* (SINGLE_ISSUER) io_uring op.  The only
              ops with op->ring != NULL are hub-ring recv/send; a Python-reachable
              one is a TCPConn.recv() that takes the single-shot iouring path
              (RUNLOOM_TCPCONN_IOURING=1 + a non-zero recv flag to bypass the
              multishot branch).  G.cancel_wait_fd() -> runloom_iouring_cancel_g()
              sees op->ring != NULL and routes the cancel through this mailbox
              (io_uring_l_ring.c.inc:407).

  GATED-OFF (classified unreachable -- see the module-level UNREACHABLE note and
  the structured report):
    * L214-250 the runloom_use_global_runq() per-g wake-state machine in
              runloom_mn_wake_g(), and
    * L285-310 runloom_mn_sweep_try_claim / runloom_mn_sweep_claim_release.
    All require runloom_use_global_runq() == true, i.e. per-g-tstate mode, which
    runloom_resolve_migratable_mode() (mn_sched_runq.c.inc) enables ONLY when
    RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 -- a KNOWN-CRASH migration mode at H>=2 and a
    HARD-forbidden env for this task.  There is no Python setter and no
    hub-count carve-out, so they cannot be reached safely.

Both reachable scenarios depend on env (RUNLOOM_IOURING_LOOP / _TCPCONN_IOURING)
that the C runtime resolves once at hub-main init, so each runs in a SUBPROCESS
with that env set; for gcov to count the lines the subprocess must EXIT CLEANLY,
so every scenario asserts a returncode of 0 AND a stdout marker carrying the
behaviour we assert on (cancel returned True, recv unblocked with ECANCELED,
echo round-tripped), not merely "it didn't crash".
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# The loop backend must be genuinely active (io_uring available on this box).
LOOP_ENV = {"RUNLOOM_IOURING_LOOP": "1"}
LOOP_TCPCONN_ENV = {"RUNLOOM_IOURING_LOOP": "1", "RUNLOOM_TCPCONN_IOURING": "1"}

pytestmark = pytest.mark.skipif(not FT, reason="M:N + io_uring loop need GIL-disabled build")


def _run(script, env_extra, timeout=60):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


def _no_crash(p, label):
    # POSIX: a process killed by a signal returns the negative signal number; a
    # crash NEVER flushes gcov counters, so it is both a finding and useless for
    # coverage.  Require a clean exit.
    assert p.returncode is not None and p.returncode >= 0, (
        "%s CRASHED with signal %d\nstdout=%s\nstderr=%s"
        % (label, -p.returncode if p.returncode else 0, p.stdout[-400:], p.stderr[-1600:]))


# ===========================================================================
# L39-42: runloom_mn_current_iouring_ring() -- the per-hub ring accessor.
# ===========================================================================
# Scenario A: serve(handler=None) runs each connection ENTIRELY in C
# (runloom_io_c_echo), which calls runloom_mn_current_iouring_ring() to pick the
# hub ring and then drives recv/send on it.  We assert the echo actually
# round-trips through the proactor ring (every reply == request), which is only
# true if the accessor returned the live hub ring (L42) -- a NULL there would
# fall back to the readiness path, still echoing, so the *behaviour* we pin is
# weaker; the stronger evidence is that the loop backend serviced many conns
# without a hang.  Combined with the recv test below (which asserts an ECANCELED
# that ONLY the hub-ring op produces), L42's non-NULL return is established.
_SERVE_CECHO = r'''
import sys, os, socket
sys.path.insert(0, "src")
import runloom
import runloom_c as rc

res = {}
def main():
    # handler=None -> the pure-C echo handler runloom_io_c_echo, which resolves
    # the hub ring via runloom_mn_current_iouring_ring().
    port, listeners = rc.serve("127.0.0.1", 0, None, 2, 64)
    def client():
        replies = []
        for _ in range(12):
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"abcdefgh")
            replies.append(c.recv(64))
            c.close()
        res["replies"] = replies
        for L in listeners:
            L.close()
    rc.mn_go(client)
runloom.run(4, main)
ok = (len(res.get("replies", [])) == 12 and
      all(r == b"abcdefgh" for r in res["replies"]))
sys.stdout.write("CECHO_OK %d\n" % (1 if ok else 0))
'''


def test_iouring_loop_cecho_drives_current_ring_accessor():
    # Drives L39-42 (and the C echo recv/send through the hub ring).
    p = _run(_SERVE_CECHO, LOOP_ENV, timeout=60)
    _no_crash(p, "iouring-loop C-echo")
    assert p.returncode == 0, "C-echo run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1500:])
    assert "CECHO_OK 1" in p.stdout, (
        "io_uring-loop C-echo did not round-trip every reply through the hub "
        "ring\nstdout=%s\nstderr=%s" % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# L257-273: runloom_mn_hub_request_iouring_cancel() -- cross-thread cancel of a
# fiber parked on a hub-ring (SINGLE_ISSUER) io_uring op, deposited in the
# owning hub's cancel mailbox.
# ===========================================================================
# A reader fiber does TCPConn.recv(64, MSG_PEEK): the non-zero flag forces the
# SINGLE-SHOT iouring recv (runloom_iouring_recv -> runloom_iouring_ring_recv on
# the hub ring), which sets g->iouring_op with op->ring == the hub ring and parks
# the fiber.  A second fiber then calls cancel_wait_fd() on it: netpoll has no
# parker for this g, so the call falls through to runloom_iouring_cancel_g, which
# sees op->ring != NULL and routes to runloom_mn_hub_request_iouring_cancel
# (CAS-publishes the op into the hub mailbox at L264, optionally signals the idle
# hub at L268-271, returns 1 at L273).  The owning hub drains the mailbox at its
# loop top and submits IORING_OP_ASYNC_CANCEL, so the recv completes -ECANCELED.
#
# Assertions that pin the function actually ran:
#   * cancel_wait_fd() returned True  -> the CAS at L264 won and L273 returned 1
#     (False would mean netpoll handled it / no op / already-pending; True can
#      ONLY come from the iouring cancel here, since netpoll_cancel_g found no
#      parker for a fiber parked on a bare ring op).
#   * the recv raised OSError with errno == ECANCELED (125) -> the hub drained
#     the mailbox and the kernel cancelled the op (end-to-end proof).
_CANCEL_HUBRING = r'''
import sys, os, socket, errno
sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

MSG_PEEK = socket.MSG_PEEK
res = {"cancel_ret": None, "exc_errno": None, "exc_type": None, "got_data": None}

def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()

    acc = {}; wg_acc = WaitGroup(); wg_acc.add(1)
    def acceptor():
        try: acc["conn"] = lconn.accept()
        finally: wg_acc.done()
    rc.mn_go(acceptor)

    client = rc.TCPConn.connect("127.0.0.1", port)
    wg_acc.wait()
    server_conn = acc["conn"]

    holder = {}; wg = WaitGroup(); wg.add(1)
    def reader():
        holder["g"] = rc.current_g()
        try:
            data = server_conn.recv(64, MSG_PEEK)   # single-shot hub-ring recv; parks (no data)
            res["got_data"] = data
        except OSError as e:
            res["exc_type"] = "OSError"; res["exc_errno"] = e.errno
        except BaseException as e:
            res["exc_type"] = type(e).__name__
        finally:
            wg.done()
    rc.mn_go(reader)

    for _ in range(400):
        if "g" in holder: break
        runloom.sleep(0.003)
    runloom.sleep(0.05)                      # ensure parked on the ring op

    g = holder["g"]
    res["cancel_ret"] = g.cancel_wait_fd()   # -> runloom_mn_hub_request_iouring_cancel

    wg.wait()                                # reader MUST unblock after the cancel
    server_conn.close(); client.close(); lconn.close()

import faulthandler; faulthandler.dump_traceback_later(40, exit=True)
runloom.run(2, main)
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("CANCEL_OK cancel_ret=%r exc_type=%r exc_errno=%r got_data=%r\n" %
                 (res["cancel_ret"], res["exc_type"], res["exc_errno"], res["got_data"]))
'''


def test_iouring_hubring_recv_cancel_routes_through_mailbox():
    # Drives L257-273 (CAS-publish into the mailbox + return 1) AND L39-42 (the
    # recv resolves the hub ring through runloom_mn_current_iouring_ring).
    p = _run(_CANCEL_HUBRING, LOOP_TCPCONN_ENV, timeout=60)
    _no_crash(p, "hub-ring recv cancel")
    assert p.returncode == 0, "cancel run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1500:])
    assert "CANCEL_OK cancel_ret=True" in p.stdout, (
        "G.cancel_wait_fd() did not report a hub-ring iouring cancel (True): the "
        "request was not routed through runloom_mn_hub_request_iouring_cancel\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-1200:]))
    assert "exc_type='OSError'" in p.stdout and "exc_errno=125" in p.stdout, (
        "the parked recv did not unblock with ECANCELED after the mailbox cancel "
        "(the hub never drained the cancel mailbox)\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1200:]))


# ===========================================================================
# L257-273, the already-pending branch (L264 CAS fails -> L266 return 0).
# ===========================================================================
# Two cancels back-to-back on the same hub-ring op: the first publishes the op
# into the mailbox (iouring_cancel_op goes 0 -> op) and returns True; while it is
# still pending the second cancel's CAS at L264 observes a non-zero
# iouring_cancel_op and returns 0 (False) at L266 -- "a cancel is already pending
# for this hub".  (If the op already completed between the two calls,
# runloom_iouring_cancel_g returns 0 earlier at its PARKED check; both yield
# False, so we assert the SECOND is False and the runtime stays correct: the
# recv still unblocks with ECANCELED and no fiber is left stranded.)
_CANCEL_DOUBLE = r'''
import sys, os, socket
sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

MSG_PEEK = socket.MSG_PEEK
res = {"c1": None, "c2": None, "exc_errno": None}

def main():
    lconn = rc.TCPConn.listen("127.0.0.1", 0)
    so = socket.socket(fileno=os.dup(lconn.fileno()))
    port = so.getsockname()[1]; so.detach()
    acc = {}; wg_acc = WaitGroup(); wg_acc.add(1)
    def acceptor():
        try: acc["conn"] = lconn.accept()
        finally: wg_acc.done()
    rc.mn_go(acceptor)
    client = rc.TCPConn.connect("127.0.0.1", port)
    wg_acc.wait()
    server_conn = acc["conn"]
    holder = {}; wg = WaitGroup(); wg.add(1)
    def reader():
        holder["g"] = rc.current_g()
        try:
            server_conn.recv(64, MSG_PEEK)
        except OSError as e:
            res["exc_errno"] = e.errno
        finally:
            wg.done()
    rc.mn_go(reader)
    for _ in range(400):
        if "g" in holder: break
        runloom.sleep(0.003)
    runloom.sleep(0.05)
    g = holder["g"]
    res["c1"] = g.cancel_wait_fd()           # publishes the cancel (True)
    res["c2"] = g.cancel_wait_fd()           # already pending / already cancelled -> False
    wg.wait()
    server_conn.close(); client.close(); lconn.close()

import faulthandler; faulthandler.dump_traceback_later(40, exit=True)
runloom.run(2, main)
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("DOUBLE_OK c1=%r c2=%r exc_errno=%r\n" %
                 (res["c1"], res["c2"], res["exc_errno"]))
'''


def test_iouring_hubring_double_cancel_is_idempotent():
    # Exercises the L264 CAS / L266 already-pending guard and proves a second
    # cancel never duplicates the wake or strands the fiber.
    p = _run(_CANCEL_DOUBLE, LOOP_TCPCONN_ENV, timeout=60)
    _no_crash(p, "hub-ring double cancel")
    assert p.returncode == 0, "double-cancel run failed rc=%d\nstderr=%s" % (
        p.returncode, p.stderr[-1500:])
    assert "DOUBLE_OK c1=True c2=False" in p.stdout, (
        "double cancel was not idempotent (expected c1=True, c2=False)\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-1200:]))
    # End-to-end: the recv still unblocked with ECANCELED -- the fiber is not
    # stranded by the redundant second cancel.
    assert "exc_errno=125" in p.stdout, (
        "after a double cancel the recv did not unblock with ECANCELED\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-1200:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
