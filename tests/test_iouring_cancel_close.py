"""Regression: closing a TCPConn must cancel an in-flight SINGLE-SHOT io_uring
recv parked on a per-hub ring (R7 item 1 / DESIGN_mn_iouring_cancel_fd.md).

History: io_uring pins the underlying struct file, so a plain close(fd) does NOT
complete a parked single-shot IORING_OP_RECV -- the kernel keeps the op live.
TCPConn.close() submits an ASYNC_CANCEL_FD to unblock such a parker, but on the
M:N scheduler a recv issued from inside a hub routes to that hub's SINGLE_ISSUER
ring, which only its OWNING thread may submit to.  close() (running on another
hub) could only cancel the GLOBAL ring, so a hub-ring single-shot recv was never
cancelled: the reader hung forever and io_uring ring teardown could D-state.

Fix: close() also calls runloom_iouring_cancel_fd_all_hubs(fd), which dup()s the
fd (a dup shares the struct file, so ASYNC_CANCEL_FD on it still matches the recv
after the original fd closes) and deposits the dup on every hub's cancel-by-fd
mailbox; each hub submits the cancel at its loop top -> the recv wakes -ECANCELED.

This drives a single-shot hub-ring recv (flags != 0 forces single-shot; a silent
peer + MSG_WAITALL forces a park) and closes it from a DIFFERENT fiber, asserting
the reader wakes with OSError rather than hanging.  A re-hang fails via the child's
own watchdog / the subprocess timeout, never wedging the suite.

Scope: this covers the M:N hub-ring path, which is what the fix changed.  The
single-thread global-ring close-cancel (unmodified runloom_iouring_cancel_fd) is
awkward to drive end-to-end here -- a parked reader monopolises the lone
scheduler's io_uring wait, so a sibling closer fiber's timer can't fire to call
close() -- so it is intentionally not exercised in this file.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="io_uring is Linux-only")


def _iouring_available():
    try:
        out = subprocess.run(
            [PY, "-c", "import sys;sys.path.insert(0,'src');import runloom_c;"
                       "print(runloom_c.iouring_available())"],
            cwd=REPO, env=dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src"),
            capture_output=True, text=True, timeout=30)
        return "True" in out.stdout
    except Exception:
        return False


requires_iouring = pytest.mark.skipif(
    not _iouring_available(), reason="io_uring not available on this kernel")


# A silent peer holds the connection open; the reader issues a single-shot recv
# (flags=MSG_WAITALL != 0 -> NOT the multishot path) that parks; a separate fiber
# closes the reader's conn; the reader must wake with OSError (ECANCELED), never
# hang.  The server closes as soon as the reader is done so mn_run/run drains --
# the WD watchdog must never be the thing that ends the run.
_BODY = r"""
import socket, sys, errno, faulthandler
sys.path.insert(0, "src")
import runloom_c

WD = {wd}
faulthandler.dump_traceback_later(WD, exit=True)   # a real re-hang -> die nonzero
MSG_WAITALL = socket.MSG_WAITALL
st = {{"port": None, "reader_result": None, "reader_conn": None, "parked": False}}

def bound_port(l):
    fd = l.fileno(); sk = socket.socket(fileno=socket.dup(fd))
    p = sk.getsockname()[1]; sk.close(); return p

def server():
    l = runloom_c.TCPConn.listen("127.0.0.1", 0)
    st["port"] = bound_port(l)
    c = l.accept()                       # hold OPEN, send NOTHING (reader parks)
    for _ in range(500):                 # close once the reader is cancelled
        if st["reader_result"] is not None:
            break
        runloom_c.sched_sleep(0.02)
    try: c.close()
    except Exception: pass
    l.close()

def reader():
    while st["port"] is None:
        runloom_c.sched_yield()
    c = runloom_c.TCPConn.connect("127.0.0.1", st["port"])
    st["reader_conn"] = c
    st["parked"] = True
    try:
        {recv_call}                                # single-shot; parks (no data)
        st["reader_result"] = ("returned", nread)
    except OSError as e:
        st["reader_result"] = ("oserror", e.errno)
    except Exception as e:
        st["reader_result"] = ("other", repr(e))

def closer():
    while not st["parked"] or st["reader_conn"] is None:
        runloom_c.sched_yield()
    runloom_c.sched_sleep(0.3)            # let the recv SQE get in flight
    st["reader_conn"].close()            # cross-fiber close -> cancel-by-fd

{drive}

res = st["reader_result"]
if res is None:
    print("FAIL: reader never completed (hang without the fix)"); sys.exit(1)
kind, val = res
# The cancel path raises OSError (ECANCELED, or EBADF/ECONNRESET depending on how
# close() and the kernel race); a clean EOF (returned, 0) also proves no-hang.
ok = (kind == "oserror" and val in (errno.ECANCELED, errno.EBADF, errno.ECONNRESET)) \
     or (kind == "returned")
print("RESULT", res)
print("CANCEL_OK" if ok else "FAIL")
sys.exit(0 if ok else 2)
"""

_DRIVE_MN = ("runloom_c.mn_init(2); runloom_c.mn_fiber(server); "
             "runloom_c.mn_fiber(reader); runloom_c.mn_fiber(closer); "
             "runloom_c.mn_run(); runloom_c.mn_fini()")

# Two entry points into the single-shot hub-ring recv, both of which must hold the
# conn critical section across submit+park (blocker #1) and be cancellable by
# close(): the bytes-returning recv() and the buffer-filling recv_into().
_RECV = 'nread = len(c.recv(65536, MSG_WAITALL) or b"")'
_RECV_INTO = '_b = bytearray(65536); nread = c.recv_into(_b, 0, MSG_WAITALL)'


def _run(drive, recv_call, wd=12):
    body = _BODY.format(wd=wd, drive=drive, recv_call=recv_call)
    try:
        p = subprocess.run(
            [PY, "-c", body], cwd=REPO,
            env=dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
                     RUNLOOM_TCPCONN_IOURING="1"),
            capture_output=True, text=True, timeout=wd + 25)
    except subprocess.TimeoutExpired:
        pytest.fail("close() did NOT cancel the parked io_uring recv (deadlock "
                    "regression -- reader hung on a dead fd)")
    assert p.returncode == 0, (
        "cancel-on-close failed rc=%d (negative => watchdog killed a hang)\n"
        "stdout=%s\nstderr=%s" % (p.returncode, p.stdout[-800:], p.stderr[-1500:]))
    assert "CANCEL_OK" in p.stdout, (p.stdout, p.stderr[-800:])


@requires_iouring
def test_mn_hub_ring_single_shot_recv_cancelled_by_close():
    """The fix target: single-shot recv() on a per-hub SINGLE_ISSUER ring, closed
    from another fiber, must be cancelled by the cancel-by-fd broadcast."""
    _run(_DRIVE_MN, _RECV)


@requires_iouring
def test_mn_hub_ring_single_shot_recv_into_cancelled_by_close():
    """Same for recv_into()'s single-shot fallback -- it must hold the conn
    critical section across submit+park and be cancellable identically to recv()."""
    _run(_DRIVE_MN, _RECV_INTO)
