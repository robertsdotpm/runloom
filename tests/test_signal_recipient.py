"""Which fiber receives a raised Python signal handler's exception.

The contract: it propagates out of the COOPERATIVE BLOCKING CALL of a fiber
parked in one, through that fiber's own stack, the way a signal interrupting a
real recv() does.  The scheduler picks the recipient in runloom_sched_drain,
and for a long time it picked wrong under one specific, common condition.

WHY THIS IS DETERMINISTIC AND THE OLD TEST WAS NOT
--------------------------------------------------
tests/test_cov95_tcp_conn.py::test_signal_interrupts_parked_io covers the same
contract but fired only ~8% of the time, on macOS only -- 23/300 measured at
the time this was written, and 0/100 on Linux.  Its OBSERVER_POLL constant was
tuned UP to 0.5s to make the failure rare, and its own comment says that is "a
probability shift, not a fix".

Turning that constant DOWN is what makes the bug structural.
runloom_netpoll_pump rounds its epoll/kevent timeout UP to whole milliseconds,
so a 20us sleeper still blocks a full millisecond -- which means the sleeper is
ALWAYS overdue when the pump returns, the ready ring is ALWAYS non-empty, and
the scheduler's signal probe is ALWAYS suppressed.  Measured at 80/80 failures
on Linux before the fix, where the stock 0.5s poll gives 0/100.

That also explains why the old test only ever failed on macOS: epoll_wait
returns EINTR immediately and is not restarted, so at a 0.5s poll the signal
reliably cuts the block before the deadline and the probe runs.

ASSERT THE RECIPIENT, NOT THE OUTCOME
-------------------------------------
These tests check WHICH fiber got the exception.  "An interrupt happened"
passes even when the wrong fiber takes it -- which is the failure, since the
unrelated sleeper dies and the fiber that should have been interrupted stays
parked forever.
"""
import os
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + "/src")
try:
    import runloom_c as _rc
    _IOURING = bool(_rc.iouring_available())
except Exception:                                    # pragma: no cover
    _IOURING = False

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

needs_sigalrm = pytest.mark.skipif(
    not hasattr(__import__("signal"), "SIGALRM"),
    reason="needs SIGALRM")


# A fiber parked in send_all against a peer that never reads, plus an observer
# sleeping on a period far below the pump's 1ms timeout floor.  W1_POLL=20us
# makes a sleeper due on essentially every drain iteration.
_W1 = r'''
import faulthandler, os, signal, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc

POLL = 0.00002
box = {}

def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

def server():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno()))
    box["port"] = s.getsockname()[1]; s.detach()
    sc = L.accept()
    sc.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, (4096).to_bytes(4, "little"))
    # Deliberately unprotected, as the original workload has it: if this loop
    # collects the interrupt it escapes the fiber entry and runloom reports it
    # unraisable -- that print is the failure signature.
    try:
        while "done" not in box:
            rc.sched_sleep(POLL)
    except KeyboardInterrupt:
        box["who"] = "observer"
        raise
    sc.close(); L.close()

def client():
    while "port" not in box:
        rc.sched_yield()
    c = rc.TCPConn.connect("127.0.0.1", box["port"])
    c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, (4096).to_bytes(4, "little"))
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.2)
        c.send_all(b"Z" * (8 * 1024 * 1024))     # parks in wait_fd(WRITE)
        box["who"] = "nobody-sent"
    except KeyboardInterrupt:
        box["who"] = "parked"                    # <-- the contract
    box["done"] = True
    c.close()

faulthandler.dump_traceback_later(20, exit=True)
rc.fiber(server); rc.fiber(client)
try:
    rc.run()
except KeyboardInterrupt:
    box.setdefault("who", "out-of-run")
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("W1 who=%s\n" % box.get("who", "hung"))
'''


def _run(script, timeout=90, env_extra=None):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


@needs_sigalrm
def test_parked_fiber_outranks_a_due_sleeper():
    """A fiber parked in send_all beats an unrelated sleeper to the signal.

    Before the fix the probe was gated on `runloom_sched_ready_empty(s) &&`,
    and a fiber parked in a cooperative call is not in the ready ring at all --
    so one unrelated sleeper coming due suppressed the probe entirely.  The
    observer's sched_sleep then collected the KeyboardInterrupt, died as
    "Exception ignored in: <function server>", and the client stayed parked
    until the faulthandler timeout.  80/80 failures on Linux at that commit.
    """
    p = _run(_W1)
    assert "W1 who=parked" in p.stdout, (
        "the signal did not come out of the parked send_all; who=%r\n"
        "stdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))
    assert "Exception ignored in" not in p.stderr, (
        "the interrupt escaped a fiber entry point instead of being delivered "
        "to the parked call\n%s" % p.stderr[-1500:])


# A fiber parked on an io_uring completion is the OTHER class of fiber whose
# only possible deliverer is the scheduler -- it holds no netpoll parker at all.
_IOU = r"""
import faulthandler, os, signal, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc
box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

def client():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno()))
    port = s.getsockname()[1]; s.detach()
    c = rc.TCPConn.connect("127.0.0.1", port)
    sc = L.accept()
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.2)
        c.recv(64)                     # no peer data -> parks on an io_uring CQE
        box["who"] = "nobody"
    except KeyboardInterrupt:
        box["who"] = "parked"
    except OSError as e:
        box["who"] = "oserror:%s" % e.errno
    sc.close(); c.close(); L.close()

faulthandler.dump_traceback_later(20, exit=True)
rc.fiber(client)
try:
    rc.run()
except KeyboardInterrupt:
    box.setdefault("who", "out-of-run")
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("IOU who=%s\n" % box.get("who", "hung"))
"""


@needs_sigalrm
@pytest.mark.skipif(not _IOURING, reason="io_uring not available")
def test_signal_reaches_a_fiber_parked_on_io_uring():
    """A fiber parked on a CQE gets the signal in its own stack.

    Two independent defects made this impossible, and each hid the other:

    1. `runloom_iouring_signal_wake()` had no declaration and no caller.  Its
       own comment said "the single-thread drain calls it after
       netpoll_signal_wake finds no parker"; `git log -S` says it never did.
       Measured before the fix: `who=out-of-run` 3/3 -- the exception left
       run() instead of the parked recv().
    2. With it wired up, delivery then died in the CONSUMER: the io_uring
       arms of TCPConn recv/recv_into/send/send_all called
       `PyErr_SetFromErrno(PyExc_OSError)` on r < 0 without first checking for
       a pending exception, so a delivered KeyboardInterrupt was overwritten:
       `SystemError: <class 'OSError'> returned a result with an exception
       set`.  The netpoll arms beside them already had the
       `PyErr_Occurred() ? NULL : ...` guard; the io_uring arms never got it,
       because defect 1 meant they were never once exercised.
    """
    p = _run(_IOU, env_extra={"RUNLOOM_TCPCONN_IOURING": "1"})
    assert "SystemError" not in p.stderr, (
        "a delivered signal was overwritten by OSError in the io_uring arm\n%s"
        % p.stderr[-1500:])
    assert "IOU who=parked" in p.stdout, (
        "the signal did not come out of the parked io_uring recv; who=%r\n"
        "stdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))
