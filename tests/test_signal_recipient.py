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
import re
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from adv_util import needs_free_threading  # noqa: E402

FT = needs_free_threading()
needs_mn = pytest.mark.skipif(not FT, reason="M:N needs a GIL-disabled build")

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


# select.poll has no kernel fd, so its wrapper reprobes on a short sleep.  That
# makes it the one cooperative blocking call the scheduler cannot see: it holds
# no parker, so "a parker outranks a sleeper" can never reach it, and its sleep
# is byte-identical to an application time.sleep().  sched_sleep_io is the tag
# that distinguishes them.
_COPOLL = r'''
import faulthandler, signal, socket, sys, time
sys.path.insert(0, "src")
import runloom_c as rc
import runloom.monkey
runloom.monkey.patch()
import selectors

box = {}
class InterruptSelect(Exception):
    pass
def handler(*a):
    raise InterruptSelect
signal.signal(signal.SIGALRM, handler)

def selector_fiber():
    rd, wr = socket.socketpair()
    s = selectors.PollSelector()
    s.register(rd, selectors.EVENT_READ)
    t0 = time.monotonic()
    try:
        s.select(10)
        box["who"] = "nobody"
    except InterruptSelect:
        box["who"] = "SELECTOR"
    box["elapsed"] = time.monotonic() - t0
    box["done"] = True
    s.close(); rd.close(); wr.close()

def observer():
    try:
        while "done" not in box:
            rc.sched_sleep(0.0005)      # dense, unrelated application sleep
    except InterruptSelect:
        box["who"] = "observer"
        box["done"] = True

faulthandler.dump_traceback_later(30, exit=True)
signal.setitimer(signal.ITIMER_REAL, 0.3)
rc.fiber(selector_fiber); rc.fiber(observer)
try:
    rc.run()
except InterruptSelect:
    box.setdefault("who", "out-of-run")
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("COPOLL who=%s elapsed=%.2f\n"
                 % (box.get("who", "hung"), box.get("elapsed", -1)))
'''


@needs_sigalrm
def test_selector_outranks_a_dense_unrelated_sleeper():
    """A select.poll wrapper beats an application sleep to its own signal.

    This is the case a parker-based rule cannot reach: CoPoll registers no
    netpoll parker at all (measured: the signal walk reports seen=0), so it is
    only ever a sleeper.  Before sched_sleep_io the dense observer took the
    interrupt and the selector ran its FULL timeout -- measured 10.00s, 5/5,
    which is exactly the shape of the CPython test_select_interrupt_exc failure
    that got two earlier fix attempts reverted.  After: 0.30s, 5/5, raised in
    the selector's own frame at the itimer deadline.
    """
    p = _run(_COPOLL, timeout=120)
    assert "COPOLL who=SELECTOR" in p.stdout, (
        "the interrupt did not land in the selector's frame: %r\n"
        "stdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))
    # The timeout is 10s and the alarm is at 0.3s: a pass that took the full
    # timeout would mean the signal arrived by some other route.
    import re
    m = re.search(r"elapsed=([0-9.]+)", p.stdout)
    assert m and float(m.group(1)) < 2.0, (
        "the selector returned but not promptly (%s) -- the signal did not "
        "interrupt the select" % p.stdout.strip())


# Delivering to a sleeper means pulling it out of the MIDDLE of the sleep heap,
# which is the one new data-structure operation in this area
# (runloom_sleep_remove: linear find, fill from the tail, restore the invariant
# in both directions).  Every other heap op only ever touches the root, so
# nothing else would exercise a hole fill.
_HEAP = r"""
import faulthandler, signal, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc
import runloom.monkey
runloom.monkey.patch()
import selectors

N = 60
box = {"woke": []}
class InterruptSelect(Exception):
    pass
def handler(*a):
    raise InterruptSelect
signal.signal(signal.SIGALRM, handler)

def sleeper(i):
    rc.sched_sleep(0.05 + i * 0.002)      # staggered: real heap structure
    box["woke"].append(i)

def selector_fiber():
    rd, wr = socket.socketpair()
    s = selectors.PollSelector()
    s.register(rd, selectors.EVENT_READ)
    try:
        s.select(10)
        box["sel"] = "nobody"
    except InterruptSelect:
        box["sel"] = "SELECTOR"
    s.close(); rd.close(); wr.close()

faulthandler.dump_traceback_later(40, exit=True)
for i in range(N):
    rc.fiber(lambda i=i: sleeper(i))
rc.fiber(selector_fiber)
signal.setitimer(signal.ITIMER_REAL, 0.1)   # fires while most sleepers are queued
try:
    rc.run()
except InterruptSelect:
    box.setdefault("sel", "out-of-run")
faulthandler.cancel_dump_traceback_later()
woke = box["woke"]
sys.stdout.write("HEAP sel=%s woke=%d/%d ordered=%s\n"
                 % (box.get("sel"), len(woke), N, woke == sorted(woke)))
"""


@needs_sigalrm
def test_sleep_heap_survives_removing_a_signalled_sleeper():
    """Yanking one sleeper out of the middle must not disturb the others.

    What this pins is runloom_sleep_remove: a linear find, a fill from the
    tail, and a restore of the heap invariant in both directions.  Every other
    heap operation only ever touches the root, so nothing else exercises a hole
    fill, and a broken one would silently drop or misorder timers.

    IT DELIBERATELY DOES NOT ASSERT WHO RECEIVES THE SIGNAL.  An earlier
    version did, and it was ~25% flaky on macOS (measured: 5 of 20 runs gave
    `sel=nobody woke=59/60`), which is what caught it in CI rather than here.
    With 60 sleepers competing, whether the selector is IN the heap at the
    instant the scheduler probes is a race: if it happens to be mid-reprobe --
    running, or already re-queued -- there is no sleep_io waiter to find, the
    probe does not run, and an unrelated sleeper collects the interrupt in its
    own eval loop.  Delivery to a sleep_io sleeper is best-effort by
    construction (100% on epoll, ~75% on kqueue here); only delivery to a
    PARKER is guaranteed.  Asserting the recipient here was asserting something
    the design does not promise.

    So the assertions are the ones that hold unconditionally: the heap stays
    ordered, and every sleeper except at most the one that took the interrupt
    still wakes.  A corrupted heap shows up as a wrong order or a lost sleeper,
    both of which this still catches.
    """
    p = _run(_HEAP, timeout=120)
    m = re.search(r"HEAP sel=(\S+) woke=(\d+)/(\d+) ordered=(\S+)", p.stdout)
    assert m, ("the heap workload produced no verdict\nstdout=%s\nstderr=%s"
               % (p.stdout, p.stderr[-1500:]))
    who, woke, total, ordered = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
    assert ordered == "True", (
        "sleep heap came back OUT OF ORDER after a mid-heap removal -- "
        "runloom_sleep_remove did not restore the invariant: %s" % p.stdout.strip())
    assert woke >= total - 1, (
        "%d of %d sleepers never woke: a removal lost entries from the heap"
        % (woke, total))
    assert who in ("SELECTOR", "nobody"), (
        "unexpected verdict from the heap workload: %s" % p.stdout.strip())


# ---------------------------------------------------------------------------
# M:N.  Until this was wired up, a raised handler was ALWAYS carried out of
# mn_run and no hub fiber was ever interrupted -- so an `except
# KeyboardInterrupt:` or a `finally:` inside a hub fiber simply did not run.
# The wake arm for hub parkers already existed in runloom_netpoll_signal_wake
# (runloom_mn_wake_g); it was unreachable behind an owner filter keyed on the
# calling scheduler, and a hub fiber's parker is owned by its own hub's sched.
# ---------------------------------------------------------------------------
_MN = r'''
import faulthandler, os, signal, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc

HUBS = int(os.environ.get("MN_HUBS", "4"))
box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

def worker():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno()))
    port = s.getsockname()[1]; s.detach()
    c = rc.TCPConn.connect("127.0.0.1", port)
    sc = L.accept()
    try:
        c.recv(64)                    # no peer data -> parks on this hub's netpoll
        box["who"] = "returned"
    except KeyboardInterrupt:
        box["who"] = "fiber"          # <-- the contract
    finally:
        box["finally_ran"] = True
    sc.close(); c.close(); L.close()

faulthandler.dump_traceback_later(30, exit=True)
rc.mn_init(HUBS)
rc.mn_fiber(worker)
signal.setitimer(signal.ITIMER_REAL, 0.3)
try:
    rc.mn_run()
except KeyboardInterrupt:
    box.setdefault("who", "out-of-mn_run")
try:
    rc.mn_fini()
except Exception:
    pass
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("MN who=%s finally_ran=%s\n"
                 % (box.get("who"), box.get("finally_ran", False)))
'''


@needs_sigalrm
@needs_mn
@pytest.mark.parametrize("hubs", ["2", "8"])
def test_signal_reaches_a_hub_fiber(hubs):
    """A fiber parked on a hub gets the interrupt in its own stack.

    Measured before the fix, every run: who=out-of-mn_run, finally_ran=False --
    the fiber stayed parked and its cleanup never ran.  After: who=fiber,
    60/60 across 2, 8 and 16 hubs.
    """
    p = _run(_MN, timeout=120, env_extra={"MN_HUBS": hubs})
    assert "MN who=fiber finally_ran=True" in p.stdout, (
        "the interrupt did not reach the parked hub fiber: %r\n"
        "stdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))


@needs_sigalrm
@needs_mn
@pytest.mark.skipif(not _IOURING, reason="io_uring not available")
def test_mn_signal_reaches_a_hub_fiber_parked_on_io_uring():
    """Same contract as the netpoll M:N test, but parked on a CQE.

    `RUNLOOM_TCPCONN_IOURING=1` routes the same `recv` through the multishot
    io_uring path instead of netpoll, so this is the io_uring arm of M:N
    delivery.  Measured `who=out-of-mn_run` 5/5 before the fix.

    TWO defects, and the first hid the second:

    1. `runloom_iou_sigwaiters` was RUNLOOM_TLS, so a hub thread's waiters were
       not merely missed by the walk -- the list holding them was unreachable
       from the main thread, the only thread that may run Python signal
       handlers.  Made process-wide under a lock.

    2. With the list shared, the walk then found the right waiter and the wake
       still did nothing, because the M:N branch parks with
       `runloom_sched_park_current` + `runloom_coro_yield` while
       `runloom_sched_wake_safe` only acts if it wins the `parked_safe` 1->0
       CAS -- a flag `park_current` never sets.  It bumped a `wake_pending`
       credit nobody consumes and returned success.  A parker now records the
       hub it parked on and the wake routes `runloom_mn_wake_g` for a hub.

    Racing the CQE's own wake is safe: mn_wake_g dedups (PARKED->QUEUED once
    under the wake_state machine; the in_sub_queue CAS in hub_submit
    otherwise), so the loser drops rather than double-queueing the g.
    """
    p = _run(_MN, env_extra={"RUNLOOM_TCPCONN_IOURING": "1"}, timeout=120)
    assert "SystemError" not in p.stderr, (
        "a delivered signal was overwritten by OSError in the io_uring arm\n%s"
        % p.stderr[-1500:])
    assert "MN who=fiber finally_ran=True" in p.stdout, (
        "the interrupt did not reach the hub fiber parked on a CQE: %r\n"
        "stdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))


# A blocked SEND has no multishot variant, so it parks on a SINGLE-SHOT io_uring
# op -- the other half of io_uring delivery, and a different park site from the
# multishot recv above (a hub owns its own ring whenever any io_uring feature is
# enabled, so runloom_iouring_send routes to runloom_iouring_ring_do, not the
# global-ring runloom_iouring_do).
_MN_SINGLEOP = r'''
import faulthandler, os, signal, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc
HUBS = int(os.environ.get("MN_HUBS", "4"))
box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

def worker():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno()))
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    port = s.getsockname()[1]; s.detach()
    c = rc.TCPConn.connect("127.0.0.1", port)
    cs = socket.socket(fileno=os.dup(c.fileno()))
    cs.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    cs.detach()
    sc = L.accept()                       # never read from -> the peer window fills
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.4)
        c.send_all(b"x" * (8 * 1024 * 1024))   # parks on a single-shot SEND CQE
        box["who"] = "returned"
    except KeyboardInterrupt:
        box["who"] = "fiber"
    except OSError as e:
        box["who"] = "oserror:%s" % e.errno
    finally:
        box["finally_ran"] = True
    try:
        sc.close(); c.close(); L.close()
    except Exception:
        pass

faulthandler.dump_traceback_later(25, exit=True)
rc.mn_init(HUBS); rc.mn_fiber(worker)
try:
    rc.mn_run()
except KeyboardInterrupt:
    box.setdefault("who", "out-of-mn_run")
try: rc.mn_fini()
except Exception: pass
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("MN-SINGLEOP who=%s finally_ran=%s\n"
                 % (box.get("who"), box.get("finally_ran", False)))
'''


@needs_sigalrm
@needs_mn
@pytest.mark.skipif(not _IOURING, reason="io_uring not available")
def test_mn_signal_reaches_a_hub_fiber_parked_on_a_single_shot_op():
    """A blocked send parks on a SINGLE-SHOT CQE, and gets the signal in-fiber.

    Distinct from the multishot test above, and it is the harder half.  A
    multishot waiter can just be woken and returned: its buffers live on the
    shared handle, so leaving early strands nothing.  A single-shot op cannot,
    because the kernel writes its CQE through the `op` record on the PARKED
    FIBER'S OWN STACK -- returning while it is in flight is a use-after-free,
    and op.result is still the INT32_MIN sentinel.

    So the signal path here cancels the op and KEEPS PARKING, exiting only once
    op.result is genuinely set (real completion, or the cancel's -ECANCELED).
    Re-parking works because op.wait is a one-way latch only a completing
    drainer moves, and the drainer stores op.result before that exchange -- a
    signal wake leaves it PARKED, so the drainer still fires our wake.

    Measured `who=out-of-mn_run finally_ran=False` before, 50/50
    `who=fiber finally_ran=True` after (10 runs each at 1/2/4/8/16 hubs).
    """
    p = _run(_MN_SINGLEOP, env_extra={"RUNLOOM_TCPCONN_IOURING": "1"}, timeout=120)
    assert "SystemError" not in p.stderr, (
        "a delivered signal was overwritten by OSError in the io_uring arm\n%s"
        % p.stderr[-1500:])
    assert "MN-SINGLEOP who=fiber finally_ran=True" in p.stdout, (
        "the interrupt did not reach the hub fiber parked on a single-shot op; "
        "who=%r\nstdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))


_MN_POLL = r'''
import faulthandler, os, signal, socket, sys
sys.path.insert(0, "src")
import runloom_c as rc
# PATCH BEFORE SPAWNING.  monkey.patch() wraps runloom_c.mn_fiber, and that
# wrapper is what bumps the thread-local counter _co_sleep_io tests; a fiber
# spawned through the UNPATCHED mn_fiber never gets wrapped, so its reprobe
# silently degrades to a plain sched_sleep and never registers as a signal
# recipient.  An earlier version of this test patched inside the worker and so
# never exercised CoPoll's sleep_io path at all.
import runloom.monkey; runloom.monkey.patch()
import selectors

HUBS = int(os.environ.get("MN_HUBS", "4"))
box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

def worker():
    L = rc.TCPConn.listen("127.0.0.1", 0)
    s = socket.socket(fileno=os.dup(L.fileno()))
    port = s.getsockname()[1]; s.detach()
    c = rc.TCPConn.connect("127.0.0.1", port)
    sc = L.accept()
    sel = selectors.PollSelector()
    box["inner"] = type(getattr(sel, "_selector", None)).__name__
    try:
        sel.register(sc.fileno(), selectors.EVENT_READ)
        sel.select(5)                 # CoPoll: poll(0) + sched_sleep_io reprobe
        sel.close()
        box["who"] = "returned"
    except KeyboardInterrupt:
        box["who"] = "fiber"
    finally:
        box["finally_ran"] = True
    try:
        sc.close(); c.close(); L.close()
    except Exception:
        pass

faulthandler.dump_traceback_later(30, exit=True)
rc.mn_init(HUBS); rc.mn_fiber(worker)
signal.setitimer(signal.ITIMER_REAL, 0.4)
try:
    rc.mn_run()
except KeyboardInterrupt:
    box.setdefault("who", "out-of-mn_run")
try: rc.mn_fini()
except Exception: pass
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("MN-POLL who=%s finally_ran=%s inner=%s\n"
                 % (box.get("who"), box.get("finally_ran", False), box.get("inner")))
'''


@needs_sigalrm
@needs_mn
def test_mn_signal_reaches_a_hub_fiber_sleeping_in_select_poll():
    """select.poll in a HUB fiber gets its own signal.  The last gap, closed.

    A poll object has no kernel fd, so CoPoll drains with poll(0) and reprobes
    on sched_sleep_io.  That sleeper lives in ITS HUB's sleep heap, and only the
    main thread may run Python signal handlers -- and the main thread must not
    touch a hub's heap, which the hub mutates on every timer tick.  That is why
    this was the last path still carrying out of mn_run.

    The fix does not reach into the heap at all.  A sleep_io sleeper registers a
    stack node in a process-wide list (the same idiom the io_uring parkers use),
    the main thread marks a node and issues the wake, and the sleeper removes
    itself from its own heap on its own thread when it resumes.

    The subtle part is that a hub sleeper has TWO possible schedulers -- its
    deadline (the hub's timer pop) and the signal wake -- and ready_push has no
    dedup, so both enqueuing it would resume the fiber twice onto a coro the
    first run-to-completion may already have freed.  Both sides now CAS
    g->sleep_claimed and only the winner enqueues; delivery is unaffected either
    way because the exception rides the waiter node, not the enqueue.

    Measured `who=out-of-mn_run finally_ran=False` before, 50/50
    `who=fiber finally_ran=True` after (10 runs each at 1/2/4/8/16 hubs).
    """
    p = _run(_MN_POLL, timeout=120)
    assert "inner=CoPoll" in p.stdout, (
        "the workload did not go through CoPoll, so it never exercised the "
        "sleep_io reprobe -- check the monkey.patch() ordering: %r\nstderr=%s"
        % (p.stdout.strip(), p.stderr[-1500:]))
    assert "MN-POLL who=fiber finally_ran=True" in p.stdout, (
        "the interrupt did not reach the hub fiber sleeping in select.poll: "
        "%r\nstdout=%s\nstderr=%s"
        % (p.stdout.strip(), p.stdout, p.stderr[-1500:]))
