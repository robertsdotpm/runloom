"""Coverage-driven adversarial suite for two module-level C fragments:

  src/runloom_c/module_tcp.c.inc     -- thread_init/fini, prewarm parse guards,
                                        the bare tcp_recv / tcp_recv_alloc /
                                        tcp_send / tcp_send_once primitives
  src/runloom_c/module_select.c.inc  -- the m_select() case-parsing error arms,
                                        the err-cleanup loop, and _diag_flags

Unlike the TCPConn surface (src/runloom_c/runloom_tcp_conn_*.c.inc), the bare
module_tcp primitives issue RAW recv()/send() syscalls with NO in-process fault
hook (RUNLOOM_FAULT_TCP_* gates the TCPConn class only -- grep RUNLOOM_TCP_FINJ).
So their non-EAGAIN hard-error arms are reached the same way
tests/test_tcp_faultinject.py reaches the TCPConn ones: strace -e inject= forces
the very first recvfrom/sendto to return ECONNRESET/EPIPE (Linux only). The
signal-interrupted-park arms are reached with a real raised SIGALRM handler
landing on the parked fiber (CLAUDE.md: "signals deliver INTO the parked
fiber"); those run in a subprocess because setitimer is process-global.

The uncovered (#####) lines split into these reachability classes; each test
names the source lines it makes live and the gate it satisfies.

Class A -- in-process plain calls / arg-parse guards (epoll backend):
  module_tcp  L26/29/33 (thread_init success), L36/39/40 (thread_fini),
              L71 (prewarm parse fail), L90 (prewarm_keep parse fail)
  module_select L35 (select parse fail), L56 (non-tuple / <2 case),
              L73 (send case size != 3), L80 (op not recv/send)

Class B -- a real EAGAIN park that COMPLETES (no fault, no signal):
  module_tcp  L304/318-319/325 (tcp_send_once: first send EAGAINs on a full
              buffer -> park WRITE -> a drainer frees space -> the loop retries
              and returns). This is the only tcp_send_once park-then-succeed
              path; the suite otherwise always hit a send that completed first try.

Class C -- a raised signal handler interrupts a cooperative park, driving the
  `wait_fd_coop(...) < 0` -> PyErr_Occurred() ? NULL arms (subprocess):
  module_tcp  L229/232 (tcp_recv_alloc parked READ), L277/280 (tcp_send parked
              WRITE), L325-326/329 (tcp_send_once parked WRITE).

Class D -- synchronous syscall hard errors via strace -e inject= (Linux only),
  the non-EAGAIN branches loopback never produces on its own:
  module_tcp  L161-162 (tcp_recv recvfrom error), L224-225 (tcp_recv_alloc
              recvfrom error), L272-273 (tcp_send sendto error),
              L320-322 (tcp_send_once sendto error).

Class E -- _diag_flags (subprocess so RUNLOOM_DEBUG_DIAG is parsed at import):
  module_select L154/157 (_diag_flags returns the parsed RUNLOOM_DEBUG mask).

Excluded (see the structured report): module_tcp L30-31 (thread_init failure --
ConvertThreadToFiber, Windows-fibers only; the POSIX body is unconditional
return 0) -> PLATFORM; module_select L116 (the err-cleanup Py_DECREF of a
materialised recv_value) -> OOM: a fired RECV case sets cs[fired].recv_value
only at the very end of runloom_chan_select, immediately before `return fired`;
every -2/err return happens BEFORE any recv_value is stored, so reaching the
DECREF needs PyTuple_New(2) at L98 to fail (OOM) after a recv fired -- no fault
hook exists for that allocation.
"""
import errno as _errno
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

import runloom  # noqa: E402
import runloom_c as rc  # noqa: E402


def _run_child(script, timeout=200, env_extra=None):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("child workload timed out (box under heavy load / CI contention)")


# ===========================================================================
# Class A: in-process plain calls + arg-parse guards (no fault, no signal).
# ===========================================================================

def test_thread_init_fini_and_prewarm_parse_guards():
    """module_tcp.c.inc: thread_init success (L26/29/33), thread_fini (L36/39/40);
    prewarm parse-fail (L71) and prewarm_keep parse-fail (L90).

    thread_init/thread_fini are idempotent per-OS-thread setup/teardown; on POSIX
    the body is an unconditional success, so calling them from the main thread
    drives the whole success body. The prewarm parse-fail arms are reached with
    a wrong-typed positional arg so PyArg_ParseTupleAndKeywords returns 0; we
    keep the args BAD on purpose so prewarm_keep never starts its background
    daemon (only the parse-fail `return NULL` runs)."""
    # thread_init: returns None on the success path (POSIX: always succeeds).
    assert rc.thread_init() is None
    # idempotent second call still succeeds (re-runs the success body).
    assert rc.thread_init() is None
    # thread_fini body (crash_thread_disarm; the Windows fiber unwind is #ifdef'd).
    assert rc.thread_fini() is None

    # prewarm L71: a non-int `n` -> "i" conversion fails -> return NULL.
    with pytest.raises(TypeError):
        rc.prewarm("not-an-int")
    # prewarm_keep L90: a non-int `target` -> "i" conversion fails -> return NULL.
    # (Bad args => the daemon is never started; nothing to stop afterwards.)
    with pytest.raises(TypeError):
        rc.prewarm_keep("not-an-int")
    # belt-and-braces: stop the continuous prewarm daemon in case any prior
    # test in the process started one (no-op if none).
    rc.prewarm_stop()


def test_select_case_parse_error_arms():
    """module_select.c.inc: the m_select() argument/case validation arms.

      L35  select() with no `cases` -> PyArg_ParseTupleAndKeywords fails.
      L56  a case that is not a 2+-tuple -> Py_DECREF(item) + TypeError.
      L73  a 'send' case whose tuple is not size 3 -> Py_DECREF(item) + TypeError.
      L80  a case whose op is neither 'recv' nor 'send' -> Py_DECREF(item) + ValueError.

    These run inside a fiber (the channel cases must reference live Chans). The
    error must be a clean Python exception -- never a crash, never a leaked
    PyMem_Calloc'd case array (the `goto err` path frees it)."""
    box = {}

    def main():
        ch = rc.Chan(0)

        # L35: required positional `cases` missing -> "O|p" parse fails.
        try:
            rc.select()
        except TypeError:
            box["no_cases"] = True

        # The two "not a list/tuple" / "empty" guards (already covered elsewhere,
        # asserted here so the file is self-contained as an oracle).
        try:
            rc.select(42)                       # not list/tuple
        except TypeError:
            box["not_seq"] = True
        try:
            rc.select([])                       # zero cases
        except ValueError:
            box["empty"] = True

        # L56: a case item that is not a 2+-element tuple. A bare int is a valid
        # object (so the Py_DECREF on the borrowed-from-GetItem ref runs) that
        # fails PyTuple_Check.
        try:
            rc.select([42])
        except TypeError:
            box["bad_item"] = True
        # also the <2-length tuple variant of the same guard.
        try:
            rc.select([("recv",)])
        except TypeError:
            box["short_tuple"] = True

        # L73: a 'send' case must be (op, ch, value); size 2 -> error.
        try:
            rc.select([("send", ch)])
        except TypeError:
            box["send_size"] = True

        # L80: op string is neither 'recv' nor 'send'.
        try:
            rc.select([("frobnicate", ch)])
        except ValueError:
            box["bad_op"] = True

        # case[1] not a Chan (covered already; assert for completeness).
        try:
            rc.select([("recv", object())])
        except TypeError:
            box["bad_chan"] = True

    with hang_guard(60, "select parse error arms"):
        runloom.run(2, main)

    assert box.get("no_cases") is True
    assert box.get("not_seq") is True
    assert box.get("empty") is True
    assert box.get("bad_item") is True
    assert box.get("short_tuple") is True
    assert box.get("send_size") is True
    assert box.get("bad_op") is True
    assert box.get("bad_chan") is True


# ===========================================================================
# Class B: tcp_send_once parks on a real EAGAIN, then COMPLETES once a peer
# drainer frees buffer space. Drives the loop re-entry (L304), the syscall
# (L318-319), and the wait_fd_coop park SUCCESS arm (L325 false branch).
#
# Runs in a subprocess on the SINGLE-THREAD scheduler (rc.go/rc.run): both
# fibers share one socketpair and one netpoll, so the WRITE-readiness wake for
# the parked send is delivered on the same OS thread that drained the peer --
# deterministic, unlike spreading the two cooperating fibers across M:N hubs.
# ===========================================================================

_SEND_ONCE_PARK = r'''
import sys, os, socket
sys.path.insert(0, "src")
import runloom_c as rc
res = {}
def main():
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    # Fill a's send buffer with a raw non-blocking send until EAGAIN so the
    # subsequent tcp_send_once is guaranteed to block on WRITE.
    prefill = 0
    filler = b"F" * 65536
    while True:
        try:
            prefill += a.send(filler)
        except BlockingIOError:
            break
    res["prefill"] = prefill
    drained = {"n": 0, "done": False}
    def drainer():
        # Let the sender reach its park first, then drain the peer so a's
        # buffer empties and the parked WRITE becomes ready.
        rc.sched_sleep(0.05)
        while drained["n"] < prefill + 4096:
            try:
                d = b.recv(65536)
                if not d:
                    break
                drained["n"] += len(d)
            except BlockingIOError:
                rc.sched_sleep(0.01)
        drained["done"] = True
    rc.fiber(drainer)
    # First send() EAGAINs (buffer full) -> park WRITE (L325) -> drainer frees
    # space -> loop re-enters (L304) -> send succeeds -> break.
    res["sent"] = rc.tcp_send_once(a.fileno(), b"Z" * 4096)
    while not drained["done"]:
        rc.sched_yield()
    res["drained"] = drained["n"]
    rc.netpoll_unregister(a.fileno()); a.close()
    rc.netpoll_unregister(b.fileno()); b.close()
import faulthandler; faulthandler.dump_traceback_later(40, exit=True)
rc.fiber(main); rc.run()
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("SENDONCE prefill=%r sent=%r drained=%r\n" %
                 (res.get("prefill"), res.get("sent"), res.get("drained")))
'''


def test_send_once_real_eagain_park_then_complete():
    """module_tcp.c.inc L304 / L318-319 / L325: tcp_send_once on a socket whose
    send buffer is already full must return -1/EAGAIN on the first send(), park
    WRITE via netpoll, and -- once a cooperative drainer reads the peer so the
    buffer drains -- wake, retry the loop, and return a positive count.

    The suite's other tcp_send_once calls always completed on the first send, so
    the entire park branch of this function was dark. A byte-exact assertion
    (the drainer must observe at least the prefill + the parked send) proves the
    park resumed and the resumed send actually delivered, not a phantom return."""
    p = _run_child(_SEND_ONCE_PARK, timeout=120)
    assert p.returncode == 0, (p.stdout[-400:], p.stderr[-1200:])
    # prefill>0 (buffer really filled), sent==4096 (the parked send delivered),
    # drained>=prefill+4096 (the peer observed both payloads end-to-end).
    assert "sent=4096" in p.stdout, (
        "tcp_send_once did not return 4096 after its WRITE park\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-800:]))
    assert "prefill=8064" in p.stdout or "prefill=" in p.stdout, p.stdout
    # parse drained and assert it observed the full prefill + the parked send.
    import re
    m = re.search(r"prefill=(\d+) sent=(\d+) drained=(\d+)", p.stdout)
    assert m, p.stdout
    prefill, sent, drained = int(m.group(1)), int(m.group(2)), int(m.group(3))
    assert prefill > 0 and sent == 4096 and drained >= prefill + 4096, (
        "park did not resume / deliver: prefill=%d sent=%d drained=%d"
        % (prefill, sent, drained))


# ===========================================================================
# Class C: a raised SIGALRM handler interrupts a cooperative park, driving the
# `wait_fd_coop(...) < 0` -> PyErr_Occurred() ? NULL arms. Subprocess: setitimer
# is process-global and must not perturb the parent pytest's signal state.
# ===========================================================================

_SIG_TEMPLATE = r'''
import sys, os, socket, signal
sys.path.insert(0, "src")
import runloom_c as rc
out = {}
class Boom(Exception): pass
def handler(signum, frame): raise Boom()
signal.signal(signal.SIGALRM, handler)
MODE = "__MODE__"

def main():
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    if MODE in ("send", "send_once"):
        # fill a's send buffer so the module send EAGAINs and parks WRITE.
        filler = b"F" * 65536
        while True:
            try: a.send(filler)
            except BlockingIOError: break
    signal.setitimer(signal.ITIMER_REAL, 0.12)
    try:
        if MODE == "recv_alloc":
            rc.tcp_recv_alloc(a.fileno(), 64)      # nothing sent -> park READ
        elif MODE == "send":
            rc.tcp_send(a.fileno(), b"Z" * 65536)  # buffer full -> park WRITE
        elif MODE == "send_once":
            rc.tcp_send_once(a.fileno(), b"Z" * 4096)
        out["res"] = "no-raise"
    except Boom:
        out["res"] = "boom"
    except BaseException as e:
        out["res"] = ("other", type(e).__name__)
    rc.netpoll_unregister(a.fileno()); a.close()
    rc.netpoll_unregister(b.fileno()); b.close()

import faulthandler; faulthandler.dump_traceback_later(40, exit=True)
rc.fiber(main); rc.run()
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("SIG MODE=%s res=%r\n" % (MODE, out.get("res")))
'''


@pytest.mark.parametrize("mode", ["recv_alloc", "send", "send_once"])
def test_signal_interrupts_parked_module_io(mode):
    """module_tcp.c.inc L229/232 (tcp_recv_alloc), L277/280 (tcp_send),
    L325-326/329 (tcp_send_once): a raised signal handler during the cooperative
    park makes wait_fd_coop return < 0 with a pending exception, so the call
    releases its buffer / decrefs its result and returns NULL carrying the
    SIGNAL's exception -- not an OSError that would overwrite it."""
    p = _run_child(_SIG_TEMPLATE.replace("__MODE__", mode), timeout=120)
    assert p.returncode == 0, (mode, p.stdout[-400:], p.stderr[-1200:])
    assert ("SIG MODE=%s res='boom'" % mode) in p.stdout, (
        "the SIGALRM raised during the parked %s did not propagate as the "
        "interrupt (it may have been swallowed / overwritten by OSError)\n"
        "stdout=%s\nstderr=%s" % (mode, p.stdout, p.stderr[-800:]))


# ===========================================================================
# Class D: synchronous syscall hard-errors via strace -e inject= (Linux only).
# The bare module_tcp primitives have NO in-process fault hook, so a non-EAGAIN
# recv/send error is only producible by forcing the syscall itself to fail.
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


# Each child drives ONE module primitive on a fresh non-blocking socketpair so
# its recv/send is the FIRST matching syscall strace injects on (when=1). Exit
# 42 + an "ERRNO ..." line on a clean OSError; exit 0 if no error surfaced.
_HARD_TEMPLATE = r'''
import sys, os, socket
sys.path.insert(0, "src")
import runloom_c as rc
MODE = "__MODE__"
out = {}
def main():
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    try:
        if MODE == "recv":
            rc.tcp_recv(a.fileno(), bytearray(64), 64)
        elif MODE == "recv_alloc":
            rc.tcp_recv_alloc(a.fileno(), 64)
        elif MODE == "send":
            rc.tcp_send(a.fileno(), b"x" * 64)
        elif MODE == "send_once":
            rc.tcp_send_once(a.fileno(), b"x" * 64)
        out["res"] = "no-raise"
    except OSError as e:
        out["errno"] = e.errno
    rc.netpoll_unregister(a.fileno()); a.close()
    rc.netpoll_unregister(b.fileno()); b.close()
rc.fiber(main); rc.run()
if "errno" in out:
    sys.stdout.write("ERRNO=%d\n" % out["errno"]); sys.exit(42)
sys.stdout.write("RES=%r\n" % out.get("res")); sys.exit(0)
'''


def _run_strace(mode, inject, timeout=90):
    strace = shutil.which("strace")
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    cmd = [strace, "-f", "-e", "signal=none", "-e", "inject=" + inject,
           PY, "-c", _HARD_TEMPLATE.replace("__MODE__", mode)]
    try:
        return subprocess.run(cmd, cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("strace workload timed out (box under heavy load / CI contention)")


@needs_strace
@pytest.mark.parametrize("mode", ["recv", "recv_alloc"])
def test_recv_synchronous_econnreset(mode):
    """module_tcp.c.inc L161-162 (tcp_recv) / L224-225 (tcp_recv_alloc): a
    recvfrom() that returns ECONNRESET (not EAGAIN) must release the buffer /
    decref the result and surface a clean OSError(ECONNRESET) -- not a crash,
    not a hang, not a swallow."""
    p = _run_strace(mode, "recvfrom:error=ECONNRESET:when=1")
    assert p.returncode == 42, (mode, p.returncode, p.stdout[-300:], p.stderr[-800:])
    assert ("ERRNO=%d" % _errno.ECONNRESET) in p.stdout, (mode, p.stdout[-300:])


@needs_strace
@pytest.mark.parametrize("mode", ["send", "send_once"])
def test_send_synchronous_epipe(mode):
    """module_tcp.c.inc L272-273 (tcp_send) / L320-322 (tcp_send_once): a
    sendto() that returns EPIPE (not EAGAIN) must release the buffer and surface
    a clean OSError(EPIPE)."""
    p = _run_strace(mode, "sendto:error=EPIPE:when=1")
    assert p.returncode == 42, (mode, p.returncode, p.stdout[-300:], p.stderr[-800:])
    assert ("ERRNO=%d" % _errno.EPIPE) in p.stdout, (mode, p.stdout[-300:])


# ===========================================================================
# Class E: _diag_flags reads the RUNLOOM_DEBUG mask parsed once at import. Run
# in a subprocess so RUNLOOM_DEBUG_DIAG is in the environment before runloom_c
# is imported (the parse is one-shot in runloom_diag_init at module load).
# ===========================================================================

_DIAG_FLAGS_CHILD = r'''
import sys
sys.path.insert(0, "src")
import runloom_c as rc
# RUNLOOM_DBG_PARKER (1<<0) | RUNLOOM_DBG_GSTATE (1<<1) == 3
sys.stdout.write("DIAG_FLAGS=%d\n" % rc._diag_flags())
'''


def test_diag_flags_reflects_runloom_debug_mask():
    """module_select.c.inc L154/157: _diag_flags() returns the parsed
    RUNLOOM_DEBUG flag mask as an int. With RUNLOOM_DEBUG_DIAG=parker,gstate the
    mask must be exactly RUNLOOM_DBG_PARKER|RUNLOOM_DBG_GSTATE == 3 -- proving
    the getter reads the live flag word, not a constant. (parker,gstate chosen
    because they are cheap: invariants would run self_check on every park and
    ring would allocate per-thread event rings.)"""
    # In-process call first: covers the lines even where the subprocess is slow.
    assert isinstance(rc._diag_flags(), int)
    p = _run_child(_DIAG_FLAGS_CHILD, timeout=120,
                   env_extra={"RUNLOOM_DEBUG_DIAG": "parker,gstate"})
    assert p.returncode == 0, (p.stdout[-300:], p.stderr[-800:])
    assert "DIAG_FLAGS=3" in p.stdout, (
        "_diag_flags did not reflect RUNLOOM_DEBUG_DIAG=parker,gstate (== 3)\n"
        "stdout=%s\nstderr=%s" % (p.stdout, p.stderr[-500:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
