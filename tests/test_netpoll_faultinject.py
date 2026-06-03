"""Syscall fault-injection harness for the netpoll backend.

Answers "do we handle every state the low-level functions can return?" -- the
question a model checker cannot, because these are kernel/libc error contracts,
not memory-model races.  We use strace's ``-e inject=`` to force the real
epoll_wait / epoll_ctl syscalls to fail with chosen errnos at chosen times, and
assert the runtime's response, running a real park/wake workload underneath
(tests/netpoll_fault_workload.py) so the injected error hits a live code path.

Cases (each a separate test so a failure is attributable):

  EINTR on epoll_wait (transient)   -- must be retried; the wake still arrives.
  EBADF on epoll_wait (persistent)  -- the impossible-but-real teardown race.
      Must NOT busy-spin: the pump backs off, so epoll_wait is called at a
      rate bounded by the backoff (~1/ms) rather than as fast as the CPU
      allows.  This is the regression test for the n<0 busy-spin fix; with the
      backoff removed the same workload issued ~14x more epoll_wait calls.
  ENOMEM on epoll_ctl               -- register fails; surfaces as a clean
      OSError(errno=ENOMEM) to the parked goroutine, never a crash or hang.
  EINVAL on epoll_ctl               -- likewise a clean OSError(errno=EINVAL).

Skipped unless: Linux + epoll backend + a strace that supports -e inject=.
"""
import os
import re
import shutil
import subprocess
import sys

import pytest

import runloom_c

HERE = os.path.dirname(os.path.abspath(__file__))
WORKLOAD = os.path.join(HERE, "netpoll_fault_workload.py")

# epoll_wait may be routed to epoll_pwait/epoll_pwait2 by glibc depending on
# version/arch; inject into and count all of them so the harness is portable.
WAIT_SYSCALLS = "epoll_wait,epoll_pwait,epoll_pwait2"

EBADF_TIMEOUT_MS = 1500
# 1 ms backoff over a TIMEOUT_MS window => ~TIMEOUT_MS calls; allow 4x slack.
# A busy-spin (no backoff) issues many times this even under strace's ptrace
# overhead, and scales UP with CPU speed while the backoff count does not.
EBADF_MAX_WAIT_CALLS = EBADF_TIMEOUT_MS * 4


def _strace_supports_inject():
    strace = shutil.which("strace")
    if not strace:
        return False
    try:
        # Feature-probe: a well-formed inject spec on a harmless run.  Target a
        # syscall `true` never makes (epoll_wait) so nothing is actually
        # injected -- we only check strace ACCEPTS the -e inject= syntax (added
        # in 4.15).  Old strace prints "invalid"/usage and exits non-zero.
        p = subprocess.run(
            [strace, "-e", "inject=epoll_wait:error=EBADF:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not sys.platform.startswith("linux"),
                       reason="strace fault injection is Linux-only"),
    pytest.mark.skipif(runloom_c.netpoll_backend() != "epoll",
                       reason="this harness targets the epoll backend"),
    pytest.mark.skipif(not _strace_supports_inject(),
                       reason="strace with -e inject= not available"),
]

STRACE = shutil.which("strace")


def _run_under_strace(inject, mode, extra=(), env_extra=None, timeout=30):
    """Run the workload under strace with one inject spec.  Returns
    (returncode, stdout_text, stderr_text)."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    if env_extra:
        env.update(env_extra)
    cmd = [STRACE, "-f", "-e", "signal=none",
           "-e", "inject=" + inject, *extra,
           sys.executable, WORKLOAD, mode]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=env, timeout=timeout)
    return p.returncode, p.stdout.decode(errors="replace"), \
        p.stderr.decode(errors="replace")


def _count_calls(strace_c_summary, *names):
    """Sum the 'calls' column for the named syscalls in a `strace -c` table."""
    total = 0
    for name in names:
        m = re.search(
            r"^\s*[\d.]+\s+[\d.]+\s+\d+\s+(\d+)\s+(?:\d+\s+)?" + re.escape(name)
            + r"\s*$", strace_c_summary, re.MULTILINE)
        if m:
            total += int(m.group(1))
    return total


def test_epoll_wait_eintr_is_retried():
    """A signal-interrupted epoll_wait (EINTR) must be transparently retried --
    the parked goroutine still wakes on the real edge."""
    rc, out, err = _run_under_strace(
        WAIT_SYSCALLS + ":error=EINTR:when=1..3", "happy")
    assert rc == 0, "EINTR should be retried, not fatal: rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "WOKE" in out, "did not wake after EINTR retries:\n%s" % out


def test_epoll_wait_ebadf_does_not_busy_spin():
    """A persistent epoll_wait error (EBADF: poll fd closed under us) must not
    turn the idle pump into a busy-spin.  The pump backs off, so the syscall
    rate is bounded by the backoff -- not by the CPU.  Regression test for the
    n<0 busy-spin fix."""
    rc, out, err = _run_under_strace(
        WAIT_SYSCALLS + ":error=EBADF:when=1+", "timeout",
        extra=["-c"],   # summary mode: count syscalls
        env_extra={"RUNLOOM_FAULT_TIMEOUT_MS": str(EBADF_TIMEOUT_MS)},
        timeout=30)
    # strace -c writes its summary to stderr; the workload's stdout still shows.
    assert rc == 0, "workload should still terminate cleanly: rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "DONE" in out, "workload did not reach clean timeout:\n%s" % out
    calls = _count_calls(err, "epoll_wait", "epoll_pwait", "epoll_pwait2")
    assert calls > 0, "no epoll_wait calls counted -- parse/setup error:\n%s" % err
    assert calls <= EBADF_MAX_WAIT_CALLS, (
        "epoll_wait busy-spin: %d calls in %d ms (ceiling %d) -- the n<0 "
        "backoff is not active\n%s" % (
            calls, EBADF_TIMEOUT_MS, EBADF_MAX_WAIT_CALLS, err))


def test_epoll_ctl_enomem_surfaces_as_oserror():
    """epoll_ctl ENOMEM at register time must surface as a clean OSError to the
    parked goroutine -- not crash, not silently strand the goroutine."""
    rc, out, err = _run_under_strace(
        "epoll_ctl:error=ENOMEM:when=1+", "happy")
    assert rc == 42, "expected clean OSERROR exit(42): rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "OSERROR errno=12" in out, "ENOMEM not surfaced cleanly:\n%s" % out


def test_epoll_ctl_einval_surfaces_as_oserror():
    """epoll_ctl EINVAL at register time must likewise surface as a clean
    OSError, not a crash."""
    rc, out, err = _run_under_strace(
        "epoll_ctl:error=EINVAL:when=1+", "happy")
    assert rc == 42, "expected clean OSERROR exit(42): rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "OSERROR errno=22" in out, "EINVAL not surfaced cleanly:\n%s" % out
