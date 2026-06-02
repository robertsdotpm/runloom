"""kqueue (FreeBSD/macOS) netpoll syscall fault-injection harness.

The Linux campaign forces epoll_wait to fail via strace -e inject=; FreeBSD has
no syscall-injecting tracer (truss/ktrace trace but can't inject), so the kqueue
backend carries the same compiled-in, env-gated injection point as the Windows
backends (PYGO_FAULT_KQUEUE_WAIT="<mode>:<errno>"; see netpoll.c).  Same
questions:

  * a transient kevent() error (once) is tolerated -- the workload still
    completes;
  * a PERSISTENT kevent() error (always) must NOT busy-spin -- the kqueue pump
    already routes n<0 through pygo_netpoll_wait_failed (EINTR retry; else 1 ms
    backoff), so the injection rate is bounded by wall-clock, not the CPU.

Reuses netpoll_inproc_fault_workload.py (the same parks-on-a-socket-with-a-
deadline workload as the Windows harness; it is backend-neutral).  kqueue-only.
"""
import os
import re
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("freebsd", "darwin", "openbsd", "netbsd")),
    reason="kqueue fault injection is for the kqueue backends (BSD/macOS)")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORKLOAD = os.path.join(HERE, "netpoll_inproc_fault_workload.py")

EINTR = 4
EBADF = 9

TIMEOUT_MS = 800
MAX_FAULTS = TIMEOUT_MS * 6        # 1 ms backoff => ~TIMEOUT_MS; 6x slack


def _run(fault_spec, timeout=40):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    env["FAULT_SITE"] = "KQUEUE_WAIT"
    env["FAULT_TIMEOUT_MS"] = str(TIMEOUT_MS)
    env["PYGO_FAULT_KQUEUE_WAIT"] = fault_spec
    return subprocess.run(
        [sys.executable, WORKLOAD], cwd=REPO, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _field(out, key):
    m = re.search(r"^%s=(.*)$" % key, out, re.MULTILINE)
    return m.group(1) if m else None


def test_persistent_kevent_error_backs_off():
    """A persistent kevent() error (EBADF: the kqueue fd closed under us, a
    teardown race) must back off, not busy-spin: the parked goroutine still
    wakes via its deadline and the injection count is bounded by the 1 ms
    backoff in pygo_netpoll_wait_failed."""
    p = _run("always:%d" % EBADF)
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, p.stdout
    assert _field(p.stdout, "BACKEND") == "kqueue", p.stdout
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "fault never fired -- injection not wired:\n%s" % p.stdout
    assert faults <= MAX_FAULTS, (
        "kevent busy-spin: %d injections in %d ms (ceiling %d) -- the n<0 "
        "backoff is not active\n%s" % (faults, TIMEOUT_MS, MAX_FAULTS, p.stdout))


def test_transient_kevent_error_tolerated():
    """A signal-interrupted kevent() (EINTR, fires once) must be retried; the
    parked goroutine still wakes and the workload completes."""
    p = _run("once:%d" % EINTR)
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, p.stdout
    assert int(_field(p.stdout, "FAULTS")) == 1, p.stdout


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
