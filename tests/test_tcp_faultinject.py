"""Syscall fault-injection harness for runloom_tcp (TCPConn).

strace's ``-e inject=`` forces connect/accept/recv/send to fail with chosen
errnos under a real loopback TCPConn workload (tests/tcp_fault_workload.py),
asserting the non-blocking + netpoll-retry loop's response.

Cases:
  connect EINTR  -- must be treated like EINPROGRESS (wait writable + SO_ERROR),
      NOT surfaced.  Regression test for the connect-path EINTR fix; before it,
      a signal on connect() spuriously failed with OSError(EINTR).
  recv  EINTR    -- retried (already handled); echo still round-trips.
  send  EINTR    -- retried; echo still round-trips.
  accept EINTR   -- retried; the server still accepts.
  recv  ECONNRESET -- a non-retryable error must surface as a clean OSError,
      never crash or hang (same code path as EPIPE on send).

On Linux, glibc recv()->recvfrom, send()->sendto, accept()->accept4.

Skipped unless Linux + a strace that supports -e inject=.
"""
import os
import shutil
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKLOAD = os.path.join(HERE, "tcp_fault_workload.py")


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


pytestmark = [
    pytest.mark.skipif(not sys.platform.startswith("linux"),
                       reason="strace fault injection is Linux-only"),
    pytest.mark.skipif(not _strace_supports_inject(),
                       reason="strace with -e inject= not available"),
]

STRACE = shutil.which("strace")


def _run(inject, mode, timeout=30):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    cmd = [STRACE, "-f", "-e", "signal=none", "-e", "inject=" + inject,
           sys.executable, WORKLOAD, mode]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=env, timeout=timeout)
    return p.returncode, p.stdout.decode(errors="replace"), \
        p.stderr.decode(errors="replace")


def test_connect_eintr_is_not_fatal():
    """A signal on connect() must not fail the connection -- it continues
    asynchronously and we wait for writability + SO_ERROR."""
    for when in ("1", "1..3"):
        rc, out, err = _run("connect:error=EINTR:when=" + when, "connectonly")
        assert rc == 0, "connect EINTR should not be fatal (when=%s): %s\n%s" % (
            when, out, err)
        assert "OK connect" in out, "connect did not complete after EINTR:\n%s" % out


def test_recv_eintr_is_retried():
    rc, out, err = _run("recvfrom:error=EINTR:when=1..6", "echo")
    assert rc == 0 and "OK ping" in out, "recv EINTR broke echo: %s\n%s" % (out, err)


def test_send_eintr_is_retried():
    rc, out, err = _run("sendto:error=EINTR:when=1..6", "echo")
    assert rc == 0 and "OK ping" in out, "send EINTR broke echo: %s\n%s" % (out, err)


def test_accept_eintr_is_retried():
    rc, out, err = _run("accept4:error=EINTR:when=1..3", "echo")
    assert rc == 0 and "OK ping" in out, "accept EINTR broke echo: %s\n%s" % (out, err)


def test_recv_econnreset_surfaces_as_oserror():
    """A non-retryable recv error (ECONNRESET) must surface as a clean OSError
    -- the same path that carries EPIPE/ECONNREFUSED."""
    rc, out, err = _run("recvfrom:error=ECONNRESET:when=1", "recvonce")
    assert rc == 42, "ECONNRESET should surface as OSError: rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "errno=104" in out, "ECONNRESET not surfaced cleanly:\n%s" % out
