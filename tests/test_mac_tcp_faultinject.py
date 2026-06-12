"""Compiled-in TCP socket-surface fault injection (kqueue backends: macOS/BSD).

The Linux campaign faults connect/accept/recv/send with strace's ``-e inject=``
(test_tcp_faultinject.py).  Darwin/BSD have no syscall-injecting tracer, so the
socket surface carries the same compiled-in, env-gated fault points as the
netpoll pump -- RUNLOOM_FAULT_TCP_<CALL>="<mode>:<errno>" (see runloom_tcp.c).  This
harness drives a real loopback TCPConn workload (tcp_fault_workload.py) and
asserts the non-blocking + netpoll-retry loop handles every errno the Darwin
socket(2)/connect(2)/accept(2)/recv(2)/send(2) man pages permit:

  socket  EMFILE/ENFILE/ENOMEM  -> clean OSError at creation, no crash.
  connect EINTR                 -> modelled like EINPROGRESS (park WRITE +
        SO_ERROR), NOT surfaced -- POSIX says an interrupted connect continues.
  connect ECONNREFUSED          -> clean OSError.
  accept  EINTR                 -> retried; the server still accepts.
  accept  ECONNABORTED          -> retried (peer reset between SYN and accept;
        Go netpoll + libuv both retry).  Regression test for the accept fix.
  recv    EINTR                 -> retried; the echo round-trips.
  recv    ECONNRESET            -> non-retryable: clean OSError.
  send    EINTR                 -> retried; the echo round-trips.

The injection points are inert unless armed, and skip cleanly off the kqueue
backend.  no-gil only (PYTHON_GIL=0).
"""
import os
import re
import subprocess
import sys

import pytest

import runloom_c

pytestmark = [
    pytest.mark.skipif(
        not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
        reason="compiled-in TCP fault injection targets the kqueue backends"),
    pytest.mark.skipif(runloom_c.netpoll_backend() != "kqueue",
                       reason="needs the kqueue backend"),
]

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORKLOAD = os.path.join(HERE, "tcp_fault_workload.py")

# Darwin/BSD errno values (identical across macOS + the BSDs for these).
EINTR, ENOMEM = 4, 12
ENFILE, EMFILE = 23, 24
EPIPE = 32
EADDRNOTAVAIL, ENETUNREACH = 49, 51
ECONNABORTED, ECONNRESET, ENOBUFS = 53, 54, 55
ENOTCONN, ETIMEDOUT, ECONNREFUSED = 57, 60, 61


def _run(site, spec, mode, timeout=30):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    env["PYTHON_GIL"] = "0"                       # focus: free-threaded only
    env["FAULT_SITE"] = site
    env["RUNLOOM_FAULT_" + site] = spec
    return subprocess.run(
        [sys.executable, WORKLOAD, mode], cwd=REPO, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _faults(out):
    m = re.search(r"^FAULTS=(\d+)$", out, re.MULTILINE)
    return int(m.group(1)) if m else None


# ---- retryable errnos: the operation still completes ------------------------

def test_recv_eintr_is_retried():
    p = _run("TCP_RECV", "once:%d" % EINTR, "echo")
    assert p.returncode == 0 and "OK ping" in p.stdout, (p.stdout, p.stderr)
    assert _faults(p.stdout) == 1, p.stdout


def test_send_eintr_is_retried():
    p = _run("TCP_SEND", "once:%d" % EINTR, "echo")
    assert p.returncode == 0 and "OK ping" in p.stdout, (p.stdout, p.stderr)
    assert _faults(p.stdout) == 1, p.stdout


def test_accept_eintr_is_retried():
    p = _run("TCP_ACCEPT", "once:%d" % EINTR, "echo")
    assert p.returncode == 0 and "OK ping" in p.stdout, (p.stdout, p.stderr)
    assert _faults(p.stdout) == 1, p.stdout


def test_accept_econnaborted_is_retried():
    """ECONNABORTED (peer reset between SYN and accept) must be retried, not
    surfaced as a fatal OSError -- matches Go netpoll + libuv.  Regression test
    for the accept-loop ECONNABORTED fix."""
    p = _run("TCP_ACCEPT", "once:%d" % ECONNABORTED, "echo")
    assert p.returncode == 0 and "OK ping" in p.stdout, (
        "ECONNABORTED not retried on accept:\n%s\n%s" % (p.stdout, p.stderr))
    assert _faults(p.stdout) == 1, p.stdout


def test_connect_eintr_is_not_fatal():
    """A signal on an in-flight connect() must not fail the connection -- it
    continues asynchronously (park WRITE + SO_ERROR)."""
    p = _run("TCP_CONNECT", "once:%d" % EINTR, "connectonly")
    assert p.returncode == 0 and "OK connect" in p.stdout, (p.stdout, p.stderr)
    assert _faults(p.stdout) == 1, p.stdout


# ---- non-retryable errnos: a clean OSError, never a crash or hang -----------

def test_recv_econnreset_surfaces_oserror():
    p = _run("TCP_RECV", "once:%d" % ECONNRESET, "recvonce")
    assert p.returncode == 42, (p.stdout, p.stderr)
    assert "errno=%d" % ECONNRESET in p.stdout, p.stdout


def test_connect_econnrefused_surfaces_oserror():
    p = _run("TCP_CONNECT", "once:%d" % ECONNREFUSED, "connectonly")
    assert p.returncode == 42, (p.stdout, p.stderr)
    assert "errno=%d" % ECONNREFUSED in p.stdout, p.stdout


@pytest.mark.parametrize("errno_", [EMFILE, ENFILE, ENOMEM],
                         ids=["EMFILE", "ENFILE", "ENOMEM"])
def test_socket_creation_failure_surfaces_oserror(errno_):
    p = _run("TCP_SOCKET", "once:%d" % errno_, "connectonly")
    assert p.returncode == 42, (errno_, p.stdout, p.stderr)
    assert "errno=%d" % errno_ in p.stdout, p.stdout


# ---- errno breadth: every other non-retryable return -> a clean OSError -----

@pytest.mark.parametrize("errno_", [EPIPE, ENOBUFS], ids=["EPIPE", "ENOBUFS"])
def test_send_hard_error_surfaces_oserror(errno_):
    """A non-retryable send error (peer gone / buffers exhausted) surfaces as a
    clean OSError -- matches Go's netFD.Write (neither retries these)."""
    p = _run("TCP_SEND", "once:%d" % errno_, "sendonce")
    assert p.returncode == 42, (errno_, p.stdout, p.stderr)
    assert "errno=%d" % errno_ in p.stdout, p.stdout


@pytest.mark.parametrize("errno_", [EADDRNOTAVAIL, ENETUNREACH, ETIMEDOUT],
                         ids=["EADDRNOTAVAIL", "ENETUNREACH", "ETIMEDOUT"])
def test_connect_hard_error_surfaces_oserror(errno_):
    p = _run("TCP_CONNECT", "once:%d" % errno_, "connectonly")
    assert p.returncode == 42, (errno_, p.stdout, p.stderr)
    assert "errno=%d" % errno_ in p.stdout, p.stdout


@pytest.mark.parametrize("errno_", [ENOTCONN, ETIMEDOUT],
                         ids=["ENOTCONN", "ETIMEDOUT"])
def test_recv_hard_error_surfaces_oserror(errno_):
    p = _run("TCP_RECV", "once:%d" % errno_, "recvonce")
    assert p.returncode == 42, (errno_, p.stdout, p.stderr)
    assert "errno=%d" % errno_ in p.stdout, p.stdout


@pytest.mark.parametrize("errno_", [EMFILE, ENFILE], ids=["EMFILE", "ENFILE"])
def test_accept_hard_error_surfaces_oserror(errno_):
    """accept() fd-exhaustion (EMFILE/ENFILE) surfaces as a clean OSError to the
    accepting fiber -- the listener is unharmed, no crash, no hang."""
    p = _run("TCP_ACCEPT", "once:%d" % errno_, "acceptfail")
    assert p.returncode == 42, (errno_, p.stdout, p.stderr)
    assert "errno=%d" % errno_ in p.stdout, p.stdout


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
