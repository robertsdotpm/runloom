"""Windows netpoll syscall fault-injection harness.

The Linux campaign (test_netpoll_faultinject.py) forces epoll_wait / epoll_ctl
to fail with chosen errnos via strace's `-e inject=`.  Windows has no strace, so
the Windows netpoll backends carry compiled-in, env-gated injection points
(PYGO_FAULT_<SITE> = "<mode>:<wsa_code>", mode in {once, always}; see
netpoll.c).  Same questions a model checker can't answer -- these are kernel
error contracts, not memory-model races:

  * a transient poll error (once) must be tolerated -- the workload still
    completes;
  * a PERSISTENT poll error (always) must NOT busy-spin -- the pump backs off
    (1 ms), so the injection rate is bounded by wall-clock, not by the CPU.
    This is the Windows analogue of the Linux EBADF n<0 busy-spin guard, which
    the Windows wsapoll/select paths previously LACKED (added alongside this);
  * an AFD-poll SUBMIT failure must surface cleanly to the parked goroutine --
    no crash, no stranded goroutine.

Each backend (wsapoll / select / iocp-afd) is forced via PYGO_NETPOLL; the
workload (netpoll_inproc_fault_workload.py) parks a goroutine on a socket so the
fault hits a live pump and the deadline still wakes it.  Windows-only.
"""
import os
import re
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Windows netpoll fault injection is Windows-only")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORKLOAD = os.path.join(HERE, "netpoll_inproc_fault_workload.py")

WSAEINTR = 10004
WSAENOTSOCK = 10038

TIMEOUT_MS = 800
# 1 ms backoff over TIMEOUT_MS => ~TIMEOUT_MS injections; allow 6x slack.  A
# busy-spin (no backoff) issues many times this and scales UP with CPU speed.
MAX_FAULTS = TIMEOUT_MS * 6


def _run(backend, site, fault_spec, timeout=40):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    env["PYTHON_GIL"] = "0"
    if backend:
        env["PYGO_NETPOLL"] = backend
    else:
        env.pop("PYGO_NETPOLL", None)            # default backend (iocp-afd)
    env["FAULT_SITE"] = site
    env["FAULT_TIMEOUT_MS"] = str(TIMEOUT_MS)
    env["PYGO_FAULT_" + site] = fault_spec
    return subprocess.run(
        [sys.executable, WORKLOAD], cwd=REPO, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _field(out, key):
    m = re.search(r"^%s=(.*)$" % key, out, re.MULTILINE)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Persistent poll error must back off, not busy-spin (wsapoll / select).
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("backend,site",
                         [("wsapoll", "WSAPOLL"), ("select", "SELECT")])
def test_persistent_poll_error_backs_off(backend, site):
    p = _run(backend, site, "always:%d" % WSAENOTSOCK)
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, p.stdout
    assert _field(p.stdout, "BACKEND") == backend, \
        "backend not forced: %s" % p.stdout
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "fault never fired -- injection not wired:\n%s" % p.stdout
    assert faults <= MAX_FAULTS, (
        "poll busy-spin: %d injections in %d ms (ceiling %d) -- the Windows "
        "wait-failed backoff is not active\n%s" % (
            faults, TIMEOUT_MS, MAX_FAULTS, p.stdout))


# ---------------------------------------------------------------------------
# Transient poll error (fires once) is tolerated; the workload completes.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("backend,site",
                         [("wsapoll", "WSAPOLL"), ("select", "SELECT")])
def test_transient_poll_error_tolerated(backend, site):
    p = _run(backend, site, "once:%d" % WSAEINTR)
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, p.stdout
    assert int(_field(p.stdout, "FAULTS")) == 1, p.stdout


# ---------------------------------------------------------------------------
# IOCP-AFD: a SUBMIT failure must surface cleanly to the goroutine (no crash,
# no stranded goroutine -- the parker's wait_fd returns/raises and the run
# completes).
# ---------------------------------------------------------------------------
def test_iocp_submit_failure_surfaces_clean():
    p = _run("", "IOCP_SUBMIT", "once:%d" % WSAENOTSOCK)
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, p.stdout
    assert _field(p.stdout, "BACKEND") == "iocp-afd", p.stdout
    assert int(_field(p.stdout, "FAULTS")) == 1, p.stdout
    # The goroutine recorded SOME outcome (didn't hang): an OSError or a value.
    assert _field(p.stdout, "RESULT") not in (None, "[]"), p.stdout


# ---------------------------------------------------------------------------
# IOCP-AFD: a persistent completion-wait error must also back off.
# ---------------------------------------------------------------------------
def test_iocp_wait_error_backs_off():
    p = _run("", "IOCP_WAIT", "always:%d" % WSAENOTSOCK)
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, p.stdout
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "fault never fired:\n%s" % p.stdout
    assert faults <= MAX_FAULTS, (
        "iocp completion-wait busy-spin: %d in %d ms (ceiling %d)\n%s" % (
            faults, TIMEOUT_MS, MAX_FAULTS, p.stdout))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
