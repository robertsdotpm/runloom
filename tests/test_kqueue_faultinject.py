"""kqueue (FreeBSD/macOS) syscall fault-injection matrix.

Darwin/BSD have no syscall-injecting tracer (dtruss/ktrace observe but cannot
inject; DYLD_INSERT_LIBRARIES is SIP-fragile), so the kqueue backend carries
compiled-in, env-gated fault points -- RUNLOOM_FAULT_<SITE>="<mode>:<errno>",
mode in {once, always}; see netpoll.c.  This harness drives every kqueue
syscall runloom issues and asserts the runtime handles each errno the Darwin
kevent(2)/kqueue(2) man pages permit:

  KQUEUE_CREATE -- kqueue() at netpoll init.  A hard init failure (ENOMEM /
      EMFILE / ENFILE) must surface as a clean OSError to the first parked
      goroutine -- never crash, never hang the scheduler.
  KQUEUE_CTL    -- the registration kevent() (EV_ADD a watched fd).  A
      failure (ENOMEM / EINVAL / EACCES / EBADF) must surface as a clean
      OSError to the goroutine that asked to park on the fd; the fd-bit is
      rolled back, so nothing is left half-registered.
  KQUEUE_WAIT   -- the event-loop kevent().  EINTR is transient (retried;
      the wake still arrives).  EVERY hard error (EBADF teardown race,
      EINVAL/ENOMEM/EFAULT/EACCES/ENOENT) must BACK OFF, not busy-spin --
      the parked goroutine still wakes via its deadline and the injected
      count is bounded by the 1 ms backoff in runloom_netpoll_wait_failed,
      not the CPU.

The workload (netpoll_inproc_fault_workload.py) parks a goroutine on a
never-readable UDP socket with a deadline, so init + register + wait all run
on a live path; it prints BACKEND / RESULT / FAULTS / DONE.  no-gil only.
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

# Darwin/BSD errno values (stable across macOS + the BSDs for these).
ENOENT, ESRCH, EINTR, EBADF, ENOMEM = 2, 3, 4, 9, 12
EACCES, EFAULT, EINVAL, ENFILE, EMFILE = 13, 14, 22, 23, 24

TIMEOUT_MS = 800
# 1 ms backoff across the deadline window => ~TIMEOUT_MS calls; allow 6x slack.
# A busy-spin (no backoff) issues orders of magnitude more and scales with CPU.
MAX_FAULTS = TIMEOUT_MS * 6


def _run(site, spec, timeout=40):
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    env["PYTHON_GIL"] = "0"                       # focus: free-threaded only
    env["FAULT_SITE"] = site
    env["FAULT_TIMEOUT_MS"] = str(TIMEOUT_MS)
    env["RUNLOOM_FAULT_" + site] = spec
    return subprocess.run(
        [sys.executable, WORKLOAD], cwd=REPO, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _field(out, key):
    m = re.search(r"^%s=(.*)$" % key, out, re.MULTILINE)
    return m.group(1) if m else None


def _assert_terminated(p):
    """The workload must always run to a clean shutdown -- a fault on any
    kqueue syscall is a recoverable error, never a crash or a hang."""
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, "workload did not finish:\n%s\n%s" % (p.stdout, p.stderr)
    assert _field(p.stdout, "BACKEND") == "kqueue", "not on kqueue:\n%s" % p.stdout


# ---- KQUEUE_WAIT: transient retry + universal backoff -----------------------

def test_wait_eintr_is_retried():
    """A signal-interrupted kevent() (EINTR, once) is retried transparently;
    the parked goroutine still wakes on its deadline."""
    p = _run("KQUEUE_WAIT", "once:%d" % EINTR)
    _assert_terminated(p)
    assert int(_field(p.stdout, "FAULTS")) == 1, p.stdout


@pytest.mark.parametrize(
    "errno_", [EBADF, EINVAL, ENOMEM, EFAULT, EACCES, ENOENT],
    ids=["EBADF", "EINVAL", "ENOMEM", "EFAULT", "EACCES", "ENOENT"])
def test_wait_persistent_error_backs_off(errno_):
    """Every persistent kevent() wait error must back off, not busy-spin: the
    deadline wake still fires and the injection count is bounded by the 1 ms
    backoff (not the CPU)."""
    p = _run("KQUEUE_WAIT", "always:%d" % errno_)
    _assert_terminated(p)
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "fault never fired -- injection not wired:\n%s" % p.stdout
    assert faults <= MAX_FAULTS, (
        "kevent busy-spin: %d injections in %d ms (ceiling %d) for errno %d\n%s"
        % (faults, TIMEOUT_MS, MAX_FAULTS, errno_, p.stdout))


# ---- KQUEUE_CTL: registration failure surfaces as a clean OSError -----------

@pytest.mark.parametrize(
    "errno_", [ENOMEM, EINVAL, EACCES, EBADF],
    ids=["ENOMEM", "EINVAL", "EACCES", "EBADF"])
def test_register_error_surfaces_as_oserror(errno_):
    """A failing registration kevent() (EV_ADD on the watched fd) must surface
    as a clean OSError(errno) to the goroutine that parked on the fd -- never a
    crash and never a silently-stranded goroutine."""
    p = _run("KQUEUE_CTL", "always:%d" % errno_)
    _assert_terminated(p)
    assert int(_field(p.stdout, "FAULTS")) > 0, "CTL fault never fired:\n%s" % p.stdout
    assert ("'oserror', %d" % errno_) in (_field(p.stdout, "RESULT") or ""), \
        "register errno %d not surfaced as OSError:\n%s" % (errno_, p.stdout)


# ---- KQUEUE_CREATE: init failure surfaces cleanly ---------------------------

@pytest.mark.parametrize(
    "errno_", [ENOMEM, EMFILE, ENFILE],
    ids=["ENOMEM", "EMFILE", "ENFILE"])
def test_kqueue_create_failure_surfaces_cleanly(errno_):
    """kqueue() failing at netpoll init must surface as a clean OSError to the
    first parked goroutine and let the scheduler unwind -- not crash, not hang
    (the subprocess timeout in _run catches a hang)."""
    p = _run("KQUEUE_CREATE", "once:%d" % errno_)
    _assert_terminated(p)
    assert int(_field(p.stdout, "FAULTS")) == 1, "CREATE fault never fired:\n%s" % p.stdout
    assert ("'oserror', %d" % errno_) in (_field(p.stdout, "RESULT") or ""), \
        "kqueue() errno %d not surfaced as OSError:\n%s" % (errno_, p.stdout)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
