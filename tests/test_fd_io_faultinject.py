"""Compiled-in fault injection for runloom_c.fd_read / fd_write (module.c).

The cooperative fd I/O loop (POSIX read(2)/write(2) with netpoll parking) is
faulted in-process via RUNLOOM_FAULT_FD_READ / RUNLOOM_FAULT_FD_WRITE (see module.c +
netpoll.c) -- uniform across kqueue/epoll/Windows, no tracer needed.  Uses the
errno module so it is correct on every platform.  Asserts:

  EINTR        -> retried (continue); the pipe round-trips.
  EAGAIN       -> parks on the fd, then retries; round-trips.
  EIO / EBADF  on read  -> a clean OSError, never a crash or hang.
  EPIPE/EIO/EBADF on write -> a clean OSError.

no-gil only.  POSIX only (on Windows fd_read/write block the OS thread -- there
is no cooperative retry loop to fault).
"""
import errno as E
import os
import re
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="fd_read/fd_write block the OS thread on Windows; no loop to fault")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORKLOAD = os.path.join(HERE, "fd_io_fault_workload.py")


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


# ---- retryable: the read/write loop recovers and the pipe round-trips -------

@pytest.mark.parametrize("err", [E.EINTR, E.EAGAIN], ids=["EINTR", "EAGAIN"])
def test_read_retryable_round_trips(err):
    p = _run("FD_READ", "once:%d" % err, "echo")
    assert p.returncode == 0 and "OK ping" in p.stdout, (p.stdout, p.stderr)
    assert _faults(p.stdout) == 1, p.stdout


@pytest.mark.parametrize("err", [E.EINTR, E.EAGAIN], ids=["EINTR", "EAGAIN"])
def test_write_retryable_round_trips(err):
    p = _run("FD_WRITE", "once:%d" % err, "echo")
    assert p.returncode == 0 and "OK ping" in p.stdout, (p.stdout, p.stderr)
    assert _faults(p.stdout) == 1, p.stdout


# ---- non-retryable: a clean OSError, never a crash or a stranded fiber ---

@pytest.mark.parametrize("err", [E.EIO, E.EBADF], ids=["EIO", "EBADF"])
def test_read_hard_error_surfaces_oserror(err):
    p = _run("FD_READ", "once:%d" % err, "readfail")
    assert p.returncode == 42, (err, p.stdout, p.stderr)
    assert "errno=%d" % err in p.stdout, p.stdout


@pytest.mark.parametrize("err", [E.EPIPE, E.EIO, E.EBADF], ids=["EPIPE", "EIO", "EBADF"])
def test_write_hard_error_surfaces_oserror(err):
    p = _run("FD_WRITE", "once:%d" % err, "writefail")
    assert p.returncode == 42, (err, p.stdout, p.stderr)
    assert "errno=%d" % err in p.stdout, p.stdout


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
