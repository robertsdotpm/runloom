"""Fault injection for the POSIX select() netpoll fallback.

select() is a BUILD-TIME backend on POSIX (RUNLOOM_NETPOLL=select -> -DRUNLOOM_FORCE_
SELECT suppresses epoll/kqueue), so this harness builds a select-forced
extension into a temp dir once, then drives netpoll_inproc_fault_workload.py
against it with RUNLOOM_FAULT_SELECT armed (the same compiled-in mechanism the
kqueue/Windows pumps use).  Asserts the POSIX select pump:

  EINTR (once)       -> retried; the parked goroutine still wakes.
  EBADF (persistent) -> BACKS OFF, not a busy-spin.  Regression test for the
      missing select-path backoff: runloom_netpoll_wait_failed was gated to
      epoll/kqueue and never compiled on a select build, so a persistent
      select() error (a parked fd closed under us) pegged a CPU.

no-gil only; POSIX only (the Windows select fallback is covered by
test_win_netpoll_faultinject.py).
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="POSIX select fallback; Windows select is in test_win_netpoll_faultinject")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
WORKLOAD = os.path.join(HERE, "netpoll_inproc_fault_workload.py")

EINTR, EBADF = 4, 9
TIMEOUT_MS = 800
# 1 ms backoff across the deadline window => ~TIMEOUT_MS calls; 6x slack.
MAX_FAULTS = TIMEOUT_MS * 6


@pytest.fixture(scope="module")
def select_build():
    """Build a RUNLOOM_NETPOLL=select extension into a temp dir (a FRESH build-temp
    so the -DRUNLOOM_FORCE_SELECT define actually takes).  Skips if it won't build
    (e.g. a toolchain that can't be invoked from the test env)."""
    lib = tempfile.mkdtemp(prefix="runloom_sel_lib_")
    tmp = tempfile.mkdtemp(prefix="runloom_sel_tmp_")
    env = dict(os.environ)
    env["RUNLOOM_NETPOLL"] = "select"
    p = subprocess.run(
        [sys.executable, "setup.py", "build_ext", "--build-lib", lib,
         "--build-temp", tmp],
        cwd=REPO, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, timeout=600)
    if p.returncode != 0:
        shutil.rmtree(lib, ignore_errors=True)
        shutil.rmtree(tmp, ignore_errors=True)
        pytest.skip("could not build select-forced extension:\n" + p.stdout[-1500:])
    yield lib
    shutil.rmtree(lib, ignore_errors=True)
    shutil.rmtree(tmp, ignore_errors=True)


def _run(select_build, spec, timeout=40):
    env = dict(os.environ)
    env["RUNLOOM_CORE_PATH"] = select_build           # workload imports runloom_c from here
    env["PYTHONPATH"] = os.path.join(REPO, "src")
    env["PYTHON_GIL"] = "0"                         # focus: free-threaded only
    env["FAULT_SITE"] = "SELECT"
    env["FAULT_TIMEOUT_MS"] = str(TIMEOUT_MS)
    if spec:
        env["RUNLOOM_FAULT_SELECT"] = spec
    return subprocess.run(
        [sys.executable, WORKLOAD], cwd=REPO, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _field(out, key):
    m = re.search(r"^%s=(.*)$" % key, out, re.MULTILINE)
    return m.group(1) if m else None


def test_select_backend_is_active(select_build):
    """Sanity: the temp build really runs the select backend."""
    p = _run(select_build, "")
    assert p.returncode == 0, (p.stdout, p.stderr)
    assert _field(p.stdout, "BACKEND") == "select", \
        "expected the select backend:\n%s" % p.stdout


def test_select_eintr_is_retried(select_build):
    p = _run(select_build, "once:%d" % EINTR)
    assert p.returncode == 0 and "DONE" in p.stdout, (p.stdout, p.stderr)
    assert _field(p.stdout, "BACKEND") == "select", p.stdout
    assert int(_field(p.stdout, "FAULTS")) == 1, p.stdout


def test_select_persistent_error_backs_off(select_build):
    """A persistent select() error must back off (bounded count), not busy-spin
    -- the regression test for the missing POSIX-select backoff."""
    p = _run(select_build, "always:%d" % EBADF)
    assert p.returncode == 0 and "DONE" in p.stdout, (p.stdout, p.stderr)
    assert _field(p.stdout, "BACKEND") == "select", p.stdout
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "fault never fired -- injection not wired:\n%s" % p.stdout
    assert faults <= MAX_FAULTS, (
        "select busy-spin: %d injections in %d ms (ceiling %d) -- the backoff "
        "is not active\n%s" % (faults, TIMEOUT_MS, MAX_FAULTS, p.stdout))


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
