"""Syscall fault-injection harness for the io_uring backend.

Same method as test_netpoll_faultinject.py: strace's ``-e inject=`` forces the
real io_uring syscalls to fail with chosen errnos and we assert the runtime's
response, with a real submit+completion workload underneath
(tests/iouring_fault_workload.py).

Cases:
  EINTR on io_uring_enter (submit) -- a signal during the (non-blocking) submit
      must be retried, not surfaced.  Regression test for the submit-path EINTR
      fix; before it, a single injected EINTR turned a clean file_read into a
      spurious OSError(EINTR).
  ENOSYS on io_uring_setup -- io_uring becomes unavailable; file_read must fall
      back to pread and still return the right bytes (graceful degradation).
  EAGAIN on io_uring_enter (submit) -- a genuine resource error must surface as
      a clean OSError, never a crash or hang.

Skipped unless Linux + io_uring available + strace with -e inject=.
"""
import os
import shutil
import subprocess
import sys

import pytest

import runloom_c

HERE = os.path.dirname(os.path.abspath(__file__))
WORKLOAD = os.path.join(HERE, "iouring_fault_workload.py")


def _strace_supports_inject():
    strace = shutil.which("strace")
    if not strace:
        return False
    try:
        p = subprocess.run(
            [strace, "-e", "inject=io_uring_enter:error=EINTR:when=1", "true"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=15)
        return p.returncode == 0 and b"invalid" not in p.stderr.lower()
    except Exception:
        return False


pytestmark = [
    pytest.mark.skipif(not sys.platform.startswith("linux"),
                       reason="strace fault injection is Linux-only"),
    pytest.mark.skipif(not runloom_c.iouring_available(),
                       reason="io_uring not available (need Linux >= 5.1)"),
    pytest.mark.skipif(not _strace_supports_inject(),
                       reason="strace with -e inject= not available"),
]

STRACE = shutil.which("strace")


def _run_under_strace(inject, mode, timeout=30):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    cmd = [STRACE, "-f", "-e", "signal=none", "-e", "inject=" + inject,
           sys.executable, WORKLOAD, mode]
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       env=env, timeout=timeout)
    return p.returncode, p.stdout.decode(errors="replace"), \
        p.stderr.decode(errors="replace")


def test_enter_eintr_on_submit_is_retried():
    """EINTR on the submit io_uring_enter must be retried, not surfaced as a
    spurious OSError.  (when=1 hits the submit; when=1..3 stresses repeats.)"""
    for when in ("1", "1..3"):
        rc, out, err = _run_under_strace(
            "io_uring_enter:error=EINTR:when=" + when, "fileread")
        assert rc == 0, (
            "EINTR on submit should be retried (when=%s): rc=%d\n%s\n%s" % (
                when, rc, out, err))
        assert "OK" in out, "file_read did not complete after EINTR retry:\n%s" % out


def test_setup_enosys_falls_back_to_pread():
    """io_uring_setup failing (ENOSYS) must leave file_read working via the
    pread fallback -- same bytes, no error."""
    rc, out, err = _run_under_strace(
        "io_uring_setup:error=ENOSYS:when=1+", "fileread")
    assert rc == 0, "setup failure should fall back cleanly: rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "OK" in out, "pread fallback did not return the bytes:\n%s" % out
    assert "IOURING_AVAIL=0" in err, (
        "expected io_uring to report unavailable after setup ENOSYS:\n%s" % err)


def test_enter_eagain_on_submit_surfaces_cleanly():
    """EAGAIN on the submit io_uring_enter is a genuine resource error; it must
    surface as a clean OSError (exit 42), never crash or hang.  (Documents
    current behaviour -- EAGAIN is not retried, unlike EINTR.)"""
    rc, out, err = _run_under_strace(
        "io_uring_enter:error=EAGAIN:when=1", "fileread")
    assert rc == 42, "EAGAIN should surface as a clean OSError: rc=%d\n%s\n%s" % (
        rc, out, err)
    assert "OSERROR errno=11" in out, "EAGAIN not surfaced cleanly:\n%s" % out
