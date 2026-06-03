"""Fork-safety torture for the monkey layer.

os.fork() + cooperative monkey-patching is a classic hazard: fork copies only
the forking thread, so the offload thread-pool's workers are dead in the child
and the self-pipe parkers are shared with the parent.  monkey._after_fork_child
(registered via os.register_at_fork) nulls the backend and drops the pooled
parkers; this hammers that reset adversarially -- fork with the pool live, with
goroutines having run, repeatedly -- and asserts the child can still run
cooperative work, the parent survives, and nothing hangs or leaks fds.

POSIX-only (needs os.fork); skipped elsewhere.
"""
import os
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest

import runloom            # noqa: E402
import runloom.monkey     # noqa: E402
import runloom_c       # noqa: E402

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork required")

runloom.monkey.patch()

CHILD_TIMEOUT = float(os.environ.get("RUNLOOM_FORK_TIMEOUT", "15"))


def _coop(workload):
    """Run workload() inside a goroutine, return its result."""
    box = []
    runloom_c.go(lambda: box.append(workload()), stack_size=8 << 20)
    runloom_c.run()
    return box[0] if box else None


def _socketpair_roundtrip():
    a, b = socket.socketpair()
    try:
        a.sendall(b"ping")
        assert b.recv(4) == b"ping"
    finally:
        a.close()
        b.close()
    return True


def _file_offload():
    # Touches the thread-pool offload backend (regular-file open/read).
    import tempfile
    fd, path = tempfile.mkstemp()
    try:
        os.write(fd, b"x" * 256)
        os.close(fd)
        with open(path, "rb") as fh:
            fh.read()
    finally:
        os.unlink(path)
    return True


def _run_child_workload_under_fork(child_workload):
    """Fork; run child_workload() (a cooperative goroutine workload) in the
    child; return the child's exit code, or raise on hang."""
    pid = os.fork()
    if pid == 0:                       # ---- child ----
        rc = 0
        try:
            _coop(child_workload)
        except BaseException:
            rc = 1
        os._exit(rc)
    # ---- parent: reap with a deadline so a child deadlock is a failure ----
    deadline = time.monotonic() + CHILD_TIMEOUT
    while True:
        done, status = os.waitpid(pid, os.WNOHANG)
        if done == pid:
            return os.waitstatus_to_exitcode(status)
        if time.monotonic() > deadline:
            os.kill(pid, 9)
            os.waitpid(pid, 0)
            raise AssertionError("forked child hung > {0}s (cooperative "
                                 "deadlock after fork)".format(CHILD_TIMEOUT))
        time.sleep(0.01)


def test_fork_child_runs_cooperative_io():
    """A clean fork: the child runs a cooperative socketpair roundtrip."""
    assert _run_child_workload_under_fork(_socketpair_roundtrip) == 0


def test_fork_after_offload_pool_started():
    """Start the offload pool in the PARENT, then fork: the child's inherited
    pool workers are dead, so the child must rebuild the backend (the
    _after_fork_child reset) when it does its own file I/O."""
    _coop(_file_offload)               # parent starts the pool
    assert _run_child_workload_under_fork(_file_offload) == 0


def test_fork_after_goroutines_ran():
    """Fork after goroutines have run in the parent (scheduler state populated).
    The child gets only the forking thread; it must still drive a fresh run."""
    for _ in range(5):
        _coop(_socketpair_roundtrip)
    assert _run_child_workload_under_fork(_socketpair_roundtrip) == 0


def test_repeated_fork_no_fd_leak():
    """Many forks, each doing cooperative work in the child, must all exit 0 and
    must not leak descriptors in the PARENT (the self-pipe / pool fds)."""
    def fd_count():
        # /proc/self/fd on Linux, /dev/fd on macOS/BSD; None where neither.
        for p in ("/proc/self/fd", "/dev/fd"):
            try:
                return len(os.listdir(p))
            except OSError:
                pass
        return None

    _coop(_file_offload)               # warm the pool first
    base = fd_count()
    for _ in range(12):
        assert _run_child_workload_under_fork(_socketpair_roundtrip) == 0
    if base is not None:
        leaked = fd_count() - base
        assert leaked <= 0, "parent leaked {0} fd(s) across forks".format(leaked)


def test_parent_survives_forks():
    """The parent keeps working cooperatively after forking children."""
    for _ in range(4):
        _run_child_workload_under_fork(_socketpair_roundtrip)
    assert _coop(_socketpair_roundtrip) is True
