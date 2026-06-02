"""pidfd, subprocess.Popen.wait, os.waitpid/wait/waitid/system."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# pidfd -- event-driven child-process reaping (Linux 5.3+)
#
# os.pidfd_open(pid) returns an fd the netpoll backend can wait on; it becomes
# readable exactly when that process terminates.  This turns the WNOHANG
# busy-poll in Popen.wait / os.waitpid / os.waitid into a single wait_fd park
# for the common "wait for this child to exit" case.  A pidfd only signals
# termination -- never stop/continue -- so callers asking for those events
# (WUNTRACED / WCONTINUED / WSTOPPED) keep the poll loop.
# ============================================================
_HAVE_PIDFD = hasattr(os, "pidfd_open")

# Wait options a pidfd cannot represent (stop/continue, not termination).
_PIDFD_INCOMPATIBLE = (getattr(os, "WUNTRACED", 0) |
                       getattr(os, "WCONTINUED", 0) |
                       getattr(os, "WSTOPPED", 0))


def _pidfd_open(pid):
    """Return a pidfd for `pid` that becomes readable when it exits, or None
    if pidfd is unavailable, `pid` is not one specific positive pid, or the
    open raced a reap (ESRCH).  Caller owns the fd and must close it."""
    if not _HAVE_PIDFD or pid is None or pid <= 0:
        return None
    try:
        return os.pidfd_open(pid)
    except (OSError, ValueError):
        # ESRCH: already reaped.  ENOSYS / EINVAL: kernel too old.
        return None


# ============================================================
# subprocess
# ============================================================
_orig_popen_wait = None


def _patched_popen_wait(self, timeout=None):
    if not _in_goroutine() or self.returncode is not None:
        return _orig_popen_wait(self, timeout)
    deadline = None if timeout is None else time.monotonic() + timeout
    # Fast path (POSIX, Linux 5.3+): park on a pidfd until the child exits,
    # then poll() reaps it -- no busy-poll tick.  poll() is what records the
    # returncode (Windows: WaitForSingleObject 0ms; POSIX: waitpid(WNOHANG)).
    pfd = _pidfd_open(getattr(self, "pid", None))
    if pfd is not None:
        try:
            while True:
                rc = self.poll()
                if rc is not None:
                    return rc
                if deadline is None:
                    pygo_core.wait_fd(pfd, READ)
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(self.args, timeout)
                    if pygo_core.wait_fd(pfd, READ,
                                         int(remaining * 1000) + 1) == 0:
                        if self.poll() is not None:
                            return self.returncode
                        raise subprocess.TimeoutExpired(self.args, timeout)
                # pidfd signalled exit -> loop back; poll() reaps the zombie.
        finally:
            os.close(pfd)
    # Fallback: portable WNOHANG poll loop (Windows, or no pidfd).  _co_sleep
    # yields to other goroutines, so this is cooperatively safe.
    step = 0.001
    while True:
        rc = self.poll()
        if rc is not None:
            return rc
        if deadline is not None:
            now = time.monotonic()
            if now >= deadline:
                raise subprocess.TimeoutExpired(self.args, timeout)
            _co_sleep(min(step, deadline - now))
        else:
            _co_sleep(step)
        if step < 0.05:
            step *= 2


def _patch_subprocess():
    global _orig_popen_wait
    _orig_popen_wait = subprocess.Popen.wait
    subprocess.Popen.wait = _patched_popen_wait


def _unpatch_subprocess():
    subprocess.Popen.wait = _orig_popen_wait


# ============================================================
# process -- os.waitpid / os.wait / os.waitid / os.system
#
# subprocess.Popen.wait is handled above, but bare os.wait* calls (used by
# code that forks directly, by os.popen, by some test harnesses) and
# os.system still block the OS thread.  On POSIX we make the wait family
# cooperative with a WNOHANG poll loop; os.system has no non-blocking form,
# so it is offloaded to the backend pool.  On Windows WNOHANG does not
# exist, so os.waitpid is offloaded too.
# ============================================================
_orig_os_waitpid = None
_orig_os_wait    = None
_orig_os_waitid  = None
_orig_os_wait3   = None
_orig_os_wait4   = None
_orig_os_system  = None

_HAVE_WNOHANG = hasattr(os, "WNOHANG")


def _patched_os_waitpid(pid, options):
    if not _in_goroutine():
        return _orig_os_waitpid(pid, options)
    if not _HAVE_WNOHANG:
        # Windows: no polling form -- offload the blocking wait.
        return _blocking_call(_orig_os_waitpid, pid, options)
    if options & os.WNOHANG:
        return _orig_os_waitpid(pid, options)
    # Event-driven fast path: park on a pidfd that signals the child's exit,
    # then reap WNOHANG.  Only for a single child waited for termination.
    pfd = None
    if not (options & _PIDFD_INCOMPATIBLE):
        pfd = _pidfd_open(pid)
    step = 0.0005
    try:
        while True:
            # WNOHANG returns (0, 0) when the requested child has not yet
            # changed state; ECHILD (no such child) propagates as it should.
            r = _orig_os_waitpid(pid, options | os.WNOHANG)
            if r[0] != 0:
                return r
            if pfd is not None:
                pygo_core.wait_fd(pfd, READ)   # park until the child exits
                os.close(pfd)
                pfd = None                     # reap next iter; poll if raced
                continue
            _co_sleep(step)
            if step < 0.02:
                step *= 2
    finally:
        if pfd is not None:
            os.close(pfd)


def _patched_os_wait():
    if not _in_goroutine() or not _HAVE_WNOHANG:
        return _orig_os_wait()
    # os.wait() == waitpid(-1, 0): wait for any child.
    return _patched_os_waitpid(-1, 0)


def _patched_os_waitid(idtype, id, options):
    if not _in_goroutine():
        return _orig_os_waitid(idtype, id, options)
    if options & os.WNOHANG:
        return _orig_os_waitid(idtype, id, options)
    # pidfd fast path only when waiting for one specific pid's termination.
    pfd = None
    if idtype == getattr(os, "P_PID", object()) and \
            not (options & _PIDFD_INCOMPATIBLE):
        pfd = _pidfd_open(id)
    step = 0.0005
    try:
        while True:
            # waitid + WNOHANG returns None when no child has changed state.
            r = _orig_os_waitid(idtype, id, options | os.WNOHANG)
            if r is not None:
                return r
            if pfd is not None:
                pygo_core.wait_fd(pfd, READ)
                os.close(pfd)
                pfd = None
                continue
            _co_sleep(step)
            if step < 0.02:
                step *= 2
    finally:
        if pfd is not None:
            os.close(pfd)


def _patched_os_wait4(pid, options):
    # wait4(pid, options) -> (pid, status, rusage); like waitpid + rusage.
    if not _in_goroutine() or not _HAVE_WNOHANG:
        return _orig_os_wait4(pid, options)
    if options & os.WNOHANG:
        return _orig_os_wait4(pid, options)
    pfd = None
    if not (options & _PIDFD_INCOMPATIBLE):
        pfd = _pidfd_open(pid)
    step = 0.0005
    try:
        while True:
            r = _orig_os_wait4(pid, options | os.WNOHANG)
            if r[0] != 0:
                return r
            if pfd is not None:
                pygo_core.wait_fd(pfd, READ)
                os.close(pfd)
                pfd = None
                continue
            _co_sleep(step)
            if step < 0.02:
                step *= 2
    finally:
        if pfd is not None:
            os.close(pfd)


def _patched_os_wait3(options):
    # wait3(options) == wait4(-1, options): any child, with rusage.
    if not _in_goroutine() or not _HAVE_WNOHANG:
        return _orig_os_wait3(options)
    return _patched_os_wait4(-1, options)


def _patched_os_system(command):
    # No non-blocking form; run it on the backend pool so the goroutine
    # parks instead of freezing the scheduler for the child's lifetime.
    return _blocking_call(_orig_os_system, command)


def _patch_process():
    global _orig_os_waitpid, _orig_os_wait, _orig_os_waitid, _orig_os_system
    global _orig_os_wait3, _orig_os_wait4
    if hasattr(os, "wait3"):
        _orig_os_wait3 = os.wait3
        os.wait3 = _patched_os_wait3
    if hasattr(os, "wait4"):
        _orig_os_wait4 = os.wait4
        os.wait4 = _patched_os_wait4
    if hasattr(os, "waitpid"):
        _orig_os_waitpid = os.waitpid
        os.waitpid = _patched_os_waitpid
    if hasattr(os, "wait"):
        _orig_os_wait = os.wait
        os.wait = _patched_os_wait
    if hasattr(os, "waitid"):
        _orig_os_waitid = os.waitid
        os.waitid = _patched_os_waitid
    if hasattr(os, "system"):
        _orig_os_system = os.system
        os.system = _patched_os_system


def _unpatch_process():
    if _orig_os_waitpid is not None:
        os.waitpid = _orig_os_waitpid
    if _orig_os_wait is not None:
        os.wait = _orig_os_wait
    if _orig_os_waitid is not None:
        os.waitid = _orig_os_waitid
    if _orig_os_wait3 is not None:
        os.wait3 = _orig_os_wait3
    if _orig_os_wait4 is not None:
        os.wait4 = _orig_os_wait4
    if _orig_os_system is not None:
        os.system = _orig_os_system
