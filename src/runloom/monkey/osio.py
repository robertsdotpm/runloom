"""Cooperative os.read/os.write/readv/writev and stdio (input, sys.stdin)."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .sockets import _netpoll_unregister, _wait_fd_coop  # noqa: F401

# ============================================================
# os.read / os.write
# ============================================================
_orig_os_read = None
_orig_os_write = None


def _patched_os_read(fd, n):
    if not _in_fiber():
        return _orig_os_read(fd, n)
    if _fd_pollable(fd):
        try:
            os.set_blocking(fd, False)
        except OSError:
            return _blocking_call(_orig_os_read, fd, n)
        while True:
            try:
                return _orig_os_read(fd, n)
            except (BlockingIOError, InterruptedError):
                _wait_fd_coop(fd, READ)
    # Regular file / non-pollable -- offload to backend pool.
    return _blocking_call(_orig_os_read, fd, n)


def _patched_os_write(fd, data):
    if not _in_fiber():
        return _orig_os_write(fd, data)
    if _fd_pollable(fd):
        try:
            os.set_blocking(fd, False)
        except OSError:
            return _blocking_call(_orig_os_write, fd, data)
        while True:
            try:
                return _orig_os_write(fd, data)
            except (BlockingIOError, InterruptedError):
                _wait_fd_coop(fd, WRITE)
    return _blocking_call(_orig_os_write, fd, data)


_orig_os_readv  = None
_orig_os_writev = None


def _patched_os_readv(fd, buffers):
    # Vectored read -- the readv analogue of read: cooperative on pollable
    # fds, pool offload on regular files.
    if not _in_fiber():
        return _orig_os_readv(fd, buffers)
    if _fd_pollable(fd):
        try:
            os.set_blocking(fd, False)
        except OSError:
            return _blocking_call(_orig_os_readv, fd, buffers)
        while True:
            try:
                return _orig_os_readv(fd, buffers)
            except (BlockingIOError, InterruptedError):
                _wait_fd_coop(fd, READ)
    return _blocking_call(_orig_os_readv, fd, buffers)


def _patched_os_writev(fd, buffers):
    if not _in_fiber():
        return _orig_os_writev(fd, buffers)
    if _fd_pollable(fd):
        try:
            os.set_blocking(fd, False)
        except OSError:
            return _blocking_call(_orig_os_writev, fd, buffers)
        while True:
            try:
                return _orig_os_writev(fd, buffers)
            except (BlockingIOError, InterruptedError):
                _wait_fd_coop(fd, WRITE)
    return _blocking_call(_orig_os_writev, fd, buffers)


_orig_os_readinto = None


def _patched_os_readinto(fd, buffer):
    # os.readinto (3.14+): read into a caller-provided writable buffer, return
    # the byte count -- the read/readv analogue.  CPython 3.14's
    # _pyio.FileIO.readall()/readinto() route through os.readinto (3.13 used
    # os.read), so a buffered stream .read() on a pipe/tty would block the whole
    # hub without this patch.  Cooperative on pollable fds, pool offload on
    # regular files; the parked fiber keeps `buffer` alive (same contract as
    # readv's buffers).
    if not _in_fiber():
        return _orig_os_readinto(fd, buffer)
    if _fd_pollable(fd):
        try:
            os.set_blocking(fd, False)
        except OSError:
            return _blocking_call(_orig_os_readinto, fd, buffer)
        while True:
            try:
                return _orig_os_readinto(fd, buffer)
            except (BlockingIOError, InterruptedError):
                _wait_fd_coop(fd, READ)
    return _blocking_call(_orig_os_readinto, fd, buffer)


_orig_os_close = None


def _patched_os_close(fd):
    """Clear the netpoll registration bit for fd before closing so
    that fd reuse re-registers cleanly under the ET register-once
    scheme.  Pipes, sockets-via-fd, ttys all funnel through here."""
    if _netpoll_unregister is not None and fd >= 0:
        _netpoll_unregister(fd)
    return _orig_os_close(fd)


def _patch_os():
    global _orig_os_read, _orig_os_write, _orig_os_close
    global _orig_os_readv, _orig_os_writev, _orig_os_readinto
    _orig_os_read  = os.read
    _orig_os_write = os.write
    _orig_os_close = os.close
    os.read  = _patched_os_read
    os.write = _patched_os_write
    os.close = _patched_os_close
    if hasattr(os, "readv"):
        _orig_os_readv = os.readv
        os.readv = _patched_os_readv
    if hasattr(os, "writev"):
        _orig_os_writev = os.writev
        os.writev = _patched_os_writev
    if hasattr(os, "readinto"):        # 3.14+: buffered .read() routes here
        _orig_os_readinto = os.readinto
        os.readinto = _patched_os_readinto


def _unpatch_os():
    os.read  = _orig_os_read
    os.write = _orig_os_write
    os.close = _orig_os_close
    if _orig_os_readv is not None:
        os.readv = _orig_os_readv
    if _orig_os_writev is not None:
        os.writev = _orig_os_writev
    if _orig_os_readinto is not None:
        os.readinto = _orig_os_readinto



# ============================================================
# stdio (input, sys.stdin)
# ============================================================
_orig_input = None
_orig_stdin_read = None
_orig_stdin_readline = None


def _patched_input(prompt=""):
    if not _in_fiber():
        return _orig_input(prompt)
    if prompt:
        sys.stdout.write(prompt)
        sys.stdout.flush()
    # Offload the blocking read to a pool worker rather than gating on
    # wait_fd.  sys.stdin is a TextIOWrapper over a BufferedReader: an
    # earlier read slurps everything the kernel pipe has to offer into the
    # user-space decode/byte buffers, and wait_fd -- which only sees
    # kernel-level readiness -- is blind to that buffered data.  Parking on
    # wait_fd for a kernel-empty fd would hang while the next line already
    # sits in the io buffer.  The worker's real read returns any buffered
    # line immediately and only truly blocks when the stream is empty,
    # parking just this fiber.
    return offload(_orig_input, "")


def _patched_stdin_read(*args):
    if not _in_fiber():
        return _orig_stdin_read(*args)
    # See _patched_input: wait_fd is blind to data already buffered in the
    # TextIOWrapper, so offload the blocking read instead of parking on it.
    return offload(_orig_stdin_read, *args)


def _patched_stdin_readline(*args):
    if not _in_fiber():
        return _orig_stdin_readline(*args)
    # See _patched_input: wait_fd is blind to data already buffered in the
    # TextIOWrapper, so offload the blocking read instead of parking on it.
    return offload(_orig_stdin_readline, *args)


def _patch_stdio():
    global _orig_input, _orig_stdin_read, _orig_stdin_readline
    _orig_input = builtins.input
    builtins.input = _patched_input
    # sys.stdin is a TextIOWrapper over a BufferedReader: the underlying
    # buffered read may already hold the next line(s) drained from the
    # kernel pipe, so the patched methods offload the blocking read to a
    # pool worker (which honours that buffering) instead of parking on
    # wait_fd, whose kernel-level readiness check is blind to buffered data.
    try:
        _orig_stdin_read     = sys.stdin.read
        _orig_stdin_readline = sys.stdin.readline
        sys.stdin.read     = _patched_stdin_read
        sys.stdin.readline = _patched_stdin_readline
    except (AttributeError, TypeError):
        # Some stdin replacements (pytest capture, IDLE) don't allow this.
        _orig_stdin_read     = None
        _orig_stdin_readline = None


def _unpatch_stdio():
    builtins.input = _orig_input
    if _orig_stdin_read is not None:
        try:
            sys.stdin.read     = _orig_stdin_read
            sys.stdin.readline = _orig_stdin_readline
        except (AttributeError, TypeError):
            pass


# ============================================================
# getpass.getpass -- offload the blocking tty password read
# ============================================================
# Unlike input(), getpass reads from /dev/tty (opened internally, with echo
# turned off via termios), so we can't simply wait_fd on a known stdin fd.  It
# is rare and human-latency-bound, so offload the whole call to a pool worker,
# parking the fiber.  The tty/termios work is thread-agnostic (no
# per-object thread affinity like sqlite), so running it on a worker is fine.
_orig_getpass = None


def _patched_getpass(prompt="Password: ", stream=None):
    if not _in_fiber():
        return _orig_getpass(prompt, stream)
    return offload(_orig_getpass, prompt, stream)


def _patch_getpass():
    global _orig_getpass
    if _orig_getpass is not None:
        return
    import getpass as _gp
    _orig_getpass = _gp.getpass
    _gp.getpass = _patched_getpass


def _unpatch_getpass():
    global _orig_getpass
    if _orig_getpass is None:
        return
    import getpass as _gp
    _gp.getpass = _orig_getpass
    _orig_getpass = None
