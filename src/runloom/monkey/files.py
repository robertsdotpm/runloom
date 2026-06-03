"""fcntl file locks, builtins.open, and disk os.* syscalls (offloaded)."""
from ._base import *  # noqa: F401,F403  (shared foundation)

# ============================================================
# fcntl -- cooperative flock / lockf (advisory file locks)
#
# A blocking flock(LOCK_EX) / lockf parks the OS thread inside the kernel
# until the lock is granted -- there is no readiness fd to hand to netpoll
# (you cannot epoll a file lock).  So the cooperative form acquires with the
# non-blocking variant (LOCK_NB) and, on contention, parks via _co_sleep and
# retries on a backoff.  This keeps the scheduler thread free and stays
# cancel-friendly (a cancelled goroutine just stops retrying), rather than
# pinning a backend-pool worker on an uninterruptible blocking lock.
#
# Pass-through (no cooperative loop) when: outside a goroutine, the caller
# already asked for LOCK_NB (wants the immediate raise), or the op is an
# unlock (LOCK_UN -- never blocks).
# ============================================================
try:
    import fcntl as _fcntl_mod
except ImportError:
    _fcntl_mod = None        # Windows: no fcntl module, patch is a no-op.

_orig_flock = None
_orig_lockf = None

# Errnos that mean "lock is held by someone else, try again", per flock(2)
# (EWOULDBLOCK) and fcntl(2) F_SETLK (EACCES / EAGAIN).
_LOCK_CONTENDED = frozenset(
    e for e in (getattr(errno, "EWOULDBLOCK", None),
                getattr(errno, "EAGAIN", None),
                getattr(errno, "EACCES", None))
    if e is not None)


def _co_lock_acquire(call, op, nb_bit, lock_bits):
    """Shared cooperative acquire loop for flock/lockf.  `call` performs the
    lock with a given operation; we OR in the non-blocking bit and retry."""
    step = 0.0005
    nb_op = op | nb_bit
    while True:
        try:
            return call(nb_op)
        except InterruptedError:
            continue                         # EINTR: retry immediately
        except (BlockingIOError, PermissionError):
            pass                             # contended: park + retry
        except OSError as e:
            if e.errno not in _LOCK_CONTENDED:
                raise
        _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patched_flock(fd, operation):
    lock_bits = _fcntl_mod.LOCK_SH | _fcntl_mod.LOCK_EX
    if not _in_goroutine() or (operation & _fcntl_mod.LOCK_NB) or \
            not (operation & lock_bits):
        return _orig_flock(fd, operation)
    return _co_lock_acquire(lambda op: _orig_flock(fd, op),
                            operation, _fcntl_mod.LOCK_NB, lock_bits)


def _patched_lockf(fd, cmd, length=0, start=0, whence=0):
    lock_bits = _fcntl_mod.LOCK_SH | _fcntl_mod.LOCK_EX
    if not _in_goroutine() or (cmd & _fcntl_mod.LOCK_NB) or \
            not (cmd & lock_bits):
        return _orig_lockf(fd, cmd, length, start, whence)
    return _co_lock_acquire(
        lambda op: _orig_lockf(fd, op, length, start, whence),
        cmd, _fcntl_mod.LOCK_NB, lock_bits)


def _patch_fcntl():
    global _orig_flock, _orig_lockf
    if _fcntl_mod is None:
        return
    if hasattr(_fcntl_mod, "flock"):
        _orig_flock = _fcntl_mod.flock
        _fcntl_mod.flock = _patched_flock
    if hasattr(_fcntl_mod, "lockf"):
        _orig_lockf = _fcntl_mod.lockf
        _fcntl_mod.lockf = _patched_lockf


def _unpatch_fcntl():
    if _fcntl_mod is None:
        return
    if _orig_flock is not None:
        _fcntl_mod.flock = _orig_flock
    if _orig_lockf is not None:
        _fcntl_mod.lockf = _orig_lockf



# ============================================================
# file -- builtins.open dispatched through the backend
#
# Wrapping open() covers the open syscall itself (cold-inode lookups, NFS,
# FUSE, slow disk) so the goroutine doesn't freeze the scheduler waiting on
# it.  NOTE: the returned file object's later .read()/.write() do NOT go
# through our os.read/os.write patches -- io.FileIO issues the read()/write()
# syscalls directly in C, bypassing the os module entirely.  For local,
# page-cache-warm files that is fast and invisible.  For genuinely slow
# media (NFS/FUSE/cold spindle) a large .read() can still stall the
# scheduler; callers on slow storage that care should use
# runloom.monkey._blocking_call(f.read, n) or os.read on the raw fd (which IS
# offloaded for regular files).  Offloading every buffered read/write is
# possible but adds a backend round trip to the common fast case, so it is
# deliberately left out of v0.
# ============================================================
_orig_open = None


def _patched_open(*args, **kwargs):
    if not _in_goroutine():
        return _orig_open(*args, **kwargs)
    return _get_backend().submit(_orig_open, args, kwargs)


def _patch_file():
    global _orig_open
    _orig_open = builtins.open
    builtins.open = _patched_open


def _unpatch_file():
    builtins.open = _orig_open



# ============================================================
# syscalls -- os.* disk operations dispatched through the backend
# ============================================================
_SYSCALL_NAMES = (
    "stat", "lstat", "fstat", "statvfs", "fstatvfs", "access",
    "listdir", "scandir",
    "mkdir", "rmdir", "rename", "replace", "unlink", "remove",
    "link", "symlink", "readlink",
    "chmod", "chown", "lchown",
    "truncate", "ftruncate",
    "fsync", "fdatasync",
    "utime",
    "open", "sendfile", "pread", "pwrite",
    # splice / copy_file_range: Linux zero-copy fd-to-fd moves.  Two fds + a
    # length; the simplest cooperative form is a backend offload (the goroutine
    # parks while a pool worker runs the blocking move), like os.system.
    "splice", "copy_file_range",
)

_orig_syscalls = {}


def _make_pool_patch(orig):
    def patched(*args, **kwargs):
        if not _in_goroutine():
            return orig(*args, **kwargs)
        return _get_backend().submit(orig, args, kwargs)
    patched.__name__ = getattr(orig, "__name__", "patched_syscall")
    return patched


def _patch_syscalls():
    for name in _SYSCALL_NAMES:
        orig = getattr(os, name, None)
        if orig is None:
            continue
        _orig_syscalls[name] = orig
        setattr(os, name, _make_pool_patch(orig))


def _unpatch_syscalls():
    for name, orig in list(_orig_syscalls.items()):
        setattr(os, name, orig)
    _orig_syscalls.clear()
