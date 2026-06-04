"""Shared foundation for the runloom monkey-patch package: stdlib
re-exports, goroutine-context detection, the self-pipe Parker, the
blocking-call backend, and cooperative sleep.  Every section module
does `from ._base import *`."""

import _thread
import builtins
import collections
import errno
import io
import os
import platform as _platform
import select as _select_mod
import socket
import ssl as _ssl_mod
import stat
import subprocess
import sys
import threading as _th
import time

_IS_WINDOWS = _platform.system() == "Windows"
_IS_DARWIN  = _platform.system() == "Darwin"

# The pid that imported this module -- i.e. the process whose runloom scheduler
# our cooperative identity (runloom.current()) is valid in.  After os.fork() the
# child inherits a *copy* of the scheduler that is not actually running, so
# runloom.current() there returns a fresh, meaningless G on every call.  This is
# captured once and never reassigned, so a pid mismatch reliably means "we are
# in a forked child" regardless of os.register_at_fork handler ordering.  It is
# only ever consulted on cold error paths (see _is_forked_child), never in the
# I/O hot path -- os.getpid() costs ~1.3us, far too much per recv/send.
_PID_IMPORT = os.getpid()


def _is_forked_child():
    return os.getpid() != _PID_IMPORT

import runloom
import runloom_c

READ  = 1
WRITE = 2

# Snapshots of stdlib primitives that the patches themselves call.
# Capturing here (at module import time) protects us against the
# patched versions calling back into themselves.
_raw_os_read  = os.read
_raw_os_write = os.write
_raw_os_close = os.close
_raw_time_sleep = time.sleep
# Captured before _patch_socket installs the cooperative versions.  Parker
# uses these to talk to its self-pipe / self-socketpair without going
# through the cooperative wrappers (which would, for instance, park
# forever on a non-blocking recv that returns BlockingIOError).
_raw_sock_recv = socket.socket.recv
_raw_sock_send = socket.socket.send
# Raw select (before polling.py installs the cooperative select).  A _Parker on
# a FOREIGN OS thread (not a goroutine) blocks the thread on its wake fd with
# this -- the patched select would re-enter the cooperative path on a thread
# with no goroutine/hub.
_raw_select = _select_mod.select
# Raw os.sendfile (before _patch_syscalls offloads it to the pool).  The
# cooperative socket.sendfile drives this directly in non-blocking mode and
# parks on wait_fd, rather than blocking a pool worker for the whole transfer.
_raw_os_sendfile = getattr(os, "sendfile", None)


# ---------- goroutine-context detection ----------
# runloom_c (C scheduler) does not expose a "current goroutine"
# accessor, so we wrap runloom_c.go / mn_go and bump a thread-local
# counter for the duration of every user callable.  The Python
# scheduler still uses runloom.current() (which works there).
_g_state = _th.local()


def _bump_in(value):
    _g_state.count = getattr(_g_state, "count", 0) + value


def _wrap_goroutine_callable(fn):
    def wrapper():
        _bump_in(1)
        try:
            return fn()
        finally:
            _bump_in(-1)
    return wrapper


def _in_goroutine():
    """True when called from inside a running goroutine.

    Handles both the C scheduler (via the thread-local counter set by
    our runloom_c.go wrapper) and the Python scheduler (via
    runloom.current())."""
    if getattr(_g_state, "count", 0) > 0:
        return True
    try:
        return runloom.current() is not None
    except Exception:
        return False


def _make_nonblocking(sock):
    """Set the underlying fd to non-blocking, idempotent.

    Also flips TCP_NODELAY on stream sockets the first time we see them.
    Without it, Nagle + delayed-ack stalls the loopback echo path by
    up to 40 ms per round trip -- which is what made the README's
    116 us/RT number look respectable when native Go does ~50 us.
    """
    if sock.gettimeout() != 0.0:
        sock.setblocking(False)
    # Flip TCP_NODELAY on stream sockets.  Cheap (sets a flag), safe
    # (request-response apps benefit unconditionally), and matches Go
    # / asyncio's default behaviour for low-latency TCP.
    try:
        if sock.type == socket.SOCK_STREAM and \
           sock.family in (socket.AF_INET, socket.AF_INET6):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except (OSError, AttributeError):
        pass


def _fd_pollable(fd):
    """Is this fd pollable by the C-side netpoll?

    POSIX (epoll/kqueue/select):
        sockets, fifos/pipes, ttys, char/block devices  -> yes
        regular files                                    -> no
    Windows (WSAPoll/select):
        SOCKET handles  -> yes (handled by the socket-layer patches,
                           not by this function -- os.read/write
                           callers never see SOCKET fds)
        pipe / file / tty fds -> no.  Win32 select() refuses anything
                                 except SOCKETs; pipe fds are kernel
                                 HANDLEs wrapped by the CRT and are
                                 not selectable.

    On Windows we therefore always return False from this helper --
    the os.read/write path can only ever see non-socket fds, which
    must go through the thread-pool backend instead of wait_fd.
    """
    if _IS_WINDOWS:
        return False
    try:
        st = os.fstat(fd)
    except OSError:
        return False
    m = st.st_mode
    return (stat.S_ISFIFO(m) or stat.S_ISSOCK(m) or
            stat.S_ISCHR(m)  or stat.S_ISBLK(m))


# ---------- self-pipe parker ----------
#
# Two implementations, chosen at import time:
#   POSIX  -> os.pipe() + os.read/os.write.  Pipes are kernel-fd ints
#             that wait_fd's epoll/kqueue/select backends can all poll.
#   Windows -> socket.socketpair() + sock.recv/sock.send.  Windows
#             select() refuses pipe fds (those are Python-side fakes
#             over Win32 HANDLEs); it ONLY polls SOCKET handles.
#             socket.socketpair() on Windows returns two AF_INET TCP
#             sockets whose fileno() values are real SOCKET handles
#             and therefore work with wait_fd's select backend.
#
# Either way, the Parker is single-thread cooperative -- the
# unpark()-before-park() race is not handled because in cooperative
# mode the parker can't be signalled until the parking goroutine has
# yielded.
class _Parker(object):
    __slots__ = ("r", "w", "_sockets")
    _pool = []
    # Real lock guarding the shared pool: under M:N several hub threads
    # build/release parkers concurrently, and `if _pool: _pool.pop()` is a
    # TOCTOU race (two threads both see it non-empty, one pops an empty list
    # -> IndexError).  Held only for the O(1) pop/append, never across I/O.
    _pool_lock = _thread.allocate_lock()

    def __init__(self):
        reused = None
        with _Parker._pool_lock:
            if _Parker._pool:
                reused = _Parker._pool.pop()
        if reused is not None:
            self.r, self.w, self._sockets = reused
        elif _IS_WINDOWS:
            s1, s2 = socket.socketpair()
            s1.setblocking(False)
            s2.setblocking(False)
            self._sockets = (s1, s2)
            self.r = s1.fileno()
            self.w = s2.fileno()
        else:
            r, w = os.pipe()
            os.set_blocking(r, False)
            os.set_blocking(w, False)
            self.r = r
            self.w = w
            self._sockets = None

    def park(self, timeout=None):
        if _in_goroutine():
            runloom_c.wait_fd(self.r, READ)     # cooperative goroutine park
        else:
            # FOREIGN OS thread (e.g. a multiprocessing.Queue _feed daemon
            # thread taking a monkey-patched threading.Condition): block the
            # THREAD on the wake fd with a real select.  runloom_c.wait_fd parks
            # a GOROUTINE on a hub's netpoll -- there is no goroutine/hub on this
            # thread, so calling it here is undefined and raced -> SIGSEGV under
            # M:N.  timeout=None blocks until unpark() writes the wake byte.
            try:
                _raw_select([self.r], [], [], timeout)
            except (OSError, ValueError):
                pass
        if self._sockets is not None:
            try:
                _raw_sock_recv(self._sockets[0], 64)
            except (BlockingIOError, OSError):
                pass
        else:
            try:
                _raw_os_read(self.r, 64)
            except (BlockingIOError, OSError):
                pass

    def unpark(self):
        if self._sockets is not None:
            try:
                _raw_sock_send(self._sockets[1], b"\x01")
            except (BlockingIOError, BrokenPipeError, OSError):
                pass
        else:
            try:
                _raw_os_write(self.w, b"\x01")
            except (BlockingIOError, BrokenPipeError, OSError):
                pass

    def release(self):
        # Drain any stale wake bytes before returning to the pool.  Must
        # use raw recv/read -- the patched versions would park forever
        # on BlockingIOError instead of returning empty.
        if self._sockets is not None:
            try:
                while _raw_sock_recv(self._sockets[0], 64):
                    pass
            except (BlockingIOError, OSError):
                pass
        else:
            try:
                while _raw_os_read(self.r, 64):
                    pass
            except (BlockingIOError, OSError):
                pass
        pooled = False
        with _Parker._pool_lock:
            if len(_Parker._pool) < 64:
                _Parker._pool.append((self.r, self.w, self._sockets))
                pooled = True
        if not pooled:
            if self._sockets is not None:
                for s in self._sockets:
                    try: s.close()
                    except OSError: pass
            else:
                try: os.close(self.r)
                except OSError: pass
                try: os.close(self.w)
                except OSError: pass


# ============================================================
# blocking-call backend (files, disk syscalls, any non-pollable I/O)
#
# Per-OS slot.  Today: thread pool everywhere.  io_uring backend on
# Linux 5.6+ can slot in with no caller-side changes: backends only
# need to expose submit(fn, args, kwargs) -> result and a fini().
# ============================================================
_real_Lock_for_backend      = _th.Lock         # captured before any patch
_real_Condition_for_backend = _th.Condition


class _BlockingBackend(object):
    name = "abstract"
    def submit(self, fn, args, kwargs):
        raise NotImplementedError
    def fini(self):
        pass


class _ThreadPoolBackend(_BlockingBackend):
    """Pre-started worker pool with real Lock/Condition.  Each submitted
    task gets a self-pipe (from the Parker pool) for wakeup -- the
    goroutine parks on wait_fd, the worker writes a byte when done."""
    name = "thread-pool"

    def __init__(self, size=None):
        if size is None:
            try:
                size = min(8, (os.cpu_count() or 4))
            except Exception:
                size = 4
        self.size = max(1, size)
        self._lock = _real_Lock_for_backend()
        self._cond = _real_Condition_for_backend(self._lock)
        self._items = collections.deque()
        self._closed = False
        self._started = 0

    def _ensure_workers(self):
        if self._started >= self.size:
            return
        # First touch -- start the whole pool at once so warm-up is paid
        # once, not amortised across the first N submissions.
        while self._started < self.size:
            _thread.start_new_thread(self._worker_loop, ())
            self._started += 1

    def _worker_loop(self):
        while True:
            with self._lock:
                while not self._items and not self._closed:
                    self._cond.wait()
                if self._closed and not self._items:
                    return
                fn, args, kwargs, box, parker = self._items.popleft()
            try:
                box[0] = fn(*args, **kwargs)
            except BaseException as e:
                box[1] = e
            parker.unpark()

    def submit(self, fn, args, kwargs):
        self._ensure_workers()
        if kwargs is None:
            kwargs = {}
        p = _Parker()
        box = [None, None]
        with self._lock:
            self._items.append((fn, args, kwargs, box, p))
            self._cond.notify()
        p.park()
        p.release()
        if box[1] is not None:
            raise box[1]
        return box[0]

    def fini(self):
        with self._lock:
            self._closed = True
            self._cond.notify_all()


_backend = None


def _select_backend():
    """Pick the fastest available blocking backend for this OS.

    Today: thread pool everywhere.  Linux io_uring (5.6+) slots in by
    returning an _IoUringBackend() here when available.  The submit()
    signature stays the same so no call site needs to change."""
    return _ThreadPoolBackend()


def _get_backend():
    global _backend
    if _backend is None:
        _backend = _select_backend()
    return _backend


def _after_fork_child():
    """Drop scheduler-derived state the child can't reuse after os.fork().

    fork() copies only the forking thread, so the thread-pool backend's
    workers are gone in the child and its self-pipe parkers are shared with
    the parent.  Null the backend so it is rebuilt on first use and drop the
    pooled parkers so the child never writes wake bytes into the parent's
    pipes.  Cheap and only runs on an actual fork."""
    global _backend
    _backend = None
    _Parker._pool = []


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_child)


def _blocking_call(fn, *args, **kwargs):
    """Run fn(*args, **kwargs) off-scheduler.  In a goroutine, dispatch
    to the backend (other goroutines keep running).  Outside a
    goroutine, call inline -- no dispatch overhead."""
    if not _in_goroutine():
        return fn(*args, **kwargs)
    return _get_backend().submit(fn, args, kwargs)


def offload(fn, *args, **kwargs):
    """Run a blocking callable on the backend thread pool, parking the current
    goroutine until it returns (run inline when not in a goroutine).

    The sanctioned escape hatch for blocking calls runloom cannot transparently
    make cooperative: buffered file .read()/.write() on slow media (io.FileIO /
    io.Buffered* are immutable C types and can't be patched), C-extension
    database drivers, CPU-bound hashing/compression, etc.

        data = runloom.monkey.offload(f.read, 65536)

    sqlite3 is NOT auto-patched: a Connection is thread-affine (it raises
    "SQLite objects created in a thread can only be used in that same thread"),
    and the pool runs the call on a worker thread.  To offload sqlite, open the
    connection with ``check_same_thread=False`` and keep your own access to it
    serialized -- one in-flight DB call at a time per connection::

        conn = sqlite3.connect(path, check_same_thread=False)
        rows = runloom.monkey.offload(conn.execute, sql, params).fetchall()
    """
    return _blocking_call(fn, *args, **kwargs)



def _co_sleep(seconds):
    """Cooperative sleep that dispatches to whichever scheduler is live.

    Inside the C scheduler (thread-local count > 0) call
    runloom_c.sched_sleep directly -- runloom.sleep there would route to
    the Python scheduler, see no current goroutine, and call time.sleep
    again (us), recursing.  Inside the Python scheduler use runloom.sleep.
    """
    if getattr(_g_state, "count", 0) > 0:
        runloom_c.sched_sleep(seconds)
    else:
        runloom.sleep(seconds)


# Re-export every name defined above (stdlib aliases, constants, helpers, the
# Parker and the backend) so a section module gets the whole foundation with a
# single `from ._base import *`.  Underscore names are included on purpose.
__all__ = [name for name in list(globals()) if not name.startswith("__")]

