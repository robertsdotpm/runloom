"""pygo monkey-patches for blocking Python APIs.

Replaces stdlib calls that would block the OS thread with versions that
park the current goroutine via pygo_core.wait_fd / pygo.sleep / a
self-pipe parker.  Other goroutines keep running while one is "blocked".

Apply once at startup:
    import pygo, pygo.monkey
    pygo.monkey.patch()                      # all categories
    pygo.monkey.patch(threading=False)       # opt out of one

Categories (all default True):
    socket       socket.socket recv/recv_into/send/sendall/accept/connect/
                 recvfrom/sendto  +  recvmsg/recvmsg_into/sendmsg (fd passing,
                 ancillary data) where the platform provides them
    time         time.sleep
    os           os.read / os.write -- wait_fd for pollable fds (pipes,
                 sockets, ttys), thread-pool offload for regular files
    select       select.select  (fast path for 1 fd; busy-poll otherwise)
    selectors    select.poll / select.epoll / select.kqueue made cooperative,
                 which transparently makes the high-level `selectors` module
                 (DefaultSelector / PollSelector / EpollSelector /
                 KqueueSelector) cooperative too -- this is what
                 subprocess.communicate(), socketserver, http.server, wsgiref
                 and most hand-rolled poll loops actually block on.  epoll /
                 kqueue wait on their own backing fd via wait_fd (event-driven,
                 no busy-poll); poll has no backing fd so it probe+yields.
    stdio        builtins.input  +  sys.stdin.read/readline
    ssl          ssl.SSLSocket recv/send/sendall/do_handshake
    subprocess   subprocess.Popen.wait  (and, via `selectors` + `os`,
                 subprocess.run / call / check_output / communicate)
    process      os.waitpid / os.wait / os.waitid (WNOHANG cooperative loop on
                 POSIX, backend offload on Windows) + os.system (offload)
    threading    Lock, RLock, Event, Condition, Semaphore, BoundedSemaphore
                 + Thread.join (cooperative is_alive() poll)
    queue        queue.SimpleQueue -> cooperative CoSimpleQueue.  queue.Queue
                 needs nothing extra: it builds on threading.Condition, which
                 is already cooperative once `threading` is patched.
    file         builtins.open (open syscall offloaded to backend)
    syscalls     os.stat/lstat/listdir/scandir/mkdir/rename/unlink/fsync/...
                 -- all disk-touching os.* calls dispatched to backend
    dns          pure-async UDP resolver (Go-netgo-style): parses
                 /etc/resolv.conf + /etc/hosts, sends queries via
                 cooperatively-patched UDP sockets, parallel A/AAAA,
                 60s result cache.  No threads.

Backend layer:
    The non-pollable I/O patches (file, syscalls, os.read/write on
    regular files) dispatch through pygo.monkey._get_backend(), which
    today returns a pre-started thread pool with self-pipe wakeup.
    Linux io_uring (5.6+) can slot in here without caller changes --
    backends only expose submit(fn, args, kwargs).

Limitations:
    * Designed for the C scheduler (pygo_core.go / pygo_core.run).  The
      pure-Python scheduler in pygo.runtime has no netpoll integration.
    * select.select with >1 fd, and select.poll(), are yield-backoff
      busy-polls (no backing fd to park on).  epoll/kqueue ARE event-driven
      (they park on their own fd).
    * Replacing threading.Lock etc. is best-effort coordination with real
      OS threads -- the single-thread cooperative model is the design target.
    * `queue.Queue` / `queue.SimpleQueue` and `selectors.*Selector` instances
      created before patch() keep the original (blocking) primitives; patch()
      early.  Buffered file .read()/.write() bypass os.read/write (see the
      file section) so they are not offloaded.

Platform notes:
    * Linux, macOS, *BSD: fully supported by the C-side netpoll (epoll,
      kqueue, select fallback).
    * Windows: the Python monkey-patch layer is Windows-aware -- Parker
      uses socket.socketpair() (the only thing Win select() will poll),
      subprocess.Popen.wait uses portable Popen.poll(), DNS falls back
      to libc getaddrinfo via the backend pool because Windows has no
      /etc/resolv.conf, hosts file resolves to
      %SystemRoot%\\System32\\drivers\\etc\\hosts.  The C extension itself
      still needs Windows support (IOCP backend) before any of this is
      usable end-to-end on Windows; that's separate from this module.
"""
import _thread
import builtins
import collections
import errno
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

# The pid that imported this module -- i.e. the process whose pygo scheduler
# our cooperative identity (pygo.current()) is valid in.  After os.fork() the
# child inherits a *copy* of the scheduler that is not actually running, so
# pygo.current() there returns a fresh, meaningless G on every call.  This is
# captured once and never reassigned, so a pid mismatch reliably means "we are
# in a forked child" regardless of os.register_at_fork handler ordering.  It is
# only ever consulted on cold error paths (see _is_forked_child), never in the
# I/O hot path -- os.getpid() costs ~1.3us, far too much per recv/send.
_PID_IMPORT = os.getpid()


def _is_forked_child():
    return os.getpid() != _PID_IMPORT

import pygo
import pygo_core

READ  = 1
WRITE = 2

# Snapshots of stdlib primitives that the patches themselves call.
# Capturing here (at module import time) protects us against the
# patched versions calling back into themselves.
_raw_os_read  = os.read
_raw_os_write = os.write
_raw_time_sleep = time.sleep
# Captured before _patch_socket installs the cooperative versions.  Parker
# uses these to talk to its self-pipe / self-socketpair without going
# through the cooperative wrappers (which would, for instance, park
# forever on a non-blocking recv that returns BlockingIOError).
_raw_sock_recv = socket.socket.recv
_raw_sock_send = socket.socket.send


# ---------- goroutine-context detection ----------
# pygo_core (C scheduler) does not expose a "current goroutine"
# accessor, so we wrap pygo_core.go / mn_go and bump a thread-local
# counter for the duration of every user callable.  The Python
# scheduler still uses pygo.current() (which works there).
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
    our pygo_core.go wrapper) and the Python scheduler (via
    pygo.current())."""
    if getattr(_g_state, "count", 0) > 0:
        return True
    try:
        return pygo.current() is not None
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

    def __init__(self):
        if _Parker._pool:
            self.r, self.w, self._sockets = _Parker._pool.pop()
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

    def park(self):
        pygo_core.wait_fd(self.r, READ)
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
        if len(_Parker._pool) < 64:
            _Parker._pool.append((self.r, self.w, self._sockets))
        else:
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


# ============================================================
# socket
# ============================================================
_orig_recv = None
_orig_recv_into = None
_orig_send = None
_orig_sendall = None
_orig_accept = None
_orig_connect = None
_orig_recvfrom = None
_orig_sendto = None
_orig_recvmsg = None
_orig_recvmsg_into = None
_orig_sendmsg = None

# recvmsg / recvmsg_into / sendmsg are POSIX-only (fd passing, ancillary
# data via SCM_RIGHTS).  Windows sockets have no equivalent, so socket.socket
# simply lacks the attributes there; the patch is skipped.
_HAVE_RECVMSG = hasattr(socket.socket, "recvmsg")
_HAVE_SENDMSG = hasattr(socket.socket, "sendmsg")


_tcp_recv_alloc = getattr(pygo_core, "tcp_recv_alloc", None)
_tcp_recv       = getattr(pygo_core, "tcp_recv", None)
_tcp_send_once  = getattr(pygo_core, "tcp_send_once", None)
_tcp_send_all   = getattr(pygo_core, "tcp_send", None)


def _patched_recv(self, bufsize, flags=0):
    """Cooperative recv.  Routes to the C primitive when available
    (saves the BlockingIOError raise/catch on every EAGAIN plus the
    Python frame around _orig_recv), falls back to the old loop
    otherwise.  Outside a goroutine, falls through to the raw
    blocking recv so non-goroutine threads (e.g. helper threads in
    tests / fixtures) still work after monkey.patch()."""
    if not _in_goroutine():
        return _orig_recv(self, bufsize, flags)
    _make_nonblocking(self)
    if _tcp_recv_alloc is not None:
        return _tcp_recv_alloc(self.fileno(), bufsize, flags)
    while True:
        try:
            return _orig_recv(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            r = pygo_core.wait_fd(self.fileno(), READ)
            if r == 0:
                raise socket.timeout("recv timed out")


def _patched_recv_into(self, buffer, nbytes=0, flags=0):
    """recv_into avoids the bytes-object allocation that recv() does
    every call.  Callers that already own a buffer (high-throughput
    proxies, line readers, framing layers) save one heap allocation
    and one memcpy per recv -- typically 10-20 us / call at 4 KB."""
    if not _in_goroutine():
        return _orig_recv_into(self, buffer, nbytes, flags)
    _make_nonblocking(self)
    if _tcp_recv is not None:
        n = nbytes if nbytes else len(buffer)
        return _tcp_recv(self.fileno(), buffer, n, flags)
    while True:
        try:
            return _orig_recv_into(self, buffer, nbytes, flags)
        except (BlockingIOError, InterruptedError):
            r = pygo_core.wait_fd(self.fileno(), READ)
            if r == 0:
                raise socket.timeout("recv_into timed out")


def _patched_send(self, data, flags=0):
    if not _in_goroutine():
        return _orig_send(self, data, flags)
    _make_nonblocking(self)
    if _tcp_send_once is not None:
        return _tcp_send_once(self.fileno(), data, flags)
    while True:
        try:
            return _orig_send(self, data, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_sendall(self, data, flags=0):
    if not _in_goroutine():
        return _orig_sendall(self, data, flags)
    _make_nonblocking(self)
    if _tcp_send_all is not None:
        _tcp_send_all(self.fileno(), data, flags)
        return None
    view = data if isinstance(data, memoryview) else memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            n = _orig_send(self, view[sent:], flags)
            if n:
                sent += n
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_accept(self):
    if not _in_goroutine():
        return _orig_accept(self)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_accept(self)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_connect(self, address):
    if not _in_goroutine():
        return _orig_connect(self, address)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_connect(self, address)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)
        except OSError as e:
            if e.errno == errno.EISCONN:
                return
            if e.errno in (errno.EINPROGRESS, errno.EALREADY):
                pygo_core.wait_fd(self.fileno(), WRITE)
                continue
            raise


def _patched_recvfrom(self, bufsize, flags=0):
    if not _in_goroutine():
        return _orig_recvfrom(self, bufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvfrom(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_sendto(self, data, *args):
    if not _in_goroutine():
        return _orig_sendto(self, data, *args)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendto(self, data, *args)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_recvmsg(self, bufsize, ancbufsize=0, flags=0):
    """Cooperative recvmsg.  Same EAGAIN -> wait_fd loop as recv, but
    carries the ancillary-data tuple (data, ancdata, msg_flags, address)
    that SCM_RIGHTS fd-passing and IP_PKTINFO callers rely on."""
    if not _in_goroutine():
        return _orig_recvmsg(self, bufsize, ancbufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvmsg(self, bufsize, ancbufsize, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_recvmsg_into(self, buffers, ancbufsize=0, flags=0):
    if not _in_goroutine():
        return _orig_recvmsg_into(self, buffers, ancbufsize, flags)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvmsg_into(self, buffers, ancbufsize, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_sendmsg(self, buffers, ancdata=(), flags=0, address=None):
    if not _in_goroutine():
        return _orig_sendmsg(self, buffers, ancdata, flags, address)
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendmsg(self, buffers, ancdata, flags, address)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


_orig_close   = None
_orig_detach  = None
_netpoll_unregister = getattr(pygo_core, "netpoll_unregister", None)


def _patched_close(self):
    """Clear the netpoll registration bit before closing so an fd
    reuse re-registers cleanly under the ET register-once scheme."""
    if _netpoll_unregister is not None:
        try:
            fd = self.fileno()
            if fd >= 0:
                _netpoll_unregister(fd)
        except (OSError, ValueError):
            pass
    return _orig_close(self)


def _patched_detach(self):
    """Same bitmap clear as close: the fd is leaving our control."""
    if _netpoll_unregister is not None:
        try:
            fd = self.fileno()
            if fd >= 0:
                _netpoll_unregister(fd)
        except (OSError, ValueError):
            pass
    return _orig_detach(self)


def _patch_socket():
    global _orig_recv, _orig_recv_into, _orig_send, _orig_sendall, _orig_accept
    global _orig_connect, _orig_recvfrom, _orig_sendto, _orig_close, _orig_detach
    global _orig_recvmsg, _orig_recvmsg_into, _orig_sendmsg
    s = socket.socket
    _orig_recv      = s.recv
    _orig_recv_into = s.recv_into
    _orig_send      = s.send
    _orig_sendall   = s.sendall
    _orig_accept    = s.accept
    _orig_connect   = s.connect
    _orig_recvfrom  = s.recvfrom
    _orig_sendto    = s.sendto
    _orig_close     = s.close
    _orig_detach    = s.detach
    s.recv      = _patched_recv
    s.recv_into = _patched_recv_into
    s.send      = _patched_send
    s.sendall   = _patched_sendall
    s.accept    = _patched_accept
    s.connect   = _patched_connect
    s.recvfrom  = _patched_recvfrom
    s.sendto    = _patched_sendto
    s.close     = _patched_close
    s.detach    = _patched_detach
    if _HAVE_RECVMSG:
        _orig_recvmsg      = s.recvmsg
        _orig_recvmsg_into = s.recvmsg_into
        s.recvmsg      = _patched_recvmsg
        s.recvmsg_into = _patched_recvmsg_into
    if _HAVE_SENDMSG:
        _orig_sendmsg = s.sendmsg
        s.sendmsg     = _patched_sendmsg


def _unpatch_socket():
    s = socket.socket
    s.recv      = _orig_recv
    s.recv_into = _orig_recv_into
    s.send      = _orig_send
    s.sendall   = _orig_sendall
    s.accept    = _orig_accept
    s.connect   = _orig_connect
    s.recvfrom  = _orig_recvfrom
    s.sendto    = _orig_sendto
    s.close     = _orig_close
    s.detach    = _orig_detach
    if _HAVE_RECVMSG:
        s.recvmsg      = _orig_recvmsg
        s.recvmsg_into = _orig_recvmsg_into
    if _HAVE_SENDMSG:
        s.sendmsg = _orig_sendmsg


# ============================================================
# time
# ============================================================
_orig_time_sleep = None


def _co_sleep(seconds):
    """Cooperative sleep that dispatches to whichever scheduler is live.

    Inside the C scheduler (thread-local count > 0) call
    pygo_core.sched_sleep directly -- pygo.sleep there would route to
    the Python scheduler, see no current goroutine, and call time.sleep
    again (us), recursing.  Inside the Python scheduler use pygo.sleep.
    """
    if getattr(_g_state, "count", 0) > 0:
        pygo_core.sched_sleep(seconds)
    else:
        pygo.sleep(seconds)


def _patched_time_sleep(seconds):
    if _in_goroutine():
        _co_sleep(seconds)
    else:
        _orig_time_sleep(seconds)


def _patch_time():
    global _orig_time_sleep
    _orig_time_sleep = time.sleep
    time.sleep = _patched_time_sleep


def _unpatch_time():
    time.sleep = _orig_time_sleep


# ============================================================
# os.read / os.write
# ============================================================
_orig_os_read = None
_orig_os_write = None


def _patched_os_read(fd, n):
    if not _in_goroutine():
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
                pygo_core.wait_fd(fd, READ)
    # Regular file / non-pollable -- offload to backend pool.
    return _blocking_call(_orig_os_read, fd, n)


def _patched_os_write(fd, data):
    if not _in_goroutine():
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
                pygo_core.wait_fd(fd, WRITE)
    return _blocking_call(_orig_os_write, fd, data)


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
    _orig_os_read  = os.read
    _orig_os_write = os.write
    _orig_os_close = os.close
    os.read  = _patched_os_read
    os.write = _patched_os_write
    os.close = _patched_os_close


def _unpatch_os():
    os.read  = _orig_os_read
    os.write = _orig_os_write
    os.close = _orig_os_close


# ============================================================
# select.select
# ============================================================
_orig_select_select = None


def _fd_of(x):
    return x.fileno() if hasattr(x, "fileno") else int(x)


def _patched_select(rlist, wlist, xlist, timeout=None):
    if not _in_goroutine():
        return _orig_select_select(rlist, wlist, xlist, timeout)

    # On Windows, only SOCKET handles can be polled.  If any fd in the
    # request isn't a socket, fall back to the OS select (which will
    # itself reject non-sockets -- same behaviour as outside a
    # goroutine, so the caller sees a consistent error path).  This
    # avoids parking forever on wait_fd for a pipe/file fd that the
    # netpoll backend can't drive.
    if _IS_WINDOWS:
        for fd_obj in list(rlist) + list(wlist) + list(xlist):
            fd = _fd_of(fd_obj)
            try:
                os.fstat(fd)
                # fstat on a socket fd raises on Windows; if it
                # succeeds, the fd is NOT a socket.
                return _orig_select_select(rlist, wlist, xlist, timeout)
            except OSError:
                pass  # likely a socket -- continue with wait_fd path

    n = len(rlist) + len(wlist)
    # Fast path: one fd, no xlist -> map to wait_fd directly.
    if n == 1 and not xlist:
        if rlist:
            fd, events, src = _fd_of(rlist[0]), READ, "r"
            obj = rlist[0]
        else:
            fd, events, src = _fd_of(wlist[0]), WRITE, "w"
            obj = wlist[0]
        timeout_ms = -1 if timeout is None else max(0, int(timeout * 1000))
        try:
            ready = pygo_core.wait_fd(fd, events, timeout_ms)
        except OSError:
            return _orig_select_select(rlist, wlist, xlist, timeout)
        if ready == 0:
            return [], [], []
        return ([obj], [], []) if src == "r" else ([], [obj], [])

    # Multi-fd: short non-blocking selects + yields.  Not free, but
    # makes select.select() cooperative enough for stdlib internals.
    deadline = None if timeout is None else time.monotonic() + timeout
    step = 0.001
    while True:
        try:
            r, w, x = _orig_select_select(rlist, wlist, xlist, 0)
        except (OSError, ValueError):
            return _orig_select_select(rlist, wlist, xlist, timeout)
        if r or w or x:
            return r, w, x
        if deadline is not None:
            now = time.monotonic()
            if now >= deadline:
                return [], [], []
            _co_sleep(min(step, deadline - now))
        else:
            _co_sleep(step)


def _patch_select():
    global _orig_select_select
    _orig_select_select = _select_mod.select
    _select_mod.select = _patched_select


def _unpatch_select():
    _select_mod.select = _orig_select_select


# ============================================================
# selectors -- cooperative select.poll / select.epoll / select.kqueue
#
# The high-level `selectors` module is what modern stdlib actually blocks
# on: selectors.DefaultSelector is EpollSelector on Linux, KqueueSelector
# on *BSD/macOS, and subprocess.communicate() uses PollSelector.  None of
# those route through select.select (only SelectSelector does, and that is
# already covered by the `select` category).  Each of the others builds its
# backing object by calling select.poll() / select.epoll() / select.kqueue()
# at instantiation time -- looked up dynamically on the `select` module --
# so replacing those three factories with cooperative wrappers makes the
# whole `selectors` module cooperative for free, and also covers code that
# uses select.poll/epoll/kqueue directly (socketserver, asyncore-style
# loops, hand-rolled poll loops).
#
# Strategy per primitive:
#   epoll / kqueue: the object owns a real kernel fd (fileno()), and an
#     epoll/kqueue fd is itself pollable -- it signals readable exactly when
#     it has >=1 ready event.  So we park on wait_fd(self.fileno(), READ)
#     and then drain with a non-blocking poll(0).  Fully event-driven, no
#     busy-poll, no goroutine fan-out, no leaked parkers.
#   poll: select.poll has no backing fd, so we fall back to a non-blocking
#     poll(0) + cooperative yield loop (same shape as multi-fd select.select).
#
# Outside a goroutine every wrapper degrades to the real blocking call, so
# helper threads keep working after patch().
# ============================================================
_real_select_poll  = getattr(_select_mod, "poll", None)
_real_select_epoll = getattr(_select_mod, "epoll", None)
_real_select_kqueue = getattr(_select_mod, "kqueue", None)

# Cap on how long a single cooperative wait blocks before re-probing, so a
# wait that was registered before its fd became ready (or a level-triggered
# edge we already drained) can never wedge longer than this.  The epoll/
# kqueue fd readiness wakes us immediately in the common case; this is only
# the backstop.
_SELECTOR_REPROBE_S = 0.05


def _backing_fd_wait(real_obj, deadline):
    """Park on an epoll/kqueue object's own fd until it has ready events
    (or the per-iteration re-probe cap elapses, or the caller deadline is
    hit).  Returns False if the caller deadline has already passed."""
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        timeout_ms = max(1, int(min(_SELECTOR_REPROBE_S, remaining) * 1000))
    else:
        timeout_ms = int(_SELECTOR_REPROBE_S * 1000)
    try:
        pygo_core.wait_fd(real_obj.fileno(), READ, timeout_ms)
    except OSError:
        # fileno() gone (closed under us) or netpoll refused it -- let the
        # caller re-probe with poll(0), which will surface the real error.
        pass
    return True


class CoPoll(object):
    """Cooperative wrapper around select.poll().

    poll objects have no kernel fd of their own, so this is a non-blocking
    poll(0) + yield loop -- the same proven shape as the multi-fd
    select.select() path.  register/modify/unregister forward to the real
    object via __getattr__."""
    __slots__ = ("_r",)

    def __init__(self):
        self._r = _real_select_poll()

    def poll(self, timeout=None):
        # poll() timeout is in MILLISECONDS; None or negative == infinite.
        if not _in_goroutine():
            return self._r.poll(timeout)
        if timeout is None or timeout < 0:
            deadline = None
        else:
            deadline = time.monotonic() + timeout / 1000.0
        step = 0.0005
        while True:
            ev = self._r.poll(0)
            if ev:
                return ev
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return []
                _co_sleep(min(step, remaining))
            else:
                _co_sleep(step)
            if step < 0.02:
                step *= 2

    def __getattr__(self, name):
        return getattr(self._r, name)


class CoEpoll(object):
    """Cooperative wrapper around select.epoll(): parks on the epoll fd."""
    __slots__ = ("_r",)

    def __init__(self, sizehint=-1, flags=0):
        self._r = _real_select_epoll(sizehint, flags)

    @classmethod
    def fromfd(cls, fd):
        self = object.__new__(cls)
        self._r = _real_select_epoll.fromfd(fd)
        return self

    def poll(self, timeout=None, maxevents=-1):
        # epoll.poll() timeout is in SECONDS (float); None or negative ==
        # infinite.  maxevents -1 == unlimited.
        if not _in_goroutine():
            return self._r.poll(timeout, maxevents)
        if timeout is None or timeout < 0:
            deadline = None
        else:
            deadline = time.monotonic() + timeout
        while True:
            ev = self._r.poll(0, maxevents)
            if ev:
                return ev
            if not _backing_fd_wait(self._r, deadline):
                return []

    def fileno(self):
        return self._r.fileno()

    def close(self):
        # The epoll fd is about to disappear; drop its netpoll registration
        # so a later fd reuse re-registers cleanly (epoll.close() goes
        # straight to the C close, bypassing our patched os.close).
        if _netpoll_unregister is not None:
            try:
                fd = self._r.fileno()
                if fd >= 0:
                    _netpoll_unregister(fd)
            except (OSError, ValueError):
                pass
        return self._r.close()

    @property
    def closed(self):
        return self._r.closed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def __getattr__(self, name):
        return getattr(self._r, name)


class CoKqueue(object):
    """Cooperative wrapper around select.kqueue(): parks on the kqueue fd.

    kqueue.control(changelist, max_events, timeout) both applies changes
    and retrieves events.  We split it: apply the changelist with a
    register-only control (max_events=0), then park on the kqueue fd and
    drain with a non-blocking control."""
    __slots__ = ("_r",)

    def __init__(self):
        self._r = _real_select_kqueue()

    @classmethod
    def fromfd(cls, fd):
        self = object.__new__(cls)
        self._r = _real_select_kqueue.fromfd(fd)
        return self

    def control(self, changelist, max_events, timeout=None):
        # max_events == 0 means register-only (never waits) -- pass through.
        if not _in_goroutine() or not max_events:
            return self._r.control(changelist, max_events, timeout)
        if changelist:
            self._r.control(changelist, 0)          # apply, retrieve nothing
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            ev = self._r.control(None, max_events, 0)
            if ev:
                return ev
            if not _backing_fd_wait(self._r, deadline):
                return []

    def fileno(self):
        return self._r.fileno()

    def close(self):
        if _netpoll_unregister is not None:
            try:
                fd = self._r.fileno()
                if fd >= 0:
                    _netpoll_unregister(fd)
            except (OSError, ValueError):
                pass
        return self._r.close()

    @property
    def closed(self):
        return self._r.closed

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()

    def __getattr__(self, name):
        return getattr(self._r, name)


import selectors as _selectors_mod

# selectors.PollSelector / EpollSelector / KqueueSelector each capture their
# backing factory as a *class attribute* (_selector_cls) at import time, so
# replacing select.poll/epoll/kqueue is not enough on its own -- the already
# imported selector classes have to be flipped too.  We do both: the select.*
# factories for code that uses select.poll()/epoll()/kqueue() directly, and
# the selectors._selector_cls attributes for code that goes through the
# high-level `selectors` module (subprocess, socketserver, http.server, ...).
_orig_selector_cls = {}    # selectors class name -> original _selector_cls

_SELECTORS_BINDINGS = (
    ("PollSelector",   CoPoll,   _real_select_poll),
    ("EpollSelector",  CoEpoll,  _real_select_epoll),
    ("KqueueSelector", CoKqueue, _real_select_kqueue),
)


def _patch_selectors():
    # Only patch what the platform actually provides: epoll is Linux-only,
    # kqueue is *BSD/macOS-only, poll is most POSIX.
    if _real_select_poll is not None:
        _select_mod.poll = CoPoll
    if _real_select_epoll is not None:
        _select_mod.epoll = CoEpoll
    if _real_select_kqueue is not None:
        _select_mod.kqueue = CoKqueue
    for clsname, co_cls, real_factory in _SELECTORS_BINDINGS:
        if real_factory is None:
            continue
        sel_cls = getattr(_selectors_mod, clsname, None)
        if sel_cls is None or not hasattr(sel_cls, "_selector_cls"):
            continue
        _orig_selector_cls[clsname] = sel_cls._selector_cls
        sel_cls._selector_cls = co_cls


def _unpatch_selectors():
    if _real_select_poll is not None:
        _select_mod.poll = _real_select_poll
    if _real_select_epoll is not None:
        _select_mod.epoll = _real_select_epoll
    if _real_select_kqueue is not None:
        _select_mod.kqueue = _real_select_kqueue
    for clsname, orig in list(_orig_selector_cls.items()):
        sel_cls = getattr(_selectors_mod, clsname, None)
        if sel_cls is not None:
            sel_cls._selector_cls = orig
    _orig_selector_cls.clear()


# ============================================================
# stdio (input, sys.stdin)
# ============================================================
_orig_input = None
_orig_stdin_read = None
_orig_stdin_readline = None


def _patched_input(prompt=""):
    if not _in_goroutine():
        return _orig_input(prompt)
    if prompt:
        sys.stdout.write(prompt)
        sys.stdout.flush()
    try:
        fd = sys.stdin.fileno()
    except (OSError, ValueError):
        return _orig_input("")
    if _fd_pollable(fd):
        pygo_core.wait_fd(fd, READ)
    return _orig_input("")


def _patched_stdin_read(*args):
    if _in_goroutine():
        try:
            fd = sys.stdin.fileno()
            if _fd_pollable(fd):
                pygo_core.wait_fd(fd, READ)
        except (OSError, ValueError):
            pass
    return _orig_stdin_read(*args)


def _patched_stdin_readline(*args):
    if _in_goroutine():
        try:
            fd = sys.stdin.fileno()
            if _fd_pollable(fd):
                pygo_core.wait_fd(fd, READ)
        except (OSError, ValueError):
            pass
    return _orig_stdin_readline(*args)


def _patch_stdio():
    global _orig_input, _orig_stdin_read, _orig_stdin_readline
    _orig_input = builtins.input
    builtins.input = _patched_input
    # sys.stdin is a TextIOWrapper -- wrapping its methods works because
    # the underlying buffered/raw reads still drain the kernel buffer
    # after wait_fd; wait_fd just keeps us off the OS-blocking syscall.
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
# ssl
# ============================================================
_orig_ssl_recv = None
_orig_ssl_recv_into = None
_orig_ssl_send = None
_orig_ssl_sendall = None
_orig_ssl_do_handshake = None
_orig_ssl_unwrap = None


def _ssl_wait(self, want):
    if want is _ssl_mod.SSLWantReadError:
        pygo_core.wait_fd(self.fileno(), READ)
    else:
        pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_ssl_recv(self, buflen=1024, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_recv(self, buflen, flags)
        except _ssl_mod.SSLWantReadError:
            pygo_core.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_ssl_recv_into(self, buffer, nbytes=None, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            if nbytes is None:
                return _orig_ssl_recv_into(self, buffer, flags=flags)
            return _orig_ssl_recv_into(self, buffer, nbytes, flags)
        except _ssl_mod.SSLWantReadError:
            pygo_core.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_ssl_send(self, data, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_send(self, data, flags)
        except _ssl_mod.SSLWantReadError:
            pygo_core.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_ssl_sendall(self, data, flags=0):
    _make_nonblocking(self)
    view = data if isinstance(data, memoryview) else memoryview(data)
    sent = 0
    while sent < len(view):
        try:
            n = _orig_ssl_send(self, view[sent:], flags)
            if n:
                sent += n
        except _ssl_mod.SSLWantReadError:
            pygo_core.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_ssl_do_handshake(self, *a, **kw):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_ssl_do_handshake(self, *a, **kw)
        except _ssl_mod.SSLWantReadError:
            pygo_core.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_ssl_unwrap(self):
    while True:
        try:
            return _orig_ssl_unwrap(self)
        except _ssl_mod.SSLWantReadError:
            pygo_core.wait_fd(self.fileno(), READ)
        except _ssl_mod.SSLWantWriteError:
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patch_ssl():
    global _orig_ssl_recv, _orig_ssl_recv_into, _orig_ssl_send
    global _orig_ssl_sendall, _orig_ssl_do_handshake, _orig_ssl_unwrap
    S = _ssl_mod.SSLSocket
    _orig_ssl_recv         = S.recv
    _orig_ssl_recv_into    = S.recv_into
    _orig_ssl_send         = S.send
    _orig_ssl_sendall      = S.sendall
    _orig_ssl_do_handshake = S.do_handshake
    _orig_ssl_unwrap       = S.unwrap
    S.recv         = _patched_ssl_recv
    S.recv_into    = _patched_ssl_recv_into
    S.send         = _patched_ssl_send
    S.sendall      = _patched_ssl_sendall
    S.do_handshake = _patched_ssl_do_handshake
    S.unwrap       = _patched_ssl_unwrap


def _unpatch_ssl():
    S = _ssl_mod.SSLSocket
    S.recv         = _orig_ssl_recv
    S.recv_into    = _orig_ssl_recv_into
    S.send         = _orig_ssl_send
    S.sendall      = _orig_ssl_sendall
    S.do_handshake = _orig_ssl_do_handshake
    S.unwrap       = _orig_ssl_unwrap


# ============================================================
# subprocess
# ============================================================
_orig_popen_wait = None


def _patched_popen_wait(self, timeout=None):
    if not _in_goroutine() or self.returncode is not None:
        return _orig_popen_wait(self, timeout)
    # Popen.poll() is portable (Windows: WaitForSingleObject with 0ms;
    # POSIX: waitpid(WNOHANG)) and updates self.returncode + the
    # internal handle state atomically.  Calling it in a sleep loop is
    # cooperatively safe -- _co_sleep yields to other goroutines.
    deadline = None if timeout is None else time.monotonic() + timeout
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
    step = 0.0005
    while True:
        # WNOHANG returns (0, 0) when the requested child has not yet
        # changed state; ECHILD (no such child) propagates as it should.
        r = _orig_os_waitpid(pid, options | os.WNOHANG)
        if r[0] != 0:
            return r
        _co_sleep(step)
        if step < 0.02:
            step *= 2


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
    step = 0.0005
    while True:
        # waitid with WNOHANG returns None when no child has changed state.
        r = _orig_os_waitid(idtype, id, options | os.WNOHANG)
        if r is not None:
            return r
        _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patched_os_system(command):
    # No non-blocking form; run it on the backend pool so the goroutine
    # parks instead of freezing the scheduler for the child's lifetime.
    return _blocking_call(_orig_os_system, command)


def _patch_process():
    global _orig_os_waitpid, _orig_os_wait, _orig_os_waitid, _orig_os_system
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
    if _orig_os_system is not None:
        os.system = _orig_os_system


# ============================================================
# threading -- cooperative Lock / RLock / Event / Condition / Semaphore
# ============================================================
_real_Lock      = _th.Lock
_real_RLock     = _th.RLock
_real_Event     = _th.Event
_real_Condition = _th.Condition
_real_Semaphore = _th.Semaphore
_real_BoundedSemaphore = _th.BoundedSemaphore
_real_get_ident = _th.get_ident
# Low-level _thread primitives.  stdlib internals capture these DIRECTLY
# (e.g. tempfile: `from _thread import allocate_lock`), bypassing threading.*,
# so patching only threading.Lock leaves those internal locks real -- and a
# real lock held by one goroutine across a yielding (offloaded) call while
# another goroutine blocks on it FREEZES the single scheduler thread (deadlock).
_real_allocate_lock = _thread.allocate_lock
_real_thread_RLock  = getattr(_thread, "RLock", None)


class CoLock(object):
    """Cooperative mutex.  Non-reentrant.

    When called from a goroutine: park on a parker queue under contention.
    When called from outside any goroutine: degrade to immediate
    acquire/release (the single-thread cooperative model never has true
    cross-thread contention between goroutines).
    """
    # __weakref__: stdlib _thread.lock is weakref-able; without this slot a
    # weakref.ref(threading.Lock()) raises TypeError under monkey (found by the
    # verbatim CPython lock_tests).
    __slots__ = ("_locked", "_owner", "_waiters", "__weakref__")

    def __init__(self):
        self._locked  = False
        self._owner   = None
        self._waiters = collections.deque()

    def acquire(self, blocking=True, timeout=-1):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        if not self._locked:
            self._locked = True
            self._owner  = cur
            return True
        if not blocking:
            return False
        if not _in_goroutine():
            # No goroutine to park; spin briefly + yield to the OS.
            t0 = time.monotonic()
            while self._locked:
                _raw_time_sleep(0.0001)
                if timeout is not None and timeout >= 0:
                    if time.monotonic() - t0 >= timeout:
                        return False
            self._locked = True
            self._owner  = cur
            return True
        p = _Parker()
        # _Parker() may yield during socketpair creation (Windows path).
        # If the lock got freed while we yielded, claim it now instead
        # of parking forever -- nobody will unpark us, because nobody
        # saw us in self._waiters when they released.
        if not self._locked:
            self._locked = True
            self._owner  = cur
            p.release()
            return True
        self._waiters.append(p)
        p.park()
        p.release()
        self._owner = cur
        return True

    def release(self):
        if not self._locked:
            # A forked child running stdlib teardown (threading._after_fork)
            # can reach a release whose matching acquire happened in the
            # parent / under an identity that no longer exists here.  That is
            # benign post-fork noise, not a real double-release, so swallow
            # it; a genuine in-process double-release still raises.
            if _is_forked_child():
                return
            raise RuntimeError("release unlocked lock")
        self._owner = None
        if self._waiters:
            # Ownership transfers to next waiter; keep _locked True.
            p = self._waiters.popleft()
            p.unpark()
        else:
            self._locked = False

    def locked(self):
        return self._locked

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()


class CoRLock(object):
    """Cooperative re-entrant lock."""
    __slots__ = ("_lock", "_owner", "_count", "__weakref__")

    def __init__(self):
        self._lock  = CoLock()
        self._owner = None
        self._count = 0

    def acquire(self, blocking=True, timeout=-1):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        # `self._owner is not None` guard: a fresh/released lock has owner
        # None, and a caller whose identity is also None (e.g. the forked
        # child running threading._after_fork, where no scheduler is live so
        # pygo.current() is None) must NOT be mistaken for the owner -- that
        # would skip the real inner acquire and then blow up on release with
        # "release unlocked lock".
        if self._owner is not None and self._owner == cur:
            self._count += 1
            return True
        ok = self._lock.acquire(blocking, timeout)
        if ok:
            self._owner = cur
            self._count = 1
        return ok

    def release(self):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        if self._owner != cur:
            # In a forked child pygo.current() returns a different G each
            # call, so a legitimate acquire/release pair (e.g. the CoRLock
            # threading._after_fork builds for _active_limbo_lock) looks like
            # a foreign release.  Detect the fork and reset instead of raising.
            if _is_forked_child():
                self._count = 0
                self._owner = None
                if self._lock.locked():
                    self._lock.release()
                return
            raise RuntimeError("cannot release un-acquired lock")
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()


class CoEvent(object):
    __slots__ = ("_flag", "_waiters", "__weakref__")

    def __init__(self):
        self._flag    = False
        self._waiters = collections.deque()

    def is_set(self):
        return self._flag
    isSet = is_set

    def set(self):
        if self._flag:
            return
        self._flag = True
        waiters, self._waiters = list(self._waiters), collections.deque()
        for p in waiters:
            p.unpark()

    def clear(self):
        self._flag = False

    def wait(self, timeout=None):
        if self._flag:
            return True
        if not _in_goroutine():
            # No cooperative scheduler to wake us; degrade to spin.
            t0 = time.monotonic()
            while not self._flag:
                _raw_time_sleep(0.001)
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    return self._flag
            return True
        p = _Parker()
        self._waiters.append(p)
        if timeout is None:
            p.park()
        else:
            # Race park against a wakeup timer.
            deadline = time.monotonic() + timeout
            done = [False]
            def waker(parker=p, dl=deadline):
                while not done[0]:
                    remaining = dl - time.monotonic()
                    if remaining <= 0:
                        parker.unpark()
                        return
                    _co_sleep(min(remaining, 0.05))
            pygo_core.go(waker)
            p.park()
            done[0] = True
        p.release()
        return self._flag


class CoCondition(object):
    """Cooperative Condition variable, lock-pattern-compatible with
    threading.Condition."""

    def __init__(self, lock=None):
        self._lock    = lock if lock is not None else CoLock()
        self._waiters = collections.deque()

    def acquire(self, *a, **kw):
        return self._lock.acquire(*a, **kw)
    def release(self):
        self._lock.release()
    def __enter__(self):
        return self._lock.__enter__()
    def __exit__(self, *a):
        return self._lock.__exit__(*a)

    def wait(self, timeout=None):
        p = _Parker()
        self._waiters.append(p)
        # Release inner lock while parked.
        owned_recursion = None
        if isinstance(self._lock, CoRLock):
            owned_recursion = self._lock._count
            self._lock._count = 1   # release() pops once; ok
        self._lock.release()
        if timeout is None:
            p.park()
            timed_out = False
        else:
            deadline = time.monotonic() + timeout
            done = [False]
            def waker(parker=p, dl=deadline):
                while not done[0]:
                    remaining = dl - time.monotonic()
                    if remaining <= 0:
                        parker.unpark()
                        return
                    _co_sleep(min(remaining, 0.05))
            if _in_goroutine():
                pygo_core.go(waker)
            p.park()
            done[0] = True
            timed_out = time.monotonic() >= deadline
        p.release()
        self._lock.acquire()
        if owned_recursion is not None:
            self._lock._count = owned_recursion
        return not timed_out

    def wait_for(self, predicate, timeout=None):
        endtime = None if timeout is None else time.monotonic() + timeout
        result = predicate()
        while not result:
            if endtime is not None:
                remaining = endtime - time.monotonic()
                if remaining <= 0:
                    break
                self.wait(remaining)
            else:
                self.wait()
            result = predicate()
        return result

    def notify(self, n=1):
        for _ in range(n):
            if not self._waiters:
                return
            p = self._waiters.popleft()
            p.unpark()

    def notify_all(self):
        waiters, self._waiters = list(self._waiters), collections.deque()
        for p in waiters:
            p.unpark()
    notifyAll = notify_all


class CoSemaphore(object):
    __slots__ = ("_value", "_waiters", "__weakref__")

    def __init__(self, value=1):
        if value < 0:
            raise ValueError("semaphore initial value must be >= 0")
        self._value   = value
        self._waiters = collections.deque()

    def acquire(self, blocking=True, timeout=None):
        if self._value > 0:
            self._value -= 1
            return True
        if not blocking:
            return False
        if not _in_goroutine():
            t0 = time.monotonic()
            while self._value == 0:
                _raw_time_sleep(0.0001)
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    return False
            self._value -= 1
            return True
        p = _Parker()
        # Same re-check as CoLock: _Parker() may yield during socketpair
        # creation, and self._value can have transitioned to > 0 in the
        # gap.  Claim the permit directly if so.
        if self._value > 0:
            self._value -= 1
            p.release()
            return True
        self._waiters.append(p)
        p.park()
        p.release()
        # release() of the producer transferred a permit to us.
        return True

    def release(self, n=1):
        for _ in range(n):
            if self._waiters:
                p = self._waiters.popleft()
                p.unpark()
            else:
                self._value += 1

    def __enter__(self):
        self.acquire(); return self
    def __exit__(self, *a):
        self.release()


class CoBoundedSemaphore(CoSemaphore):
    __slots__ = ("_initial",)

    def __init__(self, value=1):
        CoSemaphore.__init__(self, value)
        self._initial = value

    def release(self, n=1):
        if self._value + len(self._waiters) + n > self._initial:
            raise ValueError("Semaphore released too many times")
        CoSemaphore.release(self, n)


_orig_thread_join = None


def _patched_thread_join(self, timeout=None):
    """Cooperative Thread.join: a goroutine joining a real OS thread polls
    is_alive() in a _co_sleep loop instead of parking the scheduler on the
    thread's C-level _tstate_lock (which only the dying thread can release,
    so a plain join would freeze every other goroutine until it does).

    Outside a goroutine, the real blocking join is used.  The not-started /
    join-self guards mirror threading.Thread.join so error semantics match."""
    if not self._initialized:
        raise RuntimeError("Thread.__init__() not called")
    if not _in_goroutine():
        return _orig_thread_join(self, timeout)
    if not self._started.is_set():
        raise RuntimeError("cannot join thread before it is started")
    if self is _th.current_thread():
        raise RuntimeError("cannot join current thread")
    deadline = None if timeout is None else time.monotonic() + timeout
    step = 0.0005
    while self.is_alive():
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            _co_sleep(min(step, remaining))
        else:
            _co_sleep(step)
        if step < 0.02:
            step *= 2


def _patch_threading():
    global _orig_thread_join
    _th.Lock      = CoLock
    _th.RLock     = CoRLock
    _th.Event     = CoEvent
    _th.Condition = CoCondition
    _th.Semaphore = CoSemaphore
    _th.BoundedSemaphore = CoBoundedSemaphore
    # Also patch the low-level _thread factories that stdlib internals grab
    # directly (tempfile._once_lock, etc.).  CoLock() is call-compatible with
    # allocate_lock() and degrades to an immediate acquire off-goroutine.
    _thread.allocate_lock = CoLock
    if _real_thread_RLock is not None:
        _thread.RLock = CoRLock
    _orig_thread_join = _th.Thread.join
    _th.Thread.join = _patched_thread_join


def _unpatch_threading():
    _th.Lock      = _real_Lock
    _th.RLock     = _real_RLock
    _th.Event     = _real_Event
    _th.Condition = _real_Condition
    _th.Semaphore = _real_Semaphore
    _th.BoundedSemaphore = _real_BoundedSemaphore
    _thread.allocate_lock = _real_allocate_lock
    if _real_thread_RLock is not None:
        _thread.RLock = _real_thread_RLock
    if _orig_thread_join is not None:
        _th.Thread.join = _orig_thread_join


# ============================================================
# queue  -- queue.Queue picks up CoLock/CoCondition at __init__ for free;
# queue.SimpleQueue is a C type (_queue.SimpleQueue) whose .get(block=True)
# parks on a C lock the scheduler can't wake, so it needs a cooperative
# replacement.  SimpleQueue is used by logging.handlers.QueueHandler /
# QueueListener and by ThreadPoolExecutor's work queue, so goroutine code
# bumps into it more than you'd expect.
# ============================================================
import queue as _queue_mod

_real_SimpleQueue = _queue_mod.SimpleQueue


class CoSimpleQueue(object):
    """Cooperative, unbounded FIFO matching queue.SimpleQueue's surface.

    put() never blocks (SimpleQueue is unbounded), so the block/timeout
    args are accepted and ignored exactly as the C type does.  get() parks
    the goroutine on a parker when empty; a producer hands the next item to
    the longest-waiting getter."""
    __slots__ = ("_items", "_waiters")

    def __init__(self):
        self._items   = collections.deque()
        self._waiters = collections.deque()

    def put(self, item, block=True, timeout=None):
        self._items.append(item)
        while self._waiters:
            # Wake one waiter; if it already timed out (released its parker)
            # the unpark is harmless, so loop until we hand off to a live one
            # or run out -- but a single live waiter is the normal case.
            self._waiters.popleft().unpark()
            break

    def put_nowait(self, item):
        self.put(item, False)

    def get(self, block=True, timeout=None):
        if self._items:
            return self._items.popleft()
        if not block:
            raise _queue_mod.Empty
        if not _in_goroutine():
            # No goroutine to park; spin + yield to the OS so a producer on
            # a real thread can fill us.
            t0 = time.monotonic()
            while not self._items:
                _raw_time_sleep(0.0001)
                if timeout is not None and time.monotonic() - t0 >= timeout:
                    raise _queue_mod.Empty
            return self._items.popleft()
        p = _Parker()
        # _Parker() may yield (Windows socketpair path); re-check first.
        if self._items:
            p.release()
            return self._items.popleft()
        self._waiters.append(p)
        if timeout is None:
            p.park()
        else:
            deadline = time.monotonic() + timeout
            done = [False]
            def waker(parker=p, dl=deadline):
                while not done[0]:
                    remaining = dl - time.monotonic()
                    if remaining <= 0:
                        parker.unpark()
                        return
                    _co_sleep(min(remaining, 0.05))
            pygo_core.go(waker)
            p.park()
            done[0] = True
        p.release()
        if self._items:
            return self._items.popleft()
        # Woken with nothing to take -> timed out.
        try:
            self._waiters.remove(p)
        except ValueError:
            pass
        raise _queue_mod.Empty

    def get_nowait(self):
        return self.get(False)

    def empty(self):
        return not self._items

    def qsize(self):
        return len(self._items)


def _patch_queue():
    # Queue uses threading.Lock()/Condition() at instantiation, so once
    # threading is patched every new Queue is already cooperative.  Only
    # SimpleQueue (C type) needs swapping out.
    _queue_mod.SimpleQueue = CoSimpleQueue

def _unpatch_queue():
    _queue_mod.SimpleQueue = _real_SimpleQueue


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
# pygo.monkey._blocking_call(f.read, n) or os.read on the raw fd (which IS
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


# ============================================================
# DNS -- pure async UDP resolver (no thread pool)
#
# Modeled on Go's `netgo` resolver: parse /etc/resolv.conf and
# /etc/hosts at first use, send UDP queries via the cooperatively
# patched socket layer (so recvfrom parks on wait_fd, not a thread),
# parse A/AAAA records.  Result cache amortises repeat lookups to
# microseconds.  A and AAAA queries fire in parallel goroutines so
# dual-stack hosts get both families in one round-trip.
# ============================================================
import struct as _struct
import random as _rand

_DNS_PORT      = 53
_QTYPE_A       = 1
_QTYPE_AAAA    = 28
_DNS_TIMEOUT_S = 2.0
_DNS_CACHE_TTL = 60.0

_resolvers_cache = None
_hosts_cache     = None
_dns_result_cache = {}    # (lowername, qtype) -> (addrs, expire_ts)


def _resolv_conf_paths():
    """Candidate paths for resolver config, in order of preference.

    POSIX: /etc/resolv.conf is universal.
    Windows: no plain text equivalent (DNS settings live in the registry
        via GetNetworkParams); we return empty here and let the caller
        fall back to libc getaddrinfo via the backend pool.
    """
    if _IS_WINDOWS:
        return ()
    return ("/etc/resolv.conf",)


def _hosts_file_paths():
    """Candidate paths for the static hosts file."""
    if _IS_WINDOWS:
        # %SystemRoot% defaults to C:\Windows; SystemDrive is the C: part.
        sysroot = os.environ.get("SystemRoot", r"C:\Windows")
        return (os.path.join(sysroot, "System32", "drivers", "etc", "hosts"),)
    return ("/etc/hosts",)


def _read_small_file(path):
    """Read a config file without going through patched os.read."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return ""
    try:
        chunks = []
        while True:
            chunk = _raw_os_read(fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        try: os.close(fd)
        except OSError: pass
    try:
        return b"".join(chunks).decode("utf-8", "replace")
    except Exception:
        return ""


def _load_resolvers():
    """Return list of nameserver IPs.  Empty list -> no usable config;
    the resolver will fall back to libc getaddrinfo via the backend."""
    nss = []
    for path in _resolv_conf_paths():
        for line in _read_small_file(path).splitlines():
            line = line.split("#", 1)[0].split(";", 1)[0].strip()
            if line.startswith("nameserver"):
                parts = line.split()
                if len(parts) >= 2:
                    nss.append(parts[1])
        if nss:
            break
    return nss


def _load_hosts():
    hosts = {}
    for path in _hosts_file_paths():
        text = _read_small_file(path)
        if not text:
            continue
        for line in text.splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            addr = parts[0]
            for nm in parts[1:]:
                hosts.setdefault(nm.lower(), []).append(addr)
        if hosts:
            break
    return hosts


def _get_resolvers():
    global _resolvers_cache
    if _resolvers_cache is None:
        _resolvers_cache = _load_resolvers()
    return _resolvers_cache


def _get_hosts():
    global _hosts_cache
    if _hosts_cache is None:
        _hosts_cache = _load_hosts()
    return _hosts_cache


def _is_ip_literal(host):
    """Return AF if host is a numeric address, else None."""
    h = host.split("%", 1)[0]    # strip v6 zone-id
    try:
        socket.inet_pton(socket.AF_INET, h)
        return socket.AF_INET
    except (OSError, ValueError):
        pass
    try:
        socket.inet_pton(socket.AF_INET6, h)
        return socket.AF_INET6
    except (OSError, ValueError):
        pass
    return None


def _build_query(name, qtype):
    txn = _rand.randint(0, 0xFFFF)
    flags = 0x0100   # standard query + recursion desired
    hdr = _struct.pack("!HHHHHH", txn, flags, 1, 0, 0, 0)
    qname = b""
    for lbl in name.encode("ascii").split(b"."):
        if not lbl:
            continue
        if len(lbl) > 63:
            raise OSError("DNS label too long")
        qname += bytes([len(lbl)]) + lbl
    qname += b"\x00"
    qpart = qname + _struct.pack("!HH", qtype, 1)   # IN class
    return txn, hdr + qpart


def _skip_dns_name(data, off):
    while True:
        if off >= len(data):
            raise OSError("DNS name overruns packet")
        ln = data[off]
        if ln == 0:
            return off + 1
        if (ln & 0xC0) == 0xC0:
            return off + 2
        off += 1 + ln


def _parse_dns_answer(data, expected_txn):
    if len(data) < 12:
        raise OSError("DNS response too short")
    txn, flags, qd, an, _ns, _ar = _struct.unpack("!HHHHHH", data[:12])
    if txn != expected_txn:
        raise OSError("DNS txn mismatch")
    rcode = flags & 0xF
    if rcode == 3:       # NXDOMAIN
        return []
    if rcode != 0:
        raise OSError("DNS server rcode=%d" % rcode)
    off = 12
    for _ in range(qd):
        off = _skip_dns_name(data, off)
        off += 4
    addrs = []
    for _ in range(an):
        off = _skip_dns_name(data, off)
        if off + 10 > len(data):
            break
        rtype, _rclass, _ttl, rdlen = _struct.unpack("!HHIH", data[off:off+10])
        off += 10
        rdata = data[off:off+rdlen]
        off += rdlen
        if rtype == _QTYPE_A and len(rdata) == 4:
            addrs.append(socket.inet_ntoa(rdata))
        elif rtype == _QTYPE_AAAA and len(rdata) == 16:
            addrs.append(socket.inet_ntop(socket.AF_INET6, rdata))
    return addrs


def _query_nameserver(packet, txn, ns, timeout):
    """Single UDP round trip.  Uses cooperatively patched socket."""
    af = _is_ip_literal(ns)
    if af is None:
        raise OSError("non-IP nameserver: " + ns)
    s = socket.socket(af, socket.SOCK_DGRAM)
    try:
        s.settimeout(timeout)
        s.sendto(packet, (ns, _DNS_PORT))
        data, _ = s.recvfrom(4096)
        return _parse_dns_answer(data, txn)
    finally:
        s.close()


def _resolve_via_libc(name, qtype):
    """Fall back to the platform getaddrinfo, dispatched through the
    blocking-call backend so other goroutines keep running while libc's
    blocking resolver is in flight.  Used when we have no usable
    /etc/resolv.conf (Windows; chrooted POSIX without DNS config)."""
    af = socket.AF_INET if qtype == _QTYPE_A else socket.AF_INET6
    try:
        infos = _blocking_call(_orig_getaddrinfo, name, 0, af,
                               socket.SOCK_STREAM, 0, 0)
    except socket.gaierror:
        return []
    addrs = []
    for info in infos:
        sa = info[4]
        addrs.append(sa[0])
    return addrs


def _resolve_qtype(name, qtype):
    """Resolve one query type with cache + nameserver fall-through.

    Falls back to libc getaddrinfo (via backend pool) when no resolver
    config is available -- the Windows case, where DNS settings live in
    the registry rather than in /etc/resolv.conf."""
    key = (name.lower(), qtype)
    now = time.monotonic()
    cached = _dns_result_cache.get(key)
    if cached is not None and cached[1] > now:
        return cached[0]
    resolvers = _get_resolvers()
    if not resolvers:
        addrs = _resolve_via_libc(name, qtype)
        _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
        return addrs
    txn, packet = _build_query(name, qtype)
    last_err = None
    for ns in resolvers:
        try:
            addrs = _query_nameserver(packet, txn, ns, _DNS_TIMEOUT_S)
            _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
            return addrs
        except (OSError, socket.timeout) as e:
            last_err = e
            continue
    # All configured nameservers failed -- try libc as a last resort
    # rather than surfacing the per-server error, which is usually
    # more confusing than just answering through the OS.
    addrs = _resolve_via_libc(name, qtype)
    if addrs:
        _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
        return addrs
    if last_err is not None:
        raise last_err
    raise OSError("no DNS nameservers")


def _resolve_dual(name, want_v4, want_v6):
    """Concurrent A + AAAA queries when both wanted."""
    if want_v4 and not want_v6:
        return [(socket.AF_INET,  a) for a in _resolve_qtype(name, _QTYPE_A)]
    if want_v6 and not want_v4:
        return [(socket.AF_INET6, a) for a in _resolve_qtype(name, _QTYPE_AAAA)]
    # Both -- fire in parallel goroutines, gather via Parker
    results = [None, None]
    parker = _Parker()
    remaining = [2]
    def runner(idx, qtype):
        try:
            results[idx] = _resolve_qtype(name, qtype)
        except Exception:
            results[idx] = []
        remaining[0] -= 1
        if remaining[0] == 0:
            parker.unpark()
    pygo_core.go(lambda: runner(0, _QTYPE_A))
    pygo_core.go(lambda: runner(1, _QTYPE_AAAA))
    parker.park()
    parker.release()
    out = []
    for a in results[0] or ():
        out.append((socket.AF_INET, a))
    for a in results[1] or ():
        out.append((socket.AF_INET6, a))
    return out


_orig_getaddrinfo      = None
_orig_gethostbyname    = None
_orig_gethostbyname_ex = None
_orig_getnameinfo      = None
_orig_gethostbyaddr    = None


def _af_wanted(family, candidate):
    return family == 0 or family == candidate or family == socket.AF_UNSPEC


def _patched_getaddrinfo(host, port=0, family=0, type=0, proto=0, flags=0):
    # Numeric-port-only path: hand "service name" lookups to libc by
    # offloading the full call (rare in normal apps).
    if isinstance(port, str):
        try:
            port = int(port)
        except ValueError:
            return _blocking_call(_orig_getaddrinfo,
                                  host, port, family, type, proto, flags)

    if host is None or host == "":
        host = "::" if family == socket.AF_INET6 else "0.0.0.0"

    # IP literal -- no DNS round trip.
    lit_af = _is_ip_literal(host)
    if lit_af is not None:
        if not _af_wanted(family, lit_af):
            raise socket.gaierror(socket.EAI_FAMILY,
                                  "Address family mismatch")
        pairs = [(lit_af, host)]
    else:
        # /etc/hosts -- skip DNS if we have a static entry.
        hosts = _get_hosts()
        local = hosts.get(host.lower())
        if local is not None:
            pairs = []
            for a in local:
                aaf = _is_ip_literal(a)
                if aaf is None:
                    continue
                if _af_wanted(family, aaf):
                    pairs.append((aaf, a))
        else:
            want_v4 = _af_wanted(family, socket.AF_INET)
            want_v6 = _af_wanted(family, socket.AF_INET6)
            try:
                pairs = _resolve_dual(host, want_v4, want_v6)
            except OSError as e:
                raise socket.gaierror(socket.EAI_NONAME, str(e))
            if not pairs:
                raise socket.gaierror(socket.EAI_NONAME,
                                      "Name or service not known")

    st = type if type else socket.SOCK_STREAM
    result = []
    for aaf, a in pairs:
        if aaf == socket.AF_INET6:
            sa = (a, port, 0, 0)
        else:
            sa = (a, port)
        result.append((aaf, st, proto, "", sa))
    return result


def _patched_gethostbyname(name):
    infos = _patched_getaddrinfo(name, 0, socket.AF_INET)
    return infos[0][4][0]


def _patched_gethostbyname_ex(name):
    infos = _patched_getaddrinfo(name, 0, socket.AF_INET)
    addrs = [info[4][0] for info in infos]
    return (name, [], addrs)


def _patched_getnameinfo(*args, **kw):
    # Reverse lookup -- not worth re-implementing for v0.  Off-thread.
    return _blocking_call(_orig_getnameinfo, *args, **kw)


def _patched_gethostbyaddr(*args, **kw):
    return _blocking_call(_orig_gethostbyaddr, *args, **kw)


def _patch_dns():
    global _orig_getaddrinfo, _orig_gethostbyname, _orig_gethostbyname_ex
    global _orig_getnameinfo, _orig_gethostbyaddr
    _orig_getaddrinfo      = socket.getaddrinfo
    _orig_gethostbyname    = socket.gethostbyname
    _orig_gethostbyname_ex = socket.gethostbyname_ex
    _orig_getnameinfo      = socket.getnameinfo
    _orig_gethostbyaddr    = socket.gethostbyaddr
    socket.getaddrinfo      = _patched_getaddrinfo
    socket.gethostbyname    = _patched_gethostbyname
    socket.gethostbyname_ex = _patched_gethostbyname_ex
    socket.getnameinfo      = _patched_getnameinfo
    socket.gethostbyaddr    = _patched_gethostbyaddr


def _unpatch_dns():
    socket.getaddrinfo      = _orig_getaddrinfo
    socket.gethostbyname    = _orig_gethostbyname
    socket.gethostbyname_ex = _orig_gethostbyname_ex
    socket.getnameinfo      = _orig_getnameinfo
    socket.gethostbyaddr    = _orig_gethostbyaddr


# ============================================================
# top-level patch() / unpatch()
# ============================================================
_orig_pygo_core_go = None
_orig_pygo_core_mn_go = None


def _patched_pygo_core_go(fn, **kwargs):
    return _orig_pygo_core_go(_wrap_goroutine_callable(fn), **kwargs)


def _patched_pygo_core_mn_go(fn, **kwargs):
    return _orig_pygo_core_mn_go(_wrap_goroutine_callable(fn), **kwargs)


def _install_go_wrapper():
    """Wrap pygo_core.go / mn_go so user callables run with the
    goroutine-context flag set.  Idempotent."""
    global _orig_pygo_core_go, _orig_pygo_core_mn_go
    if _orig_pygo_core_go is None:
        _orig_pygo_core_go = pygo_core.go
        pygo_core.go = _patched_pygo_core_go
    if _orig_pygo_core_mn_go is None:
        _orig_pygo_core_mn_go = pygo_core.mn_go
        pygo_core.mn_go = _patched_pygo_core_mn_go


def _uninstall_go_wrapper():
    global _orig_pygo_core_go, _orig_pygo_core_mn_go
    if _orig_pygo_core_go is not None:
        pygo_core.go = _orig_pygo_core_go
        _orig_pygo_core_go = None
    if _orig_pygo_core_mn_go is not None:
        pygo_core.mn_go = _orig_pygo_core_mn_go
        _orig_pygo_core_mn_go = None


_DEFAULTS = ("socket", "time", "os", "select", "selectors", "stdio", "ssl",
             "subprocess", "process", "threading", "queue", "file",
             "syscalls", "dns")

_PATCHERS = {
    "socket":     (_patch_socket,     _unpatch_socket),
    "time":       (_patch_time,       _unpatch_time),
    "os":         (_patch_os,         _unpatch_os),
    "select":     (_patch_select,     _unpatch_select),
    "selectors":  (_patch_selectors,  _unpatch_selectors),
    "stdio":      (_patch_stdio,      _unpatch_stdio),
    "ssl":        (_patch_ssl,        _unpatch_ssl),
    "subprocess": (_patch_subprocess, _unpatch_subprocess),
    "process":    (_patch_process,    _unpatch_process),
    "threading":  (_patch_threading,  _unpatch_threading),
    "queue":      (_patch_queue,      _unpatch_queue),
    "file":       (_patch_file,       _unpatch_file),
    "syscalls":   (_patch_syscalls,   _unpatch_syscalls),
    "dns":        (_patch_dns,        _unpatch_dns),
}

_applied = set()


def patch(**flags):
    """Apply pygo monkey-patches.  Idempotent.

    All categories default to True.  Pass keyword False to opt out:
        pygo.monkey.patch(threading=False, dns=False)

    Categories: socket, time, os, select, selectors, stdio, ssl,
    subprocess, process, threading, queue, file, syscalls, dns.  See
    module docstring.
    """
    unknown = set(flags) - set(_PATCHERS)
    if unknown:
        raise TypeError("patch() got unknown category: " +
                        ", ".join(sorted(unknown)))
    _install_go_wrapper()
    # Threading must come before queue (queue is a no-op but kept for
    # symmetry); socket has to come before dns (dns wraps socket fns).
    order = list(_DEFAULTS)
    for name in order:
        if not flags.get(name, True):
            continue
        if name in _applied:
            continue
        _PATCHERS[name][0]()
        _applied.add(name)


def unpatch(**flags):
    """Reverse patches.  Without args, reverses every applied category."""
    unknown = set(flags) - set(_PATCHERS)
    if unknown:
        raise TypeError("unpatch() got unknown category: " +
                        ", ".join(sorted(unknown)))
    targets = [n for n in _DEFAULTS if flags.get(n, True)] if flags \
              else list(_DEFAULTS)
    for name in reversed(targets):
        if name not in _applied:
            continue
        _PATCHERS[name][1]()
        _applied.discard(name)
    if not _applied:
        _uninstall_go_wrapper()
