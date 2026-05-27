"""pygo monkey-patches for blocking Python APIs.

Replaces stdlib calls that would block the OS thread with versions that
park the current goroutine via pygo_core.wait_fd / pygo.sleep / a
self-pipe parker.  Other goroutines keep running while one is "blocked".

Apply once at startup:
    import pygo, pygo.monkey
    pygo.monkey.patch()                      # all categories
    pygo.monkey.patch(threading=False)       # opt out of one

Categories (all default True):
    socket       socket.socket recv/send/sendall/accept/connect/recvfrom/sendto
    time         time.sleep
    os           os.read / os.write -- wait_fd for pollable fds (pipes,
                 sockets, ttys), thread-pool offload for regular files
    select       select.select  (fast path for 1 fd; busy-poll otherwise)
    stdio        builtins.input  +  sys.stdin.read/readline
    ssl          ssl.SSLSocket recv/send/sendall/do_handshake
    subprocess   subprocess.Popen.wait
    threading    Lock, RLock, Event, Condition, Semaphore, BoundedSemaphore
    queue        no-op (works automatically once threading.Condition is cooperative)
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
    * select.select with >1 fd is a 1ms-yield busy-poll.
    * Replacing threading.Lock etc. is best-effort coordination with real
      OS threads -- the single-thread cooperative model is the design target.
    * `queue.Queue` instances created before patch() keep the original
      sync primitives; patch() early.
"""
import _thread
import builtins
import collections
import errno
import os
import select as _select_mod
import socket
import ssl as _ssl_mod
import stat
import subprocess
import sys
import threading as _th
import time

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
    """Set the underlying fd to non-blocking, idempotent."""
    if sock.gettimeout() != 0.0:
        sock.setblocking(False)


def _fd_pollable(fd):
    """epoll/kqueue can't poll regular files; only fifos/sockets/ttys/etc."""
    try:
        st = os.fstat(fd)
    except OSError:
        return False
    m = st.st_mode
    return (stat.S_ISFIFO(m) or stat.S_ISSOCK(m) or
            stat.S_ISCHR(m)  or stat.S_ISBLK(m))


# ---------- self-pipe parker ----------
class _Parker(object):
    """Per-park self-pipe.  park() yields the goroutine; unpark() wakes it.

    Pipes are pooled LIFO so contended primitives don't churn syscalls.
    Single-thread cooperative use only -- the unpark()-before-park() race
    is not handled because in cooperative mode it can't happen.
    """
    __slots__ = ("r", "w")
    _pool = []

    def __init__(self):
        if _Parker._pool:
            self.r, self.w = _Parker._pool.pop()
        else:
            self.r, self.w = os.pipe()
            os.set_blocking(self.r, False)
            os.set_blocking(self.w, False)

    def park(self):
        pygo_core.wait_fd(self.r, READ)
        try:
            _raw_os_read(self.r, 64)
        except (BlockingIOError, OSError):
            pass

    def unpark(self):
        try:
            _raw_os_write(self.w, b"\x01")
        except (BlockingIOError, BrokenPipeError, OSError):
            pass

    def release(self):
        try:
            while _raw_os_read(self.r, 64):
                pass
        except (BlockingIOError, OSError):
            pass
        if len(_Parker._pool) < 64:
            _Parker._pool.append((self.r, self.w))
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
_orig_send = None
_orig_sendall = None
_orig_accept = None
_orig_connect = None
_orig_recvfrom = None
_orig_sendto = None


def _patched_recv(self, bufsize, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recv(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            r = pygo_core.wait_fd(self.fileno(), READ)
            if r == 0:
                raise socket.timeout("recv timed out")


def _patched_send(self, data, flags=0):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_send(self, data, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patched_sendall(self, data, flags=0):
    _make_nonblocking(self)
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
    _make_nonblocking(self)
    while True:
        try:
            return _orig_accept(self)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_connect(self, address):
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
    _make_nonblocking(self)
    while True:
        try:
            return _orig_recvfrom(self, bufsize, flags)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), READ)


def _patched_sendto(self, data, *args):
    _make_nonblocking(self)
    while True:
        try:
            return _orig_sendto(self, data, *args)
        except (BlockingIOError, InterruptedError):
            pygo_core.wait_fd(self.fileno(), WRITE)


def _patch_socket():
    global _orig_recv, _orig_send, _orig_sendall, _orig_accept
    global _orig_connect, _orig_recvfrom, _orig_sendto
    s = socket.socket
    _orig_recv      = s.recv
    _orig_send      = s.send
    _orig_sendall   = s.sendall
    _orig_accept    = s.accept
    _orig_connect   = s.connect
    _orig_recvfrom  = s.recvfrom
    _orig_sendto    = s.sendto
    s.recv      = _patched_recv
    s.send      = _patched_send
    s.sendall   = _patched_sendall
    s.accept    = _patched_accept
    s.connect   = _patched_connect
    s.recvfrom  = _patched_recvfrom
    s.sendto    = _patched_sendto


def _unpatch_socket():
    s = socket.socket
    s.recv      = _orig_recv
    s.send      = _orig_send
    s.sendall   = _orig_sendall
    s.accept    = _orig_accept
    s.connect   = _orig_connect
    s.recvfrom  = _orig_recvfrom
    s.sendto    = _orig_sendto


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


def _patch_os():
    global _orig_os_read, _orig_os_write
    _orig_os_read  = os.read
    _orig_os_write = os.write
    os.read  = _patched_os_read
    os.write = _patched_os_write


def _unpatch_os():
    os.read  = _orig_os_read
    os.write = _orig_os_write


# ============================================================
# select.select
# ============================================================
_orig_select_select = None


def _fd_of(x):
    return x.fileno() if hasattr(x, "fileno") else int(x)


def _patched_select(rlist, wlist, xlist, timeout=None):
    if not _in_goroutine():
        return _orig_select_select(rlist, wlist, xlist, timeout)

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
    deadline = None if timeout is None else time.monotonic() + timeout
    step = 0.001
    while True:
        try:
            pid, sts = os.waitpid(self.pid, os.WNOHANG)
        except ChildProcessError:
            return self.returncode
        if pid != 0:
            # Some Pythons expose _handle_exitstatus; if not, fall back.
            handler = getattr(self, "_handle_exitstatus", None)
            if handler is not None:
                handler(sts)
            else:
                self.returncode = sts
            return self.returncode
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
# threading -- cooperative Lock / RLock / Event / Condition / Semaphore
# ============================================================
_real_Lock      = _th.Lock
_real_RLock     = _th.RLock
_real_Event     = _th.Event
_real_Condition = _th.Condition
_real_Semaphore = _th.Semaphore
_real_BoundedSemaphore = _th.BoundedSemaphore
_real_get_ident = _th.get_ident


class CoLock(object):
    """Cooperative mutex.  Non-reentrant.

    When called from a goroutine: park on a parker queue under contention.
    When called from outside any goroutine: degrade to immediate
    acquire/release (the single-thread cooperative model never has true
    cross-thread contention between goroutines).
    """
    __slots__ = ("_locked", "_owner", "_waiters")

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
        self._waiters.append(p)
        p.park()
        p.release()
        self._owner = cur
        return True

    def release(self):
        if not self._locked:
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
    __slots__ = ("_lock", "_owner", "_count")

    def __init__(self):
        self._lock  = CoLock()
        self._owner = None
        self._count = 0

    def acquire(self, blocking=True, timeout=-1):
        cur = pygo.current() if _in_goroutine() else _real_get_ident()
        if self._owner == cur:
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
    __slots__ = ("_flag", "_waiters")

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
    __slots__ = ("_value", "_waiters")

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


def _patch_threading():
    _th.Lock      = CoLock
    _th.RLock     = CoRLock
    _th.Event     = CoEvent
    _th.Condition = CoCondition
    _th.Semaphore = CoSemaphore
    _th.BoundedSemaphore = CoBoundedSemaphore


def _unpatch_threading():
    _th.Lock      = _real_Lock
    _th.RLock     = _real_RLock
    _th.Event     = _real_Event
    _th.Condition = _real_Condition
    _th.Semaphore = _real_Semaphore
    _th.BoundedSemaphore = _real_BoundedSemaphore


# ============================================================
# queue  -- no-op; queue.Queue picks up CoLock/CoCondition at __init__
# ============================================================
def _patch_queue():
    # Queue uses `threading.Lock()` and `threading.Condition()` at
    # instantiation time, so once threading is patched, every new Queue
    # is already cooperative.  Nothing to do here.
    pass

def _unpatch_queue():
    pass


# ============================================================
# file -- builtins.open dispatched through the backend
#
# Once open() lands on a regular file, read/write traffic flows through
# os.read/os.write, which we already dispatch to the backend for
# non-pollable fds.  Wrapping open() itself covers the open syscall
# (cold-inode lookups, NFS, FUSE, slow disk) so the goroutine doesn't
# block the scheduler waiting on it.
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
    nss = []
    for line in _read_small_file("/etc/resolv.conf").splitlines():
        line = line.split("#", 1)[0].split(";", 1)[0].strip()
        if line.startswith("nameserver"):
            parts = line.split()
            if len(parts) >= 2:
                nss.append(parts[1])
    if not nss:
        nss = ["8.8.8.8", "1.1.1.1"]
    return nss


def _load_hosts():
    hosts = {}
    for line in _read_small_file("/etc/hosts").splitlines():
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        addr = parts[0]
        for nm in parts[1:]:
            hosts.setdefault(nm.lower(), []).append(addr)
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


def _resolve_qtype(name, qtype):
    """Resolve one query type with cache + nameserver fall-through."""
    key = (name.lower(), qtype)
    now = time.monotonic()
    cached = _dns_result_cache.get(key)
    if cached is not None and cached[1] > now:
        return cached[0]
    txn, packet = _build_query(name, qtype)
    last_err = None
    for ns in _get_resolvers():
        try:
            addrs = _query_nameserver(packet, txn, ns, _DNS_TIMEOUT_S)
            _dns_result_cache[key] = (addrs, now + _DNS_CACHE_TTL)
            return addrs
        except (OSError, socket.timeout) as e:
            last_err = e
            continue
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


def _patched_pygo_core_go(fn):
    return _orig_pygo_core_go(_wrap_goroutine_callable(fn))


def _patched_pygo_core_mn_go(fn):
    return _orig_pygo_core_mn_go(_wrap_goroutine_callable(fn))


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


_DEFAULTS = ("socket", "time", "os", "select", "stdio", "ssl",
             "subprocess", "threading", "queue", "file", "syscalls", "dns")

_PATCHERS = {
    "socket":     (_patch_socket,     _unpatch_socket),
    "time":       (_patch_time,       _unpatch_time),
    "os":         (_patch_os,         _unpatch_os),
    "select":     (_patch_select,     _unpatch_select),
    "stdio":      (_patch_stdio,      _unpatch_stdio),
    "ssl":        (_patch_ssl,        _unpatch_ssl),
    "subprocess": (_patch_subprocess, _unpatch_subprocess),
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

    Categories: socket, time, os, select, stdio, ssl, subprocess,
    threading, queue, dns.  See module docstring.
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
