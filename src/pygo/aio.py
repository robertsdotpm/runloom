"""pygo.aio -- async/await on the pygo scheduler.

Approach: each asyncio.Task gets its own pygo goroutine.  The goroutine
drives `coro.send()` itself; when the coro yields a pending Future,
the goroutine parks via a 1-buffered channel and resumes when the
Future's done_callback fires.  Cooperative switching between tasks is
a stack swap (~80 ns).

Measured perf characteristics (Python 3.12 on Linux, see
examples/bench_aio_io.py):
  * Multi-await chains (n=100 k=100 awaits each): ~1.9x faster
  * Deep recursive awaits (n=100 d=20): ~1.7x faster
  * Simple fan-out (10k tasks one sleep each): ~5x SLOWER

The wins come from amortizing PygoTask setup cost across many awaits.
The losses come from PygoTask creation + Chan alloc being heavier
than asyncio's tight C-deque dispatcher for one-await fan-outs.

For workloads dominated by per-task setup (asyncio-style microservice
request handlers), stick with vanilla asyncio.  For workloads with
significant per-task work (multi-await pipelines, recursive coroutine
trees, mixed monkey-patched sync I/O), the bridge wins.

The much-larger speedup our architecture allows (3-10x) requires
bypassing the asyncio.Future protocol entirely -- a separate project.

Compatibility:
  * asyncio.Future, asyncio.gather, asyncio.wait_for, asyncio.shield: work.
  * asyncio.sleep, asyncio.Lock, asyncio.Event, asyncio.Queue: work.
  * loop.add_reader / add_writer: work (level-triggered like asyncio's
    default selector loop, just driven by pygo's netpoll).
  * asyncio.start_server / open_connection (Transport+Protocol stack):
    NOT in this MVP -- for I/O, prefer `pygo.monkey.patch()` and write
    blocking-style socket code inside an `async def`.  Stack-switching
    means it just works.

Use:
    import pygo.aio as aio
    aio.install()                        # one-shot policy install
    asyncio.run(main())                  # routed through pygo

    # or directly:
    import pygo.aio as aio
    aio.run(main())                      # equivalent of asyncio.run

A user can also opt into the bridge per-call:
    loop = aio.PygoEventLoop()
    loop.run_until_complete(main())
"""
import asyncio
import collections as _collections
import contextvars as _contextvars
import errno as _errno
import os as _os
import socket as _socket
import ssl as _ssl
import subprocess as _subprocess
import sys
import threading as _threading
import time as _time
import weakref as _weakref

import pygo_core
from . import runtime as _runtime


def _signal_wakeup_noop(signum, frame):
    # A Python-level handler must be installed for CPython to write the signum
    # to set_wakeup_fd()'s pipe; the real dispatch happens loop-side off that
    # pipe (see PygoEventLoop.add_signal_handler), so this is intentionally a
    # no-op.  A server may temporarily replace it with its own handler -- the
    # wakeup-fd write happens regardless, so loop-side dispatch survives.
    pass


# Per-task driver stack size (bytes).  PygoTask drivers run arbitrary user
# code, including deep C-recursive first-time imports (pydantic etc.) that
# overflow the scheduler's default 128 KB g-stack and SEGV.  512 KB clears
# every real-world import chain seen so far while staying cheap relative to
# the CPython object tax per task.  Set PYGO_AIO_TASK_STACK=0 to disable and
# use the scheduler default; set a custom byte count to tune.
try:
    _TASK_STACK = int(_os.environ.get("PYGO_AIO_TASK_STACK", 512 * 1024))
except ValueError:
    _TASK_STACK = 512 * 1024


# ------------------------------------------------------------------
# CONCURRENT event loops: one scheduler PER OS THREAD (pygo "Phase C").
#
# pygo_core.run() drains the CALLING thread's own (thread-local) scheduler, so
# each asyncio loop runs on its thread and is fully independent of loops on
# other threads -- exactly like stock asyncio.  A thread blocking synchronously
# inside a coroutine (run_coroutine_threadsafe().result(), anyio
# BlockingPortal, a threaded server controller with a blocking client) freezes
# only its own sched, never the others'.  No single-driver election, no global
# bootstrap queue: each loop just drives itself.
#
# The only cross-thread rule: pygo_core.go() (create_task/call_soon/call_later/
# keepalive) must run on the LOOP'S thread -- a foreign thread's go() would land
# on ITS thread's sched, which this loop never drains.  So a foreign-thread
# spawn is marshalled onto the loop's thread via call_soon_threadsafe (the
# loop's lock-guarded ts queue, drained by its keepalive on its own thread).
# ------------------------------------------------------------------


def _resolve(host, port, family, type_, proto, flags):
    """getaddrinfo via the blocking-offload pool, so DNS doesn't wedge the
    goroutine's hub (it is a non-preemptible blocking C call).  Runs inline
    when not on a goroutine -- safe in either context."""
    return pygo_core.blocking(_socket.getaddrinfo, host, port,
                              family, type_, proto, flags)


def _close_sock(sock):
    """Close a socket and tell the netpoll backend to forget about it.

    Without the netpoll_unregister, the per-fd "already registered"
    bitmap stays sticky for the closed fd.  When the OS later reuses
    that fd number for a new socket, netpoll skips re-registering and
    no edge ever fires -- the new socket's wait_fd parks forever.
    Manifests as test-run hangs after fast socket churn. """
    if sock is None:
        return
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        fd = -1
    if fd >= 0:
        try: pygo_core.netpoll_unregister(fd)
        except (AttributeError, OSError): pass
    try: sock.close()
    except OSError: pass


# A cooperative mutex (parks the goroutine, not the OS thread) imported lazily
# to keep the import graph acyclic.  Used to serialise access to one SSLSocket
# shared by a connection's recv goroutine and concurrent writers under M:N.
_CoLock = None


def _get_colock():
    global _CoLock
    if _CoLock is None:
        from .monkey import CoLock as _CL
        _CoLock = _CL
    return _CoLock


# wait_fd direction flags (match the literals used throughout this file).
_WAIT_READ = 1
_WAIT_WRITE = 2

# Sentinel pygo_core.wait_fd returns when the parked goroutine was cancelled
# out-of-band via G.cancel_wait_fd() -- a task.cancel() that targets a g blocked
# in a socket recv/accept/connect, where there's no coro await-point to throw
# CancelledError into.  _wait_fd turns it back into CancelledError so it unwinds
# the recv loop -> the coro -> the driver, which settles the task cancelled.
_WAIT_FD_CANCELLED = getattr(pygo_core, "WAIT_FD_CANCELLED", 0x40000000)


def _wait_fd(fd, events, timeout_ms=-1):
    """pygo_core.wait_fd, but a cancellation (G.cancel_wait_fd) raises
    CancelledError instead of returning the raw sentinel.  Every aio I/O loop
    parks through this, so cancelling a task blocked in any socket wait works."""
    r = pygo_core.wait_fd(fd, events, timeout_ms)
    if r == _WAIT_FD_CANCELLED:
        raise asyncio.CancelledError()
    return r


class _TLSSock(object):
    """Cooperative TLS for the asyncio bridge, working on every netpoll
    backend (epoll/kqueue/IOCP/WSAPoll/select).

    Wraps the raw socket in a real ``ssl.SSLSocket`` (which owns the fd) and
    drives its non-blocking ``recv``/``send``/``do_handshake`` with pygo's
    ``wait_fd``, mirroring pygo.monkey's validated ssl patch.  It presents the
    same blocking-cooperative socket surface (recv/send/sendall/fileno/
    shutdown/close/getpeername/...) that StreamReader/StreamWriter/
    _StreamTransport already expect, so those classes use it unchanged --
    plaintext and TLS go through the exact same I/O loops.

    SSLSocket / OpenSSL are not safe for concurrent use, so a cooperative
    CoLock serialises every SSLObject call.  Crucially the lock is RELEASED
    across every wait_fd, so a read parked waiting for inbound bytes never
    blocks a concurrent write (full-duplex keeps working).  Holding a real
    OS lock here would be wrong -- pygo can switch goroutines at a bytecode
    boundary while one holds it, deadlocking the hub; CoLock is switch-safe.
    """

    def __init__(self, raw, context, *, server_side=False,
                 server_hostname=None):
        raw.setblocking(False)
        self._ssl = context.wrap_socket(
            raw, server_side=server_side,
            server_hostname=server_hostname,
            do_handshake_on_connect=False)
        self._ssl.setblocking(False)
        self._lock = _get_colock()()
        self._closed = False

    def __getattr__(self, name):
        # Delegate socket-introspection surface we don't wrap explicitly --
        # family / type / proto / setsockopt / getsockname / ... -- to the
        # underlying ssl.SSLSocket, which subclasses socket.socket and exposes
        # them.  asyncio code that pulls the socket via
        # transport.get_extra_info("socket") treats it as a real socket; e.g.
        # aiohttp's tcp_nodelay reads sock.family and calls sock.setsockopt(),
        # which raised AttributeError on the bare _TLSSock.  The cooperative
        # recv/send/sendall/fileno/etc. are defined on the class, so they take
        # precedence and __getattr__ never shadows them.  Guard _ssl and dunders
        # to avoid recursion before __init__ binds _ssl.
        if name == "_ssl" or name.startswith("__"):
            raise AttributeError(name)
        return getattr(self._ssl, name)

    def fileno(self):
        return self._ssl.fileno()

    def do_handshake(self, timeout=None):
        # timeout (seconds) bounds the WHOLE handshake (asyncio's
        # ssl_handshake_timeout); a peer that stalls mid-handshake must not
        # park this goroutine forever.  None = wait indefinitely.
        fd = self._ssl.fileno()
        deadline = None if timeout is None else (_time.monotonic() + timeout)
        while True:
            want = None
            with self._lock:
                try:
                    self._ssl.do_handshake()
                    return
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
            if deadline is None:
                _wait_fd(fd, want)
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("TLS handshake timed out")
                # wait_fd returns (without raising) when the timeout elapses;
                # the next loop re-checks the deadline and raises above.
                _wait_fd(fd, want, max(1, int(remaining * 1000)))

    def recv(self, n):
        if self._closed:
            return b""
        fd = self._ssl.fileno()
        while True:
            want = None
            with self._lock:
                try:
                    return self._ssl.recv(n)
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except _ssl.SSLZeroReturnError:
                    return b""          # clean TLS close_notify -> EOF
                except _ssl.SSLEOFError:
                    return b""          # peer dropped without close_notify
                except OSError as e:
                    # SSLWant*/Zero/EOF are SSLError(=OSError) subclasses and
                    # are caught above; a bare EAGAIN means the kernel buffer
                    # is dry -- park for readability.  Anything else is real.
                    if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                        want = _WAIT_READ
                    else:
                        raise
            _wait_fd(fd, want)

    def recv_into(self, buffer, nbytes=0):
        if self._closed:
            return 0
        fd = self._ssl.fileno()
        while True:
            want = None
            with self._lock:
                try:
                    return self._ssl.recv_into(buffer, nbytes)
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except (_ssl.SSLZeroReturnError, _ssl.SSLEOFError):
                    return 0
                except OSError as e:
                    if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                        want = _WAIT_READ
                    else:
                        raise
            _wait_fd(fd, want)

    def send(self, data):
        fd = self._ssl.fileno()
        while True:
            want = None
            with self._lock:
                try:
                    return self._ssl.send(data)
                except _ssl.SSLWantReadError:
                    want = _WAIT_READ
                except _ssl.SSLWantWriteError:
                    want = _WAIT_WRITE
                except OSError as e:
                    if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                        want = _WAIT_WRITE
                    else:
                        raise
            _wait_fd(fd, want)

    def sendall(self, data):
        view = data if isinstance(data, memoryview) else memoryview(data)
        total = len(view)
        sent = 0
        while sent < total:
            sent += self.send(view[sent:])
        return None

    def setblocking(self, flag):
        # Always cooperative-nonblocking under the hood; ignore.
        pass

    def shutdown(self, how):
        try:
            self._ssl.shutdown(how)
        except OSError:
            pass

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._ssl.close()
        except OSError:
            pass

    def getpeername(self):
        return self._ssl.getpeername()

    def getsockname(self):
        return self._ssl.getsockname()

    def getsockopt(self, *a):
        return self._ssl.getsockopt(*a)

    @property
    def ssl_object(self):
        return self._ssl

    def __del__(self):
        # Safety net: a _TLSSock dropped without close() -- e.g. a connection
        # that errored mid-setup and never routed through transport.close(), or
        # a session torn down on error -- would otherwise let its underlying
        # ssl.SSLSocket reach GC with an open fd, raising ResourceWarning(
        # "unclosed <ssl.SSLSocket ...>").  pytest's unraisable-exception hook
        # elevates that to a test error (test_error_in_performing_request,
        # test_aiohttp_request_ctx_manager_close_sess_on_error).  Close it here
        # before SSLSocket.__del__ can warn.
        if not getattr(self, "_closed", True):
            try:
                self._ssl.close()
            except Exception:
                pass


def _tls_wrap_client(raw, ssl_arg, server_hostname, host, handshake_timeout=None):
    """Wrap a freshly-connected client socket in cooperative TLS and finish
    the handshake.  ``ssl_arg`` is True (default context) or an SSLContext."""
    context = _ssl.create_default_context() if ssl_arg is True else ssl_arg
    if server_hostname is None and isinstance(host, str) and host:
        server_hostname = host
    tls = _TLSSock(raw, context, server_side=False,
                   server_hostname=server_hostname)
    tls.do_handshake(handshake_timeout)
    return tls


# Python's per-thread C recursion counter is shared across all
# goroutines on the OS thread.  Phase B saves/restores it per-g, but
# the absolute limit is still global -- spawning thousands of tasks
# can hit RecursionError just from the depth of asyncio's frame chain
# (Task.__step -> coro.send -> awaitable.__await__ -> Future.__await__).
# Pygo's __init__.py bumps the limit when imported; pygo.aio is often
# imported standalone so we do the same here.
if sys.getrecursionlimit() < 1_000_000:
    sys.setrecursionlimit(1_000_000)


# asyncio's private "currently-running task per loop" registry.  This is
# what asyncio.current_task() reads, and several stdlib helpers
# (asyncio.timeouts, asyncio.shield, taskgroups) bail with
# "must be used inside a task" if the entry is missing.  We update it
# from PygoTask._driver around every send/throw.
try:
    _CURRENT_TASKS = asyncio.tasks._current_tasks
except AttributeError:
    # Very old Python -- fall back to a no-op dict; current_task() will
    # return None and asyncio.timeouts won't work, but the rest does.
    _CURRENT_TASKS = {}


def _run_stock_task_cb(loop, cb, fut):
    # Run a deferred stock-C-_asyncio.Task done-callback (its __wakeup) the way
    # asyncio's loop would: between task steps, with NO current task registered.
    #
    # The C Task's __wakeup -> task_step calls enter_task(loop, task), which
    # RAISES "Cannot enter into task X while another task Y is being executed"
    # if loop is already a key in _current_tasks.  Stock asyncio guarantees the
    # slot is empty when a call_soon callback runs.  pygo CANNOT: PygoTask._driver
    # keeps _current_tasks[loop] = self across the whole send/throw, and a task
    # that parks mid-step on a RAW scheduler primitive (pygo's transport I/O
    # does sock_recv/connect via pygo_core.wait_fd, not by yielding a future)
    # leaves its entry in place while the goroutine is switched out -- so this
    # deferred callback, scheduled onto another goroutine, would see a stale
    # "current" PygoTask and the stock Task.__wakeup would raise instead of
    # delivering the cancellation (the body-writer hangs forever).
    #
    # The parked PygoTask is suspended, not actually executing, so clearing its
    # slot for the duration of the (synchronous) stock-Task step is safe; we
    # restore it afterward so the PygoTask's own _driver finally still sees the
    # value it expects.  Single-thread sched per loop => no races on the swap.
    prev = _CURRENT_TASKS.pop(loop, None)
    try:
        cb(fut)
    finally:
        if prev is not None:
            _CURRENT_TASKS[loop] = prev


# Make our tasks visible to asyncio.all_tasks() (and debug tooling, anyio's
# get_running_tasks, etc.).  Use the register/unregister hooks rather than a
# specific set: 3.11 walked asyncio.tasks._all_tasks, but 3.12+ renamed it to
# _scheduled_tasks and all_tasks() enumerates THAT via _register_task -- so the
# old `_all_tasks` lookup AttributeError'd on 3.13 and our tasks never showed up.
try:
    _REGISTER_TASK = asyncio.tasks._register_task
    _UNREGISTER_TASK = asyncio.tasks._unregister_task
except AttributeError:
    _REGISTER_TASK = _UNREGISTER_TASK = None

# Default task names mirror stock asyncio's "Task-N" (some libraries -- e.g.
# aiojobs -- assert task.get_name().startswith("Task-")).
import itertools as _itertools
_TASK_NAME_COUNTER = _itertools.count(1)

# Every PygoTask, across ALL loops on this process.  The pygo scheduler is one
# per OS thread (shared by every PygoEventLoop on that thread), so loop.close()
# needs to know if a SIBLING loop still has live tasks before it drains the
# shared scheduler.  WeakSet so finished/collected tasks drop out on their own.
_PG_ALL_TASKS = _weakref.WeakSet()


# ====================================================================
# Handles -- minimal asyncio.Handle / asyncio.TimerHandle compat.
# ====================================================================
class _Handle(asyncio.Handle):
    """asyncio.Handle subclass, but created OUTSIDE the loop's call queue --
    pygo fires the callback from a goroutine after consulting `_cancelled`
    (which asyncio.Handle.cancel() sets).  Subclassing the real type so that
    `isinstance(h, asyncio.Handle)` holds -- libraries (e.g. aiocache) assert
    that loop.call_*() returns an asyncio.Handle."""
    def __init__(self, cb, args, loop, context=None):
        super().__init__(cb, args, loop, context)


class _TimerHandle(asyncio.TimerHandle):
    """asyncio.TimerHandle subclass (see _Handle).  `when` is informational --
    pygo schedules via a goroutine sched_sleep, not the loop's timer heap."""
    def __init__(self, cb, args, loop, when=0, context=None):
        super().__init__(when, cb, args, loop, context)


# ====================================================================
# PygoFuture -- pure-Python Future replacement with synchronous-fire
# callbacks.  Not a subclass of asyncio.Future (the C class blocks real
# method overrides); duck-types the future protocol asyncio uses.
#
# Why this exists: stock asyncio.Future.set_result schedules every
# done_callback through loop.call_soon -- one goroutine spawn per
# callback in our model.  At 10k concurrent tasks that's 30k+ goroutine
# spawns, more than asyncio's tight C-deque path can be beaten by.
#
# In a goroutine model the defer is unnecessary -- the callbacks we
# register are just "wake the parked goroutine" via try_send, which is
# reentrant-safe.  Firing inline turns the bridge from ~5x slower than
# asyncio (at high fan-out) into a real win.
#
# asyncio recognises us via the _asyncio_future_blocking duck-type
# protocol (used by ensure_future / isfuture / Task.__step).  No
# isinstance(asyncio.Future) checks rely on the class hierarchy in
# code paths we exercise.
# ====================================================================
_PENDING   = 0
_FINISHED  = 1
_CANCELLED = 2


class _PygoFutureMixin(object):
    """Shared Future logic for PygoFuture (over asyncio.Future) and PygoTask
    (over asyncio.Task).

    Why subclass the real asyncio types at all: libraries check
    `isinstance(x, asyncio.Future)` / `asyncio.Task)` (e.g. aiomisc's
    cancel_tasks) and SKIP objects that aren't.  asyncio's own C fast paths only
    fire for CheckExact instances, so a *subclass* gets the generic path that
    calls these public Python methods -- our overrides win and the C state
    fields are never read.

    Why a mixin + _pg* names: the C Future/Task expose _state/_result/_coro/
    _fut_waiter/... as READ-ONLY descriptors, so we can't store our state under
    those names.  We keep our own state in _pg* attrs and override every method.
    `_asyncio_future_blocking` and `_loop` ARE usable (the C base's __init__
    initialises them) so we leave those on the C object.
    """

    def _pg_future_init(self):
        self._pgstate = _PENDING
        self._pgresult = None
        self._pgexc = None
        self._pgcbs = []
        self._pgcancelmsg = None
        # asyncio's "exception was never retrieved" tracking (libraries assert
        # on _log_traceback).  Our own copy -- the C _log_traceback descriptor
        # forbids being set True.
        self._pglogtb = False

    # ---- query ----
    def done(self):       return self._pgstate != _PENDING
    def cancelled(self):  return self._pgstate == _CANCELLED

    def result(self):
        if self._pgstate == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._pgstate == _CANCELLED:
            raise self._make_cancelled_error()
        self._pglogtb = False
        if self._pgexc is not None:
            raise self._pgexc
        return self._pgresult

    def exception(self):
        if self._pgstate == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._pgstate == _CANCELLED:
            raise self._make_cancelled_error()
        self._pglogtb = False
        return self._pgexc

    def __repr__(self):
        # asyncio-compatible repr.  PygoFuture/PygoTask are drop-in asyncio
        # Future/Task; code and tests inspect the repr and expect the asyncio
        # spelling -- aiohttp's test_format_task_get asserts
        # f"{task}".startswith("<Task pending"), and StreamReader.__repr__
        # embeds repr(waiter) expecting "<Future pending>".  So present as
        # Future/Task, not the PygoFuture/PygoTask implementation class name
        # that asyncio.Future.__repr__ would otherwise emit.
        state = ("pending" if self._pgstate == _PENDING else
                 "cancelled" if self._pgstate == _CANCELLED else "finished")
        if isinstance(self, asyncio.Task):
            info = ["Task", state, "name=%r" % self._pgname]
            coro = getattr(self, "_pgcoro", None)
            if coro is not None:
                info.append("coro=%r" % (coro,))
        else:
            info = ["Future", state]
            if self._pgstate == _FINISHED:
                if self._pgexc is not None:
                    info.append("exception=%r" % (self._pgexc,))
                else:
                    info.append("result=%r" % (self._pgresult,))
        return "<%s>" % " ".join(info)

    @property
    def _log_traceback(self):
        return self._pglogtb

    @_log_traceback.setter
    def _log_traceback(self, val):
        # Some asyncio code sets this False; honour False, ignore True coming
        # from outside (we set _pglogtb ourselves in set_exception).
        if not val:
            self._pglogtb = False

    # Map the C Future's read-only descriptor NAMES to our _pg* state, so code
    # that pokes the "private" attributes directly (e.g. async-lru reads
    # task._exception to avoid clearing _log_traceback) sees our real state, not
    # the never-updated C fields.  These properties shadow the C descriptors
    # because the mixin precedes asyncio.Future/Task in the MRO.
    @property
    def _exception(self):
        return self._pgexc

    @property
    def _result(self):
        return self._pgresult

    @property
    def _callbacks(self):
        return self._pgcbs

    @property
    def _state(self):
        s = self._pgstate
        return ("PENDING" if s == _PENDING else
                "FINISHED" if s == _FINISHED else "CANCELLED")

    # ---- mutation ----
    def set_result(self, result):
        if self._pgstate != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        self._pgresult = result
        self._pgstate  = _FINISHED
        self._fire_callbacks()

    def set_exception(self, exception):
        if self._pgstate != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        if isinstance(exception, type):
            exception = exception()
        if isinstance(exception, StopIteration):
            raise TypeError(
                "StopIteration interacts badly with generators "
                "and cannot be raised into a Future")
        self._pgexc = exception
        self._pgstate = _FINISHED
        self._pglogtb = True
        self._fire_callbacks()

    def __del__(self):
        # "exception was never retrieved" warning, now that a completed task is
        # collectable (upstream c9e1db2 releases g->callable at goroutine
        # completion, breaking the task->_g->callable->task cycle).  Keep it
        # side-effect-free: for a fire-and-forget task whose only ref is
        # g->callable, this runs in the goroutine's own completion context, so
        # we must NOT re-enter the scheduler -- a plain call_exception_handler
        # (logging) is fine.
        if not self._pglogtb or self._pgexc is None:
            return
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_exception_handler({
                "message": "%s exception was never retrieved"
                           % self.__class__.__name__,
                "exception": self._pgexc,
                "future": self,
            })
        except BaseException:
            pass

    def _pg_future_cancel(self, msg=None):
        if self._pgstate != _PENDING:
            return False
        self._pgcancelmsg = msg
        self._pgstate = _CANCELLED
        self._fire_callbacks()
        return True

    # PygoFuture's public cancel IS the future-cancel; PygoTask overrides it
    # with the task-cancel and uses _pg_future_cancel internally.
    cancel = _pg_future_cancel

    def _make_cancelled_error(self):
        msg = self._pgcancelmsg
        if msg is None:
            return asyncio.CancelledError()
        return asyncio.CancelledError(msg)

    # ---- callbacks ----
    def add_done_callback(self, callback, *, context=None):
        if self._pgstate != _PENDING:
            try:
                if context is None:
                    callback(self)
                else:
                    context.run(callback, self)
            except BaseException as e:
                self._report_exc(e)
        else:
            self._pgcbs.append((callback, context))

    def remove_done_callback(self, callback):
        filtered = [(cb, ctx) for cb, ctx in self._pgcbs if cb is not callback]
        removed  = len(self._pgcbs) - len(filtered)
        self._pgcbs = filtered
        return removed

    def _fire_callbacks(self):
        cbs, self._pgcbs = self._pgcbs, []
        loop = self._loop
        for cb, ctx in cbs:
            # A stock C _asyncio.Task awaiting a PygoFuture registers its
            # Task.__wakeup as the done-callback (aiohttp creates eager-start
            # asyncio.Task()s directly, not via loop.create_task -> PygoTask).
            # Stock asyncio schedules future callbacks via loop.call_soon; firing
            # __wakeup SYNCHRONOUSLY from inside the future's own cancel()/
            # set_result is re-entrant and the C Task mishandles it -- it never
            # reschedules __step, so the awaiting task hangs (e.g. write_bytes,
            # the streaming-request body-writer, cancelled on connection close).
            # Defer ONLY those callbacks (match asyncio); every other callback --
            # pygo's _wake_unpark, library done-callbacks -- stays synchronous,
            # preserving pygo's wake timing.
            host = getattr(cb, "__self__", None)
            if (loop is not None and not loop.is_closed()
                    and isinstance(host, asyncio.Task)
                    and not isinstance(host, PygoTask)):
                try:
                    loop.call_soon(_run_stock_task_cb, loop, cb, self,
                                   context=ctx)
                except BaseException as e:
                    self._report_exc(e)
            else:
                try:
                    if ctx is None:
                        cb(self)
                    else:
                        ctx.run(cb, self)
                except BaseException as e:
                    self._report_exc(e)

    def _report_exc(self, e):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "exception in PygoFuture callback",
                "exception": e,
                "future": self,
            })

    # ---- await protocol ----
    def __await__(self):
        if self._pgstate == _PENDING:
            self._asyncio_future_blocking = True
            yield self
            assert self._pgstate != _PENDING
        return self.result()

    __iter__ = __await__


class PygoFuture(_PygoFutureMixin, asyncio.Future):
    """A real asyncio.Future subclass with pygo's synchronous-callback
    dispatch.  isinstance(x, asyncio.Future) holds; asyncio uses our overridden
    methods (subclasses miss the C fast paths)."""

    def __init__(self, *, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        # Initialise the C Future (gives us a valid _loop + _asyncio_future_
        # blocking field).  Its _state stays PENDING forever -- asyncio reads
        # our done()/result() instead, and a PENDING C Future doesn't warn at
        # GC (only Tasks do).
        asyncio.Future.__init__(self, loop=loop)
        self._asyncio_future_blocking = False
        self._pg_future_init()


def _fut_cancelled_error(fut):
    """Build the CancelledError to throw into a coroutine whose awaited future
    was cancelled, PRESERVING the future's cancel message.  Both PygoFuture and
    stdlib asyncio.Future expose _make_cancelled_error() (3.9+); fall back to a
    bare CancelledError for any exotic awaitable that lacks it."""
    mk = getattr(fut, "_make_cancelled_error", None)
    if mk is not None:
        try:
            return mk()
        except BaseException:
            pass
    return asyncio.CancelledError()


# ====================================================================
# PygoTask -- the heart of the bridge.
# ====================================================================
class PygoTask(_PygoFutureMixin, asyncio.Task):
    """A real asyncio.Task subclass (isinstance(x, asyncio.Task) holds) driven
    by a pygo goroutine instead of the C task machinery.

    We initialise only the Future half of the C object (asyncio.Future.__init__)
    -- NOT Task.__init__, which would schedule the C task-step and double-drive
    our coroutine (the C step is a C callable we can't shadow from Python).  The
    C Task's own fields (_coro, _fut_waiter, ...) stay NULL; we keep our state in
    _pg* attrs and override the readers.  On completion we settle the underlying
    C Future state so asyncio.Task.__del__ doesn't warn "destroyed but pending".
    """

    def __init__(self, coro, *, loop=None, name=None, context=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        # Future half only -- gives a valid _loop + _asyncio_future_blocking and
        # does NOT schedule a C task-step.
        asyncio.Future.__init__(self, loop=loop)
        self._asyncio_future_blocking = False
        self._pg_future_init()
        self._pgcoro = coro
        # Per-task contextvars Context, exactly like stock asyncio.Task: capture
        # a copy of the CURRENT context at creation (or honour an explicit
        # context=, as anyio's portal passes through create_task) and run every
        # coro step inside it.  Without this, contextvars set in a parent never
        # reach the task -- breaking request-id/OTel/structlog middleware and
        # any contextvar read from a threadpool-dispatched sync endpoint.
        self._pgcontext = context if context is not None \
            else _contextvars.copy_context()
        # Match asyncio.Task: only None falls back to the auto name; an explicit
        # name (incl. the empty string "") is kept as-is, str()-coerced.
        self._pgname = ("Task-%d" % next(_TASK_NAME_COUNTER)) \
            if name is None else str(name)
        # _self_g: the driver's G handle (done-callbacks / cancel wake it).
        self._self_g = None
        # _pgmustcancel: ONE-SHOT cancel-delivery flag (mirrors asyncio.Task's
        # _must_cancel); cancel() sets it, the driver throws CancelledError once
        # then clears it (a persistent re-throw would re-cancel cleanup awaits in
        # `async with __aexit__`/finally before they finish).
        self._cancel_requested = False
        self._pgmustcancel = False
        # _pgfutwaiter: the future/task we're suspended on, so cancel() can
        # propagate INTO it (asyncio.Task._fut_waiter analogue).  None while running.
        self._pgfutwaiter = None
        self._pgnumcancels = 0          # cancelling()/uncancel() counter
        # Register in asyncio.all_tasks() (Task.__init__ would normally do this).
        if _REGISTER_TASK is not None:
            try:
                _REGISTER_TASK(self)
            except Exception:
                pass
        # Also track in a pygo-global set so loop.close() can tell whether
        # ANOTHER loop on this OS thread still has live tasks (see
        # _cancel_outstanding_tasks): the pygo scheduler is shared per-thread,
        # so a close()-time sched_reset must not bulldoze a sibling loop's work.
        _PG_ALL_TASKS.add(self)
        # Driver goroutines run arbitrary user async code (deep C-recursive
        # first-time imports overflow the default 128 KB g-stack and SEGV), so
        # give them a roomier stack.  Override with PYGO_AIO_TASK_STACK.
        self._g = pygo_core.go(self._driver, stack_size=_TASK_STACK) \
            if _TASK_STACK else pygo_core.go(self._driver)

    def _pg_settle_c(self):
        # Settle the underlying C Future to FINISHED so asyncio.Task.__del__
        # doesn't warn "Task was destroyed but it is pending" -- our goroutine
        # drives the coro, so the C task machinery never settles its own state.
        # The C Future has no C callbacks (asyncio uses our add_done_callback),
        # so this fires nothing.
        try:
            if not asyncio.Future.done(self):
                asyncio.Future.set_result(self, None)
        except BaseException:
            pass
        # Drop our goroutine handles at completion.  The driver frame (still on
        # the goroutine's stack here) holds `self` as a local, so as long as the
        # task references its goroutine via _g / _self_g there is a cycle
        # task -> _g/_self_g -> g -> retained driver frame -> self that survives
        # REFCOUNTING -- it only clears on the next gc.collect().  That keeps a
        # finished task (and its captured _pgexc + traceback) alive longer than
        # stock asyncio, which a well-behaved teardown -- and anyio's
        # TestRefcycles -- relies on NOT happening.  c9e1db2 cleared g->callable
        # in C; this clears the Python-side frame path.  Both _g and _self_g
        # wrap the SAME goroutine, so clearing the Python refs (rather than
        # adding tp_traverse to the shared G type, which double-counts the one
        # g->callable across the two wrappers) is the safe break.  cancel() and
        # _wake_unpark only touch _self_g while pending, so dropping it now (the
        # task is terminal) is safe.
        self._g = None
        self._self_g = None

    def _pg_strip_driver_tb(self, exc):
        """Drop this driver's own frame(s) from the head of exc's traceback.

        An exception raised by the user coro unwinds through the driver's
        Python frame (the coro.send / coro.throw call), so exc.__traceback__'s
        leading frame is the driver frame -- which holds `self` as a local.
        Storing exc as the task's result then forms a cycle that survives
        REFCOUNTING: task -> _pgexc -> __traceback__ -> driver frame -> self,
        keeping the finished task (and its captured exception) alive until the
        next gc.collect().  Stock asyncio's task step is C, so its traceback
        never carries a self-holding Python frame; matching that (and giving
        cleaner tracebacks free of pygo internals) means stripping the driver
        frame here.  Nested exceptions (ExceptionGroup.exceptions, __cause__)
        keep their own tracebacks -- those point at user frames, not us."""
        try:
            tb = exc.__traceback__
            code = self._driver.__func__.__code__
            while tb is not None and tb.tb_frame.f_code is code:
                tb = tb.tb_next
            return exc.with_traceback(tb)
        except Exception:
            return exc

    # __repr__ is inherited from _PygoFutureMixin (asyncio-compatible
    # "<Task pending name=... coro=...>"), shared with PygoFuture.

    # ---- asyncio.Task surface ----
    def get_coro(self):
        return self._pgcoro

    def get_context(self):
        return self._pgcontext

    def get_name(self):
        return self._pgname

    def set_name(self, name):
        self._pgname = str(name)

    def cancel(self, msg=None):
        if self.done():
            return False
        self._cancel_requested = True
        # Remember the cancel message so the driver can deliver
        # CancelledError(msg) -- anyio's cancel scopes recognise their own
        # cancellation solely by exc.args[0] ("Cancelled via cancel scope ..."),
        # so dropping the message makes the scope refuse to swallow it and the
        # CancelledError escapes (breaks every StreamingResponse/SSE handler).
        self._pgcancelmsg = msg
        self._pgnumcancels += 1
        # If we're suspended on a future/task, propagate the cancel INTO it
        # (mirrors stock asyncio cancelling self._fut_waiter).  Its completion
        # then wakes us via the already-registered done-callback -- so an
        # awaited inner task runs its OWN cleanup (async with __aexit__ /
        # finally) and we wait for it before our CancelledError surfaces.
        if self._pgfutwaiter is not None:
            if self._pgfutwaiter.cancel(msg=msg):
                return True
            # _pgfutwaiter couldn't take the cancel (already cancelling/done),
            # but it WILL still wake us when it completes.  Mark a one-shot
            # cancel for the driver to deliver then, and do NOT wake now: a
            # premature unpark would abandon our wait on _pgfutwaiter, leaking it
            # half-cancelled (seen with nested wait_for where both the outer and
            # inner timeouts cancel the same task on the same tick).  Mirrors
            # stock asyncio.Task.cancel(), which sets _must_cancel without
            # rescheduling when _fut_waiter is present.
            self._pgmustcancel = True
            return True
        # Not suspended on a cancellable future (running, or parked in a C
        # wait_fd): deliver a one-shot cancel at the next driver step.
        self._pgmustcancel = True
        if self._self_g is not None:
            # If the goroutine is parked in pygo_core.wait_fd (sock_recv /
            # sock_accept / sock_connect / a transport recv loop), there is NO
            # coro await-point for the driver to throw into, and G.wake() only
            # wakes park_self parkers -- so it would hang forever.  cancel_wait_fd
            # wakes the netpoll parker: wait_fd returns the CANCELLED sentinel,
            # _wait_fd raises CancelledError, and the driver settles us cancelled.
            # Falls back to wake() for a running / park_self goroutine.
            woke = False
            cwf = getattr(self._self_g, "cancel_wait_fd", None)
            if cwf is not None:
                woke = cwf()
            if not woke:
                self._self_g.wake()
        return True

    def cancelling(self):
        """Number of unresolved cancel() calls.  Required by
        asyncio.timeouts / asyncio.TaskGroup in 3.11+."""
        return self._pgnumcancels

    def uncancel(self):
        """Decrement the cancelling counter.  When it returns to zero, clear
        the outstanding-cancel state and any not-yet-delivered one-shot cancel
        (asyncio.timeout / TaskGroup call this after handling a CancelledError,
        meaning 'don't keep cancelling me')."""
        if self._pgnumcancels > 0:
            self._pgnumcancels -= 1
        if self._pgnumcancels == 0:
            self._cancel_requested = False
            self._pgmustcancel = False
        return self._pgnumcancels

    # Shadow the C asyncio.Task descriptors with our _pg* state.  anyio's
    # _deliver_cancellation reads BOTH directly: `if task._must_cancel:
    # continue` (skip a task that already has a cancel pending) and `waiter =
    # task._fut_waiter` (only re-cancel while the awaited future isn't done).
    # The never-updated C slots are always False/None, so without these anyio
    # would hammer task.cancel() every loop cycle -- re-injecting CancelledError
    # into cleanup awaits.  Read-only: nothing on our drive path sets them (the
    # C Task.__step that would is never run); we keep state in the _pg* attrs.
    @property
    def _must_cancel(self):
        return self._pgmustcancel

    @property
    def _fut_waiter(self):
        return self._pgfutwaiter

    # ---- driver: the per-task goroutine body ----
    def _driver(self):
        # Capture our own G handle so cancel/done_callback can wake us.
        self._self_g = pygo_core.current_g()

        coro       = self._pgcoro
        send_value = None
        throw_exc  = None

        loop = self._loop

        while True:
            # --- advance the coroutine one step ---
            # Register as the loop's "current task" for the duration of
            # the send/throw.  asyncio.timeouts / current_task() rely on
            # this; without it stdlib helpers think we're not inside a
            # task and raise.
            prev_current = _CURRENT_TASKS.get(loop)
            _CURRENT_TASKS[loop] = self
            try:
                try:
                    if self._pgmustcancel and throw_exc is None:
                        # Deliver the cancel exactly once, then clear it so the
                        # coro's cleanup awaits (async with __aexit__ / finally)
                        # aren't re-cancelled before they finish.  Carry the
                        # cancel message (anyio matches on it to swallow).
                        throw_exc = self._make_cancelled_error()
                        self._pgmustcancel = False
                    if throw_exc is not None:
                        e, throw_exc = throw_exc, None
                        yielded = self._pgcontext.run(coro.throw, e)
                    else:
                        yielded = self._pgcontext.run(coro.send, send_value)
                except StopIteration as si:
                    if not self.done():
                        self.set_result(si.value)
                    self._pg_settle_c()
                    return
                except asyncio.CancelledError:
                    if not self.done():
                        # Record the task's own cancel message so a parent
                        # awaiting THIS task receives CancelledError(msg) too.
                        self._pg_future_cancel(self._pgcancelmsg)
                    self._pg_settle_c()
                    return
                except (KeyboardInterrupt, SystemExit) as e:
                    # asyncio's Task.__step records the exception on the task
                    # AND re-raises it out of the loop.  Mirror that: store it
                    # (so a parent retrieving this task's result sees it) and
                    # signal the loop to break the drive and re-raise.
                    if not self.done():
                        self.set_exception(self._pg_strip_driver_tb(e))
                    self._pg_settle_c()
                    loop._pg_signal_fatal(e)
                    return
                except BaseException as e:
                    if not self.done():
                        self.set_exception(self._pg_strip_driver_tb(e))
                    self._pg_settle_c()
                    return
            finally:
                if prev_current is None:
                    _CURRENT_TASKS.pop(loop, None)
                else:
                    _CURRENT_TASKS[loop] = prev_current

            send_value = None

            # --- classify the yielded value ---
            if yielded is None:
                # Bare `yield` (asyncio.sleep(0) shortcut, or any other
                # cooperative checkpoint).  Stock asyncio's sleep(0) runs one
                # full loop iteration, which INCLUDES a selector poll that
                # delivers pending socket I/O.  pygo's sched_yield only
                # round-robins ready goroutines and bypasses the drain loop's
                # idle netpoll pump (and the aio keepalive keeps it from going
                # idle), so without an explicit poll here a sleep(0) loop never
                # advances I/O parked on other goroutines (e.g. a peer's recv
                # loop) -- breaking the common `await asyncio.sleep(0)` idiom
                # used to let pending reads land.  Deliver ready I/O first,
                # then round-trip through the scheduler so other tasks run.
                try:
                    pygo_core.netpoll_poll()
                except AttributeError:
                    pass    # older pygo_core without the non-blocking pump
                pygo_core.sched_yield_classic()
                continue

            blocking = getattr(yielded, "_asyncio_future_blocking", None)
            if blocking is not True:
                # asyncio's contract: anything yielded from `await` must
                # be a Future-like with _asyncio_future_blocking set to
                # True.  If we get something else, the coro is buggy or
                # used a non-asyncio awaitable; raise into it.
                throw_exc = RuntimeError(
                    "yielded a non-asyncio object from await: %r" % (yielded,))
                continue

            # Mark we've registered our interest (mirrors Task.__step).
            yielded._asyncio_future_blocking = False

            # Fast path: future already resolved at yield time.  Skip
            # the park entirely.  This is the common case for
            # asyncio.gather of finished tasks.
            if yielded.done():
                try:
                    if yielded.cancelled():
                        throw_exc = _fut_cancelled_error(yielded)
                    elif yielded.exception() is not None:
                        throw_exc = yielded.exception()
                    else:
                        send_value = yielded.result()
                except asyncio.CancelledError as e:
                    throw_exc = e
                continue

            # Slow path: park the goroutine until the future fires.
            # Register the wake callback FIRST then call park_self --
            # the race where the future fires synchronously inside
            # add_done_callback is handled by park_safe / wake_safe
            # (wake_pending counter; park is a no-op if wake arrived).
            yielded.add_done_callback(self._wake_unpark)
            self._pgfutwaiter = yielded
            # select-before-wait: deliver any already-ready socket I/O before we
            # park.  Stock asyncio runs one selector poll per loop iteration, so
            # a peer goroutine parked in wait_fd advances even while this side
            # has ready work; pygo only pumps netpoll when its ready ring drains
            # to empty, so without this an `await` that parks here can leave a
            # peer's recv loop starved (e.g. a server's run_asgi never sees a
            # client's close frame before the client's teardown crosses a
            # synchronous server.shutdown() boundary -> 1012 instead of 1001).
            try:
                pygo_core.netpoll_poll()
            except AttributeError:
                pass    # older pygo_core without the non-blocking pump
            pygo_core.park_self()
            self._pgfutwaiter = None

            # We're back.  Cancel() may have propagated into `yielded` (then it
            # wakes us as a cancelled future, handled below) or, if it couldn't,
            # set the one-shot _pgmustcancel -- deliver that now.
            if self._pgmustcancel:
                self._pgmustcancel = False
                try:
                    yielded.remove_done_callback(self._wake_unpark)
                except Exception:
                    pass
                throw_exc = self._make_cancelled_error()
                continue

            try:
                if yielded.cancelled():
                    throw_exc = _fut_cancelled_error(yielded)
                elif yielded.exception() is not None:
                    throw_exc = yielded.exception()
                else:
                    send_value = yielded.result()
            except asyncio.CancelledError as e:
                throw_exc = e

    def _wake_unpark(self, fut):
        # add_done_callback gives us the future; we don't need it.
        if self._self_g is not None:
            self._self_g.wake()


# ====================================================================
# PygoEventLoop -- asyncio.AbstractEventLoop with everything we need
# for sleep / gather / Future / Lock to function.
# ====================================================================
class PygoEventLoop(asyncio.AbstractEventLoop):

    def __init__(self):
        self._running = False
        self._closed  = False
        self._readers = {}
        self._writers = {}
        self._exception_handler = None
        # Thread-safe callback queue + keepalive flag.  call_soon_threadsafe
        # (called from FOREIGN OS threads -- run_in_executor pool workers,
        # aiosqlite's per-Connection thread, etc.) appends here under the lock
        # instead of spawning on the calling thread's scheduler (which is never
        # drained).  A keepalive goroutine spawned in run_until_complete/
        # run_forever drains this queue and keeps the single-thread scheduler
        # from going idle while a goroutine is parked awaiting an external wake.
        self._ts_lock = _threading.Lock()
        self._ts_queue = []
        # Per-run keepalive stop flag, as a 1-element box.  Each
        # run_until_complete gets a FRESH box so a previous run's keepalive
        # goroutine (which may still be parked in the sleep queue when
        # sched_stop broke the drain) can never be revived by a later run
        # resetting a shared bool.  None until the first run.
        self._ka_stop_box = None
        # Set by stop(); observed by the keepalive goroutine (which runs on the
        # loop thread) to break run_forever()/run_until_complete's pygo_core.run().
        self._stopping = False
        # A KeyboardInterrupt / SystemExit raised inside a callback or task must
        # NOT be routed to the exception handler (that's for ordinary
        # exceptions) -- asyncio re-raises these BaseExceptions out of the loop
        # so a Ctrl-C / sys.exit aborts run_until_complete/run_forever.  We
        # stash the first one here and break the drive (sched_stop); _drive
        # re-raises it after pygo_core.run() returns.  None = none pending.
        self._pg_fatal_exc = None
        # Real asyncio loops (BaseEventLoop) expose these; stdlib
        # Future/Task/Timeout machinery and many libraries read them
        # directly (e.g. loop._thread_id, loop._debug).  AbstractEventLoop
        # does not provide them, so add them for compat.  We deliberately
        # do NOT enforce thread affinity (pygo is M:N: callbacks may run
        # on any hub thread), so _thread_id exists purely so attribute
        # reads + asyncio's early-return thread checks succeed.
        self._thread_id = None
        # BaseEventLoop exposes this; libraries (asgiref) read loop._default_executor
        # directly, before ever calling run_in_executor.  Filled in lazily there.
        self._default_executor = None
        # Honour asyncio's debug-mode sources (PYTHONASYNCIODEBUG / -X dev), as
        # BaseEventLoop does via coroutines._is_debug_mode(); libraries + anyio
        # read loop.get_debug() and expect it to reflect the env.
        self._debug = (sys.flags.dev_mode or
                       (not sys.flags.ignore_environment and
                        bool(_os.environ.get("PYTHONASYNCIODEBUG"))))
        try:
            self._clock_resolution = _time.get_clock_info("monotonic").resolution
        except Exception:
            self._clock_resolution = 1e-6

    # ---- state ----
    def is_running(self):  return self._running
    def is_closed(self):   return self._closed
    def get_debug(self):   return self._debug
    def set_debug(self, enabled):  self._debug = bool(enabled)
    def _timer_handle_cancelled(self, handle):
        # asyncio.TimerHandle.cancel() calls this for the loop's timer-heap
        # bookkeeping; pygo schedules timers as goroutines, so it's a no-op.
        pass
    def close(self):
        # The asyncio.run / Runner.close cleanup point (NOT
        # run_until_complete -- that must leave background tasks + parked
        # goroutines alive between calls, e.g. for IsolatedAsyncioTestCase's
        # asyncSetUp -> test -> asyncTearDown on one loop).  Stop the
        # keepalive and tear down outstanding tasks + parked goroutines
        # (accept/recv loops, call_later runners) so they don't leak.
        if self._closed:
            return
        if self._ka_stop_box is not None:
            self._ka_stop_box[0] = True
        self._closed = True
        # Restore any signal handlers we installed (matches asyncio's Unix loop)
        # so they don't leak into the next loop / test.
        for sig in list(getattr(self, "_signal_handlers", {})):
            try:
                self.remove_signal_handler(sig)
            except Exception:
                pass
        try:
            self._cancel_outstanding_tasks()
        except Exception:
            pass

    def _check_closed(self):
        if self._closed:
            raise RuntimeError("Event loop is closed")

    def _check_thread(self):
        # No-op: pygo is M:N, callbacks legitimately run on any hub
        # thread, so enforcing single-thread affinity (as BaseEventLoop
        # does) would raise spurious "non-thread-safe" errors.  The
        # attribute exists (see __init__) for code that reads it.
        return

    def time(self):
        return _time.monotonic()

    # ---- task / future ----
    def create_task(self, coro, *, name=None, context=None):
        self._check_closed()
        if self._can_spawn_here():
            return PygoTask(coro, loop=self, name=name, context=context)
        # Foreign thread: PygoTask.__init__ spawns a goroutine, which would land
        # on the CALLING thread's sched (never drained by this loop).  Marshal
        # the creation onto the loop's own thread via its thread-safe queue and
        # block for the task (mirrors asyncio.run_coroutine_threadsafe).
        box = {}
        ev = _threading.Event()
        def _mk():
            try:
                box["t"] = PygoTask(coro, loop=self, name=name, context=context)
            except BaseException as e:    # pragma: no cover - defensive
                box["e"] = e
            finally:
                ev.set()
        self.call_soon_threadsafe(_mk)
        ev.wait()
        if "e" in box:
            raise box["e"]
        return box["t"]

    def create_future(self):
        return PygoFuture(loop=self)

    # ---- callback scheduling ----
    def call_soon(self, callback, *args, context=None):
        self._check_closed()
        # Off the driver thread, go() would race the ready ring; route through
        # the thread-safe queue (the driver's keepalive runs it).
        if not self._can_spawn_here():
            return self.call_soon_threadsafe(callback, *args, context=context)
        handle = _Handle(callback, args, self, context)
        def runner():
            if not handle._cancelled:
                try:
                    # Run in the Handle's contextvars Context (captured at
                    # construction, or the explicit context=), like stock asyncio
                    # -- so a callback that does create_task/contextvar reads sees
                    # the context active when call_soon was invoked.
                    handle._context.run(callback, *args)
                except (KeyboardInterrupt, SystemExit) as e:
                    # asyncio re-raises these out of the loop (Handle._run);
                    # signal the loop to break the drive and re-raise.
                    self._pg_signal_fatal(e)
                except BaseException as e:
                    self.call_exception_handler({"message": "call_soon callback", "exception": e})
        # asyncio's done-callbacks (gather, wait_for) generally don't
        # yield -- they just walk children + set the outer future.
        # We use go_noyield to skip the per-g snap dance.  If a user
        # ever passes a callback that DOES yield, go_noyield's
        # behaviour is undefined; switch back to pygo_core.go.
        pygo_core.go(runner)
        return handle

    def call_soon_threadsafe(self, callback, *args, context=None):
        # Raise on a closed loop (asyncio parity).  asgiref relies on this to
        # detect a dead main_event_loop and fall back to a fresh loop+thread;
        # without it, async_to_sync schedules onto the closed loop and pumps
        # run_until_future() forever -- the AsyncSingleThreadContext suite hang
        # (and the sync_to_async(thread_sensitive=True) deadlock).
        self._check_closed()
        # Thread-safe: may be called from ANY OS thread.  Enqueue under the
        # lock; the keepalive goroutine on the loop thread drains and runs it.
        # We do NOT pygo_core.go() here -- from a foreign thread that would
        # spawn onto that thread's own (never-drained) scheduler.
        # _Handle captures copy_context() HERE on the calling thread (or honours
        # context=), so a contextvar set by the caller propagates to the drained
        # callback -- this is how anyio's portal carries the caller-thread
        # context into a run_coroutine_threadsafe-spawned task.
        handle = _Handle(callback, args, self, context)
        with self._ts_lock:
            self._ts_queue.append(handle)
        return handle

    def _drain_ts_queue(self):
        """Run all callbacks enqueued via call_soon_threadsafe.  Called from
        the keepalive goroutine on the loop thread."""
        with self._ts_lock:
            if not self._ts_queue:
                return
            pending, self._ts_queue = self._ts_queue, []
        for handle in pending:
            if handle._cancelled:
                continue
            try:
                handle._context.run(handle._callback, *handle._args)
            except (KeyboardInterrupt, SystemExit) as e:
                # asyncio re-raises these out of the loop; break the drive.
                self._pg_signal_fatal(e)
            except BaseException as e:
                self.call_exception_handler(
                    {"message": "call_soon_threadsafe callback", "exception": e})

    def _spawn_keepalive(self):
        """Spawn the goroutine that drains the thread-safe queue and keeps the
        scheduler alive while the run is in progress.  Idempotent per run."""
        stop = [False]
        self._ka_stop_box = stop
        def _keepalive(stop=stop):
            # Poll the cross-thread queue.  sched_sleep keeps sleep_size>0 so
            # pygo_sched_drain stays in its loop (a bare-parked goroutine alone
            # would let it return idle) and re-checks the cross-thread wake list
            # each wake.  2ms bounds foreign-wake latency; cheap for a test run.
            # `stop` is this run's private box -- a later run can't revive us.
            while not stop[0] and not self._closed and not self._stopping:
                self._drain_ts_queue()
                pygo_core.sched_sleep(0.002)
            # Drain once more so a stop()-companion callback (e.g. the
            # task.cancel() loop aiosmtpd queues alongside loop.stop()) runs.
            self._drain_ts_queue()
            if self._stopping:
                # An explicit loop.stop() must unwind run_forever()'s (or a
                # run_until_complete's) pygo_core.run().  sched_stop() acts on
                # THIS thread's scheduler, and the keepalive always runs on the
                # loop thread, so this is the one safe place to call it -- even
                # when stop() was invoked from a FOREIGN thread and merely
                # drained onto us via call_soon_threadsafe (exactly how
                # aiosmtpd's threaded Controller.stop() reaches the loop).
                try:
                    pygo_core.sched_stop()
                except Exception:
                    pass
        pygo_core.go(_keepalive)

    def call_later(self, delay, callback, *args, context=None):
        # Mirror asyncio: call_later is call_at(self.time() + delay, ...).
        return self.call_at(self.time() + delay, callback, *args,
                            context=context)

    def call_at(self, when, callback, *args, context=None):
        self._check_closed()
        # Store `when` VERBATIM in the handle, exactly like asyncio -- callers
        # read handle._when back and rely on the value (and its int-ness) they
        # passed.  aiohttp's TimeoutHandle.start() does when = ceil(loop.time()
        # + timeout) then asserts loop.call_at(when, ...)._when == that int;
        # the old round-trip through call_later (self.time() + (when -
        # self.time())) both drifted the value and forced it to float.
        handle = _TimerHandle(callback, args, self, when, context)
        def runner():
            pygo_core.sched_sleep(max(0.0, when - self.time()))
            if not handle._cancelled:
                try:
                    handle._context.run(callback, *args)
                except (KeyboardInterrupt, SystemExit) as e:
                    # asyncio re-raises these out of the loop; break the drive.
                    self._pg_signal_fatal(e)
                except BaseException as e:
                    # Keep this minimal -- printing a traceback from here
                    # can itself recurse if we're near the c_recursion limit.
                    sys.stderr.write("[pygo.aio] timer cb: %r\n" % (e,))
        if self._can_spawn_here():
            pygo_core.go(runner)
        else:
            # Foreign thread: spawn the timer goroutine on the loop's own thread.
            self.call_soon_threadsafe(lambda: pygo_core.go(runner))
        return handle

    # ---- I/O readers / writers (level-triggered, matches selector loops) ----
    def add_reader(self, fd, callback, *args):
        self._add_io(fd, 1, callback, args, self._readers)

    def remove_reader(self, fd):
        return self._remove_io(fd, self._readers)

    def add_writer(self, fd, callback, *args):
        self._add_io(fd, 2, callback, args, self._writers)

    def remove_writer(self, fd):
        return self._remove_io(fd, self._writers)

    def _add_io(self, fd, evt, callback, args, table):
        if fd in table:
            table[fd]._cancelled = True
        handle = _Handle(callback, args, self)
        table[fd] = handle
        def runner():
            while not handle._cancelled:
                try:
                    _wait_fd(fd, evt)
                except Exception:
                    return
                if handle._cancelled:
                    return
                try:
                    callback(*args)
                except Exception as e:
                    self.call_exception_handler({"message": "I/O callback", "exception": e})
                # Yield to scheduler before re-arming (mimic level-triggered).
                pygo_core.sched_yield_classic()
        pygo_core.go(runner)
        return handle

    def _remove_io(self, fd, table):
        h = table.pop(fd, None)
        if h is not None:
            h._cancelled = True
            return True
        return False

    # ---- Network: high-level loop APIs ----
    async def create_datagram_endpoint(self, protocol_factory, **kw):
        return await _create_datagram_endpoint(self, protocol_factory, **kw)

    # ---- subprocesses (thread-backed) ----
    # AbstractEventLoop.subprocess_exec/shell -- asyncio.create_subprocess_exec/
    # _shell route through these.  pygo's netpoll can't portably select() on
    # child stdio pipes (esp. Windows anonymous pipes), so we drive each pipe on
    # its own OS thread and marshal data/exit back onto the loop thread via
    # call_soon_threadsafe -- exactly how run_in_executor already bridges blocking
    # work.  Returns (SubprocessTransport, protocol) like stock asyncio.
    async def subprocess_exec(self, protocol_factory, program, *args,
                              stdin=_subprocess.PIPE, stdout=_subprocess.PIPE,
                              stderr=_subprocess.PIPE, **kwargs):
        protocol = protocol_factory()
        transport = _SubprocessTransport(
            self, protocol, [program] + list(args), shell=False,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        return transport, protocol

    async def subprocess_shell(self, protocol_factory, cmd,
                               stdin=_subprocess.PIPE, stdout=_subprocess.PIPE,
                               stderr=_subprocess.PIPE, **kwargs):
        protocol = protocol_factory()
        transport = _SubprocessTransport(
            self, protocol, cmd, shell=True,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        return transport, protocol

    # ---- pipe transports (thread-backed, like subprocess) ----
    # connect_read_pipe / connect_write_pipe wrap an arbitrary readable/writable
    # pipe or file object in a transport driving a standard Protocol.  Used by
    # aioconsole + libs doing async stdio.  Same thread-bridge as subprocess.
    async def connect_read_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        transport = _ReadPipeTransport(self, pipe, protocol)
        return transport, protocol

    async def connect_write_pipe(self, protocol_factory, pipe):
        protocol = protocol_factory()
        transport = _WritePipeTransport(self, pipe, protocol)
        return transport, protocol

    async def create_connection(self, protocol_factory, host=None, port=None, *,
                                ssl=None, family=0, proto=0, flags=0, sock=None,
                                local_addr=None, server_hostname=None,
                                ssl_handshake_timeout=None, **_ignored):
        """Lower-level create_connection.  Returns (transport, protocol).
        Builds a TCP socket + thin Transport over our Stream classes;
        protocol's connection_made / data_received / connection_lost
        get fired."""
        if sock is None:
            infos = _resolve(host, port, family or _socket.AF_UNSPEC,
                             _socket.SOCK_STREAM, proto, flags)
            last_err = None
            for fam, typ, prt, _canon, sa in infos:
                try:
                    s = _socket.socket(fam, typ, prt)
                    s.setblocking(False)
                    if local_addr is not None:
                        s.bind(local_addr)
                    try:
                        s.connect(sa)
                    except BlockingIOError:
                        _wait_fd(s.fileno(), 2)
                        err = s.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                        if err != 0:
                            raise OSError(err, "connect failed")
                    sock = s
                    break
                except OSError as e:
                    last_err = e
                    try: s.close()
                    except OSError: pass
            if sock is None:
                raise last_err or OSError("could not connect")
        else:
            sock.setblocking(False)
        if ssl is not None:
            sock = _tls_wrap_client(sock, ssl, server_hostname, host,
                                    ssl_handshake_timeout)
        protocol = protocol_factory()
        transport = _StreamTransport(sock, protocol, loop=self)
        return transport, protocol

    async def create_server(self, protocol_factory, host=None, port=None, *,
                            family=_socket.AF_UNSPEC, flags=_socket.AI_PASSIVE,
                            sock=None, backlog=100, ssl=None,
                            reuse_address=None, reuse_port=None,
                            ssl_handshake_timeout=None, **_ignored):
        if sock is not None:
            sock.setblocking(False)
            socks = [sock]
        else:
            # asyncio binds EVERY address getaddrinfo returns (one socket each),
            # not just the first -- so "localhost" listens on both 127.0.0.1 and
            # ::1.  The old code break'd after the first bind, which left no IPv4
            # socket whenever getaddrinfo sorts IPv6 first (Windows), so callers
            # that look for an AF_INET socket (websockets' get_host_port) failed.
            if host == "" or host is None:
                hosts = [None]
            elif isinstance(host, str):
                hosts = [host]
            else:
                hosts = list(host)
            # asyncio default: SO_REUSEADDR on POSIX only -- on Windows it lets a
            # second bind hijack the port, so it stays off there by default.
            if reuse_address is None:
                reuse_address = (_os.name == "posix" and sys.platform != "cygwin")
            infos = []
            seen = set()
            for hst in hosts:
                for info in _resolve(hst, port, family,
                                     _socket.SOCK_STREAM, 0, flags):
                    fam, typ, prt, _canon, sa = info
                    key = (fam, sa)
                    if key in seen:
                        continue
                    seen.add(key)
                    infos.append(info)
            socks = []
            last_err = None
            completed = False
            try:
                for fam, typ, prt, _canon, sa in infos:
                    try:
                        s = _socket.socket(fam, typ, prt)
                    except OSError:
                        # getaddrinfo can return a family the host can't create
                        # (e.g. AF_INET6 with IPv6 disabled) -- skip it.
                        continue
                    socks.append(s)
                    if reuse_address:
                        s.setsockopt(_socket.SOL_SOCKET,
                                     _socket.SO_REUSEADDR, 1)
                    if reuse_port and hasattr(_socket, "SO_REUSEPORT"):
                        s.setsockopt(_socket.SOL_SOCKET,
                                     _socket.SO_REUSEPORT, 1)
                    # Keep the IPv6 wildcard socket from also grabbing the IPv4
                    # wildcard (dual-stack) and colliding with the AF_INET bind.
                    if (fam == _socket.AF_INET6
                            and hasattr(_socket, "IPPROTO_IPV6")
                            and hasattr(_socket, "IPV6_V6ONLY")):
                        try:
                            s.setsockopt(_socket.IPPROTO_IPV6,
                                         _socket.IPV6_V6ONLY, 1)
                        except OSError:
                            pass
                    s.setblocking(False)
                    try:
                        s.bind(sa)
                    except OSError as e:
                        last_err = OSError(
                            e.errno,
                            "error while attempting to bind on address %r: %s"
                            % (sa, e.strerror))
                        raise last_err
                completed = True
            finally:
                if not completed:
                    for s in socks:
                        _close_sock(s)
            if not socks:
                raise last_err or OSError("could not bind to any address")
        # listen() on EVERY socket -- including a caller-supplied sock= (asyncio's
        # create_server(sock=...) always listens on it).  aiohttp's TestServer
        # pre-binds a socket and hands it over un-listened via SockSite(sock=...);
        # without this it stayed bound-but-not-listening and every client got
        # ECONNREFUSED.  listen() on an already-listening socket is harmless.
        for s in socks:
            s.listen(backlog)
        # cb=None: caller wired up via protocol factory + Transport.
        # We still need an accept loop per socket that builds Transports per conn.
        return _ProtocolServer(socks, protocol_factory, loop=self, ssl_context=ssl,
                               ssl_handshake_timeout=ssl_handshake_timeout)

    # ---- Unix domain sockets (loop.create_unix_server / _connection) ----
    # The base class raises NotImplementedError; UDS is common for local IPC
    # (uvicorn/gunicorn --uds, database sockets).  Mirror create_server /
    # create_connection with an AF_UNIX socket.
    async def create_unix_server(self, protocol_factory, path=None, *, sock=None,
                                 backlog=100, ssl=None,
                                 ssl_handshake_timeout=None, **_ignored):
        if sock is None:
            if path is None:
                raise ValueError("path was not specified, and no sock specified")
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            try:
                sock.bind(path)
            except OSError as e:
                sock.close()
                if e.errno == _errno.EADDRINUSE:
                    raise OSError(e.errno, "Address %r is already in use" % (path,))
                raise
            except Exception:
                sock.close()
                raise
        sock.setblocking(False)
        sock.listen(backlog)
        return _ProtocolServer([sock], protocol_factory, loop=self,
                               ssl_context=ssl,
                               ssl_handshake_timeout=ssl_handshake_timeout)

    async def create_unix_connection(self, protocol_factory, path=None, *,
                                     ssl=None, sock=None, server_hostname=None,
                                     ssl_handshake_timeout=None, **_ignored):
        if sock is None:
            if path is None:
                raise ValueError("no path and no sock were specified")
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                sock.connect(path)
            except BlockingIOError:
                _wait_fd(sock.fileno(), 2)
                err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                if err != 0:
                    sock.close()
                    raise OSError(err, "connect failed")
        else:
            sock.setblocking(False)
        if ssl is not None:
            sock = _tls_wrap_client(sock, ssl, server_hostname, None,
                                    ssl_handshake_timeout)
        protocol = protocol_factory()
        transport = _StreamTransport(sock, protocol, loop=self)
        return transport, protocol

    async def sendfile(self, transport, file, offset=0, count=None, *,
                       fallback=True):
        """asyncio.loop.sendfile.  We have no OS sendfile path, so do the
        portable read+write fallback (asyncio falls back to this too when the
        native path is unavailable).  Used by aiohttp's FileResponse etc.; the
        base class raises NotImplementedError.  Blocking file reads are offloaded
        so they don't wedge the loop."""
        if transport.is_closing():
            raise RuntimeError("Transport is closing")
        if not fallback:
            # Caller demanded the native path, which pygo transports lack.
            raise asyncio.SendfileNotAvailableError(
                "sendfile syscall path is not available on pygo transports")
        if offset:
            await self.run_in_executor(None, file.seek, offset)
        blocksize = 16384
        total = 0
        while True:
            want = blocksize
            if count is not None:
                want = min(blocksize, count - total)
                if want <= 0:
                    break
            data = await self.run_in_executor(None, file.read, want)
            if not data:
                break
            transport.write(data)
            total += len(data)
        return total

    async def start_tls(self, transport, protocol, sslcontext, *,
                        server_side=False, server_hostname=None,
                        ssl_handshake_timeout=None, ssl_shutdown_timeout=None,
                        **_ignored):
        """Upgrade an existing connection to TLS in place (STARTTLS, asyncpg
        SSL).  AbstractEventLoop raises NotImplementedError.  Quiesce the
        plaintext transport's recv loop (without closing the fd), wrap the same
        socket in cooperative TLS, handshake, and return a new transport over
        the TLS socket reusing the SAME protocol (connection_made is not
        re-fired, matching asyncio)."""
        sock = getattr(transport, "_sock", None)
        if sock is None:
            raise TypeError("transport does not expose a socket for start_tls")
        # Stop the plaintext recv loop consuming the fd; it exits WITHOUT
        # closing the socket (TLS takes fd ownership).  Suppress its
        # connection_lost so the protocol stays "connected" across the upgrade.
        transport._paused = True
        transport._stopping = True
        transport._conn_lost_called = True
        transport._closed = True
        await asyncio.sleep(0)   # give the old recv loop a turn to observe + exit
        tls = _TLSSock(sock, sslcontext, server_side=server_side,
                       server_hostname=server_hostname)
        tls.do_handshake(ssl_handshake_timeout)
        return _StreamTransport(tls, protocol, loop=self,
                                call_connection_made=False)

    async def connect_accepted_socket(self, protocol_factory, sock, *, ssl=None,
                                      ssl_handshake_timeout=None, **_ignored):
        """Wrap an already-accepted socket into a transport (server side).
        AbstractEventLoop raises NotImplementedError; servers that accept()
        manually (some test harnesses, custom acceptors) hand the socket here."""
        sock.setblocking(False)
        if ssl is not None:
            tls = _TLSSock(sock, ssl, server_side=True)
            tls.do_handshake(ssl_handshake_timeout)
            sock = tls
        protocol = protocol_factory()
        transport = _StreamTransport(sock, protocol, loop=self)
        return transport, protocol

    async def getaddrinfo(self, host, port, *, family=0, type=0, proto=0, flags=0):
        # Offloaded to the blocking pool so DNS doesn't wedge the hub.
        # monkey.py may still patch this to a cooperative resolver.
        return _resolve(host, port, family, type, proto, flags)

    async def getnameinfo(self, sockaddr, flags=0):
        return _socket.getnameinfo(sockaddr, flags)

    # ---- low-level socket ops (loop.sock_*) ----
    async def sock_connect(self, sock, address):
        sock.setblocking(False)
        try:
            sock.connect(address)
        except BlockingIOError:
            _wait_fd(sock.fileno(), 2)
            err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
            if err != 0:
                raise OSError(err, "connect failed")

    async def sock_accept(self, sock):
        sock.setblocking(False)
        while True:
            try:
                return sock.accept()
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recv(self, sock, nbytes):
        sock.setblocking(False)
        while True:
            try:
                return sock.recv(nbytes)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recv_into(self, sock, buf):
        sock.setblocking(False)
        while True:
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recvfrom(self, sock, bufsize):
        sock.setblocking(False)
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recvfrom_into(self, sock, buf, nbytes=0):
        # asyncio 3.11+ API; base class raises NotImplementedError.
        sock.setblocking(False)
        while True:
            try:
                return sock.recvfrom_into(buf, nbytes)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_sendfile(self, sock, file, offset=0, count=None, *,
                            fallback=True):
        # No OS sendfile path on pygo; mirror asyncio's "native unavailable"
        # signal so callers fall back to read+send (loop.sendfile handles the
        # transport-level fallback).
        raise asyncio.SendfileNotAvailableError(
            "sock_sendfile syscall path is not available on pygo")

    async def sock_sendall(self, sock, data):
        sock.setblocking(False)
        view = memoryview(data)
        sent = 0
        while sent < len(view):
            try:
                n = sock.send(view[sent:])
                sent += n
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 2)

    async def sock_sendto(self, sock, data, address):
        sock.setblocking(False)
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 2)

    # ---- executor (thread pool) ----
    def run_in_executor(self, executor, func, *args):
        """Run func(*args) on a thread pool.  Returns a PygoFuture
        that resolves when the thread completes.  We hand out a real
        threadpool via concurrent.futures."""
        import concurrent.futures as _cf
        if executor is None:
            # Lazy-init default pool.
            if self._default_executor is None:
                self._default_executor = _cf.ThreadPoolExecutor(max_workers=8)
            executor = self._default_executor
        fut = PygoFuture(loop=self)
        cf_fut = executor.submit(func, *args)
        def _on_thread_done(_cf_fut):
            # Marshal the thread's result back into our PygoFuture.
            # call_soon_threadsafe wakes the loop.
            def _set():
                if cf_fut.cancelled():
                    fut.cancel()
                elif cf_fut.exception() is not None:
                    fut.set_exception(cf_fut.exception())
                else:
                    fut.set_result(cf_fut.result())
            try:
                self.call_soon_threadsafe(_set)
            except RuntimeError:
                # Loop closed before the pool thread finished -- nothing to
                # resolve into; drop the result (matches stock asyncio, whose
                # wrap_future done-callback no-ops once the loop is closed).
                pass
        cf_fut.add_done_callback(_on_thread_done)
        return fut

    def set_default_executor(self, executor):
        """asyncio.AbstractEventLoop.set_default_executor.  Used by
        run_in_executor(None, ...).  Libraries (aiomisc) inject their own
        thread pool through this; the base class raises NotImplementedError."""
        self._default_executor = executor

    # ---- Unix signals (loop.add_signal_handler) ----
    # The base class raises NotImplementedError; servers (uvicorn, hypercorn,
    # aiohttp) install SIGINT/SIGTERM handlers for graceful shutdown, so without
    # this they can't run under pygo.  signal.signal must be called from the
    # main thread (asyncio has the same constraint); the handler itself runs on
    # the main thread, and we marshal the user callback onto the loop thread via
    # call_soon_threadsafe so it runs cooperatively like asyncio's wakeup-fd path.
    def add_signal_handler(self, sig, callback, *args):
        import signal as _signal
        if _threading.current_thread() is not _threading.main_thread():
            raise ValueError("add_signal_handler() can only be called from the "
                             "main thread")
        self._check_closed()
        handle = _Handle(callback, args, self)
        if not hasattr(self, "_signal_handlers"):
            self._signal_handlers = {}
        # Dispatch via signal.set_wakeup_fd + a self-pipe, exactly like
        # asyncio's Unix loop -- NOT via our own signal.signal() callback.
        # Servers (uvicorn, hypercorn) install their OWN
        # signal.signal(sig, handle_exit) for graceful shutdown, which would
        # clobber a Python-level handler we set and silently drop the user's
        # callback.  CPython, however, still writes the signum to the wakeup fd
        # for whatever Python handler is current, so the loop-side dispatch off
        # that pipe survives a server overriding signal.signal().
        self._setup_signal_wakeup()
        try:
            # A handler must be installed for CPython to write to the wakeup fd;
            # a no-op suffices (real work is loop-side).  siginterrupt(False)
            # so the wakeup doesn't EINTR a syscall on the main thread.
            _signal.signal(sig, _signal_wakeup_noop)
            try:
                _signal.siginterrupt(sig, False)
            except (OSError, ValueError):
                pass
        except (ValueError, OSError, RuntimeError) as e:
            raise RuntimeError(str(e))
        self._signal_handlers[sig] = handle

    def _setup_signal_wakeup(self):
        if getattr(self, "_signal_wakeup_setup", False):
            return
        import signal as _signal
        self._signal_rsock, self._signal_wsock = _socket.socketpair()
        self._signal_rsock.setblocking(False)
        self._signal_wsock.setblocking(False)
        try:
            self._signal_old_wakeup_fd = _signal.set_wakeup_fd(
                self._signal_wsock.fileno(), warn_on_full_buffer=False)
        except TypeError:    # pre-3.7 signature; shouldn't happen on 3.12+
            self._signal_old_wakeup_fd = _signal.set_wakeup_fd(
                self._signal_wsock.fileno())
        # Drain the pipe on the loop and dispatch each pending signum's handler.
        self.add_reader(self._signal_rsock.fileno(), self._read_signal_wakeup)
        self._signal_wakeup_setup = True

    def _read_signal_wakeup(self):
        try:
            data = self._signal_rsock.recv(4096)
        except (BlockingIOError, InterruptedError):
            return
        except OSError:
            return
        if not data:
            return
        handlers = getattr(self, "_signal_handlers", None)
        if not handlers:
            return
        for signum in data:
            handle = handlers.get(signum)
            if handle is not None and not handle._cancelled:
                # Run the user callback on the loop in the Handle's captured
                # context (matches asyncio's _handle_signal -> _add_callback).
                try:
                    self.call_soon(handle._callback, *handle._args,
                                   context=handle._context)
                except RuntimeError:
                    pass

    def remove_signal_handler(self, sig):
        import signal as _signal
        handlers = getattr(self, "_signal_handlers", None)
        if not handlers or sig not in handlers:
            return False
        handlers.pop(sig)._cancelled = True
        try:
            if sig == _signal.SIGINT:
                _signal.signal(sig, _signal.default_int_handler)
            else:
                _signal.signal(sig, _signal.SIG_DFL)
        except (ValueError, OSError):
            pass
        if not handlers:
            self._teardown_signal_wakeup()
        return True

    def _teardown_signal_wakeup(self):
        if not getattr(self, "_signal_wakeup_setup", False):
            return
        import signal as _signal
        try:
            self.remove_reader(self._signal_rsock.fileno())
        except Exception:
            pass
        try:
            _signal.set_wakeup_fd(self._signal_old_wakeup_fd
                                  if self._signal_old_wakeup_fd is not None
                                  else -1)
        except (ValueError, OSError):
            pass
        for s in (getattr(self, "_signal_rsock", None),
                  getattr(self, "_signal_wsock", None)):
            try:
                if s is not None:
                    s.close()
            except OSError:
                pass
        self._signal_rsock = None
        self._signal_wsock = None
        self._signal_wakeup_setup = False

    # ---- run loop ----
    # ---- per-thread run machinery (Phase C: one sched per OS thread) ----
    def _can_spawn_here(self):
        """True iff pygo_core.go is safe on the CALLING thread for THIS loop:
        we are the thread running the loop (go() lands on our own sched) or the
        loop isn't running yet (the calling thread will drive it).  A FOREIGN
        thread must marshal spawns via call_soon_threadsafe -- its go() would
        land on ITS thread's sched, which this loop never drains."""
        tid = self._thread_id
        return tid is None or tid == _threading.get_ident()

    def _drive(self):
        """Drain THIS thread's scheduler until the run's stop fires.  Each loop
        runs on its own OS thread and drains its own (thread-local) sched, so
        loops on different threads are INDEPENDENT: one thread blocking
        synchronously inside a coroutine (run_coroutine_threadsafe().result(),
        anyio BlockingPortal, a threaded server controller with a blocking
        client) freezes only its own sched, never the others'.  pygo_core.run()
        returns when sched_stop fires (the future-done callback, or the
        keepalive observing loop.stop()) or the scheduler empties."""
        self._running = True
        self._thread_id = _threading.get_ident()
        asyncio._set_running_loop(self)
        try:
            pygo_core.run()
        finally:
            self._running = False
            self._thread_id = None
            asyncio._set_running_loop(None)
            # Retire the keepalive so it can't linger parked in the sleep queue
            # into the next run on this loop.
            if self._ka_stop_box is not None:
                self._ka_stop_box[0] = True
        # A KeyboardInterrupt / SystemExit raised in a callback or task during
        # the drive was stashed by _pg_signal_fatal (which sched_stop'd us out
        # of pygo_core.run()).  Re-raise it so it propagates out of
        # run_until_complete / run_forever, as asyncio does.  Pop it first so a
        # subsequent run on this loop (asyncio.Runner cleanup) starts clean.
        fatal = self._pg_fatal_exc
        if fatal is not None:
            self._pg_fatal_exc = None
            raise fatal

    def run_until_complete(self, future):
        if asyncio.iscoroutine(future):
            future = self.create_task(future)
        elif not (isinstance(future, asyncio.Future)
                  or isinstance(future, PygoFuture)
                  or asyncio.isfuture(future)):
            raise TypeError("argument must be a Future or coroutine")
        # Resolve deep, non-yielding stdlib imports (e.g. getaddrinfo's
        # first-call codec import) before any goroutine runs them on a small
        # stack -- see prewarm_stdlib.
        _runtime.prewarm_stdlib()
        # Clear any stale stop request from a prior run on this loop.
        self._stopping = False
        if not future.done():
            # When the user-visible future completes, break our drain (matches
            # asyncio.run -- don't block on background accept/ticker goroutines
            # the user didn't join).
            def _stop_on_done(_fut):
                box = self._ka_stop_box
                if box is not None:
                    box[0] = True
                pygo_core.sched_stop()
            future.add_done_callback(_stop_on_done)
            self._spawn_keepalive()
            # Remove the stop callback when the drive returns, no matter HOW it
            # returns (future done, KeyboardInterrupt out of a callback, or the
            # scheduler emptying) -- exactly as stock asyncio's
            # run_until_complete does in its finally.  Otherwise a future that
            # this run abandoned (e.g. a task left parked when a Ctrl-C aborted
            # the run) keeps the stale callback, and when a LATER run completes
            # that task its _stop_on_done fires and sched_stop()s the wrong
            # drive -- breaking it before its own future is done -> a spurious
            # "event loop stopped before Future completed" that masks the
            # original KeyboardInterrupt (asyncio.Runner cleanup hits this).
            try:
                self._drive()
            finally:
                future.remove_done_callback(_stop_on_done)
        # IMPORTANT: do NOT cancel outstanding tasks / sched_reset here.
        # run_until_complete must leave other tasks + parked goroutines ALIVE
        # (IsolatedAsyncioTestCase / asyncio.Runner reuse one loop across
        # asyncSetUp / test / asyncTearDown).  asyncio.run-style teardown
        # lives in close().
        if not future.done():
            raise RuntimeError("event loop stopped before Future completed")
        return future.result()

    def _cancel_outstanding_tasks(self):
        """Cancel every PygoTask still alive on this loop and clear
        the scheduler's leftover state.  Called from run_until_complete
        after the main future resolves so background goroutines
        (call_later runners, accept loops, ticker goroutines) don't
        leak into the next paio.run.

        Strategy: cancel all known tasks (best-effort -- not all are
        interruptible), then sched_reset() the scheduler's ready+sleep
        queues so the next pygo_core.run() sees a clean slate."""
        try:
            tasks = [t for t in asyncio.all_tasks(self) if t._loop is self]
        except Exception:
            tasks = []
        for t in tasks:
            try:
                t.cancel()
            except Exception:
                pass
        # Forcibly drop anything still scheduled.  Goroutines parked on
        # netpoll/wake/chan that aren't interrupted by cancel get
        # abandoned; the underlying coro and snap are freed when the
        # last Python reference drops.
        # Only drain the shared per-thread scheduler if NO sibling loop still
        # has live tasks on it.  The pygo scheduler is one-per-OS-thread, shared
        # by every PygoEventLoop on the thread; a blind sched_reset here would
        # bulldoze another loop's still-needed goroutines -- e.g. a background
        # server task's in-flight asyncio.sleep sitting in the shared sleep heap
        # -- deadlocking that loop when it is next driven (the hypercorn /
        # pytest-asyncio fixture-vs-test multi-loop case).
        sibling_busy = any(
            (t._loop is not self and not t.done()) for t in list(_PG_ALL_TASKS))
        if not sibling_busy:
            try:
                pygo_core.sched_reset()
            except AttributeError:
                pass  # Older build without sched_reset; best-effort drain.

    def run_forever(self):
        # Resolve deep, non-yielding stdlib imports (e.g. getaddrinfo's
        # first-call codec import) before any goroutine runs them on a small
        # stack -- see prewarm_stdlib.
        _runtime.prewarm_stdlib()
        # Do NOT reset self._stopping here.  asyncio honors a stop() issued
        # BEFORE run_forever() -- it runs one iteration and returns (stock checks
        # self._stopping at the top of each loop pass and only clears it on
        # EXIT).  Resetting it at entry wipes that request, so the keepalive
        # goroutine never sees the stop and spins sched_sleep forever -- the
        # `loop.stop(); loop.run_forever()` cleanup idiom (aiohttp's synchronous
        # test_streams/test_web_app default-loop tests) hangs.  When _stopping is
        # already True the keepalive calls sched_stop() on its first pass and the
        # drive returns immediately.
        self._spawn_keepalive()
        try:
            self._drive()
        finally:
            self._stopping = False

    def stop(self):
        # asyncio contract: request the loop stop after the current iteration.
        # Setting the flag is thread-safe (a plain bool store); the keepalive
        # goroutine -- which runs on the loop thread -- observes it and calls
        # pygo_core.sched_stop() to return from run_forever()'s pygo_core.run().
        # Works whether stop() is called directly on the loop thread or, per
        # asyncio's rules, via call_soon_threadsafe() from another thread.
        self._stopping = True

    def _pg_signal_fatal(self, exc):
        """Record a KeyboardInterrupt / SystemExit raised inside a callback or
        task and break the drive so it propagates OUT of the current run.

        asyncio routes ordinary callback/task exceptions to the exception
        handler, but re-raises (KeyboardInterrupt, SystemExit) out of the loop
        (Handle._run / Task.__step re-raise them) so Ctrl-C and sys.exit abort
        run_until_complete / run_forever.  We can't unwind the C drain through a
        goroutine's raise, so we stash the first such exception here and call
        sched_stop() to return from pygo_core.run(); _drive re-raises it.

        Always called on the loop thread (every callback/task runner runs
        there), so sched_stop() targets this thread's scheduler."""
        if self._pg_fatal_exc is None:
            self._pg_fatal_exc = exc
        if self._ka_stop_box is not None:
            self._ka_stop_box[0] = True
        try:
            pygo_core.sched_stop()
        except Exception:
            pass

    # asyncio.run() shutdown protocol -- minimal no-ops so user code
    # written against asyncio.run works through `paio.install()`.
    async def shutdown_asyncgens(self):
        return None

    async def shutdown_default_executor(self, timeout=None):
        return None

    def get_task_factory(self):
        return None

    def set_task_factory(self, factory):
        pass

    # ---- exception handling ----
    def set_exception_handler(self, handler):
        self._exception_handler = handler

    def get_exception_handler(self):
        return self._exception_handler

    def default_exception_handler(self, context):
        # Log through the "asyncio" logger like stock asyncio (not raw stderr),
        # so logging config + pytest's caplog (e.g. async-lru's
        # test_done_callback_exception_logs) see it.
        import logging
        message = context.get("message") or "Unhandled exception in event loop"
        exc = context.get("exception")
        exc_info = (type(exc), exc, exc.__traceback__) if exc is not None else False
        log_lines = [message]
        for key in sorted(context):
            if key in ("message", "exception"):
                continue
            log_lines.append("%s: %r" % (key, context[key]))
        logging.getLogger("asyncio").error("\n".join(log_lines), exc_info=exc_info)

    def call_exception_handler(self, context):
        if self._exception_handler is not None:
            try:
                self._exception_handler(self, context)
                return
            except Exception:
                pass
        self.default_exception_handler(context)


# ====================================================================
# Policy + convenience entry points
# ====================================================================
# ====================================================================
# Network: open_connection / start_server with StreamReader/Writer.
#
# We bypass asyncio's Transport/Protocol stack entirely.  Each connection
# is a pygo goroutine doing cooperative socket I/O via wait_fd.  The
# StreamReader/Writer classes we hand to user code present the standard
# asyncio API surface (read / readline / readuntil / readexactly /
# write / drain / close) so existing async TCP code Just Works.
# ====================================================================
class StreamReader(object):
    """asyncio.StreamReader-compatible reader backed by cooperative
    socket recv.  Implements: read, readline, readuntil, readexactly,
    at_eof, feed_eof.  Buffers internally so readline / readuntil
    don't have to issue per-byte recvs."""

    def __init__(self, sock, *, limit=2**16, loop=None):
        self._sock = sock
        self._buf  = bytearray()
        self._eof  = False
        self._limit = limit
        self._loop = loop

    def at_eof(self):
        return self._eof and not self._buf

    def feed_eof(self):
        self._eof = True

    def _fill(self):
        """Block (cooperatively) until at least one chunk arrives, or
        the peer closes.  Returns True if data was read, False at EOF."""
        if self._eof:
            return False
        while True:
            try:
                chunk = self._sock.recv(self._limit)
            except (BlockingIOError, InterruptedError):
                _wait_fd(self._sock.fileno(), 1)
                continue
            except OSError as e:
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK, _errno.EINTR):
                    _wait_fd(self._sock.fileno(), 1)
                    continue
                raise
            if not chunk:
                self._eof = True
                return False
            self._buf.extend(chunk)
            return True

    async def read(self, n=-1):
        """Read up to n bytes (-1 = until EOF)."""
        if n == 0:
            return b""
        if n < 0:
            # Read until EOF.
            while not self._eof:
                self._fill()
            data, self._buf = bytes(self._buf), bytearray()
            return data

        # n > 0: ensure we have at least one byte, then return up to n.
        while not self._buf and not self._eof:
            self._fill()
        if not self._buf:
            return b""
        take = min(n, len(self._buf))
        data = bytes(self._buf[:take])
        del self._buf[:take]
        return data

    async def readexactly(self, n):
        """Read exactly n bytes, or raise asyncio.IncompleteReadError."""
        while len(self._buf) < n:
            if not self._fill():
                # EOF -- partial data.
                partial = bytes(self._buf)
                self._buf.clear()
                raise asyncio.IncompleteReadError(partial, n)
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    async def readuntil(self, separator=b"\n"):
        """Read until separator (inclusive), or raise
        asyncio.IncompleteReadError on EOF."""
        seplen = len(separator)
        while True:
            idx = self._buf.find(separator)
            if idx >= 0:
                end = idx + seplen
                data = bytes(self._buf[:end])
                del self._buf[:end]
                return data
            if not self._fill():
                partial = bytes(self._buf)
                self._buf.clear()
                raise asyncio.IncompleteReadError(partial, None)

    async def readline(self):
        try:
            return await self.readuntil(b"\n")
        except asyncio.IncompleteReadError as e:
            return e.partial


class StreamWriter(object):
    """asyncio.StreamWriter-compatible writer backed by cooperative
    socket sendall.  Implements: write, writelines, drain, close,
    wait_closed, get_extra_info."""

    def __init__(self, sock, reader=None, *, loop=None):
        self._sock = sock
        self._reader = reader
        self._loop = loop
        self._closed = False
        # Buffer for write/drain semantics.  asyncio's StreamWriter
        # buffers on writes and flushes on drain; we send immediately
        # (cooperative blocking) so drain is a no-op but kept for API
        # parity.
        self._buf = bytearray()

    def write(self, data):
        if self._closed:
            raise RuntimeError("write on closed StreamWriter")
        self._buf.extend(data)
        # Try a non-blocking flush so small writes don't accumulate
        # in pathological cases.  If the socket would block, the next
        # drain() will handle it.
        self._try_flush()

    def writelines(self, lines):
        for line in lines:
            self._buf.extend(line)
        self._try_flush()

    def _try_flush(self):
        """Best-effort non-blocking flush.  Leaves residue in _buf."""
        while self._buf:
            try:
                n = self._sock.send(self._buf)
            except (BlockingIOError, InterruptedError):
                return
            except OSError as e:
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    return
                raise
            if n <= 0:
                return
            del self._buf[:n]

    async def drain(self):
        """Block (cooperatively) until all buffered data is on the wire."""
        while self._buf:
            try:
                n = self._sock.send(self._buf)
                if n > 0:
                    del self._buf[:n]
                    continue
            except (BlockingIOError, InterruptedError):
                pass
            except OSError as e:
                if e.errno not in (_errno.EAGAIN, _errno.EWOULDBLOCK, _errno.EINTR):
                    raise
            _wait_fd(self._sock.fileno(), 2)

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._sock.shutdown(_socket.SHUT_RDWR)
        except OSError:
            pass
        _close_sock(self._sock)

    def is_closing(self):
        return self._closed

    async def wait_closed(self):
        # Our close is synchronous; nothing to wait on.  Yield once so
        # callers using `await writer.wait_closed()` don't see surprise
        # tight loops.
        await asyncio.sleep(0)

    def get_extra_info(self, name, default=None):
        if name == "peername":
            try:
                return self._sock.getpeername()
            except OSError:
                return default
        if name == "sockname":
            try:
                return self._sock.getsockname()
            except OSError:
                return default
        if name == "socket":
            return self._sock
        obj = getattr(self._sock, "ssl_object", None)
        if name == "ssl_object":
            return obj if obj is not None else default
        if name == "peercert":
            return obj.getpeercert() if obj is not None else default
        if name == "cipher":
            return obj.cipher() if obj is not None else default
        if name == "sslcontext":
            return obj.context if obj is not None else default
        return default

    @property
    def transport(self):
        # asyncio code commonly does writer.transport.get_extra_info(...);
        # forward to ourselves for compat.
        return self


async def open_connection(host=None, port=None, *, family=0, proto=0,
                          flags=0, sock=None, local_addr=None,
                          server_hostname=None, ssl=None,
                          ssl_handshake_timeout=None,
                          limit=2**16, **_ignored):
    """Establish a TCP connection and return (reader, writer).

    Mirrors asyncio.open_connection but bypasses Transport/Protocol --
    our Stream classes talk to the socket directly via cooperative
    wait_fd.  TLS is handled by the cooperative _TLSSock wrapper.
    """
    if sock is None:
        if host is None or port is None:
            raise ValueError("open_connection requires host+port or sock=")
        # getaddrinfo is a blocking C call; offload it so it doesn't wedge
        # the hub (aionetiface's monkey patch may also make it cooperative).
        infos = _resolve(host, port,
                         family or _socket.AF_UNSPEC,
                         _socket.SOCK_STREAM,
                         proto, flags)
        last_err = None
        for fam, typ, prt, _canon, sa in infos:
            try:
                s = _socket.socket(fam, typ, prt)
                s.setblocking(False)
                if local_addr is not None:
                    s.bind(local_addr)
                try:
                    s.connect(sa)
                except BlockingIOError:
                    _wait_fd(s.fileno(), 2)
                    err = s.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                    if err != 0:
                        raise OSError(err, "connect failed")
                sock = s
                break
            except OSError as e:
                last_err = e
                try: s.close()
                except OSError: pass
        if sock is None:
            raise last_err or OSError("could not connect")
    else:
        sock.setblocking(False)

    if ssl is not None:
        sock = _tls_wrap_client(sock, ssl, server_hostname, host,
                                ssl_handshake_timeout)
    reader = StreamReader(sock, limit=limit)
    writer = StreamWriter(sock, reader=reader)
    return reader, writer


class _Server(object):
    """asyncio.Server compatible: keeps the listening socket alive and
    the accept-loop goroutine running until close() is called."""

    def __init__(self, sock, client_connected_cb, *, limit=2**16,
                 ssl_context=None, ssl_handshake_timeout=None):
        self._sock = sock
        self._cb   = client_connected_cb
        self._limit = limit
        self._ssl_context = ssl_context
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._closed = False
        self._accept_g = pygo_core.go(self._accept_loop)

    def _accept_loop(self):
        while not self._closed:
            try:
                conn, _addr = self._sock.accept()
            except (BlockingIOError, InterruptedError):
                if self._closed:
                    return
                _wait_fd(self._sock.fileno(), 1)
                continue
            except OSError as e:
                # close() will close the listening socket; the next
                # accept fails with EBADF / EINVAL.  Treat that as the
                # signal to exit cleanly.
                if self._closed:
                    return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    _wait_fd(self._sock.fileno(), 1)
                    continue
                # Real error -- record and exit.
                self._closed = True
                return
            conn.setblocking(False)
            if self._ssl_context is not None:
                # Handshake off the accept loop so a slow client can't stall it.
                pygo_core.go(lambda c=conn: self._setup_conn_tls(c))
            else:
                self._spawn_conn(conn)

    def _spawn_conn(self, sock):
        reader = StreamReader(sock, limit=self._limit)
        writer = StreamWriter(sock, reader=reader)
        # Build the connection coroutine and drive it directly as a PygoTask.
        # We're already inside a non-task goroutine (the accept loop or a
        # per-conn TLS goroutine); creating PygoTask directly here -- the
        # earlier "wrap in pygo_core.go then PygoTask inside" added a second
        # goroutine spawn for no real benefit.
        coro = self._cb(reader, writer)
        if asyncio.iscoroutine(coro):
            PygoTask(coro, loop=asyncio.get_event_loop())

    def _setup_conn_tls(self, conn):
        try:
            tls = _TLSSock(conn, self._ssl_context, server_side=True)
        except Exception:
            _close_sock(conn)
            return
        try:
            tls.do_handshake(self._ssl_handshake_timeout)
        except Exception:
            _close_sock(tls)
            return
        self._spawn_conn(tls)

    def is_serving(self):
        return not self._closed

    def close(self):
        if self._closed:
            return
        self._closed = True
        # shutdown() before close() wakes any goroutine parked on this
        # fd via wait_fd -- epoll/kqueue/IOCP all signal POLLIN+POLLHUP
        # on the listen socket, which our netpoll routes back to the
        # accept_loop's wait_fd call.  close() alone doesn't reliably
        # wake parked pollers on Linux.
        try:
            self._sock.shutdown(_socket.SHUT_RDWR)
        except OSError:
            pass
        _close_sock(self._sock)

    async def wait_closed(self):
        # Best-effort; we don't currently track outstanding client tasks.
        await asyncio.sleep(0)

    @property
    def sockets(self):
        return (self._sock,) if not self._closed else ()


# ====================================================================
# UDP: DatagramTransport + create_datagram_endpoint.
#
# Datagram socket goroutine: one g per endpoint runs the recv loop,
# delivering each packet to the protocol's datagram_received().
# send_to bypasses the loop entirely -- just non-blocking sendto with
# wait_fd on EAGAIN.
# ====================================================================
# ====================================================================
# _StreamTransport / _ProtocolServer: lower-level Transport+Protocol
# pair used by loop.create_connection / loop.create_server.  Most user
# code uses the StreamReader/Writer high-level path above; these exist
# for libraries (like aionetiface) that consume the protocol API.
# ====================================================================
class _StreamTransport(asyncio.Transport):
    """Thin TCP transport over a socket.  Drives the protocol's
    data_received via a recv goroutine; transports its write() through
    cooperative sendall."""

    def __init__(self, sock, protocol, *, loop=None, call_connection_made=True,
                 context=None):
        # Per-connection contextvars Context.  Stock asyncio runs a transport's
        # protocol callbacks (connection_made / data_received / eof_received /
        # connection_lost) inside the context captured when its reader Handle
        # was registered -- i.e. the context active in create_server's accept
        # callback (or create_connection's caller).  pygo's recv goroutine
        # otherwise runs them in the bare scheduler context, so any contextvar
        # set before the server/connection was created (request-id middleware,
        # uvicorn's "context preserved by default") is invisible inside the
        # ASGI task that data_received spawns.  Capture a fresh copy here (each
        # connection independent, matching asyncio's per-transport copy_context)
        # and run every protocol callback through it.
        if context is not None:
            self._context = context.run(_contextvars.copy_context)
        else:
            self._context = _contextvars.copy_context()
        # Populate the asyncio.Transport _extra dict so the INHERITED
        # get_extra_info works -- libraries read these and tests
        # @patch("asyncio.Transport.get_extra_info"), which only intercepts
        # when we don't shadow it with our own method.
        extra = {"socket": sock}
        try: extra["sockname"] = sock.getsockname()
        except OSError: pass
        try: extra["peername"] = sock.getpeername()
        except OSError: pass
        ssl_obj = getattr(sock, "ssl_object", None)
        if ssl_obj is not None:
            extra["ssl_object"] = ssl_obj
            extra["sslcontext"] = ssl_obj.context
            try: extra["peercert"] = ssl_obj.getpeercert()
            except Exception: pass
            try: extra["cipher"] = ssl_obj.cipher()
            except Exception: pass
        super().__init__(extra=extra)
        self._sock = sock
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        self._stopping = False
        self._paused = False        # pause_reading() flow control
        self._eof_written = False   # write_eof() called -> write() must raise
        self._conn_lost_called = False  # connection_lost fires exactly once
        self._in_context = False    # re-entrancy guard for _run_cb (see below)
        # start_tls reuses an already-connected protocol, so it suppresses the
        # re-fire (asyncio doesn't call connection_made again on TLS upgrade).
        if call_connection_made:
            try:
                self._run_cb(protocol.connection_made, self)
            except Exception as e:
                self._report(e, "connection_made")
        self._recv_g = pygo_core.go(self._recv_loop)

    def _run_cb(self, fn, *args):
        # Run a protocol callback inside this connection's contextvars Context
        # (so contextvars set before the connection -- e.g. uvicorn's request
        # context -- reach any task it spawns).  A Context cannot be entered
        # re-entrantly, and our callbacks fire synchronously: data_received may
        # call transport.close() (-> connection_lost) while still inside its own
        # _context.run.  Stock asyncio sidesteps this by scheduling each
        # callback in its own loop iteration; we instead detect the nested case
        # and call directly -- we are already executing inside self._context, so
        # the contextvars are identical.  Goroutines are cooperative and these
        # callbacks never await, so the flag needs no lock.
        if self._in_context:
            return fn(*args)
        self._in_context = True
        try:
            return self._context.run(fn, *args)
        finally:
            self._in_context = False

    def _recv_loop(self):
        sock = self._sock
        while not self._stopping:
            if self._paused:
                # Flow control: paused by pause_reading().  Poll the flag
                # cooperatively (resume_reading() clears it).  Pauses are
                # short backpressure windows, so a 1 ms tick is fine.
                pygo_core.sched_sleep(0.001)
                continue
            try:
                data = sock.recv(65536)
            except (BlockingIOError, InterruptedError):
                if self._stopping: return
                try:
                    _wait_fd(sock.fileno(), 1)
                except Exception:
                    return
                continue
            except OSError as e:
                if self._stopping: return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    _wait_fd(sock.fileno(), 1)
                    continue
                # Route through close() so connection_lost(e) fires exactly
                # once (the guard) rather than racing close()'s own call.
                self.close(e)
                return
            if not data:
                # EOF: the peer half-closed its write side, so recv() now
                # returns b'' immediately and forever.  Stop the recv loop
                # either way -- mirrors stock asyncio removing the reader on
                # EOF.  `continue`ing here would busy-spin recv()->b'' at
                # 100% CPU, hogging the hub and starving every other
                # goroutine (e.g. the peer task still awaiting its read).
                # Close only if the protocol didn't ask to keep the
                # transport open (eof_received() -> True) for its own writes.
                try:
                    keep = self._run_cb(self._protocol.eof_received)
                except Exception as e:
                    self._report(e, "eof_received")
                    keep = False
                if not keep:
                    self.close()
                return
            try:
                self._run_cb(self._protocol.data_received, data)
            except Exception as e:
                # asyncio treats an exception out of data_received() as fatal:
                # it closes the transport and delivers connection_lost(exc).
                # Without this a protocol that faults mid-read never gets
                # connection_lost, so any await on closure (e.g. websockets
                # recv() -> shield(connection_lost_waiter)) hangs forever.
                # close()'s _conn_lost_called guard keeps it single-fire.
                self._report(e, "data_received")
                self.close(e)
                return
            # Hand the scheduler to any goroutine data_received just woke (a
            # protocol coroutine awaiting this read) BEFORE we recv() again.
            # Stock asyncio does exactly one recv per loop iteration, then runs
            # ready callbacks; without this yield our recv loop can drain the
            # whole response AND the EOF/close in one burst, firing
            # connection_lost (-> protocol state CLOSED) before the woken coro
            # ran its post-read step.  That breaks ordering-sensitive protocols
            # -- e.g. websockets' client handshake asserts state is CONNECTING
            # in connection_open(), which runs only after the read it's blocked
            # on; if connection_lost beats it the assert fails.
            pygo_core.sched_yield_classic()

    def write(self, data):
        if self._eof_written:
            # Mirror stock asyncio's selector transport so callers (e.g.
            # websockets' broadcast) see the failure they expect, with the
            # same message they assert on.
            raise RuntimeError("Cannot call write() after write_eof()")
        if self._closed:
            return
        try:
            n = self._sock.send(data)
            if n < len(data):
                # Spawn a goroutine to finish.  Rare on small writes
                # to a healthy peer.
                rest = bytes(data[n:])
                def _flush(b=rest):
                    while b:
                        try:
                            sent = self._sock.send(b)
                            b = b[sent:]
                        except (BlockingIOError, InterruptedError):
                            try: _wait_fd(self._sock.fileno(), 2)
                            except Exception: return
                        except OSError:
                            return
                pygo_core.go(_flush)
        except (BlockingIOError, InterruptedError):
            rest = bytes(data)
            def _flush(b=rest):
                while b:
                    try:
                        sent = self._sock.send(b)
                        b = b[sent:]
                    except (BlockingIOError, InterruptedError):
                        try: _wait_fd(self._sock.fileno(), 2)
                        except Exception: return
                    except OSError:
                        return
            pygo_core.go(_flush)
        except OSError as e:
            # close() delivers connection_lost(e) exactly once -- calling it
            # here too double-fires it (websockets' connection_lost sets a
            # one-shot Future -> InvalidStateError "Future already done").
            self.close(e)

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def close(self, exc=None):
        if self._closed:
            return
        self._closed = True
        self._stopping = True
        # asyncio closes the fd inside the DEFERRED _call_connection_lost, NOT
        # synchronously here.  Code routinely reads the socket right after
        # transport.close() -- e.g. aiohttp's fingerprint-mismatch path does
        # transport.close() then transport.get_extra_info("socket").getpeername()
        # to drop the bad peer -- so closing the fd synchronously gives them
        # EBADF (and the resulting OSError masks the ServerFingerprintMismatch
        # they expect).  Defer the shutdown+close to the same loop turn that
        # delivers connection_lost.
        deliver_cl = not self._conn_lost_called
        if deliver_cl:
            self._conn_lost_called = True
        self._deliver_connection_lost(exc, deliver_cl)

    def _deliver_connection_lost(self, exc, deliver_cl=True):
        # Schedule connection_lost (if not already delivered) AND the socket
        # shutdown+close on the loop in this connection's context, NEVER inline
        # -- exactly like asyncio's _call_connection_lost via call_soon, which
        # also closes self._sock only after connection_lost.  Deferring matters
        # on EOF: the recv loop may have just delivered the peer's final bytes
        # (e.g. a websocket Close frame) to data_received, waking the protocol's
        # reader task; that task must run and consume them BEFORE connection_lost
        # (or the protocol reports an abnormal close), and the fd must stay valid
        # until this turn so a post-close() getpeername() doesn't hit EBADF.
        def _close_sock_now():
            try:
                self._sock.shutdown(_socket.SHUT_RDWR)
            except OSError:
                pass
            _close_sock(self._sock)
        def _deliver():
            if deliver_cl:
                try:
                    self._protocol.connection_lost(exc)
                except Exception as e:
                    self._report(e, "connection_lost")
            _close_sock_now()
        loop = self._loop if self._loop is not None else asyncio.get_event_loop()
        try:
            loop.call_soon(_deliver, context=self._context)
        except RuntimeError:
            # Loop already closed: best-effort inline so done-futures resolve.
            if deliver_cl:
                try:
                    self._run_cb(self._protocol.connection_lost, exc)
                except Exception as e:
                    self._report(e, "connection_lost")
            _close_sock_now()

    def is_closing(self):
        return self._closed

    # get_extra_info is inherited from asyncio.Transport (returns
    # self._extra.get(name, default), populated in __init__) so it stays
    # asyncio-compatible and patchable via asyncio.Transport.get_extra_info.

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    # ---- flow control (read side) ----
    def pause_reading(self):
        self._paused = True

    def resume_reading(self):
        self._paused = False

    def is_reading(self):
        return not self._paused and not self._closed

    # ---- abort / half-close ----
    def abort(self):
        # Immediate teardown (no graceful flush); close() already does a
        # shutdown + connection_lost, which is acceptable for abort here.
        self.close()

    def can_write_eof(self):
        return True

    def write_eof(self):
        if self._closed or self._eof_written:
            return
        self._eof_written = True
        try:
            self._sock.shutdown(_socket.SHUT_WR)
        except OSError:
            pass

    # ---- write-buffer flow control: we write synchronously / via a flush
    # goroutine, so the buffer is effectively always drained.  Report 0 and
    # never invoke pause_writing; accept the setters as no-ops. ----
    def set_write_buffer_limits(self, high=None, low=None):
        pass

    def get_write_buffer_limits(self):
        return (0, 0)

    def get_write_buffer_size(self):
        return 0

    def _report(self, exc, where):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "StreamTransport " + where + " raised",
                "exception": exc,
            })


class _ProtocolServer(object):
    """Server compatible with asyncio.Server: per-accept builds a
    _StreamTransport and a protocol via factory."""

    def __init__(self, socks, protocol_factory, *, loop=None, ssl_context=None,
                 ssl_handshake_timeout=None):
        # create_server may bind several sockets (one per address family);
        # accept independently on each.
        self._socks = list(socks)
        self._factory = protocol_factory
        self._loop = loop
        # asyncio.Server exposes _ssl_context (None when no TLS); libraries
        # (e.g. websockets' test helpers) read it off the server object.  It
        # holds the real SSLContext when create_server was given ssl=.
        self._ssl_context = ssl_context
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._closed = False
        # Context active when the server was created (inside the awaiting
        # create_server coroutine).  Each accepted connection's transport runs
        # its protocol callbacks in a fresh copy of this -- so a contextvar set
        # before create_server (uvicorn's "context preserved by default")
        # reaches the ASGI task spawned from data_received.  Mirrors asyncio's
        # accept-callback context flowing into each transport's reader Handle.
        self._context = _contextvars.copy_context()
        # Track live client transports so close()/abort_clients() can tear
        # them down.  Without this, stopping the server (e.g. aiosmtpd's
        # Controller.stop()) leaves accepted connections' sockets open with no
        # goroutine servicing them -- a peer mid-request (a client between DATA
        # and the terminating dot) then blocks forever waiting for a reply that
        # never comes.  WeakSet so a finished connection's transport is pruned
        # once its recv goroutine ends and drops the last reference.
        self._conns = _weakref.WeakSet()
        self._accept_gs = [pygo_core.go(lambda s=s: self._accept_loop(s))
                           for s in self._socks]

    def _accept_loop(self, sock):
        while not self._closed:
            try:
                conn, _addr = sock.accept()
            except (BlockingIOError, InterruptedError):
                if self._closed: return
                _wait_fd(sock.fileno(), 1)
                continue
            except OSError:
                # One listener erroring stops accepting on it but must not
                # tear down the whole (multi-socket) server.
                return
            conn.setblocking(False)
            if self._ssl_context is not None:
                # Finish the TLS handshake in its own goroutine so a slow or
                # stalled client never blocks accepting new connections.
                pygo_core.go(lambda c=conn: self._setup_tls_conn(c))
            else:
                protocol = self._factory()
                self._conns.add(_StreamTransport(conn, protocol, loop=self._loop,
                                                 context=self._context))

    def _setup_tls_conn(self, conn):
        try:
            tls = _TLSSock(conn, self._ssl_context, server_side=True)
        except Exception:
            _close_sock(conn)
            return
        try:
            tls.do_handshake(self._ssl_handshake_timeout)
        except Exception:
            # Bad cert / SNI / protocol error, or a peer that stalled past
            # ssl_handshake_timeout: drop it quietly, like asyncio's SSL
            # transport does.
            _close_sock(tls)
            return
        protocol = self._factory()
        self._conns.add(_StreamTransport(tls, protocol, loop=self._loop,
                                         context=self._context))

    def get_loop(self):
        """asyncio.Server.get_loop().  Libraries (websockets) call this on
        the server returned by create_server to schedule cleanup tasks."""
        return self._loop if self._loop is not None else asyncio.get_event_loop()

    def is_serving(self):
        return not self._closed

    async def start_serving(self):
        # The accept loop is started in __init__, so we are already serving;
        # this mirrors asyncio.Server.start_serving() as a no-op when already up.
        return None

    async def serve_forever(self):
        # Run until close() (or cancellation of this coroutine) ends it.
        while not self._closed:
            await asyncio.sleep(0.05)

    def close(self):
        if self._closed: return
        self._closed = True
        # asyncio.Server.close() ONLY stops the listeners; established
        # connections keep running until they finish (or are closed explicitly
        # via close_clients()/abort_clients(), or cancelled when the loop ends).
        # Closing client transports here breaks callers that close() the server
        # and THEN message the live connections -- e.g. uvicorn's graceful
        # shutdown closes the server, then sends each websocket a 1012 close
        # frame; if we'd already torn the transport down that frame is dropped
        # and the peer sees an abnormal 1006 close.  (We used to close clients
        # here to dodge the cancel-can't-interrupt-wait_fd hang; that's fixed in
        # the C core now, so the recv goroutines get cleaned up on loop teardown.)
        for sock in self._socks:
            try: sock.shutdown(_socket.SHUT_RDWR)
            except OSError: pass
            _close_sock(sock)

    def close_clients(self):
        # asyncio 3.13+ API: gracefully close all client connections.
        for tr in list(self._conns):
            try:
                tr.close()
            except Exception:
                pass

    def abort_clients(self):
        # asyncio 3.13+ API: abort (our close() already does an immediate
        # shutdown + connection_lost, so it doubles as abort).
        for tr in list(self._conns):
            try:
                tr.abort()
            except Exception:
                pass

    async def wait_closed(self):
        await asyncio.sleep(0)

    @property
    def sockets(self):
        return tuple(self._socks) if not self._closed else ()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.close()
        await self.wait_closed()


class DatagramTransport(object):
    """asyncio.DatagramTransport-compatible transport.

    Wires a UDP socket to a user-supplied DatagramProtocol.  The
    protocol's datagram_received(data, addr) / error_received(exc) /
    connection_lost(exc) methods are called from our recv goroutine.
    """

    def __init__(self, sock, protocol, *, loop=None):
        self._sock = sock
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        # Tells the recv loop to bail on next iteration.
        self._stopping = False
        # connection_made fires before any recv work.
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        # Spawn the recv loop.
        self._recv_g = pygo_core.go(self._recv_loop)

    def _recv_loop(self):
        sock = self._sock
        while not self._stopping:
            try:
                data, addr = sock.recvfrom(65536)
            except (BlockingIOError, InterruptedError):
                if self._stopping: return
                try:
                    _wait_fd(sock.fileno(), 1)
                except Exception:
                    return
                continue
            except OSError as e:
                if self._stopping: return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    _wait_fd(sock.fileno(), 1)
                    continue
                # Error -- notify protocol and stop.
                try:
                    self._protocol.error_received(e)
                except Exception as e2:
                    self._report(e2, "error_received")
                return
            try:
                self._protocol.datagram_received(data, addr)
            except Exception as e:
                self._report(e, "datagram_received")

    def sendto(self, data, addr=None):
        if self._closed:
            return
        try:
            if addr is None:
                self._sock.send(data)
            else:
                self._sock.sendto(data, addr)
        except (BlockingIOError, InterruptedError):
            # UDP send rarely blocks, but if it does we just drop.
            # asyncio's selector loop does the same (best-effort).
            pass
        except OSError as e:
            try:
                self._protocol.error_received(e)
            except Exception as e2:
                self._report(e2, "error_received")

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stopping = True
        _close_sock(self._sock)
        try:
            self._protocol.connection_lost(None)
        except Exception as e:
            self._report(e, "connection_lost")

    def is_closing(self):
        return self._closed

    def get_extra_info(self, name, default=None):
        if name == "socket":
            return self._sock
        if name == "sockname":
            try:
                return self._sock.getsockname()
            except OSError:
                return default
        if name == "peername":
            try:
                return self._sock.getpeername()
            except OSError:
                return default
        return default

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def _report(self, exc, where):
        if self._loop is not None:
            self._loop.call_exception_handler({
                "message": "Datagram " + where + " raised",
                "exception": exc,
            })


# ====================================================================
# Subprocess support (thread-backed).  Each child pipe is pumped on its own
# OS thread; data + EOF + process-exit are marshalled back to the loop thread
# via call_soon_threadsafe, so the protocol callbacks all run on the loop.
# ====================================================================
class _SubprocessReadPipe(asyncio.ReadTransport):
    """ReadTransport for a child's stdout/stderr.  A reader thread (started by
    the SubprocessTransport) does the blocking reads and calls
    protocol.pipe_data_received; this object exists for the protocol's
    get_pipe_transport(fd) + flow-control calls."""
    def __init__(self, proc_transport, fd, pipe):
        self._proc = proc_transport
        self._fd = fd
        self._pipe = pipe
        self._paused = False

    # Flow control: the reader thread checks _paused between reads.  Not a hard
    # backpressure guarantee (one read may already be buffered), but enough for
    # StreamReader's high-water-mark pause to take effect.
    def pause_reading(self):
        self._paused = True

    def resume_reading(self):
        self._paused = False

    def is_closing(self):
        return self._proc._closed

    def close(self):
        self._proc.close()

    def get_protocol(self):
        return self._proc._protocol

    def get_extra_info(self, name, default=None):
        if name == "pipe":
            return self._pipe
        return default


class _SubprocessWritePipe(asyncio.WriteTransport):
    """WriteTransport for a child's stdin.  Writes are queued and drained by a
    dedicated thread so a full pipe buffer never blocks the loop thread."""
    def __init__(self, proc_transport, fd, pipe):
        self._proc = proc_transport
        self._fd = fd
        self._pipe = pipe
        self._closed = False
        self._q = _collections.deque()
        self._cond = _threading.Condition()
        self._eof = False
        self._thread = _threading.Thread(
            target=self._drain, name="pygo-subproc-stdin", daemon=True)
        self._thread.start()

    def _drain(self):
        while True:
            with self._cond:
                while not self._q and not self._eof:
                    self._cond.wait()
                if self._q:
                    data = self._q.popleft()
                else:
                    break    # _eof and queue empty
            try:
                self._pipe.write(data)
                self._pipe.flush()
            except (BrokenPipeError, OSError):
                break
        try:
            self._pipe.close()
        except Exception:
            pass

    def write(self, data):
        if self._closed or self._eof:
            return
        with self._cond:
            self._q.append(bytes(data))
            self._cond.notify()

    def writelines(self, list_of_data):
        self.write(b"".join(list_of_data))

    def write_eof(self):
        with self._cond:
            self._eof = True
            self._cond.notify()

    def can_write_eof(self):
        return True

    def close(self):
        self._closed = True
        self.write_eof()

    def abort(self):
        self.close()

    def is_closing(self):
        return self._closed or self._eof

    def get_protocol(self):
        return self._proc._protocol

    def get_extra_info(self, name, default=None):
        if name == "pipe":
            return self._pipe
        return default


class _SubprocessTransport(asyncio.SubprocessTransport):
    def __init__(self, loop, protocol, args, *, shell,
                 stdin, stdout, stderr, **kwargs):
        self._loop = loop
        self._protocol = protocol
        self._closed = False
        self._returncode = None
        self._exit_waiters = []
        self._pipes = {}        # fd -> pipe transport
        self._pipes_open = 0    # outstanding stdout/stderr reader threads
        self._extra = {}
        # bufsize=0: unbuffered, so reader threads see data promptly and our
        # stdin writer controls flushing.  Spawn the child.
        self._proc = _subprocess.Popen(
            args, shell=shell, stdin=stdin, stdout=stdout, stderr=stderr,
            bufsize=0, **kwargs)
        self._pid = self._proc.pid
        self._extra["subprocess"] = self._proc
        # Build the pipe transports FIRST: the protocol's connection_made calls
        # get_pipe_transport(fd) to create its StreamReader/Writer, so they must
        # exist before we fire it.
        if self._proc.stdin is not None:
            self._pipes[0] = _SubprocessWritePipe(self, 0, self._proc.stdin)
        if self._proc.stdout is not None:
            self._pipes[1] = _SubprocessReadPipe(self, 1, self._proc.stdout)
        if self._proc.stderr is not None:
            self._pipes[2] = _SubprocessReadPipe(self, 2, self._proc.stderr)
        # connection_made before any pipe-data/exit callback (asyncio contract).
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        # Only NOW start the reader/wait threads -- so no pipe_data_received or
        # process_exited can land before connection_made has run.
        if 1 in self._pipes:
            self._start_reader(1, self._proc.stdout)
        if 2 in self._pipes:
            self._start_reader(2, self._proc.stderr)
        _threading.Thread(target=self._wait_thread,
                          name="pygo-subproc-wait", daemon=True).start()

    def _start_reader(self, fd, pipe):
        self._pipes_open += 1
        rp = self._pipes[fd]

        def reader():
            try:
                while True:
                    if rp._paused:
                        _time.sleep(0.001)
                        continue
                    data = pipe.read(32768)
                    if not data:
                        break
                    self._loop.call_soon_threadsafe(
                        self._protocol.pipe_data_received, fd, data)
            except (BrokenPipeError, OSError):
                pass
            finally:
                self._loop.call_soon_threadsafe(self._pipe_lost, fd, None)

        _threading.Thread(target=reader, name="pygo-subproc-read-%d" % fd,
                          daemon=True).start()

    def _pipe_lost(self, fd, exc):
        try:
            self._protocol.pipe_connection_lost(fd, exc)
        except Exception as e:
            self._report(e, "pipe_connection_lost")
        self._pipes_open -= 1
        self._maybe_connection_lost()

    def _wait_thread(self):
        rc = self._proc.wait()
        self._loop.call_soon_threadsafe(self._on_exit, rc)

    def _on_exit(self, rc):
        if self._returncode is not None:
            return
        self._returncode = rc
        try:
            self._protocol.process_exited()
        except Exception as e:
            self._report(e, "process_exited")
        for fut in self._exit_waiters:
            if not fut.done():
                fut.set_result(rc)
        self._exit_waiters.clear()
        self._maybe_connection_lost()

    def _maybe_connection_lost(self):
        # Mirror asyncio: connection_lost fires once the process has exited AND
        # both output pipes have hit EOF.
        if self._returncode is not None and self._pipes_open <= 0 \
                and not self._closed:
            self._closed = True
            try:
                self._protocol.connection_lost(None)
            except Exception as e:
                self._report(e, "connection_lost")

    # ---- asyncio.SubprocessTransport interface ----
    def get_pid(self):
        return self._pid

    def get_returncode(self):
        return self._returncode

    def get_pipe_transport(self, fd):
        return self._pipes.get(fd)

    def _wait(self):
        # asyncio.subprocess.Process.wait() awaits this.
        fut = self._loop.create_future()
        if self._returncode is not None:
            fut.set_result(self._returncode)
        else:
            self._exit_waiters.append(fut)
        return fut

    def send_signal(self, signal):
        self._proc.send_signal(signal)

    def terminate(self):
        self._proc.terminate()

    def kill(self):
        self._proc.kill()

    def is_closing(self):
        return self._closed

    def close(self):
        # Best-effort: kill a still-running child, then close stdin.
        if self._returncode is None:
            try:
                self._proc.kill()
            except (ProcessLookupError, OSError):
                pass
        stdin = self._pipes.get(0)
        if stdin is not None:
            stdin.close()

    def get_protocol(self):
        return self._protocol

    def get_extra_info(self, name, default=None):
        return self._extra.get(name, default)

    def _report(self, exc, where):
        self._loop.call_exception_handler({
            "message": "Subprocess " + where + " raised",
            "exception": exc,
        })


class _ReadPipeTransport(asyncio.ReadTransport):
    """connect_read_pipe transport: a thread does blocking reads on the pipe and
    feeds protocol.data_received; EOF -> eof_received + connection_lost."""
    def __init__(self, loop, pipe, protocol):
        self._loop = loop
        self._pipe = pipe
        self._protocol = protocol
        self._closing = False
        self._paused = False
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        _threading.Thread(target=self._reader, name="pygo-readpipe",
                          daemon=True).start()

    def _reader(self):
        try:
            while not self._closing:
                if self._paused:
                    _time.sleep(0.001)
                    continue
                data = self._pipe.read(32768)
                if not data:
                    break
                self._loop.call_soon_threadsafe(self._deliver, data)
        except (BrokenPipeError, OSError):
            pass
        finally:
            self._loop.call_soon_threadsafe(self._eof)

    def _deliver(self, data):
        if not self._closing:
            try:
                self._protocol.data_received(data)
            except Exception as e:
                self._report(e, "data_received")

    def _eof(self):
        keep_open = False
        try:
            keep_open = bool(self._protocol.eof_received())
        except Exception as e:
            self._report(e, "eof_received")
            keep_open = False
        if not keep_open:
            self._close(None)

    def _close(self, exc):
        if self._closing:
            return
        self._closing = True
        try:
            self._pipe.close()
        except Exception:
            pass
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")

    def pause_reading(self):
        self._paused = True

    def resume_reading(self):
        self._paused = False

    def close(self):
        self._close(None)

    def is_closing(self):
        return self._closing

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_extra_info(self, name, default=None):
        return self._pipe if name == "pipe" else default

    def _report(self, exc, where):
        self._loop.call_exception_handler(
            {"message": "Read pipe " + where + " raised", "exception": exc})


class _WritePipeTransport(asyncio.WriteTransport):
    """connect_write_pipe transport: queued writes drained by a thread so a full
    pipe never blocks the loop; connection_lost fires on close/EOF/error."""
    def __init__(self, loop, pipe, protocol):
        self._loop = loop
        self._pipe = pipe
        self._protocol = protocol
        self._closing = False
        self._eof = False
        self._q = _collections.deque()
        self._cond = _threading.Condition()
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        _threading.Thread(target=self._drain, name="pygo-writepipe",
                          daemon=True).start()

    def _drain(self):
        exc = None
        while True:
            with self._cond:
                while not self._q and not self._eof:
                    self._cond.wait()
                if self._q:
                    data = self._q.popleft()
                else:
                    break
            try:
                self._pipe.write(data)
                self._pipe.flush()
            except (BrokenPipeError, OSError) as e:
                exc = e
                break
        try:
            self._pipe.close()
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._lost, exc)

    def _lost(self, exc):
        if self._closing:
            return
        self._closing = True
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")

    def write(self, data):
        if self._closing or self._eof:
            return
        with self._cond:
            self._q.append(bytes(data))
            self._cond.notify()

    def writelines(self, list_of_data):
        self.write(b"".join(list_of_data))

    def write_eof(self):
        with self._cond:
            self._eof = True
            self._cond.notify()

    def can_write_eof(self):
        return True

    def close(self):
        self.write_eof()

    def abort(self):
        self.write_eof()

    def is_closing(self):
        return self._closing or self._eof

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def get_extra_info(self, name, default=None):
        return self._pipe if name == "pipe" else default

    def _report(self, exc, where):
        self._loop.call_exception_handler(
            {"message": "Write pipe " + where + " raised", "exception": exc})


async def _create_datagram_endpoint(loop, protocol_factory, local_addr=None,
                                    remote_addr=None, family=0, proto=0,
                                    flags=0, reuse_address=None,
                                    reuse_port=None, allow_broadcast=None,
                                    sock=None):
    """Implementation of loop.create_datagram_endpoint."""
    if sock is None:
        if local_addr is None and remote_addr is None:
            family = family or _socket.AF_INET
        if family == 0:
            family = _socket.AF_INET
        sock = _socket.socket(family, _socket.SOCK_DGRAM, proto)
        sock.setblocking(False)
        if reuse_address:
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        if reuse_port and hasattr(_socket, "SO_REUSEPORT"):
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
        if allow_broadcast:
            sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_BROADCAST, 1)
        if local_addr is not None:
            sock.bind(local_addr)
        if remote_addr is not None:
            try:
                sock.connect(remote_addr)
            except BlockingIOError:
                _wait_fd(sock.fileno(), 2)
    else:
        sock.setblocking(False)

    protocol = protocol_factory()
    transport = DatagramTransport(sock, protocol, loop=loop)
    return transport, protocol


async def start_server(client_connected_cb, host=None, port=None, *,
                       family=_socket.AF_UNSPEC, flags=_socket.AI_PASSIVE,
                       sock=None, backlog=100, limit=2**16,
                       reuse_address=None, reuse_port=None,
                       ssl=None, ssl_handshake_timeout=None, **_ignored):
    """Listen on host:port and call client_connected_cb(reader, writer)
    per accepted connection.  Returns a _Server with .close() / .sockets.

    Compared to asyncio.start_server, we skip Transport/Protocol but still
    wrap accepted connections in cooperative TLS when ssl= is given."""
    if sock is None:
        infos = _socket.getaddrinfo(host, port, family,
                                    _socket.SOCK_STREAM, 0, flags)
        last_err = None
        for fam, typ, prt, _canon, sa in infos:
            try:
                sock = _socket.socket(fam, typ, prt)
                if reuse_address is not False:
                    sock.setsockopt(_socket.SOL_SOCKET,
                                    _socket.SO_REUSEADDR, 1)
                sock.setblocking(False)
                sock.bind(sa)
                sock.listen(backlog)
                break
            except OSError as e:
                last_err = e
                _close_sock(sock)
                sock = None
        if sock is None:
            raise last_err or OSError("could not bind")
    else:
        sock.setblocking(False)

    return _Server(sock, client_connected_cb, limit=limit, ssl_context=ssl,
                   ssl_handshake_timeout=ssl_handshake_timeout)


class PygoEventLoopPolicy(asyncio.AbstractEventLoopPolicy):
    def __init__(self):
        self._loop = None

    def get_event_loop(self):
        if self._loop is None or self._loop.is_closed():
            self._loop = PygoEventLoop()
        return self._loop

    def set_event_loop(self, loop):
        self._loop = loop

    def new_event_loop(self):
        return PygoEventLoop()

    # Child-watcher stubs (asyncio asks for these on Unix).
    def get_child_watcher(self):
        return None

    def set_child_watcher(self, watcher):
        pass


def install():
    """Install PygoEventLoopPolicy globally.  After this, every
    `asyncio.run(...)` / `asyncio.new_event_loop()` returns a pygo
    loop instead of the stdlib selector / proactor loop."""
    asyncio.set_event_loop_policy(PygoEventLoopPolicy())


def run(coro, *, debug=False):
    """Drop-in for `asyncio.run`.  Creates a fresh PygoEventLoop,
    runs `coro` to completion, returns the result.  Caller doesn't
    need to call install() first."""
    loop = PygoEventLoop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)
