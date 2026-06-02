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
import inspect as _inspect
import os as _os
import socket as _socket
import ssl as _ssl
import subprocess as _subprocess
import sys
import threading as _threading
import time as _time
import warnings as _warnings
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
# Module-root frame for task-driver goroutines.
#
# A PygoTask drives its coroutine on the goroutine's own swapped C stack,
# whose Python frame chain pygo_core deliberately severs at the goroutine
# root (so tracebacks / recursion don't bleed across goroutines).  Stock
# asyncio instead runs a Task's coro synchronously nested under
# _run_once -> run_forever -> ... -> "<module>" on ONE stack, so a library
# that derives its module name by walking frame.f_back to the first
# co_name == "<module>" -- aiohttp's web.AppKey (helpers.py) -- finds it.
# Under pygo the walk dead-ends at the driver and AppKey raises
# UnboundLocalError (test_web_app subapp tests; pass under stock asyncio).
#
# Fix: run the driver coroutine *underneath* a real "<module>"-named frame.
# compile(src, name, "exec") yields a top code object whose co_name is
# literally "<module>"; exec'ing it with a globals dict carrying the right
# __name__ seats a genuine, lifecycle-correct module frame at the goroutine
# root.  No hand-built _PyInterpreterFrame, and crucially no cross-stack
# f_back link to the spawner (that would dangle the moment the spawner's
# stack is swapped away or returns, and would be a lie -- the spawner is
# concurrent, not on the goroutine's call stack).  Only task-driver
# goroutines are wrapped; raw pygo_core.go() goroutines (netpoll pump,
# keepalive, timers) are untouched, so the per-goroutine cost stays off the
# scale-out path.  Disable with PYGO_AIO_MODULE_ROOT=0.
_PG_MODULE_ROOT_ON = _os.environ.get("PYGO_AIO_MODULE_ROOT", "1") != "0"
_PG_ROOT_CODE = compile("__pygo_body__()", "<pygo-task-root>", "exec")


def _pg_capture_module_name(default="__main__"):
    """Walk the CREATOR's live stack (PygoTask.__init__ runs synchronously on
    it) to the nearest "<module>" frame and copy its __name__ -- the same
    module asyncio's frame walk would reach.  Holds only the string, never the
    frame.  A nested create_task() finds the parent task's own module-root
    frame first, so the name self-propagates down the task tree."""
    f = _inspect.currentframe()
    try:
        f = f.f_back if f is not None else None     # skip our own frame
        while f is not None:
            if f.f_code.co_name == "<module>":
                name = f.f_globals.get("__name__")
                if name is not None:
                    return name
            f = f.f_back
    finally:
        del f                                       # don't strand a frame ref
    return default


def _pg_run_with_module_root(body, module_name):
    exec(_PG_ROOT_CODE, {"__name__": module_name, "__pygo_body__": body})


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


def _blocking(fn, *args):
    """pygo_core.blocking (offload fn to the blocking-pool), but deliver a
    cancellation requested WHILE we were in the call.

    pygo_core.blocking parks the goroutine in C with no driver await-point, so
    task.cancel() cannot interrupt it -- it only sets the task's one-shot
    _pgmustcancel and wakes us (which pygo_core.blocking now ignores until the
    worker is done, to avoid freeing the in-flight job).  Stock asyncio resolves
    via run_in_executor, an await that raises CancelledError on cancel; mirror
    that here so a cancel during DNS doesn't silently get swallowed and let the
    caller go on to park uncancellably (e.g. in the connect wait_fd -> hang)."""
    r = pygo_core.blocking(fn, *args)
    task = asyncio.current_task()
    if task is not None and getattr(task, "_pgmustcancel", False):
        task._pgmustcancel = False
        raise asyncio.CancelledError()
    return r


def _resolve(host, port, family, type_, proto, flags):
    """getaddrinfo via the blocking-offload pool, so DNS doesn't wedge the
    goroutine's hub (it is a non-preemptible blocking C call).  Runs inline
    when not on a goroutine -- safe in either context."""
    return _blocking(_socket.getaddrinfo, host, port,
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


def _reject_subprocess_text_mode(kwargs):
    """Mirror asyncio.base_events.subprocess_{exec,shell}: an asyncio
    subprocess is always binary and unbuffered.  Reject text-mode / buffering
    kwargs with ValueError (CPython's test_subprocess asserts these raises),
    and pop bufsize so it can't collide with our hardcoded bufsize=0 Popen."""
    if kwargs.get("universal_newlines"):
        raise ValueError("universal_newlines must be False")
    if kwargs.get("text"):
        raise ValueError("text must be False")
    if kwargs.get("encoding") is not None:
        raise ValueError("encoding must be None")
    if kwargs.get("errors") is not None:
        raise ValueError("errors must be None")
    if kwargs.pop("bufsize", 0) != 0:
        raise ValueError("bufsize must be 0")


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


# asyncio's non-raising "loop running on this thread, or None" accessor (C fn);
# used on the hot socket-I/O path in _wait_fd to keep current_task() correct.
_PG_GET_RUNNING_LOOP = asyncio.events._get_running_loop


def _wait_fd(fd, events, timeout_ms=-1):
    """pygo_core.wait_fd, but a cancellation (G.cancel_wait_fd) raises
    CancelledError instead of returning the raw sentinel.  Every aio I/O loop
    parks through this, so cancelling a task blocked in any socket wait works.

    Also preserves asyncio's "current task" across the park.  The PygoTask
    driver sets _CURRENT_TASKS[loop] = self around each coro.send and restores
    it in a finally -- but a coroutine that parks HERE for socket I/O suspends
    the goroutine MID-send, so that finally can't run until the send eventually
    returns.  While we're parked, other tasks run and their drivers mutate the
    single shared per-loop slot (and pop it back to whatever preceded them), so
    on resume the slot may name the wrong task or be empty -- breaking
    asyncio.current_task() and hence asyncio.timeout()/wait_for() ("Timeout
    should be used inside a task").  Snapshot our task before the C park and
    re-establish it on resume so current_task() is correct across a socket wait.
    (Future-based awaits don't need this: they park BETWEEN sends, after the
    driver's finally has already run.)"""
    loop = _PG_GET_RUNNING_LOOP()
    saved = _CURRENT_TASKS.get(loop) if loop is not None else None
    try:
        r = pygo_core.wait_fd(fd, events, timeout_ms)
    finally:
        if saved is not None and _CURRENT_TASKS.get(loop) is not saved:
            _CURRENT_TASKS[loop] = saved
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
        # Mirror asyncio's sslproto normalisation: a falsy server_hostname --
        # notably the empty string, which create_connection accepts to mean
        # "TLS without SNI / hostname verification" -- and every server-side
        # wrap must pass None to the ssl machinery.  ssl.wrap_socket raises
        # ValueError on an empty (or leading-dot) server_hostname, so without
        # this an explicit server_hostname='' blows up the whole handshake.
        if server_side or not server_hostname:
            server_hostname = None
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
                except _ssl.SSLEOFError:
                    # Peer closed the connection mid-handshake (EOF in
                    # violation of protocol).  asyncio's sslproto translates a
                    # premature EOF while DO_HANDSHAKE into ConnectionResetError
                    # (eof_received -> _on_handshake_complete(ConnectionResetError));
                    # mirror that so callers see the connection-reset they expect
                    # rather than a raw ssl.SSLEOFError.
                    raise ConnectionResetError(
                        "Connection lost during TLS handshake") from None
            if deadline is None:
                _wait_fd(fd, want)
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    # Match asyncio's ssl_handshake_timeout exception type and
                    # message (sslproto._check_handshake_timeout) so callers'
                    # assertRaisesRegex(ConnectionAbortedError, 'SSL handshake.*
                    # is taking longer') holds.
                    raise ConnectionAbortedError(
                        "SSL handshake is taking longer than {0} seconds: "
                        "aborting the connection".format(timeout))
                # wait_fd returns (without raising) when the timeout elapses;
                # the next loop re-checks the deadline and raises above.
                _wait_fd(fd, want, max(1, int(remaining * 1000)))

    def recv_nb(self, n):
        # SINGLE non-blocking recv attempt: returns decrypted bytes, b'' on EOF,
        # or raises BlockingIOError if no application data is ready yet.  Never
        # parks -- the merged _StreamTransport io goroutine must not block in
        # recv (it would stall the write drain on the SAME goroutine, a
        # full-duplex deadlock).  The cooperative parking recv() below is for
        # callers that own a dedicated read goroutine.
        if self._closed:
            return b""
        with self._lock:
            try:
                return self._ssl.recv(n)
            except (_ssl.SSLWantReadError, _ssl.SSLWantWriteError):
                raise BlockingIOError()
            except _ssl.SSLZeroReturnError:
                self._peer_close_notify = True
                return b""
            except _ssl.SSLEOFError:
                return b""

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


class _MemoryBIOTLS(object):
    """Cooperative TLS over a MemoryBIO pair -- the asyncio-faithful design
    (mirrors ssl.SSLObject + sslproto.SSLProtocol).  pygo owns every handshake
    byte: it feeds the *incoming* BIO from the raw socket and drains the
    *outgoing* BIO to it.  That makes possible what the fd-based _TLSSock can't:

      * start_tls where the peer's ClientHello (or trailing plaintext) was
        ALREADY read off the socket into a Python buffer -- pass those bytes as
        ``incoming_data`` and they seed the handshake (gh-142352);
      * a real ``_ssl_protocol`` exposing ``_sslcontext`` (white-box code/tests
        read transport._ssl_protocol._sslcontext);
      * an explicit, cooperative close_notify on close.

    Presents the SAME socket surface (fileno/recv/recv_nb/recv_into/send/sendall/
    shutdown/close/getpeername/...) the transport already drives, so it is a
    drop-in for _TLSSock.  Runs inside the transport's single io goroutine, so
    blocking helpers may park; recv_nb never parks for inbound data.  One CoLock
    serialises SSLObject calls (released across every wait_fd)."""

    def __init__(self, raw, context, *, server_side=False,
                 server_hostname=None, incoming_data=b""):
        raw.setblocking(False)
        self._raw = raw
        self._fd = raw.fileno()
        if server_side or not server_hostname:
            server_hostname = None
        self._context = context
        self._server_side = server_side
        self._inc = _ssl.MemoryBIO()
        self._out = _ssl.MemoryBIO()
        self._obj = context.wrap_bio(self._inc, self._out,
                                     server_side=server_side,
                                     server_hostname=server_hostname)
        if incoming_data:
            self._inc.write(incoming_data)
        self._lock = _get_colock()()
        self._closed = False
        self._peer_close_notify = False
        self._eof_in = False

    # ---- BIO <-> raw-socket plumbing (caller holds NO lock for the socket
    # waits; SSLObject calls are serialised by self._lock) ----
    def _pump_out(self):
        # Drain the outgoing BIO to the raw socket, parking on WRITE until every
        # queued encrypted byte is on the wire.  Runs in the io goroutine, so
        # parking is safe.  Returns False if the socket died.
        data = self._out.read()
        if not data:
            return True
        view = memoryview(data)
        while view:
            try:
                n = self._raw.send(view)
                view = view[n:]
            except (BlockingIOError, InterruptedError):
                try:
                    _wait_fd(self._fd, _WAIT_WRITE)
                except Exception:
                    return False
            except OSError:
                return False
        return True

    def _feed_in_nb(self):
        # Read encrypted bytes from the raw socket NON-BLOCKING into the incoming
        # BIO.  True = bytes fed; raises BlockingIOError if the socket is dry;
        # False = peer EOF.
        #
        # DELIBERATELY does NOT self._inc.write_eof() on EOF: write_eof'ing the
        # incoming BIO poisons the SSLObject's WRITE side too -- a subsequent
        # self._obj.write() then raises SSLEOFError ("EOF in violation of
        # protocol").  That breaks a TLS half-close where the peer sent a bare
        # FIN (socket.shutdown(SHUT_WR), no close_notify) but is still READING
        # our queued trailing data (test_remote_shutdown_receives_trailing_data's
        # eof_server): we must keep draining 4MB through _obj.write() AFTER our
        # read side EOF'd.  asyncio's SSLProtocol never write_eof's its incoming
        # BIO either; it surfaces read-EOF via its state machine.  We do the
        # same with the _eof_in flag, which recv_nb turns into a b'' return.
        if self._eof_in:
            return False
        try:
            enc = self._raw.recv(65536)
        except (BlockingIOError, InterruptedError):
            raise BlockingIOError()
        except OSError:
            self._eof_in = True
            return False
        if not enc:
            self._eof_in = True
            return False
        self._inc.write(enc)
        return True

    def fileno(self):
        return self._fd

    def pending(self):
        # Bytes the SSL layer can yield without reading the socket: already
        # decrypted (SSLObject.pending) PLUS undecrypted bytes sitting in the
        # incoming BIO from a read-ahead (a recv that pulled the handshake's
        # final flight AND following app data, or several records at once).
        # The transport's io loop drains this before parking READ -- the socket
        # isn't readable for BIO-buffered data, so otherwise it strands.
        try:
            return self._obj.pending() + self._inc.pending
        except Exception:
            return 0

    def do_handshake(self, timeout=None):
        deadline = None if timeout is None else (_time.monotonic() + timeout)
        while True:
            want_read = False
            with self._lock:
                try:
                    self._obj.do_handshake()
                    done = True
                except _ssl.SSLWantReadError:
                    done = False
                    want_read = True
                except _ssl.SSLWantWriteError:
                    done = False
                except _ssl.SSLEOFError:
                    raise ConnectionResetError(
                        "Connection lost during TLS handshake") from None
                except _ssl.SSLError:
                    raise
            # Always flush whatever the handshake produced (our flight).
            if not self._pump_out():
                raise ConnectionResetError(
                    "Connection lost during TLS handshake") from None
            if done:
                return
            if not want_read:
                # WantWrite: we just pumped; loop to retry.
                continue
            # WantRead: pull the peer's next flight.
            if deadline is None:
                ms = -1
            else:
                remaining = deadline - _time.monotonic()
                if remaining <= 0:
                    raise ConnectionAbortedError(
                        "SSL handshake is taking longer than {0} seconds: "
                        "aborting the connection".format(timeout))
                ms = max(1, int(remaining * 1000))
            try:
                fed = self._feed_in_nb()
            except BlockingIOError:
                try:
                    _wait_fd(self._fd, _WAIT_READ, ms)
                except Exception:
                    raise ConnectionResetError(
                        "Connection lost during TLS handshake") from None
                continue
            if not fed:
                raise ConnectionResetError(
                    "Connection lost during TLS handshake") from None

    def recv_nb(self, n):
        # SINGLE non-blocking decrypt attempt.  Returns decrypted bytes, b'' on
        # EOF/close_notify, or raises BlockingIOError if no app data is ready.
        if self._closed:
            return b""
        while True:
            with self._lock:
                try:
                    return self._obj.read(n)
                except _ssl.SSLWantReadError:
                    want_read = True
                except _ssl.SSLWantWriteError:
                    want_read = False          # renegotiation wants to send
                except _ssl.SSLZeroReturnError:
                    self._peer_close_notify = True
                    return b""
                except _ssl.SSLEOFError:
                    return b""
            # Outside the lock: flush any output we produced, then (if the read
            # wanted inbound) pull more encrypted bytes -- NON-BLOCKING.
            if not self._pump_out():
                return b""
            if want_read:
                fed = self._feed_in_nb()       # raises BlockingIOError if dry
                if not fed:
                    # Peer EOF.  A clean close_notify arrives as DATA (an alert
                    # record), so read() already raised SSLZeroReturnError above
                    # -- reaching here means a BARE FIN with no close_notify and
                    # no buffered record.  _feed_in_nb does NOT write_eof the BIO
                    # (that would poison a concurrent _obj.write() draining our
                    # trailing data), so read() would just keep wanting-read;
                    # surface the EOF directly as b''.
                    return b""
            # want_write path: we pumped; loop to retry the read.

    def recv(self, n):
        # Cooperative parking recv (used by code that owns a dedicated read g).
        if self._closed:
            return b""
        while True:
            try:
                return self.recv_nb(n)
            except BlockingIOError:
                try:
                    _wait_fd(self._fd, _WAIT_READ)
                except Exception:
                    return b""

    def recv_into(self, buffer, nbytes=0):
        n = nbytes if nbytes else len(buffer)
        data = self.recv(n)
        if not data:
            return 0
        buffer[:len(data)] = data
        return len(data)

    def send(self, data):
        # Encrypt `data` into the outgoing BIO, then flush it to the raw socket
        # (parking on WRITE until drained).  Returns the app-byte count consumed.
        if self._closed:
            raise OSError(_errno.EBADF, "TLS socket closed")
        while True:
            with self._lock:
                try:
                    n = self._obj.write(data)
                    break
                except _ssl.SSLWantReadError:
                    pass
                except _ssl.SSLWantWriteError:
                    pass
            # Renegotiation mid-write: flush + feed, then retry the write.
            if not self._pump_out():
                raise ConnectionResetError("Connection lost")
            try:
                self._feed_in_nb()
            except BlockingIOError:
                try:
                    _wait_fd(self._fd, _WAIT_READ)
                except Exception:
                    raise ConnectionResetError("Connection lost")
        if not self._pump_out():
            raise ConnectionResetError("Connection lost")
        return n

    def sendall(self, data):
        view = data if isinstance(data, memoryview) else memoryview(data)
        total = len(view)
        sent = 0
        while sent < total:
            sent += self.send(view[sent:])
        return None

    def setblocking(self, flag):
        pass

    def send_close_notify(self):
        # Best-effort: emit a close_notify alert (unwrap()) and flush it -- so a
        # peer doing a clean ssl.SSLSocket.unwrap() (which waits for ours)
        # completes instead of seeing a bare FIN.  But NOT if the peer already
        # dropped with a bare FIN itself (TCP half-close, no close_notify): the
        # session is being torn down unexpectedly and a peer that did
        # socket.shutdown(SHUT_WR) is now in recv()==b'' -- our close_notify
        # bytes would be read as junk (test_shutdown_timeout_handler_not_set).
        # asyncio's SSLProtocol likewise shuts down cleanly only when the
        # session is healthy or the peer sent its own close_notify first.
        if self._eof_in and not self._peer_close_notify:
            return
        try:
            with self._lock:
                try:
                    self._obj.unwrap()
                except (_ssl.SSLError, OSError, ValueError):
                    pass
            self._pump_out()
        except Exception:
            pass

    def shutdown(self, how):
        try:
            self._raw.shutdown(how)
        except OSError:
            pass

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._raw.close()
        except OSError:
            pass
        # Drop the SSLObject + SSLContext (and the BIO pair) so they are freed
        # promptly -- asyncio's SSLProtocol releases its sslcontext on
        # connection_lost, so the context dies even though the user's
        # transport<->protocol reference cycle lingers until the GC runs.
        # test_create_connection_memory_leak asserts the client SSLContext is
        # gone via weakref the instant the connection closes (no gc.collect()).
        # recv_nb()/send() already short-circuit on self._closed, so nothing
        # touches _obj after this; the ssl_object/context/_sslobj views just
        # return None on a closed transport, exactly like asyncio.  (Only _obj +
        # _context hold the SSLContext -- the SSLObject keeps it internally -- so
        # those are the ones to drop; the BIO pair holds no context ref.)
        self._obj = None
        self._context = None

    def getpeername(self):
        return self._raw.getpeername()

    def getsockname(self):
        return self._raw.getsockname()

    def getsockopt(self, *a):
        return self._raw.getsockopt(*a)

    def setsockopt(self, *a):
        return self._raw.setsockopt(*a)

    @property
    def family(self):
        return self._raw.family

    @property
    def type(self):
        return self._raw.type

    @property
    def proto(self):
        return self._raw.proto

    @property
    def ssl_object(self):
        return self._obj

    @property
    def context(self):
        return self._context

    @property
    def _ssl_protocol(self):
        # A real (minimal) stand-in for asyncio's SSLProtocol: white-box code
        # and tests read transport._ssl_protocol._sslcontext to recover the
        # SSLContext a connection was made with.
        return _SSLProtocolView(self)

    def __getattr__(self, name):
        if name.startswith("_") or name in ("_raw",):
            raise AttributeError(name)
        return getattr(self._raw, name)

    def __del__(self):
        if not getattr(self, "_closed", True):
            try:
                self._raw.close()
            except Exception:
                pass


class _SSLProtocolView(object):
    """Minimal asyncio-SSLProtocol-compatible view over a _MemoryBIOTLS, so
    transport._ssl_protocol._sslcontext / .ssl_object resolve like asyncio's."""
    __slots__ = ("_tls",)

    def __init__(self, tls):
        self._tls = tls

    @property
    def _sslcontext(self):
        return self._tls._context

    @property
    def _sslobj(self):
        return self._tls._obj

    def _get_extra_info(self, name, default=None):
        return default

    def pause_writing(self):
        # asyncio's SSLProtocol.pause_writing(): hold app writes off the wire.
        # We pause the driving transport's write drain so _write_buf accumulates.
        tr = getattr(self._tls, "_pg_transport", None)
        if tr is not None:
            tr._write_paused = True

    def resume_writing(self):
        tr = getattr(self._tls, "_pg_transport", None)
        if tr is not None and tr._write_paused:
            tr._write_paused = False
            tr._kick_io()   # respawn/wake the io goroutine to flush _write_buf


def _tls_wrap_client(raw, ssl_arg, server_hostname, host, handshake_timeout=None):
    """Wrap a freshly-connected client socket in cooperative TLS and finish
    the handshake.  ``ssl_arg`` is True (default context) or an SSLContext."""
    context = _ssl.create_default_context() if ssl_arg is True else ssl_arg
    if server_hostname is None and isinstance(host, str) and host:
        server_hostname = host
    tls = _MemoryBIOTLS(raw, context, server_side=False,
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

# Stock asyncio.Task types whose wakeups must be DEFERRED (scheduled via
# call_soon), never run synchronously from inside a future's set_result/cancel.
# Two distinct stock implementations exist: the C `_asyncio.Task` (exposed as
# asyncio.Task) and the pure-Python `asyncio.tasks._PyTask` -- and the Python
# one is NOT a subclass of the C one, so an `isinstance(host, asyncio.Task)`
# check alone misses every _PyTask (CPython's own test_asyncio drives many).
# We list BOTH; PygoTask (our own) is excluded at the call site since it wants
# synchronous wakes.  See _fire_callbacks.
_STOCK_TASK_TYPES = (asyncio.Task,)
_PyTaskCls = getattr(asyncio.tasks, "_PyTask", None)
if _PyTaskCls is not None and _PyTaskCls is not asyncio.Task:
    _STOCK_TASK_TYPES = (asyncio.Task, _PyTaskCls)


def _pg_convert_future_exc(exc):
    """Convert a concurrent.futures exception to its asyncio twin when a
    concurrent.futures.Future result is marshalled into an asyncio Future
    (run_in_executor / wrap_future).  Reuses asyncio's own
    futures._convert_future_exc when present (handles CancelledError +
    InvalidStateError, version-correctly); falls back to a local mapping."""
    try:
        conv = asyncio.futures._convert_future_exc
    except AttributeError:
        conv = None
    if conv is not None:
        try:
            return conv(exc)
        except Exception:
            pass
    import concurrent.futures as _cf
    klass = type(exc)
    if klass is _cf.CancelledError:
        return asyncio.CancelledError(*exc.args).with_traceback(exc.__traceback__)
    ise = getattr(_cf, "InvalidStateError", None)
    if ise is not None and klass is ise:
        return asyncio.InvalidStateError(*exc.args).with_traceback(exc.__traceback__)
    return exc


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

# Every PygoEventLoop that has been constructed and not yet close()'d, across
# the process.  close()'s sched_reset() bulldozes the SHARED per-thread sleep
# heap + ready ring, which would drop another still-open loop's in-flight work
# -- and not just its tasks: a raw call_later timer goroutine (an asyncio.sleep
# that a server handler on a sibling loop is parked on) lives in that shared
# sleep heap too, invisible to the _PG_ALL_TASKS task guard.  So close() only
# resets when it is the LAST open loop (see _cancel_outstanding_tasks).  WeakSet
# so a loop that is GC'd without close() drops out on its own.
_PG_OPEN_LOOPS = _weakref.WeakSet()


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
        # The actual CancelledError instance a cancelled coroutine raised, so
        # result()/exception() re-raise the SAME object (identity + chained
        # context), matching asyncio.Future._cancelled_exc.  None until set.
        self._pg_cancelled_exc = None
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
        # Read the _closed ATTRIBUTE, not the is_closed() method: asyncio's
        # internal machinery never invokes the (user-overridable) is_closed()
        # for its own checks, and janus's test_closed_loop_non_failing asserts
        # an exact is_closed() call count -- calling the method here inflates it.
        if loop is None or getattr(loop, "_closed", False):
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
        # Preserve the exact CancelledError a cancelled coroutine raised (its
        # identity and __context__), exactly like asyncio.Future, so
        # `assertIs(awaited_exc, raised)` holds and chained context survives.
        exc = self._pg_cancelled_exc
        if exc is not None:
            self._pg_cancelled_exc = None
            return exc
        msg = self._pgcancelmsg
        if msg is None:
            return asyncio.CancelledError()
        return asyncio.CancelledError(msg)

    # ---- callbacks ----
    def add_done_callback(self, callback, *, context=None):
        if self._pgstate != _PENDING:
            # asyncio contract: a callback added to an ALREADY-DONE future is
            # scheduled via call_soon, NEVER run inline.  Library code depends on
            # this -- e.g. asyncio.as_completed adds _handle_completion to each
            # future inside its own setup loop; firing it synchronously re-enters
            # before _todo exists (AttributeError) and the async-for hangs.  (The
            # PENDING->done path in _fire_callbacks stays synchronous on purpose
            # for pygo's wake timing; only THIS already-done path must defer.)
            loop = self._loop
            if loop is not None and not getattr(loop, "_closed", False):
                try:
                    loop.call_soon(callback, self, context=context)
                except BaseException as e:
                    self._report_exc(e)
            else:
                # No usable loop (teardown): best-effort inline.
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
            # A stock asyncio.Task (C `_asyncio.Task` OR pure-Python `_PyTask`)
            # awaiting a PygoFuture registers its Task.__wakeup as the
            # done-callback, WITH context=task._context (aiohttp eager-starts
            # C Task()s directly; CPython's own test_asyncio drives _PyTask).
            # Stock asyncio schedules future callbacks via loop.call_soon; firing
            # __wakeup SYNCHRONOUSLY from inside the future's own cancel()/
            # set_result is wrong two ways: the C Task mishandles re-entry (never
            # reschedules __step -> the awaiting task hangs, e.g. write_bytes,
            # the streaming-request body-writer, cancelled on connection close),
            # and a _PyTask whose task._context is ALREADY entered higher on the
            # stack (a self-cancelling task: cancel() inside its own coro fires
            # the just-registered wakeup before __step returns) makes ctx.run
            # raise "cannot enter context" -> the wake is dropped -> hang
            # (test_tasks::test_cancel_current_task).  Defer BOTH stock-task
            # kinds (match asyncio); every other callback -- pygo's _wake_unpark,
            # library done-callbacks -- stays synchronous, preserving pygo's wake
            # timing.
            host = getattr(cb, "__self__", None)
            if (loop is not None and not getattr(loop, "_closed", False)
                    and isinstance(host, _STOCK_TASK_TYPES)
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
                except RuntimeError as e:
                    # Defensive net for ANY callback registered with a context
                    # that is already entered higher on this stack (a future
                    # completed synchronously from inside that very context).
                    # Context.run rejects re-entry BEFORE invoking cb, so cb has
                    # NOT run; deferring to the next loop tick (the context has
                    # exited by then) mirrors asyncio's always-call_soon dispatch
                    # rather than dropping the wake and hanging the awaiter.
                    if (ctx is not None and loop is not None
                            and not getattr(loop, "_closed", False)
                            and str(e).startswith("cannot enter context")):
                        try:
                            loop.call_soon(cb, self, context=ctx)
                        except BaseException as e2:
                            self._report_exc(e2)
                    else:
                        self._report_exc(e)
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
        # Run the driver under a "<module>" root frame so libraries that derive
        # their module by walking frame.f_back (aiohttp web.AppKey) reach one,
        # matching asyncio.  Capture the creator's module name HERE (we're on
        # its live stack) before the goroutine swaps stacks.  See
        # _pg_run_with_module_root.  Clearing g->callable at completion still
        # breaks the task<->g cycle: the closure's only strong ref to self is
        # the bound self._driver it carries, dropped when the closure is.
        if _PG_MODULE_ROOT_ON:
            _modname = _pg_capture_module_name()
            _driver = self._driver
            _body = lambda: _pg_run_with_module_root(_driver, _modname)
        else:
            _body = self._driver
        # Driver goroutines run arbitrary user async code (deep C-recursive
        # first-time imports overflow the default 128 KB g-stack and SEGV), so
        # give them a roomier stack.  Override with PYGO_AIO_TASK_STACK.
        self._g = pygo_core.go(_body, stack_size=_TASK_STACK) \
            if _TASK_STACK else pygo_core.go(_body)

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
                except asyncio.CancelledError as cancel_exc:
                    if not self.done():
                        # Keep the SAME CancelledError instance the coroutine
                        # raised so a parent awaiting THIS task receives it
                        # unchanged (identity + chained context), like asyncio's
                        # Task._cancelled_exc.  _pgcancelmsg still carries the msg.
                        self._pg_cancelled_exc = cancel_exc
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
        _PG_OPEN_LOOPS.add(self)
        # fd -> {"r": reader _Handle|None, "w": writer _Handle|None,
        #        "g": the single per-fd I/O goroutine}.  See add_reader.
        self._io = {}
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
        # loop.set_task_factory() target.  None => default (build a PygoTask);
        # otherwise a callable (loop, coro, **kwargs) -> Task that create_task
        # delegates to.  Custom factories install Task subclasses for OTel /
        # structlog / contextvar instrumentation and are exercised directly by
        # CPython's test_asyncio (RunCoroutineThreadsafe + task-factory tests).
        self._task_factory = None
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
    def create_task(self, coro, *, name=None, context=None, **kwargs):
        self._check_closed()
        if self._can_spawn_here():
            return self._pg_make_task(coro, name, context, kwargs)
        # Foreign thread: PygoTask.__init__ spawns a goroutine, which would land
        # on the CALLING thread's sched (never drained by this loop).  Marshal
        # the creation onto the loop's own thread via its thread-safe queue and
        # block for the task (mirrors asyncio.run_coroutine_threadsafe).
        box = {}
        ev = _threading.Event()
        def _mk():
            try:
                box["t"] = self._pg_make_task(coro, name, context, kwargs)
            except BaseException as e:
                box["e"] = e
            finally:
                ev.set()
        self.call_soon_threadsafe(_mk)
        ev.wait()
        if "e" in box:
            raise box["e"]
        return box["t"]

    def _pg_make_task(self, coro, name, context, kwargs):
        # Build the task for create_task, honouring a custom task factory
        # (loop.set_task_factory).  Mirrors BaseEventLoop.create_task: the
        # factory is called WITHOUT name (context only when non-None), then
        # task.set_name(name) applies the name -- so a factory installing a
        # plain asyncio.Task / Task subclass works exactly as on stock.  No
        # factory => our own PygoTask (the default, goroutine-driven path).
        factory = self._task_factory
        if factory is not None:
            if context is not None:
                kwargs = dict(kwargs)
                kwargs["context"] = context
            task = factory(self, coro, **kwargs)
            task.set_name(name)
            return task
        # Default path: PygoTask ignores any stray kwargs (eager_start etc. --
        # the eager-task factory installs its own factory above).
        return PygoTask(coro, loop=self, name=name, context=context)

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
            try:
                while not stop[0] and not self._closed and not self._stopping:
                    self._drain_ts_queue()
                    pygo_core.sched_sleep(0.002)
                # Drain once more so a stop()-companion callback (e.g. the
                # task.cancel() loop aiosmtpd queues alongside loop.stop()) runs.
                self._drain_ts_queue()
            except BaseException as e:
                # An async signal handler fired in THIS goroutine's eval loop:
                # SIGINT's default handler raises KeyboardInterrupt (Ctrl-C),
                # sys.exit raises SystemExit, and a custom handler (e.g.
                # pytest-timeout's SIGALRM) may raise anything.  The keepalive
                # runs Python every ~2ms while the loop is otherwise idle, so
                # it's the goroutine that most often catches a signal during a
                # parked run_forever() -- the only Python making progress.
                # CPython delivers the pending handler at a bytecode boundary on
                # the main thread regardless of which goroutine is current; if
                # we just let the keepalive die, an idle loop with any still-
                # parked task would hang forever (the signal never reaches
                # run_forever).  _drain_ts_queue() already swallows ordinary
                # callback exceptions (routing them to the exception handler),
                # so the ONLY thing that reaches here is an async signal-handler
                # raise (or a fatal internal error) -- either way break the
                # drive and re-raise it OUT of run_forever()/run_until_complete,
                # exactly as stock asyncio propagates a signal handler's
                # exception out of the loop.
                self._pg_signal_fatal(e)
                return
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
    # Stock asyncio keeps ONE selector key per fd carrying a COMBINED event mask
    # (READ|WRITE) and services both directions from a single readiness check.
    # pygo MUST mirror that with ONE goroutine per fd: a separate goroutine per
    # direction would park the SAME fd in netpoll twice, and the arm is one-shot
    # per fd -- so the second registration's direction silently overwrites the
    # first's.  A reader AND a writer on one fd (e.g. tornado IOStream: an
    # add_writer to detect connect completion + an add_reader for the response,
    # both live at once) then lose one direction's wakeups: the connect-write
    # event never fires, the queued request never flushes, the peer hangs.  So a
    # single per-fd goroutine parks on the UNION mask and dispatches by the
    # ready mask wait_fd returns; interest changes wake it to re-evaluate.
    def _pg_fileobj_to_fd(self, fileobj):
        # asyncio/selectors contract: accept an int fd or an object exposing
        # fileno(); anything else is a ValueError (test_add_reader_invalid_
        # argument).  Without this an arbitrary object would be stored as a live
        # io key and silently ignored instead of erroring.
        if isinstance(fileobj, int):
            fd = fileobj
        else:
            try:
                fd = int(fileobj.fileno())
            except (AttributeError, TypeError, ValueError):
                raise ValueError(
                    "Invalid file object: {0!r}".format(fileobj)) from None
        if fd < 0:
            raise ValueError("Invalid file descriptor: {0}".format(fd))
        return fd

    def add_reader(self, fd, callback, *args):
        return self._pg_set_io(self._pg_fileobj_to_fd(fd), 1,
                               _Handle(callback, args, self))

    def remove_reader(self, fd):
        return self._pg_clear_io(self._pg_fileobj_to_fd(fd), 1)

    def add_writer(self, fd, callback, *args):
        return self._pg_set_io(self._pg_fileobj_to_fd(fd), 2,
                               _Handle(callback, args, self))

    def remove_writer(self, fd):
        return self._pg_clear_io(self._pg_fileobj_to_fd(fd), 2)

    def _pg_set_io(self, fd, evt, handle):
        st = self._io.get(fd)
        if st is None:
            st = {"r": None, "w": None, "g": None}
            self._io[fd] = st
        key = "r" if evt == 1 else "w"
        old = st[key]
        if old is not None:
            old._cancelled = True
        st[key] = handle
        self._pg_kick_io(fd, st)
        return handle

    def _pg_clear_io(self, fd, evt):
        st = self._io.get(fd)
        if st is None:
            return False
        key = "r" if evt == 1 else "w"
        h = st[key]
        if h is None:
            return False
        h._cancelled = True
        st[key] = None
        if st["r"] is None and st["w"] is None:
            self._io.pop(fd, None)
        self._pg_kick_io(fd, st)
        return True

    def _pg_kick_io(self, fd, st):
        # Wake the fd's I/O goroutine (if parked) so it re-reads the interest
        # mask after a reader/writer was added/removed; spawn it if none runs.
        g = st["g"]
        if g is not None:
            try:
                g.cancel_wait_fd()   # raises CancelledError in its _wait_fd;
            except Exception:        # the runner catches it and re-evaluates.
                pass
        if g is None and (st["r"] is not None or st["w"] is not None):
            st["g"] = pygo_core.go(lambda: self._pg_io_runner(fd, st))

    def _pg_io_runner(self, fd, st):
        while True:
            r = st["r"]; w = st["w"]
            mask = (1 if (r is not None and not r._cancelled) else 0) \
                 | (2 if (w is not None and not w._cancelled) else 0)
            if mask == 0:
                st["g"] = None
                return
            try:
                ready = _wait_fd(fd, mask)
            except asyncio.CancelledError:
                # Interest changed (or fd dropped) via _pg_kick_io -- re-loop to
                # recompute the mask and re-park, or exit if nothing's left.
                continue
            except Exception:
                st["g"] = None
                return
            # Re-read each slot at dispatch time: a reader callback may add/remove
            # the writer (or close the fd) before we service the write side.
            if (ready & 1) and st["r"] is not None and not st["r"]._cancelled:
                st["r"]._run()
            if (ready & 2) and st["w"] is not None and not st["w"]._cancelled:
                st["w"]._run()
            # Yield before re-arming, mimicking a level-triggered selector pass.
            pygo_core.sched_yield_classic()

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
    async def _make_subprocess(self, protocol, args, *, shell,
                               stdin, stdout, stderr, **kwargs):
        # Spawn the child, then connect its pipes by AWAITING connect_*_pipe.  If
        # that connect raises (e.g. cancellation) the child is already running,
        # so kill + reap it before propagating -- mirror asyncio's
        # _make_subprocess_transport so create_subprocess_* never leaks a child.
        transport = _SubprocessTransport(
            self, protocol, args, shell=shell,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        try:
            await transport._connect_pipes()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            transport.close()
            await transport._wait()
            raise
        return transport

    async def subprocess_exec(self, protocol_factory, program, *args,
                              stdin=_subprocess.PIPE, stdout=_subprocess.PIPE,
                              stderr=_subprocess.PIPE, **kwargs):
        _reject_subprocess_text_mode(kwargs)
        protocol = protocol_factory()
        transport = await self._make_subprocess(
            protocol, [program] + list(args), shell=False,
            stdin=stdin, stdout=stdout, stderr=stderr, **kwargs)
        return transport, protocol

    async def subprocess_shell(self, protocol_factory, cmd,
                               stdin=_subprocess.PIPE, stdout=_subprocess.PIPE,
                               stderr=_subprocess.PIPE, **kwargs):
        _reject_subprocess_text_mode(kwargs)
        protocol = protocol_factory()
        transport = await self._make_subprocess(
            protocol, cmd, shell=True,
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
                # Clear last_err as we raise so the propagating exception's
                # traceback frame doesn't keep referencing it (exc -> tb ->
                # this frame -> last_err -> exc).  asyncio breaks the same cycle
                # explicitly; test_open_connection_happy_eyeball_refcycles
                # asserts gc.get_referrers(exc) == [].
                try:
                    raise last_err or OSError("could not connect")
                finally:
                    last_err = None
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
                            ssl_handshake_timeout=None, start_serving=True,
                            **_ignored):
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
                               ssl_handshake_timeout=ssl_handshake_timeout,
                               start_serving=start_serving)

    # ---- Unix domain sockets (loop.create_unix_server / _connection) ----
    # The base class raises NotImplementedError; UDS is common for local IPC
    # (uvicorn/gunicorn --uds, database sockets).  Mirror create_server /
    # create_connection with an AF_UNIX socket.
    async def create_unix_server(self, protocol_factory, path=None, *, sock=None,
                                 backlog=100, ssl=None, cleanup_socket=True,
                                 ssl_handshake_timeout=None, start_serving=True,
                                 **_ignored):
        if path is not None and sock is not None:
            raise ValueError(
                "path and sock can not be specified at the same time")
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
                               ssl_context=ssl, cleanup_unix=cleanup_socket,
                               ssl_handshake_timeout=ssl_handshake_timeout,
                               start_serving=start_serving)

    async def create_unix_connection(self, protocol_factory, path=None, *,
                                     ssl=None, sock=None, server_hostname=None,
                                     ssl_handshake_timeout=None, **_ignored):
        if path is not None and sock is not None:
            raise ValueError(
                "path and sock can not be specified at the same time")
        if sock is None:
            if path is None:
                raise ValueError("no path and sock were specified")
            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                try:
                    sock.connect(path)
                except BlockingIOError:
                    _wait_fd(sock.fileno(), 2)
                    err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
                    if err != 0:
                        raise OSError(err, "connect failed")
            except BaseException:
                # connect() to a missing/forbidden path fails IMMEDIATELY with
                # FileNotFoundError / PermissionError -- never raising
                # BlockingIOError -- so close the socket on ANY failure (as
                # asyncio's create_unix_connection does), or it leaks and
                # surfaces as ResourceWarning("unclosed <socket ...>").
                sock.close()
                raise
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
        # The old io goroutine is parked in _wait_fd on this fd; a bare sleep(0)
        # won't wake it, so it would linger parked and STEAL the post-handshake
        # data wakeup meant for the new TLS transport (then exit), stranding the
        # read -> b''.  Cancel its park so it observes _stopping and exits NOW.
        old_g = getattr(transport, "_io_g", None)
        if old_g is not None:
            try:
                old_g.cancel_wait_fd()
            except Exception:
                pass
        await asyncio.sleep(0)   # give the old io loop a turn to observe + exit
        # gh-142352: on the server side the peer's TLS ClientHello may have
        # ALREADY been read off the plaintext socket into the StreamReader's
        # buffer (a server that waits for data before calling start_tls -- see
        # test_streams::test_start_tls_buffered_data).  Those bytes are gone from
        # the socket, so seed them into the handshake's incoming BIO or the
        # server's do_handshake() blocks forever waiting for a ClientHello that
        # already arrived.  Mirror asyncio's base_events.start_tls: pull the
        # StreamReaderProtocol's _stream_reader._buffer and clear it.
        incoming_data = b""
        if server_side:
            stream_reader = getattr(protocol, "_stream_reader", None)
            if stream_reader is not None:
                buffer = getattr(stream_reader, "_buffer", None)
                if buffer:
                    incoming_data = bytes(buffer)
                    buffer.clear()
        tls = _MemoryBIOTLS(sock, sslcontext, server_side=server_side,
                       server_hostname=server_hostname,
                       incoming_data=incoming_data)
        tls.do_handshake(ssl_handshake_timeout)
        # Transfer the accepting server's registration from the old (now
        # quiesced) transport to the new TLS one.  The accepted transport sits in
        # the server's _conns set and its connection_lost would _detach it -- but
        # we suppressed that connection_lost for the upgrade, so without moving
        # the registration the old transport lingers in _conns forever (it is
        # also pinned by its parked io goroutine) and the new transport never
        # detaches, so server.wait_closed() blocks for good (the scheduler then
        # drains to empty -> "event loop stopped before Future completed").
        srv = getattr(transport, "_pg_server", None)
        if srv is not None:
            try:
                srv._conns.discard(transport)
            except Exception:
                pass
        new_tr = _StreamTransport(tls, protocol, loop=self,
                                  call_connection_made=False, server=srv)
        if srv is not None:
            try:
                srv._conns.add(new_tr)
            except Exception:
                pass
        return new_tr

    async def connect_accepted_socket(self, protocol_factory, sock, *, ssl=None,
                                      ssl_handshake_timeout=None, **_ignored):
        """Wrap an already-accepted socket into a transport (server side).
        AbstractEventLoop raises NotImplementedError; servers that accept()
        manually (some test harnesses, custom acceptors) hand the socket here."""
        sock.setblocking(False)
        if ssl is not None:
            tls = _MemoryBIOTLS(sock, ssl, server_side=True)
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
    def _check_sock_nonblocking(self, sock):
        # asyncio's contract for the low-level sock_* ops: in debug mode a
        # blocking socket is a usage error (it would block the whole loop).
        # Matches CPython BaseSelectorEventLoop (selector_events.py).  Outside
        # debug pygo stays lenient and coerces the socket non-blocking below.
        if self._debug and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

    async def sock_connect(self, sock, address):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        try:
            sock.connect(address)
        except BlockingIOError:
            _wait_fd(sock.fileno(), 2)
            err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
            if err != 0:
                raise OSError(err, "connect failed")

    async def sock_accept(self, sock):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                conn, addr = sock.accept()
                conn.setblocking(False)   # asyncio returns a non-blocking conn
                return conn, addr
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recv(self, sock, nbytes):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recv(nbytes)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recv_into(self, sock, buf):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recvfrom(self, sock, bufsize):
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 1)

    async def sock_recvfrom_into(self, sock, buf, nbytes=0):
        # asyncio 3.11+ API; base class raises NotImplementedError.
        self._check_sock_nonblocking(sock)
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
        self._check_sock_nonblocking(sock)
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
        self._check_sock_nonblocking(sock)
        sock.setblocking(False)
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                _wait_fd(sock.fileno(), 2)

    # recvmsg / sendmsg (POSIX): ancillary-data + SCM_RIGHTS fd passing over the
    # loop.  Not part of AbstractEventLoop, but pygo.monkey makes the blocking
    # socket.recvmsg/sendmsg cooperative and the bridge (monkey OFF) needs an
    # equivalent -- same EAGAIN -> wait_fd loop as the other sock_* ops.
    if hasattr(_socket.socket, "recvmsg"):
        async def sock_recvmsg(self, sock, bufsize, ancbufsize=0, flags=0):
            self._check_sock_nonblocking(sock)
            sock.setblocking(False)
            while True:
                try:
                    return sock.recvmsg(bufsize, ancbufsize, flags)
                except (BlockingIOError, InterruptedError):
                    _wait_fd(sock.fileno(), 1)

        async def sock_recvmsg_into(self, sock, buffers, ancbufsize=0, flags=0):
            self._check_sock_nonblocking(sock)
            sock.setblocking(False)
            while True:
                try:
                    return sock.recvmsg_into(buffers, ancbufsize, flags)
                except (BlockingIOError, InterruptedError):
                    _wait_fd(sock.fileno(), 1)

        async def sock_sendmsg(self, sock, buffers, ancdata=(), flags=0,
                               address=None):
            self._check_sock_nonblocking(sock)
            sock.setblocking(False)
            while True:
                try:
                    if address is None:
                        return sock.sendmsg(buffers, ancdata, flags)
                    return sock.sendmsg(buffers, ancdata, flags, address)
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
                    # A concurrent.futures exception crossing into asyncio-land
                    # must become its asyncio twin: concurrent.futures.Cancelled
                    # Error subclasses Exception, but asyncio.CancelledError
                    # subclasses BaseException -- distinct classes, so the raw
                    # concurrent kind slips past `except asyncio.CancelledError`.
                    # Stock wrap_future/_chain_future runs this same conversion.
                    fut.set_exception(_pg_convert_future_exc(cf_fut.exception()))
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
        # This runs in the per-fd I/O goroutine that watches the signal
        # self-pipe.  set_wakeup_fd writes EVERY caught signum to the pipe, so
        # this goroutine wakes and runs Python on the loop thread whenever any
        # signal fires -- which means CPython delivers a pending Python signal
        # handler (e.g. pytest-timeout's SIGALRM raising, or a user SIGUSR1
        # handler) at a bytecode boundary INSIDE this body.  Everything below is
        # exception-safe on its own (recv errors handled; call_soon guarded), so
        # any BaseException reaching the outer handler is an async signal-handler
        # raise -> route it out of run_forever() via the fatal path, exactly as
        # the keepalive does (otherwise this goroutine swallows it and an idle
        # loop hangs -- the reason aiosmtpd's main() under pytest-timeout hung).
        try:
            try:
                data = self._signal_rsock.recv(4096)
            except (BlockingIOError, InterruptedError, OSError):
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
        except BaseException as e:
            self._pg_signal_fatal(e)

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

    def _check_running(self):
        # asyncio contract (BaseEventLoop._check_running): a loop may not be
        # re-entered, and only ONE loop may run on a thread at a time.  Without
        # the second check a nested run_forever()/run_until_complete() (e.g. a
        # coroutine that calls another loop's run_forever) drains forever instead
        # of raising -> hang (test_base_events::test_running_loop_within_a_loop).
        if self.is_running():
            raise RuntimeError("This event loop is already running")
        if asyncio.events._get_running_loop() is not None:
            raise RuntimeError(
                "Cannot run the event loop while another loop is running")

    def run_until_complete(self, future):
        self._check_running()
        if asyncio.iscoroutine(future):
            future = self.create_task(future)
        elif not (isinstance(future, asyncio.Future)
                  or isinstance(future, PygoFuture)
                  or asyncio.isfuture(future)):
            if hasattr(future, "__await__"):
                # asyncio's run_until_complete accepts ANY awaitable -- its
                # ensure_future wraps a bare __await__ object in a coroutine.
                # aiohttp's Connector.close()/ClientSession.close() return such
                # deprecation-wrapper awaitables, so run_until_complete(
                # conn.close()) must accept them instead of rejecting anything
                # that isn't already a coroutine/Future.  Reuse asyncio's own
                # wrapper (it calls our create_task under the hood).
                future = asyncio.ensure_future(future, loop=self)
            else:
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
        # sched_reset() bulldozes the SHARED per-thread scheduler (ready ring +
        # sleep heap).  Any OTHER open loop on this thread may have live work
        # sitting there -- including raw call_later timer goroutines (a server
        # handler's in-flight asyncio.sleep) that the _PG_ALL_TASKS task guard
        # cannot see.  So only reset when we are the LAST open loop; otherwise a
        # sibling's pending sleep is silently dropped and the goroutine awaiting
        # it hangs forever (aiohttp's streaming-handler teardown deadlock).
        other_loop_open = any(
            (lp is not self and not lp._closed) for lp in list(_PG_OPEN_LOOPS))
        if not sibling_busy and not other_loop_open:
            try:
                pygo_core.sched_reset()
            except AttributeError:
                pass  # Older build without sched_reset; best-effort drain.

    def run_forever(self):
        self._check_running()
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
        return self._task_factory

    def set_task_factory(self, factory):
        # asyncio contract: None resets to the default factory; otherwise the
        # factory must be callable.  BaseEventLoop raises TypeError on a
        # non-callable, and test_asyncio asserts that.
        if factory is not None and not callable(factory):
            raise TypeError("task factory must be a callable or None")
        self._task_factory = factory

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
            tls = _MemoryBIOTLS(conn, self._ssl_context, server_side=True)
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
                 context=None, server=None):
        # The _ProtocolServer that accepted this connection (None for client
        # transports), so connection_lost can _detach() it and let the server's
        # wait_closed() complete once every connection has dropped.
        self._pg_server = server
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
        # asyncio enables TCP_NODELAY (Nagle off) on every TCP stream transport
        # -- _SelectorSocketTransport calls _set_nodelay in __init__.  Without it
        # a small write (e.g. a websocket ping frame) sits in the send buffer
        # until the idle peer's delayed ACK (up to ~40 ms), stalling
        # request/response and keepalive round-trips that stock asyncio completes
        # in microseconds.  Mirror asyncio's _set_nodelay exactly: TCP sockets
        # only (AF_INET/AF_INET6 + SOCK_STREAM + IPPROTO_TCP); never AF_UNIX.
        if (sock.family in (_socket.AF_INET, _socket.AF_INET6) and
                sock.type == _socket.SOCK_STREAM and
                sock.proto == _socket.IPPROTO_TCP):
            try:
                sock.setsockopt(_socket.IPPROTO_TCP, _socket.TCP_NODELAY, 1)
            except OSError:
                pass
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        self._stopping = False
        self._paused = False        # pause_reading() flow control
        self._read_eof = False      # peer half-closed: stop reading, keep writing
        self._eof_written = False   # write_eof() done -> write() must raise
        self._eof_pending = False   # write_eof() requested, buffer not yet drained
        self._tls_shutdown_sent = False  # answered a peer close_notify with ours
        self._conn_lost_called = False  # connection_lost fires exactly once
        self._in_context = False    # re-entrancy guard for _run_cb (see below)
        # ---- write buffering (single ordered buffer, drained by the ONE io
        # goroutine) ----  pygo's netpoll arms one direction per fd one-shot, so
        # a separate write goroutine parking EPOLLOUT would clobber the recv
        # goroutine's EPOLLIN arm and strand the read under full-duplex
        # backpressure (verified).  So recv AND drain share a single goroutine
        # that parks on the UNION mask, exactly like add_reader/add_writer's
        # _pg_io_runner.  write() appends here and kicks that goroutine.
        self._write_buf = bytearray()
        self._protocol_paused = False   # did we call protocol.pause_writing()?
        # Explicit write-side flow control: transport._ssl_protocol.pause_writing()
        # (white-box TLS tests, e.g. test_ssl test_flush_before_shutdown) stops
        # the io goroutine from draining _write_buf so app writes accumulate;
        # resume_writing() flushes.  A close() always clears it (a graceful close
        # must flush the buffer regardless).  Distinct from _paused (read side).
        self._write_paused = False
        self._high_water = 64 * 1024
        self._low_water = 16 * 1024
        # Let the TLS layer's _SSLProtocolView reach back to us for the
        # pause_writing()/resume_writing() flow-control surface asyncio's
        # SSLProtocol exposes.  Only MemoryBIO TLS socks accept the attribute;
        # plaintext sockets don't (and have no _ssl_protocol anyway).
        try:
            sock._pg_transport = self
        except (AttributeError, TypeError):
            pass
        # Graceful close: close() with data still queued flushes the buffer
        # before tearing down (asyncio semantics -- a write()+close() must not
        # drop the write).  The io goroutine's _drain_step fires the teardown
        # once the buffer empties.
        self._close_when_drained = False
        self._close_exc = None
        self._close_deliver_cl = False
        # A plaintext non-blocking socket's send() never parks, so write() can
        # fast-path an immediate send from the caller's goroutine.  A _TLSSock
        # send() can park EPOLLOUT (and clobber the recv arm), so TLS writes
        # ALWAYS go through the buffer + single io goroutine.
        self._sock_is_plain = getattr(sock, "ssl_object", None) is None
        # start_tls reuses an already-connected protocol, so it suppresses the
        # re-fire (asyncio doesn't call connection_made again on TLS upgrade).
        if call_connection_made:
            try:
                self._run_cb(protocol.connection_made, self)
            except Exception as e:
                self._report(e, "connection_made")
        self._io_g = pygo_core.go(self._io_loop)

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

    def _io_loop(self):
        # The ONE goroutine for this fd.  Parks on the union of the directions
        # we currently need (READ unless paused / read-EOF'd, WRITE while the
        # write buffer is non-empty) and services whichever is ready.  Merging
        # recv and drain into one goroutine is mandatory on pygo's netpoll: a
        # second goroutine parking the other direction on the same fd clobbers
        # this one's arm (one-shot per fd) and strands it.
        sock = self._sock
        while not self._stopping:
            # TLS bidirectional half-close: once the peer's close_notify has
            # been read (read EOF) AND our write buffer is fully drained, answer
            # with OUR close_notify -- the asyncio/sslproto behaviour.  Without
            # it a peer doing a clean ssl.SSLSocket.unwrap() blocks forever
            # waiting for our close_notify (test_remote_shutdown trailing-data:
            # its server reads our data, then ends its read loop only on our
            # close_notify).  Fire once, here in the io goroutine (send may park).
            if (self._read_eof and not self._write_buf and not self._closed
                    and not self._sock_is_plain and not self._tls_shutdown_sent):
                self._send_tls_close_notify()
            mask = 0
            if not self._paused and not self._read_eof and not self._closed:
                mask |= 1
            if self._write_buf and not self._write_paused:
                mask |= 2
            if mask == 0:
                # Paused/EOF'd for reading and nothing queued to write: nobody
                # needs the fd right now.  Exit; resume_reading()/write() will
                # respawn us via _kick_io.  (Incoming bytes wait in the kernel
                # buffer = correct read backpressure.)
                self._io_g = None
                return
            if (mask & 1) and not self._sock_is_plain:
                # TLS read-ahead: the SSL layer may hold decrypted bytes OR whole
                # undecrypted records buffered (read together with the handshake
                # flight / a prior record).  The socket isn't readable for that,
                # so _wait_fd(READ) would never fire -- drain it before parking.
                # Stop draining when pending() stops dropping (a partial record
                # that needs more socket bytes).
                drained = False
                while not self._stopping:
                    try:
                        before = self._sock.pending()
                    except Exception:
                        before = 0
                    if not before:
                        break
                    if not self._recv_step():
                        return
                    drained = True
                    try:
                        if self._sock.pending() >= before:
                            break          # no progress: partial record, park
                    except Exception:
                        break
                if drained:
                    pygo_core.sched_yield_classic()
                    # RE-LOOP, don't fall through: _recv_step delivered data,
                    # which can wake a peer coroutine that queues writes during
                    # the yield above (a streaming write loop, or a test's
                    # _test__append_write_backlog).  `mask` was computed at the
                    # top of THIS iteration -- stale now -- so parking on it
                    # would park READ-only and strand the new write buffer.
                    # Re-evaluating the mask picks up WRITE.
                    continue
            try:
                fd = sock.fileno()
            except Exception:
                self._io_g = None
                return
            try:
                ready = _wait_fd(fd, mask)
            except asyncio.CancelledError:
                # Interest changed via _kick_io (write()/resume_reading()/
                # close()): re-loop to recompute the mask (or exit on _stopping).
                continue
            except Exception:
                if self._stopping:
                    return
                continue
            if self._stopping:
                return
            # Drain queued writes first so output flushes promptly, then read.
            # Re-check flags between steps: a drain error or a data_received
            # callback may close() the transport.
            if (ready & 2) and self._write_buf and not self._stopping:
                self._drain_step()
            if (ready & 1) and not self._paused and not self._read_eof \
                    and not self._stopping:
                if not self._recv_step():
                    return
            # Hand the scheduler to any goroutine a data_received just woke (a
            # protocol coroutine awaiting this read) BEFORE we recv() again.
            # Without this yield the loop can drain the whole response AND the
            # EOF/close in one burst, firing connection_lost (-> protocol state
            # CLOSED) before the woken coro ran its post-read step -- breaking
            # ordering-sensitive protocols (e.g. websockets' client handshake
            # asserts CONNECTING in connection_open()).
            pygo_core.sched_yield_classic()
        self._io_g = None

    def _recv_step(self):
        # One NON-BLOCKING recv + dispatch.  Returns True to keep looping, False
        # to stop the io goroutine (transport closed).  Must not park: a
        # plaintext socket.recv raises BlockingIOError when dry; a _TLSSock's
        # parking recv() would stall the write drain on this same goroutine, so
        # use its single-shot recv_nb instead.
        #
        # BufferedProtocol path: ask the protocol for a buffer and recv straight
        # into it (get_buffer -> recv_into -> buffer_updated), asyncio's
        # zero-copy read contract, instead of recv() -> data_received().  Checked
        # dynamically so a set_protocol() swap is always honoured.
        proto = self._protocol
        if isinstance(proto, asyncio.BufferedProtocol):
            return self._recv_step_buffered(proto)
        sock = self._sock
        try:
            recv_nb = getattr(sock, "recv_nb", None)
            data = recv_nb(65536) if recv_nb is not None else sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return True            # nothing ready / spurious readiness; re-park
        except OSError as e:
            if self._stopping: return False
            if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                return True
            # Route through close() so connection_lost(e) fires exactly once
            # (the guard) rather than racing close()'s own call.
            self.close(e)
            return False
        if not data:
            return self._handle_read_eof()
        try:
            self._run_cb(self._protocol.data_received, data)
        except Exception as e:
            # asyncio treats an exception out of data_received() as fatal: close
            # the transport and deliver connection_lost(exc).  Without this a
            # protocol that faults mid-read never gets connection_lost, so any
            # await on closure (websockets recv() -> shield(connection_lost_
            # waiter)) hangs forever.  close()'s guard keeps it single-fire.
            self._report(e, "data_received")
            self.close(e)
            return False
        return True

    def _handle_read_eof(self):
        # EOF: peer half-closed its write side; recv() now returns b'' forever,
        # so stop READING (a `continue` here would busy-spin at 100% CPU).  Keep
        # the transport (and this goroutine, for our own writes) only if the
        # protocol asked (eof_received() -> True).  Shared by the data_received
        # and BufferedProtocol read paths.
        try:
            keep = self._run_cb(self._protocol.eof_received)
        except Exception as e:
            self._report(e, "eof_received")
            keep = False
        if not keep:
            self.close()
            # A TLS half-close: the peer sent close_notify (our read side is now
            # EOF) but we may still owe it queued output -- the classic
            # remote-shutdown-with-trailing-data case
            # (test_remote_shutdown_receives_trailing_data, where the peer reads
            # 4MB AFTER sending close_notify).  close() deferred teardown to
            # flush that backlog (_close_when_drained); KEEP this io goroutine
            # alive to drain it -- _closed now masks READ off so we only pump
            # WRITE, and _drain_step fires the teardown (and our close_notify)
            # once empty.  With nothing queued, close() tore down already ->
            # stop the goroutine.
            return self._close_when_drained
        self._read_eof = True   # mask drops READ; loop stays for writes
        return True

    def _recv_step_buffered(self, proto):
        # BufferedProtocol read: get_buffer(-1) -> recv_into(buf) ->
        # buffer_updated(nbytes).  Mirrors asyncio _SelectorSocketTransport's
        # _read_ready__get_buffer.  Must not park (same constraint as
        # _recv_step): plain sockets recv_into non-blocking and zero-copy; a
        # _TLSSock has no non-blocking recv_into, so use its single-shot recv_nb
        # and copy into the protocol's buffer.
        sock = self._sock
        try:
            buf = self._run_cb(proto.get_buffer, -1)
            if not len(buf):
                raise RuntimeError("get_buffer() returned an empty buffer")
        except Exception as e:
            self._report(e, "get_buffer")
            self.close(e)
            return False
        try:
            recv_nb = getattr(sock, "recv_nb", None)
            if recv_nb is None:
                nbytes = sock.recv_into(buf)          # plain: zero-copy
            else:
                data = recv_nb(len(memoryview(buf)))  # TLS: single-shot + copy
                if data:
                    memoryview(buf)[:len(data)] = data
                nbytes = len(data)
        except (BlockingIOError, InterruptedError):
            return True
        except OSError as e:
            if self._stopping:
                return False
            if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                return True
            self.close(e)
            return False
        if not nbytes:
            return self._handle_read_eof()
        try:
            self._run_cb(proto.buffer_updated, nbytes)
        except Exception as e:
            self._report(e, "buffer_updated")
            self.close(e)
            return False
        return True

    def _drain_step(self):
        # Send as much of the write buffer as the socket accepts now.  Snapshot
        # a bounded chunk: a _TLSSock send() can park (releasing its CoLock), so
        # a concurrent write() may append meanwhile -- snapshotting keeps the
        # bytes we send stable, and del[:n] still removes exactly the consumed
        # prefix (appends stay at the tail for the next pass).
        sock = self._sock
        chunk = bytes(self._write_buf[:262144])
        try:
            n = sock.send(chunk)
        except (BlockingIOError, InterruptedError):
            return                 # not actually writable; re-park
        except OSError as e:
            # Peer dropped mid-drain.  If we were draining for a graceful close,
            # close() already ran (so it'd early-return) -- fire the teardown
            # directly; otherwise route through close(e).
            if self._close_when_drained:
                self._close_when_drained = False
                self._stopping = True
                self._deliver_connection_lost(e, self._close_deliver_cl)
            else:
                self.close(e)
            return
        if n:
            del self._write_buf[:n]
            self._maybe_resume_writing()
        if not self._write_buf:
            if self._eof_pending and not self._eof_written and not self._closed:
                # Buffer drained: honour the deferred write_eof half-close.
                self._eof_pending = False
                self._eof_written = True
                try:
                    sock.shutdown(_socket.SHUT_WR)
                except OSError:
                    pass
            if self._close_when_drained and not self._stopping:
                # Graceful close's queued output is flushed: send our TLS
                # close_notify (so a peer in unwrap() completes), then tear down.
                if self._close_exc is None:
                    self._send_tls_close_notify()
                self._close_when_drained = False
                self._stopping = True
                self._deliver_connection_lost(self._close_exc,
                                              self._close_deliver_cl)

    def _kick_io(self):
        # Re-evaluate the io goroutine's interest mask after write()/resume_
        # reading() changed it: wake it if parked, or respawn it if it had
        # exited (mask was 0).
        if self._stopping:
            return
        g = self._io_g
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
        else:
            self._io_g = pygo_core.go(self._io_loop)

    def _maybe_pause_writing(self):
        if not self._protocol_paused and len(self._write_buf) > self._high_water:
            self._protocol_paused = True
            try:
                self._run_cb(self._protocol.pause_writing)
            except Exception as e:
                self._report(e, "pause_writing")

    def _maybe_resume_writing(self):
        if self._protocol_paused and len(self._write_buf) <= self._low_water:
            self._protocol_paused = False
            try:
                self._run_cb(self._protocol.resume_writing)
            except Exception as e:
                self._report(e, "resume_writing")

    def write(self, data):
        if self._eof_written or self._eof_pending:
            # Mirror stock asyncio's selector transport so callers (e.g.
            # websockets' broadcast) see the failure they expect, with the
            # same message they assert on.
            raise RuntimeError("Cannot call write() after write_eof()")
        if self._closed:
            return
        if not data:
            return
        if not self._write_buf and self._sock_is_plain:
            # Fast path: nothing queued and a plaintext non-blocking socket whose
            # send() never parks -- send straight from the caller's goroutine.
            # (A _TLSSock send() can park EPOLLOUT and clobber the recv arm, so
            # TLS skips this and always buffers + lets the io goroutine drain.)
            try:
                n = self._sock.send(data)
            except (BlockingIOError, InterruptedError):
                n = 0
            except OSError as e:
                # close() delivers connection_lost(e) exactly once -- calling it
                # here too double-fires it (websockets' connection_lost sets a
                # one-shot Future -> InvalidStateError "Future already done").
                self.close(e)
                return
            if n >= len(data):
                return                          # fully sent, no buffer needed
            data = memoryview(data)[n:]         # buffer the remainder, in order
        # Queue (preserving order) and hand off to the single io goroutine,
        # which drains EPOLLOUT on the SAME union-mask park as the recv side.
        self._write_buf += bytes(data)
        self._maybe_pause_writing()
        self._kick_io()

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def _send_tls_close_notify(self):
        # Graceful TLS close: emit OUR close_notify so a peer doing a clean
        # ssl.SSLSocket.unwrap() -- which sends its close_notify then BLOCKS
        # for ours -- completes instead of seeing a bare TCP FIN (which it
        # surfaces as SSLEOFError UNEXPECTED_EOF_WHILE_READING and treats as a
        # protocol violation).  asyncio's SSLProtocol sends close_notify on
        # close whether or not the peer sent theirs first; pygo's stream-EOF
        # path (_handle_read_eof -> eof_received()==False -> close()) used to
        # close the raw socket with no close_notify, so a server doing the
        # symmetric unwrap() (test_ssl::test_shutdown_cleanly) aborted -- and
        # under its sequential threaded server that abort cascaded a FIN to
        # every still-handshaking client.  Idempotent (the io-loop half-close
        # block shares _tls_shutdown_sent) and a no-op on plaintext sockets.
        if self._sock_is_plain or self._tls_shutdown_sent:
            return
        snc = getattr(self._sock, "send_close_notify", None)
        if snc is None:
            return
        self._tls_shutdown_sent = True
        snc()

    def close(self, exc=None):
        if self._closed:
            return
        # _closed marks the transport closing: is_closing() True, further
        # write()s dropped, the io loop stops READing.  But DON'T tear down yet
        # if a graceful close still has queued output -- flush it first.
        self._closed = True
        # A graceful close must flush queued output even if writing was paused
        # via _ssl_protocol.pause_writing() -- asyncio drains the buffer before
        # connection_lost.  Lift the pause so the io goroutine can drain.
        self._write_paused = False
        deliver_cl = not self._conn_lost_called
        if deliver_cl:
            self._conn_lost_called = True
        if exc is None and self._write_buf:
            # Graceful close with data queued: let the io goroutine drain the
            # buffer; its _drain_step fires the teardown (and our close_notify)
            # once empty (asyncio flushes the write buffer before
            # connection_lost).
            self._close_exc = None
            self._close_deliver_cl = deliver_cl
            self._close_when_drained = True
            self._kick_io()
            return
        # Graceful close with nothing queued: send our TLS close_notify before
        # the FIN so a peer blocked in unwrap() completes (see helper).  Only on
        # a clean close -- an error/abort close (exc set) skips it.
        if exc is None:
            self._send_tls_close_notify()
        # Error/abort close, or nothing queued: tear down now.
        # asyncio closes the fd inside the DEFERRED _call_connection_lost, NOT
        # synchronously here.  Code routinely reads the socket right after
        # transport.close() -- e.g. aiohttp's fingerprint-mismatch path does
        # transport.close() then transport.get_extra_info("socket").getpeername()
        # to drop the bad peer -- so closing the fd synchronously gives them
        # EBADF (and the resulting OSError masks the ServerFingerprintMismatch
        # they expect).  Defer the shutdown+close to the loop turn that delivers
        # connection_lost.
        self._stopping = True
        # Wake the io goroutine if parked so it sees _stopping and exits now.
        g = self._io_g
        if g is not None:
            try:
                g.cancel_wait_fd()
            except Exception:
                pass
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
        def _detach_server():
            # Let the accepting server's wait_closed() learn this connection is
            # gone (asyncio calls server._detach from connection_lost).
            srv = self._pg_server
            if srv is not None and deliver_cl:
                try:
                    srv._detach(self)
                except Exception:
                    pass
        def _deliver():
            if deliver_cl:
                try:
                    self._protocol.connection_lost(exc)
                except Exception as e:
                    self._report(e, "connection_lost")
            _close_sock_now()
            _detach_server()
            # Release the SSL references the extra dict holds (the SSLObject and
            # SSLContext stored at construction for get_extra_info).  asyncio's
            # SSLProtocol drops its sslcontext on connection_lost, so the context
            # dies even though the user's transport<->protocol reference cycle
            # lingers until the GC runs (test_create_connection_memory_leak
            # asserts the client SSLContext is gone via weakref the instant the
            # connection closes -- no gc.collect()).  _MemoryBIOTLS.close()
            # already cleared its own _obj/_context; these are the only other
            # strong refs.
            extra = getattr(self, "_extra", None)
            if extra:
                for key in ("ssl_object", "sslcontext", "peercert", "cipher"):
                    extra.pop(key, None)
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
            _detach_server()

    def is_closing(self):
        return self._closed

    # get_extra_info is inherited from asyncio.Transport (returns
    # self._extra.get(name, default), populated in __init__) so it stays
    # asyncio-compatible and patchable via asyncio.Transport.get_extra_info.

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    @property
    def _sslcontext(self):
        # White-box compat: code/tests read a transport's SSLContext via the
        # private _sslcontext (asyncio's _SSLProtocolTransport attribute).
        # aiohttp's test_tcp_connector_do_not_raise_connector_ssl_error asserts
        # `transport._sslcontext is client_ssl_ctx` to verify the connector
        # reuses the caller's context.  Surface the context the _TLSSock wrapped
        # the socket with (None for a plaintext transport, like stock asyncio).
        obj = getattr(self._sock, "ssl_object", None)
        return obj.context if obj is not None else None

    @property
    def _ssl_protocol(self):
        # White-box compat: asyncio's _SSLProtocolTransport exposes the
        # SSLProtocol as `_ssl_protocol`; code/tests read
        # transport._ssl_protocol._sslcontext (aiohttp's
        # test_tcp_connector_do_not_raise_connector_ssl_error).  Delegate to the
        # MemoryBIO TLS layer's view.  Plaintext transports have no
        # _ssl_protocol -- raise AttributeError, exactly like asyncio.
        sp = getattr(self._sock, "_ssl_protocol", None)
        if sp is None:
            raise AttributeError("_ssl_protocol")
        return sp

    # ---- flow control (read side) ----
    def pause_reading(self):
        self._paused = True

    def resume_reading(self):
        if not self._paused:
            return
        self._paused = False
        # The io goroutine may have exited (mask 0) or be parked WRITE-only;
        # kick it so READ re-enters its interest mask.
        self._kick_io()

    def is_reading(self):
        return not self._paused and not self._closed

    # ---- abort / half-close ----
    def abort(self):
        # Immediate teardown: discard any queued output so close() tears down
        # now instead of draining (asyncio's abort() drops the write buffer).
        self._write_buf = bytearray()
        self.close()

    def can_write_eof(self):
        return True

    def write_eof(self):
        if self._closed or self._eof_written or self._eof_pending:
            return
        if self._write_buf:
            # Defer the half-close until the buffer drains (asyncio flushes the
            # write buffer before shutting the write side); the io goroutine's
            # _drain_step does the SHUT_WR once the buffer empties.
            self._eof_pending = True
            return
        self._eof_written = True
        try:
            self._sock.shutdown(_socket.SHUT_WR)
        except OSError:
            pass

    # ---- write-buffer flow control ----  A single ordered buffer drained by
    # the io goroutine, with real high/low watermarks driving the protocol's
    # pause_writing/resume_writing (so a slow peer applies backpressure instead
    # of unbounded memory growth) and an accurate get_write_buffer_size.
    def set_write_buffer_limits(self, high=None, low=None):
        if high is None:
            high = 4 * low if low is not None else 64 * 1024
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError(
                "high (%r) must be >= low (%r) must be >= 0" % (high, low))
        self._high_water = high
        self._low_water = low
        self._maybe_pause_writing()
        self._maybe_resume_writing()

    def get_write_buffer_limits(self):
        return (self._low_water, self._high_water)

    def get_write_buffer_size(self):
        return len(self._write_buf)

    def _test__append_write_backlog(self, data):
        # asyncio's _SSLProtocolTransport exposes this test-only hook (see
        # sslproto.py) to QUEUE data without an immediate flush -- simulating a
        # filled write backlog so tests can exercise trailing-data delivery
        # after a remote shutdown.  With our single ordered write buffer it maps
        # cleanly: append (preserving order) and let the io goroutine drain it.
        if not data:
            return
        self._write_buf += bytes(data)
        self._maybe_pause_writing()
        self._kick_io()

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
                 ssl_handshake_timeout=None, cleanup_unix=True,
                 start_serving=True):
        # create_server may bind several sockets (one per address family);
        # accept independently on each.  Named _sockets to match asyncio.Server
        # (libraries / tests read srv._sockets); nulled in close().
        self._sockets = list(socks)
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
        # asyncio.Server.wait_closed(): block until the server is closed AND
        # every accepted connection has finished.  _waiters is a list while
        # pending; _wakeup() (called when both conditions hold) sets it to None.
        self._waiters = []
        # Unix server sockets bound to a filesystem path: remember (path, inode)
        # so close() unlinks the socket file like asyncio's _unix_server_sockets
        # -- only when the inode still matches (never unlink a file that replaced
        # ours).  Abstract-namespace (\0-prefixed) and unbound sockets are skipped.
        self._unix_paths = []
        if cleanup_unix:
            for s in self._sockets:
                try:
                    if s.family == _socket.AF_UNIX:
                        path = s.getsockname()
                        if isinstance(path, str) and path and not path.startswith("\0"):
                            self._unix_paths.append((path, _os.stat(path).st_ino))
                except OSError:
                    pass
        # asyncio create_server(start_serving=False): bind+listen now, but don't
        # accept until start_serving()/serve_forever().  is_serving() reflects it.
        self._serving = False
        self._accept_gs = []
        if start_serving:
            self._start_accepting()

    def _start_accepting(self):
        if self._serving or self._closed or self._sockets is None:
            return
        self._serving = True
        self._accept_gs = [pygo_core.go(lambda s=s: self._accept_loop(s))
                           for s in self._sockets]

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
                                                 context=self._context, server=self))

    def _setup_tls_conn(self, conn):
        try:
            tls = _MemoryBIOTLS(conn, self._ssl_context, server_side=True)
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
                                         context=self._context, server=self))

    def get_loop(self):
        """asyncio.Server.get_loop().  Libraries (websockets) call this on
        the server returned by create_server to schedule cleanup tasks."""
        return self._loop if self._loop is not None else asyncio.get_event_loop()

    def is_serving(self):
        return self._serving and not self._closed

    async def start_serving(self):
        # asyncio.Server.start_serving(): begin accepting (idempotent).  For a
        # server created with start_serving=False this spawns the accept loops.
        self._start_accepting()

    async def serve_forever(self):
        # asyncio.Server.serve_forever(): start accepting if not already, then
        # run until close() (or cancellation of this coroutine) ends it.  On a
        # closed server it raises, like asyncio.
        if self._closed:
            raise RuntimeError("server {0!r} is closed".format(self))
        self._start_accepting()
        while not self._closed:
            await asyncio.sleep(0.05)

    def close(self):
        if self._closed: return
        self._closed = True
        self._serving = False
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
        for sock in self._sockets:
            try: sock.shutdown(_socket.SHUT_RDWR)
            except OSError: pass
            _close_sock(sock)
        # asyncio.Server nulls _sockets on close (the public `sockets` property
        # then returns ()); tests assert `srv._sockets is None` afterward.
        self._sockets = None
        # Unlink unix server socket files (inode-checked), like asyncio's
        # _UnixSelectorEventLoop._stop_serving -- test_unix_server_addr_cleanup
        # asserts os.path.exists(addr) is False right after close().
        for path, ino in self._unix_paths:
            try:
                if _os.stat(path).st_ino == ino:
                    _os.unlink(path)
            except FileNotFoundError:
                pass
            except OSError:
                pass
        self._unix_paths = []
        # If no connections are live, the "closed AND drained" condition holds
        # now -- wake wait_closed() waiters.  Otherwise the last _detach() will.
        if not self._conns:
            self._wakeup()

    def _detach(self, transport):
        # Called by an accepted connection's transport when it finishes.  Once
        # the server is closed and the last connection drops, wake wait_closed().
        self._conns.discard(transport)
        if self._closed and not self._conns:
            self._wakeup()

    def _wakeup(self):
        waiters = self._waiters
        if waiters is None:
            return
        self._waiters = None
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

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
        # Block until the server is closed AND every connection has dropped, in
        # either order (asyncio.Server.wait_closed).  _waiters is None only once
        # _wakeup() has fired, i.e. both conditions already hold.
        if self._waiters is None:
            return
        loop = self._loop if self._loop is not None else asyncio.get_event_loop()
        waiter = loop.create_future()
        self._waiters.append(waiter)
        await waiter

    @property
    def sockets(self):
        if self._closed or self._sockets is None:
            return ()
        return tuple(self._sockets)

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
class _WriteSubprocessPipeProto(asyncio.BaseProtocol):
    """Bridge protocol for a child's stdin pipe.  connect_write_pipe drives the
    real _WritePipeTransport; this forwards its lifecycle to the owning
    _SubprocessTransport (mirrors CPython asyncio.base_subprocess.
    WriteSubprocessPipeProto)."""
    def __init__(self, proc, fd):
        self.proc = proc
        self.fd = fd
        self.pipe = None
        self.disconnected = False

    def connection_made(self, transport):
        self.pipe = transport

    def connection_lost(self, exc):
        self.disconnected = True
        self.proc._pipe_connection_lost(self.fd, exc)
        self.proc = None

    def pause_writing(self):
        self.proc._protocol.pause_writing()

    def resume_writing(self):
        self.proc._protocol.resume_writing()


class _ReadSubprocessPipeProto(_WriteSubprocessPipeProto, asyncio.Protocol):
    """Bridge protocol for a child's stdout/stderr pipe; adds data forwarding."""
    def data_received(self, data):
        self.proc._pipe_data_received(self.fd, data)


class _SubprocessTransport(asyncio.SubprocessTransport):
    """Subprocess transport that, like CPython's BaseSubprocessTransport, builds
    its per-fd pipe transports by AWAITING loop.connect_write_pipe /
    connect_read_pipe (with the bridge protocols above) rather than constructing
    them inline.  Routing through those loop methods is what makes start-time
    cancellation propagate and lets flow-control / pause_reading be observed on
    the returned pipe transport (test_subprocess's pipe-cancel + pause_reading
    tests mock exactly those loop methods)."""
    def __init__(self, loop, protocol, args, *, shell,
                 stdin, stdout, stderr, **kwargs):
        self._loop = loop
        self._protocol = protocol
        self._closed = False
        self._finished = False
        self._returncode = None
        self._exit_waiters = []
        self._pipes = {}            # fd -> bridge protocol (.pipe = transport)
        self._pipes_connected = False
        self._extra = {}
        # _pending_calls holds pipe_data_received / process_exited that land
        # before the protocol's connection_made has run; flushed by _connect_pipes.
        self._pending_calls = []
        # bufsize=0: unbuffered, so reads see data promptly and our stdin writer
        # controls flushing.  Spawn the child, then reap it on a wait thread.
        self._proc = _subprocess.Popen(
            args, shell=shell, stdin=stdin, stdout=stdout, stderr=stderr,
            bufsize=0, **kwargs)
        self._pid = self._proc.pid
        self._extra["subprocess"] = self._proc
        # Placeholder ONLY for fds that actually have a pipe: Popen leaves
        # proc.std* None for DEVNULL / an inherited fd / a passed file, and those
        # must NOT seed a _pipes entry -- _connect_pipes connects exactly the same
        # set (gated on proc.std* is not None), so a None placeholder here would
        # never be filled and _try_finish's all-disconnected gate (hence wait())
        # would hang (test_devnull_input).
        if self._proc.stdin is not None:
            self._pipes[0] = None
        if self._proc.stdout is not None:
            self._pipes[1] = None
        if self._proc.stderr is not None:
            self._pipes[2] = None
        self._start_reaper()

    def _start_reaper(self):
        # Reap the child COOPERATIVELY via its pidfd (Linux 5.3+ / py3.9+): a
        # goroutine parks on the pidfd -- which becomes readable exactly when the
        # child exits -- instead of burning a dedicated OS thread blocked in
        # Popen.wait().  This is the asyncio-bridge equivalent of pygo.monkey's
        # cooperative os.waitpid.  Falls back to the wait thread where pidfd_open
        # is missing or fails (older kernels, non-Linux).
        pidfd = None
        opener = getattr(_os, "pidfd_open", None)
        if opener is not None:
            try:
                pidfd = opener(self._pid)
            except OSError:
                pidfd = None
        if pidfd is None:
            _threading.Thread(target=self._wait_thread,
                              name="pygo-subproc-wait", daemon=True).start()
            return

        def reaper():
            try:
                _wait_fd(pidfd, _WAIT_READ)   # readable once the child exits
            except BaseException:
                pass
            finally:
                try:
                    _os.close(pidfd)
                except OSError:
                    pass
            # The child has exited: Popen.wait() reaps it immediately (no block),
            # and we are on the loop thread (this goroutine), so deliver inline.
            try:
                rc = self._proc.wait()
            except Exception:
                rc = self._proc.poll()
                if rc is None:
                    rc = -1
            self._process_exited(rc)

        pygo_core.go(reaper)

    async def _connect_pipes(self):
        # Mirror asyncio.base_subprocess._connect_pipes: await the loop's pipe
        # connectors so a cancellation there (or any failure) propagates to the
        # create_subprocess_* caller, then fire connection_made and flush the
        # callbacks that arrived while connecting.
        loop = self._loop
        proc = self._proc
        if proc.stdin is not None:
            _, proto = await loop.connect_write_pipe(
                lambda: _WriteSubprocessPipeProto(self, 0), proc.stdin)
            self._pipes[0] = proto
        if proc.stdout is not None:
            _, proto = await loop.connect_read_pipe(
                lambda: _ReadSubprocessPipeProto(self, 1), proc.stdout)
            self._pipes[1] = proto
        if proc.stderr is not None:
            _, proto = await loop.connect_read_pipe(
                lambda: _ReadSubprocessPipeProto(self, 2), proc.stderr)
            self._pipes[2] = proto
        # connection_made must run BEFORE create_subprocess_*'s Process.__init__
        # reads protocol.stdout/stderr (SubprocessStreamProtocol sets those in
        # connection_made).  Stock asyncio relies on a waiter + FIFO call_soon to
        # order it; pygo awaits _connect_pipes directly and it completes without
        # suspending (connect_*_pipe never awaits the loop), so a call_soon here
        # would run AFTER Process.__init__ -> stdout=None -> stdin never closed ->
        # deadlock.  Call it inline instead; pipe_data_received/process_exited
        # that arrived mid-connect are still deferred (call_soon) so they land
        # after connection_made.
        try:
            self._protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        for cb, data in self._pending_calls:
            loop.call_soon(cb, *data)
        self._pending_calls = None
        self._pipes_connected = True
        self._try_finish()

    def _call(self, cb, *data):
        # Before connection_made: queue; after: dispatch on the loop.
        if self._pending_calls is not None:
            self._pending_calls.append((cb, data))
        else:
            self._loop.call_soon(cb, *data)

    def _pipe_data_received(self, fd, data):
        self._call(self._protocol.pipe_data_received, fd, data)

    def _pipe_connection_lost(self, fd, exc):
        self._call(self._protocol.pipe_connection_lost, fd, exc)
        self._try_finish()

    def _wait_thread(self):
        rc = self._proc.wait()
        self._loop.call_soon_threadsafe(self._process_exited, rc)

    def _process_exited(self, rc):
        if self._returncode is not None:
            return
        self._returncode = rc
        self._call(self._protocol.process_exited)
        # The child is gone, so its stdin read end is closed.  Stock asyncio sees
        # that as POLLHUP and disconnects the write pipe; pygo's stdin transport
        # is a blocking-thread _WritePipeTransport that can't observe the hangup
        # on its own, so close it here -- otherwise it never disconnects and
        # _try_finish's all-pipes-disconnected gate (hence wait()) hangs for any
        # process whose stdin was left open (e.g. one just awaiting exit).
        # Output pipes are left alone: they EOF naturally and may still hold
        # buffered data to deliver.
        stdin = self._pipes.get(0)
        if stdin is not None and stdin.pipe is not None and not stdin.disconnected:
            try:
                stdin.pipe.close()
            except Exception:
                pass
        self._try_finish()

    def _try_finish(self):
        # connection_lost fires once the process has exited AND every connected
        # pipe has disconnected -- mirror asyncio.base_subprocess._try_finish.
        if self._returncode is None or self._finished:
            return
        if not self._pipes_connected:
            # _connect_pipes never completed (failed / cancelled): wake wait()ers
            # so they don't hang, but don't deliver connection_made/lost.
            for fut in self._exit_waiters:
                if not fut.done():
                    fut.set_result(self._returncode)
            self._exit_waiters = []
            return
        if all(p is not None and p.disconnected for p in self._pipes.values()):
            self._finished = True
            self._closed = True
            self._call(self._call_connection_lost, None)

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")
        finally:
            for fut in self._exit_waiters:
                if not fut.done():
                    fut.set_result(self._returncode)
            self._exit_waiters = []

    # ---- asyncio.SubprocessTransport interface ----
    def get_pid(self):
        return self._pid

    def get_returncode(self):
        return self._returncode

    def get_pipe_transport(self, fd):
        proto = self._pipes.get(fd)
        return proto.pipe if proto is not None else None

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
        # Best-effort: close every connected pipe transport, then kill a child
        # that is genuinely still running.  Only kill if poll() confirms it is
        # alive -- self._returncode is set ASYNCHRONOUSLY by the wait thread, so
        # a child that already exited may not have been notified yet, and close()
        # must not kill a finished process (test_close_dont_kill_finished).
        if self._closed:
            return
        self._closed = True
        for proto in self._pipes.values():
            if proto is not None and proto.pipe is not None:
                try:
                    proto.pipe.close()
                except Exception:
                    pass
        if self._returncode is None and self._proc.poll() is None:
            try:
                self._proc.kill()
            except (ProcessLookupError, OSError):
                pass

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
    """connect_read_pipe transport: a goroutine parks on the pipe fd via wait_fd
    (cooperative, no OS thread) and feeds protocol.data_received; EOF ->
    eof_received + connection_lost."""
    def __init__(self, loop, pipe, protocol):
        self._loop = loop
        self._pipe = pipe
        self._fd = pipe.fileno()
        self._protocol = protocol
        self._closing = False
        self._paused = False
        self._read_g = None
        try:
            _os.set_blocking(self._fd, False)
        except OSError:
            pass
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        self._read_g = pygo_core.go(self._read_loop)

    def _read_loop(self):
        # Cooperative replacement for the old reader thread: non-blocking os.read
        # + wait_fd(READ) on the raw pipe fd, on the loop thread, so data_received
        # / eof fire inline (no call_soon_threadsafe).  Exits on pause (respawned
        # by resume_reading), close, or EOF.
        fd = self._fd
        eof = False
        while True:
            if self._closing or self._paused:
                self._read_g = None
                return
            try:
                data = _os.read(fd, 32768)
            except (BlockingIOError, InterruptedError):
                try:
                    _wait_fd(fd, _WAIT_READ)
                except asyncio.CancelledError:
                    continue          # interest changed (pause/close): re-check
                except Exception:
                    eof = True
                    break
                continue
            except OSError:
                eof = True            # peer reset etc. -> treat as EOF
                break
            if not data:
                eof = True            # clean EOF
                break
            self._deliver(data)
            # Hand the scheduler to a woken consumer before reading again.
            pygo_core.sched_yield_classic()
        self._read_g = None
        if eof and not self._closing:
            self._eof()

    def _deliver(self, data):
        if not self._closing:
            try:
                self._protocol.data_received(data)
            except Exception as e:
                self._report(e, "data_received")

    def _eof(self):
        # A read pipe is unidirectional, so EOF is terminal -- there is nothing
        # left to read and no write side to keep half-open.  Like CPython's
        # _UnixReadPipeTransport._read_ready, call eof_received() for the
        # protocol's sake (it feeds EOF to a StreamReader) but IGNORE its return
        # and ALWAYS close: honouring a True return (StreamReaderProtocol over a
        # pipe returns True) left self._pipe open forever, so an abandoned pipe
        # transport leaked its FileIO -> a stray "unclosed file" ResourceWarning
        # at the next gc (test_streams::test_unclosed_resource_warnings counts
        # ResourceWarnings and saw 2 instead of 1).  Defer the close one turn so
        # a pending reader.read() drains the buffered data before connection_lost.
        try:
            self._protocol.eof_received()
        except Exception as e:
            self._report(e, "eof_received")
        self._loop.call_soon(self._close, None)

    def _close(self, exc):
        if self._closing:
            return
        self._closing = True
        g = self._read_g
        if g is not None:
            try:
                g.cancel_wait_fd()   # wake the parked read goroutine so it exits
            except Exception:
                pass
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
        g = self._read_g
        if g is not None:
            try:
                g.cancel_wait_fd()   # wake it so it observes _paused and exits
            except Exception:
                pass

    def resume_reading(self):
        if not self._paused:
            return
        self._paused = False
        if self._read_g is None and not self._closing:
            self._read_g = pygo_core.go(self._read_loop)

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
    """connect_write_pipe transport: a goroutine drains the write buffer to the
    pipe fd via wait_fd (cooperative, no OS thread); connection_lost fires on
    close/EOF/error.  Implements the asyncio watermark flow-control contract so
    StreamWriter.drain() blocks until the backlog flushes or the pipe breaks."""
    def __init__(self, loop, pipe, protocol):
        self._loop = loop
        self._pipe = pipe
        self._fd = pipe.fileno()
        self._protocol = protocol
        self._closing = False
        self._eof_requested = False
        self._conn_lost_fired = False
        self._buf = bytearray()
        self._high_water = 64 * 1024
        self._low_water = 16 * 1024
        self._protocol_paused = False
        self._drain_g = None
        try:
            _os.set_blocking(self._fd, False)
        except OSError:
            pass
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")

    def _kick(self):
        # Wake/spawn the drain goroutine after write()/write_eof changed the
        # buffer.  write() and the drain both run on the loop thread, so there is
        # no cross-thread queue -- just one bytearray + one goroutine.
        if self._drain_g is None:
            if not self._closing:
                self._drain_g = pygo_core.go(self._drain_loop)
        else:
            try:
                self._drain_g.cancel_wait_fd()   # wake it if parked on WRITE
            except Exception:
                pass

    def _drain_loop(self):
        fd = self._fd
        while True:
            if not self._buf:
                if self._eof_requested:
                    self._drain_g = None
                    self._finish(None)
                    return
                self._drain_g = None
                return                           # idle; respawn on next write()
            chunk = bytes(self._buf[:262144])
            try:
                n = _os.write(fd, chunk)
            except (BlockingIOError, InterruptedError):
                try:
                    _wait_fd(fd, _WAIT_WRITE)
                except asyncio.CancelledError:
                    continue                     # new write()/close: re-check
                except Exception as e:
                    self._drain_g = None
                    self._finish(e)
                    return
                continue
            except OSError as e:                 # BrokenPipe etc.
                self._drain_g = None
                self._finish(e)
                return
            if n:
                del self._buf[:n]
                self._maybe_resume()

    def _maybe_resume(self):
        if self._protocol_paused and len(self._buf) <= self._low_water:
            self._protocol_paused = False
            try:
                self._protocol.resume_writing()
            except Exception as e:
                self._report(e, "resume_writing")

    def _finish(self, exc):
        try:
            self._pipe.close()
        except Exception:
            pass
        if self._conn_lost_fired:
            return
        self._conn_lost_fired = True
        self._closing = True
        try:
            self._protocol.connection_lost(exc)
        except Exception as e:
            self._report(e, "connection_lost")

    def write(self, data):
        if self._closing or self._eof_requested:
            return
        data = bytes(data)
        if not data:
            return
        self._buf += data
        if (not self._protocol_paused) and len(self._buf) > self._high_water:
            self._protocol_paused = True
            try:
                self._protocol.pause_writing()
            except Exception as e:
                self._report(e, "pause_writing")
        self._kick()

    def writelines(self, list_of_data):
        self.write(b"".join(list_of_data))

    def get_write_buffer_size(self):
        return len(self._buf)

    def get_write_buffer_limits(self):
        return (self._low_water, self._high_water)

    def set_write_buffer_limits(self, high=None, low=None):
        if high is None:
            high = 64 * 1024 if low is None else 4 * low
        if low is None:
            low = high // 4
        self._high_water = high
        self._low_water = low

    def write_eof(self):
        if self._eof_requested:
            return
        self._eof_requested = True
        # Drain whatever is queued, then finish.  If nothing is queued and no
        # drain goroutine is running, finish inline now.
        if self._drain_g is None and not self._buf:
            self._finish(None)
        else:
            self._kick()

    def can_write_eof(self):
        return True

    def close(self):
        self.write_eof()

    def abort(self):
        self.write_eof()

    def is_closing(self):
        return self._closing or self._eof_requested

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
        self._set_called = False
        self._child_watcher = None

    def get_event_loop(self):
        # Mirror CPython's BaseDefaultEventLoopPolicy exactly: lazily create a
        # loop ONLY on the main thread and ONLY if set_event_loop() was never
        # called; once a loop has been explicitly set (even to None, as the
        # test suites do via test_utils.set_event_loop -> set_event_loop(None)
        # to force loops to be passed explicitly), or off the main thread, a
        # missing loop is an ERROR -- raise instead of silently fabricating one.
        # The old unconditional auto-create masked that contract and broke
        # test_streams::test_streamreader*_constructor_without_loop.
        if (self._loop is None
                and not self._set_called
                and _threading.current_thread() is _threading.main_thread()):
            stacklevel = 2
            try:
                f = sys._getframe(1)
            except AttributeError:
                pass
            else:
                while f:
                    module = f.f_globals.get("__name__")
                    if module == "asyncio" or (
                            module and module.startswith("asyncio.")):
                        f = f.f_back
                        stacklevel += 1
                    else:
                        break
            _warnings.warn(
                "There is no current event loop",
                DeprecationWarning, stacklevel=stacklevel)
            self.set_event_loop(self.new_event_loop())
        if self._loop is None:
            raise RuntimeError(
                "There is no current event loop in thread %r."
                % _threading.current_thread().name)
        return self._loop

    def set_event_loop(self, loop):
        self._set_called = True
        self._loop = loop

    def new_event_loop(self):
        return PygoEventLoop()

    # Child-watcher accessors (deprecated asyncio API still asked for on Unix).
    # pygo drives subprocesses with its own per-process _wait_thread, NOT an
    # asyncio child watcher, so any watcher set here is INERT -- pygo never
    # calls add_child_handler on it.  But we must still store and hand back the
    # exact object set, or callers that do the set/get/attach_loop(None)/close
    # lifecycle (e.g. CPython's test_subprocess watcher mixins) crash on a None.
    def get_child_watcher(self):
        _warnings._deprecated(
            "get_child_watcher",
            "{name!r} is deprecated as of Python 3.12 and will be "
            "removed in Python {remove}.", remove=(3, 14))
        return self._child_watcher

    def set_child_watcher(self, watcher):
        self._child_watcher = watcher
        _warnings._deprecated(
            "set_child_watcher",
            "{name!r} is deprecated as of Python 3.12 and will be "
            "removed in Python {remove}.", remove=(3, 14))


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
