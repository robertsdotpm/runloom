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
import errno as _errno
import socket as _socket
import sys
import threading as _threading
import time as _time

import pygo_core
from . import runtime as _runtime


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


# WeakSet that asyncio.all_tasks() walks.  Registering keeps debug
# tooling happy and lets external code see our tasks.
try:
    _ALL_TASKS = asyncio.tasks._all_tasks
except AttributeError:
    _ALL_TASKS = None


# ====================================================================
# Handles -- minimal asyncio.Handle / asyncio.TimerHandle compat.
# ====================================================================
class _Handle(object):
    """Stand-in for asyncio.Handle.  The backing goroutine consults
    _cancelled before firing the callback."""
    __slots__ = ("_cancelled", "_callback", "_args")
    def __init__(self, cb, args):
        self._cancelled = False
        self._callback  = cb
        self._args      = args
    def cancel(self):
        self._cancelled = True
    def cancelled(self):
        return self._cancelled


class _TimerHandle(_Handle):
    """asyncio.TimerHandle compat shim."""


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


class PygoFuture(object):
    """Duck-typed Future with synchronous-callback dispatch.

    Used by PygoEventLoop.create_future and as the base of PygoTask.
    Intentionally no __slots__ -- asyncio.gather and friends set extra
    attributes (_log_destroy_pending, _cancel_message, ...) on futures
    they adopt, and we need to accept whatever they throw at us."""

    def __init__(self, *, loop=None):
        self._state    = _PENDING
        self._result   = None
        self._exception = None
        self._callbacks = []
        self._loop     = loop
        # Required by asyncio's await protocol.  Task.__step sets this
        # to False when it adopts the future; we re-arm to True in
        # __await__ each time we suspend.
        self._asyncio_future_blocking = False

    # ---- query ----
    def done(self):       return self._state != _PENDING
    def cancelled(self):  return self._state == _CANCELLED
    def get_loop(self):   return self._loop

    def result(self):
        if self._state == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._state == _CANCELLED:
            raise asyncio.CancelledError()
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self):
        if self._state == _PENDING:
            raise asyncio.InvalidStateError("Future not done")
        if self._state == _CANCELLED:
            raise asyncio.CancelledError()
        return self._exception

    # ---- mutation ----
    def set_result(self, result):
        if self._state != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        self._result = result
        self._state  = _FINISHED
        self._fire_callbacks()

    def set_exception(self, exception):
        if self._state != _PENDING:
            raise asyncio.InvalidStateError("Future already done")
        if isinstance(exception, type):
            exception = exception()
        if isinstance(exception, StopIteration):
            raise TypeError(
                "StopIteration interacts badly with generators "
                "and cannot be raised into a Future")
        self._exception = exception
        self._state     = _FINISHED
        self._fire_callbacks()

    def cancel(self, msg=None):
        if self._state != _PENDING:
            return False
        self._cancel_message = msg
        self._state = _CANCELLED
        self._fire_callbacks()
        return True

    def _make_cancelled_error(self):
        """Build the CancelledError that asyncio internals (gather's
        _done_callback, Task.__step's cancellation path) expect."""
        msg = getattr(self, "_cancel_message", None)
        if msg is None:
            return asyncio.CancelledError()
        return asyncio.CancelledError(msg)

    # ---- callbacks ----
    def add_done_callback(self, callback, *, context=None):
        if self._state != _PENDING:
            # Already resolved -- fire this one callback immediately,
            # consistent with asyncio.Future's semantics for late
            # add_done_callback.
            try:
                callback(self)
            except BaseException as e:
                self._report_exc(e)
        else:
            self._callbacks.append((callback, context))

    def remove_done_callback(self, callback):
        filtered = [(cb, ctx) for cb, ctx in self._callbacks if cb is not callback]
        removed  = len(self._callbacks) - len(filtered)
        self._callbacks = filtered
        return removed

    def _fire_callbacks(self):
        cbs, self._callbacks = self._callbacks, []
        for cb, ctx in cbs:
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
        if self._state == _PENDING:
            self._asyncio_future_blocking = True
            yield self
            assert self._state != _PENDING
        return self.result()

    # Generators implement __iter__; tasks expect that to exist for await.
    __iter__ = __await__


# ====================================================================
# PygoTask -- the heart of the bridge.
# ====================================================================
class PygoTask(PygoFuture):
    """asyncio.Task replacement.  Each task owns a goroutine that drives
    the coroutine; the Future side exposes the asyncio-visible state
    so external code can `await task` etc.

    Subclasses PygoFuture, not asyncio.Future -- the C Future class
    forbids real method overrides and we need set_result/set_exception/
    cancel to fire callbacks synchronously (otherwise our gather-in-
    flight done-callback cascade goes through N call_soon -> N
    goroutine spawns and the bridge ends up slower than asyncio).
    """

    def __init__(self, coro, *, loop=None, name=None):
        if loop is None:
            loop = asyncio.get_event_loop()
        super().__init__(loop=loop)
        self._coro    = coro
        self._name    = name or "pygo-task"
        # _self_g is captured by the driver on its first iteration.
        # The driver-internal G handle is what done-callbacks wake.
        # Cheaper than the previous Chan(1) approach -- saves a Chan
        # alloc + try_send/recv per task at fan-out time.
        self._self_g = None
        self._cancel_requested = False
        # cancelling()/uncancel() are required by asyncio.timeouts in
        # 3.11+.  Mirrors asyncio.Task: count of unresolved cancels.
        self._num_cancels_requested = 0
        # Register in asyncio.all_tasks() for introspection parity.
        if _ALL_TASKS is not None:
            try:
                _ALL_TASKS.add(self)
            except TypeError:
                pass
        # Off we go.  The goroutine owns the coro from here.
        self._g = pygo_core.go(self._driver)

    # ---- asyncio.Task surface ----
    def get_name(self):
        return self._name

    def set_name(self, name):
        self._name = name

    def get_coro(self):
        return self._coro

    def cancel(self, msg=None):
        if self.done():
            return False
        self._cancel_requested = True
        self._num_cancels_requested += 1
        # Unblock the driver so it sees _cancel_requested on next iter.
        if self._self_g is not None:
            self._self_g.wake()
        return True

    def cancelling(self):
        """Number of unresolved cancel() calls.  Required by
        asyncio.timeouts / asyncio.TaskGroup in 3.11+."""
        return self._num_cancels_requested

    def uncancel(self):
        """Decrement the cancelling counter.  When it returns to zero,
        clear the cancel-requested flag so the driver stops trying to
        raise CancelledError."""
        if self._num_cancels_requested > 0:
            self._num_cancels_requested -= 1
        if self._num_cancels_requested == 0:
            self._cancel_requested = False
        return self._num_cancels_requested

    # ---- driver: the per-task goroutine body ----
    def _driver(self):
        # Capture our own G handle so cancel/done_callback can wake us.
        self._self_g = pygo_core.current_g()

        coro       = self._coro
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
                    if self._cancel_requested and throw_exc is None:
                        throw_exc = asyncio.CancelledError()
                    if throw_exc is not None:
                        e, throw_exc = throw_exc, None
                        yielded = coro.throw(e)
                    else:
                        yielded = coro.send(send_value)
                except StopIteration as si:
                    if not self.done():
                        self.set_result(si.value)
                    return
                except asyncio.CancelledError:
                    if not self.done():
                        super().cancel()
                    return
                except BaseException as e:
                    if not self.done():
                        self.set_exception(e)
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
                # cooperative checkpoint).  Round-trip through the
                # scheduler so other tasks can run.
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
                        throw_exc = asyncio.CancelledError()
                    elif yielded.exception() is not None:
                        throw_exc = yielded.exception()
                    else:
                        send_value = yielded.result()
                except asyncio.CancelledError:
                    throw_exc = asyncio.CancelledError()
                continue

            # Slow path: park the goroutine until the future fires.
            # Register the wake callback FIRST then call park_self --
            # the race where the future fires synchronously inside
            # add_done_callback is handled by park_safe / wake_safe
            # (wake_pending counter; park is a no-op if wake arrived).
            yielded.add_done_callback(self._wake_unpark)
            pygo_core.park_self()

            # We're back.  Either the future is done, or we were
            # cancelled; figure out which.
            if self._cancel_requested:
                try:
                    yielded.remove_done_callback(self._wake_unpark)
                except Exception:
                    pass
                throw_exc = asyncio.CancelledError()
                continue

            try:
                if yielded.cancelled():
                    throw_exc = asyncio.CancelledError()
                elif yielded.exception() is not None:
                    throw_exc = yielded.exception()
                else:
                    send_value = yielded.result()
            except asyncio.CancelledError:
                throw_exc = asyncio.CancelledError()

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
        # Real asyncio loops (BaseEventLoop) expose these; stdlib
        # Future/Task/Timeout machinery and many libraries read them
        # directly (e.g. loop._thread_id, loop._debug).  AbstractEventLoop
        # does not provide them, so add them for compat.  We deliberately
        # do NOT enforce thread affinity (pygo is M:N: callbacks may run
        # on any hub thread), so _thread_id exists purely so attribute
        # reads + asyncio's early-return thread checks succeed.
        self._thread_id = None
        self._debug = False
        try:
            self._clock_resolution = _time.get_clock_info("monotonic").resolution
        except Exception:
            self._clock_resolution = 1e-6

    # ---- state ----
    def is_running(self):  return self._running
    def is_closed(self):   return self._closed
    def get_debug(self):   return self._debug
    def set_debug(self, enabled):  self._debug = bool(enabled)
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
        return PygoTask(coro, loop=self, name=name)

    def create_future(self):
        return PygoFuture(loop=self)

    # ---- callback scheduling ----
    def call_soon(self, callback, *args, context=None):
        handle = _Handle(callback, args)
        def runner():
            if not handle._cancelled:
                try:
                    callback(*args)
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
        # Thread-safe: may be called from ANY OS thread.  Enqueue under the
        # lock; the keepalive goroutine on the loop thread drains and runs it.
        # We do NOT pygo_core.go() here -- from a foreign thread that would
        # spawn onto that thread's own (never-drained) scheduler.
        handle = _Handle(callback, args)
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
                handle._callback(*handle._args)
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
            while not stop[0] and not self._closed:
                self._drain_ts_queue()
                pygo_core.sched_sleep(0.002)
            self._drain_ts_queue()
        pygo_core.go(_keepalive)

    def call_later(self, delay, callback, *args, context=None):
        handle = _TimerHandle(callback, args)
        loop_self = self
        def runner():
            pygo_core.sched_sleep(delay)
            if not handle._cancelled:
                try:
                    callback(*args)
                except BaseException as e:
                    # Keep this minimal -- printing a traceback from here
                    # can itself recurse if we're near the c_recursion limit.
                    sys.stderr.write("[pygo.aio] call_later cb: %r\n" % (e,))
        pygo_core.go(runner)
        return handle

    def call_at(self, when, callback, *args, context=None):
        delay = max(0.0, when - self.time())
        return self.call_later(delay, callback, *args, context=context)

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
        handle = _Handle(callback, args)
        table[fd] = handle
        def runner():
            while not handle._cancelled:
                try:
                    pygo_core.wait_fd(fd, evt)
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

    async def create_connection(self, protocol_factory, host=None, port=None, *,
                                ssl=None, family=0, proto=0, flags=0, sock=None,
                                local_addr=None, server_hostname=None, **_ignored):
        """Lower-level create_connection.  Returns (transport, protocol).
        Builds a TCP socket + thin Transport over our Stream classes;
        protocol's connection_made / data_received / connection_lost
        get fired."""
        if ssl is not None:
            raise NotImplementedError("ssl not supported in pygo.aio loop.create_connection")
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
                        pygo_core.wait_fd(s.fileno(), 2)
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
        protocol = protocol_factory()
        transport = _StreamTransport(sock, protocol, loop=self)
        return transport, protocol

    async def create_server(self, protocol_factory, host=None, port=None, *,
                            family=_socket.AF_UNSPEC, flags=_socket.AI_PASSIVE,
                            sock=None, backlog=100, ssl=None,
                            reuse_address=None, reuse_port=None, **_ignored):
        if ssl is not None:
            raise NotImplementedError("ssl not supported in pygo.aio loop.create_server")
        if sock is None:
            infos = _resolve(host, port, family,
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
        # cb=None: caller wired up via protocol factory + Transport.
        # We still need an accept loop that builds Transports per conn.
        return _ProtocolServer(sock, protocol_factory, loop=self)

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
            pygo_core.wait_fd(sock.fileno(), 2)
            err = sock.getsockopt(_socket.SOL_SOCKET, _socket.SO_ERROR)
            if err != 0:
                raise OSError(err, "connect failed")

    async def sock_accept(self, sock):
        sock.setblocking(False)
        while True:
            try:
                return sock.accept()
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(sock.fileno(), 1)

    async def sock_recv(self, sock, nbytes):
        sock.setblocking(False)
        while True:
            try:
                return sock.recv(nbytes)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(sock.fileno(), 1)

    async def sock_recv_into(self, sock, buf):
        sock.setblocking(False)
        while True:
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(sock.fileno(), 1)

    async def sock_recvfrom(self, sock, bufsize):
        sock.setblocking(False)
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(sock.fileno(), 1)

    async def sock_sendall(self, sock, data):
        sock.setblocking(False)
        view = memoryview(data)
        sent = 0
        while sent < len(view):
            try:
                n = sock.send(view[sent:])
                sent += n
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(sock.fileno(), 2)

    async def sock_sendto(self, sock, data, address):
        sock.setblocking(False)
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                pygo_core.wait_fd(sock.fileno(), 2)

    # ---- executor (thread pool) ----
    def run_in_executor(self, executor, func, *args):
        """Run func(*args) on a thread pool.  Returns a PygoFuture
        that resolves when the thread completes.  We hand out a real
        threadpool via concurrent.futures."""
        import concurrent.futures as _cf
        if executor is None:
            # Lazy-init default pool.
            if not hasattr(self, "_default_executor"):
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
            self.call_soon_threadsafe(_set)
        cf_fut.add_done_callback(_on_thread_done)
        return fut

    # ---- run loop ----
    def run_until_complete(self, future):
        if asyncio.iscoroutine(future):
            future = self.create_task(future)
        elif not (isinstance(future, asyncio.Future)
                  or isinstance(future, PygoFuture)
                  or asyncio.isfuture(future)):
            raise TypeError("argument must be a Future or coroutine")

        # When the user-visible future completes, kick the scheduler
        # out of its drain loop so we don't block on background tasks
        # (accept loops, ticker goroutines, etc.) the user didn't
        # explicitly join.  Matches asyncio.run's semantics.
        def _stop_on_done(_fut):
            box = self._ka_stop_box
            if box is not None:
                box[0] = True
            pygo_core.sched_stop()
        future.add_done_callback(_stop_on_done)
        # Keepalive: drains call_soon_threadsafe + keeps the scheduler from
        # returning idle while a goroutine is parked awaiting an external wake.
        self._spawn_keepalive()

        # Resolve deep, non-yielding stdlib imports (e.g. getaddrinfo's
        # first-call codec import) on the main thread before any driver
        # goroutine runs them on a small stack -- see prewarm_stdlib.
        _runtime.prewarm_stdlib()
        self._running = True
        self._thread_id = _threading.get_ident()
        asyncio._set_running_loop(self)
        try:
            pygo_core.run()
        finally:
            self._running = False
            self._thread_id = None
            asyncio._set_running_loop(None)
            # IMPORTANT: do NOT cancel outstanding tasks / sched_reset here.
            # run_until_complete must leave other tasks + parked goroutines
            # ALIVE -- IsolatedAsyncioTestCase (and asyncio.Runner generally)
            # call run_until_complete once each for asyncSetUp / the test /
            # asyncTearDown on the SAME loop, and rely on connections (their
            # recv goroutines) created in setUp surviving into the test body.
            # The asyncio.run-style teardown now lives in close() instead,
            # which asyncio.run / Runner.close invoke exactly once at the end.

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
        if _ALL_TASKS is not None:
            tasks = [t for t in list(_ALL_TASKS)
                     if not t.done() and t._loop is self]
            for t in tasks:
                try:
                    t.cancel()
                except Exception:
                    pass
        # Forcibly drop anything still scheduled.  Goroutines parked on
        # netpoll/wake/chan that aren't interrupted by cancel get
        # abandoned; the underlying coro and snap are freed when the
        # last Python reference drops.
        try:
            pygo_core.sched_reset()
        except AttributeError:
            # Older build without sched_reset; best-effort drain.
            pass

    def run_forever(self):
        # Resolve deep, non-yielding stdlib imports (e.g. getaddrinfo's
        # first-call codec import) on the main thread before any driver
        # goroutine runs them on a small stack -- see prewarm_stdlib.
        _runtime.prewarm_stdlib()
        self._running = True
        self._thread_id = _threading.get_ident()
        asyncio._set_running_loop(self)
        try:
            pygo_core.run()
        finally:
            self._running = False
            self._thread_id = None
            asyncio._set_running_loop(None)

    def stop(self):
        # Schedule a sentinel task that just exits, in case run_forever
        # is waiting.  In practice users should call cancel() on tasks.
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
        msg = context.get("message", "unhandled exception")
        exc = context.get("exception")
        sys.stderr.write("[pygo.aio] %s: %r\n" % (msg, exc))
        if exc is not None:
            import traceback
            traceback.print_exception(type(exc), exc, exc.__traceback__,
                                      file=sys.stderr)

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
                pygo_core.wait_fd(self._sock.fileno(), 1)
                continue
            except OSError as e:
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK, _errno.EINTR):
                    pygo_core.wait_fd(self._sock.fileno(), 1)
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
            pygo_core.wait_fd(self._sock.fileno(), 2)

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
        return default

    @property
    def transport(self):
        # asyncio code commonly does writer.transport.get_extra_info(...);
        # forward to ourselves for compat.
        return self


async def open_connection(host=None, port=None, *, family=0, proto=0,
                          flags=0, sock=None, local_addr=None,
                          server_hostname=None, ssl=None,
                          limit=2**16, **_ignored):
    """Establish a TCP connection and return (reader, writer).

    Mirrors asyncio.open_connection but bypasses Transport/Protocol --
    our Stream classes talk to the socket directly via cooperative
    wait_fd.  SSL not supported in this MVP.
    """
    if ssl is not None:
        raise NotImplementedError("ssl= in pygo.aio.open_connection is not yet supported")

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
                    pygo_core.wait_fd(s.fileno(), 2)
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

    reader = StreamReader(sock, limit=limit)
    writer = StreamWriter(sock, reader=reader)
    return reader, writer


class _Server(object):
    """asyncio.Server compatible: keeps the listening socket alive and
    the accept-loop goroutine running until close() is called."""

    def __init__(self, sock, client_connected_cb, *, limit=2**16):
        self._sock = sock
        self._cb   = client_connected_cb
        self._limit = limit
        self._closed = False
        self._accept_g = pygo_core.go(self._accept_loop)

    def _accept_loop(self):
        while not self._closed:
            try:
                conn, _addr = self._sock.accept()
            except (BlockingIOError, InterruptedError):
                if self._closed:
                    return
                pygo_core.wait_fd(self._sock.fileno(), 1)
                continue
            except OSError as e:
                # close() will close the listening socket; the next
                # accept fails with EBADF / EINVAL.  Treat that as the
                # signal to exit cleanly.
                if self._closed:
                    return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    pygo_core.wait_fd(self._sock.fileno(), 1)
                    continue
                # Real error -- record and exit.
                self._closed = True
                return
            conn.setblocking(False)
            reader = StreamReader(conn, limit=self._limit)
            writer = StreamWriter(conn, reader=reader)

            # Build the connection coroutine and drive it directly as
            # a PygoTask.  We're already inside a non-task goroutine
            # (the accept loop); creating PygoTask directly here -- the
            # earlier "wrap in pygo_core.go then PygoTask inside" added
            # a second goroutine spawn for no real benefit.
            coro = self._cb(reader, writer)
            if asyncio.iscoroutine(coro):
                PygoTask(coro, loop=asyncio.get_event_loop())

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
class _StreamTransport(object):
    """Thin TCP transport over a socket.  Drives the protocol's
    data_received via a recv goroutine; transports its write() through
    cooperative sendall."""

    def __init__(self, sock, protocol, *, loop=None):
        self._sock = sock
        self._protocol = protocol
        self._loop = loop
        self._closed = False
        self._stopping = False
        self._paused = False        # pause_reading() flow control
        try:
            protocol.connection_made(self)
        except Exception as e:
            self._report(e, "connection_made")
        self._recv_g = pygo_core.go(self._recv_loop)

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
                    pygo_core.wait_fd(sock.fileno(), 1)
                except Exception:
                    return
                continue
            except OSError as e:
                if self._stopping: return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    pygo_core.wait_fd(sock.fileno(), 1)
                    continue
                try:
                    self._protocol.connection_lost(e)
                except Exception as e2:
                    self._report(e2, "connection_lost")
                return
            if not data:
                # EOF
                try:
                    keep = self._protocol.eof_received()
                except Exception as e:
                    self._report(e, "eof_received")
                    keep = False
                if not keep:
                    self.close()
                    return
                continue
            try:
                self._protocol.data_received(data)
            except Exception as e:
                self._report(e, "data_received")

    def write(self, data):
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
                            try: pygo_core.wait_fd(self._sock.fileno(), 2)
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
                        try: pygo_core.wait_fd(self._sock.fileno(), 2)
                        except Exception: return
                    except OSError:
                        return
            pygo_core.go(_flush)
        except OSError as e:
            try:
                self._protocol.connection_lost(e)
            except Exception:
                pass
            self.close()

    def writelines(self, lines):
        for line in lines:
            self.write(line)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stopping = True
        try:
            self._sock.shutdown(_socket.SHUT_RDWR)
        except OSError:
            pass
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
            try: return self._sock.getsockname()
            except OSError: return default
        if name == "peername":
            try: return self._sock.getpeername()
            except OSError: return default
        return default

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
        if self._closed:
            return
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

    def __init__(self, sock, protocol_factory, *, loop=None):
        self._sock = sock
        self._factory = protocol_factory
        self._loop = loop
        # asyncio.Server exposes _ssl_context (None when no TLS); libraries
        # (e.g. websockets' test helpers) read it off the server object.
        # pygo.aio's create_server rejects ssl=, so it is always None here.
        self._ssl_context = None
        self._closed = False
        self._accept_g = pygo_core.go(self._accept_loop)

    def _accept_loop(self):
        while not self._closed:
            try:
                conn, _addr = self._sock.accept()
            except (BlockingIOError, InterruptedError):
                if self._closed: return
                pygo_core.wait_fd(self._sock.fileno(), 1)
                continue
            except OSError:
                if self._closed: return
                self._closed = True
                return
            conn.setblocking(False)
            protocol = self._factory()
            _StreamTransport(conn, protocol, loop=self._loop)

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
        try: self._sock.shutdown(_socket.SHUT_RDWR)
        except OSError: pass
        _close_sock(self._sock)

    def close_clients(self):
        # asyncio 3.13+ API; we don't track client transports here yet.
        pass

    def abort_clients(self):
        pass

    async def wait_closed(self):
        await asyncio.sleep(0)

    @property
    def sockets(self):
        return (self._sock,) if not self._closed else ()

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
                    pygo_core.wait_fd(sock.fileno(), 1)
                except Exception:
                    return
                continue
            except OSError as e:
                if self._stopping: return
                if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                    pygo_core.wait_fd(sock.fileno(), 1)
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
                pygo_core.wait_fd(sock.fileno(), 2)
    else:
        sock.setblocking(False)

    protocol = protocol_factory()
    transport = DatagramTransport(sock, protocol, loop=loop)
    return transport, protocol


async def start_server(client_connected_cb, host=None, port=None, *,
                       family=_socket.AF_UNSPEC, flags=_socket.AI_PASSIVE,
                       sock=None, backlog=100, limit=2**16,
                       reuse_address=None, reuse_port=None,
                       ssl=None, **_ignored):
    """Listen on host:port and call client_connected_cb(reader, writer)
    per accepted connection.  Returns a _Server with .close() / .sockets.

    Compared to asyncio.start_server, we skip Transport/Protocol and
    the SSL-handshake plumbing.  Suitable for plain TCP services."""
    if ssl is not None:
        raise NotImplementedError("ssl= in pygo.aio.start_server is not yet supported")

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

    return _Server(sock, client_connected_cb, limit=limit)


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
