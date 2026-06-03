"""PygoEventLoop: lifecycle, debug, time, and exception-handler/task-
factory plumbing."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .handles import _PG_OPEN_LOOPS  # noqa: F401

class _LoopCoreMixin(object):
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
        # The loop clock: monotonic normally, but the single-thread logical
        # clock when PYGO_LOGICAL_CLOCK is on -- so call_at/call_later deadlines
        # line up with sched_sleep's logical deadlines and the timer schedule
        # replays deterministically (e.g. under PYGO_PCT_SEED).  Identical to
        # _time.monotonic() when the logical clock is off.
        return pygo_core.loop_clock()

    # ---- task / future ----

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
