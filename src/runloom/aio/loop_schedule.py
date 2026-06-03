"""RunloomEventLoop: create_task/future, call_soon(_threadsafe), call_later/
call_at, the keepalive timer."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .futures import RunloomFuture  # noqa: F401
from .handles import _Handle, _TimerHandle  # noqa: F401
from .tasks import RunloomTask  # noqa: F401

class _LoopScheduleMixin(object):
    def create_task(self, coro, *, name=None, context=None, **kwargs):
        self._check_closed()
        if self._can_spawn_here():
            return self._pg_make_task(coro, name, context, kwargs)
        # Foreign thread: RunloomTask.__init__ spawns a goroutine, which would land
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
        # factory => our own RunloomTask (the default, goroutine-driven path).
        factory = self._task_factory
        if factory is not None:
            if context is not None:
                kwargs = dict(kwargs)
                kwargs["context"] = context
            task = factory(self, coro, **kwargs)
            task.set_name(name)
            return task
        # Default path: RunloomTask ignores any stray kwargs (eager_start etc. --
        # the eager-task factory installs its own factory above).
        return RunloomTask(coro, loop=self, name=name, context=context)

    def create_future(self):
        return RunloomFuture(loop=self)

    def _pg_run_loop_cb(self, ctx, callback, args):
        # Run a loop-level callback (call_soon / call_at / call_soon_threadsafe)
        # with NO current task active, exactly like stock asyncio's _run_once:
        # current_task() is None there, and a deferred Task.__step does its own
        # enter_task/leave_task.  This matters because a RunloomTask that parks
        # MID-send (a raw runloom park inside coro.send -- a blocking offload, etc.,
        # i.e. not the driver's clean future-park which restores the slot) leaves
        # _CURRENT_TASKS[loop] pointing at itself.  If a deferred STOCK-asyncio
        # Task wakeup then runs here, its enter_task sees that stale task and
        # raises "Cannot enter into task X while another task Y is being executed"
        # -- runloom swallows it as a callback error and the wakeup is DROPPED, so
        # the woken task hangs forever (aiohttp's connector _wait_for_close: the
        # ClientSession/AppRunner teardown deadlocks).  Clear the slot for the
        # callback and restore it after (symmetry with the driver's finally).
        prev = _CURRENT_TASKS.get(self)
        if prev is not None:
            _CURRENT_TASKS.pop(self, None)
        try:
            ctx.run(callback, *args)
        finally:
            if prev is not None:
                _CURRENT_TASKS[self] = prev

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
                    # the context active when call_soon was invoked.  At loop
                    # level: no current task (see _pg_run_loop_cb).
                    self._pg_run_loop_cb(handle._context, callback, args)
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
        # behaviour is undefined; switch back to runloom_c.go.
        # Roomier stack: call_soon delivers protocol callbacks (data_received,
        # pipe_data_received, ...) that can run deep C-recursive code (crypto),
        # which overflows the default 32 KB g-stack and SEGVs -- see _IO_STACK.
        _go_io(runner)
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
        # We do NOT runloom_c.go() here -- from a foreign thread that would
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
                self._pg_run_loop_cb(handle._context, handle._callback,
                                     handle._args)
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
            # runloom_sched_drain stays in its loop (a bare-parked goroutine alone
            # would let it return idle) and re-checks the cross-thread wake list
            # each wake.  2ms bounds foreign-wake latency; cheap for a test run.
            # `stop` is this run's private box -- a later run can't revive us.
            try:
                while not stop[0] and not self._closed and not self._stopping:
                    self._drain_ts_queue()
                    # REAL-time heartbeat: poll every 2 ms of WALL time.  Must NOT
                    # ride the logical clock (RUNLOOM_LOGICAL_CLOCK) -- a logical 2 ms
                    # advances instantly and busy-loops, starving real progress.
                    runloom_c.sched_sleep_real(0.002)
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
                # run_until_complete's) runloom_c.run().  sched_stop() acts on
                # THIS thread's scheduler, and the keepalive always runs on the
                # loop thread, so this is the one safe place to call it -- even
                # when stop() was invoked from a FOREIGN thread and merely
                # drained onto us via call_soon_threadsafe (exactly how
                # aiosmtpd's threaded Controller.stop() reaches the loop).
                try:
                    runloom_c.sched_stop()
                except Exception:
                    pass
        # Roomier stack: the keepalive runs call_soon_threadsafe callbacks
        # (_drain_ts_queue), which may be deep protocol/crypto callbacks.
        _go_io(_keepalive)

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
            runloom_c.sched_sleep(max(0.0, when - self.time()))
            if not handle._cancelled:
                try:
                    # Read the callback/args THROUGH the handle, never via closure
                    # capture: asyncio.Handle.cancel() nulls handle._callback /
                    # handle._args, so a cancelled timer's still-sleeping goroutine
                    # then holds NO reference to the callback (or anything it closes
                    # over).  Capturing `callback`/`args` directly here kept them --
                    # and e.g. an aiocoap retransmit's whole message/transport --
                    # alive until the original deadline, leaking past cancel and
                    # failing strict gc-leak teardown checks.  At loop level: no
                    # current task (see _pg_run_loop_cb).
                    self._pg_run_loop_cb(handle._context, handle._callback,
                                         handle._args)
                except (KeyboardInterrupt, SystemExit) as e:
                    # asyncio re-raises these out of the loop; break the drive.
                    self._pg_signal_fatal(e)
                except BaseException as e:
                    # Keep this minimal -- printing a traceback from here
                    # can itself recurse if we're near the c_recursion limit.
                    sys.stderr.write("[runloom.aio] timer cb: %r\n" % (e,))
        if self._can_spawn_here():
            _go_io(runner)
        else:
            # Foreign thread: spawn the timer goroutine on the loop's own thread.
            self.call_soon_threadsafe(lambda: _go_io(runner))
        return handle

    # ---- I/O readers / writers (level-triggered, matches selector loops) ----
    # Stock asyncio keeps ONE selector key per fd carrying a COMBINED event mask
    # (READ|WRITE) and services both directions from a single readiness check.
    # runloom MUST mirror that with ONE goroutine per fd: a separate goroutine per
    # direction would park the SAME fd in netpoll twice, and the arm is one-shot
    # per fd -- so the second registration's direction silently overwrites the
    # first's.  A reader AND a writer on one fd (e.g. tornado IOStream: an
    # add_writer to detect connect completion + an add_reader for the response,
    # both live at once) then lose one direction's wakeups: the connect-write
    # event never fires, the queued request never flushes, the peer hangs.  So a
    # single per-fd goroutine parks on the UNION mask and dispatches by the
    # ready mask wait_fd returns; interest changes wake it to re-evaluate.
