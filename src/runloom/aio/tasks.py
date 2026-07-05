"""RunloomTask: the asyncio.Task <-> fiber bridge (heart of the bridge)."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .futures import _RunloomFutureMixin, _fut_cancelled_error  # noqa: F401
from .handles import (_PG_ALL_TASKS, _REGISTER_TASK, _UNREGISTER_TASK,  # noqa: F401
                      _TASK_NAME_COUNTER)

class RunloomTask(_RunloomFutureMixin, asyncio.Task):
    """A real asyncio.Task subclass (isinstance(x, asyncio.Task) holds) driven
    by a runloom fiber instead of the C task machinery.

    We initialise only the Future half of the C object (asyncio.Future.__init__)
    -- NOT Task.__init__, which would schedule the C task-step and double-drive
    our coroutine (the C step is a C callable we can't shadow from Python).  The
    C Task's own fields (_coro, _fut_waiter, ...) stay NULL; we keep our state in
    _pg* attrs and override the readers.  On completion we settle the underlying
    C Future state so asyncio.Task.__del__ doesn't warn "destroyed but pending".
    """

    def __init__(self, coro, *, loop=None, name=None, context=None):
        # Match asyncio.Task.__init__: reject a non-coroutine SYNCHRONOUSLY.
        # loop.create_task(123) / create_task(some_plain_fn) must raise TypeError
        # right here -- not accept the bad arg and later blow up in the driver
        # with "'NoneType'/'int' object has no attribute 'send'" (logged as an
        # unretrieved task exception).  Defensive callers rely on this (falcon's
        # resp.schedule type-check, test_scheduled_jobs_type_error).
        if not asyncio.iscoroutine(coro):
            raise TypeError("a coroutine was expected, got {0!r}".format(coro))
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
        # Also track in a runloom-global set so loop.close() can tell whether
        # ANOTHER loop on this OS thread still has live tasks (see
        # _cancel_outstanding_tasks): the runloom scheduler is shared per-thread,
        # so a close()-time sched_reset must not bulldoze a sibling loop's work.
        _PG_ALL_TASKS.add(self)
        # Run the driver under a "<module>" root frame so libraries that derive
        # their module by walking frame.f_back (aiohttp web.AppKey) reach one,
        # matching asyncio.  Capture the creator's module name HERE (we're on
        # its live stack) before the fiber swaps stacks.  See
        # _pg_run_with_module_root.  Clearing g->callable at completion still
        # breaks the task<->g cycle: the closure's only strong ref to self is
        # the bound self._driver it carries, dropped when the closure is.
        if _PG_MODULE_ROOT_ON:
            _modname = _pg_capture_module_name()
            _driver = self._driver
            _body = lambda: _pg_run_with_module_root(_driver, _modname)
        else:
            _body = self._driver
        # Driver fibers run arbitrary user async code (deep C-recursive
        # first-time imports overflow the default 32 KB g-stack and SEGV), so
        # give them a roomier stack.  Override with RUNLOOM_AIO_TASK_STACK.
        # fifo=True: task STEPS are scheduled call_soon-FIFO in asyncio, so the
        # PCT controlled scheduler must keep this driver in order with the loop's
        # other call_soon fibers (else a sleep(0) resume can race ahead of a
        # call_soon callback -- a false positive, not a bug).  See pct_fifo.
        try:
            self._g = runloom_c.fiber(_body, stack_size=_TASK_STACK, fifo=True) \
                if _TASK_STACK else runloom_c.fiber(_body, fifo=True)
        except BaseException:
            # Driver spawn failed (genuine ENOMEM, or the RUNLOOM_FAULT_SPAWN_*
            # injection).  We are already registered in asyncio.all_tasks()
            # and _PG_ALL_TASKS (above) but have NO fiber, so nothing can ever
            # run or settle this task: leaving it registered wedges
            # loop.close() -- _cancel_outstanding_tasks cancel()s it (a no-op
            # with no fiber to wake), then drives
            # run_until_complete(gather(pending)) which waits FOREVER on a
            # future nothing will settle, hanging the process on the teardown
            # path while the real spawn error sits in aio.run's finally
            # (gate fault-injection SPAWN_G/SPAWN_STACK hang).  Unwind both
            # registrations and settle the future as CANCELLED (it never
            # started), then let the spawn error propagate to the caller.
            _PG_ALL_TASKS.discard(self)
            if _UNREGISTER_TASK is not None:
                try:
                    _UNREGISTER_TASK(self)
                except Exception:
                    pass
            try:
                asyncio.Future.cancel(self)
            except BaseException:
                pass
            try:
                coro.close()        # never started: silence "never awaited"
            except BaseException:
                pass
            raise

    def _pg_settle_c(self):
        # Settle the underlying C Future to FINISHED so asyncio.Task.__del__
        # doesn't warn "Task was destroyed but it is pending" -- our fiber
        # drives the coro, so the C task machinery never settles its own state.
        # The C Future has no C callbacks (asyncio uses our add_done_callback),
        # so this fires nothing.
        try:
            if not asyncio.Future.done(self):
                asyncio.Future.set_result(self, None)
        except BaseException:
            pass
        # Drop our fiber handles at completion.  The driver frame (still on
        # the fiber's stack here) holds `self` as a local, so as long as the
        # task references its fiber via _g / _self_g there is a cycle
        # task -> _g/_self_g -> g -> retained driver frame -> self that survives
        # REFCOUNTING -- it only clears on the next gc.collect().  That keeps a
        # finished task (and its captured _pgexc + traceback) alive longer than
        # stock asyncio, which a well-behaved teardown -- and anyio's
        # TestRefcycles -- relies on NOT happening.  c9e1db2 cleared g->callable
        # in C; this clears the Python-side frame path.  Both _g and _self_g
        # wrap the SAME fiber, so clearing the Python refs (rather than
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
        cleaner tracebacks free of runloom internals) means stripping the driver
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

    # __repr__ is inherited from _RunloomFutureMixin (asyncio-compatible
    # "<Task pending name=... coro=...>"), shared with RunloomFuture.

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
            # If the fiber is parked in runloom_c.wait_fd (sock_recv /
            # sock_accept / sock_connect / a transport recv loop), there is NO
            # coro await-point for the driver to throw into, and G.wake() only
            # wakes park_self parkers -- so it would hang forever.  cancel_wait_fd
            # wakes the netpoll parker: wait_fd returns the CANCELLED sentinel,
            # _wait_fd raises CancelledError, and the driver settles us cancelled.
            # Falls back to wake() for a running / park_self fiber.
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

    # ---- driver: the per-task fiber body ----
    def _driver(self):
        # Capture our own G handle so cancel/done_callback can wake us.
        self._self_g = runloom_c.current_g()

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
            prev_current = _pg_set_current_task(loop, self)
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
                # Restore whatever was current before this step (None clears
                # the slot) -- _pg_set_current_task handles None as a clear.
                _pg_set_current_task(loop, prev_current)

            send_value = None

            # --- classify the yielded value ---
            if yielded is None:
                # Bare `yield` (asyncio.sleep(0) shortcut, or any other
                # cooperative checkpoint).  Stock asyncio's sleep(0) runs one
                # full loop iteration, which INCLUDES a selector poll that
                # delivers pending socket I/O.  runloom's sched_yield only
                # round-robins ready fibers and bypasses the drain loop's
                # idle netpoll pump (and the aio keepalive keeps it from going
                # idle), so without an explicit poll here a sleep(0) loop never
                # advances I/O parked on other fibers (e.g. a peer's recv
                # loop) -- breaking the common `await asyncio.sleep(0)` idiom
                # used to let pending reads land.  Deliver ready I/O first,
                # then round-trip through the scheduler so other tasks run.
                try:
                    runloom_c.netpoll_poll()
                except AttributeError:
                    pass    # older runloom_c without the non-blocking pump
                runloom_c.sched_yield_classic()
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

            if yielded is self:
                # A task awaiting itself.  Stock asyncio.Task.__step raises this
                # immediately; without the check the driver would register
                # _wake_unpark on itself, set _pgfutwaiter = self and park
                # forever (silent deadlock), and a later cancel() would recurse
                # unboundedly through self._pgfutwaiter.cancel() -> cancel() ...
                # until RecursionError.  Throw the same RuntimeError INTO the
                # coro so the task settles with an immediate diagnostic instead.
                throw_exc = RuntimeError(
                    "Task cannot await on itself: %r" % (self,))
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
                        # Resume with None, NOT yielded.result() -- exactly like
                        # asyncio's Task.__step, which always does coro.send(None).
                        # A Future's __await__ retrieves its own value (`return
                        # self.result()`) and ignores what was sent in; injecting
                        # the result is redundant for Futures and BREAKS a custom
                        # C awaitable-iterator that propagates the sent value (e.g.
                        # aiocsv's _Parser, which delegates to an executor future):
                        # a non-None send routes PyIter_Send to its `.send()`
                        # branch instead of `__next__`, raising "object has no
                        # attribute 'send'".
                        send_value = None
                except asyncio.CancelledError as e:
                    throw_exc = e
                continue

            # Slow path: park the fiber until the future fires.
            # Register the wake callback FIRST then call park_self --
            # the race where the future fires synchronously inside
            # add_done_callback is handled by park_safe / wake_safe
            # (wake_pending counter; park is a no-op if wake arrived).
            yielded.add_done_callback(self._wake_unpark)
            self._pgfutwaiter = yielded
            # select-before-wait: deliver any already-ready socket I/O before we
            # park.  Stock asyncio runs one selector poll per loop iteration, so
            # a peer fiber parked in wait_fd advances even while this side
            # has ready work; runloom only pumps netpoll when its ready ring drains
            # to empty, so without this an `await` that parks here can leave a
            # peer's recv loop starved (e.g. a server's run_asgi never sees a
            # client's close frame before the client's teardown crosses a
            # synchronous server.shutdown() boundary -> 1012 instead of 1001).
            try:
                runloom_c.netpoll_poll()
            except AttributeError:
                pass    # older runloom_c without the non-blocking pump
            runloom_c.park_self()
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
                    # Resume with None (asyncio's Task.__step always sends None);
                    # the Future's __await__ returns its own self.result(). See
                    # the matching note in the fast path above -- injecting the
                    # result breaks custom C awaitable-iterators (aiocsv _Parser).
                    send_value = None
            except asyncio.CancelledError as e:
                throw_exc = e

    def _wake_unpark(self, fut):
        # add_done_callback gives us the future; we don't need it.
        if self._self_g is not None:
            self._self_g.wake()
