"""RunloomEventLoop: run_until_complete/run_forever/stop, the _drive pump,
and shutdown_*."""
from ._base import *  # noqa: F401,F403  (shared foundation)
from .futures import RunloomFuture  # noqa: F401
from .handles import _PG_ALL_TASKS, _PG_OPEN_LOOPS  # noqa: F401

class _LoopRunMixin(object):
    def _can_spawn_here(self):
        """True iff runloom_c.fiber is safe on the CALLING thread for THIS loop:
        its fiber() lands on a scheduler this loop will drain.  A FOREIGN thread
        must marshal via call_soon_threadsafe / defer into _ts_queue -- its
        fiber() would land on ITS own sched, which this loop never drains.

        RUNNING (_thread_id set): only the loop thread.  PRE-RUN (_thread_id
        None): only the thread that entered run() and is about to drive (claimed
        as _pg_driver_tid) -- NOT any thread, which the old `tid is None`
        shortcut wrongly allowed, stranding a foreign thread's pre-run
        call_soon/create_task on its own sched (R7 item 2)."""
        tid = self._thread_id
        if tid is not None:
            return tid == _threading.get_ident()
        return self._pg_driver_tid == _threading.get_ident()

    def _drive(self):
        """Drain THIS thread's scheduler until the run's stop fires.  Each loop
        runs on its own OS thread and drains its own (thread-local) sched, so
        loops on different threads are INDEPENDENT: one thread blocking
        synchronously inside a coroutine (run_coroutine_threadsafe().result(),
        anyio BlockingPortal, a threaded server controller with a blocking
        client) freezes only its own sched, never the others'.  runloom_c.run()
        returns when sched_stop fires (the future-done callback, or the
        keepalive observing loop.stop()) or the scheduler empties."""
        self._running = True
        self._thread_id = _threading.get_ident()
        asyncio._set_running_loop(self)
        try:
            runloom_c.run()
        finally:
            self._running = False
            self._thread_id = None
            # The run is over; drop the pre-run driver claim so a later
            # create_task on this thread (loop not running) is treated as
            # foreign and deferred, not direct-spawned onto an undrained sched.
            self._pg_driver_tid = None
            asyncio._set_running_loop(None)
            # Retire the keepalive so it can't linger parked in the sleep queue
            # into the next run on this loop.
            if self._ka_stop_box is not None:
                self._ka_stop_box[0] = True
        # A KeyboardInterrupt / SystemExit raised in a callback or task during
        # the drive was stashed by _pg_signal_fatal (which sched_stop'd us out
        # of runloom_c.run()).  Re-raise it so it propagates out of
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
        self._check_closed()
        self._check_running()
        # Claim the loop for THIS (soon-to-drive) thread across the pre-run
        # window, so our own create_task(future) below spawns directly while a
        # FOREIGN thread's concurrent pre-run scheduling defers (see
        # _can_spawn_here).  Cleared in _drive's finally on the normal path; the
        # try/except here covers an abort BEFORE _drive (create_task TypeError,
        # ensure_future, prewarm, _spawn_keepalive) so a stale claim can't leak.
        self._pg_driver_tid = _threading.get_ident()
        try:
            if asyncio.iscoroutine(future):
                future = self.create_task(future)
            elif not (isinstance(future, asyncio.Future)
                      or isinstance(future, RunloomFuture)
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
            # first-call codec import) before any fiber runs them on a small
            # stack -- see prewarm_stdlib.
            _runtime.prewarm_stdlib()
        except BaseException:
            self._pg_driver_tid = None
            raise
        # Clear any stale stop request from a prior run on this loop.
        self._stopping = False
        # Always drive the loop for at least one iteration, EVEN when `future`
        # is already done.  Stock BaseEventLoop.run_until_complete always calls
        # run_forever(), so callbacks previously scheduled with call_soon run
        # before it returns (asyncio defers the done future's stop through
        # call_soon, so the loop processes its ready queue for one iteration and
        # then stops).  Gating the entire drive on `if not future.done()`
        # skipped that iteration for an already-completed future, stranding the
        # fibers those earlier call_soon() calls spawned onto this thread's
        # ready ring -- they sat unexecuted until some unrelated later run
        # happened to drive this thread's scheduler (firing spuriously then).
        #
        # When the user-visible future completes, break our drain (matches
        # asyncio.run -- don't block on background accept/ticker fibers
        # the user didn't join).
        def _stop_on_done(_fut):
            box = self._ka_stop_box
            if box is not None:
                box[0] = True
            runloom_c.sched_stop()
        # Fire synchronously the instant the main future completes (do NOT
        # defer through call_soon like a library done-callback): this is the
        # run loop's own control hook -- it must stop the drive in the same
        # turn, not a tick later. See _fire_callbacks (_runloom_fire_sync).
        # (For an ALREADY-done future add_done_callback defers this through
        # call_soon anyway, so the single drive iteration still runs the
        # pending ready work first, then _stop_on_done stops the drive.)
        _stop_on_done._runloom_fire_sync = True
        future.add_done_callback(_stop_on_done)
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
            self._spawn_keepalive()
            self._drive()
        finally:
            # Idempotent: _drive's finally already cleared the driver claim if it
            # ran; this also clears it if _spawn_keepalive aborted before _drive.
            self._pg_driver_tid = None
            future.remove_done_callback(_stop_on_done)
        # IMPORTANT: do NOT cancel outstanding tasks / sched_reset here.
        # run_until_complete must leave other tasks + parked fibers ALIVE
        # (IsolatedAsyncioTestCase / asyncio.Runner reuse one loop across
        # asyncSetUp / test / asyncTearDown).  asyncio.run-style teardown
        # lives in close().
        if not future.done():
            raise RuntimeError("event loop stopped before Future completed")
        return future.result()

    def _cancel_outstanding_tasks(self):
        """Cancel every RunloomTask still alive on this loop and clear
        the scheduler's leftover state.  Called from run_until_complete
        after the main future resolves so background fibers
        (call_later runners, accept loops, ticker fibers) don't
        leak into the next paio.run.

        Strategy: cancel all known tasks (best-effort -- not all are
        interruptible), then sched_reset() the scheduler's ready+sleep
        queues so the next runloom_c.run() sees a clean slate."""
        try:
            tasks = [t for t in asyncio.all_tasks(self) if t._loop is self]
        except Exception:
            tasks = []
        for t in tasks:
            try:
                t.cancel()
            except Exception:
                pass
        # Drive the loop so the cancellations actually PROPAGATE.  t.cancel()
        # only requests a cancel (waking the task's parked fiber); the woken
        # fiber must then run so its CancelledError unwinds try/finally and
        # async-with __aexit__ cleanup and the task settles.  asyncio.run's
        # Runner does exactly this -- it run_until_complete(gather(...))s the
        # cancelled tasks AFTER cancelling them.  Without a drive here the woken
        # fibers never run and cleanup is silently skipped (locks stay held,
        # files/sockets/connections leak, aclose() bodies are dropped).
        # close() has already flipped _closed=True (asyncio, by contrast,
        # cancels BEFORE close()), so briefly re-open the loop for this cleanup
        # drive: call_soon and gather's own done-callback bookkeeping raise on a
        # closed loop, which would strand the cleanup.  Restore _closed after.
        pending = [t for t in tasks if not t.done()]
        if pending:
            was_closed = self._closed
            self._closed = False
            try:
                self.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            finally:
                self._closed = was_closed
        # Forcibly drop anything still scheduled.  Goroutines parked on
        # netpoll/wake/chan that aren't interrupted by cancel get
        # abandoned; the underlying coro and snap are freed when the
        # last Python reference drops.
        # Only drain the shared per-thread scheduler if NO sibling loop still
        # has live tasks on it.  The runloom scheduler is one-per-OS-thread, shared
        # by every RunloomEventLoop on the thread; a blind sched_reset here would
        # bulldoze another loop's still-needed fibers -- e.g. a background
        # server task's in-flight asyncio.sleep sitting in the shared sleep heap
        # -- deadlocking that loop when it is next driven (the hypercorn /
        # pytest-asyncio fixture-vs-test multi-loop case).
        # A task on a CLOSED loop can never be driven again, so it is dead even
        # if it never reached done() (e.g. a gather() first-exception that
        # cancelled its siblings but stopped the loop before the cancellation
        # propagated).  Such zombies must NOT count as "busy" -- otherwise every
        # subsequent run() skips sched_reset() and strands its own accept-fiber
        # netpoll parker, which accumulates one per run (finding #7).  Open
        # sibling loops are still protected, both here and by other_loop_open.
        sibling_busy = any(
            (t._loop is not self and not t.done()
             and not getattr(t._loop, "_closed", False))
            for t in list(_PG_ALL_TASKS))
        # sched_reset() bulldozes the SHARED per-thread scheduler (ready ring +
        # sleep heap).  Any OTHER open loop on this thread may have live work
        # sitting there -- including raw call_later timer fibers (a server
        # handler's in-flight asyncio.sleep) that the _PG_ALL_TASKS task guard
        # cannot see.  So only reset when we are the LAST open loop; otherwise a
        # sibling's pending sleep is silently dropped and the fiber awaiting
        # it hangs forever (aiohttp's streaming-handler teardown deadlock).
        other_loop_open = any(
            (lp is not self and not lp._closed) for lp in list(_PG_OPEN_LOOPS))
        if not sibling_busy and not other_loop_open:
            try:
                runloom_c.sched_reset()
            except AttributeError:
                pass  # Older build without sched_reset; best-effort drain.

    def run_forever(self):
        self._check_closed()
        self._check_running()
        # Claim the loop for THIS soon-to-drive thread (see run_until_complete /
        # _can_spawn_here): a foreign thread's pre-run call_soon/create_task now
        # defers into _ts_queue instead of stranding on its own sched.
        self._pg_driver_tid = _threading.get_ident()
        try:
            # Resolve deep, non-yielding stdlib imports (e.g. getaddrinfo's
            # first-call codec import) before any fiber runs them on a small
            # stack -- see prewarm_stdlib.
            _runtime.prewarm_stdlib()
        except BaseException:
            self._pg_driver_tid = None
            raise
        # Do NOT reset self._stopping here.  asyncio honors a stop() issued
        # BEFORE run_forever() -- it runs one iteration and returns (stock checks
        # self._stopping at the top of each loop pass and only clears it on
        # EXIT).  Resetting it at entry wipes that request, so the keepalive
        # fiber never sees the stop and spins sched_sleep forever -- the
        # `loop.stop(); loop.run_forever()` cleanup idiom (aiohttp's synchronous
        # test_streams/test_web_app default-loop tests) hangs.  When _stopping is
        # already True the keepalive calls sched_stop() on its first pass and the
        # drive returns immediately.
        try:
            self._spawn_keepalive()
            self._drive()
        finally:
            # _drive's finally clears the driver claim on the normal path; this
            # also covers a _spawn_keepalive abort before _drive.
            self._pg_driver_tid = None
            self._stopping = False

    def stop(self):
        # asyncio contract: request the loop stop after the current iteration.
        # Setting the flag is thread-safe (a plain bool store); the keepalive
        # fiber -- which runs on the loop thread -- observes it and calls
        # runloom_c.sched_stop() to return from run_forever()'s runloom_c.run().
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
        fiber's raise, so we stash the first such exception here and call
        sched_stop() to return from runloom_c.run(); _drive re-raises it.

        Always called on the loop thread (every callback/task runner runs
        there), so sched_stop() targets this thread's scheduler."""
        if self._pg_fatal_exc is None:
            self._pg_fatal_exc = exc
        if self._ka_stop_box is not None:
            self._ka_stop_box[0] = True
        try:
            runloom_c.sched_stop()
        except Exception:
            pass

    # asyncio.run() shutdown protocol -- minimal no-ops so user code
    # written against asyncio.run works through `paio.install()`.
    async def shutdown_asyncgens(self):
        return None

    async def shutdown_default_executor(self, timeout=None):
        return None
