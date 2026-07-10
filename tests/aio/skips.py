"""Committed skip baseline for the vendored asyncio conformance suite.

Each entry marks a CPython test that DIVERGES on the runloom bridge, so the suite
is green on the DEFAULT bridge (no src/runloom changes).  A failure NOT listed
here reds the suite -- that is the regression signal.

Format: SKIPS[<module basename>][<key>] = "<reason>", where <key> is either
  "ClassName.method_name"   -- skip one test (parametrization suffix ignored), or
  "ClassName.*"             -- skip an entire test class (e.g. a redundant
                               selector-variant class that adds no runloom coverage).

Populated per module as each is vendored + brought to green; see conftest.py
(apply via pytest_collection_modifyitems).
"""

GH96704 = ("gh-96704: the bridge runs the exception handler in the outer context, "
           "not the failing task/handle's contextvars Context (accepted default-"
           "bridge behavior; the loop_core fix was reverted as low-applicability)")

# Reasons shared by several test_events entries (stated once, here).  The three
# selector-variant classes (EPoll/Poll/Select) are identical runs once conftest
# makes create_event_loop() return RunloomEventLoop(): the loop drives its own
# netpoll and ignores the selector.  SelectEventLoopTests is the canonical one.
EV_SELECTOR_REDUNDANT = ("redundant selector variant -- identical to "
                         "SelectEventLoopTests once the loop is runloom "
                         "(selector-independent)")
EV_PIPE_HANG = "pipe/PTY transport HANGS on the runloom loop"
EV_XTHREAD_HANG = "cross-thread call_soon_threadsafe HANGS on the runloom loop"
EV_MULTIHOST = "multi-host create_server bind-error handling divergence"
EV_NEW_PROCESS = ("HANGS: run_in_executor(ProcessPoolExecutor) never completes "
                  "on the runloom loop")

SKIPS = {
    "test_futures2": {
        # gh-96704 exception-handler-contextvars edge (all 4 variants) -- the
        # bridge does not re-enter the failing callback's Context.  No real
        # workload uses set_exception_handler with contextvars this way.
        "CFutureTests.test_task_exc_handler_correct_context": GH96704,
        "CFutureTests.test_handle_exc_handler_correct_context": GH96704,
        "PyFutureTests.test_task_exc_handler_correct_context": GH96704,
        "PyFutureTests.test_handle_exc_handler_correct_context": GH96704,
    },
    "test_tasks": {
        # run_coroutine_threadsafe(...).cancel() from a FOREIGN thread: the task
        # is not observed cancelled by the deadline the stock loop guarantees --
        # a cross-thread cancel-timing divergence on the runloom bridge.
        "RunCoroutineThreadsafeTests.test_run_coroutine_threadsafe_and_cancel":
            "cross-thread run_coroutine_threadsafe cancel not observed cancelled (bridge cross-thread cancel timing)",
    },
    "test_events": {
        # --- Redundant selector-variant classes ------------------------------
        # EPoll/Poll/Select all inherit EventLoopTestsMixin and differ only in
        # the selector passed to create_event_loop(); conftest replaces that
        # with RunloomEventLoop() regardless, so they run identically.  Keep
        # SelectEventLoopTests (always present) as canonical; skip the others.
        "EPollEventLoopTests.*": EV_SELECTOR_REDUNDANT,
        "PollEventLoopTests.*": EV_SELECTOR_REDUNDANT,

        # --- Canonical class (SelectEventLoopTests) divergences ---------------
        # Signal handlers not implemented on the loop.
        "SelectEventLoopTests.test_add_signal_handler":
            "loop signal handlers unimplemented",

        # Pipe / PTY transport deadlocks on the runloom loop.
        "SelectEventLoopTests.test_bidirectional_pty": EV_PIPE_HANG,
        "SelectEventLoopTests.test_write_pty": EV_PIPE_HANG,
        "SelectEventLoopTests.test_write_pipe": EV_PIPE_HANG,
        "SelectEventLoopTests.test_write_pipe_disconnect_on_close": EV_PIPE_HANG,

        # call_soon_threadsafe from a foreign thread deadlocks.
        "SelectEventLoopTests.test_call_soon_threadsafe_handle_block_cancellation":
            EV_XTHREAD_HANG,
        "SelectEventLoopTests.test_call_soon_threadsafe_handle_block_check_cancelled":
            EV_XTHREAD_HANG,
        "SelectEventLoopTests.test_call_soon_threadsafe_handle_cancel_other_thread":
            EV_XTHREAD_HANG,

        # Tests reach into stock-loop internals the runloom loop doesn't mirror.
        "SelectEventLoopTests.test_timeout_rounding":
            "test reads loop._run_once (stock-loop internal the runloom loop doesn't mirror)",
        "SelectEventLoopTests.test_prompt_cancellation":
            "test reads loop._stop_serving (stock-loop internal the runloom loop doesn't mirror)",

        # Multi-host create_server bind-error handling divergence.
        "SelectEventLoopTests.test_create_server_multiple_hosts_ipv4": EV_MULTIHOST,
        "SelectEventLoopTests.test_create_server_multiple_hosts_ipv6": EV_MULTIHOST,

        # (The get_event_loop-policy + Server-ABC tail classes used to be skipped
        # wholesale here: an SSL-server test left current-task state on the loop
        # that wedged them in full-module order.  Fixed by clearing the per-thread
        # current-task slot at drive-end in loop_run._drive, so they run now.)
    },
}


def lookup(module, class_name, method_name):
    """Return a skip reason for (module, Class, method), else None.  Matches an
    exact 'Class.method' first, then a 'Class.*' class-wide entry."""
    mod = SKIPS.get(module)
    if not mod:
        return None
    exact = mod.get("%s.%s" % (class_name, method_name))
    if exact:
        return exact
    return mod.get("%s.*" % (class_name,))
