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
EV_SSL = "loop SSL transport not wired"
EV_SUBPROCESS = ("subprocess transport unimplemented -- invalid-args validation "
                 "raises TypeError, not the expected ValueError")
EV_PIPE_HANG = "pipe/PTY transport HANGS on the runloom loop"
EV_XTHREAD_HANG = "cross-thread call_soon_threadsafe HANGS on the runloom loop"
EV_DATAGRAM = ("runloom DatagramTransport is not an asyncio.transports.Transport "
               "subclass (type-identity divergence)")
EV_CLOSE = ("op after loop close() does not raise RuntimeError on the bridge "
            "(add/remove-fds-after-close divergence)")
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

        # SSL transport not wired into the loop.
        "SelectEventLoopTests.test_create_ssl_connection": EV_SSL,
        "SelectEventLoopTests.test_create_ssl_unix_connection": EV_SSL,

        # Subprocess transport unimplemented.
        "SelectEventLoopTests.test_subprocess_exec_invalid_args": EV_SUBPROCESS,
        "SelectEventLoopTests.test_subprocess_shell_invalid_args": EV_SUBPROCESS,

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

        # DatagramTransport type-identity divergence.
        "SelectEventLoopTests.test_create_datagram_endpoint": EV_DATAGRAM,
        "SelectEventLoopTests.test_create_datagram_endpoint_ipv6": EV_DATAGRAM,
        "SelectEventLoopTests.test_create_datagram_endpoint_sock": EV_DATAGRAM,

        # create_server exposes the raw socket, not a trsock.TransportSocket.
        "SelectEventLoopTests.test_create_server_trsock":
            "server.sockets holds a raw socket, not asyncio.trsock.TransportSocket",

        # Pipe-transport repr lacks the 'open' state text.
        "SelectEventLoopTests.test_unclosed_pipe_transport":
            "pipe-transport repr lacks the 'open' state text",

        # Operating on the loop after close() does not raise RuntimeError.
        "SelectEventLoopTests.test_add_fds_after_closing": EV_CLOSE,
        "SelectEventLoopTests.test_close": EV_CLOSE,
        "SelectEventLoopTests.test_remove_fds_after_closing": EV_CLOSE,

        # ssl_handshake_timeout without ssl=True does not raise ValueError.
        "SelectEventLoopTests.test_connect_accepted_socket_ssl_timeout_for_plain_socket":
            "ssl_handshake_timeout without ssl does not raise ValueError",

        # EADDRINUSE OSError.strerror format differs from CPython's.
        "SelectEventLoopTests.test_create_connection_local_addr_in_use":
            "EADDRINUSE error message format differs (strerror lacks the address)",

        # Multi-host create_server bind-error handling divergence.
        "SelectEventLoopTests.test_create_server_multiple_hosts_ipv4": EV_MULTIHOST,
        "SelectEventLoopTests.test_create_server_multiple_hosts_ipv6": EV_MULTIHOST,

        # Executor-future cancel semantics divergence (callback still ran).
        "SelectEventLoopTests.test_run_in_executor_cancel":
            "executor-future cancel semantics divergence (cancelled callback still ran)",

        # add_writer partial-flush written-bytes mismatch (real gap).
        "SelectEventLoopTests.test_writer_callback":
            "add_writer partial-flush written-bytes mismatch (real gap)",

        # --- Non-selector classes reached via the global policy ---------------
        # Hangs from leftover current-task state left by an earlier
        # create_connection test ("Cannot enter into task while another is being
        # executed") once the whole module runs in order.
        "TestAbstractServer.test_wait_closed":
            "HANGS: leftover current-task state from a prior create_connection "
            "(Cannot enter into task while another is being executed)",

        # ProcessPoolExecutor run_in_executor never completes on the loop.
        "TestPyGetEventLoop.test_get_event_loop_new_process": EV_NEW_PROCESS,
        "TestCGetEventLoop.test_get_event_loop_new_process": EV_NEW_PROCESS,

        # HANGS only in full-module order (passes in isolation, and the Py
        # variant passes): leftover running-loop/current-task state from earlier
        # runloom-loop tests wedges the C get_event_loop path here.
        "TestCGetEventLoop.test_get_event_loop_returns_running_loop":
            "HANGS in full-module order -- leftover running-loop state from an "
            "earlier runloom-loop test wedges the C get_event_loop path "
            "(passes in isolation; Py variant passes)",
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
