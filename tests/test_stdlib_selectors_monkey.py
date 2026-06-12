"""CPython's OWN Lib/test/test_selectors.py, run verbatim under
``runloom.monkey.patch()``.

The ``selectors`` category of the monkey layer makes select.poll/epoll/kqueue
(and thus the high-level ``selectors`` module) cooperative.  This runs
CPython's real selector test classes unchanged to prove the cooperative
versions keep CPython's observable contract.  Complements the hand-adapted
tests/test_selectors_compat.py.
"""
import pytest

from _monkey_stdlib import HAVE_CPYTHON_TESTS, hosted, patch_module, unpatch_module

pytestmark = pytest.mark.skipif(
    not HAVE_CPYTHON_TESTS,
    reason="CPython stdlib `test` package not installed on this interpreter")

setUpModule = patch_module
tearDownModule = unpatch_module

if HAVE_CPYTHON_TESTS:
    from test import test_selectors as _m

    # The default selector + the explicit pollable backends.  Epoll/Kqueue/
    # Devpoll classes self-skip on platforms where that primitive is absent.
    TestRunloomDefaultSelector = hosted(_m.DefaultSelectorTestCase, "TestRunloomDefaultSelector")
    TestRunloomSelectSelector = hosted(_m.SelectSelectorTestCase, "TestRunloomSelectSelector")
    if hasattr(_m, "PollSelectorTestCase"):
        TestRunloomPollSelector = hosted(_m.PollSelectorTestCase, "TestRunloomPollSelector")
    if hasattr(_m, "EpollSelectorTestCase"):
        TestRunloomEpollSelector = hosted(_m.EpollSelectorTestCase, "TestRunloomEpollSelector")
    if hasattr(_m, "KqueueSelectorTestCase"):
        TestRunloomKqueueSelector = hosted(_m.KqueueSelectorTestCase, "TestRunloomKqueueSelector")

    # test_select_interrupt_exc installs a SIGALRM handler that raises during
    # select() and expects the exception to propagate out of the call.  Through
    # the cooperative wait_fd park this works on every backend: when a signal
    # EINTRs the scheduler's idle pump, the scheduler runs the pending handler
    # and, if it raises, hands the exception to the parked fiber (which
    # restores it on resume and returns out of select()), instead of swallowing
    # it or carrying it out of run().  That delivery path is in the backend-
    # independent scheduler/wait_fd core (runloom_netpoll_signal_wake +
    # RUNLOOM_NETPOLL_SIGNALED), so epoll / kqueue / select all pass -- no skip.
    # See tests/test_signal_interrupt.py for the focused regression test.
