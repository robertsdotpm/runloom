"""CPython's OWN Lib/test/test_selectors.py, run verbatim under
``pygo.monkey.patch()``.

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
    TestPygoDefaultSelector = hosted(_m.DefaultSelectorTestCase, "TestPygoDefaultSelector")
    TestPygoSelectSelector = hosted(_m.SelectSelectorTestCase, "TestPygoSelectSelector")
    if hasattr(_m, "PollSelectorTestCase"):
        TestPygoPollSelector = hosted(_m.PollSelectorTestCase, "TestPygoPollSelector")
    if hasattr(_m, "EpollSelectorTestCase"):
        TestPygoEpollSelector = hosted(_m.EpollSelectorTestCase, "TestPygoEpollSelector")
    if hasattr(_m, "KqueueSelectorTestCase"):
        TestPygoKqueueSelector = hosted(_m.KqueueSelectorTestCase, "TestPygoKqueueSelector")
