"""CPython's OWN Lib/test/lock_tests.py, run verbatim against pygo.monkey's
cooperative threading primitives.

monkey.patch() replaces threading.Lock / RLock / Event / Condition / Semaphore
/ BoundedSemaphore with cooperative versions.  CPython's generic lock-test
suite (the same one that validates the real primitives) is run unchanged
against them.  This is the highest-risk monkey category: the tests spawn REAL
OS threads that coordinate through the cooperative primitives, which the layer
itself documents as "best-effort coordination with real OS threads."

patch() is applied at import time, BEFORE the cooperative types are captured
into the hosted classes' ``locktype``/``eventtype``/... attributes.
"""
import pytest

from _monkey_stdlib import (HAVE_CPYTHON_TESTS, REALTHREAD, REALTHREAD_REASON, hosted)
import pygo.monkey

pytestmark = [
    pytest.mark.skipif(
        not HAVE_CPYTHON_TESTS,
        reason="CPython stdlib `test` package not installed on this interpreter"),
    pytest.mark.skipif(not REALTHREAD, reason=REALTHREAD_REASON),
]

if HAVE_CPYTHON_TESTS:
    pygo.monkey.patch()
    import threading
    from test import lock_tests as _L

    # (The CoLock/CoRLock weakref divergence the verbatim run first surfaced is
    # FIXED -- the Co* types now carry a __weakref__ slot -- so no skips here.)
    TestPygoLock = hosted(_L.LockTests, "TestPygoLock",
                          attrs={"locktype": staticmethod(threading.Lock)})
    TestPygoRLock = hosted(_L.RLockTests, "TestPygoRLock",
                           attrs={"locktype": staticmethod(threading.RLock)})
    TestPygoEvent = hosted(_L.EventTests, "TestPygoEvent",
                           attrs={"eventtype": staticmethod(threading.Event)})
    TestPygoCondition = hosted(_L.ConditionTests, "TestPygoCondition",
                               attrs={"condtype": staticmethod(threading.Condition)})
    TestPygoSemaphore = hosted(_L.SemaphoreTests, "TestPygoSemaphore",
                               attrs={"semtype": staticmethod(threading.Semaphore)})
    TestPygoBoundedSemaphore = hosted(_L.BoundedSemaphoreTests, "TestPygoBoundedSemaphore",
                                      attrs={"semtype": staticmethod(threading.BoundedSemaphore)})
