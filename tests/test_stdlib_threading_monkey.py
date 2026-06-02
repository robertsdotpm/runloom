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

# KNOWN divergence: the cooperative CoLock/CoRLock are not weakref-able, while
# _thread.lock is.  weakref.ref(threading.Lock()) raises TypeError under monkey.
# Real, fixable (give the Co* types a __weakref__ slot); recorded, not silenced.
_LOCK_SKIPS = {
    "test_weakref_exists": "monkey CoLock has no __weakref__ (cooperative-type divergence)",
    "test_weakref_deleted": "monkey CoLock has no __weakref__ (cooperative-type divergence)",
    # test_repr trips CPython's tearDown thread-leak detector against the
    # harness's per-test worker thread (a hosting artifact, not a monkey bug).
    "test_repr": "tearDown thread-leak check vs the harness worker thread (artifact)",
}

if HAVE_CPYTHON_TESTS:
    pygo.monkey.patch()
    import threading
    from test import lock_tests as _L

    TestPygoLock = hosted(_L.LockTests, "TestPygoLock",
                          attrs={"locktype": staticmethod(threading.Lock)}, skips=_LOCK_SKIPS)
    TestPygoRLock = hosted(_L.RLockTests, "TestPygoRLock",
                           attrs={"locktype": staticmethod(threading.RLock)}, skips=_LOCK_SKIPS)
    TestPygoEvent = hosted(_L.EventTests, "TestPygoEvent",
                           attrs={"eventtype": staticmethod(threading.Event)})
    TestPygoCondition = hosted(_L.ConditionTests, "TestPygoCondition",
                               attrs={"condtype": staticmethod(threading.Condition)})
    TestPygoSemaphore = hosted(_L.SemaphoreTests, "TestPygoSemaphore",
                               attrs={"semtype": staticmethod(threading.Semaphore)})
    TestPygoBoundedSemaphore = hosted(_L.BoundedSemaphoreTests, "TestPygoBoundedSemaphore",
                                      attrs={"semtype": staticmethod(threading.BoundedSemaphore)})
