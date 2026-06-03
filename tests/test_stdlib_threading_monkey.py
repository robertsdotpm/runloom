"""CPython's OWN Lib/test/lock_tests.py, run verbatim against runloom.monkey's
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
import runloom.monkey

pytestmark = [
    pytest.mark.skipif(
        not HAVE_CPYTHON_TESTS,
        reason="CPython stdlib `test` package not installed on this interpreter"),
    pytest.mark.skipif(not REALTHREAD, reason=REALTHREAD_REASON),
]

if HAVE_CPYTHON_TESTS:
    runloom.monkey.patch()
    import threading
    from test import lock_tests as _L

    # (The CoLock/CoRLock weakref divergence the verbatim run first surfaced is
    # FIXED -- the Co* types now carry a __weakref__ slot -- so no skips here.)
    TestRunloomLock = hosted(_L.LockTests, "TestRunloomLock",
                          attrs={"locktype": staticmethod(threading.Lock)})
    TestRunloomRLock = hosted(_L.RLockTests, "TestRunloomRLock",
                           attrs={"locktype": staticmethod(threading.RLock)})
    TestRunloomEvent = hosted(_L.EventTests, "TestRunloomEvent",
                           attrs={"eventtype": staticmethod(threading.Event)})
    TestRunloomCondition = hosted(_L.ConditionTests, "TestRunloomCondition",
                               attrs={"condtype": staticmethod(threading.Condition)})
    TestRunloomSemaphore = hosted(_L.SemaphoreTests, "TestRunloomSemaphore",
                               attrs={"semtype": staticmethod(threading.Semaphore)})
    TestRunloomBoundedSemaphore = hosted(_L.BoundedSemaphoreTests, "TestRunloomBoundedSemaphore",
                                      attrs={"semtype": staticmethod(threading.BoundedSemaphore)})
