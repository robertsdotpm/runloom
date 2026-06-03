"""CPython's OWN Lib/test/test_queue.py, run verbatim under runloom.monkey.patch().

monkey makes ``queue.SimpleQueue`` cooperative (CoSimpleQueue) and ``queue.Queue``
cooperative transitively (it's built on the now-cooperative threading.Condition).
We host the PURE-PYTHON queue test classes -- the C ``_queue`` implementation
has its own internal locking that the monkey layer does not patch, so only the
Python variants go cooperative.  The blocking get/put tests spawn real OS
threads, exercising cooperative-primitive <-> real-thread coordination.
"""
import pytest

from _monkey_stdlib import (HAVE_CPYTHON_TESTS, REALTHREAD, REALTHREAD_REASON,
                            hosted, patch_module, unpatch_module)

pytestmark = [
    pytest.mark.skipif(
        not HAVE_CPYTHON_TESTS,
        reason="CPython stdlib `test` package not installed on this interpreter"),
    pytest.mark.skipif(not REALTHREAD, reason=REALTHREAD_REASON),
]

setUpModule = patch_module
tearDownModule = unpatch_module

if HAVE_CPYTHON_TESTS:
    from test import test_queue as _m

    TestRunloomSimpleQueue = hosted(_m.PySimpleQueueTest, "TestRunloomSimpleQueue")
    TestRunloomQueue = hosted(_m.PyQueueTest, "TestRunloomQueue")
    TestRunloomLifoQueue = hosted(_m.PyLifoQueueTest, "TestRunloomLifoQueue")
    TestRunloomPriorityQueue = hosted(_m.PyPriorityQueueTest, "TestRunloomPriorityQueue")
