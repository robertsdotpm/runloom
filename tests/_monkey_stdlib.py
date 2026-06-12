"""Support for running CPython's OWN stdlib test suites VERBATIM under
``runloom.monkey.patch()`` -- the gevent-style "run the real stdlib tests green"
conformance (see ``tests/test_stdlib_*_monkey.py``).

Why verbatim and not the hand-adapted ``tests/test_*_compat.py``: the compat
suites exercise a curated subset of each cooperative primitive.  Running
CPython's *own* ``Lib/test/test_*.py`` bodies unchanged -- the same code the
blocking stdlib is validated with -- is the gold standard: any divergence is a
real cooperative-semantics bug in the monkey layer, found by code we didn't
write to be kind to it.  This mirrors ``tests/test_asyncio_*_conformance.py``,
which runs CPython's ``test_asyncio`` against ``RunloomEventLoop``.

Mechanics: each CPython test method is run inside its own runloom fiber (the
``monkey`` patches only cooperate under the C scheduler) with a LARGE stack.
The large stack is required, not cosmetic: CPython test bodies do deep,
non-yielding imports (e.g. ``mock.patch`` -> ``importlib._find_and_load``) and
C-stack-heavy work; a non-yielding burst can't be rescued by the copy-grow
path (which only grows at yield points), so on a default small fiber stack
it overflows into the guard page and SIGSEGVs.  ``stack_size=`` is the
documented knob for exactly this ("entry function known to recurse deeply or
call into a C extension that consumes large C stack").
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import runloom            # noqa: E402
import runloom.monkey     # noqa: E402
import runloom_c       # noqa: E402

# CPython's stdlib `test` package isn't shipped with every interpreter
# (embedded / some Windows builds); conformance files skip cleanly when absent.
try:
    import test as _cpython_test          # noqa: F401
    HAVE_CPYTHON_TESTS = True
except ImportError:
    HAVE_CPYTHON_TESTS = False

# 8 MB: a normal-thread-sized stack, so arbitrary CPython test bodies (deep
# imports, recursion, C extensions) run without overflowing.  Override with
# RUNLOOM_STDLIB_STACK.
STACK = int(os.environ.get("RUNLOOM_STDLIB_STACK", str(8 << 20)))

# Some CPython stdlib test modules (threading lock_tests, queue, socket) drive
# their blocking primitives from REAL OS threads -- the producer/consumer or
# client/server is a `threading.Thread`, not a fiber.  Under the cooperative
# model that's "best-effort coordination with real OS threads" (monkey's own
# words), and those tests can DEADLOCK: a real thread blocked in a cooperative
# primitive needs the single-thread scheduler to wake it, but the scheduler is
# elsewhere.  Such modules are gated off by default (they'd hang the gate) and
# run only for boundary exploration with RUNLOOM_STDLIB_REALTHREAD=1.  The
# cooperative primitives' GOROUTINE-driven behaviour is covered cleanly by the
# hand-adapted tests/test_*_compat.py.
REALTHREAD = os.environ.get("RUNLOOM_STDLIB_REALTHREAD") == "1"
REALTHREAD_REASON = ("verbatim CPython tests here drive blocking primitives from "
                     "REAL OS threads, which deadlock under the cooperative model; "
                     "exploratory -- set RUNLOOM_STDLIB_REALTHREAD=1 to run")


class MonkeyHosted(object):
    """Mixin -- put FIRST in the MRO, before the CPython TestCase.

    Overrides ``run`` so each test method executes inside its own big-stack
    runloom fiber on the MAIN thread (setUp / the test / tearDown all in the
    one fiber), driven by the C scheduler the way real ``runloom.monkey``
    users drive their code.  Main-thread (not a per-test worker) on purpose:
    CPython's own tests run a thread-leak detector in tearDown, and a per-test
    helper thread would dangle and trip it with false failures.  A genuine
    cooperative deadlock therefore hangs the file; that's contained by
    tests/run_isolated.py's per-file subprocess timeout (the canonical runner),
    and the offending test is then added to the file's documented skips."""

    def run(self, result=None):
        holder = {}

        def body():
            holder["r"] = super(MonkeyHosted, self).run(result)

        runloom_c.go(body, stack_size=STACK)
        runloom_c.run()
        return holder.get("r", result)


def skip_methods(cls, mapping):
    """Replace named test methods on ``cls`` with documented skips.  Used to
    record KNOWN monkey divergences (and a couple of harness artifacts) without
    silencing them -- each shows up as a skip with its reason, exactly like the
    asyncio-conformance _KNOWN_GAPS pattern, keeping the suite green."""
    for meth, reason in mapping.items():
        orig = getattr(cls, meth, None)
        if orig is not None:
            setattr(cls, meth, unittest.skip(reason)(orig))
    return cls


def hosted(cpython_cls, name, skips=None, attrs=None):
    """Build a pytest-collectable ``MonkeyHosted`` subclass of a CPython test
    class.  ``name`` must start with ``Test`` so pytest collects it.  ``attrs``
    sets extra class attributes (e.g. a ``locktype`` factory); ``skips`` maps
    method name -> reason for documented known-divergence skips."""
    cls = type(name, (MonkeyHosted, cpython_cls), dict(attrs or {}))
    if skips:
        skip_methods(cls, skips)
    return cls


def patch_module():
    """setUpModule hook."""
    if HAVE_CPYTHON_TESTS:
        runloom.monkey.patch()


def unpatch_module():
    """tearDownModule hook."""
    if HAVE_CPYTHON_TESTS:
        runloom.monkey.unpatch()
