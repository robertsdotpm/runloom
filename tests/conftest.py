"""Per-test invariant checks for the runloom suite.

Every test in this directory runs through an autouse fixture that, AFTER the
test body, asserts two things about the C runtime:

  1. ``runloom_c._self_check(0) == 0`` -- a structural walk of every live
     scheduler / netpoll data structure (no list cycle, no self-looping
     per-fd bucket, the atomic parked count matches the walked count, no
     bucket entry missing from the global list).  This is a pure consistency
     invariant: it holds regardless of how much work a test left behind, so
     it never false-positives on a legitimately-busy background loop thread.

  2. No *leaked* netpoll parker.  We snapshot ``stats()['netpoll_parked']``
     before the test and re-read it after; a fiber that parked in
     ``wait_fd`` and never got woken (the cross-thread-drain / leaked-parker
     class of bug) shows up as a count that never settles back.  A short
     settle window absorbs teardown races where a background thread is about
     to drain its own parker.

Why this exists: in practice a leaked parker did not fail the test that
caused it -- it wedged an *unrelated* ``runloom_c.run()`` several files later,
which is brutal to bisect.  Attributing the leak to the test that created it
(via a per-test before/after delta) turns "the suite hangs sometimes" into
"this one test leaked a parker."

Opt out with ``@pytest.mark.runloom_leaky`` for a test that deliberately leaves a
parker behind (e.g. the regression that proves a leaked parker no longer
wedges other threads).

Env knobs:
  RUNLOOM_TEST_LEAK_REPORT=1  -- print the per-test parked delta instead of
                              failing on it (survey mode; self_check still
                              hard-asserts).
  RUNLOOM_TEST_NO_INVARIANTS=1 -- disable the fixture entirely.
"""
import os
import sys
import time

# Match run_tests.py / test_mn.py: test the in-tree .so, not whatever else
# might be on the path.  Harmless if runloom_c is already imported (Python
# caches the module, so the fixture inspects the same runtime the tests use).
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(REPO, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import threading

import pytest

try:
    import runloom_c
except Exception:  # pragma: no cover - runloom_c should always import here
    runloom_c = None

_REPORT_ONLY = os.environ.get("RUNLOOM_TEST_LEAK_REPORT") == "1"
_DISABLED = os.environ.get("RUNLOOM_TEST_NO_INVARIANTS") == "1"

# --- swallowed-error gate (QA-steal-V2 #3) ---------------------------------
# Errors raised on a path that cannot propagate -- a tp_dealloc / weakref
# finalizer that raises, an exception in a callback run on a hub OS thread, an
# unawaited-task error -- go to sys.unraisablehook / threading.excepthook and
# VANISH (in the free-threaded build, concurrently across many hubs), often
# corrupting half-reclaimed state that later surfaces as an unrelated UAF.
# Install a process-wide gate (below) so any such swallowed error fails the test
# it fired under instead of disappearing.  A test that INTENTIONALLY raises on
# such a path opts out with @pytest.mark.runloom_allow_unraisable; tests using
# test.support.catch_unraisable_exception install their own hook for their scope
# and are unaffected.  RUNLOOM_TEST_LEAK_REPORT=1 makes it report-only too.
_UNRAISABLE = []
_pg_saved_unraisablehook = None
_pg_saved_threadexcepthook = None


def _pg_unraisable_hook(unraisable):
    try:
        _UNRAISABLE.append("unraisable[{0}]: {1!r} (obj {2!r})".format(
            getattr(unraisable, "err_msg", None) or "Exception ignored",
            getattr(unraisable, "exc_value", None),
            getattr(unraisable, "object", None)))
    except Exception:  # never let the hook itself raise
        _UNRAISABLE.append("unraisable: <unformattable>")


def _pg_thread_excepthook(args):
    try:
        _UNRAISABLE.append("thread-excepthook: {0!r} on {1!r}".format(
            getattr(args, "exc_value", None), getattr(args, "thread", None)))
    except Exception:
        _UNRAISABLE.append("thread-excepthook: <unformattable>")

# How long to let a background thread finish draining its own parker before we
# call a non-zero delta a real leak.  Real leaks never drain, so this only
# costs wall-clock on a genuine failure or a slow teardown.
_SETTLE_DEADLINE_S = 0.5
_SETTLE_STEP_S = 0.01


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "runloom_leaky: test deliberately leaves a netpoll parker behind; "
        "skip the post-test parked-leak invariant for it.")
    config.addinivalue_line(
        "markers",
        "runloom_allow_unraisable: test intentionally raises on a dealloc / "
        "finalizer / hub-thread path; skip the swallowed-error gate for it.")
    if not _DISABLED:
        global _pg_saved_unraisablehook, _pg_saved_threadexcepthook
        _pg_saved_unraisablehook = sys.unraisablehook
        _pg_saved_threadexcepthook = threading.excepthook
        sys.unraisablehook = _pg_unraisable_hook
        threading.excepthook = _pg_thread_excepthook


def pytest_unconfigure(config):
    if _pg_saved_unraisablehook is not None:
        sys.unraisablehook = _pg_saved_unraisablehook
    if _pg_saved_threadexcepthook is not None:
        threading.excepthook = _pg_saved_threadexcepthook


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    # Stash each phase's report on the item so the fixture teardown can tell
    # whether the test body itself failed (in which case piling a leak error
    # on top is just noise -- the real failure already explains it).
    rep = yield
    setattr(item, "_pg_rep_" + rep.when, rep)
    return rep


def _parked():
    # Per-sched count (this thread's sched), not the global one: a parker
    # stranded on another/since-exited thread's sched (e.g. a test that
    # deliberately leaks one on a dead thread) is not this test's leak and
    # must not trip the check.  Falls back to the global count on an older .so.
    s = runloom_c.stats()
    return int(s.get("netpoll_parked_self", s["netpoll_parked"]))


def _settle_parked(baseline):
    """Return the parked count, giving in-flight teardown up to the settle
    deadline to bring it back down to <= baseline."""
    cur = _parked()
    if cur <= baseline:
        return cur
    deadline = time.monotonic() + _SETTLE_DEADLINE_S
    while time.monotonic() < deadline:
        time.sleep(_SETTLE_STEP_S)   # let background loop threads run + drain
        cur = _parked()
        if cur <= baseline:
            break
    return cur


@pytest.fixture(autouse=True)
def runloom_invariants(request):
    if _DISABLED or runloom_c is None:
        yield
        return

    baseline = _parked()
    del _UNRAISABLE[:]          # count only THIS test's swallowed errors
    yield

    # Don't mask a real test failure with a teardown invariant error.
    call_rep = getattr(request.node, "_pg_rep_call", None)
    if call_rep is not None and not call_rep.passed:
        return

    # (0) swallowed-error gate: an unraisable / thread-excepthook that fired
    # during this test (a raise on a dealloc / finalizer / hub-thread path that
    # cannot propagate) is a real fault, not benign -- surface it here.
    if (_UNRAISABLE
            and request.node.get_closest_marker("runloom_allow_unraisable") is None):
        caught = list(_UNRAISABLE)
        del _UNRAISABLE[:]
        msg = ("{0} error(s) swallowed on a dealloc/finalizer/hub-thread path "
               "during this test (sys.unraisablehook / threading.excepthook): "
               "{1}".format(len(caught), " | ".join(caught[:5])))
        if _REPORT_ONLY:
            sys.stderr.write("[runloom-unraisable] {0}::{1}: {2}\n".format(
                request.node.module.__name__, request.node.name, msg))
        else:
            pytest.fail(msg, pytrace=False)

    # (1) structural integrity -- always holds, cheap, no false positives.
    viol = runloom_c._self_check(0)
    assert viol == 0, (
        "runloom_c._self_check reported {0} violation(s) after this test "
        "(see stderr [runloom-diag] lines): netpoll/scheduler structures are "
        "inconsistent.".format(viol))

    # (2) leaked-parker delta.
    if request.node.get_closest_marker("runloom_leaky") is not None:
        return
    after = _settle_parked(baseline)
    delta = after - baseline
    if delta > 0:
        msg = ("leaked {0} netpoll parker(s): netpoll_parked was {1} before "
               "the test and {2} after (did not drain within {3}s). A "
               "fiber parked in wait_fd was never woken -- mark the test "
               "@pytest.mark.runloom_leaky if that is intentional.".format(
                   delta, baseline, after, _SETTLE_DEADLINE_S))
        if _REPORT_ONLY:
            sys.stderr.write("[runloom-leak] {0}::{1}: {2}\n".format(
                request.node.module.__name__, request.node.name, msg))
        else:
            pytest.fail(msg, pytrace=False)
