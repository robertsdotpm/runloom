"""Per-test invariant checks for the pygo suite.

Every test in this directory runs through an autouse fixture that, AFTER the
test body, asserts two things about the C runtime:

  1. ``pygo_core._self_check(0) == 0`` -- a structural walk of every live
     scheduler / netpoll data structure (no list cycle, no self-looping
     per-fd bucket, the atomic parked count matches the walked count, no
     bucket entry missing from the global list).  This is a pure consistency
     invariant: it holds regardless of how much work a test left behind, so
     it never false-positives on a legitimately-busy background loop thread.

  2. No *leaked* netpoll parker.  We snapshot ``stats()['netpoll_parked']``
     before the test and re-read it after; a goroutine that parked in
     ``wait_fd`` and never got woken (the cross-thread-drain / leaked-parker
     class of bug) shows up as a count that never settles back.  A short
     settle window absorbs teardown races where a background thread is about
     to drain its own parker.

Why this exists: in practice a leaked parker did not fail the test that
caused it -- it wedged an *unrelated* ``pygo_core.run()`` several files later,
which is brutal to bisect.  Attributing the leak to the test that created it
(via a per-test before/after delta) turns "the suite hangs sometimes" into
"this one test leaked a parker."

Opt out with ``@pytest.mark.pygo_leaky`` for a test that deliberately leaves a
parker behind (e.g. the regression that proves a leaked parker no longer
wedges other threads).

Env knobs:
  PYGO_TEST_LEAK_REPORT=1  -- print the per-test parked delta instead of
                              failing on it (survey mode; self_check still
                              hard-asserts).
  PYGO_TEST_NO_INVARIANTS=1 -- disable the fixture entirely.
"""
import os
import sys
import time

# Match run_tests.py / test_mn.py: test the in-tree .so, not whatever else
# might be on the path.  Harmless if pygo_core is already imported (Python
# caches the module, so the fixture inspects the same runtime the tests use).
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(REPO, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pytest

try:
    import pygo_core
except Exception:  # pragma: no cover - pygo_core should always import here
    pygo_core = None

_REPORT_ONLY = os.environ.get("PYGO_TEST_LEAK_REPORT") == "1"
_DISABLED = os.environ.get("PYGO_TEST_NO_INVARIANTS") == "1"

# How long to let a background thread finish draining its own parker before we
# call a non-zero delta a real leak.  Real leaks never drain, so this only
# costs wall-clock on a genuine failure or a slow teardown.
_SETTLE_DEADLINE_S = 0.5
_SETTLE_STEP_S = 0.01


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "pygo_leaky: test deliberately leaves a netpoll parker behind; "
        "skip the post-test parked-leak invariant for it.")


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    # Stash each phase's report on the item so the fixture teardown can tell
    # whether the test body itself failed (in which case piling a leak error
    # on top is just noise -- the real failure already explains it).
    rep = yield
    setattr(item, "_pg_rep_" + rep.when, rep)
    return rep


def _parked():
    return int(pygo_core.stats()["netpoll_parked"])


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
def pygo_invariants(request):
    if _DISABLED or pygo_core is None:
        yield
        return

    baseline = _parked()
    yield

    # Don't mask a real test failure with a teardown invariant error.
    call_rep = getattr(request.node, "_pg_rep_call", None)
    if call_rep is not None and not call_rep.passed:
        return

    # (1) structural integrity -- always holds, cheap, no false positives.
    viol = pygo_core._self_check(0)
    assert viol == 0, (
        "pygo_core._self_check reported {0} violation(s) after this test "
        "(see stderr [pygo-diag] lines): netpoll/scheduler structures are "
        "inconsistent.".format(viol))

    # (2) leaked-parker delta.
    if request.node.get_closest_marker("pygo_leaky") is not None:
        return
    after = _settle_parked(baseline)
    delta = after - baseline
    if delta > 0:
        msg = ("leaked {0} netpoll parker(s): netpoll_parked was {1} before "
               "the test and {2} after (did not drain within {3}s). A "
               "goroutine parked in wait_fd was never woken -- mark the test "
               "@pytest.mark.pygo_leaky if that is intentional.".format(
                   delta, baseline, after, _SETTLE_DEADLINE_S))
        if _REPORT_ONLY:
            sys.stderr.write("[pygo-leak] {0}::{1}: {2}\n".format(
                request.node.module.__name__, request.node.name, msg))
        else:
            pytest.fail(msg, pytrace=False)
