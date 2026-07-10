"""FAST bounded slice of the uvloop-style stdlib asyncio conformance (Pillar B).

The full sweep lives in ``tools/conformance/run_asyncio.py`` (13 submodules,
~2000 tests, ~40s) and in the forever soak; that is too heavy for the per-file
ceiling in ``tests/run_isolated.py``.  This file runs ONE small, fast submodule
-- CPython's OWN ``test.test_asyncio.test_locks`` (75 tests, <1s) -- against
``runloom.aio.RunloomEventLoop`` via the exact same hooks the runner uses (global
RunloomEventLoopPolicy + ``loop_factory`` pinned on each IsolatedAsyncioTestCase),
minus anything parked in ``asyncio_known_failures.txt``.  It is the standing,
in-suite regression guard that the conformance harness itself still wires up and
that test_locks stays green under runloom.

Skips cleanly if CPython's stdlib ``test`` package is absent (embedded / some
Windows builds).
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SRC = os.path.join(REPO, "src")
CONF = os.path.join(REPO, "tools", "conformance")
for p in (SRC, CONF):
    if p not in sys.path:
        sys.path.insert(0, p)

import conformance_lib as cl  # noqa: E402

pytestmark = pytest.mark.skipif(
    not cl.have_cpython_tests(),
    reason="CPython stdlib `test` package not installed on this interpreter")

# One small, fast submodule -- keep this BOUNDED so the file is safe under the
# run_isolated per-file ceiling.  (The heavy multi-module run is in tools/.)
SLICE_MODULE = "test_locks"
PREFIX = "test.test_asyncio."


def test_stdlib_asyncio_locks_under_runloom_loop():
    """CPython's test_asyncio.test_locks, verbatim, on a RunloomEventLoop, is
    green modulo asyncio_known_failures.txt."""
    loop_factory = cl.install_asyncio_policy()
    known = cl.load_known_failures(
        os.path.join(CONF, "asyncio_known_failures.txt"))

    __import__(PREFIX + SLICE_MODULE)
    module = sys.modules[PREFIX + SLICE_MODULE]
    patched = cl.patch_isolated_loop_factory(module, loop_factory)
    assert patched > 0, "no IsolatedAsyncioTestCase classes found to pin"

    stats = cl.run_module(module, known, PREFIX, verbosity=0)

    assert stats["ran"] > 0, "slice ran zero tests -- collection broke"
    assert not stats["red_short_ids"], (
        "genuine (non-known) asyncio conformance regressions under runloom: %s"
        % stats["red_short_ids"])
