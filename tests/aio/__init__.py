"""Vendored CPython asyncio conformance suite, pinned + run on runloom's bridge.

The test_*.py files here are near-verbatim copies of CPython's Lib/test/test_asyncio
(PSF licence), pinned from CPython 3.14.4, edited ONLY to (a) rewrite intra-package
imports to relative (`from . import utils`) and (b) nothing else -- the loop is
swapped to runloom.aio.RunloomEventLoop centrally in conftest.py, so the bodies
stay diffable against upstream.

WHY vendored (vs the old dynamic tools/conformance runner): the bridge conformance
was being run to green and then not SAVED; this pins the actual test bodies + a
committed skip baseline (skips.py) so the green state is a self-contained,
re-runnable regression guard that does NOT depend on the pyenv shipping the stdlib
`test` package.

SCOPE: only the submodules that genuinely exercise RunloomEventLoop are vendored
(the IsolatedAsyncioTestCase suites + the real-I/O create_event_loop/new_loop
suites).  test_futures (fake TestLoop), test_selector_events (mock selector), and
test_transports/test_protocols (abstract/mock) are deliberately NOT vendored --
they run on a fake/stock loop and would prove nothing about runloom.

DEFAULT BRIDGE, no src changes: divergences on the runloom loop (unimplemented
loop transports, type-identity/repr, a few hangs) are SKIPPED via skips.py so the
suite is green as-is.  A NEW failure not in skips reds the suite.
"""
