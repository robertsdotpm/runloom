"""CPython asyncio BufferedProtocol conformance against RunloomEventLoop.

Companion to test_asyncio_conformance.py.  Runs CPython's OWN
test_buffered_proto.BaseTestBufferedProtocol verbatim (the get_buffer() ->
recv_into -> buffer_updated() read contract) against runloom.aio.RunloomEventLoop via
the suite's new_loop hook.

This was a documented gap (RunloomEventLoop's stream transport used to call
protocol.data_received even for an asyncio.BufferedProtocol; the buffered read
path was unimplemented).  Implemented in _StreamTransport._recv_step_buffered;
this is the standing regression guard for it.
"""
import sys
import unittest

import pytest

sys.path.insert(0, "src")

import runloom.aio as paio

try:
    from test.test_asyncio import test_buffered_proto as _tbp
    _HAVE_CPYTHON_TESTS = True
except ImportError:                   # stdlib `test` package not installed
    _HAVE_CPYTHON_TESTS = False

pytestmark = pytest.mark.skipif(
    not _HAVE_CPYTHON_TESTS,
    reason="CPython stdlib `test` package not installed on this interpreter")

if not _HAVE_CPYTHON_TESTS:
    class _tbp:                       # noqa: N801 - placeholder; module skipped
        class BaseTestBufferedProtocol:
            pass


class RunloomBufferedProtocolConformance(_tbp.BaseTestBufferedProtocol,
                                      unittest.TestCase):
    """CPython's BaseTestBufferedProtocol, driven by a RunloomEventLoop.

    Every test_* method is CPython's, unmodified; only the loop is runloom's."""

    def new_loop(self):
        return paio.RunloomEventLoop()


if __name__ == "__main__":
    unittest.main()
