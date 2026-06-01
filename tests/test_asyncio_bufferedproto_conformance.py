"""CPython asyncio BufferedProtocol conformance against PygoEventLoop.

Companion to test_asyncio_conformance.py.  Runs CPython's OWN
test_buffered_proto.BaseTestBufferedProtocol verbatim (the get_buffer() ->
recv_into -> buffer_updated() read contract) against pygo.aio.PygoEventLoop via
the suite's new_loop hook.

This was a documented gap (PygoEventLoop's stream transport used to call
protocol.data_received even for an asyncio.BufferedProtocol; the buffered read
path was unimplemented).  Implemented in _StreamTransport._recv_step_buffered;
this is the standing regression guard for it.
"""
import sys
import unittest

sys.path.insert(0, "src")

import pygo.aio as paio

from test.test_asyncio import test_buffered_proto as _tbp


class PygoBufferedProtocolConformance(_tbp.BaseTestBufferedProtocol,
                                      unittest.TestCase):
    """CPython's BaseTestBufferedProtocol, driven by a PygoEventLoop.

    Every test_* method is CPython's, unmodified; only the loop is pygo's."""

    def new_loop(self):
        return paio.PygoEventLoop()


if __name__ == "__main__":
    unittest.main()
