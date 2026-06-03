"""CPython asyncio start_server lifecycle conformance against RunloomEventLoop.

Companion to test_asyncio_conformance.py.  Runs CPython's OWN
test_server.BaseStartServer verbatim against runloom.aio.RunloomEventLoop via the
suite's new_loop hook.  It exercises the full server lifecycle:
create_server(start_serving=False) -> is_serving() False -> serve_forever()
starts accepting -> cancel -> close()/wait_closed() -> serve_forever() on a
closed server raises.

This was a documented gap (runloom's create_server ignored start_serving and began
accepting in __init__, so is_serving() was True immediately).  Fixed in
_ProtocolServer (start_serving / _start_accepting / is_serving / serve_forever);
this is the standing regression guard.
"""
import sys
import unittest

import pytest

sys.path.insert(0, "src")

import runloom.aio as paio

try:
    from test.test_asyncio import test_server as _tsrv
    _HAVE_CPYTHON_TESTS = True
except ImportError:                   # stdlib `test` package not installed
    _HAVE_CPYTHON_TESTS = False

pytestmark = pytest.mark.skipif(
    not _HAVE_CPYTHON_TESTS,
    reason="CPython stdlib `test` package not installed on this interpreter")

if not _HAVE_CPYTHON_TESTS:
    class _tsrv:                       # noqa: N801 - placeholder; module skipped
        class BaseStartServer:
            pass


class RunloomStartServerConformance(_tsrv.BaseStartServer, unittest.TestCase):
    """CPython's BaseStartServer, driven by a RunloomEventLoop.

    Every test_* method is CPython's, unmodified; only the loop is runloom's."""

    def new_loop(self):
        return paio.RunloomEventLoop()


if __name__ == "__main__":
    unittest.main()
