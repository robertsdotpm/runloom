"""CPython asyncio start_server lifecycle conformance against PygoEventLoop.

Companion to test_asyncio_conformance.py.  Runs CPython's OWN
test_server.BaseStartServer verbatim against pygo.aio.PygoEventLoop via the
suite's new_loop hook.  It exercises the full server lifecycle:
create_server(start_serving=False) -> is_serving() False -> serve_forever()
starts accepting -> cancel -> close()/wait_closed() -> serve_forever() on a
closed server raises.

This was a documented gap (pygo's create_server ignored start_serving and began
accepting in __init__, so is_serving() was True immediately).  Fixed in
_ProtocolServer (start_serving / _start_accepting / is_serving / serve_forever);
this is the standing regression guard.
"""
import sys
import unittest

sys.path.insert(0, "src")

import pygo.aio as paio

from test.test_asyncio import test_server as _tsrv


class PygoStartServerConformance(_tsrv.BaseStartServer, unittest.TestCase):
    """CPython's BaseStartServer, driven by a PygoEventLoop.

    Every test_* method is CPython's, unmodified; only the loop is pygo's."""

    def new_loop(self):
        return paio.PygoEventLoop()


if __name__ == "__main__":
    unittest.main()
