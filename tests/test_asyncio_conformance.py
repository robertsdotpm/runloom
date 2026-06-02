"""CPython asyncio conformance: run CPython's OWN test_asyncio test bodies
against pygo's event loop (pygo.aio.PygoEventLoop).

The user asked about "CPython's socket tests".  The stdlib ``socket`` module
isn't something pygo replaces, but pygo DOES ship an asyncio event loop, so the
canonical conformance suite is CPython's ``Lib/test/test_asyncio`` -- and it is
importable on this build.  Rather than re-author those scenarios, this runs
CPython's real ``BaseSockTestsMixin`` (test_sock_lowlevel.py: the low-level
loop.sock_recv / sock_sendall / sock_connect / sock_accept / sock_recvfrom /
huge-content backpressure tests -- exactly pygo's loop primitives) verbatim,
with the loop swapped to PygoEventLoop via the suite's own ``create_event_loop``
hook.  If a future change regresses pygo.aio's sock layer, CPython's own
assertions turn red here.

CONFORMANCE (updated 2026-06-02 after fixing the gaps it surfaced):

  test_sock_lowlevel.BaseSockTestsMixin -- 13 / 13 PASS verbatim.

  The first run was 10/13; the 3 failures it found were small, real pygo.aio
  gaps, since fixed:
    * sock_* now enforce asyncio's "socket must be non-blocking" precondition
      in debug mode (ValueError), matching CPython.
    * sock_accept now returns a non-blocking accepted socket (gettimeout()==0).

  Companion file test_asyncio_bufferedproto_conformance.py covers CPython's
  BaseTestBufferedProtocol after the get_buffer()/buffer_updated() path was
  implemented.
"""
import sys
import unittest

import pytest

sys.path.insert(0, "src")

import pygo.aio as paio

# CPython's own asyncio test machinery.  Not every interpreter ships the
# stdlib `test` package (e.g. some manually-installed/embedded Windows builds);
# skip cleanly there instead of erroring at collection.
try:
    from test.test_asyncio import test_sock_lowlevel as _tsl
    from test.test_asyncio import utils as _test_utils
    _HAVE_CPYTHON_TESTS = True
except ImportError:
    _HAVE_CPYTHON_TESTS = False

pytestmark = pytest.mark.skipif(
    not _HAVE_CPYTHON_TESTS,
    reason="CPython stdlib `test` package not installed on this interpreter")

if not _HAVE_CPYTHON_TESTS:
    class _tsl:                       # noqa: N801 - placeholder so the class
        class BaseSockTestsMixin:     # body below still parses when test is
            pass                      # absent (the whole module is skipped)
    class _test_utils:                # noqa: N801
        class TestCase:
            pass


# Methods that fail against PygoEventLoop for a *characterised* reason -- each
# is a small, real pygo.aio conformance gap, skipped (not silenced) with the
# exact divergence so it reads as a TODO, not a mystery.
#
# (Previously this listed test_sock_client_ops, test_unix_sock_client_ops and
# test_sock_accept; all three were FIXED -- sock_* now enforce asyncio's
# non-blocking precondition in debug mode and sock_accept returns a non-blocking
# socket -- so BaseSockTestsMixin is now 13/13.)
_KNOWN_GAPS = {}


class PygoSockLowlevelConformance(_tsl.BaseSockTestsMixin, _test_utils.TestCase):
    """CPython's BaseSockTestsMixin, driven by a PygoEventLoop.

    Every test_* method here is CPython's, unmodified; only the loop under test
    is pygo's.  The known-gap methods are replaced by skips below."""

    def create_event_loop(self):
        return paio.PygoEventLoop()


# Replace the known-gap methods with documented skips so the file stays green
# while still running every conformant CPython test body.
def _make_skip(reason):
    @unittest.skip(reason)
    def _skipped(self):
        pass
    return _skipped


for _name, _reason in _KNOWN_GAPS.items():
    setattr(PygoSockLowlevelConformance, _name, _make_skip(_reason))


if __name__ == "__main__":
    unittest.main()
