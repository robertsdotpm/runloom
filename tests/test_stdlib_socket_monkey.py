"""CPython's OWN Lib/test/test_socket.py, run verbatim under pygo.monkey.patch().

monkey makes blocking socket recv/send/recvfrom/sendto/accept/connect (and
recvmsg/sendmsg) cooperative.  We host:

  * GeneralModuleTests -- the socket API/option/name-resolution surface
    (mostly no blocking I/O; checks the cooperative wrappers didn't change the
    observable contract);
  * BasicTCPTest / BasicUDPTest -- real client/server data transfer (the server
    side runs on a real OS thread; the client side is the cooperative path).

Complements the hand-adapted tests/test_socket_compat.py with CPython's own
much broader assertions.
"""
import pytest

from _monkey_stdlib import (HAVE_CPYTHON_TESTS, REALTHREAD, REALTHREAD_REASON,
                            hosted, patch_module, unpatch_module)

pytestmark = [
    pytest.mark.skipif(
        not HAVE_CPYTHON_TESTS,
        reason="CPython stdlib `test` package not installed on this interpreter"),
    pytest.mark.skipif(not REALTHREAD, reason=REALTHREAD_REASON),
]

setUpModule = patch_module
tearDownModule = unpatch_module

if HAVE_CPYTHON_TESTS:
    from test import test_socket as _m

    TestPygoSocketGeneral = hosted(_m.GeneralModuleTests, "TestPygoSocketGeneral")
    if hasattr(_m, "BasicTCPTest"):
        TestPygoSocketBasicTCP = hosted(_m.BasicTCPTest, "TestPygoSocketBasicTCP")
    if hasattr(_m, "BasicUDPTest"):
        TestPygoSocketBasicUDP = hosted(_m.BasicUDPTest, "TestPygoSocketBasicUDP")
