"""CPython's OWN Lib/test/test_subprocess.py (ProcessTestCase) verbatim under
pygo.monkey.patch().

monkey makes ``subprocess.Popen.wait`` cooperative (via the cooperative
``selectors`` + ``os`` categories) and routes the pipe I/O through the
cooperative path.  CPython's core ProcessTestCase exercises Popen lifecycle,
``wait`` / ``communicate``, pipe stdin/stdout/stderr, timeouts and the
write-pipe backpressure path -- exactly the new cooperative subprocess surface.
"""
import pytest

from _monkey_stdlib import HAVE_CPYTHON_TESTS, hosted, patch_module, unpatch_module

pytestmark = pytest.mark.skipif(
    not HAVE_CPYTHON_TESTS,
    reason="CPython stdlib `test` package not installed on this interpreter")

setUpModule = patch_module
tearDownModule = unpatch_module

if HAVE_CPYTHON_TESTS:
    from test import test_subprocess as _m

    TestPygoSubprocess = hosted(_m.ProcessTestCase, "TestPygoSubprocess")
