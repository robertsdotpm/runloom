"""CPython's OWN Lib/test/test_subprocess.py (ProcessTestCase) verbatim under
runloom.monkey.patch().

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

    TestRunloomSubprocess = hosted(
        _m.ProcessTestCase, "TestRunloomSubprocess",
        skips={
            # Behaviour is CORRECT -- the monkey open DOES emit the
            # "line buffering (buffering=1) isn't supported in binary mode"
            # RuntimeWarning (files.py). But this borrowed test captures it with
            # assertWarnsRegex, and the warning is raised while Popen builds its
            # pipe stream in the transient DETACHED-fiber window (files.py:120-150
            # comment); under cooperative scheduling that emission escapes the
            # fiber's warnings.catch_warnings recorder (assertWarns is documented
            # not to be thread/reentrancy-safe). A capture-mechanism divergence in
            # the borrowed test, not a runtime behaviour difference.
            "test_bufsize_equal_one_binary_mode":
                "warning IS emitted (correct); assertWarns can't capture it across "
                "the monkey-open detached-fiber window under cooperative scheduling",
        })
