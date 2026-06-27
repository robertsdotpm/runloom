"""Sub-interpreter (PEP 684) isolation contract for runloom_c.

CONTRACT (verified 2026-06-27 on free-threaded 3.13t):
runloom_c is a SINGLE-PHASE C extension with PROCESS-GLOBAL state (the M:N
scheduler, the shared netpoll, hub OS threads, init-once globals in mn_sched.c).
It must NOT be loaded into more than one interpreter -- two interpreters driving
the same global hub state would corrupt it (the one-owner-per-process assumption
the scheduler rests on).

CPython enforces exactly this for single-phase modules: importing one into a
sub-interpreter raises `ImportError: module <name> does not support loading in
subinterpreters`.  This test pins that the protection HOLDS -- in BOTH the
`isolated` (own-state) and `legacy` (shared) sub-interpreter configs, on the
free-threaded build.  It is a REGRESSION GUARD: if runloom is ever converted to
multi-phase init (PEP 489) without declaring
`Py_mod_multiple_interpreters = Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED`, the
import would start *succeeding* and this test would fail, flagging that the
process-global state is now reachable from a second interpreter.

HISTORY: an earlier characterization claimed "import LOADS in an isolated
sub-interpreter on FT (latent footgun)". That was a FALSE POSITIVE from an
unreliable test harness (`_interpreters.run_string` returning without raising was
misread as the sub-interpreter code succeeding; nested shell quoting also mangled
the probe). A file-based driver shows the import is in fact cleanly REFUSED, so
no guard code is needed -- CPython already protects us. This test uses the
reliable file channel.
"""
import os
import tempfile
import unittest

try:
    import _interpreters
    _HAVE = True
except ImportError:
    _HAVE = False

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")


def _import_runloom_in_subinterp(config):
    """Create a sub-interpreter of `config`, attempt `import runloom_c`, and
    return the outcome string the sub-interpreter wrote to a temp file.
    (A file is the reliable cross-interpreter channel: run_string swallows
    SystemExit and redirects std streams.)"""
    fd, res = tempfile.mkstemp(prefix="subinterp_", suffix=".txt")
    os.close(fd)
    code = (
        "import sys\n"
        "sys.path.insert(0, %r)\n" % SRC +
        "r = open(%r, 'w')\n" % res +
        "try:\n"
        "    import runloom_c\n"
        "    r.write('IMPORT_OK')\n"
        "except ImportError as e:\n"
        "    r.write('IMPORT_REFUSED:' + str(e)[:120])\n"
        "except BaseException as e:\n"
        "    r.write('IMPORT_ERR:' + type(e).__name__ + ':' + str(e)[:120])\n"
        "finally:\n"
        "    r.flush(); r.close()\n"
    )
    iid = _interpreters.create(config)
    try:
        _interpreters.run_string(iid, code)
    finally:
        _interpreters.destroy(iid)
    try:
        with open(res) as f:
            return f.read()
    finally:
        os.unlink(res)


@unittest.skipUnless(_HAVE, "_interpreters not available")
class SubinterpIsolation(unittest.TestCase):

    def test_import_refused_in_subinterpreters(self):
        """runloom_c (single-phase, process-global) MUST be refused in any
        sub-interpreter -- this is the protection against cross-interpreter
        corruption of the global M:N scheduler."""
        for config in ("isolated", "legacy"):
            outcome = _import_runloom_in_subinterp(config)
            self.assertTrue(
                outcome.startswith("IMPORT_REFUSED"),
                "{0} sub-interpreter: expected runloom_c import to be REFUSED "
                "(single-phase protection), got {1!r}. If runloom was made "
                "multi-phase, declare Py_mod_multiple_interpreters=NOT_SUPPORTED."
                .format(config, outcome))
            self.assertIn("subinterpreters", outcome,
                          "{0}: refused, but not for the expected reason: {1!r}"
                          .format(config, outcome))

    def test_main_interpreter_unaffected(self):
        """Sanity: runloom_c imports + initializes fine in the main interpreter."""
        import runloom_c
        runloom_c.mn_init(2)
        runloom_c.mn_fini()


if __name__ == "__main__":
    unittest.main()
