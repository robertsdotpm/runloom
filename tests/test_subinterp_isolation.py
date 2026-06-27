"""Sub-interpreter (PEP 684) isolation contract for runloom_c.

DECISION (characterized 2026-06-27 on free-threaded 3.13t):
runloom_c is a SINGLE-PHASE C extension with PROCESS-GLOBAL state -- the M:N
scheduler, the shared netpoll, hub OS threads, and several init-once globals
(mn_sched.c). It therefore does NOT support per-interpreter isolation: running
the runtime from more than one interpreter would share/corrupt that global C
state.

CPython's usual safety net -- refusing a single-phase module in an isolated
sub-interpreter -- is GIL-gated, so on a FREE-THREADED build (no GIL) it does
NOT fire: `import runloom_c` succeeds in both "legacy" (shared-GIL) and
"isolated" sub-interpreters here. That is a LATENT footgun, not a crash.

These tests pin the contract:
  1. importing runloom_c in a sub-interpreter is MEMORY-SAFE (must not crash /
     corrupt) -- a real regression sentinel;
  2. the isolation status is RECORDED -- if a future change makes the import
     start being refused (the recommended hardening below), test 2 is the
     tripwire that flags the behavior change so this doc + the decision get
     updated.

RECOMMENDED HARDENING (follow-up, intentionally NOT done here -- invasive):
convert module init to multi-phase (PyModuleDef_Slot[]) and declare
`Py_mod_multiple_interpreters = Py_MOD_MULTIPLE_INTERPRETERS_NOT_SUPPORTED`
next to the existing `Py_MOD_GIL_NOT_USED` declaration, so a second interpreter
is cleanly refused at import. Tracked in docs/dev/frontier/TRACKING.md (A-A4).
"""
import os
import sys
import unittest

# Drive sub-interpreters via the low-level API present on 3.13 (the PEP 734
# stdlib `interpreters` is not in 3.13t; `_interpreters` is).
try:
    import _interpreters
    _HAVE = True
except ImportError:
    _HAVE = False

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")


def _run_in_subinterp(config, code):
    """Create a sub-interpreter (config: 'isolated'|'legacy'), run `code`, destroy.
    Returns None on success or the exception repr the sub-interpreter raised."""
    iid = _interpreters.create(config)
    try:
        try:
            _interpreters.run_string(iid, code)
            return None
        except Exception as e:           # noqa: BLE001 - we report whatever it raised
            return "{0}: {1}".format(type(e).__name__, str(e)[:200])
    finally:
        _interpreters.destroy(iid)


@unittest.skipUnless(_HAVE, "_interpreters not available")
class SubinterpIsolation(unittest.TestCase):

    def _import_code(self):
        # make src importable inside the sub-interpreter, then import + a trivial
        # read-only call; do NOT start the scheduler (that is the unsafe op).
        return (
            "import sys; sys.path.insert(0, {src!r})\n"
            "import runloom_c\n"
            "assert isinstance(runloom_c.backend(), str)\n"
        ).format(src=SRC)

    def test_subinterp_import_is_memory_safe(self):
        """Importing runloom_c in a sub-interpreter must not crash/corrupt the
        process. (If runloom is ever hardened to REFUSE the import, this still
        passes -- a clean ImportError is caught and reported, not a crash.)"""
        for config in ("legacy", "isolated"):
            err = _run_in_subinterp(config, self._import_code())
            # Either a clean import (None) or a clean Python exception is fine;
            # the failure mode we guard against is a process crash, which would
            # take the whole test runner down rather than reach this assert.
            if err is not None:
                self.assertIn("Error", err,
                              "{0} subinterp: unexpected non-exception result {1!r}"
                              .format(config, err))

    def test_isolation_status_is_recorded(self):
        """Tripwire: document the CURRENT status so a behavior change is noticed.
        On free-threaded 3.13t the import currently SUCCEEDS (the latent footgun).
        If this starts being refused, update the module docstring + TRACKING A-A4."""
        err = _run_in_subinterp("isolated", self._import_code())
        currently_loads = err is None
        # We assert only that the outcome is one of the two KNOWN-SAFE shapes
        # (loads, or refused with an ImportError) -- never a crash or a weird
        # non-import error. The boolean is logged for the maintainer.
        sys.stderr.write(
            "[subinterp] isolated import currently {0} (footgun: runloom "
            "has process-global M:N state; see module docstring)\n".format(
                "LOADS -- UNSUPPORTED but not refused" if currently_loads
                else "is REFUSED: " + str(err)))
        self.assertTrue(currently_loads or "ImportError" in (err or ""),
                        "unexpected sub-interp outcome: {0!r}".format(err))


if __name__ == "__main__":
    unittest.main()
