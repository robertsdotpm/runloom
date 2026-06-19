"""Regression: a module attribute MISS inside a fiber must raise a clean
AttributeError, never crash.

CPython 3.13's module getattr, on a miss, calls _PyModule_IsPossiblyShadowing to
append a "did you shadow a stdlib module?" hint to the AttributeError.  That
helper reserves ~32 KB of C stack (two wchar_t[MAXPATHLEN] path buffers) -- more
than a whole default fiber stack -- so an ordinary attribute miss
(hasattr / getattr feature-detection, a namespace __getattr__ proxy) inside a
fiber used to overflow the stack and SIGSEGV.

runloom replaces PyModule_Type's getattr slot to skip that hint while running on a
fiber's small stack (the AttributeError itself -- type, .name/.obj, message
core -- is unchanged).  See src/runloom_c/module_init.c.inc.
"""
import os
import sys
import tempfile
import types
import unittest

import runloom_c

MODNAME = "runloom_modmiss_mod"


def _drive(fn):
    """Run fn() inside a single-thread fiber; re-raise anything it raised."""
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:   # noqa: BLE001
            box[1] = e

    runloom_c.fiber(runner)
    runloom_c.run()
    if box[1] is not None:
        raise box[1]
    return box[0]


class TestModuleGetattrGoroutine(unittest.TestCase):
    def setUp(self):
        # A module WITH a __file__ is what makes CPython attempt the 32 KB
        # shadowing hint on a miss (the path that overflows the fiber stack).
        fd, self.path = tempfile.mkstemp(suffix=".py", prefix="runloom_modmiss_")
        os.close(fd)
        self.mod = types.ModuleType(MODNAME)
        self.mod.__file__ = self.path
        self.mod.present = 123
        sys.modules[MODNAME] = self.mod

    def tearDown(self):
        sys.modules.pop(MODNAME, None)
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def test_miss_in_fiber_raises_attributeerror(self):
        def body():
            with self.assertRaises(AttributeError):
                getattr(self.mod, "definitely_missing")
            return "ok"
        self.assertEqual(_drive(body), "ok")

    def test_hit_in_fiber_still_works(self):
        self.assertEqual(_drive(lambda: getattr(self.mod, "present")), 123)

    def test_hasattr_miss_in_fiber(self):
        self.assertIs(_drive(lambda: hasattr(self.mod, "nope")), False)

    def test_module_level_getattr_function_honoured(self):
        # PEP 562 module __getattr__ must still be called on a miss (in-fiber).
        seen = []

        def mod_getattr(name):
            seen.append(name)
            if name == "magic":
                return "conjured"
            raise AttributeError(name)

        self.mod.__getattr__ = mod_getattr
        self.assertEqual(_drive(lambda: getattr(self.mod, "magic")), "conjured")
        with self.assertRaises(AttributeError):
            _drive(lambda: getattr(self.mod, "still_missing"))
        self.assertIn("magic", seen)

    def test_miss_under_mn_scheduler(self):
        box = [None]

        def runner():
            try:
                getattr(self.mod, "missing_mn")
                box[0] = "NO ERROR"
            except AttributeError:
                box[0] = "ok"

        runloom_c.mn_init(2)
        try:
            runloom_c.mn_fiber(runner)
            runloom_c.mn_run()
        finally:
            runloom_c.mn_fini()
        self.assertEqual(box[0], "ok")

    def test_subclass_getattr_miss(self):
        # A ModuleType subclass with a class-level __getattr__ reaches stock
        # module getattr via slot_tp_getattr_hook -> the __getattribute__ wrapper
        # descriptor, NOT the tp_getattro slot.  Must still be safe.
        class _NS(types.ModuleType):
            def __getattr__(self, name):
                raise AttributeError(name)

        m = _NS("runloom_modmiss_subcls")
        m.__file__ = self.path

        def body():
            with self.assertRaises(AttributeError):
                m.definitely_missing
            return "ok"
        self.assertEqual(_drive(body), "ok")

    def test_explicit_getattribute_miss(self):
        # Calling module.__getattribute__('missing') directly also goes through
        # the wrapper descriptor, bypassing the slot.  Must still be safe.
        def body():
            with self.assertRaises(AttributeError):
                type(self.mod).__getattribute__(self.mod, "definitely_missing")
            return "ok"
        self.assertEqual(_drive(body), "ok")


if __name__ == "__main__":
    unittest.main()
