"""Goroutine registry + dump (pygo.inspect / pygo_core introspection)."""
import io
import os
import sys
import tempfile
import unittest

import pytest

sys.path.insert(0, "src")

import pygo
import pygo_core
import pygo.inspect as gi

# Goroutine introspection is POSIX-only (pygo_introspect.c is wrapped in
# #if !defined(_WIN32)); the C functions aren't built on Windows, so skip
# wherever the API is absent rather than hardcoding a platform.
pytestmark = pytest.mark.skipif(
    not hasattr(pygo_core, "goroutine_count"),
    reason="goroutine introspection is POSIX-only (not built on this platform)")


class TestCountAndRegistry(unittest.TestCase):
    def test_count_zero_when_idle(self):
        self.assertEqual(pygo_core.goroutine_count(), 0)
        self.assertEqual(pygo_core.goroutines(), [])

    def test_count_tracks_live_goroutines(self):
        seen = {}

        def sleeper():
            pygo.sleep(0.03)

        def main():
            for _ in range(5):
                pygo.go(sleeper)
            pygo.sleep(0.005)            # let them park
            seen["count"] = pygo_core.goroutine_count()
            seen["states"] = [g["state"] for g in pygo_core.goroutines()]

        pygo.run(main)
        # 5 sleepers + main itself
        self.assertEqual(seen["count"], 6)
        self.assertEqual(seen["states"].count("sleep"), 5)
        # everything drained -> registry reports zero live again
        self.assertEqual(pygo_core.goroutine_count(), 0)

    def test_registry_balances_under_churn(self):
        def noop():
            return 1

        def main():
            for _ in range(2000):
                pygo.go(noop)
                pygo.yield_()

        pygo.run(main)
        self.assertEqual(pygo_core.goroutine_count(), 0)

    def test_ids_are_unique(self):
        ids = {}

        def sleeper():
            pygo.sleep(0.02)

        def main():
            for _ in range(8):
                pygo.go(sleeper)
            pygo.sleep(0.005)
            ids["set"] = [g["id"] for g in pygo_core.goroutines()]

        pygo.run(main)
        got = ids["set"]
        self.assertEqual(len(got), len(set(got)))   # all unique
        self.assertTrue(all(i > 0 for i in got))


class TestStates(unittest.TestCase):
    def test_sleep_state_and_wake_in(self):
        cap = {}

        def sleeper():
            pygo.sleep(0.05)

        def main():
            pygo.go(sleeper)
            pygo.sleep(0.005)
            g = [x for x in pygo_core.goroutines() if x["state"] == "sleep"][0]
            cap["wake_in"] = g["wake_in"]
            cap["blocked_on"] = g["blocked_on"]

        pygo.run(main)
        self.assertIsNotNone(cap["wake_in"])
        self.assertGreater(cap["wake_in"], 0.0)
        self.assertEqual(cap["blocked_on"], "timer")

    def test_io_wait_reports_fd(self):
        r, w = os.pipe()
        cap = {}
        try:
            def waiter():
                pygo_core.wait_fd(r, 1, -1)     # park on readable
                os.read(r, 1)

            def main():
                pygo.go(waiter)
                pygo.sleep(0.01)
                iow = [g for g in pygo_core.goroutines()
                       if g["state"] == "io-wait"]
                cap["iow"] = iow
                os.write(w, b"x")               # wake it -> drains
                pygo.sleep(0.01)

            pygo.run(main)
        finally:
            os.close(r)
            os.close(w)
        self.assertEqual(len(cap["iow"]), 1)
        self.assertEqual(cap["iow"][0]["fd"], r)
        self.assertEqual(cap["iow"][0]["events"], "R")
        self.assertEqual(cap["iow"][0]["blocked_on"], "io")


class TestStackReconstruction(unittest.TestCase):
    def test_full_stack_of_parked_goroutine(self):
        cap = {}

        def leaf():
            pygo.sleep(0.05)

        def middle():
            leaf()

        def top():
            middle()

        def main():
            pygo.go(top)
            pygo.sleep(0.01)
            gid = [g for g in pygo_core.goroutines()
                   if g["state"] == "sleep"][0]["id"]
            cap["frames"] = gi.stack(gid)
            cap["entry"] = gi.entry(gid)

        pygo.run(main)
        # co_qualname is fully-qualified (Class.method.<locals>.leaf); match
        # on suffix.  single-thread scheduler -> full user stack, deepest first.
        funcs = [name for (_fn, _ln, name) in cap["frames"]]
        self.assertTrue(any(n.endswith("leaf") for n in funcs), funcs)
        self.assertTrue(any(n.endswith("middle") for n in funcs), funcs)
        self.assertTrue(any(n.endswith("top") for n in funcs), funcs)
        # deepest frame is the pygo.sleep internal; user frames follow
        self.assertEqual(funcs[0], "sleep")
        self.assertIn("top", cap["entry"])


class TestAge(unittest.TestCase):
    def test_age_tracking_opt_in(self):
        cap = {}
        gi.enable_timestamps(True)
        try:
            def sleeper():
                pygo.sleep(0.05)

            def main():
                pygo.go(sleeper)
                pygo.sleep(0.02)
                g = [x for x in pygo_core.goroutines()
                     if x["state"] == "sleep"][0]
                cap["age"] = g["age"]

            pygo.run(main)
        finally:
            gi.enable_timestamps(False)
        self.assertIsNotNone(cap["age"])
        self.assertGreaterEqual(cap["age"], 0.0)


class TestDump(unittest.TestCase):
    def test_dump_goroutines_fd_writes(self):
        # C structural dump to a temp fd (the signal-safe path).
        out = {}

        def sleeper():
            pygo.sleep(0.03)

        def main():
            pygo.go(sleeper)
            pygo.sleep(0.005)
            fd, path = tempfile.mkstemp()
            out["path"] = path
            pygo_core.dump_goroutines(fd)
            os.close(fd)

        pygo.run(main)
        with open(out["path"]) as f:
            text = f.read()
        os.unlink(out["path"])
        self.assertIn("goroutine dump", text)
        self.assertIn("sleep", text)

    def test_inspect_format_string(self):
        cap = {}

        def sleeper():
            pygo.sleep(0.03)

        def main():
            pygo.go(sleeper)
            pygo.sleep(0.005)
            cap["text"] = gi.format(stacks=True)

        pygo.run(main)
        self.assertIn("pygo goroutines:", cap["text"])
        self.assertIn("sleep", cap["text"])

    def test_dump_to_file_object(self):
        cap = {}

        def sleeper():
            pygo.sleep(0.03)

        def main():
            pygo.go(sleeper)
            pygo.sleep(0.005)
            buf = io.StringIO()
            gi.dump(file=buf, stacks=True)
            cap["text"] = buf.getvalue()

        pygo.run(main)
        self.assertIn("goroutine", cap["text"])


import subprocess


def _run_script(code, env_extra=None):
    """Run `code` in a fresh interpreter (full isolation: a deadlock leaves
    goroutines parked, which mustn't pollute the test process).  Returns
    (returncode, stdout+stderr)."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYGO_SYSMON"] = "0"
    env["PYTHONPATH"] = "src" + os.pathsep + env.get("PYTHONPATH", "")
    if env_extra:
        env.update(env_extra)
    p = subprocess.run([sys.executable, "-c", code], env=env,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                       timeout=30)
    return p.returncode, p.stdout.decode("utf-8", "replace")


class TestDeadlockDetection(unittest.TestCase):
    def test_mode_get_set(self):
        old = gi.deadlock_mode()
        try:
            gi.set_deadlock_mode("off")
            self.assertEqual(gi.deadlock_mode(), "off")
            gi.set_deadlock_mode("raise")
            self.assertEqual(gi.deadlock_mode(), "raise")
            gi.set_deadlock_mode("warn")
            self.assertEqual(gi.deadlock_mode(), "warn")
        finally:
            gi.set_deadlock_mode(old)

    def test_count_deadlocked_zero_when_idle(self):
        self.assertEqual(pygo_core.count_deadlocked(), 0)

    def test_raise_end_to_end(self):
        rc, out = _run_script(
            "import pygo, pygo_core, pygo.inspect as gi\n"
            "gi.set_deadlock_mode('raise')\n"
            "try:\n"
            "    pygo.run(lambda: pygo_core.Chan(0).recv())\n"
            "    print('NO_RAISE')\n"
            "except RuntimeError as e:\n"
            "    print('RAISED_OK' if 'deadlock' in str(e).lower() else 'WRONG')\n")
        self.assertIn("RAISED_OK", out)
        self.assertIn("DEADLOCK", out)            # the dump printed too

    def test_warn_is_non_fatal(self):
        rc, out = _run_script(
            "import pygo, pygo_core, pygo.inspect as gi\n"
            "gi.set_deadlock_mode('warn')\n"
            "def waiter(): pygo_core.Chan(0).recv()\n"
            "def main():\n"
            "    pygo.go(waiter)\n"
            "    pygo_core.Chan(0).recv()\n"
            "pygo.run(main)\n"            # warn -> prints, no raise
            "print('SURVIVED')\n")
        self.assertEqual(rc, 0)
        self.assertIn("SURVIVED", out)
        self.assertIn("DEADLOCK", out)
        self.assertIn("chan-wait", out)

    def test_off_mode_silent(self):
        rc, out = _run_script(
            "import pygo, pygo_core, pygo.inspect as gi\n"
            "gi.set_deadlock_mode('off')\n"
            "pygo.run(lambda: pygo_core.Chan(0).recv())\n"
            "print('SURVIVED')\n")
        self.assertEqual(rc, 0)
        self.assertIn("SURVIVED", out)
        self.assertNotIn("DEADLOCK", out)

    def test_no_false_positive_on_clean_run(self):
        rc, out = _run_script(
            "import pygo, pygo.inspect as gi\n"
            "gi.set_deadlock_mode('raise')\n"
            "def worker():\n"
            "    [pygo.yield_() for _ in range(3)]\n"
            "def main():\n"
            "    [pygo.go(worker) for _ in range(5)]\n"
            "    pygo.sleep(0.005)\n"
            "pygo.run(main)\n"            # completes -> no deadlock
            "print('CLEAN_OK')\n")
        self.assertEqual(rc, 0)
        self.assertIn("CLEAN_OK", out)
        self.assertNotIn("DEADLOCK", out)


class TestOutsideGoroutine(unittest.TestCase):
    def test_apis_safe_when_idle(self):
        # No scheduler running: must not crash.
        self.assertEqual(pygo_core.goroutine_count(), 0)
        self.assertEqual(pygo_core.goroutines(), [])
        rep, frames = pygo_core.goroutine_stack(999999)
        self.assertIsNone(rep)
        self.assertEqual(frames, [])


if __name__ == "__main__":
    unittest.main()
