"""Goroutine registry + dump (runloom.inspect / runloom_c introspection)."""
import io
import os
import sys
import tempfile
import unittest

import pytest

sys.path.insert(0, "src")

import runloom
import runloom_c
import runloom.inspect as gi

# Goroutine introspection is POSIX-only (runloom_introspect.c is wrapped in
# #if !defined(_WIN32)); the C functions aren't built on Windows, so skip
# wherever the API is absent rather than hardcoding a platform.
pytestmark = pytest.mark.skipif(
    not hasattr(runloom_c, "fiber_count"),
    reason="fiber introspection is POSIX-only (not built on this platform)")


class TestCountAndRegistry(unittest.TestCase):
    def test_count_zero_when_idle(self):
        self.assertEqual(runloom_c.fiber_count(), 0)
        self.assertEqual(runloom_c.fibers(), [])

    def test_count_tracks_live_fibers(self):
        seen = {}

        def sleeper():
            runloom.sleep(0.03)

        def main():
            for _ in range(5):
                runloom.fiber(sleeper)
            runloom.sleep(0.005)            # let them park
            seen["count"] = runloom_c.fiber_count()
            seen["states"] = [g["state"] for g in runloom_c.fibers()]

        runloom.run(1, main)
        # 5 sleepers + main itself
        self.assertEqual(seen["count"], 6)
        self.assertEqual(seen["states"].count("sleep"), 5)
        # everything drained -> registry reports zero live again
        self.assertEqual(runloom_c.fiber_count(), 0)

    def test_registry_balances_under_churn(self):
        def noop():
            return 1

        def main():
            for _ in range(2000):
                runloom.fiber(noop)
                runloom.yield_()

        runloom.run(1, main)
        self.assertEqual(runloom_c.fiber_count(), 0)

    def test_ids_are_unique(self):
        ids = {}

        def sleeper():
            runloom.sleep(0.02)

        def main():
            for _ in range(8):
                runloom.fiber(sleeper)
            runloom.sleep(0.005)
            ids["set"] = [g["id"] for g in runloom_c.fibers()]

        runloom.run(1, main)
        got = ids["set"]
        self.assertEqual(len(got), len(set(got)))   # all unique
        self.assertTrue(all(i > 0 for i in got))


class TestStates(unittest.TestCase):
    def test_sleep_state_and_wake_in(self):
        cap = {}

        def sleeper():
            runloom.sleep(0.05)

        def main():
            runloom.fiber(sleeper)
            runloom.sleep(0.005)
            g = [x for x in runloom_c.fibers() if x["state"] == "sleep"][0]
            cap["wake_in"] = g["wake_in"]
            cap["blocked_on"] = g["blocked_on"]

        runloom.run(1, main)
        self.assertIsNotNone(cap["wake_in"])
        self.assertGreater(cap["wake_in"], 0.0)
        self.assertEqual(cap["blocked_on"], "timer")

    @pytest.mark.skipif(sys.platform == "win32",
                        reason="pipe fds aren't pollable by the Windows netpoll "
                               "(no io-wait park); socket I/O covers it instead")
    def test_io_wait_reports_fd(self):
        r, w = os.pipe()
        cap = {}
        try:
            def waiter():
                runloom_c.wait_fd(r, 1, -1)     # park on readable
                os.read(r, 1)

            def main():
                runloom.fiber(waiter)
                runloom.sleep(0.01)
                iow = [g for g in runloom_c.fibers()
                       if g["state"] == "io-wait"]
                cap["iow"] = iow
                os.write(w, b"x")               # wake it -> drains
                runloom.sleep(0.01)

            runloom.run(1, main)
        finally:
            os.close(r)
            os.close(w)
        self.assertEqual(len(cap["iow"]), 1)
        self.assertEqual(cap["iow"][0]["fd"], r)
        self.assertEqual(cap["iow"][0]["events"], "R")
        self.assertEqual(cap["iow"][0]["blocked_on"], "io")


class TestStackReconstruction(unittest.TestCase):
    @pytest.mark.skipif(sys.version_info < (3, 13),
                        reason="interpreter-frame stack reconstruction needs "
                               "3.13+ (the PyUnstable frame API / internal walk)")
    def test_full_stack_of_parked_fiber(self):
        cap = {}

        def leaf():
            runloom.sleep(0.05)

        def middle():
            leaf()

        def top():
            middle()

        def main():
            runloom.fiber(top)
            runloom.sleep(0.01)
            gid = [g for g in runloom_c.fibers()
                   if g["state"] == "sleep"][0]["id"]
            cap["frames"] = gi.stack(gid)
            cap["entry"] = gi.entry(gid)

        runloom.run(1, main)
        # co_qualname is fully-qualified (Class.method.<locals>.leaf); match
        # on suffix.  single-thread scheduler -> full user stack, deepest first.
        funcs = [name for (_fn, _ln, name) in cap["frames"]]
        self.assertTrue(any(n.endswith("leaf") for n in funcs), funcs)
        self.assertTrue(any(n.endswith("middle") for n in funcs), funcs)
        self.assertTrue(any(n.endswith("top") for n in funcs), funcs)
        # deepest frame is the runloom.sleep internal; user frames follow
        self.assertEqual(funcs[0], "sleep")
        self.assertIn("top", cap["entry"])


class TestAge(unittest.TestCase):
    def test_age_tracking_opt_in(self):
        cap = {}
        gi.enable_timestamps(True)
        try:
            def sleeper():
                runloom.sleep(0.05)

            def main():
                runloom.fiber(sleeper)
                runloom.sleep(0.02)
                g = [x for x in runloom_c.fibers()
                     if x["state"] == "sleep"][0]
                cap["age"] = g["age"]

            runloom.run(1, main)
        finally:
            gi.enable_timestamps(False)
        self.assertIsNotNone(cap["age"])
        self.assertGreaterEqual(cap["age"], 0.0)


class TestDump(unittest.TestCase):
    def test_dump_fibers_fd_writes(self):
        # C structural dump to a temp fd (the signal-safe path).
        out = {}

        def sleeper():
            runloom.sleep(0.03)

        def main():
            runloom.fiber(sleeper)
            runloom.sleep(0.005)
            fd, path = tempfile.mkstemp()
            out["path"] = path
            runloom_c.dump_fibers(fd)
            os.close(fd)

        runloom.run(1, main)
        with open(out["path"]) as f:
            text = f.read()
        os.unlink(out["path"])
        self.assertIn("fiber dump", text)
        self.assertIn("sleep", text)

    def test_inspect_format_string(self):
        cap = {}

        def sleeper():
            runloom.sleep(0.03)

        def main():
            runloom.fiber(sleeper)
            runloom.sleep(0.005)
            cap["text"] = gi.format(stacks=True)

        runloom.run(1, main)
        self.assertIn("runloom fibers:", cap["text"])
        self.assertIn("sleep", cap["text"])

    def test_dump_to_file_object(self):
        cap = {}

        def sleeper():
            runloom.sleep(0.03)

        def main():
            runloom.fiber(sleeper)
            runloom.sleep(0.005)
            buf = io.StringIO()
            gi.dump(file=buf, stacks=True)
            cap["text"] = buf.getvalue()

        runloom.run(1, main)
        self.assertIn("fiber", cap["text"])


import subprocess


def _run_script(code, env_extra=None):
    """Run `code` in a fresh interpreter (full isolation: a deadlock leaves
    fibers parked, which mustn't pollute the test process).  Returns
    (returncode, stdout+stderr)."""
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_SYSMON"] = "0"
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
        self.assertEqual(runloom_c.count_deadlocked(), 0)

    def test_raise_end_to_end(self):
        rc, out = _run_script(
            "import runloom, runloom_c, runloom.inspect as gi\n"
            "gi.set_deadlock_mode('raise')\n"
            "try:\n"
            "    runloom.run(1, lambda: runloom_c.Chan(0).recv())\n"
            "    print('NO_RAISE')\n"
            "except RuntimeError as e:\n"
            "    print('RAISED_OK' if 'deadlock' in str(e).lower() else 'WRONG')\n")
        self.assertIn("RAISED_OK", out)
        self.assertIn("DEADLOCK", out)            # the dump printed too

    def test_warn_is_non_fatal(self):
        rc, out = _run_script(
            "import runloom, runloom_c, runloom.inspect as gi\n"
            "gi.set_deadlock_mode('warn')\n"
            "def waiter(): runloom_c.Chan(0).recv()\n"
            "def main():\n"
            "    runloom.fiber(waiter)\n"
            "    runloom_c.Chan(0).recv()\n"
            "runloom.run(1, main)\n"            # warn -> prints, no raise
            "print('SURVIVED')\n")
        self.assertEqual(rc, 0)
        self.assertIn("SURVIVED", out)
        self.assertIn("DEADLOCK", out)
        self.assertIn("chan-wait", out)

    def test_off_mode_silent(self):
        rc, out = _run_script(
            "import runloom, runloom_c, runloom.inspect as gi\n"
            "gi.set_deadlock_mode('off')\n"
            "runloom.run(1, lambda: runloom_c.Chan(0).recv())\n"
            "print('SURVIVED')\n")
        self.assertEqual(rc, 0)
        self.assertIn("SURVIVED", out)
        self.assertNotIn("DEADLOCK", out)

    def test_no_false_positive_on_clean_run(self):
        rc, out = _run_script(
            "import runloom, runloom.inspect as gi\n"
            "gi.set_deadlock_mode('raise')\n"
            "def worker():\n"
            "    [runloom.yield_() for _ in range(3)]\n"
            "def main():\n"
            "    [runloom.fiber(worker) for _ in range(5)]\n"
            "    runloom.sleep(0.005)\n"
            "runloom.run(1, main)\n"            # completes -> no deadlock
            "print('CLEAN_OK')\n")
        self.assertEqual(rc, 0)
        self.assertIn("CLEAN_OK", out)
        self.assertNotIn("DEADLOCK", out)


class TestLeakWatchdog(unittest.TestCase):
    def test_leaked_finds_old_parkers(self):
        cap = {}

        def sleeper():
            runloom.sleep(0.06)

        def main():
            gi.enable_timestamps(True)
            for _ in range(3):
                runloom.fiber(sleeper)
            runloom.sleep(0.03)            # let them age
            cap["hits"] = gi.leaked(min_age=0.01, states=("sleep",))
            cap["none"] = gi.leaked(min_age=10.0, states=("sleep",))

        runloom.run(1, main)
        self.assertEqual(len(cap["hits"]), 3)
        self.assertTrue(all(g["age"] >= 0.01 for g in cap["hits"]))
        self.assertEqual(cap["none"], [])   # nothing parked >10s

    def test_leaked_auto_enables_timestamps(self):
        gi.enable_timestamps(False)
        # leaked() should turn tracking on rather than error
        self.assertEqual(gi.leaked(min_age=0.01), [])
        self.assertTrue(runloom_c.get_introspect_timestamps())


class TestMaxGoroutines(unittest.TestCase):
    def tearDown(self):
        gi.set_max_fibers(0)

    def test_admission_gate_rejects_over_cap(self):
        cap = {}

        def main():
            gi.set_max_fibers(5)
            spawned = rejected = 0
            def parker():
                runloom_c.park_self()       # occupies a slot
            for _ in range(20):
                try:
                    runloom.fiber(parker)
                    spawned += 1
                except RuntimeError:
                    rejected += 1
            cap["spawned"] = spawned
            cap["rejected"] = rejected
            cap["live"] = gi.live_fibers()
            runloom_c.sched_reset()         # finish the parkers -> free slots

        runloom.run(1, main)
        self.assertEqual(cap["spawned"], 5)
        self.assertEqual(cap["rejected"], 15)
        self.assertEqual(cap["live"], 5)
        self.assertEqual(gi.live_fibers(), 0)   # counter balanced

    def test_counter_no_drift_over_recycling(self):
        # spawn many short-lived fibers under a small cap; slots must
        # recycle (all run) and the live counter must return to 0.
        ran = {"n": 0}

        def main():
            gi.set_max_fibers(8)
            def quick():
                ran["n"] += 1
            for _ in range(500):
                runloom.fiber(quick)
                runloom.yield_()               # let some finish, freeing slots

        runloom.run(1, main)
        self.assertEqual(ran["n"], 500)
        self.assertEqual(gi.live_fibers(), 0)

    def test_unlimited_by_default(self):
        self.assertEqual(gi.max_fibers(), 0)
        self.assertEqual(gi.live_fibers(), 0)


class TestOutsideGoroutine(unittest.TestCase):
    def test_apis_safe_when_idle(self):
        # No scheduler running: must not crash.
        self.assertEqual(runloom_c.fiber_count(), 0)
        self.assertEqual(runloom_c.fibers(), [])
        rep, frames = runloom_c.fiber_stack(999999)
        self.assertIsNone(rep)
        self.assertEqual(frames, [])


if __name__ == "__main__":
    unittest.main()
