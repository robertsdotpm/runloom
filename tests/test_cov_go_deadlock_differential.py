"""Go-vs-pygo deadlock-detection differential (QA-steal-V2 #14).

Uses the REAL Go runtime as the specification oracle for what constitutes an
all-goroutines deadlock.  test_go_channel_oracle.py already differentials channel
*semantics* vs Go; this differentials the deadlock *classification*: for each
scenario, Go's `checkdead` verdict (panics "all goroutines are asleep -
deadlock!" vs runs clean) must MATCH pygo's deadlock census verdict
(set_deadlock_mode(2) raises vs run() completes).  A pygo census that flagged a
non-deadlock (false positive) or missed a real one (false negative) diverges from
the Go ground truth here.

(pygo's default mode does NOT auto-detect -- a deliberate divergence from Go, for
foreign-thread / long-lived-server tolerance; the parity is asserted with the
census explicitly armed, which is the mode Go's always-on behavior corresponds
to.)

Skips cleanly if `go` is not installed.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

GO = shutil.which("go")
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# Each scenario: a Go body and an equivalent pygo body, plus the expected verdict.
SCENARIOS = {
    "recv_no_sender": {
        "deadlock": True,
        "go": "ch := make(chan int); <-ch",
        "pg": "runloom_c.Chan(0).recv()",
    },
    "send_no_receiver": {
        "deadlock": True,
        "go": "ch := make(chan int); ch <- 1",
        "pg": "runloom_c.Chan(0).send(1)",
    },
    "completes": {
        "deadlock": False,
        "go": "ch := make(chan int, 1); ch <- 1; <-ch",
        "pg": "c = runloom_c.Chan(1); c.send(1); c.recv()",
    },
}

GO_TMPL = "package main\nfunc main() {{\n\t{body}\n}}\n"
PG_TMPL = textwrap.dedent("""\
    import os, sys
    sys.path.insert(0, {src!r})
    import runloom, runloom_c
    runloom_c.set_deadlock_mode(2)          # Go-equivalent always-on census
    def body():
        {body}
    try:
        runloom.run(2, main_fn=lambda: runloom.fiber(body))
        print("COMPLETED")
    except BaseException as e:              # census raises on a real deadlock
        print("DEADLOCK", type(e).__name__)
    """)


@unittest.skipUnless(GO, "go toolchain not installed")
class TestGoDeadlockDifferential(unittest.TestCase):
    def _go_verdict(self, body):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "m.go")
            with open(p, "w") as f:
                f.write(GO_TMPL.format(body=body))
            r = subprocess.run([GO, "run", p], capture_output=True, text=True,
                               timeout=60)
        return "deadlock" in (r.stdout + r.stderr).lower()

    def _pg_verdict(self, body):
        src = os.path.join(REPO, "src")
        prog = PG_TMPL.format(src=src, body=body)
        env = dict(os.environ, PYTHON_GIL="0", PYTHON_TLBC="0",
                   PYTHONPATH=src)
        r = subprocess.run([PY, "-c", prog], capture_output=True, text=True,
                           timeout=60, env=env)
        out = r.stdout
        if "DEADLOCK" in out:
            return True
        if "COMPLETED" in out:
            return False
        self.fail("pygo produced no verdict: rc={0} out={1!r} err={2!r}".format(
            r.returncode, out, r.stderr[-200:]))

    def test_go_pygo_deadlock_verdicts_agree(self):
        for name, sc in SCENARIOS.items():
            with self.subTest(scenario=name):
                go_dl = self._go_verdict(sc["go"])
                self.assertEqual(go_dl, sc["deadlock"],
                                 "Go oracle unexpected for {0}".format(name))
                pg_dl = self._pg_verdict(sc["pg"])
                self.assertEqual(pg_dl, go_dl,
                                 "DIVERGENCE on {0}: Go deadlock={1} but pygo "
                                 "deadlock={2}".format(name, go_dl, pg_dl))


if __name__ == "__main__":
    unittest.main()
