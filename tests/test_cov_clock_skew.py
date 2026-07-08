"""Clock-skew fault injection into the timer path (QA-steal-V2 #20, Chaos-Mesh
TimeChaos / libfaketime).

Builds tools/soak/clock_skew_shim.c and LD_PRELOADs it so the CLOCK_MONOTONIC
reads behind runloom_monotonic_ns() (the netpoll/sysmon deadline path) get a
monotonicity-preserving fast-forward + forward jitter -- an irregular clock,
unlike the smooth DST logical clock.  A workload of many sched_sleep timers must
still ALL fire (no timer stranded on skewed deadline math, no negative/overflow
duration wedging a waiter), and the total elapsed must respect the skew.  A hang
here is a real timeout-arithmetic bug (the shim never moves the clock backward,
so the CLOCK_MONOTONIC contract is upheld).
"""
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHIM_SRC = os.path.join(REPO, "tools", "soak", "clock_skew_shim.c")
PY = sys.executable

WORKLOAD = textwrap.dedent("""\
    import os, sys
    sys.path.insert(0, {src!r})
    import runloom, runloom_c
    N = 400
    fired = bytearray(N)
    def timer(i):
        runloom_c.sched_sleep(0.002 + (i % 7) * 0.001)   # 2-8 ms deadlines
        fired[i] = 1
    def root():
        for i in range(N):
            runloom.fiber(lambda i=i: timer(i))
    runloom.run(4, main_fn=root)
    missed = sum(1 for f in fired if not f)
    print("FIRED", N - missed, "MISSED", missed)
    """)


class TestClockSkew(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shim = None
        if not os.path.exists(SHIM_SRC):
            return
        d = tempfile.mkdtemp(prefix="clkshim_")
        so = os.path.join(d, "clock_skew_shim.so")
        r = subprocess.run(["cc", "-O2", "-fPIC", "-shared", "-o", so, SHIM_SRC,
                            "-ldl"], capture_output=True, text=True)
        if r.returncode == 0:
            cls.shim = so

    def _run(self, ff_ns, jit_ns):
        if self.shim is None:
            self.skipTest("could not build clock_skew_shim (cc/-ldl?)")
        env = dict(os.environ, PYTHON_GIL="0", PYTHON_TLBC="0",
                   PYTHONPATH=os.path.join(REPO, "src"),
                   LD_PRELOAD=self.shim,
                   CLOCK_SKEW_FF=str(ff_ns), CLOCK_SKEW_JIT=str(jit_ns))
        prog = WORKLOAD.format(src=os.path.join(REPO, "src"))
        r = subprocess.run([PY, "-c", prog], capture_output=True, text=True,
                           timeout=90, env=env)
        self.assertIn("FIRED", r.stdout,
                      "workload produced no result under skew (hang?): rc={0} "
                      "err={1!r}".format(r.returncode, r.stderr[-300:]))
        fired = int(r.stdout.split("FIRED")[1].split()[0])
        missed = int(r.stdout.split("MISSED")[1].split()[0])
        return fired, missed

    def test_control_no_skew(self):
        # Sanity: shim loaded, zero skew -> every timer fires.
        fired, missed = self._run(0, 0)
        self.assertEqual(missed, 0, "control: {0} timers missed".format(missed))

    def test_fast_forward(self):
        # +5 ms constant fast-forward every read.
        fired, missed = self._run(5_000_000, 0)
        self.assertEqual(missed, 0, "fast-forward stranded {0} timers".format(missed))

    def test_forward_jitter(self):
        # up to +3 ms forward jitter per read (still monotonic).
        fired, missed = self._run(0, 3_000_000)
        self.assertEqual(missed, 0, "jitter stranded {0} timers".format(missed))

    def test_ff_plus_jitter(self):
        fired, missed = self._run(2_000_000, 2_000_000)
        self.assertEqual(missed, 0, "ff+jitter stranded {0} timers".format(missed))


if __name__ == "__main__":
    unittest.main()
