"""MN_SIM_DST_PLAN.md I1 -- the ns-native mn census clock.

The ctrl struct's logical clock is now ns-native (`logical_now_ns` is the
authority; the double is a derived mirror under one fixed rounding rule), the
census advance mirrors into the single-thread global clock (one time plane),
rc._logical_ns reads the EXACT census ns under controlled M:N, and
ctrl_arm/sim_reset zero every plane so back-to-back in-process runs are
bit-identical.

Subprocesses use mn_digest.hermetic_env (strips inherited RUNLOOM_*); asserts
on printed output only (mn fiber exceptions are swallowed).
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "dst"))

import mn_digest  # noqa: E402

from adv_util import needs_free_threading  # noqa: E402

pytestmark = pytest.mark.skipif(
    not needs_free_threading(),
    reason="the M:N scheduler is only real on free-threaded builds")


def run_snippet(code, seed=12345):
    env = mn_digest.hermetic_env({"RUNLOOM_MN_SEED": str(seed)})
    p = subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                       timeout=60, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, text=True)
    return p


class TestMnNsClock:
    def test_census_clock_exact_ns(self):
        """Two sleepers (3ms, 5ms): after mn_run the clock sits at EXACTLY the
        last advance -- 5_000_000 ns, no double round-trip error -- and the
        sleepers woke in deadline order."""
        p = run_snippet(
            "import runloom_c as rc\n"
            "order = []\n"
            "def s(k, secs):\n"
            "    def w():\n"
            "        rc.sched_sleep(secs); order.append(k)\n"
            "    return w\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(s(1, 0.003))\n"
            "rc.mn_fiber(s(2, 0.005))\n"
            "rc.mn_run()\n"
            "print('NS', rc._logical_ns(), 'ORDER', order)\n"
            "rc.mn_fini()\n")
        assert "NS 5000000 ORDER [1, 2]" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_clock_monotone_across_wakes(self):
        """Each sleeper reads the clock as it wakes: the readings are the exact
        per-deadline advances, in order -- the census clock tracks sched_sleep
        boundaries and jumps equal the sleep deltas."""
        p = run_snippet(
            "import runloom_c as rc\n"
            "seen = []\n"
            "def s(secs):\n"
            "    def w():\n"
            "        rc.sched_sleep(secs); seen.append(rc._logical_ns())\n"
            "    return w\n"
            "rc.mn_init(2)\n"
            "for ms in (2, 4, 7):\n"
            "    rc.mn_fiber(s(ms / 1000.0))\n"
            "rc.mn_run()\n"
            "print('SEEN', seen)\n"
            "rc.mn_fini()\n")
        assert "SEEN [2000000, 4000000, 7000000]" in p.stdout, \
            (p.stdout, p.stderr[-800:])

    def test_back_to_back_runs_bit_identical(self):
        """sim_reset + re-arm between two mn_runs in ONE process: both runs
        report the same final clock and the same completion order (per-run
        clock zeroing at ctrl_arm + the sim_reset all-planes hook)."""
        body = (
            "order = []\n"
            "def s(k, secs):\n"
            "    def w():\n"
            "        rc.sched_sleep(secs); order.append(k)\n"
            "    return w\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(s(1, 0.003))\n"
            "rc.mn_fiber(s(2, 0.005))\n"
            "rc.mn_run()\n"
            "print('R{n}', rc._logical_ns(), order)\n"
            "rc.mn_fini()\n")
        p = run_snippet(
            "import runloom_c as rc\n" +
            body.format(n=1) +
            "rc.sim_reset()\n" +
            body.format(n=2))
        lines = [ln for ln in p.stdout.splitlines() if ln.startswith("R")]
        assert len(lines) == 2, (p.stdout, p.stderr[-800:])
        assert lines[0][2:] == lines[1][2:], \
            "back-to-back runs diverged: {0}".format(lines)

    def test_fractional_deadline_fires(self):
        """I1-review regression 1 (census livelock): a deadline whose ns
        fraction rounds the double mirror BELOW the exact wake_at --
        sched_sleep(1/3) -- must fire.  The advance keeps the double plane
        EXACT (pre-I1 semantics) and derives ns from it; a mirrored
        ns->double clock stranded this sleeper forever."""
        p = run_snippet(
            "import runloom_c as rc\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(lambda: rc.sched_sleep(1.0 / 3.0))\n"
            "rc.mn_run()\n"
            "print('FRAC_OK', rc._logical_ns())\n"
            "rc.mn_fini()\n")
        assert "FRAC_OK 333333333" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_gap_sleeper_run_again(self):
        """I1-review regression 2 (arm wipe): spawn-again/run-again on a live
        pool with a gap fiber that sleeps -- an unconditional deadline wipe at
        ctrl_arm stranded the sleeper (hang ~iter 10 under jitter).  40
        jittered iterations must complete."""
        p = run_snippet(
            "import time, runloom_c as rc\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(lambda: None)\n"
            "rc.mn_run()\n"
            "for i in range(40):\n"
            "    rc.mn_fiber(lambda: rc.sched_sleep(0.001))\n"
            "    time.sleep((i % 10) * 0.0001)\n"
            "    rc.mn_run()\n"
            "rc.mn_fini()\n"
            "print('GAP_OK 40')\n", seed=1)
        assert "GAP_OK 40" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_no_global_clock_leak_into_h1(self):
        """I1-review regression 3 (plane leak): an H=1 logical-clock run AFTER
        mn_fini must start at 0 (ctrl_fini resets the global mirror); the
        leaked mirror made it start at the mn run's final instant."""
        env = mn_digest.hermetic_env({"RUNLOOM_MN_SEED": "12345",
                                      "RUNLOOM_LOGICAL_CLOCK": "1"})
        p = subprocess.run(
            [sys.executable, "-c",
             "import runloom_c as rc\n"
             "rc.mn_init(2)\n"
             "rc.mn_fiber(lambda: rc.sched_sleep(0.005))\n"
             "rc.mn_run(); rc.mn_fini()\n"
             "rc.fiber(lambda: rc.sched_sleep(0.004))\n"
             "rc.run()\n"
             "print('H1_AFTER_MN', rc._logical_ns())\n"],
            cwd=REPO, env=env, timeout=60,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert "H1_AFTER_MN 4000000" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_h1_legacy_plane_unchanged(self):
        """The single-thread logical clock still works standalone (no mn):
        rc.run() under RUNLOOM_LOGICAL_CLOCK advances to the sleeper deadline
        exactly as before the I1 mirror was added."""
        env = mn_digest.hermetic_env({"RUNLOOM_LOGICAL_CLOCK": "1"})
        p = subprocess.run(
            [sys.executable, "-c",
             "import runloom_c as rc\n"
             "def w(): rc.sched_sleep(0.004)\n"
             "rc.fiber(w)\n"
             "rc.run()\n"
             "print('H1NS', rc._logical_ns())\n"],
            cwd=REPO, env=env, timeout=60,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert "H1NS 4000000" in p.stdout, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
