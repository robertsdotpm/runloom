"""MN_SIM_DST_PLAN.md I0 -- baton determinism locked in CI + the SIM+mn fence.

Two jobs:

1. FREEZE the empirically-proven property the whole native mn-sim plane builds
   on: under RUNLOOM_MN_SEED the M:N scheduler is deterministic -- same
   (workload, hubs, seed) gives a bit-identical fiber completion order across
   fresh processes, and a different seed gives a different order.  Probed
   2026-07-09 (P1 CPU+yield 7/7, P2 timers 5/5); locked here for CPU+yield,
   timers, AND the chan-heavy shape (staged cross-hub chan wakes -- wake
   contract #3's first empirical support), at H=2 and H=4 (first H>2 coverage;
   a choose()/census issue at H=4 must not surface five increments late).

2. FENCE the silently-broken combo: RUNLOOM_SIM=1 + mn_init must raise a
   RuntimeError naming RUNLOOM_SIM_MN (probe P4 found wait_fd timeouts collapse
   silently under it), and RUNLOOM_SIM_MN=1 must open the bring-up path.

Digests come from tools/dst/mn_digest.py: subprocess per run, PYTHONHASHSEED
pinned, assertions on PRINTED digests never exit codes (mn fiber exceptions are
swallowed and mn_run exits 0 -- the probe caveat).
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

SEED_A, SEED_B = 12345, 999
REPEATS = 3


def assert_deterministic(workload, hubs):
    """Same seed x REPEATS identical; different seed differs."""
    runs = [mn_digest.run_digest(workload, hubs, SEED_A) for i in range(REPEATS)]
    assert len(set(runs)) == 1, \
        "{0} H={1}: same-seed digests diverged: {2}".format(workload, hubs, runs)
    other = mn_digest.run_digest(workload, hubs, SEED_B)
    assert other != runs[0], \
        "{0} H={1}: seed {2} and {3} gave the SAME digest -- schedule not " \
        "seed-driven".format(workload, hubs, SEED_A, SEED_B)


class TestBatonDeterminism:
    def test_cpu_yield_h2(self):
        assert_deterministic("cpu_yield", 2)

    def test_timers_h2(self):
        assert_deterministic("timers", 2)

    def test_chan_h2(self):
        """Staged cross-hub chan wakes (stage_flush) -- wake contract #3."""
        assert_deterministic("chan", 2)

    def test_cpu_yield_h4(self):
        assert_deterministic("cpu_yield", 4)

    def test_timers_h4(self):
        assert_deterministic("timers", 4)


class TestSimMnFence:
    def run_snippet(self, code, extra_env):
        # hermetic_env strips inherited RUNLOOM_* -- several sim test modules
        # set os.environ["RUNLOOM_SIM"] at import, so an in-process
        # `pytest tests/` run would otherwise contaminate these subprocesses
        # (e.g. test_plain_mn_unaffected would trip the fence it asserts absent).
        env = mn_digest.hermetic_env(extra_env)
        return subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                              timeout=60, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True)

    def test_sim_plus_mn_init_raises(self):
        """RUNLOOM_SIM without the opt-in: mn_init raises, the message names
        RUNLOOM_SIM_MN (the silent-corruption fence, probe P4)."""
        p = self.run_snippet(
            "import runloom_c as rc\n"
            "try:\n"
            "    rc.mn_init(2)\n"
            "    print('FENCE_MISSING')\n"
            "except RuntimeError as e:\n"
            "    print('FENCE_RAISED' if 'RUNLOOM_SIM_MN' in str(e)\n"
            "          else 'FENCE_WRONG_MSG', str(e)[:120])\n",
            {"RUNLOOM_SIM": "1"})
        assert "FENCE_RAISED" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_sim_mn_optin_opens_path(self):
        """RUNLOOM_SIM_MN=1 + RUNLOOM_MN_SEED opens the native path (since I2
        the opt-in also requires the seed -- the census dispatches the ledger,
        and no census exists without controlled mode)."""
        # NOTE: an mn_run before mn_fini is required here -- mn_fini WITHOUT a
        # prior mn_run hangs under ANY controlled-mode (RUNLOOM_MN_SEED) init,
        # sim or not: hubs park at the ctrl_wait_armed gate and fini never
        # releases them.  Verified PRE-EXISTING (plain-seed control hangs
        # identically); not an mn-sim regression.
        p = self.run_snippet(
            "import runloom_c as rc\n"
            "print('OPTIN_OK', rc.mn_init(2))\n"
            "rc.mn_fiber(lambda: None)\n"
            "rc.mn_run()\n"
            "rc.mn_fini()\n"
            "print('FINI_OK')\n",
            {"RUNLOOM_SIM": "1", "RUNLOOM_SIM_MN": "1",
             "RUNLOOM_MN_SEED": "12345"})
        assert "OPTIN_OK 2" in p.stdout and "FINI_OK" in p.stdout, \
            (p.stdout, p.stderr[-800:])

    def test_sim_mn_zero_still_fenced(self):
        """RUNLOOM_SIM_MN=0 is NOT an opt-in (flag semantics, not seed
        semantics -- unlike RUNLOOM_SIM itself, where '0' is a valid seed)."""
        p = self.run_snippet(
            "import runloom_c as rc\n"
            "try:\n"
            "    rc.mn_init(2)\n"
            "    print('FENCE_MISSING')\n"
            "except RuntimeError:\n"
            "    print('FENCE_RAISED')\n",
            {"RUNLOOM_SIM": "1", "RUNLOOM_SIM_MN": "0"})
        assert "FENCE_RAISED" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_plain_mn_unaffected(self):
        """No RUNLOOM_SIM: mn_init works exactly as before (fence inert)."""
        p = self.run_snippet(
            "import runloom_c as rc\n"
            "print('PLAIN_OK', rc.mn_init(2))\n"
            "rc.mn_fini()\n",
            {})
        assert "PLAIN_OK 2" in p.stdout, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
