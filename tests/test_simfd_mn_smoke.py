"""MN_SIM_DST_PLAN.md I6/I7 smoke -- the native mn byte-plane programs.

Three seeds of each mn program, each internally run TWICE with the trace
digest compared (the reason carries it) -- the per-seed bit-stability oracle
the fleet uses.  The deep sweep lives in lifefuzz (LIFEFUZZ_KIND=simfd_mn /
simfd_dgram_mn) and the forever hunt loop.
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


def run_seed(program, seed):
    env = mn_digest.hermetic_env({
        "RUNLOOM_SIM": "1", "RUNLOOM_SIM_MN": "1",
        "RUNLOOM_MN_SEED": str(seed),
    })
    code = (
        "import sys; sys.path.insert(0, 'tools/dst')\n"
        "import simnet_fd\n"
        "ok1, r1 = simnet_fd.{0}({1})\n"
        "ok2, r2 = simnet_fd.{0}({1})\n"
        "print('RES', ok1 and ok2 and r1 == r2, repr(r1))\n").format(program, seed)
    p = subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                       timeout=90, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, text=True)
    assert p.returncode == 0, (p.stdout, p.stderr[-800:])
    assert "RES True" in p.stdout, (program, seed, p.stdout, p.stderr[-600:])


class TestSimFdMnSmoke:
    def test_stream_seeds(self):
        for seed in (777, 4242, 90001):
            run_seed("simfd_mn_program", seed)

    def test_dgram_seeds(self):
        for seed in (777, 4242, 90001):
            run_seed("simfd_dgram_mn_program", seed)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
