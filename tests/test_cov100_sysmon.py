"""Adversarial coverage suite for src/runloom_c/mn_sched_sysmon.c.inc.

The sysmon watchdog is DETECT + PREEMPT only: it logs a wedged hub and, for an
ATTACHED (CPU-bound) wedge, arms preemption.  A DETACHED (blocking-IO) wedge
needs no recovery here -- normal work-stealing already drains a stalled hub's
fresh fibers to idle hubs.  This test pins that property: a workload that wedges
several hubs on real blocking calls while a fan-out of fresh fibers is queued
must STILL complete every fiber, with the wedged hubs' fresh work drained by the
idle hubs (no standby "rescue" thread is involved -- that subsystem was removed).

The OOM / spawn-failure early-outs of runloom_sysmon_main, and
runloom_sched_freeze_for_crash (only reached on a fatal-signal death, where gcov
never flushes), have no fault-injection site -- see the `unreachable` report.

The subprocess EXITS CLEANLY (rc==0 + a stdout marker) so gcov counters flush.
"""
import os
import subprocess
import sys

import pytest

import runloom_c as rc  # noqa: F401  (import side effects + the FT gate below)
from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(not FT, reason="M:N work-stealing needs the GIL-disabled build")


def _run(body, env_extra, timeout=90):
    """Run `body` as a fresh child Python process under the given env; return it.

    The child imports the same in-tree runloom_c (PYTHONPATH=src, cwd=REPO) and
    must finish cleanly for gcov counters to flush -- we assert rc==0 + marker.
    """
    src = ("import sys\n"
           "sys.path.insert(0, 'src')\n"
           "import runloom\n"
           "import runloom_c as rc\n"
           "import time\n"
           "from runloom.sync import WaitGroup\n") + body
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run([PY, "-c", src], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


# --------------------------------------------------------------------------- #
# Several hubs wedge on a real blocking call while a fan-out of fresh fibers is
# queued.  Work-stealing must drain the stranded fresh fibers to the idle hubs,
# so every fiber completes (no rescue thread exists).  RUNLOOM_SYSMON=1 +
# a low RUNLOOM_SYSMON_MS arm the detector so its instrumentation is exercised.
# --------------------------------------------------------------------------- #
def test_wedged_hubs_drain_via_work_stealing():
    body = r"""
NHUBS = 4
NFRESH = 120
done = bytearray(NFRESH)
R = {}

def main():
    wg = WaitGroup(); wg.add(NFRESH)
    def fresh(i):
        try:
            x = 0
            for k in range(600):
                x += k
            done[i] = 1                     # single writer per slot, race-free
        finally:
            wg.done()
    for i in range(NFRESH):
        rc.mn_fiber(lambda i=i: fresh(i))

    def blocker():
        time.sleep(0.3)                     # DETACHED wedge per hub
    for _ in range(NHUBS):
        rc.mn_fiber(blocker)

    wg.wait()
    R["done"] = sum(done)

runloom.run(NHUBS, main)
# Every fresh fiber must complete (drained off the wedged hubs by idle hubs, or
# by the owners after the blockers wake).  A work-stealing bug that stranded a
# fiber behind a wedged hub would show up as done < NFRESH (or a hang).
assert R["done"] == 120, R
print("WORKSTEAL_OK done=%d" % R["done"])
"""
    p = _run(body, {"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1",
                    "RUNLOOM_SYSMON_MS": "20"})
    assert p.returncode == 0, "wedge workload crashed (rc=%d)\nstderr=%s" % (
        p.returncode, p.stderr[-2000:])
    assert "WORKSTEAL_OK done=120" in p.stdout, (
        "wedge workload incomplete\nout=%s\nerr=%s" % (p.stdout, p.stderr[-1500:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
