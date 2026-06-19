"""M:N teardown must not deadlock against a thread's startup stop-the-world.

mn_fini() joins the rescue + hub threads.  On free-threaded 3.13t a hub/rescue
thread's startup PyThreadState_New does a qsbr-slot stop-the-world, which waits
for EVERY attached thread to reach a safe point.  If mn_fini joins such a thread
(or blocks on a lock it holds, e.g. runloom_hub_tstate_lock) while the MAIN thread
is still ATTACHED, the STW can never complete -> deadlock.  It is a startup race,
so it fires only when the workload is instant enough that mn_run() returns before
a hub/rescue thread finishes starting -- i.e. exactly the trivial M:N programs
below.  Was ~80% hang; the fix detaches the main thread around the rescue join
and defers rescue-tstate deletion until after the hubs are joined.

The hang has no FV model (it is a CPython-runtime STW/attach interaction, not a
runloom lock-free algorithm); the gate is this stress -- a deadlock trips the
suite timeout, so a green run IS the assertion.
"""
import os
import subprocess
import sys

import pytest

import runloom
import runloom_c


def _trivial_cycle(nhubs):
    box = bytearray(1)

    def runner():
        # Touch a cooperative primitive so the runner is a real (if instant) g.
        mu = runloom_c.Mutex()
        with mu:
            pass
        box[0] = 1

    runloom_c.mn_init(nhubs)
    runloom_c.mn_fiber(runner)
    runloom_c.mn_run()
    runloom_c.mn_fini()
    return box[0]


def test_repeated_trivial_mn_teardown():
    # Many instant cycles across hub counts -- each is a fresh shot at the
    # startup-STW-vs-join race.  At the old hang rate a handful would already
    # deadlock; the whole loop completing means teardown is clean.
    for i in range(60):
        nhubs = (i % 4) + 1            # 1, 2, 4(=3+1)... spread small + large
        if nhubs == 3:
            nhubs = 8
        assert _trivial_cycle(nhubs) == 1, i


def test_trivial_mn_teardown_via_run():
    # The public wrapper (runloom.run) takes the same mn_init/mn_run/mn_fini path.
    for i in range(30):
        box = bytearray(1)

        def main():
            box[0] = 1

        runloom.run((i % 4) + 1, main)
        assert box[0] == 1, i


# Deterministic version of the same guard: instead of hoping the OS schedules a
# rescue thread late enough to put its startup PyThreadState_New (qsbr
# stop-the-world) inside mn_fini's join window, FORCE it there with the test-only
# RUNLOOM_HANDOFF_STARTUP_DELAY_MS knob (mn_sched_handoff.c.inc).  With the knob
# set, every cycle's teardown deterministically races a rescue startup-STW, which
# is exactly what the main-detach + deferred-tstate-deletion fix must survive.
# Run in a timeout-bounded subprocess so a regression (the STW wedging the join)
# is a clear failure rather than a silent suite hang.  Documented in
# docs/dev/mn_fini_hang.md.
_FORCED_STW_DRIVER = """
import os, runloom_c
for i in range(24):
    nhubs = (i % 4) + 1
    if nhubs == 3:
        nhubs = 8
    runloom_c.mn_init(nhubs)
    runloom_c.mn_fiber(lambda: None)
    runloom_c.mn_run()
    runloom_c.mn_fini()
print("OK")
"""


def test_teardown_under_forced_startup_stw():
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_HANDOFF"] = "1"
    # 30ms per rescue thread -> its startup PyThreadState_New STW lands squarely
    # in mn_fini's rescue-join window on every cycle.
    env["RUNLOOM_HANDOFF_STARTUP_DELAY_MS"] = "30"
    try:
        cp = subprocess.run(
            [sys.executable, "-c", _FORCED_STW_DRIVER],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,   # generous: 24 cycles * a few hubs * 30ms is well under 1s of delay
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "mn_fini DEADLOCKED with the rescue startup-STW forced into the "
            "teardown window (RUNLOOM_HANDOFF_STARTUP_DELAY_MS=30) -- the "
            "main-detach / deferred-tstate-deletion teardown fix regressed."
        )
    assert cp.returncode == 0, (cp.returncode, cp.stdout, cp.stderr)
    assert "OK" in cp.stdout, (cp.stdout, cp.stderr)
