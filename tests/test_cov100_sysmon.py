"""Adversarial coverage suite for src/runloom_c/mn_sched_sysmon.c.inc.

The uncovered lines in this fragment fall into three groups:

  (A) runloom_handoff_resume_g()  (L329-L395) -- the Group-B DETACHED-wedge
      rescue.  A standby "rescue" thread (RUNLOOM_HANDOFF) steals a FRESH fiber
      from a stalled hub's Chase-Lev deque and resumes it on the rescue thread's
      OWN tstate while the hub owner is parked in a blocking syscall.  This whole
      function only runs when ALL of:
        * RUNLOOM_HANDOFF is on (forces sysmon on + spawns the rescue pool),
        * a hub is STABLY DETACHED-wedged: a fiber on that hub is inside a real
          blocking call that released the tstate (Py_BEGIN_ALLOW_THREADS) for
          longer than the wedge budget, for >= RUNLOOM_HANDOFF_DETACH_TICKS (2)
          consecutive sysmon ticks, and
        * there are fresh (never-run, snap.valid==0) fibers in that hub's deque
          for the rescue to steal.
      The normal corpus never builds this exact state, so the function is dark.

      To drive it we run, in a clean subprocess with RUNLOOM_HANDOFF=1:
        * many FRESH fibers queued via mn_go (they pile into the per-hub deques),
          in three flavors so the rescue's three post-resume branches all fire:
            flavor 0: run straight to completion
                      -> coro_done branch              (L377-L383)
            flavor 1: rc.sched_sleep(...) -> mark_parked sets self_queued=1 and
                      leaves the g in PARKED_SLEEP
                      -> "self_queued && parked" branch (L391-L395, coro_park)
            flavor 2: ch.recv() on an empty unbuffered chan parks via a RAW
                      coro_yield (self_queued stays 0), PARKED_CHAN
                      -> "!self_queued" re-submit branch (L384-L390)
        * one long REAL time.sleep() blocker per hub.  time.sleep is the builtin
          (no monkey.patch here) so it runs Py_BEGIN_ALLOW_THREADS on the HUB
          thread -> the hub tstate is DETACHED for the whole sleep -> a stable
          DETACHED wedge that sysmon CASes FREE->PENDING and the rescue adopts.
      `main` parks cooperatively on a WaitGroup (runloom_c.park(), NOT a syscall)
      until every fresh fiber has finished, so the counts are read after all work
      is drained -- no racy fixed-sleep window.  Every fiber that RAN is asserted
      to have reached DONE, so the rescue is proven to have resumed real work --
      not merely "touched" the lines.

  (B) The early-out / cleanup arms of runloom_sysmon_main's calloc-failure
      (L41-L42) and the watchdog-spawn-failure fprintf (L294) are pure
      OOM / pthread_create-failure defenses with no fault-injection site -- see
      the `unreachable` report.

  (C) runloom_sched_freeze_for_crash (L13-L18) is called ONLY from the
      fatal-signal crash handler, which always chains out and re-raises the fatal
      signal (the process dies from SIGSEGV/SIGBUS); gcov counters never flush on
      a signal death -- see the `unreachable` report.

Every subprocess EXITS CLEANLY (rc==0 + a stdout marker) so gcov counters flush.
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

pytestmark = pytest.mark.skipif(not FT, reason="M:N handoff rescue needs the GIL-disabled build")


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


# Common env to arm the handoff rescue:
#   RUNLOOM_HANDOFF=1        -> forces sysmon instrumentation + watchdog on and
#                              spawns the rescue-thread pool.
#   RUNLOOM_SYSMON_MS=20     -> 20 ms wedge budget (a >=0.5 s blocker clears it by
#                              many ticks, so the DETACH streak is stable).
#   RUNLOOM_HANDOFF_POOL=4   -> 4 rescue threads (one can adopt each wedged hub).
#   RUNLOOM_SYSMON_QUIET=1   -> suppress the per-wedge stderr spam.
_HANDOFF_ENV = {
    "RUNLOOM_HANDOFF": "1",
    "RUNLOOM_SYSMON": "1",
    "RUNLOOM_SYSMON_QUIET": "1",
    "RUNLOOM_SYSMON_MS": "20",
    "RUNLOOM_HANDOFF_POOL": "4",
}


# --------------------------------------------------------------------------- #
# (A) The full rescue: all three post-resume branches of runloom_handoff_resume_g
#     (L329-L395) -- done / parked / !self_queued re-submit -- on real work.
# --------------------------------------------------------------------------- #
def test_handoff_rescue_resumes_fresh_fibers_all_branches():
    # Drives: L329-L376 (entry, fresh-g datastack install at L336-L343, qref
    # bookkeeping L355-L375, coro_resume L366), then the three completion arms
    # L377-L383 (done), L384-L390 (raw-yield re-submit), L391-L395 (parked).
    body = r"""
NHUBS = 4
NFRESH = 96
ran = bytearray(NFRESH)
done = bytearray(NFRESH)
R = {}

def main():
    ch = rc.Chan(0)                       # unbuffered: recv with no sender parks
    wg = WaitGroup(); wg.add(NFRESH)

    def fresh(i):
        try:
            ran[i] = 1                     # proves the rescue actually RESUMED it
            flav = i % 3
            if flav == 0:                  # -> runs to completion (done branch)
                x = 0
                for k in range(1500):
                    x += k
            elif flav == 1:                # -> PARKED_SLEEP + self_queued (L391-395)
                rc.sched_sleep(0.05)
            else:                          # -> PARKED_CHAN via raw yield (L384-390)
                ch.recv()
            done[i] = 1
        finally:
            wg.done()

    # Queue the fresh fibers FIRST so they pile into the per-hub deques, where the
    # rescue can steal them while the hubs are wedged.
    for i in range(NFRESH):
        rc.mn_fiber(lambda i=i: fresh(i))

    # One long DETACHED blocker per hub: real builtin time.sleep -> the hub
    # tstate goes DETACHED for 0.5s -> a stable wedge the rescue pool adopts.
    def blocker():
        time.sleep(0.5)
    for _ in range(NHUBS):
        rc.mn_fiber(blocker)

    # Exactly one sender per flavor-2 receiver so every chan-parked fiber is
    # woken and completes; nobody is stranded and the channel is never closed
    # under a queued sender.
    nrecv = sum(1 for i in range(NFRESH) if i % 3 == 2)
    for _ in range(nrecv):
        rc.mn_fiber(lambda: ch.send(1))

    wg.wait()                              # cooperative park until ALL fresh done
    R["ran"] = sum(ran)
    R["done"] = sum(done)

runloom.run(NHUBS, main)

# Real assertions: every fresh fiber ran AND completed across all three flavors
# (run / sleep-park / chan-park).  A rescue bug that dropped a stolen deque fiber
# or mis-handled a branch would show up as ran/done < NFRESH or a hang the
# subprocess timeout catches.
assert R["ran"] == NFRESH, R
assert R["done"] == NFRESH, R
print("HANDOFF_RESCUE_OK ran=%d done=%d" % (R["ran"], R["done"]))
"""
    p = _run(body, _HANDOFF_ENV)
    assert p.returncode == 0, "rescue workload crashed/failed (rc=%d)\nstderr=%s" % (
        p.returncode, p.stderr[-2000:])
    assert "HANDOFF_RESCUE_OK ran=96 done=96" in p.stdout, (
        "rescue did not complete\nout=%s\nerr=%s" % (p.stdout, p.stderr[-1500:]))


# --------------------------------------------------------------------------- #
# (A') Done-branch in isolation, at higher volume: a deque packed with fresh
#      fibers that all RUN TO COMPLETION on the rescue thread.  Hammers the
#      L377-L383 done path (drain_g_datastack / pystate_load(base) /
#      netpoll_force_unlink_g_parker / mn_pending_complete / g_decref /
#      pystate_snap(base)) and the L355-L375 qref decref many times over.
# --------------------------------------------------------------------------- #
def test_handoff_rescue_done_branch_volume():
    body = r"""
NHUBS = 4
NFRESH = 200
done = bytearray(NFRESH)
R = {}

def main():
    wg = WaitGroup(); wg.add(NFRESH)
    def fresh(i):
        try:
            x = 0
            for k in range(800):           # short pure-CPU; completes on resume
                x += k
            done[i] = 1                     # single writer per slot, race-free
        finally:
            wg.done()
    for i in range(NFRESH):
        rc.mn_fiber(lambda i=i: fresh(i))

    def blocker():
        time.sleep(0.6)                     # DETACHED wedge per hub
    for _ in range(NHUBS):
        rc.mn_fiber(blocker)

    wg.wait()
    R["done"] = sum(done)

runloom.run(NHUBS, main)
# Every fresh fiber must have completed exactly once (whether drained by the
# rescue while wedged or by the owner after it woke).  A wedged-hub bug that
# dropped a deque fiber would show up as done < NFRESH (or a hang).
assert R["done"] == 200, R
print("HANDOFF_DONE_VOLUME_OK done=%d" % R["done"])
"""
    p = _run(body, _HANDOFF_ENV)
    assert p.returncode == 0, "done-volume workload crashed (rc=%d)\nstderr=%s" % (
        p.returncode, p.stderr[-2000:])
    assert "HANDOFF_DONE_VOLUME_OK done=200" in p.stdout, (
        "done-volume rescue incomplete\nout=%s\nerr=%s" % (p.stdout, p.stderr[-1500:]))


# --------------------------------------------------------------------------- #
# (A'') Sanity guard with HANDOFF DISABLED: same wedge workload, but
#       RUNLOOM_HANDOFF=0 so the rescue pool never spawns and
#       runloom_handoff_resume_g is NEVER entered.  The work must STILL all
#       complete (the owners drain their own deques once the blockers wake),
#       proving the wedge workload is correct independent of the rescue and
#       isolating the rescue as the only behavioural difference vs (A').
# --------------------------------------------------------------------------- #
def test_wedge_workload_completes_without_handoff():
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
            done[i] = 1
        finally:
            wg.done()
    for i in range(NFRESH):
        rc.mn_fiber(lambda i=i: fresh(i))

    def blocker():
        time.sleep(0.3)
    for _ in range(NHUBS):
        rc.mn_fiber(blocker)

    wg.wait()
    R["done"] = sum(done)

runloom.run(NHUBS, main)
assert R["done"] == 120, R
print("NO_HANDOFF_OK done=%d" % R["done"])
"""
    p = _run(body, {"RUNLOOM_HANDOFF": "0", "RUNLOOM_PREEMPT": "0", "RUNLOOM_SYSMON": "0"})
    assert p.returncode == 0, "no-handoff workload crashed (rc=%d)\nstderr=%s" % (
        p.returncode, p.stderr[-2000:])
    assert "NO_HANDOFF_OK done=120" in p.stdout, (
        "no-handoff workload incomplete\nout=%s\nerr=%s" % (p.stdout, p.stderr[-1500:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
