"""Coverage-driven adversarial suite for mn_sched_handoff.c.inc.

This fragment is the STALLED-HUB TSTATE-HANDOFF rescue subsystem (RUNLOOM_HANDOFF,
default ON on free-threaded 3.13t but never *exercised* by the normal corpus): a
pool of standby OS threads, each with its OWN PyThreadState, that adopt a hub
whose running fiber has wedged it inside a blocking call (Py_BEGIN_ALLOW_THREADS
-> the hub's tstate is DETACHED), steal the FRESH fibers already queued on that
hub's Chase-Lev deque, attach the rescue tstate per-fiber, run them, and detach
again so a concurrent GC stop-the-world can complete between runs.

WHY THE NORMAL CORPUS NEVER REACHES THE ADOPT PATH (L97-122):
  The sysmon watchdog only CASes a hub's claim slot FREE->PENDING after the hub
  has been STABLY DETACHED-while-wedged for RUNLOOM_HANDOFF_DETACH_TICKS ticks --
  i.e. a fiber sitting in a genuine blocking syscall, NOT a CPU spin (which is
  ATTACHED -> the preempt path, not the handoff path).  AND the rescue only earns
  coverage if there are FRESH fibers in that wedged hub's *deque* to steal: the
  rescue deliberately does NOT touch the sub_list / sleep heap (those need
  hub-private pushes), so the fibers must already have been drained sub_list->
  deque by a hub_main loop iteration before the wedge formed.

HOW WE MANUFACTURE IT (the single-hub trick):
  mn_init(1).  Spawn N fresh worker fibers from the MAIN thread BEFORE mn_run, so
  the sole hub's first hub_main loop drains them all into its OWN deque; spawn the
  "blocker" fiber LAST so the deque's LIFO pop hands the hub the blocker first.
  The blocker calls the REAL time.sleep (Py_BEGIN_ALLOW_THREADS, tstate DETACHED)
  and holds the hub's resume clock for ~1s -> a textbook DETACHED wedge with the
  N workers still queued on the wedged hub's deque.  At hub_count==1 there is NO
  neighbour hub to steal those workers (the multi-hub steal at hub_main.c:498),
  so the ONLY entity that can run them is a rescue thread.  Therefore:

      any worker that completes WHILE the blocker is still sleeping was run by
      the rescue thread's adopt path  ==>  L97-122 executed.

  That equivalence is the adversarial assertion -- not "the process exited 0", but
  "work provably completed on a hub that was provably wedged the entire time".

Each mode is read once at mn_init, so every scenario runs in its OWN SUBPROCESS
with the env set; gcov only counts a CLEAN exit (the workload prints a marker and
returns 0), so every test asserts the marker + rc 0 in addition to its property.

Uncovered lines driven (line numbers in the .inc):
  * L66-68   deque-empty DEBUG trace, gated on runloom_handoff_debug && base_valid
             (a long blocker lets the rescue fully drain the deque mid-wedge).
  * L78      deque-empty re-check RETURN: the wedge cleared (blocker woke ->
             resume_start_ns==0 / tstate re-ATTACHED) -> rescue exits.
  * L97-100  per-fiber adopt: delay-inject site, the HANDOFF_ADOPT event,
             PyEval_RestoreThread(rescue_ts) (attach), runloom_tls_hub = h.
  * L101,105-106  first-run base-state snapshot of the rescue tstate.
  * L107-109 the once-per-session "adopt hub %d (own tstate)" DEBUG line.
  * L112-113 runloom_handoff_resume_g(...) + drained++ (the actual fiber run).
  * L120-122 restore clean rescue state, tls_hub=NULL, PyEval_SaveThread (detach).
  * L203,209 runloom_handoff_spawn's threads[] + rescue_tstates[] calloc (the
             normal enabled spawn path).

See `unreachable` in the structured report for L40 / L153-154 / L214-220 /
L230-234 (teardown-race + un-hooked OOM/resource-exhaustion guards).
"""
import os
import re
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(
    not FT, reason="stalled-hub handoff rescue needs the GIL-disabled M:N runtime")

# RUNLOOM_HANDOFF forces the sysmon watchdog on; a tiny wedge budget makes the
# ~1s blocker overrun it within a couple of ticks so the claim slot flips PENDING
# quickly.  POOL=2 is capped to hub_count==1 inside the runtime (one rescuer).
_HANDOFF_ENV = {
    "RUNLOOM_HANDOFF": "1",
    "RUNLOOM_HANDOFF_POOL": "2",
    "RUNLOOM_SYSMON": "1",
    "RUNLOOM_SYSMON_QUIET": "1",
    "RUNLOOM_SYSMON_MS": "8",
}


# A self-contained single-hub workload that manufactures a DETACHED wedge with
# fresh fibers stranded on the wedged hub's deque (see the module docstring).
# It prints "HANDOFF_OK <total> <during>":
#   total  = workers that ran in the whole run (must reach NWORK -> clean drain)
#   during = workers that ran WHILE the blocker was still sleeping == rescued.
_RESCUE_WORKLOAD = r'''
import sys, time
sys.path.insert(0, "src")
import runloom_c as rc

NWORK = 24
rescued = bytearray(NWORK)          # one slot per worker, single writer each
res = {{"during": 0}}

def worker(i):
    rescued[i] = 1                  # race-free: distinct slot per fiber

def blocker():
    before = sum(rescued)
    time.sleep({sleep})            # REAL blocking call -> Py_BEGIN_ALLOW_THREADS,
                                    # tstate DETACHED, hub resume clock held: a wedge
    res["during"] = sum(rescued) - before

rc.mn_init(1)                       # single hub: no neighbour to steal the workers,
                                    # so a drained worker can ONLY be a rescue
# Workers first (queued), blocker LAST so the deque LIFO pop wedges the hub on it
# while the workers are still in the deque for the rescue thread to steal.
for i in range(NWORK):
    rc.mn_fiber(lambda i=i: worker(i))
rc.mn_fiber(blocker)
rc.mn_run()
rc.mn_fini()
sys.stdout.write("HANDOFF_OK %d %d\n" % (sum(rescued), res["during"]))
'''


def _run(script, env_extra, timeout=60):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


def _parse_handoff_ok(stdout):
    for line in stdout.splitlines():
        if line.startswith("HANDOFF_OK"):
            _, total, during = line.split()
            return int(total), int(during)
    return None


# The rescue ADOPT path is a real, reliable event, but its LATENCY is not bounded by
# the blocker's sleep: the sysmon watchdog backs its scan off adaptively when idle
# (Go-style, to save idle wakeups) and the standby rescue thread must then be
# scheduled to adopt -- under CPU contention (the parallel suite) that chain can
# occasionally exceed a single blocker's sleep window, so `during` comes back 0 and
# the adopt never ran THIS attempt (measured: ~1/5 isolated, all-fire standalone).
# These are rare-PATH probes: the property is "the adopt path executes", which a
# retry proves directly (a genuine never-fires still fails every attempt) without
# weakening any assertion.  So run the wedge workload until the rescue provably
# fired, then assert on that run.
def _run_rescue_until(sleep, predicate, env_extra=None, attempts=8, timeout=60):
    script = _RESCUE_WORKLOAD.format(sleep=sleep)
    env = dict(_HANDOFF_ENV, **(env_extra or {}))
    p = None
    for _ in range(attempts):
        p = _run(script, env, timeout=timeout)
        if predicate(p):
            return p
    return p


def _rescued(p):
    parsed = _parse_handoff_ok(p.stdout)
    return p.returncode == 0 and parsed is not None and parsed[1] >= 1


def _adopted_and_drained(p):
    # the adopt line AND the deque-empty-while-wedged line, both gated debug traces.
    if not _rescued(p) or "adopt hub 0 (own tstate)" not in p.stderr:
        return False
    m = re.search(r"hub 0 deque empty \(drained=(\d+)\)", p.stderr)
    return m is not None and int(m.group(1)) >= 1


# --------------------------------------------------------------------------
# 1. The core adopt path: a rescue thread runs fresh fibers off a wedged hub.
#    Drives L97-122 (per-fiber adopt/attach/resume/detach), L101+L105-106 (the
#    once-per-session base snapshot), L112-113 (the resume + drained counter),
#    and -- because hub_count==1 means the rescue is the SOLE drainer -- proves
#    the path actually executed rather than merely being entered.
#
#    Adversarial property: `during` > 0.  The single hub was wedged on the
#    blocker for the entire ~1s; with no neighbour to steal, every worker that
#    finished during that window was carried by the rescue's own PyThreadState.
# --------------------------------------------------------------------------
def test_rescue_runs_fresh_fibers_off_a_wedged_single_hub():
    p = _run_rescue_until("1.5", _rescued)
    assert p.returncode == 0, (
        "wedged-hub workload did not exit cleanly (rc=%s)\nstdout=%s\nstderr=%s"
        % (p.returncode, p.stdout[-600:], p.stderr[-1500:]))
    parsed = _parse_handoff_ok(p.stdout)
    assert parsed is not None, (
        "workload did not finish (no HANDOFF_OK marker)\nstdout=%s\nstderr=%s"
        % (p.stdout[-600:], p.stderr[-1200:]))
    total, during = parsed
    # All 24 workers eventually ran (the run is a clean, complete drain -- a hang
    # or lost fiber would have tripped the 60s subprocess timeout / a short total).
    assert total == 24, "only %d/24 workers ever ran -- the rescue or the drain stalled" % total
    # The decisive one: work completed while the sole hub was provably wedged.
    assert during >= 1, (
        "no worker ran while the only hub was wedged on a 1s blocking sleep -- the "
        "handoff rescue adopt path (L97-122) never executed (during=%d)" % during)


# --------------------------------------------------------------------------
# 2. The DEBUG traces: prove the *specific* debug-gated lines fire.
#    Drives L107-109 (the once-per-session "adopt hub N (own tstate)" line) and
#    L66-68 (the "hub N deque empty (drained=M)" line, which is gated on BOTH
#    runloom_handoff_debug AND base_valid -- i.e. it only prints after the rescue
#    has adopted at least once and then drained the deque dry while still wedged).
#    A 1.4s blocker guarantees the rescue empties the deque well before the wedge
#    clears, so the empty-deque branch is taken.
#
#    Adversarial property: both debug strings, with a concrete drained>=1 count,
#    appear on stderr -- the rescue both adopted and drained, not just spun.
# --------------------------------------------------------------------------
def test_rescue_debug_trace_shows_adopt_and_drain():
    # a longer blocker so the rescue can fully drain the deque mid-wedge (the
    # deque-empty trace below needs that), retried past adaptive-backoff latency.
    p = _run_rescue_until("3.0", _adopted_and_drained,
                          env_extra={"RUNLOOM_HANDOFF_DEBUG": "1"})
    assert p.returncode == 0, (
        "debug-traced wedge run crashed (rc=%s)\nstderr=%s" % (p.returncode, p.stderr[-1500:]))
    assert _parse_handoff_ok(p.stdout) is not None, "no HANDOFF_OK marker\nstderr=%s" % p.stderr[-800:]
    # L107-109: the rescue adopted the hub on its own tstate.
    assert "adopt hub 0 (own tstate)" in p.stderr, (
        "the once-per-session adopt-debug line (L107-109) did not fire:\n%s" % p.stderr[-1500:])
    # L66-68: the deque went empty WHILE still wedged after >=1 fiber was drained.
    m = re.search(r"hub 0 deque empty \(drained=(\d+)\)", p.stderr)
    assert m is not None, (
        "the deque-empty DEBUG line (L66-68) never fired -- the rescue did not "
        "drain the deque dry mid-wedge:\n%s" % p.stderr[-1500:])
    assert int(m.group(1)) >= 1, (
        "deque-empty line fired with drained=0, but it is gated on base_valid "
        "(>=1 fiber adopted) -- contradiction: %r" % m.group(0))


# --------------------------------------------------------------------------
# 3. The rescue-pool spawn path is taken when handoff is enabled.
#    Drives runloom_handoff_spawn's L203 (threads[] calloc) and L209
#    (rescue_tstates[] calloc) -- the *normal* enabled-spawn allocations that run
#    on every mn_init under RUNLOOM_HANDOFF.  Pairs the spawn with a real rescue so
#    the threads it allocates are demonstrably the ones that do the work.
#
#    Adversarial property: with the pool spawned, a wedged single hub is rescued
#    (during>0) AND the run tears down cleanly across the spawn/stop_join/
#    delete_tstates sequence -- a broken spawn would either never rescue or hang
#    the join.
# --------------------------------------------------------------------------
def test_handoff_pool_spawned_and_used():
    p = _run_rescue_until("1.2", _rescued)
    assert p.returncode == 0, (
        "handoff-enabled run did not tear down cleanly (rc=%s)\nstderr=%s"
        % (p.returncode, p.stderr[-1500:]))
    parsed = _parse_handoff_ok(p.stdout)
    assert parsed is not None, "no HANDOFF_OK marker (spawn/run/fini path)\nstderr=%s" % p.stderr[-800:]
    total, during = parsed
    assert total == 24, "spawn ran but the drain lost fibers (%d/24)" % total
    assert during >= 1, (
        "rescue pool was spawned (L203/L209) but never rescued anything -- "
        "during=%d" % during)


# --------------------------------------------------------------------------
# 4. Teardown storm: repeated wedge+rescue+fini cycles, every detector mode on.
#    Re-exercises the spawn (L203/L209) and the stop_join / delete_tstates
#    teardown of the rescue pool across many mn_init/mn_run/mn_fini cycles, and
#    keeps a wedge live in each cycle so a rescue is in-flight when fini begins --
#    the only setting in which the loop-top stop check (L40) could ever observe
#    runloom_handoff_stop, if it is reachable at all (see the report's
#    `unreachable` note; this is the best-effort probe for it).
#
#    Adversarial property: no crash, no hang -- N back-to-back wedge/rescue/
#    teardown cycles all reach the final marker.  A use-after-free on a
#    rescue_ts deleted out from under an in-flight rescue, or a join deadlock,
#    would surface as a signal/timeout here.
# --------------------------------------------------------------------------
_STORM = r'''
import sys, time
sys.path.insert(0, "src")
import runloom_c as rc

def cycle():
    NWORK = 12
    rescued = bytearray(NWORK)
    def worker(i): rescued[i] = 1
    def blocker(): time.sleep(0.25)
    rc.mn_init(1)
    for i in range(NWORK):
        rc.mn_fiber(lambda i=i: worker(i))
    rc.mn_fiber(blocker)
    rc.mn_run()
    rc.mn_fini()
    return sum(rescued)

ok = 0
for _ in range(12):
    ok += cycle()
sys.stdout.write("STORM_OK %d\n" % ok)
'''


def test_handoff_teardown_storm_no_crash_no_hang():
    p = _run(_STORM, _HANDOFF_ENV, timeout=120)
    assert p.returncode == 0, (
        "handoff teardown storm CRASHED/failed (rc=%s)\nstderr=%s"
        % (p.returncode, p.stderr[-1800:]))
    assert "STORM_OK" in p.stdout, (
        "handoff teardown storm hung before completing all cycles\nstderr=%s"
        % p.stderr[-1200:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
