"""Adversarial stress of the M:N scheduler + epoll backend -- NOT the happy path.

Weaponises the runtime's built-in fault-injection points (RUNLOOM_FAULT_<SITE>=
once|always:<errno>) and the env-gated scheduler modes to manufacture the
conditions that break lock-free schedulers: a spawn that fails mid-storm
(admission-slot backout), I/O syscalls that error under a running workload,
teardown raced against in-flight gs, exception storms, fiber-admission
exhaustion, channel close raced against parked senders/receivers, and a
guard-page stack overflow on a hub.

Every scenario runs in a SUBPROCESS so a SIGSEGV/abort is contained and OBSERVED
(a negative returncode) rather than killing the suite.  The assertion is the
adversarial one: the runtime must NOT crash (signal) or hang (timeout) -- a
clean Python error is fine (it handled the injected fault), a signal/timeout is
a finding.  The one deliberate-crash scenario (stack overflow) asserts the crash
is a CLASSIFIED guard-page trap, not silent corruption.
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

SYSMON_ON = {"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "5"}
ALL_MODES = dict(SYSMON_ON, **{
    "RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "5",
    "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
    "RUNLOOM_HUB_IDLE_WAKE": "0", "RUNLOOM_WORLD_YIELD_NS": "2000",
})


def _run(env_extra, cmd, timeout=60):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run(cmd, cwd=REPO, env=env, capture_output=True, text=True, timeout=timeout)


def _assert_no_crash(p, label):
    # POSIX: a process killed by a signal returns the negative signal number.
    assert p.returncode is None or p.returncode >= 0, (
        "%s CRASHED with signal %d\nstdout=%s\nstderr=%s"
        % (label, -p.returncode, p.stdout[-400:], p.stderr[-1800:]))


# --------------------------------------------------------------------------
# spawn faults: the admission-slot backout / cleanup path under a storm
# --------------------------------------------------------------------------
_SPAWN_FAULT = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
def main():
    ran = [0]; failed = [0]
    def child(): ran[0] += 1
    for _ in range(400):
        try:
            rc.mn_fiber(child)
        except BaseException:
            failed[0] += 1
runloom.run(4, main)
sys.stdout.write("SPAWN_OK r=%d\n" % 0)
'''


@pytest.mark.skipif(not FT, reason="M:N")
@pytest.mark.parametrize("site", ["SPAWN_G", "SPAWN_STACK", "SPAWN_TSTATE"])
@pytest.mark.parametrize("spec", ["once:12", "always:12"])
def test_spawn_fault_no_crash(site, spec):
    p = _run({"RUNLOOM_FAULT_" + site: spec, "RUNLOOM_GOROUTINE_PANIC": "silent"},
             [PY, "-c", _SPAWN_FAULT])
    _assert_no_crash(p, "spawn-fault %s=%s" % (site, spec))


# --------------------------------------------------------------------------
# I/O faults under a running M:N workload (TCP/fd syscalls erroring)
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N")
@pytest.mark.parametrize("site,spec", [
    ("TCP_RECV", "once:104"), ("TCP_SEND", "once:32"),
    ("FD_READ", "once:5"), ("FD_WRITE", "once:5"),
    ("TCP_CONNECT", "once:111"), ("TCP_ACCEPT", "once:24"),
])
def test_io_fault_under_workload_no_crash(site, spec):
    p = _run(dict(SYSMON_ON, **{"RUNLOOM_FAULT_" + site: spec,
                                "RUNLOOM_GOROUTINE_PANIC": "silent"}),
             [PY, "tests/cov_workload.py", "--hubs", "4"])
    _assert_no_crash(p, "io-fault %s=%s" % (site, spec))


# --------------------------------------------------------------------------
# teardown raced against in-flight gs, under the detector modes
# --------------------------------------------------------------------------
_TEARDOWN_STORM = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
for cycle in range(40):
    rc.mn_init(8)
    seen = [0]
    def w(): seen[0] += 1
    for _ in range(120):
        rc.mn_fiber(w)
    rc.mn_run()
    rc.mn_fini()
sys.stdout.write("TEARDOWN_OK\n")
'''


@pytest.mark.skipif(not FT, reason="M:N")
def test_teardown_storm_under_detectors_no_hang():
    p = _run(ALL_MODES, [PY, "-c", _TEARDOWN_STORM], timeout=90)
    _assert_no_crash(p, "teardown storm")
    assert "TEARDOWN_OK" in p.stdout, "teardown storm hung/failed\nerr=%s" % p.stderr[-800:]


# --------------------------------------------------------------------------
# exception storm: half the gs raise, under every mode at once
# --------------------------------------------------------------------------
_EXC_STORM = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
def main():
    def boom(): raise ValueError("storm")
    def ok(): pass
    for i in range(600):
        rc.mn_fiber(boom if (i % 2) else ok)
runloom.run(4, main)
sys.stdout.write("EXC_OK\n")
'''


@pytest.mark.skipif(not FT, reason="M:N")
def test_exception_storm_all_modes_no_crash():
    p = _run(dict(ALL_MODES, RUNLOOM_GOROUTINE_PANIC="silent"),
             [PY, "-c", _EXC_STORM], timeout=90)
    _assert_no_crash(p, "exception storm")
    assert "EXC_OK" in p.stdout, "exception storm hung\nerr=%s" % p.stderr[-800:]


# --------------------------------------------------------------------------
# fiber-admission exhaustion under an M:N spawn storm
# --------------------------------------------------------------------------
_MAXFIB = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.set_max_fibers(16)
def main():
    ok = [0]; err = [0]
    def child(): rc.sched_yield()
    for _ in range(800):
        try:
            rc.mn_fiber(child); ok[0] += 1
        except RuntimeError:
            err[0] += 1
runloom.run(4, main)
rc.set_max_fibers(0)
sys.stdout.write("MAXFIB_OK\n")
'''


@pytest.mark.skipif(not FT, reason="M:N")
def test_fiber_admission_exhaustion_no_crash():
    p = _run(SYSMON_ON, [PY, "-c", _MAXFIB], timeout=60)
    _assert_no_crash(p, "max-fibers exhaustion")
    assert "MAXFIB_OK" in p.stdout, p.stderr[-800:]


# --------------------------------------------------------------------------
# channel close raced against parked senders + receivers, under M:N
# --------------------------------------------------------------------------
_CHAN_CLOSE = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
def main():
    for _ in range(25):
        ch = rc.Chan(0)
        wg = WaitGroup(); wg.add(40)
        def sender():
            try: ch.send(1)
            except ValueError: pass
            finally: wg.done()
        def receiver():
            try: ch.recv()
            finally: wg.done()
        for _ in range(20): rc.mn_fiber(sender)
        for _ in range(20): rc.mn_fiber(receiver)
        rc.sched_sleep(0.001)
        ch.close()
        wg.wait()
runloom.run(4, main)
sys.stdout.write("CHANCLOSE_OK\n")
'''


@pytest.mark.skipif(not FT, reason="M:N")
def test_channel_close_race_storm_no_crash():
    p = _run(SYSMON_ON, [PY, "-c", _CHAN_CLOSE], timeout=90)
    _assert_no_crash(p, "channel close race")
    assert "CHANCLOSE_OK" in p.stdout, "close-race hung\nerr=%s" % p.stderr[-800:]


# --------------------------------------------------------------------------
# deliberate guard-page stack overflow ON A HUB -> classified crash
# --------------------------------------------------------------------------
_OVERFLOW = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.install_crash_handler("backtrace")
def main():
    rc.mn_fiber(lambda: rc._crash_selftest_overflow(), 131072)   # small hub stack
runloom.run(2, main)
sys.stdout.write("UNREACHABLE\n")
'''


@pytest.mark.skipif(not FT, reason="M:N")
def test_hub_stack_overflow_is_classified_not_silent():
    p = _run({}, [PY, "-c", _OVERFLOW], timeout=30)
    assert p.returncode != 0 and "UNREACHABLE" not in p.stdout, "overflow did not crash"
    # the crash handler must CLASSIFY it as a guard-page overflow, not a bare SIGSEGV
    assert "STACK OVERFLOW" in p.stderr and "guard page" in p.stderr.lower(), (
        "hub overflow not classified by the crash handler:\n%s" % p.stderr[-1500:])


# --------------------------------------------------------------------------
# everything hostile at once: all modes + I/O fault + the full workload
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N")
def test_all_modes_plus_io_fault_no_crash():
    p = _run(dict(ALL_MODES, RUNLOOM_FAULT_FD_READ="once:5",
                  RUNLOOM_FAULT_TCP_SEND="once:32", RUNLOOM_GOROUTINE_PANIC="silent"),
             [PY, "tests/cov_workload.py", "--hubs", "6"], timeout=90)
    _assert_no_crash(p, "all-modes + io-fault")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
