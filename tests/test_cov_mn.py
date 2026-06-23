"""Coverage-driven unit tests for the M:N scheduler (src/runloom_c/mn_sched.c
and its mn_sched_*.c.inc fragments).

Half the M:N scheduler's lines live behind env-gated modes the normal corpus
never enables: the controlled-replay / PCT barrier, fiber_n bulk spawn, the sysmon
stalled-hub detector, the DETACHED-tstate handoff rescue, ATTACHED preemption,
the idle-condvar-vs-nanosleep wake, the stack-park idle sweep, world-yield,
hub-affinity, the io_uring-as-loop backend, and the gated-off migratable-mode
warn path.  Each `test_mn_mode_*` runs the shared diverse workload
(tests/cov_workload.py) in a subprocess with that mode's env set, driving its C
paths; the in-process tests cover the default-scheduler surfaces (varied hub
counts, fiber_n bulk, serve(), deadlock-raise, and the hubinfo/diag introspection).

The subprocess assertion is "the mode ran the workload to completion (exit 0,
WORKLOAD_OK) without crashing or hanging" -- the coverage benefit is the C lines
each mode exercises; correctness of the work itself is checked inside the
workload (it asserts no channel value was lost).
"""
import os
import subprocess
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
_DEVNULL = os.open(os.devnull, os.O_WRONLY)

# (label, extra-env) -- each drives a distinct gated path through mn_sched.c.
MODES = [
    ("default",      {}),
    ("barrier_pct",  {"RUNLOOM_MN_BARRIER": "1", "RUNLOOM_MN_SEED": "7", "RUNLOOM_MN_PCT": "8"}),
    ("gon_bulk",     {"RUNLOOM_GON_BULK": "1"}),
    ("sysmon",       {"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1",
                      "RUNLOOM_SYSMON_MS": "8", "RUNLOOM_COV_CPU": "40000000"}),
    ("preempt",      {"RUNLOOM_PREEMPT": "1", "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1",
                      "RUNLOOM_PREEMPT_MS": "8", "RUNLOOM_COV_CPU": "40000000"}),
    ("idle_wake_off", {"RUNLOOM_HUB_IDLE_WAKE": "0"}),
    ("stack_park_sweep", {"RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1"}),
    ("world_yield",  {"RUNLOOM_WORLD_YIELD_NS": "2000"}),
    ("hub_affinity", {"RUNLOOM_HUB_AFFINITY": "1"}),
    ("perg_tstate_warn", {"RUNLOOM_PER_G_TSTATE": "1"}),   # gated off -> warn + default sched
    ("iouring_loop", {"RUNLOOM_IOURING_LOOP": "1"}),
    ("deadlock_ms",  {"RUNLOOM_DEADLOCK_MS": "50"}),
    ("ready_starve", {"RUNLOOM_READY_STARVE_BOUND": "2"}),
    ("dbg_migrate",  {"RUNLOOM_DBG_MIGRATE": "1"}),
]


def _run_workload(env_extra, hubs=4, timeout=60):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    p = subprocess.run([PY, "tests/cov_workload.py", "--hubs", str(hubs)],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=timeout)
    return p


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.parametrize("label,env", MODES, ids=[m[0] for m in MODES])
def test_mn_scheduler_mode(label, env):
    p = _run_workload(env)
    assert p.returncode == 0, "mode %s crashed/failed (rc=%d)\nstderr=%s" % (
        label, p.returncode, p.stderr[-1500:])
    assert "WORKLOAD_OK" in p.stdout, "mode %s did not complete the workload\nout=%s err=%s" % (
        label, p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# in-process default-scheduler coverage
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
@pytest.mark.parametrize("hubs", [1, 2, 3, 8])
def test_mn_varied_hub_counts(hubs):
    from runloom.sync import WaitGroup
    N = 200
    done = bytearray(1)
    box = {"n": 0}
    def main():
        wg = WaitGroup(); wg.add(N)
        def w():
            try:
                rc.sched_yield()
            finally:
                wg.done()
        for _ in range(N):
            runloom.fiber(w)        # dispatches: single-thread go for run(1), mn_fiber for run(N>1)
        wg.wait()
    with hang_guard(40, "mn hubs=%d" % hubs):
        runloom.run(hubs, main)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_fiber_n_bulk_indexed():
    # fiber_n is the bulk/arena spawn path (mn_sched_init_fini.c.inc).
    seen = bytearray(256)
    def worker(i):
        if 0 <= i < 256:
            seen[i] = 1
    def main():
        rc.fiber_n(worker, 256, 0, True)    # indexed bulk spawn
    with hang_guard(40, "fiber_n bulk"):
        rc.mn_init(4); rc.mn_fiber(main); rc.mn_run(); rc.mn_fini()
    assert sum(seen) == 256


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_serve_echo():
    # serve() spawns SO_REUSEPORT acceptors + per-conn handler fibers (module_io
    # + the hub path); requires the M:N runtime.
    import socket
    result = {}
    def main():
        def handler(conn):
            data = conn.recv(64)
            conn.send_all(b"s:" + data)
            conn.close()
        port, listeners = rc.serve("127.0.0.1", 0, handler, 2, 64)
        result["port"] = port
        def client():
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"hi")
            result["reply"] = c.recv(64)
            c.close()
            for L in listeners:
                L.close()
        rc.mn_fiber(client)
    with hang_guard(30, "serve echo"):
        runloom.run(4, main)
    assert result.get("reply") == b"s:hi"


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_deadlock_raise():
    prev = rc.get_deadlock_mode()
    rc.set_deadlock_mode(2)               # raise
    try:
        def main():
            rc.mn_fiber(lambda: rc.Chan(0).recv())   # nobody sends -> deadlock
        rc.mn_init(2); rc.mn_fiber(main)
        with pytest.raises(RuntimeError):
            rc.mn_run()
    finally:
        rc.mn_fini()
        rc.set_deadlock_mode(prev)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_mn_hubinfo_and_diag_introspection():
    # mn_hub_states / fibers / dump_fibers / _dump_parkers / _diag_dump while gs
    # run + park -> mn_sched_hubinfo.c.inc + the netpoll diag.
    snap = {}
    def main():
        ch = rc.Chan(0)
        for _ in range(20):
            rc.mn_fiber(lambda: ch.recv())   # park on channel
        rc.sched_sleep(0.01)              # let them park
        snap["hubs"] = rc.mn_hub_states()
        snap["hub_count"] = rc.mn_hub_count()
        snap["fibers"] = len(rc.fibers())
        snap["self_check"] = rc._self_check(1)   # verbose
        rc.dump_fibers(_DEVNULL)
        rc._dump_parkers()
        rc._diag_dump(_DEVNULL)
        ch.close()                        # release
    with hang_guard(30, "mn hubinfo/diag"):
        runloom.run(3, main)
    assert snap["hub_count"] == 3
    assert isinstance(snap["hubs"], list) and len(snap["hubs"]) == 3
    assert snap["self_check"] == 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
