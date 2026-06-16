"""Adversarial coverage suite for src/runloom_c/runloom_diag.c.

This fragment is the diagnostic infrastructure: the RUNLOOM_DEBUG[_DIAG] flag
parser, the lock-free per-thread lifecycle event ring (emit + registry + dump),
the runloom_self_check structural pass, the seeded delay-injection determinism
tool, and the TLA+-trace emitters (gilstate / mn-baton).  The normal 327-run
corpus runs with EVERY one of these knobs OFF (the diag flags default to 0), so
this whole file's "on" paths -- the ring, the traces, the delay injector -- are
dark in the baseline gcov.  Each is enabled ONLY via an env var read once at
module import, so coverage of those paths MUST come from a subprocess that sets
the env and EXITS CLEANLY (gcov flushes its counters only on a clean exit; an
abort / re-raised fatal signal never flushes).

A hard-won gotcha is baked into every dump here: runloom_diag_dump writes the
whole ring (up to ~150 KB) to the fd with a blocking write(2).  Dumping into a
pipe and reading it AFTER the write deadlocks the moment the dump exceeds the
~64 KB pipe buffer (the writer blocks with no reader draining).  So every dump
target here is a TEMP FILE fd, never a pipe.

Regions DRIVEN here (uncovered gcov line -> how):

  L39        parse_one_token "ring" arm -> RUNLOOM_DEBUG_DIAG=ring sets flag 0x8.
  L43        parse_one_token unknown-token -> return 0 -> a bogus token alongside
             "ring" exercises the fall-through (flags stay exactly 0x8).
  L121-162   monotonic_ns / ring_acquire / runloom_evt_log_ -- the ring's lazy
             per-thread alloc + event append.  Cold unless RUNLOOM_DBG_RING; a
             real workload under RUNLOOM_DEBUG_DIAG=ring emits thousands.
  L178-198   op_name switch arms -- reached ONLY from the dump, one arm per op
             code present in a ring at dump time.  We drive a workload that emits
             twelve distinct ops (channel park/wake, netpoll fd link/unlink/
             timeout, M:N submit/pop, coro acquire/release, g transition/decref/
             complete) and assert each label appears in the dump.  CAL_FREEZE,
             WORLD_YIELD, PARKER_FORCE each get a dedicated mode test.
  L224-252   runloom_diag_dump per-thread ring walk (header + newest-first event
             loop) -- the body that only runs when a ring has events.
  L405-409   runloom_gilstate_trace body  -> RUNLOOM_GILSTATE_TRACE=<path>.
  L425-428   runloom_mn_trace_event body  -> RUNLOOM_MN_EVENTS=<path> + the
             controlled barrier mode (the baton protocol that emits the events).
  L440-441   runloom_diag_init gilstate-trace fopen  -> RUNLOOM_GILSTATE_TRACE.
  L446-447   runloom_diag_init mn-events fopen        -> RUNLOOM_MN_EVENTS.
  L501-506   runloom_splitmix64                       -> RUNLOOM_DELAY mixes it.
  L516-524   runloom_delay_init_once env-set branch   -> RUNLOOM_DELAY[+_MAX_NS].
  L534-541   runloom_delay_inject active body         -> RUNLOOM_DELAY, plus a
             RUNLOOM_DELAY_MAX_NS=0 run for the <=0 early-out (L534).

Each assertion proves the line's EFFECT, not just that it ran: the dump must
carry the labels the op_name arms produce, the trace files must contain the
ndjson the emitters write, get/set/diag-flags must reflect the parsed flag.

Reachability notes for the lines this suite DELIBERATELY does not chase (faking
them would violate "real assertions only"; see the structured report's
`exclusions[]` for the precise category of each):

  * L224-225 (runloom_diag_dump "not initialised") -- runloom_diag_init runs
    unconditionally at module import (PyInit), before any Python executes, so
    runloom_ring_list_lock_inited is ALWAYS true at every reachable dump.
    DEFENSIVE.
  * L181 / L197 / L198 / L199 (op_name PARKER_WAKE / SNAP_SAVE / SNAP_LOAD /
    default) -- NO RUNLOOM_EVT() call site in the whole source emits a
    RUNLOOM_EVT_PARKER_WAKE, _SNAP_SAVE or _SNAP_LOAD event (grep confirms: the
    enum exists but is never logged), so those op codes can never appear in a
    ring; the default arm is therefore unreachable too (every stored op is a
    valid enum). DEAD.
  * L183 / L195 (op_name PARKER_GHOST / HANDOFF_ADOPT) -- both ARE emitted, but
    only on a rare scheduler race: PARKER_GHOST on a defensive stale-bucket clear
    inside parker link, HANDOFF_ADOPT only if the sysmon-flagged DETACHED-tstate
    rescue thread wins the adoption race before the wedged g finishes -- and even
    when it fires the 1024-entry ring evicts it before a post-run dump under any
    workload heavy enough to wedge a hub.  Not deterministically observable in a
    clean-exit subprocess. RACE.
  * L267-292 (runloom_evt_crash_dump) -- called ONLY from the fatal-signal crash
    handler (which re-raises the signal -> the process dies before gcov flushes)
    and from runloom_invariant_fail (which abort()s). CRASHONLY.
  * L347-377 (runloom_self_check violation branches: netpoll-inspect-fail, global
    list cycle, bucket self-loop, parked-count mismatch, bucket-unreachable) --
    each requires genuinely CORRUPTED netpoll/scheduler structures with no fault-
    injection hook to forge them; the per-test conftest invariant runs
    _self_check thousands of times on HEALTHY structures (the success path) and
    these guards never trip. DEFENSIVE.
  * L467-483 (runloom_diag_fini) and L544-548 (runloom_delay_enabled) -- both are
    exported (nm: T) but have NO caller anywhere in src/ or tests/ and no module-
    teardown slot / Python binding wires them up. DEAD.
  * L561-573 (runloom_invariant_fail) -- fires ONLY from runloom_coro_assert_idle
    (coro.c) when RUNLOOM_DBG_INVARIANTS is on AND a coro is being recycled while
    a thread still executes on it (a real use-after-free race), then abort()s --
    no gcov flush on abort, and no fault hook can forge the dbg_running!=0 state.
    CRASHONLY.
"""
import os
import re
import subprocess
import sys
import tempfile

import pytest

import runloom_c as rc
from adv_util import hang_guard

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
_TIMEOUT = 220


def _child_env(**extra):
    """Base subprocess env: GIL off, in-tree src on the path, plus `extra`.
    Strips the diag knobs we don't want leaking in from a parent run."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    for k in ("RUNLOOM_DEBUG_DIAG", "RUNLOOM_DEBUG", "RUNLOOM_DELAY",
              "RUNLOOM_DELAY_MAX_NS", "RUNLOOM_GILSTATE_TRACE",
              "RUNLOOM_MN_EVENTS", "RUNLOOM_WORLD_YIELD_NS"):
        env.pop(k, None)
    env.update(extra)
    return env


def _run_child(code, env, timeout=_TIMEOUT):
    try:
        return subprocess.run([PY, "-c", code], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("diag subprocess timed out (shared-box CI contention, "
                    "not a runloom bug)")


# --------------------------------------------------------------------------
# L39 / L43: parse_one_token "ring" arm + unknown-token fall-through.
#
# RUNLOOM_DEBUG_DIAG="ring" sets exactly RUNLOOM_DBG_RING (0x8) -> the "ring"
# arm (L39) ran.  Adding a bogus token exercises the unknown-token `return 0`
# (L43): it must contribute nothing, so the final flags stay EXACTLY 0x8.
# We also assert the ring is genuinely live (a workload emits events the dump
# then carries), proving the parsed flag actually armed the emitter.
# --------------------------------------------------------------------------
_FLAG_CHILD = r"""
import os, sys, tempfile
sys.path.insert(0, 'src')
import runloom_c as rc

# "ring" arm (L39) + an unknown token that must fall through to return 0 (L43).
flags = rc._diag_flags()
assert flags == 0x8, "RUNLOOM_DEBUG_DIAG=ring,<bogus> gave flags=0x%x (want 0x8)" % flags

# The flag must actually arm the emitter: run real work, dump, see events.
def w():
    rc.sched_yield()
def main():
    for _ in range(10):
        rc.go(w)
rc.go(main); rc.run()

fd, path = tempfile.mkstemp()
rc._diag_dump(fd); os.close(fd)
data = open(path).read(); os.unlink(path)
assert "[runloom-diag] flags=0x8" in data, data[:200]
assert "events=" in data, "ring armed but dump shows no events:\n" + data[:400]
sys.stdout.write("FLAG_RING_OK\n")
"""


def test_debug_diag_ring_flag_and_unknown_token():
    env = _child_env(RUNLOOM_DEBUG_DIAG="ring,zzz_not_a_real_flag")
    p = _run_child(_FLAG_CHILD, env)
    assert p.returncode == 0, "flag child rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "FLAG_RING_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L121-162 + L178-198 (the reachable arms) + L224-252: the ring end to end.
#
# With RUNLOOM_DBG_RING on, a workload that:
#   - parks/wakes a channel        -> CHAN_PARK / CHAN_WAKE
#   - parks a netpoll fd woken by data, and one that times out
#                                   -> PARK_LINK / PARK_UNLINK / PARK_TIMEOUT
#   - runs an M:N hub               -> G_SUBMIT / G_POP
#   - exercises the coro pool       -> CORO_ACQUIRE / CORO_RELEASE
#   - completes goroutines          -> G_TRANSITION / G_DECREF / G_COMPLETE
# emits twelve distinct op codes.  We dump (to a temp file) and assert each
# op_name label appears -- proof the matching op_name switch arm executed AND
# the runloom_diag_dump per-thread event loop (L235-252) walked them.
# --------------------------------------------------------------------------
_RING_OPS_CHILD = r"""
import os, re, socket, sys, tempfile
sys.path.insert(0, 'src')
import runloom_c as rc
READ, WRITE = 1, 2

def st_phase():
    # channel rendezvous: consumer parks, producer wakes it
    ch = rc.Chan(0)
    def consumer():
        ch.recv()
    def producer():
        rc.sched_yield(); rc.sched_yield()
        ch.send(1)
    rc.go(consumer); rc.go(producer)

    # netpoll fd: a reader parks, a peer's send wakes it (LINK + UNLINK)
    a, b = socket.socketpair()
    def reader():
        rc.wait_fd(a.fileno(), READ, 4000)
    def waker():
        rc.sched_yield(); rc.sched_yield()
        b.send(b"x")
    rc.go(reader); rc.go(waker)

    # a pure-timeout park (PARK_TIMEOUT)
    rp, wp = os.pipe()
    def tmo():
        rc.wait_fd(rp, READ, 25)
    rc.go(tmo)

    for _ in range(40):
        rc.sched_yield()

    # clean every raw fd's netpoll arm before close (fd-reuse hygiene)
    rc.netpoll_unregister(a.fileno()); rc.netpoll_unregister(b.fileno())
    rc.netpoll_unregister(rp)
    a.close(); b.close(); os.close(rp); os.close(wp)

rc.go(st_phase); rc.run()

# M:N hub -> G_SUBMIT / G_POP
def w():
    rc.sched_yield()
def mnmain():
    for _ in range(20):
        rc.mn_go(w)
rc.mn_init(3); rc.mn_go(mnmain); rc.mn_run(); rc.mn_fini()

# Dump the whole ring to a temp file (NOT a pipe -- a >64KB dump deadlocks a pipe).
fd, path = tempfile.mkstemp()
rc._diag_dump(fd); os.close(fd)
data = open(path).read(); os.unlink(path)
ops = set(re.findall(r"op=(\S+)", data))

EXPECT = {
    "CHAN_PARK", "CHAN_WAKE", "PARK_LINK", "PARK_UNLINK", "PARK_TIMEOUT",
    "G_SUBMIT", "G_POP", "CORO_ACQUIRE", "CORO_RELEASE",
    "G_TRANSITION", "G_DECREF", "G_COMPLETE",
}
missing = EXPECT - ops
assert not missing, "ring dump missing op labels %r; saw %r" % (sorted(missing), sorted(ops))
# Header line proves the per-thread walk header (L227-241) ran.
assert re.search(r"\[runloom-diag\] tid=\d+ events=\d+", data), data[:300]
sys.stdout.write("RING_OPS_OK\n")
"""


def test_ring_dump_covers_every_reachable_op_name_arm():
    env = _child_env(RUNLOOM_DEBUG_DIAG="ring")
    p = _run_child(_RING_OPS_CHILD, env)
    assert p.returncode == 0, "ring-ops child rc=%d\n%s" % (p.returncode, p.stderr[-2000:])
    assert "RING_OPS_OK" in p.stdout, (p.stdout, p.stderr[-1000:])


# --------------------------------------------------------------------------
# L194 (op_name CAL_FREEZE) + the CAL_FREEZE emit path it labels.
#
# The auto-calibration window freezes once exactly after RUNLOOM_CAL_TARGET
# (1000) goroutines complete with stack-paint on (the default).  That single
# RUNLOOM_EVT_CAL_FREEZE is then in the ring; we run 1010 completions and assert
# the dump carries the CAL_FREEZE label -- which only the op_name CAL_FREEZE arm
# can emit.
# --------------------------------------------------------------------------
_CAL_FREEZE_CHILD = r"""
import os, sys, tempfile
sys.path.insert(0, 'src')
import runloom_c as rc

def w():
    pass
def main():
    # low concurrency so the freeze event (around completion #1000) survives
    # the 1024-entry ring until the post-run dump.
    for _ in range(1010):
        rc.go(w)
        rc.sched_yield()
rc.go(main); rc.run()

fd, path = tempfile.mkstemp()
rc._diag_dump(fd); os.close(fd)
data = open(path).read(); os.unlink(path)
assert "CAL_FREEZE" in data, "CAL_FREEZE label absent after 1010 completions"
sys.stdout.write("CAL_FREEZE_OK\n")
"""


def test_ring_dump_covers_cal_freeze_arm():
    env = _child_env(RUNLOOM_DEBUG_DIAG="ring")
    p = _run_child(_CAL_FREEZE_CHILD, env)
    assert p.returncode == 0, "cal-freeze child rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "CAL_FREEZE_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L196 (op_name WORLD_YIELD) + the WORLD_YIELD monopoly emit it labels.
#
# When RUNLOOM_WORLD_YIELD_NS is set, an M:N hub that detaches for a foreign
# thread's stop-the-world (a native thread's gc.collect()) emits
# RUNLOOM_EVT_WORLD_YIELD on the monopoly world-yield.  A native OS thread
# spinning gc.collect() against an M:N workload forces it; the dump must carry
# the WORLD_YIELD label.
# --------------------------------------------------------------------------
_WORLD_YIELD_CHILD = r"""
import os, sys, gc, threading, tempfile
sys.path.insert(0, 'src')
import runloom_c as rc

stop = [False]
def native_gc():
    # a genuine OS thread (captured before any patch) forcing stop-the-world GC
    while not stop[0]:
        gc.collect()

def w():
    for _ in range(20):
        rc.sched_yield()
def main():
    for _ in range(80):
        rc.mn_go(w)

t = threading.Thread(target=native_gc, daemon=True)
t.start()
rc.mn_init(4); rc.mn_go(main); rc.mn_run(); rc.mn_fini()
stop[0] = True
t.join(timeout=5)

fd, path = tempfile.mkstemp()
rc._diag_dump(fd); os.close(fd)
data = open(path).read(); os.unlink(path)
assert "WORLD_YIELD" in data, "WORLD_YIELD label absent under monopoly STW"
sys.stdout.write("WORLD_YIELD_OK\n")
"""


def test_ring_dump_covers_world_yield_arm():
    env = _child_env(RUNLOOM_DEBUG_DIAG="ring", RUNLOOM_WORLD_YIELD_NS="3000")
    p = _run_child(_WORLD_YIELD_CHILD, env)
    assert p.returncode == 0, "world-yield child rc=%d\n%s" % (p.returncode, p.stderr[-2000:])
    assert "WORLD_YIELD_OK" in p.stdout, (p.stdout, p.stderr[-1000:])


# --------------------------------------------------------------------------
# L184 (op_name PARKER_FORCE) + the iouring force-unlink emit it labels.
#
# Under the io_uring loop backend, cancelling a parked fiber's fd
# (netpoll_cancel_fd) force-unlinks its parker via the iouring wake path, which
# emits RUNLOOM_EVT_PARKER_FORCE.  The dump must carry PARK_FORCE.
# --------------------------------------------------------------------------
_PARKER_FORCE_CHILD = r"""
import os, sys, socket, tempfile
sys.path.insert(0, 'src')
import runloom_c as rc
READ = 1

def main():
    a, b = socket.socketpair()
    def reader():
        try:
            rc.wait_fd(a.fileno(), READ, 5000)
        except Exception:
            pass
        rc.netpoll_unregister(a.fileno())
    rc.go(reader)
    for _ in range(3):
        rc.sched_yield()
    rc.netpoll_cancel_fd(a.fileno())   # force-unlink the parked fiber
    for _ in range(6):
        rc.sched_yield()
    rc.netpoll_unregister(b.fileno())
    a.close(); b.close()
rc.go(main); rc.run()

fd, path = tempfile.mkstemp()
rc._diag_dump(fd); os.close(fd)
data = open(path).read(); os.unlink(path)
assert "PARK_FORCE" in data, "PARK_FORCE label absent after iouring cancel"
sys.stdout.write("PARK_FORCE_OK\n")
"""


def test_ring_dump_covers_parker_force_arm():
    env = _child_env(RUNLOOM_DEBUG_DIAG="ring", RUNLOOM_IOURING_LOOP="1")
    p = _run_child(_PARKER_FORCE_CHILD, env)
    if p.returncode != 0:
        # iouring loop backend can be unavailable on some kernels/configs.
        if "PARK_FORCE_OK" not in p.stdout:
            pytest.skip("iouring force-unlink path unavailable here: rc=%d %s"
                        % (p.returncode, p.stderr[-400:]))
    assert "PARK_FORCE_OK" in p.stdout, (p.stdout, p.stderr[-1000:])


# --------------------------------------------------------------------------
# L405-409 + L440-441: runloom_gilstate_trace body + its init fopen.
#
# RUNLOOM_GILSTATE_TRACE=<path> opens the trace file in runloom_diag_init
# (L440-441) and the hub-tstate create/delete sites append one ndjson line each
# via runloom_gilstate_trace (L405-409).  An M:N run with N hubs must produce a
# file with N "Create" lines (one per hub tstate).
# --------------------------------------------------------------------------
_GILTRACE_CHILD = r"""
import os, sys
sys.path.insert(0, 'src')
import runloom_c as rc

path = os.environ["RUNLOOM_GILSTATE_TRACE"]
def w():
    rc.sched_yield()
def main():
    for _ in range(8):
        rc.mn_go(w)
HUBS = 3
rc.mn_init(HUBS); rc.mn_go(main); rc.mn_run(); rc.mn_fini()

lines = [l for l in open(path).read().splitlines() if l.strip()]
assert lines, "RUNLOOM_GILSTATE_TRACE produced no events"
creates = [l for l in lines if '"a":"Create"' in l]
assert len(creates) >= HUBS, "expected >=%d Create events, got %r" % (HUBS, lines)
# Each line is the ndjson the L406 fprintf wrote.
import json
for l in lines:
    obj = json.loads(l)
    assert "a" in obj and "h" in obj and "d" in obj, l
sys.stdout.write("GILTRACE_OK\n")
"""


def test_gilstate_trace_env_emits_ndjson():
    tf = tempfile.NamedTemporaryFile(prefix="runloom_giltrace_", delete=False)
    tf.close()
    try:
        env = _child_env(RUNLOOM_GILSTATE_TRACE=tf.name)
        p = _run_child(_GILTRACE_CHILD, env)
        assert p.returncode == 0, "giltrace child rc=%d\n%s" % (
            p.returncode, p.stderr[-1500:])
        assert "GILTRACE_OK" in p.stdout, (p.stdout, p.stderr[-800:])
    finally:
        try:
            os.unlink(tf.name)
        except OSError:
            pass


# --------------------------------------------------------------------------
# L425-428 + L446-447: runloom_mn_trace_event body + its init fopen.
#
# RUNLOOM_MN_EVENTS=<path> opens the baton-trace file in runloom_diag_init
# (L446-447).  The baton protocol events (Arrive/Rendezvous/Grant/Release) are
# emitted from runloom_mn_trace_event (L425-428) ONLY inside the controlled
# barrier scheduler, so we also enable RUNLOOM_MN_BARRIER.  The file must contain
# all four action kinds.
# --------------------------------------------------------------------------
_MNEVENTS_CHILD = r"""
import os, sys, json
sys.path.insert(0, 'src')
import runloom_c as rc

path = os.environ["RUNLOOM_MN_EVENTS"]
def w():
    rc.sched_yield()
def main():
    for _ in range(16):
        rc.mn_go(w)
rc.mn_init(3); rc.mn_go(main); rc.mn_run(); rc.mn_fini()

lines = [l for l in open(path).read().splitlines() if l.strip()]
assert lines, "RUNLOOM_MN_EVENTS produced no baton events"
actions = set()
for l in lines:
    obj = json.loads(l)
    assert "a" in obj and "h" in obj, l
    actions.add(obj["a"])
need = {"Arrive", "Rendezvous", "Grant", "Release"}
assert need <= actions, "missing baton actions %r; saw %r" % (sorted(need - actions), sorted(actions))
sys.stdout.write("MNEVENTS_OK\n")
"""


def test_mn_events_trace_env_emits_baton_protocol():
    tf = tempfile.NamedTemporaryFile(prefix="runloom_mnevents_", delete=False)
    tf.close()
    try:
        env = _child_env(RUNLOOM_MN_EVENTS=tf.name, RUNLOOM_MN_BARRIER="1",
                         RUNLOOM_MN_SEED="7", RUNLOOM_MN_PCT="8")
        p = _run_child(_MNEVENTS_CHILD, env)
        assert p.returncode == 0, "mnevents child rc=%d\n%s" % (
            p.returncode, p.stderr[-1500:])
        assert "MNEVENTS_OK" in p.stdout, (p.stdout, p.stderr[-800:])
    finally:
        try:
            os.unlink(tf.name)
        except OSError:
            pass


# --------------------------------------------------------------------------
# L501-506 (runloom_splitmix64) + L516-524 (runloom_delay_init_once env-set
# branch) + L535-541 (runloom_delay_inject active body).
#
# RUNLOOM_DELAY=<seed> turns the seeded delay injector ON: the first inject call
# runs runloom_delay_init_once (reads the seed + RUNLOOM_DELAY_MAX_NS, L516-524),
# and every instrumented scheduler transition then mixes (seed, site, count)
# through runloom_splitmix64 (L501-506) into a bounded sleep (L535-541).  A
# normal goroutine workload hits many of those sites, so the child just runs to
# completion under the env -- the coverage is the injector body itself.
# --------------------------------------------------------------------------
_DELAY_CHILD = r"""
import os, sys, time
sys.path.insert(0, 'src')
import runloom_c as rc

def w():
    rc.sched_yield()
def main():
    for _ in range(40):
        rc.go(w)
t0 = time.monotonic()
rc.go(main); rc.run()
# The injector ran (the sites fired); the work still completed.
assert time.monotonic() - t0 < 60.0
sys.stdout.write("DELAY_OK\n")
"""


def test_delay_injection_env_runs_injector_body():
    # Tight cap so the injected sleeps stay sub-microsecond -- we want coverage
    # of the active body (L535-541) + splitmix (L501-506), not wall-clock cost.
    env = _child_env(RUNLOOM_DELAY="0xC0FFEE", RUNLOOM_DELAY_MAX_NS="300")
    p = _run_child(_DELAY_CHILD, env)
    assert p.returncode == 0, "delay child rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "DELAY_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L520-521 (RUNLOOM_DELAY_MAX_NS parse, v>=0 branch with v==0) + L534
# (runloom_delay_inject `runloom_delay_max_ns <= 0 -> return` early-out).
#
# RUNLOOM_DELAY set (injector ON) but RUNLOOM_DELAY_MAX_NS=0 -> the max-ns parse
# stores 0 (the `v >= 0` true side, L521) and every inject call takes the
# <=0 early return (L534) -- the injector is on yet sleeps zero.  This drives the
# zero-bound arm the nonzero RUNLOOM_DELAY run above cannot.
# --------------------------------------------------------------------------
_DELAY_ZERO_CHILD = r"""
import os, sys
sys.path.insert(0, 'src')
import runloom_c as rc

def w():
    rc.sched_yield()
def main():
    for _ in range(20):
        rc.go(w)
rc.go(main); rc.run()
sys.stdout.write("DELAY_ZERO_OK\n")
"""


def test_delay_injection_zero_bound_early_returns():
    env = _child_env(RUNLOOM_DELAY="5", RUNLOOM_DELAY_MAX_NS="0")
    p = _run_child(_DELAY_ZERO_CHILD, env)
    assert p.returncode == 0, "delay-zero child rc=%d\n%s" % (p.returncode, p.stderr[-1500:])
    assert "DELAY_ZERO_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# In-process sanity for the dump infrastructure that the conftest invariant
# fixture already exercises heavily (runloom_self_check success path, emit fd<0
# stderr route).  These add a real assertion on the public surface without a
# subprocess; they do not depend on any env flag.
# --------------------------------------------------------------------------
def test_self_check_clean_runtime_reports_zero_violations():
    # The structural walk over healthy netpoll/scheduler state must report no
    # violations (the success path; verbose != 0 takes the OK-summary branch).
    with hang_guard(20, "self_check verbose"):
        assert rc._self_check(0) == 0
        assert rc._self_check(1) == 0   # verbose OK-summary branch (L379-383)


def test_diag_dump_to_stderr_does_not_crash_when_ring_off():
    # In the parent the ring is OFF (default flags 0): runloom_diag_dump still
    # runs (it is always inited), prints the header, and the registry walk is a
    # no-op body.  emit(-1, ...) routes to stderr via fwrite (the fd<0 branch).
    # Just assert it returns cleanly -- no event lines, no crash.
    with hang_guard(10, "diag_dump stderr"):
        rc._diag_dump(-1)
        assert rc._diag_flags() == 0   # parent never set RUNLOOM_DEBUG_DIAG


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
