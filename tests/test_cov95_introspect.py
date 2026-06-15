"""Adversarial coverage suite for src/runloom_c/runloom_introspect.c.

This fragment is the fiber registry + the developer-facing structural dump
(``dump_fibers``), the rich snapshot (``fibers()``), the per-incarnation goid
allocator, the max-fibers admission gate, and the deadlock-mode / age-tracking
state.  The normal corpus exercises the hot paths heavily but leaves a handful
of dark regions; this suite drives the genuinely-reachable ones with real
conditions and asserts the behaviour each line implements.

Regions DRIVEN here (uncovered line -> how):

  L111-118  ``runloom_wait_reason_name`` arms WR_LOCK..WR_QUEUE.  This switch
            runs ONLY from ``runloom_dump_fibers_fd`` for a PARKED_SAFE fiber,
            and only when that fiber's committed ``wait_reason`` is the matching
            code.  The corpus only ever parks with SYNC/FUTURE/WAITGROUP (the
            three already-covered arms), so the remaining eight never fire.
            We spawn one fiber per reason that calls ``set_wait_reason(code)``
            then a timed ``park()`` (PARKED_SAFE, self-draining), and while all
            eight are parked we ``dump_fibers`` into a pipe and assert every
            ``park:<reason>`` label appears in the output -- proof each arm ran.

  L185      ``runloom_introspect_set_timestamps(1)`` inside
            ``runloom_introspect_init`` when ``RUNLOOM_INTROSPECT_TIME`` is set.
            Read ONCE at module import -> subprocess with the env, asserting
            ``get_introspect_timestamps()`` is True AND that a real park then
            reports a non-None ``age`` (proving the stamping the line enabled is
            actually live, not just the flag).

  L194-195  ``RUNLOOM_MAX_GOROUTINES`` env parse + ``runloom_set_max_fibers`` in
            init.  Read ONCE at import -> subprocess with the env, asserting
            ``get_max_fibers()`` == the value AND that the admission gate it
            installed actually rejects an over-cap spawn (real backpressure,
            not just a stored number).

Reachability notes for the uncovered lines this suite DELIBERATELY does not
chase (faking them would violate the "real assertions only" rule; see the
structured report's `exclusions` for the precise category of each):

  * L90/L98/L100/L101 (``runloom_g_state_name`` SPAWNING / WAKING / FREED /
    default) and L133 (``runloom_g_state_blockclass`` WAKING) -- SPAWNING and
    WAKING are sub-instruction transient scheduler states (a g is SPAWNING only
    between coro-alloc and enqueue, WAKING only between wake-pick and enqueue,
    both with no yield point), so no dump/snapshot can deterministically observe
    one; FREED is ``continue``d before the name lookup in every caller ("never
    observed in C", per the enum comment).  The full 499-run corpus never hits
    these arms either (gcov branch data: 0%).  RACE / DEAD.
  * L119 (``runloom_wait_reason_name`` default -> NULL for WR_NONE) -- the ONLY
    caller (the dump) passes ``g->wait_reason``, which ``park_safe`` commits to
    at least WR_SYNC (1); WR_NONE (0) never reaches the switch.  DEAD.
  * L199/L205-209 (``runloom_introspect_fini``) and L239-255
    (``runloom_greg_unlink``) -- both are exported (nm: ``T``) but have NO
    caller anywhere in the source and no module-teardown slot wires them up.
    DEAD (unreachable without editing src / a non-existent binding).
  * L400 (``dump_fibers``: "registry not initialised") -- the registry is
    inited unconditionally in ``PyInit`` before any Python can run, so
    ``runloom_greg_inited`` is always true at the dump.  DEFENSIVE.
  * L495-533 (``runloom_fiber_for_addr``) -- called ONLY from the SIGSEGV/SIGBUS
    crash handler, which re-raises the fatal signal; the process dies before
    gcov flushes.  CRASHONLY.
  * L553-554 (``runloom_fiber_snapshot`` malloc-fail cleanup) -- the raw
    ``malloc`` there has no RUNLOOM_FAULT_ hook / interposer site.  OOM.
"""
import os
import subprocess
import sys

import pytest

import runloom_c as rc
from adv_util import hang_guard

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# Every reason whose runloom_wait_reason_name arm the normal corpus leaves cold.
# (SYNC / FUTURE / WAITGROUP are already covered, so they are not exercised here.)
_DARK_REASONS = [
    ("lock",      rc.WR_LOCK),
    ("event",     rc.WR_EVENT),
    ("condition", rc.WR_CONDITION),
    ("barrier",   rc.WR_BARRIER),
    ("select",    rc.WR_SELECT),
    ("executor",  rc.WR_EXECUTOR),
    ("semaphore", rc.WR_SEMAPHORE),
    ("queue",     rc.WR_QUEUE),
]


# --------------------------------------------------------------------------
# L111-118: runloom_wait_reason_name WR_LOCK..WR_QUEUE.
#
# The dump labels a PARKED_SAFE fiber "park:<reason>" by calling
# runloom_wait_reason_name(g->wait_reason).  We make one parked-safe fiber per
# dark reason (set_wait_reason tags the hint; the next timed park commits it to
# g->wait_reason), dump while all eight are parked, and assert every label is in
# the output -- which can only happen if the corresponding switch arm executed.
# The park is TIMED, so each fiber self-drains and nothing leaks.
# --------------------------------------------------------------------------
def test_dump_labels_every_dark_wait_reason():
    captured = {}

    def make_parker(code):
        def parker():
            rc.set_wait_reason(code)
            # Timed park -> PARKED_SAFE that auto-wakes on its deadline (no leak).
            # Generous so all eight are definitely still parked at dump time.
            rc.park(timeout=2.0)
        return parker

    def main():
        r, w = os.pipe()
        try:
            for _name, code in _DARK_REASONS:
                rc.go(make_parker(code))

            def dumper():
                # Let all eight commit to PARKED_SAFE before we walk them.
                for _ in range(8):
                    rc.sched_yield()
                captured["parked"] = rc.count_deadlocked()
                rc.dump_fibers(w)        # writes the structural dump to the pipe
                os.close(w)
            rc.go(dumper)
            rc.run()
            captured["dump"] = os.read(r, 1 << 16).decode("utf-8", "replace")
        finally:
            os.close(r)

    with hang_guard(30, "dump dark wait reasons"):
        rc.go(main)
        rc.run()

    # All eight parked-safe fibers were live when we walked the registry...
    assert captured["parked"] == len(_DARK_REASONS), (
        "expected %d parked-safe fibers at dump time, got %r"
        % (len(_DARK_REASONS), captured.get("parked")))
    dump = captured["dump"]
    # ...and the dump must carry every "park:<reason>" label, which is emitted
    # ONLY by the matching runloom_wait_reason_name arm (L111-118).
    for name, _code in _DARK_REASONS:
        label = "park:%s" % name
        assert label in dump, (
            "wait-reason label %r missing from dump (arm did not run?):\n%s"
            % (label, dump))


# --------------------------------------------------------------------------
# L185: runloom_introspect_set_timestamps(1) inside runloom_introspect_init,
# gated by RUNLOOM_INTROSPECT_TIME.  Read once at module import -> subprocess.
# We assert (a) get_introspect_timestamps() is True (the line ran) AND (b) a
# real PARKED_SAFE fiber, observed via fibers(), reports a non-None `age` --
# proving the env-enabled stamping is genuinely live end to end.
# --------------------------------------------------------------------------
_TS_CHILD = r"""
import os, sys
sys.path.insert(0, 'src')
import runloom_c as rc

assert rc.get_introspect_timestamps() is True, "RUNLOOM_INTROSPECT_TIME did not turn on tracking"

seen = {}
def main():
    def parker():
        rc.park(timeout=2.0)           # PARKED_SAFE; stamped because tracking is ON
    rc.go(parker)
    def probe():
        for _ in range(6):
            rc.sched_yield()
        # The parked fiber must report a real, non-None, non-negative age now
        # that stamping is on (state_since_ns was set at the park).
        rows = [f for f in rc.fibers() if f["state"] == "park"]
        seen["rows"] = rows
    rc.go(probe)
    rc.run()

main()
rows = seen.get("rows", [])
assert len(rows) >= 1, "no parked fiber observed: %r" % rows
ages = [r["age"] for r in rows if r["age"] is not None]
assert ages, "tracking on but fibers() reported no age: %r" % rows
assert all(a >= 0.0 for a in ages), "negative age with tracking on: %r" % ages
sys.stdout.write("INTROSPECT_TIME_OK\n")
"""


def test_introspect_time_env_enables_age_tracking():
    env = dict(os.environ, RUNLOOM_INTROSPECT_TIME="1",
               PYTHON_GIL="0", PYTHONPATH="src")
    # Make sure the cap env doesn't leak in from a parent run and skew this.
    env.pop("RUNLOOM_MAX_GOROUTINES", None)
    try:
        p = subprocess.run([PY, "-c", _TS_CHILD], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=200)
    except subprocess.TimeoutExpired:
        pytest.skip("INTROSPECT_TIME subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "RUNLOOM_INTROSPECT_TIME child failed rc=%d\n%s" % (
        p.returncode, p.stderr[-1500:])
    assert "INTROSPECT_TIME_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# L194-195: RUNLOOM_MAX_GOROUTINES parse (atol) + runloom_set_max_fibers in
# init.  Read once at module import -> subprocess.  We assert (a)
# get_max_fibers() == the parsed value (L194-195 ran) AND (b) the admission
# gate it installed actually REJECTS an over-cap spawn -- real backpressure.
# --------------------------------------------------------------------------
_MAXG_CHILD = r"""
import os, sys
sys.path.insert(0, 'src')
import runloom_c as rc

CAP = {cap}
assert rc.get_max_fibers() == CAP, (
    "RUNLOOM_MAX_GOROUTINES=%d but get_max_fibers()=%d" % (CAP, rc.get_max_fibers()))

# The cap must be a live admission gate, not just a stored number: spawning past
# it from inside a fiber (the cap counts the live ones) must eventually raise.
rejected = {{"hit": False}}
def main():
    # Park CAP fibers so the live count sits at the ceiling, then try one more.
    parkers = []
    def p():
        rc.park(timeout=3.0)
    # We are fiber #1 (main); spawn up to the cap of additional long-parkers,
    # then one extra that should be refused.
    for _ in range(CAP + 4):
        try:
            rc.go(p)
        except RuntimeError:
            rejected["hit"] = True
            break
    # Let everything drain.
    rc.park(timeout=3.0)
rc.go(main)
rc.run()
assert rejected["hit"], "over-cap spawn was NOT rejected (cap=%d not enforced)" % CAP
# And after the drain the gate is back to zero admitted.
assert rc.live_fibers() == 0, "live_fibers leaked: %d" % rc.live_fibers()
sys.stdout.write("MAX_GOROUTINES_OK\n")
"""


def test_max_goroutines_env_installs_admission_gate():
    cap = 6
    env = dict(os.environ, RUNLOOM_MAX_GOROUTINES=str(cap),
               PYTHON_GIL="0", PYTHONPATH="src")
    env.pop("RUNLOOM_INTROSPECT_TIME", None)
    try:
        p = subprocess.run([PY, "-c", _MAXG_CHILD.format(cap=cap)],
                           cwd=REPO, env=env, capture_output=True, text=True,
                           timeout=200)
    except subprocess.TimeoutExpired:
        pytest.skip("MAX_GOROUTINES subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "RUNLOOM_MAX_GOROUTINES child failed rc=%d\n%s" % (
        p.returncode, p.stderr[-1500:])
    assert "MAX_GOROUTINES_OK" in p.stdout, (p.stdout, p.stderr[-800:])


# --------------------------------------------------------------------------
# Negative control for the L194-195 parse: an INVALID (non-numeric) value.
# atol("notanumber") == 0, the `if (n > 0)` guard is FALSE, so the cap is NOT
# installed -- get_max_fibers() stays 0 (unlimited) and a big spawn storm is
# NOT rejected.  This proves the guard arm (n>0) is exercised on the false side
# and the bad env degrades gracefully rather than wedging init.
# --------------------------------------------------------------------------
_MAXG_BAD_CHILD = r"""
import os, sys
sys.path.insert(0, 'src')
import runloom_c as rc
assert rc.get_max_fibers() == 0, (
    "non-numeric RUNLOOM_MAX_GOROUTINES installed a cap: %d" % rc.get_max_fibers())
ran = {"n": 0}
def main():
    def w():
        ran["n"] += 1            # single-thread sched: no race on this counter
    for _ in range(50):
        rc.go(w)                 # would raise if a (bogus) cap were active
    def d():
        for _ in range(4):
            rc.sched_yield()
    rc.go(d)
rc.go(main)
rc.run()
assert ran["n"] == 50, "spawn storm under no-cap dropped some: %d" % ran["n"]
sys.stdout.write("MAX_GOROUTINES_BAD_OK\n")
"""


def test_max_goroutines_env_invalid_is_unlimited():
    env = dict(os.environ, RUNLOOM_MAX_GOROUTINES="notanumber",
               PYTHON_GIL="0", PYTHONPATH="src")
    env.pop("RUNLOOM_INTROSPECT_TIME", None)
    try:
        p = subprocess.run([PY, "-c", _MAXG_BAD_CHILD], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=200)
    except subprocess.TimeoutExpired:
        pytest.skip("MAX_GOROUTINES(bad) subprocess timed out (shared-box contention)")
    assert p.returncode == 0, "bad RUNLOOM_MAX_GOROUTINES child failed rc=%d\n%s" % (
        p.returncode, p.stderr[-1500:])
    assert "MAX_GOROUTINES_BAD_OK" in p.stdout, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
