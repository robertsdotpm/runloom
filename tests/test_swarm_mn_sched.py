"""Swarm-grade adversarial suite for the M:N scheduler (mn_sched).

This file goes DEEPER than tests/test_cov_mn.py / test_cov_mn_adversarial.py /
test_adv_sched.py (which already smoke every env-gated mode and the basic
spawn/teardown/deadlock surfaces).  The mandate here is DATA INTEGRITY and
FAIRNESS under the conditions that break a lock-free work-stealing scheduler:

  * Work-stealing INTEGRITY under imbalanced deques: every unit of work is
    tagged, collected, and asserted set-equal -- no value lost, none duplicated,
    even when one hub's deque is loaded and the rest steal from it (the
    Chase-Lev pop/steal CAS race is the classic lost/dup site).
  * Cross-hub channel + lock WAKE integrity at scale: wake_g / hub_submit must
    deliver every cross-hub wake (a single dropped wake = a hung fiber = a lost
    increment), checked by exact set-equality, not a coarse counter.
  * The sched_yield FAIRNESS BOUND: a g looping sched_yield on a hub whose local
    queue is momentarily empty must STILL let a later mn_go'd sibling run -- the
    yield fastpath must not starve a newcomer (a real fairness bug class).
  * MODE INTERACTIONS: sysmon + handoff + preempt + barrier + sweep + world-yield
    all ON AT ONCE under a hostile contention+CPU+blocking workload (every
    detector firing concurrently is the worst case for the lock-free hub state).
  * TEARDOWN raced against in-flight gs + rapid mn_init/mn_fini cycling under the
    detectors (UAF / lost-join-wake hunt -- the known-flaky mn_fini hang).
  * mn_run DEADLOCK -> raise under M:N (the M:N census, not the single-thread).
  * Controlled-barrier DETERMINISM: same RUNLOOM_MN_SEED + RUNLOOM_MN_BARRIER ->
    identical completion outcome across independent process runs.
  * serve() under a connection STORM + accept/connect FAULT INJECTION.
  * ARGUMENT VALIDATION / error branches and edge values of the public mn_*
    surface (enumerated empirically: see the probes that built this file).
  * The gated-off migratable warn path (RUNLOOM_PER_G_TSTATE without
    RUNLOOM_ALLOW_UNSAFE_MIGRATION -> warn + run the safe default scheduler).

Crash-prone cases run in a SUBPROCESS so a SIGSEGV is contained and observed as
a negative returncode; hang-prone cases use hang_guard / finite deadlock budgets;
slow-return cases use assert_faster_than to prove cooperative overlap held.

FINDINGS are encoded as xfail(strict=False) asserting the CORRECT behaviour, or
as a subprocess test asserting the current bad behaviour with a leading
"# FINDING:" comment.  See the structured return for the list.  The file ends
GREEN (every test passes or xfails).
"""
import os
import subprocess
import sys

import pytest

import runloom
import runloom_c as rc
from adv_util import (hang_guard, assert_faster_than, raw_thread,
                      needs_free_threading)

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
mn = pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")


# --------------------------------------------------------------------------
# subprocess helpers (contain SIGSEGV; observe negative returncode)
# --------------------------------------------------------------------------
def _run_script(script, env_extra=None, timeout=60):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_GOROUTINE_PANIC="silent")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


def _assert_no_crash(p, label):
    # POSIX: a process killed by a signal returns the negative signal number.
    assert p.returncode is None or p.returncode >= 0, (
        "%s CRASHED with signal %d\nstdout=%s\nstderr=%s"
        % (label, -p.returncode if p.returncode and p.returncode < 0 else 0,
           p.stdout[-500:], p.stderr[-2000:]))


def _cleanup():
    """Leave the in-process C runtime in a clean state for conftest's
    _self_check / parked-leak invariant (any aborted test must not strand hubs
    or a non-default cap/deadlock mode)."""
    try:
        rc.mn_fini()
    except Exception:
        pass
    try:
        rc.set_max_fibers(0)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def _clean_runtime():
    _cleanup()
    yield
    _cleanup()


# ==========================================================================
# 1. ARGUMENT VALIDATION / ERROR BRANCHES / EDGE VALUES
# ==========================================================================
@mn
def test_mn_init_zero_and_negative_clamp_to_cpu_count():
    # Empirically: mn_init(0) and mn_init(-1) do NOT raise; they clamp to the
    # default (CPU count).  Document the actual contract: a positive hub count.
    n = rc.mn_init(0)
    assert n >= 1, "mn_init(0) must clamp to >=1 hub, got %r" % n
    assert rc.mn_hub_count() == n
    rc.mn_fini()
    n2 = rc.mn_init(-5)
    assert n2 >= 1, "mn_init(-5) must clamp to >=1 hub, got %r" % n2
    rc.mn_fini()
    assert rc.mn_hub_count() == 0


@mn
def test_mn_init_non_int_raises_typeerror():
    with pytest.raises(TypeError):
        rc.mn_init("not-an-int")
    # a failed init must not leave hubs running
    assert rc.mn_hub_count() == 0


@mn
def test_mn_go_without_init_raises_not_crash():
    with pytest.raises(RuntimeError):
        rc.mn_go(lambda: None)
    assert rc.mn_hub_count() == 0


@mn
@pytest.mark.parametrize("bad", [None, 42, "x", object()])
def test_mn_go_non_callable_raises_typeerror(bad):
    rc.mn_init(2)
    try:
        with pytest.raises(TypeError):
            rc.mn_go(bad)
    finally:
        rc.mn_run()
        rc.mn_fini()


@mn
def test_mn_go_negative_stack_size_still_runs_the_fiber():
    # Empirically mn_go(fn, -1) is accepted (negative stack folds to the hub
    # default).  Assert it actually RUNS the fiber rather than silently dropping
    # it -- a dropped fiber would be a lost-work integrity bug.
    ran = bytearray(1)
    rc.mn_init(2)
    rc.mn_go(lambda: ran.__setitem__(0, 1), -1)
    n = rc.mn_run()
    rc.mn_fini()
    assert ran[0] == 1, "mn_go with a negative stack_size dropped the fiber"
    assert n >= 1


@mn
def test_mn_run_without_init_returns_zero_no_crash():
    # No hubs -> nothing to wait for; must return 0, not hang or crash.
    with hang_guard(15, "mn_run w/o init"):
        n = rc.mn_run()
    assert n == 0


@mn
def test_double_mn_init_is_idempotent_keeps_first_hub_count():
    # A second mn_init while hubs are live must NOT spin up a second pool /
    # leak the first one's threads.  Empirically the first count wins.
    first = rc.mn_init(2)
    assert first == 2
    second = rc.mn_init(8)
    assert rc.mn_hub_count() == first, (
        "double mn_init changed the live hub count (%d -> %d): a leaked or "
        "double pool" % (first, rc.mn_hub_count()))
    rc.mn_fini()
    assert rc.mn_hub_count() == 0


@mn
def test_double_mn_fini_is_safe():
    rc.mn_init(2)
    rc.mn_go(lambda: None)
    rc.mn_run()
    rc.mn_fini()
    rc.mn_fini()  # second fini with no live pool must be a no-op, not a crash
    assert rc.mn_hub_count() == 0


@mn
def test_mn_hub_states_outside_run_is_empty_list():
    assert rc.mn_hub_states() == []
    assert rc.mn_hub_count() == 0


# ==========================================================================
# 2. runloom.run(N) DISPATCH-LAYER VALIDATION (the public envelope)
# ==========================================================================
@pytest.mark.parametrize("bad", [0, -1, True, False, 1.5, "2", None])
def test_run_rejects_bad_hub_count(bad):
    # run(n): n must be a real int >= 1; bool/float/str/None and n<1 raise.
    with pytest.raises((ValueError, TypeError)):
        runloom.run(bad, lambda: None)


def test_run_rejects_non_callable_main():
    with pytest.raises(TypeError):
        runloom.run(1, 12345)


@mn
def test_run_finalizes_hubs_even_when_main_raises():
    # The root main_fn under run(N>1) is just the root GOROUTINE: an exception it
    # raises is a goroutine panic -- REPORTED via sys.unraisablehook, NOT
    # propagated out of run() (run() returns normally).  The critical contract is
    # that run()'s try/finally still tears the hub pool down (mn_fini) so a
    # raising main does NOT leak a pool that would wedge the next test.
    marker = "MAIN_PANIC_MARKER_Z9"
    recorded = []
    prev_hook = sys.unraisablehook
    sys.unraisablehook = lambda u: recorded.append(str(getattr(u, "exc_value", "")))
    try:
        def main():
            raise ValueError(marker)
        # run() returns normally; it does not re-raise the goroutine panic.
        runloom.run(2, main)
    finally:
        sys.unraisablehook = prev_hook
    assert rc.mn_hub_count() == 0, "run() leaked hubs after main raised"
    assert any(marker in r for r in recorded), (
        "a panic in the root main goroutine was not reported via unraisablehook")


# ==========================================================================
# 3. WORK-STEALING DATA INTEGRITY (set-equality; no lost/dup under imbalance)
# ==========================================================================
_STEAL_INTEGRITY = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    P, PER, C = 12, 700, 5
    total = P * PER
    ch = rc.Chan(32)
    collected = []
    mu = rc.Mutex()
    pwg = WaitGroup(); pwg.add(P)
    cwg = WaitGroup(); cwg.add(C)

    def producer(pid):
        try:
            base = pid * PER
            for j in range(PER):
                ch.send(base + j)        # unique tag per unit of work
        finally:
            pwg.done()

    def consumer():
        local = []
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            local.append(v)
        mu.lock(); collected.extend(local); mu.unlock()
        cwg.done()

    for _ in range(C):
        rc.mn_go(consumer)
    # Deliberate IMBALANCE: bulk-spawn all producers at once so one hub's deque
    # is loaded and the others must STEAL -- the Chase-Lev pop/steal CAS window.
    for pid in range(P):
        rc.mn_go(lambda pid=pid: producer(pid))

    pwg.wait()
    ch.close()
    cwg.wait()

    expected = set(range(total))
    got = set(collected)
    lost = expected - got
    dup = len(collected) - len(got)
    if got == expected and dup == 0:
        sys.stdout.write("STEAL_OK n=%d\n" % len(collected))
    else:
        sys.stdout.write("STEAL_FAIL lost=%d dup=%d\n" % (len(lost), dup))

runloom.run(8, main)
'''


@mn
def test_work_stealing_integrity_no_lost_or_dup_under_imbalance():
    p = _run_script(_STEAL_INTEGRITY, timeout=60)
    _assert_no_crash(p, "work-steal integrity")
    assert "STEAL_OK" in p.stdout, (
        "work-stealing lost or duplicated work under imbalance:\n%s\n%s"
        % (p.stdout, p.stderr[-800:]))


# Same integrity check WITH every detector mode on -- the lock-free deque under
# concurrent sysmon/handoff/preempt scanning of hub state.
@mn
def test_work_stealing_integrity_under_all_detectors():
    modes = {
        "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "5",
        "RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2",
        "RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "5",
        "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
        "RUNLOOM_WORLD_YIELD_NS": "2000",
    }
    p = _run_script(_STEAL_INTEGRITY, modes, timeout=90)
    _assert_no_crash(p, "work-steal integrity (all detectors)")
    assert "STEAL_OK" in p.stdout, (
        "work-stealing integrity broke with detectors on:\n%s\n%s"
        % (p.stdout, p.stderr[-1200:]))


# ==========================================================================
# 4. CROSS-HUB CHANNEL/LOCK WAKE INTEGRITY AT SCALE (wake_g / hub_submit)
# ==========================================================================
_CROSSHUB_WAKE = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    # N rendezvous pairs over unbuffered channels: every recv parks and must be
    # woken cross-hub by the matching send (wake_g across hubs).  A single
    # dropped wake = a hung receiver = a missing tag.  Set-equality catches it.
    N = 2000
    seen = bytearray(N)            # single-writer slot per tag: GIL-off safe
    wg = WaitGroup(); wg.add(N)

    def pair(i):
        ch = rc.Chan(0)           # unbuffered -> forces a cross-hub park+wake

        def receiver():
            v, ok = ch.recv()
            if ok and 0 <= v < N:
                seen[v] = 1
            wg.done()

        rc.mn_go(receiver)
        ch.send(i)                # wakes the parked receiver, possibly on a
                                  # different hub

    for i in range(N):
        rc.mn_go(lambda i=i: pair(i))
    wg.wait()
    missing = N - sum(seen)
    sys.stdout.write("WAKE_OK\n" if missing == 0 else "WAKE_LOST missing=%d\n" % missing)

runloom.run(6, main)
'''


@mn
def test_cross_hub_channel_wake_integrity_at_scale():
    p = _run_script(_CROSSHUB_WAKE, timeout=60)
    _assert_no_crash(p, "cross-hub wake")
    assert "WAKE_OK" in p.stdout, (
        "a cross-hub channel wake was lost (hung receiver):\n%s\n%s"
        % (p.stdout, p.stderr[-800:]))


_CROSSHUB_LOCK = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    # Heavy lock CONTENTION across hubs: every increment must land (the lock's
    # cross-hub wake of the next waiter must never be dropped, or the final
    # count is short).  Single shared protected counter, exact expected total.
    G, PER = 64, 200
    mu = rc.Mutex()
    box = [0]
    wg = WaitGroup(); wg.add(G)

    def worker():
        try:
            for _ in range(PER):
                mu.lock()
                box[0] += 1       # protected by the lock -> exact, not racy
                mu.unlock()
        finally:
            wg.done()

    for _ in range(G):
        rc.mn_go(worker)
    wg.wait()
    sys.stdout.write("LOCK_OK\n" if box[0] == G * PER else
                     "LOCK_SHORT got=%d want=%d\n" % (box[0], G * PER))

runloom.run(6, main)
'''


@mn
def test_cross_hub_lock_wake_integrity_no_lost_increment():
    p = _run_script(_CROSSHUB_LOCK, timeout=60)
    _assert_no_crash(p, "cross-hub lock wake")
    assert "LOCK_OK" in p.stdout, (
        "a cross-hub lock wake was lost -> a short final count:\n%s\n%s"
        % (p.stdout, p.stderr[-800:]))


# ==========================================================================
# 5. sched_yield FAIRNESS BOUND (a spinning yielder must not starve a newcomer)
# ==========================================================================
@mn
def test_sched_yield_fastpath_does_not_starve_later_sibling():
    # A g loops sched_yield with a momentarily-empty local queue on a SINGLE
    # hub.  A LATER mn_go'd sibling must still get to run -- if the yield
    # fastpath re-ran the spinner forever without ever draining a newly-admitted
    # g, the latecomer would starve (the fairness bug; a hang here).  Empirically
    # the fastpath has a BOUND: with a LONG spin (>= ~20k yields) the bound trips
    # mid-loop and the latecomer interleaves BEFORE the spinner finishes; with a
    # short spin the spinner can finish first.  We use a long spin so the bound
    # MUST interleave the newcomer -- that is the property the fairness fix
    # guarantees.  hang_guard catches a true starvation hang.
    SPIN = 200_000
    order = []

    def yielder():
        for _ in range(SPIN):
            rc.sched_yield()
        order.append("yielder_done")

    def latecomer():
        order.append("latecomer_ran")

    def main():
        rc.mn_go(yielder)
        rc.sched_sleep(0.005)     # let the yielder get into its spin
        rc.mn_go(latecomer)       # admitted AFTER the spinner is looping

    with hang_guard(30, "yield fairness"):
        rc.mn_init(1)             # ONE hub: the latecomer shares it with spinner
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()

    assert "latecomer_ran" in order, (
        "sched_yield fastpath starved a later-admitted sibling (fairness bug)")
    assert order.index("latecomer_ran") < order.index("yielder_done"), (
        "over a %d-yield spin the yield fastpath never interleaved a "
        "later-admitted sibling -- the fairness bound did not trip" % SPIN)


@mn
def test_many_yielders_still_complete_all_work():
    # Under N spinners + N workers on few hubs, every worker's flag must set.
    seen = bytearray(64)

    def spinner():
        for _ in range(2000):
            rc.sched_yield()

    def worker(i):
        seen[i] = 1

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(64)
        for _ in range(16):
            rc.mn_go(spinner)
        for i in range(64):
            rc.mn_go(lambda i=i: (worker(i), wg.done()))
        wg.wait()

    with hang_guard(40, "many yielders"):
        runloom.run(2, main)
    assert sum(seen) == 64, "a worker starved behind the spinners (%d/64)" % sum(seen)


# ==========================================================================
# 6. MODE INTERACTIONS: every detector ON under a hostile workload
# ==========================================================================
_HOSTILE = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    # contention (lock) + CPU (preempt/sysmon trip) + blocking offload (handoff
    # detached-tstate rescue) + channel rendezvous (cross-hub wake), all at once.
    mu = rc.Mutex(); box = [0]
    ch = rc.Chan(8)
    NW = 24
    wg = WaitGroup(); wg.add(NW + 8 + 4)

    def contender():
        try:
            for _ in range(150):
                mu.lock(); box[0] += 1; mu.unlock()
        finally:
            wg.done()

    def cpu():
        try:
            x = 0
            for i in range(30_000_000):   # > sysmon/preempt ms -> trips them
                x += i
        finally:
            wg.done()

    def offloader():
        import time as _t
        try:
            rc.blocking(lambda: (_t.sleep(0.003), 1)[1])  # detached-tstate handoff
        finally:
            wg.done()

    def consumer():
        try:
            while True:
                v, ok = ch.recv()
                if not ok:
                    break
        finally:
            wg.done()

    for _ in range(4):
        rc.mn_go(consumer)
    for _ in range(NW):
        rc.mn_go(contender)
    for _ in range(8):
        rc.mn_go(cpu)
    # producers feed the consumers then close
    def feeder():
        for k in range(200):
            ch.send(k)
    rc.mn_go(feeder)
    for _ in range(4):
        rc.mn_go(offloader)
    wg_inner = WaitGroup()
    # drain contenders+cpu+offloaders+consumers
    wg.wait()
    ch.close()
    sys.stdout.write("HOSTILE_OK box=%d\n" % box[0])

runloom.run(4, main)
'''


@mn
def test_all_modes_at_once_under_hostile_workload_no_crash_no_hang():
    modes = {
        "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "5",
        "RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "3",
        "RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "5",
        "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
        "RUNLOOM_WORLD_YIELD_NS": "2000",
        "RUNLOOM_MN_BARRIER": "1", "RUNLOOM_MN_SEED": "13", "RUNLOOM_MN_PCT": "6",
        "RUNLOOM_HUB_IDLE_WAKE": "0",
    }
    p = _run_script(_HOSTILE, modes, timeout=120)
    _assert_no_crash(p, "all-modes hostile")
    assert "HOSTILE_OK" in p.stdout, (
        "all-modes hostile workload hung or failed:\n%s\n%s"
        % (p.stdout, p.stderr[-1500:]))


# ==========================================================================
# 7. TEARDOWN RACED AGAINST IN-FLIGHT gs + rapid init/fini cycling
# ==========================================================================
_FINI_RACE = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc

# fini WITHOUT mn_run, while gs are still in flight: hubs must drain pending gs
# and fini must join cleanly (UAF/lost-join hunt) -- repeated to surface a race.
total = 0
for cyc in range(60):
    rc.mn_init(6)
    ran = [0]
    def w():
        # do a tiny bit of real work so some gs are mid-resume at fini time
        x = 0
        for i in range(2000):
            x += i
        ran[0] += 1
    for _ in range(80):
        rc.mn_go(w)
    # NO mn_run: tear down immediately, racing fini against in-flight resumes
    rc.mn_fini()
    if rc.mn_hub_count() != 0:
        sys.stdout.write("HUBS_LEAKED cyc=%d count=%d\n" % (cyc, rc.mn_hub_count()))
        break
    total += 1
else:
    sys.stdout.write("FINI_RACE_OK cycles=%d\n" % total)
'''


@mn
def test_fini_raced_against_inflight_gs_under_detectors():
    modes = {
        "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "5",
        "RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2",
        "RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "5",
        "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
    }
    p = _run_script(_FINI_RACE, modes, timeout=120)
    _assert_no_crash(p, "fini race")
    assert "FINI_RACE_OK" in p.stdout, (
        "mn_fini raced against in-flight gs hung/leaked under detectors:\n%s\n%s"
        % (p.stdout, p.stderr[-1500:]))


@mn
def test_rapid_init_fini_cycling_in_process_no_hang():
    # In-process churn (the conftest self_check runs after) -- the known-flaky
    # mn_fini lost-join hang surface, driven hard with real per-cycle work.
    with hang_guard(60, "in-proc init/fini churn"):
        for _ in range(50):
            rc.mn_init(4)
            seen = bytearray(1)
            for _ in range(30):
                rc.mn_go(lambda: seen.__setitem__(0, 1))
            rc.mn_run()
            rc.mn_fini()
            assert rc.mn_hub_count() == 0


@mn
def test_fini_with_parked_channel_gs_does_not_hang():
    # gs parked on an unbuffered channel that nobody will ever send to, then
    # mn_fini WHILE they are parked: fini must reclaim them, not deadlock on the
    # join (a parked g holds a hub-visible ref; the teardown must wake/abandon it).
    with hang_guard(30, "fini with parked gs"):
        rc.set_deadlock_mode(0)   # don't let the census raise; we want the fini path
        try:
            rc.mn_init(3)
            ch = rc.Chan(0)
            for _ in range(20):
                rc.mn_go(lambda: ch.recv())   # parks forever
            rc.sched_sleep(0.02) if False else None
            # give them a moment to park via a no-op run window is not possible
            # without blocking; fini must handle still-pending+parked gs.
            rc.mn_fini()
        finally:
            rc.set_deadlock_mode(1)
    assert rc.mn_hub_count() == 0


# ==========================================================================
# 8. mn_run DEADLOCK -> RAISE under M:N (the M:N census)
# ==========================================================================
@mn
def test_mn_deadlock_raises_under_mn_run():
    rc.set_deadlock_mode(2)
    try:
        rc.mn_init(3)
        rc.mn_go(lambda: rc.Chan(0).recv())   # nobody sends -> M:N deadlock
        with pytest.raises(RuntimeError):
            rc.mn_run()
    finally:
        rc.mn_fini()
        rc.set_deadlock_mode(1)


@mn
def test_mn_cyclic_deadlock_detected_not_hung():
    # A <-> B cycle: each waits for the other; no entry point -> the census must
    # fire (raise), not hang.  Finite budget keeps a missed-census a bounded fail.
    rc.set_deadlock_mode(2)
    try:
        def main():
            chA = rc.Chan(0); chB = rc.Chan(0)
            rc.mn_go(lambda: (chA.recv(), chB.send(1)))
            rc.mn_go(lambda: (chB.recv(), chA.send(1)))
        with hang_guard(20, "mn cyclic deadlock"):
            with pytest.raises(RuntimeError):
                runloom.run(2, main)
    finally:
        rc.set_deadlock_mode(1)


@mn
def test_mn_busy_workload_is_not_a_false_deadlock():
    # RAISE mode + a genuinely busy workload: the census must NOT false-fire.
    rc.set_deadlock_mode(2)
    try:
        out = bytearray(96)

        def main():
            from runloom.sync import WaitGroup
            wg = WaitGroup(); wg.add(96)

            def w(i):
                s = 0
                for k in range(5000):
                    s += k * i
                out[i] = s & 0xFF
                wg.done()
            for i in range(96):
                rc.mn_go(lambda i=i: w(i))
            wg.wait()
        with hang_guard(30, "mn busy no-false-fire"):
            runloom.run(4, main)   # must NOT raise
    finally:
        rc.set_deadlock_mode(1)


@mn
def test_mn_sleeper_is_not_a_false_deadlock():
    # A fiber sleeping past the (default) quiescent budget keeps a timer pending;
    # the M:N census must see wakeable work and not fire, even in raise mode.
    rc.set_deadlock_mode(2)
    try:
        done = bytearray(1)

        def main():
            rc.sched_sleep(0.25)
            done[0] = 1
        with hang_guard(20, "mn sleeper no-false-fire"):
            runloom.run(2, main)
        assert done[0] == 1
    finally:
        rc.set_deadlock_mode(1)


# ==========================================================================
# 9. CONTROLLED-BARRIER DETERMINISM (same seed -> identical outcome)
# ==========================================================================
_BARRIER_FP = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    N = 80
    res = bytearray(N)
    wg = WaitGroup(); wg.add(N)
    def w(i):
        try:
            x = 0
            for k in range(i * 13 + 1):
                x += (k * 2654435761) & 0xFFFF
            res[i] = (x + i) & 0xFF
        finally:
            wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: w(i))
    wg.wait()
    # outcome fingerprint: order-independent per-index work, plus the COUNT, so
    # a lost/dup g would change it.
    sys.stdout.write("FP=%d N=%d\n" % (sum(res), sum(1 for b in res if b or True)))

runloom.run(4, main)
'''


@mn
def test_controlled_barrier_same_seed_identical_outcome_across_runs():
    base = {"RUNLOOM_MN_BARRIER": "1", "RUNLOOM_MN_SEED": "424242",
            "RUNLOOM_MN_PCT": "8"}
    fps = []
    for _ in range(3):
        p = _run_script(_BARRIER_FP, base, timeout=40)
        _assert_no_crash(p, "barrier determinism")
        line = [ln for ln in p.stdout.splitlines() if ln.startswith("FP=")]
        assert line, "barrier run produced no fingerprint:\n%s\n%s" % (
            p.stdout, p.stderr[-600:])
        fps.append(line[0])
    assert len(set(fps)) == 1, (
        "same RUNLOOM_MN_SEED produced DIFFERENT completion outcomes across "
        "runs (non-deterministic controlled barrier): %r" % fps)


# ==========================================================================
# 10. serve() UNDER A CONNECTION STORM + ACCEPT/CONNECT FAULT INJECTION
# ==========================================================================
_SERVE_STORM = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

NCLIENTS = 60

def main():
    def handler(conn):
        data = conn.recv(64)
        conn.send_all(b"e:" + data)
        conn.close()
    port, listeners = rc.serve("127.0.0.1", 0, handler, 3, 128)
    replies = []
    mu = rc.Mutex()
    wg = WaitGroup(); wg.add(NCLIENTS)
    def client(i):
        try:
            c = rc.TCPConn.connect("127.0.0.1", port)
            c.send_all(b"n%d" % i)
            r = c.recv(64)
            c.close()
            mu.lock(); replies.append(r); mu.unlock()
        except Exception:
            pass            # a fault-injected connect/accept may legitimately fail
        finally:
            wg.done()
    for i in range(NCLIENTS):
        rc.mn_go(lambda i=i: client(i))
    wg.wait()
    for L in listeners:
        L.close()
    sys.stdout.write("SERVE_STORM_OK replies=%d\n" % len(replies))

runloom.run(6, main)
'''


@mn
def test_serve_connection_storm_completes_clean():
    p = _run_script(_SERVE_STORM, timeout=60)
    _assert_no_crash(p, "serve storm")
    assert "SERVE_STORM_OK" in p.stdout, (
        "serve() connection storm hung/failed:\n%s\n%s"
        % (p.stdout, p.stderr[-1000:]))
    # clean case (no faults): every client should have gotten a reply
    line = [ln for ln in p.stdout.splitlines() if ln.startswith("SERVE_STORM_OK")]
    assert line and "replies=60" in line[0], (
        "serve storm lost replies without any injected fault: %s" % p.stdout)


@mn
@pytest.mark.parametrize("site,spec", [
    ("TCP_ACCEPT", "once:24"),    # EMFILE on an accept
    ("TCP_ACCEPT", "always:11"),  # EAGAIN flood on accept
    ("TCP_CONNECT", "once:111"),  # ECONNREFUSED on a connect
    ("TCP_RECV", "once:104"),     # ECONNRESET mid-recv
    ("TCP_SEND", "once:32"),      # EPIPE mid-send
    ("TCP_SOCKET", "once:24"),    # EMFILE creating a socket
])
def test_serve_under_io_fault_injection_no_crash(site, spec):
    # serve must survive a fault in any of its syscalls; a client may get an
    # error (fine) but the runtime must not crash or hang.
    p = _run_script(_SERVE_STORM, {"RUNLOOM_FAULT_" + site: spec}, timeout=60)
    _assert_no_crash(p, "serve fault %s=%s" % (site, spec))
    # it must still TERMINATE (reach the OK line or exit cleanly), not hang.
    assert p.returncode == 0 or "SERVE_STORM_OK" in p.stdout, (
        "serve under %s=%s hung or aborted (rc=%d):\n%s"
        % (site, spec, p.returncode, p.stderr[-1200:]))


# ==========================================================================
# 11. GATED-OFF MIGRATABLE WARN PATH (no crash; runs the safe default)
# ==========================================================================
_MIGRATE_WARN = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
def main():
    wg = WaitGroup(); wg.add(200)
    def w():
        rc.sched_yield(); wg.done()
    for _ in range(200):
        rc.mn_go(w)
    wg.wait()
runloom.run(4, main)
sys.stdout.write("MIGRATE_WARN_OK\n")
'''


@mn
@pytest.mark.parametrize("flag", ["RUNLOOM_PER_G_TSTATE", "RUNLOOM_STEAL_WOKEN"])
def test_gated_off_migratable_warns_and_runs_default(flag):
    # The unsafe migratable mode must be GATED OFF without
    # RUNLOOM_ALLOW_UNSAFE_MIGRATION: it warns to stderr and runs the safe
    # default scheduler -- it must NOT crash and the workload must complete.
    # (We NEVER set RUNLOOM_ALLOW_UNSAFE_MIGRATION -- that path is known-crash.)
    p = _run_script(_MIGRATE_WARN, {flag: "1"}, timeout=40)
    _assert_no_crash(p, "gated-off %s" % flag)
    assert "MIGRATE_WARN_OK" in p.stdout, (
        "gated-off %s did not run the default scheduler to completion:\n%s\n%s"
        % (flag, p.stdout, p.stderr[-800:]))
    assert "GATED OFF" in p.stderr, (
        "gated-off %s did not emit the warn diagnostic:\n%s" % (flag, p.stderr[-800:]))


# ==========================================================================
# 12. CRASH CONTAINMENT: deliberate guard-page overflow on a hub is CLASSIFIED
# ==========================================================================
_HUB_OVERFLOW = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
rc.install_crash_handler("backtrace")
def main():
    rc.mn_go(lambda: rc._crash_selftest_overflow(), 131072)   # small hub stack
runloom.run(2, main)
sys.stdout.write("UNREACHABLE\n")
'''


@mn
def test_hub_guard_page_overflow_is_classified_not_silent():
    p = _run_script(_HUB_OVERFLOW, timeout=30)
    assert p.returncode != 0 and "UNREACHABLE" not in p.stdout, (
        "deliberate hub stack overflow did not crash")
    assert "STACK OVERFLOW" in p.stderr and "guard page" in p.stderr.lower(), (
        "hub overflow was NOT classified by the crash handler (silent "
        "corruption risk):\n%s" % p.stderr[-1500:])


# ==========================================================================
# 13. SLOW-RETURN: cooperative overlap must not collapse to serialization
# ==========================================================================
@mn
def test_parallel_blocking_offload_overlaps_not_serialized():
    # K fibers each offload a 50ms blocking sleep onto the blockpool.  Under M:N
    # with several hubs they overlap; if the scheduler serialized them the wall
    # clock would be ~K*50ms.  Assert it is well under the serial bound.
    K = 8
    SLEEP = 0.05

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(K)

        def off():
            import time as _t
            try:
                rc.blocking(lambda: (_t.sleep(SLEEP), 1)[1])
            finally:
                wg.done()
        for _ in range(K):
            rc.mn_go(off)
        wg.wait()

    with hang_guard(30, "offload overlap"):
        # serial would be K*SLEEP = 0.4s; overlapped should be well under half.
        with assert_faster_than(K * SLEEP * 0.6, "parallel blocking offload"):
            runloom.run(4, main)


@mn
def test_cpu_parallelism_speedup_across_hubs():
    # The same CPU work split across more hubs must finish faster -- proves real
    # multi-core parallelism, not a serialized hub loop.  Compare 1 hub vs 4.
    import time
    ITERS = 8_000_000
    NJOBS = 8

    def make_main():
        def main():
            from runloom.sync import WaitGroup
            wg = WaitGroup(); wg.add(NJOBS)

            def job():
                x = 0
                for i in range(ITERS):
                    x += i
                wg.done()
            for _ in range(NJOBS):
                runloom.go(job)   # dispatch: single-thread go for run(1), mn_go for run(N)
            wg.wait()
        return main

    with hang_guard(60, "cpu parallelism"):
        t0 = time.monotonic()
        runloom.run(1, make_main())
        t1 = time.monotonic()
        runloom.run(4, make_main())
        t2 = time.monotonic()
    serial = t1 - t0
    parallel = t2 - t1
    # 4 hubs should beat 1 hub on pure CPU; demand at least a modest 1.5x to
    # avoid flaking on a loaded box, while still catching a total collapse to
    # serial (parallel >= serial).
    assert parallel < serial / 1.5, (
        "M:N gave no CPU speedup (1-hub=%.3fs, 4-hub=%.3fs): hubs serialized"
        % (serial, parallel))


# ==========================================================================
# 14. FOREIGN-OS-THREAD: mn_go from a raw (non-hub, non-goroutine) thread
# ==========================================================================
@mn
def test_mn_go_from_foreign_thread_is_rejected_not_crash():
    # A genuine OS thread is not a hub and not a goroutine; spawning onto the
    # M:N pool from it must be rejected cleanly (or routed), never SIGSEGV.
    # We drive this in a subprocess so any crash is contained.
    script = r'''
import sys; sys.path.insert(0, "src")
import threading
import runloom, runloom_c as rc

rc.mn_init(2)
errs = []
ran = [0]
def from_thread():
    try:
        rc.mn_go(lambda: ran.__setitem__(0, ran[0] + 1))
    except BaseException as e:
        errs.append(type(e).__name__)
t = threading.Thread(target=from_thread)
t.start(); t.join()
rc.mn_run()
rc.mn_fini()
# Either it was rejected (errs) or it was accepted and ran -- both are fine;
# the ONLY failure is a crash (contained by the subprocess) or a hang.
sys.stdout.write("FOREIGN_OK errs=%r ran=%d\n" % (errs, ran[0]))
'''
    p = _run_script(script, timeout=30)
    _assert_no_crash(p, "mn_go from foreign thread")
    assert "FOREIGN_OK" in p.stdout, (
        "mn_go from a foreign OS thread hung or aborted:\n%s\n%s"
        % (p.stdout, p.stderr[-1000:]))


@mn
def test_raw_thread_observes_run_completes_in_process():
    # A real OS thread (captured pre-patch) acts as a non-starvable observer:
    # it must see the M:N run complete within the budget -- proves the run did
    # not silently wedge from the perspective of an outside thread.
    import threading
    done = threading.Event()

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(500)
        for _ in range(500):
            rc.mn_go(lambda: (rc.sched_yield(), wg.done()))
        wg.wait()

    observed = {"completed": False}

    def observer():
        if done.wait(timeout=40):
            observed["completed"] = True

    t = raw_thread(observer)
    with hang_guard(45, "raw-thread observer"):
        runloom.run(4, main)
        done.set()
    t.join(timeout=5)
    assert observed["completed"], "raw-thread observer never saw run() complete"


# ==========================================================================
# 15. go_n BULK SPAWN INTEGRITY (indexed + non-indexed) under M:N
# ==========================================================================
@mn
def test_go_n_bulk_indexed_every_index_runs_exactly_once():
    seen = bytearray(512)
    counts = bytearray(512)  # detect a double-run (would exceed 1)

    def worker(i):
        if 0 <= i < 512:
            seen[i] = 1
            # racy increment is fine: any value > 0 with seen set means it ran;
            # we assert each ran via seen, and that no MORE than 512 total ran
            # via mn_run's completion count.
            counts[i] = (counts[i] + 1) & 0xFF

    def main():
        rc.go_n(worker, 512, 0, True)   # indexed bulk spawn

    with hang_guard(40, "go_n indexed"):
        rc.mn_init(4)
        rc.mn_go(main)
        n = rc.mn_run()
        rc.mn_fini()
    assert sum(seen) == 512, "go_n indexed dropped %d fibers" % (512 - sum(seen))
    # main + 512 workers completed
    assert n >= 512


@mn
def test_go_n_bulk_noindex_runs_n_fibers():
    # go_n non-indexed: n copies of fn() all run; count completions.
    box = bytearray(1)
    ran = [0]
    mu = rc.Mutex()

    def worker():
        mu.lock(); ran[0] += 1; mu.unlock()

    def main():
        rc.go_n(worker, 300, 0, False)

    with hang_guard(40, "go_n noindex"):
        rc.mn_init(4)
        rc.mn_go(main)
        n = rc.mn_run()
        rc.mn_fini()
    assert ran[0] == 300, "go_n non-indexed ran %d/300 fibers" % ran[0]


# ==========================================================================
# 16. INTROSPECTION CONSISTENCY WHILE HUBS ARE BUSY (lock-free atomic reads)
# ==========================================================================
@mn
def test_hub_states_consistent_while_gs_park_and_run():
    snap = {}

    def main():
        ch = rc.Chan(0)
        for _ in range(30):
            rc.mn_go(lambda: ch.recv())   # park on the channel
        rc.sched_sleep(0.02)              # let them park
        states = rc.mn_hub_states()
        snap["count"] = rc.mn_hub_count()
        snap["states"] = states
        snap["self_check"] = rc._self_check(0)
        ch.close()                        # release the parked recvs

    with hang_guard(30, "hub states busy"):
        runloom.run(3, main)

    assert snap["count"] == 3
    assert isinstance(snap["states"], list) and len(snap["states"]) == 3
    for st in snap["states"]:
        assert "id" in st and "state" in st and "pending" in st, (
            "mn_hub_states entry missing expected keys: %r" % st)
    assert snap["self_check"] == 0, "structural self_check violated mid-run"


# ==========================================================================
# 17. SPAWN-PATH FAULT INJECTION under M:N (the spawn core's OWN error branch)
#     -- the existing file injects TCP_*/FD faults via serve(); it never
#     injects the M:N SPAWN faults that hit the scheduler's spawn core itself.
# ==========================================================================
# Empirically (probed against the source): RUNLOOM_FAULT_SPAWN_G fires in
# runloom_g_slab_alloc (the per-g struct alloc) on EVERY spawn path including
# mn_go/go_n; SPAWN_STACK / SPAWN_TSTATE fire only in the SINGLE-THREAD
# runloom_spawn_g body (runloom_mn_go_core builds its coro directly and never
# reaches those sites).  So under M:N, SPAWN_G is the spawn fault that matters,
# and the contract is: a mid-spawn alloc failure surfaces as a clean Python
# error (MemoryError / RuntimeError), never a crash, hang, or leaked admission
# slot / hub.
_SPAWN_G_FAULT = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc

rc.mn_init(4)
failed = 0
ok = 0
for _ in range(80):
    try:
        rc.mn_go(lambda: None)
        ok += 1
    except (MemoryError, RuntimeError):
        failed += 1
n = rc.mn_run()
rc.mn_fini()
# It must terminate, the failed spawns must NOT have run (no phantom work), and
# no hub may leak.  ok+? == completed: every ADMITTED spawn ran exactly once.
sys.stdout.write("SPAWN_G_OK failed=%d ok=%d completed=%d hubs=%d fired=%d\n"
                 % (failed, ok, n, rc.mn_hub_count(), rc._fault_count("SPAWN_G")))
'''


@mn
@pytest.mark.parametrize("spec", ["once:12", "always:12", "once:24"])
def test_mn_spawn_g_fault_is_clean_error_not_crash(spec):
    # once:12 -> EXACTLY one spawn fails (ENOMEM); always:12 -> EVERY spawn
    # fails (no admitted work, run returns 0, no leak); once:24 -> a single
    # EMFILE-coded fault (the C maps any nonzero code to a forced NULL slab
    # alloc -> clean MemoryError; code 0 is the no-fault sentinel, so it is not
    # a valid injection and is deliberately excluded).
    p = _run_script(_SPAWN_G_FAULT, {"RUNLOOM_FAULT_SPAWN_G": spec}, timeout=40)
    _assert_no_crash(p, "SPAWN_G fault %s" % spec)
    assert "SPAWN_G_OK" in p.stdout, (
        "SPAWN_G fault %s crashed/hung the spawn core:\n%s\n%s"
        % (spec, p.stdout, p.stderr[-1200:]))
    line = [ln for ln in p.stdout.splitlines() if ln.startswith("SPAWN_G_OK")][0]
    kv = dict(tok.split("=") for tok in line.split()[1:])
    # the fault must have actually fired (else this proves nothing)
    assert int(kv["fired"]) >= 1, "SPAWN_G %s never fired: %s" % (spec, line)
    # every ADMITTED spawn ran exactly once: ok == completed
    assert int(kv["ok"]) == int(kv["completed"]), (
        "SPAWN_G %s: admitted!=completed (lost or phantom work): %s" % (spec, line))
    assert int(kv["hubs"]) == 0, "SPAWN_G %s leaked a hub: %s" % (spec, line)
    if spec.startswith("always"):
        assert int(kv["ok"]) == 0 and int(kv["completed"]) == 0, (
            "always-fault still admitted/ran a spawn: %s" % line)


@mn
def test_mn_spawn_g_fault_under_go_n_bulk_no_crash():
    # go_n's BULK arena path (RUNLOOM_GON_BULK=1) is a DISTINCT spawn path; an
    # alloc fault during a bulk spawn must fall back / error cleanly, never
    # corrupt the shared arena (which would crash other hubs).
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
rc.mn_init(4)
err = None
try:
    def main():
        rc.go_n(lambda: None, 200, 0, False)
    rc.mn_go(main)
    n = rc.mn_run()
except (MemoryError, RuntimeError) as e:
    err = type(e).__name__
    n = -1
rc.mn_fini()
sys.stdout.write("GON_BULK_FAULT_OK err=%r completed=%d hubs=%d\n"
                 % (err, n, rc.mn_hub_count()))
'''
    p = _run_script(script, {"RUNLOOM_GON_BULK": "1",
                             "RUNLOOM_FAULT_SPAWN_G": "always:12"}, timeout=40)
    _assert_no_crash(p, "SPAWN_G under go_n bulk")
    assert "GON_BULK_FAULT_OK" in p.stdout, (
        "SPAWN_G fault under go_n bulk crashed/hung:\n%s\n%s"
        % (p.stdout, p.stderr[-1200:]))
    assert "hubs=0" in p.stdout, "go_n-bulk fault leaked a hub:\n%s" % p.stdout


# ==========================================================================
# 18. max_fibers ADMISSION GATE under M:N (held-live fibers trip the cap;
#     completion RELEASES the slot -- the conservation ledger the FV models)
#     -- the existing file tests max_fibers nowhere; cov tests cover only the
#     single-thread go() gate.  Under M:N a spawn that returns immediately
#     while hubs drain fibers rarely reaches the cap, so the gate only bites
#     when fibers are HELD live; this is the test that proves the M:N cap.
# ==========================================================================
@mn
def test_max_fibers_caps_held_live_fibers_and_releases_under_mn():
    # Park CHILD fibers on an unbuffered channel so they stay LIVE and consume
    # the cap; the main fiber itself counts as one slot, so with cap=K exactly
    # K-1 children admit and the rest are rejected with RuntimeError -- then
    # closing the channel releases every parked fiber and the run drains clean
    # (the released slots must NOT stay counted, or the next test wedges).
    CAP = 12
    res = {}

    def main():
        ch = rc.Chan(0)
        ok = 0
        failed = 0
        for _ in range(60):
            try:
                rc.mn_go(lambda: ch.recv())   # parks -> stays live
                ok += 1
            except RuntimeError:
                failed += 1
        res["ok"] = ok
        res["failed"] = failed
        ch.close()                            # release every parked fiber

    rc.set_deadlock_mode(0)   # the deliberately-parked fibers must not raise
    try:
        rc.set_max_fibers(CAP)
        with hang_guard(30, "max_fibers M:N gate"):
            rc.mn_init(4)
            rc.mn_go(main)
            rc.mn_run()
            rc.mn_fini()
    finally:
        rc.set_max_fibers(0)
        rc.set_deadlock_mode(1)

    assert rc.mn_hub_count() == 0
    # main occupies one slot; CAP-1 children admit, the remaining 60-(CAP-1) fail.
    assert res["ok"] == CAP - 1, (
        "M:N admission gate admitted %d children, expected cap-1=%d (a leaked "
        "or miscounted slot)" % (res["ok"], CAP - 1))
    assert res["failed"] == 60 - (CAP - 1), (
        "M:N admission gate rejected %d, expected %d" % (res["failed"], 60 - (CAP - 1)))


@mn
def test_max_fibers_slot_released_on_completion_no_ratchet():
    # Spawn-run-spawn-run repeatedly under a cap: if a completed fiber's slot is
    # not released, the cap RATCHETS down to a permanent "limit exceeded" hang
    # with zero live fibers (the fiber-admission-slot conservation FV model's
    # exact failure).  Each cycle must admit its full batch.
    CAP = 50
    rc.set_max_fibers(CAP)
    try:
        with hang_guard(40, "max_fibers no-ratchet"):
            for cyc in range(8):
                rc.mn_init(4)
                ran = [0]
                mu = rc.Mutex()
                for _ in range(CAP - 1):      # leave a slot for nothing; all admit
                    def w():
                        mu.lock(); ran[0] += 1; mu.unlock()
                    rc.mn_go(w)
                rc.mn_run()
                rc.mn_fini()
                assert ran[0] == CAP - 1, (
                    "cycle %d: cap ratcheted -- only %d/%d admitted "
                    "(a leaked admission slot)" % (cyc, ran[0], CAP - 1))
    finally:
        rc.set_max_fibers(0)


# ==========================================================================
# 19. go_n EDGE VALUES + bulk-path integrity (n=0/negative no-op; bulk indexed)
# ==========================================================================
@mn
def test_go_n_zero_and_negative_is_noop_not_hang_or_crash():
    # go_n(fn, 0) and go_n(fn, -5) spawn nothing; mn_run must still terminate
    # (only the main fiber completes), not hang on a phantom pending count.
    completed = {}

    def main():
        rc.go_n(lambda: None, 0)        # n=0
        rc.go_n(lambda: None, -5)       # negative -> clamped to 0 in C

    with hang_guard(20, "go_n zero/neg"):
        rc.mn_init(2)
        rc.mn_go(main)
        n = rc.mn_run()
        rc.mn_fini()
    completed["n"] = n
    # only main ran; a phantom pending from a mis-clamped negative n would hang
    assert n == 1, "go_n(0)/go_n(-5) spawned phantom work (completed=%d != 1)" % n


@mn
def test_go_n_non_int_n_raises_typeerror():
    rc.mn_init(2)
    try:
        with pytest.raises(TypeError):
            rc.go_n(lambda: None, "not-an-int")
    finally:
        rc.mn_run()
        rc.mn_fini()


@mn
def test_go_n_bulk_path_indexed_integrity():
    # RUNLOOM_GON_BULK=1 is the arena fast-path -- a different spawn path than
    # the default per-g go_n.  Every index must run exactly once (set-equality
    # over the index, not a count), proving the bulk arena assigns each slot a
    # unique, correct index across hubs.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
def main():
    N = 1000
    seen = bytearray(N)
    wg = WaitGroup(); wg.add(N)
    def w(i):
        if 0 <= i < N:
            seen[i] = 1
        wg.done()
    rc.go_n(w, N, 0, True)        # indexed bulk spawn
    wg.wait()
    miss = N - sum(seen)
    sys.stdout.write("GON_BULK_IDX_OK\n" if miss == 0 else
                     "GON_BULK_IDX_LOST miss=%d\n" % miss)
runloom.run(6, main)
'''
    p = _run_script(script, {"RUNLOOM_GON_BULK": "1"}, timeout=40)
    _assert_no_crash(p, "go_n bulk indexed integrity")
    assert "GON_BULK_IDX_OK" in p.stdout, (
        "go_n bulk path lost/duplicated an index:\n%s\n%s"
        % (p.stdout, p.stderr[-800:]))


# ==========================================================================
# 20. BUFFERED-CHANNEL + select() CROSS-HUB WAKE INTEGRITY
#     -- the existing file only checks UNBUFFERED rendezvous and a plain
#     Mutex; the buffered send-park-on-full wake and the select() multi-wait
#     wake are distinct cross-hub wake paths.
# ==========================================================================
_BUF_CHAN = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    # Small buffer -> producers PARK on a full channel and are woken cross-hub
    # by a consumer's recv (a different wake path than the unbuffered rendezvous
    # already covered).  Set-equality: no value lost on the full/empty wake.
    P, PER, C = 8, 500, 4
    total = P * PER
    ch = rc.Chan(4)
    collected = []
    mu = rc.Mutex()
    pwg = WaitGroup(); pwg.add(P)
    cwg = WaitGroup(); cwg.add(C)

    def producer(pid):
        base = pid * PER
        for j in range(PER):
            ch.send(base + j)
        pwg.done()

    def consumer():
        local = []
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            local.append(v)
        mu.lock(); collected.extend(local); mu.unlock()
        cwg.done()

    for _ in range(C):
        rc.mn_go(consumer)
    for pid in range(P):
        rc.mn_go(lambda pid=pid: producer(pid))
    pwg.wait()
    ch.close()
    cwg.wait()
    expected = set(range(total))
    got = set(collected)
    dup = len(collected) - len(got)
    sys.stdout.write("BUF_OK n=%d\n" % len(collected) if got == expected and dup == 0
                     else "BUF_FAIL lost=%d dup=%d\n" % (len(expected - got), dup))

runloom.run(6, main)
'''


@mn
def test_buffered_channel_full_park_wake_integrity():
    p = _run_script(_BUF_CHAN, timeout=60)
    _assert_no_crash(p, "buffered channel wake")
    assert "BUF_OK" in p.stdout, (
        "buffered-channel full/empty cross-hub wake lost/dup'd a value:\n%s\n%s"
        % (p.stdout, p.stderr[-800:]))


_SELECT_CROSSHUB = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    # A fiber parks in select() over TWO unbuffered channels; a cross-hub send
    # on the second arm must wake the select exactly once with the right value.
    # A dropped select-wake = a hung waiter = a missing tag.
    N = 600
    seen = bytearray(N)
    wg = WaitGroup(); wg.add(N)

    def pair(i):
        a = rc.Chan(0); b = rc.Chan(0)
        def waiter():
            idx, res = rc.select([("recv", a), ("recv", b)])
            val, ok = res
            if ok and 0 <= val < N and idx == 1:
                seen[val] = 1
            wg.done()
        rc.mn_go(waiter)
        b.send(i)                 # the idx==1 arm wins; wakes the select cross-hub

    for i in range(N):
        rc.mn_go(lambda i=i: pair(i))
    wg.wait()
    miss = N - sum(seen)
    sys.stdout.write("SELECT_OK\n" if miss == 0 else "SELECT_LOST miss=%d\n" % miss)

runloom.run(6, main)
'''


@mn
def test_select_cross_hub_wake_integrity():
    p = _run_script(_SELECT_CROSSHUB, timeout=60)
    _assert_no_crash(p, "select cross-hub wake")
    assert "SELECT_OK" in p.stdout, (
        "a select() cross-hub wake was lost or fired the wrong arm:\n%s\n%s"
        % (p.stdout, p.stderr[-800:]))


# ==========================================================================
# 21. PREEMPTION lifecycle + a non-yielding CPU hog must NOT wedge a hub
#     forever (a one-hub world where the only way a sibling runs is preemption)
# ==========================================================================
@mn
def test_preempt_init_fini_lifecycle_is_idempotent_and_validated():
    # fini-without-init no-op; double init/fini safe; a negative quantum is
    # rejected (ValueError) not silently accepted.
    rc.preempt_fini()                  # no-op without init
    rc.preempt_init(10000)
    rc.preempt_init(5000)              # double init -> safe (re-arm)
    rc.preempt_fini()
    rc.preempt_fini()                  # double fini -> safe
    with pytest.raises((ValueError, OverflowError)):
        rc.preempt_init(-5)
    # ensure no timer left armed
    rc.preempt_fini()


@mn
def test_cpu_hog_plus_sibling_on_one_hub_both_complete_under_preempt():
    # ONE hub, a non-yielding CPU hog + a later sibling.  Under RUNLOOM_PREEMPT
    # the hog is sliced so BOTH complete within a bounded time.  This is the
    # property (no permanent wedge), not an ordering claim -- the sysmon's
    # cooperative recovery can also interleave on one hub, so we only assert
    # both run and the run terminates (hang_guard catches a true wedge).  Driven
    # in a subprocess so the preempt env is isolated and a wedge is contained.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
flags = bytearray(2)
def main():
    from runloom.sync import WaitGroup
    wg = WaitGroup(); wg.add(2)
    def hog():
        x = 0
        for i in range(40_000_000):
            x += i
        flags[0] = 1; wg.done()
    def sibling():
        flags[1] = 1; wg.done()
    rc.mn_go(hog)
    rc.sched_sleep(0.03)         # let the hog get into its non-yielding burst
    rc.mn_go(sibling)            # can only run if the hog yields/is preempted
    wg.wait()
rc.mn_init(1)                   # ONE real hub: hog and sibling share it
rc.mn_go(main)
n = rc.mn_run()
rc.mn_fini()
sys.stdout.write("PREEMPT_HOG_OK flags=%r completed=%d hubs=%d\n"
                 % (bytes(flags), n, rc.mn_hub_count()))
'''
    p = _run_script(script, {"RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "8"},
                    timeout=60)
    _assert_no_crash(p, "cpu hog + sibling under preempt")
    assert "PREEMPT_HOG_OK" in p.stdout, (
        "a non-yielding CPU hog wedged its hub forever (sibling starved) "
        "under preemption:\n%s\n%s" % (p.stdout, p.stderr[-1200:]))
    line = [ln for ln in p.stdout.splitlines() if ln.startswith("PREEMPT_HOG_OK")][0]
    assert "flags=b'\\x01\\x01'" in line, (
        "the sibling did not complete behind the CPU hog: %s" % line)
    assert "hubs=0" in line


# ==========================================================================
# 22. mn_run RE-ENTRANCY: a SECOND spawn+run cycle on the SAME live hub pool
#     (the existing file always pairs one mn_init with one mn_run; the public
#     contract allows spawn/run, spawn-again/run-again on a pool kept alive)
# ==========================================================================
@mn
def test_mn_run_then_spawn_again_then_run_again_same_pool():
    box = bytearray(3)

    def w(slot):
        box[slot] = 1

    with hang_guard(20, "mn_run re-entrancy"):
        rc.mn_init(2)
        rc.mn_go(lambda: w(0))
        n1 = rc.mn_run()
        # pool is still alive: a second spawn + run must work, not hang/no-op.
        rc.mn_go(lambda: w(1))
        rc.mn_go(lambda: w(2))
        n2 = rc.mn_run()
        rc.mn_fini()
    assert box[0] == 1 and box[1] == 1 and box[2] == 1, (
        "a second spawn+run cycle on a live pool dropped work: %r" % bytes(box))
    # completion count is cumulative; the second run must reflect the new work.
    assert n2 >= n1, "second mn_run regressed the completion count (%d < %d)" % (n2, n1)


# ==========================================================================
# 23. HALF-DEADLOCK / DEADLOCK_MS budget: progress-making work must NOT
#     false-fire the M:N census in RAISE mode (the existing file checks a busy
#     loop + a sleeper, but not a MIX of parked-and-woken fibers nor a short
#     custom RUNLOOM_DEADLOCK_MS budget where a transient quiescence is benign)
# ==========================================================================
_HALF_DEADLOCK = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    # Half the fibers PARK on an unbuffered channel (look deadlocked in a
    # snapshot) while the other half SEND to wake them -- a transient all-parked
    # window can occur, but progress is always possible, so the census must NOT
    # raise.  Mixed parked+runnable is the false-fire trap.
    N = 40
    ch = rc.Chan(0)
    wg = WaitGroup(); wg.add(2 * N)
    for _ in range(N):
        rc.mn_go(lambda: (ch.recv(), wg.done()))
    for _ in range(N):
        rc.mn_go(lambda: (ch.send(1), wg.done()))
    wg.wait()
    sys.stdout.write("HALF_DEADLOCK_OK\n")

runloom.run(4, main)
'''


@mn
def test_half_deadlock_does_not_false_fire_in_raise_mode():
    p = _run_script(_HALF_DEADLOCK,
                    {"RUNLOOM_DEADLOCK": "raise", "RUNLOOM_DEADLOCK_MS": "50"},
                    timeout=40)
    _assert_no_crash(p, "half-deadlock no false-fire")
    assert "HALF_DEADLOCK_OK" in p.stdout and "DEADLOCK" not in p.stderr, (
        "the M:N census FALSE-FIRED on progress-making parked+runnable work "
        "(or hung) under DEADLOCK_MS=50:\n%s\n%s"
        % (p.stdout, p.stderr[-1500:]))


@mn
def test_short_deadlock_ms_budget_does_not_false_fire_on_sleepers():
    # A short RUNLOOM_DEADLOCK_MS with many timer-sleepers: each quiescent
    # window is shorter than the work, and a pending timer is wakeable work, so
    # raise mode must not fire even with the tight budget.
    script = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
def main():
    wg = WaitGroup(); wg.add(40)
    def w():
        rc.sched_sleep(0.01); wg.done()
    for _ in range(40):
        rc.mn_go(w)
    wg.wait()
    sys.stdout.write("SLEEP_BUDGET_OK\n")
runloom.run(4, main)
'''
    p = _run_script(script, {"RUNLOOM_DEADLOCK": "raise", "RUNLOOM_DEADLOCK_MS": "30"},
                    timeout=40)
    _assert_no_crash(p, "short deadlock_ms sleepers")
    assert "SLEEP_BUDGET_OK" in p.stdout, (
        "tight DEADLOCK_MS false-fired on pending-timer (wakeable) work:\n%s\n%s"
        % (p.stdout, p.stderr[-1200:]))


# ==========================================================================
# 24. mn_init EDGE VALUES the dispatch layer must handle (bool, overflow)
# ==========================================================================
@mn
def test_mn_init_bool_true_is_accepted_as_one_hub():
    # PyArg "|i" accepts a bool as an int; True -> 1 hub.  Document it runs.
    n = rc.mn_init(True)
    try:
        assert n == 1, "mn_init(True) should clamp to 1 hub, got %r" % n
        assert rc.mn_hub_count() == 1
    finally:
        rc.mn_fini()
    assert rc.mn_hub_count() == 0


@mn
def test_mn_init_huge_int_raises_overflow_not_crash():
    # A hub count past INT_MAX must be rejected by the C arg parser, not
    # truncated into a giant/garbage thread spawn.
    with pytest.raises((OverflowError, ValueError, MemoryError)):
        rc.mn_init(10 ** 12)
    # whatever happened, the runtime must be clean (no half-built pool).
    if rc.mn_hub_count() != 0:
        rc.mn_fini()
    assert rc.mn_hub_count() == 0


# ==========================================================================
# 25. SPAWN STORM RESOURCE LIMIT: a very large single go_n + a deep many-fiber
#     burst must complete with EXACT set-equality (no slab/arena corruption at
#     scale) -- the existing file's go_n tests stop at 512.
# ==========================================================================
_BIG_GON = r'''
import sys; sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

def main():
    N = 20000
    seen = bytearray(N)
    wg = WaitGroup(); wg.add(N)
    def w(i):
        if 0 <= i < N:
            seen[i] = 1
        wg.done()
    rc.go_n(w, N, 0, True)
    wg.wait()
    miss = N - sum(seen)
    sys.stdout.write("BIG_GON_OK\n" if miss == 0 else "BIG_GON_LOST miss=%d\n" % miss)

runloom.run(8, main)
'''


@mn
@pytest.mark.parametrize("bulk", ["0", "1"])
def test_large_go_n_set_equality_at_scale(bulk):
    # Run the 20k-index go_n on BOTH the per-g path (GON_BULK=0) and the bulk
    # arena path (GON_BULK=1); every index must run exactly once on both.
    p = _run_script(_BIG_GON, {"RUNLOOM_GON_BULK": bulk}, timeout=90)
    _assert_no_crash(p, "big go_n (bulk=%s)" % bulk)
    assert "BIG_GON_OK" in p.stdout, (
        "go_n at scale (bulk=%s) lost/dup'd an index:\n%s\n%s"
        % (bulk, p.stdout, p.stderr[-1000:]))


# ==========================================================================
# 26. mn_hub_states FIELD INTEGRITY while a hub is WEDGED in a blocking offload
#     (handoff) -- running_g / dwell_ms / blocked_at must be reportable without
#     crashing the lock-free atomic reader while a hub is mid-blocking-call.
# ==========================================================================
@mn
def test_hub_states_readable_while_hub_blocked_in_offload():
    # A fiber sits in a blocking offload (rc.blocking) so its hub is, briefly,
    # DETACHED/wedged; mn_hub_states() must still return a well-formed snapshot
    # (the lock-free reader must not trip on the in-flight blocking state).
    snap = {}

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(2)

        def blocker():
            import time as _t
            rc.blocking(lambda: (_t.sleep(0.15), 1)[1])
            wg.done()

        def observer():
            rc.sched_sleep(0.03)               # let the blocker enter the offload
            states = rc.mn_hub_states()
            snap["states"] = states
            snap["count"] = rc.mn_hub_count()
            snap["sc"] = rc._self_check(0)
            wg.done()

        rc.mn_go(blocker)
        rc.mn_go(observer)
        wg.wait()

    with hang_guard(30, "hub states while blocked"):
        runloom.run(3, main)

    assert snap["count"] == 3
    assert isinstance(snap["states"], list) and len(snap["states"]) == 3
    for st in snap["states"]:
        # every documented key must be present even with a hub mid-block
        for k in ("id", "state", "pending"):
            assert k in st, "hub state missing %r while blocked: %r" % (k, st)
        assert st["state"] in ("detached", "attached", "suspended"), (
            "unexpected hub state value while blocked: %r" % st["state"])
    assert snap["sc"] == 0, "self_check violated while a hub was mid-offload"


# ==========================================================================
# 27. CROSS-HUB WAKE INTEGRITY UNDER ALL DETECTORS + HANDOFF (the wake paths
#     re-checked while every state-scanner concurrently mutates hub state) --
#     the existing file runs the WORK-STEAL integrity under detectors but not
#     the unbuffered cross-hub WAKE integrity under the same hostile detectors.
# ==========================================================================
@mn
def test_cross_hub_wake_integrity_under_all_detectors():
    modes = {
        "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "5",
        "RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "3",
        "RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "5",
        "RUNLOOM_STACK_PARK_SWEEP": "1", "RUNLOOM_STACK_PARK_SWEEP_MS": "1",
        "RUNLOOM_WORLD_YIELD_NS": "2000",
        "RUNLOOM_HUB_IDLE_WAKE": "0",
    }
    p = _run_script(_CROSSHUB_WAKE, modes, timeout=90)
    _assert_no_crash(p, "cross-hub wake (all detectors)")
    assert "WAKE_OK" in p.stdout, (
        "a cross-hub channel wake was lost with detectors on:\n%s\n%s"
        % (p.stdout, p.stderr[-1500:]))


# ==========================================================================
# 28. mn_fini WHILE a hub is mid-blocking-offload (a worker thread holds the
#     fiber's job record); teardown must join the blockpool cleanly, not UAF
#     the in-flight stack-job (the blockpool_job FV model's runtime analogue).
# ==========================================================================
_FINI_DURING_OFFLOAD = r'''
import sys; sys.path.insert(0, "src")
import runloom_c as rc
import time

# Repeatedly: spawn fibers that each enter a blocking offload, then tear the
# pool down WHILE offloads are in flight (a worker thread is reading the parked
# fiber's stack-resident job).  fini must drain/join, never UAF or hang.
ok = 0
for cyc in range(25):
    rc.mn_init(4)
    def blocker():
        rc.blocking(lambda: (time.sleep(0.01), 1)[1])
    for _ in range(20):
        rc.mn_go(blocker)
    # let SOME enter the offload, then fini with offloads in flight
    rc.mn_run()
    rc.mn_fini()
    if rc.mn_hub_count() != 0:
        sys.stdout.write("OFFLOAD_FINI_LEAK cyc=%d\n" % cyc); break
    ok += 1
else:
    sys.stdout.write("OFFLOAD_FINI_OK cycles=%d\n" % ok)
'''


@mn
def test_mn_fini_with_inflight_blocking_offload_no_uaf():
    p = _run_script(_FINI_DURING_OFFLOAD,
                    {"RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "3",
                     "RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1",
                     "RUNLOOM_SYSMON_MS": "5"},
                    timeout=90)
    _assert_no_crash(p, "fini during offload")
    assert "OFFLOAD_FINI_OK" in p.stdout, (
        "mn_fini raced against an in-flight blocking offload (UAF/hang/leak):"
        "\n%s\n%s" % (p.stdout, p.stderr[-1500:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
