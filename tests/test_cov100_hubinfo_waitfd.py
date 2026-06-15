"""Coverage-driven adversarial suite for two fragments:

  * src/runloom_c/mn_sched_hubinfo.c.inc   -- the per-hub diagnostic snapshot
    (runloom.inspect.hubs() / rc.mn_hub_states()), in particular the `blocked_at`
    capture of a DETACHED-wedged hub's top Python frame and the handoff-claim
    CAS that locks the rescue out for the duration of that frame walk.
  * src/runloom_c/netpoll_wait_fd.c.inc    -- the drain/signal-wake CAS-retry
    loops and the wait_fd park/abort/resume cleanup paths.

WHY THE NORMAL CORPUS MISSES THESE
----------------------------------
hubinfo's `blocked_at` block (L111-140) runs ONLY when a hub has been
DETACHED (its running fiber did Py_BEGIN_ALLOW_THREADS and is parked in a real
blocking syscall) for longer than the sysmon wedge budget -- and the
`resume_start_ns` clock that the dwell test reads is stamped ONLY when
RUNLOOM_SYSMON is on (runloom_hub_resume_begin).  The handoff-claim CAS inside it
(L115-117 lock, L134-136 unlock) additionally needs RUNLOOM_HANDOFF on.  None of
that is in the default scheduler mode, so every line gated on it is dark unless a
test deliberately manufactures a DETACHED wedge under those env modes AND samples
mn_hub_states() during the wedge window.

The wait_fd drain-loop CAS load (L37) only runs when sched_reset() finds a
parker OWNED BY THE CALLING THREAD's single-thread scheduler; the corpus calls
sched_reset() but apparently never with a live same-thread wait_fd parker linked.
The signal-wake CAS load (L101) only runs when a raised Python signal handler is
handed to a fiber parked in wait_fd on the idle single-thread scheduler.

TECHNIQUES
----------
Env-gated modes (sysmon/handoff) are read once at import/first-run, so every
mode-dependent scenario runs in its OWN SUBPROCESS with the env set and asserts
on a stdout marker + a clean (rc 0) exit -- gcov only counts a clean exit.  The
two single-thread-scheduler paths (drain, signal-wake) need no env mode and run
in-process / in a hygienic subprocess.

Uncovered lines driven (line numbers in each .inc):

  mn_sched_hubinfo.c.inc
    * L115-117  handoff-claim CAS FREE->OWNED (lock the rescue out before the
                frame walk).  Reached: RUNLOOM_HANDOFF on + a DETACHED-wedged
                hub sampled by mn_hub_states() mid-wedge.  A captured non-None
                `blocked_at` under handoff PROVES the CAS won (locked==1), since
                blocked_at is gated on `!handoff_on || locked`.
    * L134-136  the matching CAS OWNED->FREE the instant the walk is done.  Same
                trigger: a captured blocked_at under handoff implies locked==1,
                so both the lock and the unlock executed.

  netpoll_wait_fd.c.inc
    * L37       drain_parked CAS-retry load: `cur = load(&p->commit)` inside the
                claim loop, run for every same-thread parker sched_reset() drains.
    * L101      signal_wake CAS-retry load: the same claim-loop load, run for the
                wait_fd parker that takes a raised signal handler.
    * L316-320  post-register pending re-check unlink+release+return: an
                ADD-synthesized edge processed by a SIBLING hub's pump sets the
                fd's pending-wake bit between this parker's link and its second
                consume.  Driven stochastically by many fibers parking on
                already-ready sockets across several hubs.

See the structured `unreachable` report for L53 (the co_name fallback) and
L56 / L347 / L390 (defensive guards / weak-memory-only races), with reasons.
"""
import os
import socket
import subprocess
import sys

import pytest

import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

READ, WRITE = 1, 2
FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable


def _drop(fd):
    """Close an fd the netpoll-clean way: clear its arm cache first."""
    try:
        rc.netpoll_unregister(fd)
    except Exception:
        pass
    try:
        os.close(fd)
    except OSError:
        pass


def _run_subprocess(script, env_extra, timeout=40):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


# ==========================================================================
# mn_sched_hubinfo.c.inc -- the DETACHED-wedge blocked_at capture + handoff CAS
# ==========================================================================

# A fiber does a REAL time.sleep (Py_BEGIN_ALLOW_THREADS -> the hub's bound
# tstate goes DETACHED) and holds the hub's resume clock for the whole sleep --
# a textbook DETACHED wedge.  A SECOND fiber, on a DIFFERENT hub, samples
# mn_hub_states() tightly during the wedge.  Each sample that returns a non-None
# `blocked_at` for a 'detached' hub ran the L111-140 block: it CAS-locked the
# handoff claim (L115-117), re-confirmed DETACHED, walked the top frame
# (L123-131), and CAS-unlocked (L134-136).
_HUBINFO_WEDGE = r'''
import sys, time
sys.path.insert(0, "src")
import runloom
import runloom_c as rc

def main():
    import time as _t
    def wedger():
        _t.sleep({sleep})                 # REAL blocking sleep -> tstate DETACHED
    def watcher():
        got = 0
        names = set()
        deadline = _t.time() + {watch}
        while _t.time() < deadline:
            for s in rc.mn_hub_states():
                ba = s.get("blocked_at")
                if ba and s.get("state") == "detached":
                    got += 1
                    names.add(ba.split(" ")[0])   # the qualname captured
            _t.sleep(0.003)
        sys.stdout.write("HUBINFO_OK %d %s\n" % (got, "wedger" if any(
            "wedger" in n for n in names) else "none"))
        sys.stdout.flush()
    rc.mn_go(wedger)
    rc.mn_go(watcher)

runloom.run(3, main)
'''


def _parse_hubinfo_ok(stdout):
    for line in stdout.splitlines():
        if line.startswith("HUBINFO_OK"):
            parts = line.split()
            return int(parts[1]), parts[2]
    return None


@pytest.mark.skipif(not FT, reason="DETACHED-wedge snapshot needs the M:N runtime")
def test_hubinfo_blocked_at_under_handoff_drives_claim_cas():
    """Drives mn_sched_hubinfo.c.inc L111-140 INCLUDING the handoff-claim CAS
    lock (L115-117) and unlock (L134-136).

    With RUNLOOM_HANDOFF on, `blocked_at` is written only inside the
    `if (!handoff_on || locked)` guard -- and handoff_on is true -- so any
    captured blocked_at proves the FREE->OWNED CAS *won* (locked==1).  That in
    turn means both the locking CAS (L115-117) and the unlocking CAS (L134-136)
    executed, and the frame walk ran between them.  The decisive assertion is
    that the captured frame is the WEDGER's own call site, i.e. we read the top
    frame of the genuinely-DETACHED hub, not a spurious one."""
    env = {
        "RUNLOOM_HANDOFF": "1",
        "RUNLOOM_HANDOFF_POOL": "2",
        "RUNLOOM_SYSMON": "1",
        "RUNLOOM_SYSMON_QUIET": "1",
        # 50ms wedge budget: wide enough that the snapshot's CAS reliably wins
        # the claim before the rescue adopts and clears the resume clock.
        "RUNLOOM_SYSMON_MS": "50",
        # keep ATTACHED-preempt out of the picture -- we want a pure DETACHED wedge.
        "RUNLOOM_PREEMPT": "0",
    }
    script = _HUBINFO_WEDGE.format(sleep="0.6", watch="0.55")
    p = _run_subprocess(script, env, timeout=40)
    assert p.returncode == 0, (
        "handoff-wedge hubinfo run did not exit cleanly (rc=%s)\nstderr=%s"
        % (p.returncode, p.stderr[-1500:]))
    parsed = _parse_hubinfo_ok(p.stdout)
    assert parsed is not None, (
        "no HUBINFO_OK marker -- the workload hung/crashed\nstdout=%s\nstderr=%s"
        % (p.stdout[-500:], p.stderr[-1200:]))
    got, who = parsed
    assert got >= 1, (
        "mn_hub_states() never captured a blocked_at for a DETACHED hub under "
        "handoff -- the handoff-claim CAS path (L115-117 / L134-136) and the "
        "frame walk never ran (got=%d)" % got)
    assert who == "wedger", (
        "captured a blocked_at, but not the wedger's frame (who=%r) -- the walk "
        "did not read the wedged hub's top frame" % who)


@pytest.mark.skipif(not FT, reason="DETACHED-wedge snapshot needs the M:N runtime")
def test_hubinfo_blocked_at_without_handoff_takes_no_cas_branch():
    """Complementary path: with RUNLOOM_HANDOFF off, the same DETACHED wedge
    still yields a `blocked_at` (the `!handoff_on` branch of L120), but WITHOUT
    touching the handoff-claim CAS.  This pins the behavioural contract that the
    blocked_at capture is independent of the rescue subsystem, and exercises the
    frame walk (L123-131) on the no-handoff side so the two branches are
    distinguished rather than conflated.

    Adversarial property: the wedger's frame is captured even though no rescue
    pool exists to lock out -- and the run still tears down cleanly."""
    env = {
        "RUNLOOM_SYSMON": "1",
        "RUNLOOM_SYSMON_QUIET": "1",
        "RUNLOOM_SYSMON_MS": "30",
        "RUNLOOM_HANDOFF": "0",
        "RUNLOOM_PREEMPT": "0",
    }
    script = _HUBINFO_WEDGE.format(sleep="0.5", watch="0.45")
    p = _run_subprocess(script, env, timeout=40)
    assert p.returncode == 0, (
        "no-handoff wedge run crashed (rc=%s)\nstderr=%s"
        % (p.returncode, p.stderr[-1500:]))
    parsed = _parse_hubinfo_ok(p.stdout)
    assert parsed is not None, "no HUBINFO_OK marker\nstderr=%s" % p.stderr[-1000:]
    got, who = parsed
    assert got >= 1 and who == "wedger", (
        "no-handoff DETACHED wedge did not surface the wedger's frame "
        "(got=%d who=%r)" % (got, who))


# ==========================================================================
# netpoll_wait_fd.c.inc -- drain_parked CAS-retry load (L37)
# ==========================================================================
@pytest.mark.skipif(not FT, reason="single-thread drain needs the runtime")
def test_sched_reset_drains_same_thread_wait_fd_parker():
    """Drives netpoll_wait_fd.c.inc L37 (the `cur = load(&p->commit)` inside
    drain_parked's claim loop).

    runloom_netpoll_drain_parked is SCOPED to the calling thread's scheduler:
    it only touches parkers whose g->owner == this thread's sched.  A fiber that
    parks forever in wait_fd on the single-thread scheduler, then a sibling
    fiber on the SAME thread calling sched_reset(), is exactly that case -- the
    drain finds the parker, runs the claim loop (L36-44, hence the L37 load),
    sets ready_out=-1 (L50), and re-queues the committed g (L54-59).

    Adversarial property: sched_reset() reports n_parked==1 (it found and
    cancelled the parker), and afterwards the runtime is structurally clean
    (self_check==0) with the per-sched parked count back to 0 -- a drain that
    failed to claim/unlink the parker would leave it linked (parked>0) or trip
    self_check."""
    res = {}

    def waiter():
        r, w = os.pipe()
        res["fds"] = (r, w)
        # park forever; only the drain frees it.
        res["rv"] = rc.wait_fd(r, READ, -1)

    def driver():
        rc.sched_yield(); rc.sched_yield()      # let the waiter commit its park
        # n_ready, n_sleep, n_parked
        res["reset"] = rc.sched_reset()

    with hang_guard(15, "sched_reset drain"):
        rc.go(waiter)
        rc.go(driver)
        rc.run()

    r, w = res["fds"]
    _drop(r); _drop(w)
    assert res.get("reset") is not None, "sched_reset never ran"
    n_ready, n_sleep, n_parked = res["reset"]
    assert n_parked == 1, (
        "sched_reset drained %d parkers, expected exactly 1 (the same-thread "
        "wait_fd waiter) -- the drain's owner-scoped claim loop (L37) did not "
        "match the parker" % n_parked)
    assert rc._self_check(0) == 0, "drain left the netpoll structures inconsistent"
    s = rc.stats()
    assert int(s.get("netpoll_parked_self", s["netpoll_parked"])) == 0, (
        "a parker survived the drain -- the claim/unlink in the L36-44 loop did "
        "not complete")


@pytest.mark.skipif(not FT, reason="single-thread drain needs the runtime")
def test_sched_reset_drains_many_same_thread_parkers():
    """Re-exercises the drain claim loop (L37) across MANY parkers so the loop
    is walked repeatedly (each linked parker runs the L36-44 claim), not just
    once.  Hardens the L37 coverage against a single-parker fast path and proves
    the per-fd bucket/global-list/heap unlink stays consistent across a batch.

    Adversarial property: every one of N same-thread parkers is drained
    (n_parked==N) and the structures are clean afterward."""
    N = 16
    res = {"fds": []}

    def waiter():
        r, w = os.pipe()
        res["fds"].append((r, w))
        rc.wait_fd(r, READ, -1)

    def driver():
        for _ in range(4):
            rc.sched_yield()
        res["reset"] = rc.sched_reset()

    with hang_guard(20, "sched_reset drain many"):
        for _ in range(N):
            rc.go(waiter)
        rc.go(driver)
        rc.run()

    for r, w in res["fds"]:
        _drop(r); _drop(w)
    assert res.get("reset") is not None
    assert res["reset"][2] == N, (
        "drained %d/%d parkers -- the claim loop did not process every linked "
        "same-thread parker" % (res["reset"][2], N))
    assert rc._self_check(0) == 0


# ==========================================================================
# netpoll_wait_fd.c.inc -- signal_wake CAS-retry load (L101)
# ==========================================================================

# A raised Python signal handler is handed to a fiber parked in wait_fd via
# runloom_netpoll_signal_wake, whose claim loop (L100-107) runs the L101 load for
# the matching parker.  We use a raw wait_fd park on a never-ready socket and a
# real SIGALRM whose handler raises, in a subprocess so the process signal state
# stays out of the pytest process.  On resume wait_fd restores the exception
# (L401-411) and returns -1 so it raises out of the cooperative call.
_SIGNAL_WAKE = r'''
import sys, signal, socket
sys.path.insert(0, "src")
import runloom_c as rc
READ = 1

class Boom(Exception):
    pass

def handler(signum, frame):
    raise Boom

box = {}
def body():
    rd, wr = socket.socketpair()
    box["socks"] = (rd, wr)
    signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, 0.3)
    try:
        # park forever in wait_fd; ONLY the signal frees it -- so the wake came
        # through runloom_netpoll_signal_wake (the L101 claim loop) and the
        # wait_fd signal-restore tail (L401-411).
        box["rv"] = rc.wait_fd(rd.fileno(), READ, -1)
    except Boom:
        box["caught"] = True

try:
    rc.go(body)
    rc.run()
except BaseException as e:
    box["escaped"] = repr(e)

rd, wr = box["socks"]
try:
    rc.netpoll_unregister(rd.fileno())
except Exception:
    pass
rd.close(); wr.close()
# A clean run: the signal raised INTO the fiber (caught), nothing escaped run(),
# and wait_fd did NOT return a value (it raised instead).
sys.stdout.write("SIGWAKE caught=%r rv=%r escaped=%r\n" % (
    box.get("caught"), box.get("rv"), box.get("escaped")))
'''


@pytest.mark.skipif(not FT, reason="signal-into-wait_fd needs the runtime")
def test_signal_handler_raises_into_wait_fd_parker():
    """Drives netpoll_wait_fd.c.inc L101 (the signal_wake claim-loop load) and
    the wait_fd signal-restore tail (L401-411).

    A SIGALRM whose handler raises is delivered while a fiber is parked forever
    in wait_fd on a never-ready socket.  The idle scheduler runs the handler,
    it raises, and runloom_netpoll_signal_wake hands the exception to the parked
    fiber: it iterates parkers, runs the claim CAS loop (L100-107, hence L101),
    stamps the SIGNALED sentinel, and re-queues the fiber.  On resume wait_fd
    restores the exception into the fiber's tstate and returns -1 so it raises
    out of the cooperative blocking call.

    Adversarial property: the exception is caught INSIDE the fiber's own
    try/except (caught == True), wait_fd returned no value (rv is None -- it
    raised), and NOTHING escaped out of run().  A swallowed or out-of-run()
    delivery (the bug this path guards against) would flip all three."""
    p = _run_subprocess(_SIGNAL_WAKE, {}, timeout=25)
    assert p.returncode == 0, (
        "signal-wake run crashed (rc=%s)\nstderr=%s" % (p.returncode, p.stderr[-1500:]))
    line = next((L for L in p.stdout.splitlines() if L.startswith("SIGWAKE")), None)
    assert line is not None, (
        "no SIGWAKE marker -- the signal never freed the wait_fd parker (a lost "
        "wake)\nstdout=%s\nstderr=%s" % (p.stdout[-400:], p.stderr[-1000:]))
    assert "caught=True" in line, (
        "the signal handler's exception did NOT raise into the fiber's "
        "try/except -- signal_wake (L101) did not deliver it to the parker: %r" % line)
    assert "rv=None" in line, (
        "wait_fd returned a value instead of raising the signal exception -- the "
        "L401-411 restore tail did not run: %r" % line)
    assert "escaped=None" in line, (
        "the signal exception escaped out of run() instead of into the parked "
        "fiber -- the scheduler stole it (the bug signal_wake fixes): %r" % line)


# ==========================================================================
# netpoll_wait_fd.c.inc -- post-register pending re-check (L313-322 / L316-320)
# ==========================================================================
@pytest.mark.skipif(not FT, reason="needs the multi-hub M:N runtime")
def test_park_on_ready_sockets_across_hubs_post_register_recheck():
    """Targets netpoll_wait_fd.c.inc L313-322 (the SECOND pending-wake consume,
    after netpoll_register): when a parker links and then epoll ADD synthesizes
    an edge for an already-ready fd, a SIBLING hub's pump can process that edge
    and stash a pending-wake bit BEFORE this parker's post-register consume runs
    -- which then unlinks the parker and returns the readiness directly
    (L316-320) instead of parking.

    The workload maximises that race: many fibers across several hubs park
    WRITE on freshly-made socketpairs that are IMMEDIATELY writable, so every
    wait_fd issues an ADD whose synthesized writable edge competes with the
    sibling pumps.  Repeating per fiber widens the window.

    Adversarial property (race-tolerant but real): every park returns WRITE-ready
    promptly (no lost wake / no hang within the guard), and the runtime stays
    structurally consistent with no leaked parker -- the only correct outcome
    whether a given park took the pending-consume fast path or the commit-CAS
    path."""
    from runloom.sync import WaitGroup
    NWORK = 120
    socks = []
    wg = WaitGroup(); wg.add(NWORK)
    bad = bytearray(NWORK)            # one slot per fiber, single writer each

    def worker(i):
        try:
            a, b = socket.socketpair()
            a.setblocking(False); b.setblocking(False)
            socks.append((a, b))
            for _ in range(6):
                rv = rc.wait_fd(a.fileno(), WRITE, 2000)
                if not (rv & WRITE):     # timeout (0) or cancel (-1) == a lost wake
                    bad[i] = 1
                    break
                rc.sched_yield()
        finally:
            wg.done()

    def main():
        for i in range(NWORK):
            rc.mn_go(lambda i=i: worker(i))
        wg.wait()

    with hang_guard(30, "post-register recheck race"):
        import runloom
        runloom.run(6, main)

    for a, b in socks:
        _drop(a.fileno()); _drop(b.fileno())
    assert sum(bad) == 0, (
        "%d/%d fibers saw a non-WRITE result parking on an already-writable "
        "socket -- a wakeup was lost in the link/register/consume window"
        % (sum(bad), NWORK))
    assert rc._self_check(0) == 0, "concurrent parking left the structures inconsistent"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
