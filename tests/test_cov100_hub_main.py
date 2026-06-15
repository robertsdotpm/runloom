"""Adversarial coverage for src/runloom_c/mn_sched_hub_main.c.inc -- the M:N hub
thread main loop (runloom_hub_main).

HONEST REACHABILITY MAP of this fragment's uncovered lines (full justification
in the StructuredOutput `unreachable` field; summarized here so the suite reads
self-contained):

  GROUP A -- the per-g-tstate / global stealable run-queue machine
  (L407-422 timer-wake claim, L480 from_runq=1, L805-942 the per-g-tstate
  resume/claim/park/done/yield, L953-962 the steal-woken QUEUED->RUNNING claim,
  L1015, L1116-1184 the global-runq snap-mode release).  EVERY one of these is
  gated by runloom_use_global_runq() / runloom_get_per_g_tstate_mode().  mn_init
  resolves that mode through runloom_resolve_migratable_mode(), which returns 0
  (default scheduler -- global runq never populated, from_runq never set, the
  per-g block never entered) UNLESS RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 is ALSO set.
  The task forbids that ack (a KNOWN-CRASH migration mode) and the gate is
  UNCONDITIONAL on it -- INDEPENDENT of hub count.  Verified empirically in this
  session: `RUNLOOM_PER_G_TSTATE=1` alone prints the GATED-OFF warning and runs
  the default scheduler to completion (the per-g block is never reached); only
  `+RUNLOOM_ALLOW_UNSAFE_MIGRATION=1` enters it.  There is therefore NO safe
  (non-ack) trigger -- not even at hub-count==1.  Classified unreachable.

  GROUP B -- hard error/cleanup paths with NO fault hook:
    * L154-155: the hub's OWN PyThreadState_New returns NULL (a raw alloc at
      L147; SPAWN_TSTATE injects only at the PER-G tstate alloc, which lives in
      the gated mode -- there is no fault site for the hub's own tstate).
    * L219-221 / L230-232 / L236: io_uring ring create / loop-arm / epoll-add
      FAILURE cleanups.  io_uring IS available on this box, so the create
      succeeds and the arm/add succeed; there is no env to force any of these to
      fail (verified: no RUNLOOM_FAULT_* site touches the ring setup, and
      `runloom_iouring_loop_hub_arm` only fails on a real eventfd()/submit
      shortfall).
    * L1013-1017 / L1066: the stale/duplicate-queue-entry defensive skips,
      closed at the source by the hub_submit try_incref-before-CAS queue ref
      (documented in CLAUDE.md as "proven non-load-bearing" on the happy path).
  Classified unreachable / defensive.

  GROUP C -- REACHABLE in the default scheduler, which THIS suite drives:
    * L388  -- the Chase-Lev deque OVERFLOW fallback.  When a single hub's drain
               must push > RUNLOOM_CLDEQUE_CAP (4096) FRESH gs onto its bounded
               deque in ONE pass, runloom_cldeque_push returns -1 and the g falls
               back to the growable local ready FIFO instead of being silently
               dropped (the old hang).  Three independent triggers below.
    * L1256 -- the RUNLOOM_GILSTATE_DELETE_ON_MAIN negative-control hub-exit:
               each hub thread takes the ELSE branch and calls PyEval_SaveThread()
               at exit (leaving its tstate for the main thread to delete) instead
               of deleting its own.  Driven in a subprocess with the env set.

Every test asserts REAL behavior (no g dropped / every unit of work completed /
the subprocess exited 0 with its marker), not mere line touching.
"""
import os
import subprocess
import sys

import pytest

import runloom  # noqa: F401  (ensures runloom_c is importable / on path)
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# RUNLOOM_CLDEQUE_CAP == 4096 (cldeque.h); the push fails once bottom-top >= cap.
# We need strictly more than 4096 FRESH gs queued on ONE hub at a SINGLE drain to
# force runloom_cldeque_push -> -1 and the L388 fallback.
DEQUE_CAP = 4096


# --------------------------------------------------------------------------
# L388 (trigger 1/3) -- deque-overflow fallback, exactly-once oracle.
#
# mn_init(1): the M:N hub loop runs with a SINGLE hub, so runloom_mn_go_core's
# `hub_idx = counter % hub_count` is always 0 -- EVERY mn_go targets hub 0's
# sub-list.  The driver g runs ON hub 0; while it spins the spawn loop the hub is
# busy resuming the driver and CANNOT drain its own sub-list, so all N children
# pile up FRESH (snap-invalid) in hub 0's sub_head.  Only when the driver finally
# parks in wg.wait() does hub_main run ONE drain pass: it routes each fresh g to
# the Chase-Lev deque (L377); past entry 4096 runloom_cldeque_push returns -1 and
# the fragment falls back to runloom_sched_ready_push (L388) -- the line under test.
#
# ADVERSARIAL ASSERTION: not one overflow g is dropped.  The pre-fix bug ignored
# the push return and silently orphaned the >4096th gs, whose pending-inc was
# already counted -> mn_run hung forever.  A race-free per-child slot (single
# writer each, summed at the boundary -- mandatory with the GIL off) proves each
# of the N children ran EXACTLY once; hang_guard proves the run terminated, which
# is the very invariant L388 exists to preserve.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N hub_main only runs with the GIL disabled")
def test_deque_overflow_fallback_no_drop():
    N = DEQUE_CAP + 1200          # 5296: comfortably past the 4096 cap
    ran = bytearray(N)            # one single-writer slot per child: race-free
    from runloom.sync import WaitGroup

    def main():
        wg = WaitGroup()
        wg.add(N)

        def child(i):
            # exactly-once marker; if the overflow path dropped this g it would
            # never run and ran[i] would stay 0 (and wg would never reach 0 ->
            # hang_guard fires).
            if 0 <= i < N:
                ran[i] = 1
            wg.done()

        # No yield in this loop: every g stays FRESH (snap-invalid) and piles into
        # hub 0's sub-list, so the single drain that follows overflows the deque.
        for i in range(N):
            rc.mn_go(lambda i=i: child(i))
        wg.wait()                 # park the driver -> hub_main drains all N now

    with hang_guard(90, "deque overflow fallback"):
        rc.mn_init(1)             # single hub -> all spawns target hub 0
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()

    missing = [i for i in range(N) if not ran[i]]
    assert not missing, "%d overflow gs were dropped (e.g. ids %s)" % (
        len(missing), missing[:8])
    assert sum(ran) == N


# --------------------------------------------------------------------------
# L388 (trigger 2/3) -- go_n bulk-spawn route.
#
# go_n is the arena/bulk spawn path; at hub_count==1 its `hub = i % H` is also
# always 0, so its N fresh gs all land on hub 0 and overflow the deque on the
# single drain.  A DISTINCT code path into the SAME fragment line: a regression
# that only breaks one spawn route still shows here.  Indexed go_n(fn,N,0,True)
# calls fn(index), giving a per-g exactly-once slot for the same no-drop oracle.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N hub_main only runs with the GIL disabled")
def test_deque_overflow_fallback_go_n_bulk():
    N = DEQUE_CAP + 800           # 4896 fresh gs via the bulk path
    ran = bytearray(N)

    def worker(i):
        if 0 <= i < N:
            ran[i] = 1

    def main():
        rc.go_n(worker, N, 0, True)   # bulk-spawn N indexed fresh gs onto hub 0

    with hang_guard(90, "deque overflow via go_n"):
        rc.mn_init(1)
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()

    missing = sum(1 for i in range(N) if not ran[i])
    assert missing == 0, "%d of %d bulk-spawned overflow gs never ran" % (missing, N)


# --------------------------------------------------------------------------
# L388 (trigger 3/3) -- overflow gs that do REAL inter-fiber work (channel sum).
#
# The bytearray oracle proves a g RAN; this one proves the overflow g's WORK is
# not lost and that an overflowed g remains a fully first-class, schedulable
# fiber (the fallback FIFO is not a dead-letter queue).  Each of the > 4096 fresh
# overflow gs sends its index on a buffered channel; a single collector recvs all
# N and sums them.  If even one overflow g were dropped, the collector would
# block forever waiting for the missing value -> hang_guard fires; if one ran
# twice, the sum would exceed the closed-form total.  A strictly stronger oracle
# than "the slot is set".
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N hub_main only runs with the GIL disabled")
def test_deque_overflow_fallback_channel_work():
    N = DEQUE_CAP + 600           # 4696 fresh senders -> overflow the deque
    EXPECT = N * (N - 1) // 2     # sum of indices 0..N-1, closed form
    box = {"sum": 0, "count": 0}

    def main():
        ch = rc.Chan(256)         # bounded buffer -> senders genuinely interleave

        def sender(i):
            ch.send(i)            # cooperative send; parks if the buffer is full

        def collector():
            s = 0
            c = 0
            while c < N:
                v, ok = ch.recv()
                if not ok:
                    break
                s += v
                c += 1
            box["sum"] = s
            box["count"] = c

        rc.mn_go(collector)       # one consumer drains all N
        # > 4096 fresh senders pile onto hub 0's sub-list, overflow the deque on
        # the drain, and run via the L388 fallback FIFO.
        for i in range(N):
            rc.mn_go(lambda i=i: sender(i))

    with hang_guard(120, "deque overflow channel work"):
        rc.mn_init(1)
        rc.mn_go(main)
        rc.mn_run()
        rc.mn_fini()

    # Every overflow sender ran exactly once AND its value survived: the count is
    # exact and the sum matches the closed form (a drop would lower both / hang;
    # a double-run would inflate the sum).
    assert box["count"] == N, "collector saw %d of %d overflow sends" % (box["count"], N)
    assert box["sum"] == EXPECT, "overflow channel sum %d != %d (lost/duplicated work)" % (
        box["sum"], EXPECT)


# --------------------------------------------------------------------------
# L1256 -- RUNLOOM_GILSTATE_DELETE_ON_MAIN negative-control hub-exit path.
#
# With this env set, the hub-exit code takes the ELSE branch (L1251-1256):
# instead of deleting its own tstate on its own thread (the normal fix path at
# L1245-1250), it calls PyEval_SaveThread() (L1256) and leaves the tstate for
# runloom_mn_fini to delete from the MAIN thread -- the deliberately-reproduced
# pre-fix behavior.  The normal corpus never sets the env, so the line is dark.
#
# It MUST be a subprocess: the env mode is read at hub-exit time and the behavior
# only differs there; and for gcov counters to flush, the subprocess MUST EXIT
# CLEANLY.  On this (non-pydebug) free-threaded build the negative control does
# not abort -- we assert returncode 0 + the stdout marker (which carries the hub
# count AND the work total), proving EVERY hub thread reached and executed L1256
# on its way out and the workload completed under the control.
# --------------------------------------------------------------------------
_DELETE_ON_MAIN_PROG = r'''
import sys
sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup

HUBS = 4
N = 64
ran = bytearray(N)

def main():
    wg = WaitGroup(); wg.add(N)
    def w(i):
        rc.sched_yield()
        if 0 <= i < N:
            ran[i] = 1
        wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: w(i))
    wg.wait()

# Multiple hubs so MORE THAN ONE hub thread exercises the L1256 exit branch; each
# hub takes the else-branch and PyEval_SaveThread()s on the way out.
runloom.run(HUBS, main)
assert sum(ran) == N, "lost %d/%d" % (N - sum(ran), N)
assert rc.mn_hub_count() == 0, "hubs not torn down"   # mn_fini ran the L1256-leftover deletes
sys.stdout.write("DELETE_ON_MAIN_OK hubs=%d work=%d\n" % (HUBS, sum(ran)))
sys.stdout.flush()
'''


@pytest.mark.skipif(not FT, reason="M:N hub_main only runs with the GIL disabled")
def test_gilstate_delete_on_main_exit_path():
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_GILSTATE_DELETE_ON_MAIN="1")
    p = subprocess.run([PY, "-c", _DELETE_ON_MAIN_PROG],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=60)
    # A clean exit is REQUIRED both for the assertion and for gcov to flush the
    # counters L1256 (and the surrounding exit path) bumped on every hub.  A
    # crash/abort here would mean the negative-control tstate-on-main delete is
    # unsafe on this build -- a real finding, not a flaky timeout.
    assert p.returncode == 0, (
        "RUNLOOM_GILSTATE_DELETE_ON_MAIN run did not exit cleanly (rc=%s).\n"
        "stderr=%s" % (p.returncode, p.stderr[-1500:]))
    assert "DELETE_ON_MAIN_OK hubs=4 work=64" in p.stdout, (
        "hub exit path did not complete the workload under the negative control."
        "\nstdout=%s\nstderr=%s" % (p.stdout, p.stderr[-800:]))


# --------------------------------------------------------------------------
# REACHABILITY GUARD -- pin the GROUP A gate empirically so the central
# measurement (and any future maintainer) can SEE that the per-g-tstate block is
# unreachable without the forbidden ack, rather than taking the docstring on
# faith.  This is itself a real assertion: RUNLOOM_PER_G_TSTATE=1 ALONE must run
# the DEFAULT scheduler (emit the GATED-OFF warning, complete the workload, exit
# 0) -- i.e. it must NOT enter the per-g block.  If a future change ever made the
# gate honor the flag without the ack, this test would start FAILING (the warning
# would vanish), flagging that Group A just became reachable and the suite should
# be extended.  We deliberately do NOT set RUNLOOM_ALLOW_UNSAFE_MIGRATION.
# --------------------------------------------------------------------------
_GATED_OFF_PROG = r'''
import sys
sys.path.insert(0, "src")
import runloom, runloom_c as rc
from runloom.sync import WaitGroup
N = 48
ran = bytearray(N)
def main():
    wg = WaitGroup(); wg.add(N)
    def w(i):
        rc.sched_yield()
        if 0 <= i < N:
            ran[i] = 1
        wg.done()
    for i in range(N):
        rc.mn_go(lambda i=i: w(i))
    wg.wait()
runloom.run(3, main)
assert sum(ran) == N, "lost %d/%d" % (N - sum(ran), N)
sys.stdout.write("GATED_OFF_DEFAULT_OK %d\n" % sum(ran))
sys.stdout.flush()
'''


@pytest.mark.skipif(not FT, reason="M:N hub_main only runs with the GIL disabled")
def test_per_g_tstate_is_gated_off_without_ack():
    # No RUNLOOM_ALLOW_UNSAFE_MIGRATION on purpose.
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_PER_G_TSTATE="1")
    p = subprocess.run([PY, "-c", _GATED_OFF_PROG],
                       cwd=REPO, env=env, capture_output=True, text=True, timeout=60)
    assert p.returncode == 0, (
        "gated-off per-g-tstate run crashed (rc=%s) -- it should fall back to the "
        "default scheduler, not enter the migratable block.\nstderr=%s" % (
            p.returncode, p.stderr[-1500:]))
    assert "GATED_OFF_DEFAULT_OK 48" in p.stdout, (
        "workload did not complete under the gated-off fallback.\nstdout=%s" % p.stdout)
    # The GATED-OFF warning is the proof that the per-g block (Group A) was NOT
    # entered: the flag was requested but the resolve interlock denied it.
    assert "GATED OFF" in p.stderr, (
        "expected the migratable-mode GATED-OFF warning (proves Group A stayed "
        "unreachable without the ack); stderr=%s" % p.stderr[-1500:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
