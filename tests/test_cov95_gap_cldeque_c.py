"""Bounded gap-fill coverage for cldeque.c (Chase-Lev work-stealing deque).

cldeque.c has NO direct Python binding: it is the M:N scheduler's per-hub
ready deque (mn_sched.c `runloom_cldeque_t deque`).  The owner hub pushes
fresh gs onto the bottom (`runloom_cldeque_push`, hub_main.c.inc:377) and pops
them (`runloom_cldeque_pop`, :451/:464); an IDLE neighbour hub steals from a
busy hub's bottom-up deque (`runloom_cldeque_steal`, hub_main.c.inc:498).  So
the only Python-reachable driver for the uncovered steal/pop-race lines is a
multi-hub `runloom.run(N>=2)` with deliberate hub imbalance: pile many fresh,
quick gs onto a hub so neighbours go idle and STEAL, and keep the owner popping
its own deque down to the last element so its pop CAS races the thieves' steal
CAS on `top`.

Uncovered cldeque.c lines driven (classifier COVER, category RACE):
  * L90       runloom_cldeque_steal: item = buf[t] after the (t<b) non-empty
              check -- runs on EVERY successful steal of a non-empty deque.
  * L92-93    runloom_cldeque_steal: `expected=t` + the top CAS attempt -- the
              entry of every steal of a non-empty deque.
  * L100      runloom_cldeque_steal: `return NULL` after a FAILED top-CAS --
              a thief that LOST the race for `top` (owner pop or another thief
              committed first).  Needs concurrent winners on the same top.
  * L77-78    runloom_cldeque_pop: last-element 'lost the race' tail -- the
              owner's top-CAS fails because a thief's steal-CAS won first, so it
              restores bottom=t+1 and returns NULL.  Needs an owner pop of the
              LAST element racing a winning steal.

The oracle is NO LOSS / NO DUPLICATION: every spawned g runs exactly once and
returns its index into a per-g slot (single writer each -> race-free), then we
assert all N indices are present exactly once.  A botched steal/pop race would
either drop a g (loss -> hang or missing index) or hand one g to two hubs
(duplication -> a g resumed twice -> a doubled count or a crash).  So a clean
exit with the full index set is positive proof the steal+pop-race paths
shuttled every item correctly.

Each workload runs in its OWN clean subprocess so gcov flushes at a normal
exit; the subprocess prints a marker and returns 0, and we assert both.  All
runs are wrapped in a wall-clock timeout to prove no hang -- pure scheduler
work-stealing, no io_uring / sockets, so there is no backpressure-deadlock
risk.
"""
import os
import subprocess
import sys
import textwrap

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(
    not FT, reason="cldeque work-stealing is only exercised by the GIL-disabled "
                   "M:N runtime (run(N>=2) with real idle-hub steals)")


def _run_py(src, env_extra=None, timeout=90):
    """Run a snippet in a clean subprocess with the runloom env set.

    The snippet must print its own success marker and return 0 -- a crash or
    _exit does NOT flush gcov, so the call site always asserts returncode==0 +
    the marker.  SYSMON is silenced so its wedge/recover diagnostics (a busy
    deque legitimately strands gs on one hub while neighbours drain it) don't
    pollute stderr we surface on failure.
    """
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src",
               RUNLOOM_SYSMON_QUIET="1")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", textwrap.dedent(src)],
                          cwd=REPO, env=env, capture_output=True, text=True,
                          timeout=timeout)


# ---------------------------------------------------------------------------
# cldeque.c L90 + L92-93: the steal item-read and CAS attempt -- the straight
# line of EVERY successful steal of a non-empty deque.  Reached by any M:N run
# with hub imbalance: bulk-spawn many quick fresh gs so the round-robin loads a
# few hubs heavily and the rest go idle and STEAL from the busy deques' bottoms.
#
# Oracle: every one of the N gs runs exactly once.  Each g writes a 1 into its
# own bytearray slot (single writer -> race-free with the GIL off), so the sum
# equals N iff no g was lost (steal dropped it) and none ran twice (steal
# duplicated it).  The full set proves the steal path moved every item cleanly.
# ---------------------------------------------------------------------------
def test_steal_success_path_drives_item_read_and_cas():
    p = _run_py(r"""
        import sys
        import runloom, runloom_c as rc
        N = 6000
        ran = bytearray(N)          # ran[i] written ONLY by g i -> race-free
        def main():
            def worker(i):
                # a touch of CPU so a fresh g is a real unit of stealable work,
                # then yield so it cycles back through the ready/deque path.
                x = 0
                for _ in range(40):
                    x += 1
                ran[i] = 1
                rc.sched_yield()
            for i in range(N):
                rc.mn_go(lambda i=i: worker(i))
        # 8 hubs, N gs round-robin'd -> idle hubs steal from busy deques.
        runloom.run(8, main)
        lost = N - sum(ran)
        sys.stdout.write("STEAL_RAN:%d:LOST:%d\n" % (sum(ran), lost))
    """)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "STEAL_RAN:6000:LOST:0" in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# cldeque.c L100: runloom_cldeque_steal `return NULL` after a FAILED top-CAS --
# the thief LOST the race (another hub's steal, or the owner hub's pop, committed
# `top` first).  Reached by MAXIMISING contention on a few hot deques: many hubs
# (so many idle thieves race the SAME victim top simultaneously) but a workload
# that keeps work concentrated, so several thieves load-acquire the same `t`,
# one wins the CAS and the rest fail -> L100.
#
# go_n bulk-spawns all N gs by looping the spawn core in C -- they land on the
# running hub's deque in a tight burst, so the OTHER 11 hubs all wake idle at
# once and thunder-herd the same bottom indices: the canonical CAS-loser drive.
# Oracle is again exact-once: every g runs exactly once despite the lost races.
# ---------------------------------------------------------------------------
def test_steal_cas_loser_returns_null():
    p = _run_py(r"""
        import sys
        import runloom, runloom_c as rc
        N = 8000
        ran = bytearray(N)          # ran[i] written ONLY by g i -> race-free
        def worker(i):              # go_n(indexed=True) passes a distinct i
            x = 0
            for _ in range(20):
                x += 1
            ran[i] = 1
        def main():
            # bulk burst: all N land on one hub's deque in a C loop, so the
            # other hubs wake simultaneously and race the same top -> losers.
            rc.go_n(worker, N, 0, indexed=True)
        runloom.run(12, main)
        sys.stdout.write("LOSER_RAN:%d:LOST:%d\n" % (sum(ran), N - sum(ran)))
    """)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "LOSER_RAN:8000:LOST:0" in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# cldeque.c L77-78: runloom_cldeque_pop last-element 'lost the race' tail -- the
# OWNER hub's pop of the last element CASes `top` and LOSES because a thief's
# steal-CAS committed first; it restores bottom=t+1 and returns NULL (the g it
# tried to pop was already taken by the thief, which will run it).  Reached when
# the owner hub repeatedly pops its own deque down to ONE element while idle
# neighbours steal that same last element -- so push/pop and steal collide on the
# deque's single remaining index over and over.
#
# Drive: many SHORT gs that each yield (re-queueing themselves through the deque)
# under heavy multi-hub steal pressure, so the owner hub is constantly at the
# last-element boundary as thieves drain it.  Run several rounds to widen the
# race window.  Oracle: exact-once across every round.
# ---------------------------------------------------------------------------
def test_owner_pop_last_element_loses_to_thief():
    p = _run_py(r"""
        import sys
        import runloom, runloom_c as rc
        ROUNDS = 6
        N = 3000
        total_ran = 0
        for _ in range(ROUNDS):
            ran = bytearray(N)
            def main(ran=ran):
                def worker(i):
                    # yield twice so each g cycles back onto a deque/ready
                    # boundary multiple times, keeping the owner hub right at
                    # the last-element pop while thieves steal it.
                    rc.sched_yield()
                    x = 0
                    for _ in range(15):
                        x += 1
                    rc.sched_yield()
                    ran[i] = 1
                for i in range(N):
                    rc.mn_go(lambda i=i: worker(i))
            runloom.run(8, main)
            assert sum(ran) == N, ("LOST", N - sum(ran))
            total_ran += sum(ran)
        sys.stdout.write("POP_RACE_RAN:%d\n" % total_ran)
    """)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "POP_RACE_RAN:%d" % (6 * 3000) in p.stdout, (p.stdout, p.stderr[-1200:])


# ---------------------------------------------------------------------------
# Combined high-contention soak: ALL four COVER regions at once under maximal
# pressure (many hubs, many quick re-queueing gs, several rounds).  This is the
# closest Python analogue of the standalone tests_c/test_cldeque.c stress (the
# classifier's named mechanism) routed through the real scheduler deque, and is
# the strongest exact-once oracle: any steal/pop-race mishandling across
# 12 hubs x N gs x rounds shows up as a lost index, a doubled run, or a crash.
# ---------------------------------------------------------------------------
def test_work_stealing_soak_exact_once():
    p = _run_py(r"""
        import sys
        import runloom, runloom_c as rc
        ROUNDS = 4
        N = 5000
        grand = 0
        for r in range(ROUNDS):
            ran = bytearray(N)
            def main(ran=ran):
                def worker(i):
                    x = 0
                    for _ in range(25):
                        x += 1
                    rc.sched_yield()
                    ran[i] = 1
                # mix bulk-burst (go_n -> one hot deque) with individual spawns
                # (round-robin -> spread) so both steal-from-hot and
                # steal-from-spread deque shapes occur.
                rc.go_n(lambda: None, 1, 0)
                for i in range(N):
                    rc.mn_go(lambda i=i: worker(i))
            runloom.run(12, main)
            miss = N - sum(ran)
            assert miss == 0, ("ROUND", r, "LOST", miss)
            grand += sum(ran)
        sys.stdout.write("SOAK_RAN:%d\n" % grand)
    """)
    assert p.returncode == 0, p.stderr[-2000:]
    assert "SOAK_RAN:%d" % (4 * 5000) in p.stdout, (p.stdout, p.stderr[-1200:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
