"""big_100 / 49 -- barrier stress.

Goroutines split into groups; each group repeatedly meets at a reusable
threading.Barrier.  Because the barrier releases every member of a group
together each generation, the members' cycle counts stay locked within one of
each other -- if the barrier let someone race ahead, the spread would blow up.

Stresses: reusable barrier generations, wake ordering, the Condition under it.
"""
import threading

import harness

PARTY = 16          # goroutines per barrier group


def setup(H):
    ngroups = max(1, H.funcs // PARTY)
    H.state = {
        "barriers": [threading.Barrier(PARTY) for _ in range(ngroups)],
        "cycles": [0] * (ngroups * PARTY),
        "ngroups": ngroups,
    }


def worker(H, wid, rng, state):
    group = wid // PARTY
    if group >= state["ngroups"]:
        return
    barrier = state["barriers"][group]
    cycles = state["cycles"]
    while H.running():
        try:
            barrier.wait(timeout=10)
        except threading.BrokenBarrierError:
            break
        except Exception:
            break
        cycles[wid] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        H.sleep(2.0)
        while H.running():
            H.sleep(1.0)
            for g in range(H.state["ngroups"]):
                base = g * PARTY
                grp = H.state["cycles"][base:base + PARTY]
                if min(grp) == 0:
                    continue            # group not all started
                if not H.check(max(grp) - min(grp) <= 1,
                               "barrier group {0} desynced: {1}".format(
                                   g, max(grp) - min(grp))):
                    return

    H.go(auditor)
    # Abort the barriers at teardown so any member parked in wait() wakes via
    # BrokenBarrierError instead of hanging the drain.

    def breaker():
        while H.running():
            H.sleep(0.1)
        for b in H.state["barriers"]:
            try:
                b.abort()
            except Exception:
                pass

    H.go(breaker)


if __name__ == "__main__":
    harness.main("p49_barrier_stress", body, setup=setup, default_funcs=2048,
                 describe="reusable barriers keep group members in lock-step")
