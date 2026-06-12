"""Phase 2b: untimed Event/Condition/Semaphore waits park IN MEMORY (0 per-waiter
fds), via runloom_c.park() + g.wake(), instead of one OS pipe/socketpair per
waiter.  A million coroutines on event.wait() previously cost ~2M fds; now ~1
(the shared run-alive anchor).  Timed waits and FOREIGN-thread waits keep the
fd-backed park (park() has no deadline and can't serve a non-goroutine).  The
wake side (_unpark_all) wakes both kinds; a foreign SETTER wakes in-memory
waiters via g.wake() (the run-alive anchor keeps run() alive for it).

Guards the fd reduction AND the load-bearing correctness: wake-before-park, a
foreign-thread setter, and that timed/foreign paths still work.
"""
import os
import threading

import runloom
import runloom.monkey as monkey

monkey.patch()
import threading as th   # noqa: E402  (patched -> Co* primitives)


def _count_fds():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def test_untimed_event_waiters_are_fd_free():
    out = {}

    def main():
        ev = th.Event()
        woke = bytearray(300)
        before = _count_fds()

        def waiter(i):
            ev.wait()
            woke[i] = 1

        for i in range(300):
            runloom.go(waiter, i)
        runloom.sleep(0.15)              # all 300 parked in memory
        parked = _count_fds()
        ev.set()
        runloom.sleep(0.15)
        out["woke"] = sum(woke)
        out["delta"] = parked - before

    runloom.run(8, main)
    assert out["woke"] == 300                      # all woke
    # 300 in-memory waiters add at most a couple of fds (the shared anchor),
    # NOT ~600 (2 per waiter).  Generous bound to stay robust.
    assert out["delta"] <= 8, out["delta"]


def test_event_foreign_thread_setter_wakes_inmem_waiter():
    out = {}

    def main():
        ok = 0
        for _ in range(15):
            ev = th.Event()
            done = bytearray(1)

            def waiter():
                ev.wait()
                done[0] = 1

            runloom.go(waiter)
            runloom.sleep(0.02)                    # waiter parked in memory
            t = threading.Thread(target=ev.set)    # REAL OS thread sets
            t.start()
            t.join()
            runloom.sleep(0.04)
            ok += done[0]
        out["ok"] = ok

    runloom.run(8, main)
    assert out["ok"] == 15


def test_event_wake_before_park():
    out = {}

    def main():
        ok = 0
        for _ in range(80):
            ev = th.Event()
            done = bytearray(1)

            def waiter():
                ev.wait()
                done[0] = 1

            runloom.go(waiter)
            ev.set()                                # races the park commit
            for _ in range(200):
                if done[0]:
                    break
                runloom.sleep(0.001)
            ok += done[0]
        out["ok"] = ok

    runloom.run(8, main)
    assert out["ok"] == 80


def test_timed_and_condition_and_semaphore_still_work():
    out = {}

    def main():
        out["timeout"] = th.Event().wait(0.05)     # times out -> False

        ev = th.Event()
        got = []
        runloom.go(lambda: got.append(ev.wait(2.0)))
        runloom.sleep(0.02)
        ev.set()
        runloom.sleep(0.05)
        out["timed_set"] = got                     # [True]

        cond = th.Condition()
        cwoke = bytearray(1)

        def cw():
            with cond:
                cond.wait()
            cwoke[0] = 1

        runloom.go(cw)
        runloom.sleep(0.03)
        with cond:
            cond.notify_all()
        runloom.sleep(0.05)
        out["cond"] = cwoke[0]

        sem = th.Semaphore(0)
        swoke = bytearray(1)
        runloom.go(lambda: (sem.acquire(), swoke.__setitem__(0, 1)))
        runloom.sleep(0.03)
        sem.release()
        runloom.sleep(0.05)
        out["sem"] = swoke[0]

    runloom.run(8, main)
    assert out["timeout"] is False
    assert out["timed_set"] == [True]
    assert out["cond"] == 1
    assert out["sem"] == 1
