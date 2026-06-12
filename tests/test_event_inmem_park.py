"""Event/Condition/Semaphore waits park IN MEMORY (0 per-waiter fds), via
runloom_c.park()[/park(timeout=...)] + g.wake(), instead of one OS pipe/socketpair
per waiter.  A million coroutines on event.wait() previously cost ~2M fds; now ~1
(the shared run-alive anchor).  TIMED goroutine waits are ALSO fd-free: they ride
the scheduler's per-hub timer heap (runloom_park_generic_timed -- the same
parked_safe CAS, exactly-once vs a real wake; FV: verify/spin/park_generic_timed).
Only a FOREIGN-thread wait keeps an fd (park can't serve a non-goroutine).

Guards the fd reduction AND the load-bearing correctness: wake-before-park, a
foreign-thread setter, the timed-park exactly-once race, and the timed paths.
"""
import os
import threading
import time

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


def test_timed_event_waiters_are_fd_free():
    out = {}

    def main():
        evs = [th.Event() for _ in range(150)]
        done = bytearray(150)
        before = _count_fds()

        def waiter(i):
            done[i] = 1 if evs[i].wait(0.12) else 0   # 0 == timed out (expected)

        for i in range(150):
            runloom.go(waiter, i)
        runloom.sleep(0.05)
        out["delta"] = _count_fds() - before          # while all parked, timed
        runloom.sleep(0.15)                            # let them time out
        out["all_timed_out"] = sum(done) == 0

    runloom.run(8, main)
    assert out["all_timed_out"]                        # every wait(0.12) -> False
    assert out["delta"] <= 8, out["delta"]             # ~the shared anchor, not ~300


def test_timed_wait_woken_before_deadline():
    out = {}

    def main():
        ev = th.Event()
        got = []
        runloom.go(lambda: got.append(ev.wait(5.0)))
        runloom.sleep(0.03)
        ev.set()
        runloom.sleep(0.05)
        out["got"] = got                               # [True] -- woken, not timed out

    runloom.run(8, main)
    assert out["got"] == [True]


def test_timed_wake_vs_timeout_exactly_once():
    # Hammer the race: a timed wait whose deadline straddles a set().  The waiter
    # must resume EXACTLY ONCE with a self-consistent result.
    out = {}

    def main():
        rounds = 1500
        resumes = bytearray(rounds)
        bad = [0]

        def one(i):
            ev = th.Event()
            dl = 0.004
            box = {}

            def waiter():
                t0 = time.monotonic()
                r = ev.wait(dl)
                box["rd"] = (r, time.monotonic() - t0)   # single atomic write (no
                resumes[i] += 1                          # torn read of r vs dt)

            runloom.go(waiter)
            runloom.sleep(dl * (0.5 + (i % 7) / 7.0))
            ev.set()
            runloom.sleep(0.003)
            rd = box.get("rd")
            if rd is None:
                bad[0] += 1                            # never resumed
            else:
                r, dt = rd
                if r is False and dt < dl * 0.7:
                    bad[0] += 1                        # premature timeout

        for i in range(rounds):
            one(i)
        out["bad"] = bad[0]
        out["not_once"] = sum(1 for x in resumes if x != 1)

    runloom.run(8, main)
    assert out["bad"] == 0, out
    assert out["not_once"] == 0, out


def test_condition_timeout_does_not_steal_a_later_notify():
    # A timed Condition.wait that TIMES OUT must remove its parker, so it does not
    # steal a notify meant for a later, live waiter.  (The fd path masked this via
    # fd pooling; in-memory parkers don't share, so the removal is load-bearing.)
    out = {}

    def main():
        cond = th.Condition()
        # first waiter times out
        with cond:
            r0 = cond.wait(0.05)
        out["timed_out"] = (r0 is False)
        # now a live waiter + a single notify
        woke = []

        def w():
            with cond:
                woke.append(cond.wait(2.0))

        runloom.go(w)
        runloom.sleep(0.03)
        with cond:
            cond.notify()
        runloom.sleep(0.05)
        out["woke"] = woke                             # [True] -- notify reached it

    runloom.run(8, main)
    assert out["timed_out"]
    assert out["woke"] == [True], out["woke"]
