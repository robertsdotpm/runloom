"""Event/Condition/Semaphore waits park IN MEMORY (0 per-waiter fds), via
runloom_c.park()[/park(timeout=...)] + g.wake(), instead of one OS pipe/socketpair
per waiter.  A million coroutines on event.wait() previously cost ~2M fds; now ~1
(the shared run-alive anchor).  TIMED fiber waits are ALSO fd-free: they ride
the scheduler's per-hub timer heap (runloom_park_generic_timed -- the same
parked_safe CAS, exactly-once vs a real wake; FV: verify/spin/park_generic_timed).
Only a FOREIGN-thread wait keeps an fd (park can't serve a non-fiber).

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
            runloom.fiber(waiter, i)
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

            runloom.fiber(waiter)
            # Event is sticky: a set() that beats the park is NOT lost (the
            # wake_pending handshake, proven by test_event_wake_before_park),
            # so no pre-set sleep is needed to "park first".
            t = threading.Thread(target=ev.set)    # REAL OS thread sets
            t.start()
            t.join()
            # Deterministically wait for the woken waiter to run and write its
            # byte, instead of a fixed sleep that a loaded scheduler can outrun.
            j = 0
            while not done[0] and j < 1000000:
                runloom.sleep(0)
                j += 1
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

            runloom.fiber(waiter)
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
        # Sticky Event: set() before the timed park is not lost.  Set, then poll
        # for the result instead of a fixed sleep a loaded scheduler can outrun.
        runloom.fiber(lambda: got.append(ev.wait(2.0)))
        ev.set()
        i = 0
        while not got and i < 1000000:
            runloom.sleep(0)
            i += 1
        out["timed_set"] = got                     # [True]

        cond = th.Condition()
        cwoke = bytearray(1)
        cwaiting = bytearray(1)

        def cw():
            with cond:
                cwaiting[0] = 1        # reached the cond block, still holding it
                cond.wait()            # atomically releases cond as it parks
            cwoke[0] = 1

        runloom.fiber(cw)
        # Condition has NO sticky pending state (unlike Event/Semaphore): a
        # notify that beats the park is LOST and the waiter hangs forever.
        # Deterministic park-before-notify handshake: wait for the waiter to be
        # inside the cond block, then take the cond lock -- which cannot be
        # acquired until cond.wait() has released it, i.e. the waiter is parked.
        i = 0
        while not cwaiting[0] and i < 1000000:
            runloom.sleep(0)
            i += 1
        with cond:
            cond.notify_all()
        i = 0
        while not cwoke[0] and i < 1000000:
            runloom.sleep(0)
            i += 1
        out["cond"] = cwoke[0]

        sem = th.Semaphore(0)
        swoke = bytearray(1)
        # Sticky counter: release() before acquire() parks is not lost (the
        # token is banked).  Release, then poll for the result.
        runloom.fiber(lambda: (sem.acquire(), swoke.__setitem__(0, 1)))
        sem.release()
        i = 0
        while not swoke[0] and i < 1000000:
            runloom.sleep(0)
            i += 1
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
            runloom.fiber(waiter, i)
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
        # Sticky Event + a 5s deadline: set() always wins the wake (never lost,
        # never a timeout), even if it beats the park.  Poll for the result.
        runloom.fiber(lambda: got.append(ev.wait(5.0)))
        ev.set()
        i = 0
        while not got and i < 1000000:
            runloom.sleep(0)
            i += 1
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

            runloom.fiber(waiter)
            runloom.sleep(dl * (0.5 + (i % 7) / 7.0))
            ev.set()
            # Wait for the waiter to actually resume before classifying.  A woken
            # fiber under 8-hub load may not be SCHEDULED to write box for many
            # ms after its wake commits -- a fixed tiny wait races that and reads a
            # not-yet-written box as a false "never resumed" (the overnight flake:
            # bad=1 with not_once=0, i.e. it DID resume exactly once, just later).
            # Poll to a generous ceiling; only a genuine no-resume past it is a bug.
            deadline = time.monotonic() + 0.5
            while box.get("rd") is None and time.monotonic() < deadline:
                runloom.sleep(0.002)
            rd = box.get("rd")
            if rd is None:
                bad[0] += 1                            # never resumed (real bug)
            else:
                r, dt = rd
                if r is False and dt < dl * 0.7:
                    bad[0] += 1                        # premature timeout (real bug)

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
        waiting = bytearray(1)

        def w():
            with cond:
                waiting[0] = 1                 # inside the cond block, holds it
                woke.append(cond.wait(2.0))    # releases cond as it parks

        runloom.fiber(w)
        # Condition notify has no sticky pending state: a notify that beats the
        # park is LOST.  Deterministic park-before-notify handshake -- wait for
        # the waiter to enter the cond block, then take the cond lock, which it
        # cannot hand over until cond.wait() released it (waiter now parked).
        i = 0
        while not waiting[0] and i < 1000000:
            runloom.sleep(0)
            i += 1
        with cond:
            cond.notify()
        i = 0
        while not woke and i < 1000000:
            runloom.sleep(0)
            i += 1
        out["woke"] = woke                             # [True] -- notify reached it

    runloom.run(8, main)
    assert out["timed_out"]
    assert out["woke"] == [True], out["woke"]
