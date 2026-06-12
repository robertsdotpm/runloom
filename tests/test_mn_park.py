"""runloom_c.park() -- the generic M:N in-memory park (no fd) + G.wake().

park_self (= sched_park_safe) is SINGLE-THREAD only: on an M:N hub it returns
immediately (sched->current is NULL) so a loop around it busy-spins (the #4
WaitGroup/Future bug).  park() routes by hub presence (park_current+coro_yield on
a hub, park_safe single-thread), records g->park_hub so G.wake() re-queues via the
right path (mn_wake_g vs wake_safe), and -- with foreign_wakeable=True -- arms a
shared run-alive anchor so a foreign-OS-thread waker cannot race a single-thread
run()'s exit.  These cover all four (hub x waker-kind) combinations + the
wake-before-park race + a multi-parker stress.
"""
import threading   # REAL OS thread (no monkey.patch here)
import time

import runloom
import runloom_c


# ---- park() must BLOCK, not busy-spin (the #4 bug) ------------------------

def _park_returns(hubs):
    """How many times park() returns in 0.1s with NO wake.  1 == blocks; a huge
    number == the park_self busy-spin."""
    returns = [0]
    hb = {}
    stop = [False]

    def waiter():
        hb["g"] = runloom_c.current_g()
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.1 and not stop[0]:
            runloom_c.park()
            returns[0] += 1

    def main():
        runloom.go(waiter)
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.13:
            runloom.sleep(0.01)
        stop[0] = True
        if "g" in hb:
            hb["g"].wake()
    runloom.run(hubs, main)
    return returns[0]


def test_park_blocks_under_mn():
    # park_self spun ~180000x here; park() must block (a couple at most).
    assert _park_returns(8) < 10


def test_park_blocks_single_thread():
    assert _park_returns(1) < 10


# ---- wake delivers across (hub x waker-kind) -----------------------------

def _wake_case(hubs, foreign, fw):
    box = []
    hb = {}

    def waiter():
        hb["g"] = runloom_c.current_g()
        runloom_c.park(foreign_wakeable=fw)
        box.append("woke")

    def main():
        runloom.go(waiter)
        runloom.sleep(0.08)               # waiter parks
        if foreign:
            t = threading.Thread(target=lambda: hb["g"].wake())
            t.start(); t.join()
        else:
            hb["g"].wake()
        runloom.sleep(0.12)
    runloom.run(hubs, main)
    return box == ["woke"]


def test_mn_goroutine_wake():
    assert _wake_case(8, foreign=False, fw=False)


def test_single_goroutine_wake():
    assert _wake_case(1, foreign=False, fw=False)


def test_single_foreign_wake():
    # The run-alive anchor case: a foreign thread wakes a single-thread parker;
    # run() must stay alive for it (foreign_wakeable=True).
    assert _wake_case(1, foreign=True, fw=True)


def test_mn_foreign_wake_stable():
    # Foreign-OS-thread wake of an M:N hub parker, repeated (the idle_cond signal
    # is best-effort, so confirm it is not flaky).
    for _ in range(8):
        assert _wake_case(8, foreign=True, fw=True)


# ---- wake-before-park race ------------------------------------------------

def test_wake_before_park_race_stress():
    """The legitimate wake-before-park race: a setter wakes the waiter while it is
    still RUNNING (just before park() commits), which is exactly the window the
    Future/WaitGroup primitives hit (record handle under guard, release, then
    park; setter wakes after).  Many rounds with the wake fired the instant the
    handle is visible -- a lost wake-before-park would hang."""
    def main():
        ok = 0
        for _ in range(80):
            hb = {}
            done = [False]

            def waiter():
                hb["g"] = runloom_c.current_g()
                runloom_c.park()
                done[0] = True

            runloom.go(waiter)
            spins = 0
            while "g" not in hb and spins < 1000000:   # busy-wait, NO yield
                spins += 1
            hb["g"].wake()                              # often lands before park()
            for _ in range(200):
                if done[0]:
                    break
                runloom.sleep(0.001)
            if done[0]:
                ok += 1
        main.ok = ok
    runloom.run(8, main)
    assert main.ok == 80


# ---- many parkers, one runtime -------------------------------------------

def test_many_parkers_all_woken():
    def main():
        n = 200
        handles = [None] * n
        woke = bytearray(n)

        def waiter(i):
            handles[i] = runloom_c.current_g()
            runloom_c.park()
            woke[i] = 1

        for i in range(n):
            runloom.go(waiter, i)
        runloom.sleep(0.15)                # all parked
        for h in handles:
            h.wake()
        runloom.sleep(0.2)
        main.total = sum(woke)
    runloom.run(8, main)
    assert main.total == 200
