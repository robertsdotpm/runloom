"""Phase 3 runloom.sync primitives: RWMutex, weighted Semaphore, Once/once_value/
once_func, singleflight Group, Watch, JoinSet.

All ride the GenMC-verified park()/g.wake() handshake + the runloom_c.Mutex guard +
the fiber-resolution contract.  These pin the contract AND the failure mode
that matters for a park/wake primitive -- a lost wakeup / wrong-mutual-exclusion
under M:N -- which would hang (caught by the timeout) or miscount (caught by an
assert).
"""
import time

import pytest

import runloom
import runloom_c
from runloom import sync


def _drive(fn, hubs=8):
    box = [None, None]

    def runner():
        try:
            box[0] = fn()
        except BaseException as e:  # noqa: BLE001
            box[1] = e

    runloom.run(hubs, runner)
    if box[1] is not None:
        raise box[1]
    return box[0]


# ---- RWMutex -------------------------------------------------------------

def test_rwmutex_mutual_exclusion():
    def body():
        rw = sync.RWMutex()
        ctr = {"n": 0}
        n, k = 8, 300
        wg = sync.WaitGroup()
        wg.add(n)

        def writer():
            for _ in range(k):
                with rw:
                    ctr["n"] += 1      # exact total only if writers are exclusive
            wg.done()

        for _ in range(n):
            runloom.go(writer)
        wg.wait()
        return ctr["n"]
    assert _drive(body) == 8 * 300


def test_rwmutex_readers_concurrent_writer_exclusive():
    def body():
        rw = sync.RWMutex()
        gd = runloom_c.Mutex()          # guard shared counters (GIL off -> += races)
        state = {"writing": False, "max_readers": 0, "cur_readers": 0,
                 "overlap_violation": False}
        wg = sync.WaitGroup()

        def reader():
            rw.rlock()
            gd.lock()
            state["cur_readers"] += 1
            if state["writing"]:
                state["overlap_violation"] = True
            if state["cur_readers"] > state["max_readers"]:
                state["max_readers"] = state["cur_readers"]
            gd.unlock()
            runloom.sleep(0.005)
            gd.lock()
            state["cur_readers"] -= 1
            gd.unlock()
            rw.runlock()
            wg.done()

        def writer():
            with rw:
                gd.lock()
                state["writing"] = True
                if state["cur_readers"] > 0:
                    state["overlap_violation"] = True
                gd.unlock()
                runloom.sleep(0.003)
                gd.lock()
                state["writing"] = False
                gd.unlock()
            wg.done()

        wg.add(12)
        for _ in range(10):
            runloom.go(reader)
        for _ in range(2):
            runloom.go(writer)
        wg.wait()
        return state
    s = _drive(body)
    assert not s["overlap_violation"]      # never reader+writer or writer+writer
    assert s["max_readers"] >= 2           # readers actually ran concurrently


def test_rwmutex_runlock_not_held_raises():
    def body():
        rw = sync.RWMutex()
        with pytest.raises(RuntimeError):
            rw.runlock()
        with pytest.raises(RuntimeError):
            rw.unlock()
        return True
    assert _drive(body)


# ---- weighted Semaphore --------------------------------------------------

def test_semaphore_weighted_limits_concurrency():
    def body():
        sem = sync.Semaphore(3)
        gd = runloom_c.Mutex()          # guard the shared counter (GIL off -> += races)
        state = {"cur": 0, "max": 0}
        wg = sync.WaitGroup()
        wg.add(20)

        def worker():
            sem.acquire(1)
            gd.lock()
            state["cur"] += 1
            if state["cur"] > state["max"]:
                state["max"] = state["cur"]
            gd.unlock()
            runloom.sleep(0.003)
            gd.lock()
            state["cur"] -= 1
            gd.unlock()
            sem.release(1)
            wg.done()

        for _ in range(20):
            runloom.go(worker)
        wg.wait()
        return state["max"]
    assert _drive(body) == 3            # never more than 3 in the section


def test_semaphore_weighted_n_and_fifo_no_starvation():
    def body():
        sem = sync.Semaphore(10)
        order = []
        wg = sync.WaitGroup()
        # one big (n=10) waiter, then a stream of small (n=1) ones behind it.
        sem.acquire(10)                  # hold all permits
        wg.add(6)

        def big():
            sem.acquire(10)
            order.append("big")
            sem.release(10)
            wg.done()

        def small(i):
            sem.acquire(1)
            order.append("small%d" % i)
            sem.release(1)
            wg.done()

        runloom.go(big)
        # Deterministic handshake: spin until big has actually APPENDED itself to
        # the FIFO waiter queue before spawning the small stream.  A sleep here is
        # load-dependent -- if big hasn't run sem.acquire(10) yet when the smalls
        # queue, a small lands at the FIFO front and the order[0]=="big" assertion
        # inverts.  Polling the real queue length removes that race; the cap only
        # bounds a hang (the happy path queues in ~100 yields).
        _spin = 0
        while len(sem._waiters) < 1 and _spin < 200000:
            runloom_c.sched_yield(); _spin += 1
        for i in range(5):
            runloom.go(small, i)
        # And wait until all 6 (big + 5 small) have queued before releasing, so the
        # release grants strictly in FIFO order from a fully-populated queue.
        _spin = 0
        while len(sem._waiters) < 6 and _spin < 200000:
            runloom_c.sched_yield(); _spin += 1
        sem.release(10)                  # free everything -> big (FIFO front) first
        wg.wait()
        return order
    order = _drive(body)
    assert order[0] == "big", order      # big not starved by the small stream


def test_semaphore_acquire_timeout():
    def body():
        sem = sync.Semaphore(1)
        sem.acquire(1)
        t0 = time.monotonic()
        got = sem.acquire(1, timeout=0.08)
        dt = time.monotonic() - t0
        return got, dt
    got, dt = _drive(body)
    assert got is False and 0.06 < dt < 0.5


def test_semaphore_try_acquire():
    def body():
        sem = sync.Semaphore(1)
        return sem.try_acquire(1), sem.try_acquire(1)
    assert _drive(body) == (True, False)


def test_semaphore_acquire_over_limit_raises():
    def body():
        sem = sync.Semaphore(2)
        with pytest.raises(ValueError):
            sem.acquire(3)
        return True
    assert _drive(body)


# ---- Once / once_value / once_func ---------------------------------------

def test_once_runs_exactly_once_concurrent():
    def body():
        once = sync.Once()
        runs = {"n": 0}
        wg = sync.WaitGroup()
        wg.add(30)

        def fn():
            runs["n"] += 1
            runloom.sleep(0.005)

        def caller():
            once.do(fn)
            wg.done()

        for _ in range(30):
            runloom.go(caller)
        wg.wait()
        return runs["n"]
    assert _drive(body) == 1


def test_once_executor_sees_exception_others_dont():
    def body():
        once = sync.Once()
        saw = bytearray(10)               # per-caller slot (race-free): 1 == saw exc

        def boom():
            raise ValueError("boom")

        def caller(i):
            try:
                once.do(boom)
            except ValueError:
                saw[i] = 1
        wg = sync.WaitGroup()
        wg.add(10)

        def run(i):
            caller(i)
            wg.done()
        for i in range(10):
            runloom.go(run, i)
            runloom.sleep(0.002)
        wg.wait()
        return sum(saw)
    # exactly ONE caller (the executor) saw the exception (Go semantics)
    assert _drive(body) == 1


def test_once_value_caches_result_and_exception():
    def body():
        calls = {"n": 0}

        def make():
            calls["n"] += 1
            return 42
        ov = sync.once_value(make)
        a, b = ov(), ov()

        def boom():
            raise KeyError("nope")
        ob = sync.once_value(boom)
        raised = 0
        for _ in range(3):
            try:
                ob()
            except KeyError:
                raised += 1
        return a, b, calls["n"], raised
    a, b, n, raised = _drive(body)
    assert a == 42 and b == 42 and n == 1
    assert raised == 3                     # cached exception re-raised to ALL callers


# ---- singleflight --------------------------------------------------------

def test_singleflight_dedupes_and_shares():
    def body():
        g = sync.Group()
        calls = {"n": 0}
        results = []
        shared_flags = []
        wg = sync.WaitGroup()
        wg.add(20)

        def fn():
            calls["n"] += 1
            runloom.sleep(0.02)
            return "val-%d" % calls["n"]

        def caller():
            v, shared = g.do("k", fn)
            results.append(v)
            shared_flags.append(shared)
            wg.done()

        for _ in range(20):
            runloom.go(caller)
        wg.wait()
        return calls["n"], set(results), sum(shared_flags)
    n, vals, n_shared = _drive(body)
    assert n == 1                          # fn ran ONCE
    assert vals == {"val-1"}               # all callers got the same value
    assert n_shared == 19                  # 19 waiters shared; 1 executor did not


def test_singleflight_exception_shared():
    def body():
        g = sync.Group()
        caught = bytearray(8)              # per-caller slot (race-free)
        wg = sync.WaitGroup()
        wg.add(8)

        def boom():
            runloom.sleep(0.01)
            raise ValueError("x")

        def caller(i):
            try:
                g.do("k", boom)
            except ValueError:
                caught[i] = 1
            wg.done()

        for i in range(8):
            runloom.go(caller, i)
        wg.wait()
        return sum(caught)
    assert _drive(body) == 8               # the exception reached every caller


def test_singleflight_forget_reruns():
    def body():
        g = sync.Group()
        calls = {"n": 0}

        def fn():
            calls["n"] += 1
            return calls["n"]
        v1, _ = g.do("k", fn)
        v2, _ = g.do("k", fn)              # entry already deleted after v1 -> reruns
        g.forget("k")
        v3, _ = g.do("k", fn)
        return v1, v2, v3, calls["n"]
    v1, v2, v3, n = _drive(body)
    assert (v1, v2, v3) == (1, 2, 3) and n == 3


# ---- Watch ---------------------------------------------------------------

def test_watch_broadcast_and_version():
    def body():
        w = sync.Watch(0)
        got = []
        wg = sync.WaitGroup()
        wg.add(5)

        def observer():
            v, ver = w.wait_changed(0)     # block until version > 0
            got.append((v, ver))
            wg.done()

        for _ in range(5):
            runloom.go(observer)
        runloom.sleep(0.03)                # all 5 parked on wait_changed
        w.set(99)
        wg.wait()
        return got, w.version()
    got, ver = _drive(body)
    assert ver == 1
    assert got == [(99, 1)] * 5            # all observers saw the one change


def test_watch_no_missed_change_and_timeout():
    def body():
        w = sync.Watch("a")
        w.set("b")                         # version 1 before anyone waits
        # a waiter that has seen version 0 must immediately see the change
        r = w.wait_changed(0, timeout=0.05)
        # a waiter caught up to the latest version times out
        seen_ver = w.version()
        t0 = time.monotonic()
        r2 = w.wait_changed(seen_ver, timeout=0.06)
        dt = time.monotonic() - t0
        return r, r2, dt
    r, r2, dt = _drive(body)
    assert r == ("b", 1)
    assert r2 is None and 0.04 < dt < 0.4  # nothing changed -> timeout


# ---- JoinSet -------------------------------------------------------------

def test_joinset_order_and_results():
    def body():
        js = sync.JoinSet()
        for i in range(5):
            js.spawn(lambda i=i: i * 10)
        return js.join_all()
    assert _drive(body) == [0, 10, 20, 30, 40]


def test_joinset_first_exception_by_spawn_order():
    def body():
        js = sync.JoinSet()
        js.spawn(lambda: 1)

        def boom():
            raise KeyError("first")
        js.spawn(boom)
        js.spawn(lambda: 3)
        with pytest.raises(KeyError):
            js.join_all()
        return True
    assert _drive(body)


def test_joinset_context_manager():
    def body():
        out = []
        with sync.JoinSet() as n:
            for i in range(4):
                n.spawn(lambda i=i: out.append(i))
        # block-exit joined all spawned tasks
        return sorted(out)
    assert _drive(body) == [0, 1, 2, 3]


def test_joinset_context_manager_propagates_task_error():
    def body():
        def boom():
            raise ValueError("task")
        with pytest.raises(ValueError):
            with sync.JoinSet() as n:
                n.spawn(lambda: 1)
                n.spawn(boom)
        return True
    assert _drive(body)
