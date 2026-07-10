"""Ports of Go's canonical runtime/sync test invariants to real runloom programs.

Unlike tests/test_go_channel_oracle.py (which diffs runloom against a COMPILED Go
binary), each test here encodes the behavioral invariant of a specific Go test
DIRECTLY, so it runs with no Go toolchain.  The Go source mirrored by each case is
named in that case's docstring; sources live at:

  * /usr/lib/go-1.22/src/runtime/chan_test.go   (TestChan, TestSelfSelect,
    TestSelectStress, TestNonblockSelectRace/Race2, TestMultiConsumer)
  * /usr/lib/go-1.22/src/sync/{mutex,rwmutex,waitgroup,once,cond}_test.go
  * golang.org/x/sync/semaphore/semaphore_test.go (TestWeighted*, TestLarge...)

This file EXTENDS (does not duplicate) the Go-behaviour coverage already in
tests/test_adv_chan.py, tests/test_adv_sync.py, tests/test_sync_primitives.py and
tests/test_go_channel_oracle.py -- e.g. close-wakes-receivers, singleflight,
JoinSet, Watch, and the differential-vs-Go scenarios are NOT re-ported here.

Style: %-formatting, no f-strings, no leading-underscore names introduced here.
Deterministic ordering checks run single-thread under runloom.run(1, ...); every
contention / no-overlap / no-loss check runs REAL M:N under runloom.run(H>=2, ...)
(GIL-disabled build only) with a hang_guard backstop and race-free per-goroutine
or guarded counters.
"""
import sys

import pytest

import runloom
import runloom_c as rc
from runloom.sync import (Lock, RWMutex, Semaphore, WaitGroup, Condition,
                          Once, once_func)
from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
mn = pytest.mark.skipif(
    not FT, reason="real M:N parallelism needs the GIL-disabled build")


# --------------------------------------------------------------------------
# tiny harness: run a body as the run(H) main; propagate its return / exception
# --------------------------------------------------------------------------
def run_body(body, hubs=1):
    box = {}

    def main():
        try:
            box["r"] = body()
        except BaseException as exc:      # noqa: BLE001  (re-raised below)
            box["e"] = exc

    runloom.run(hubs, main)
    if "e" in box:
        raise box["e"]
    return box.get("r")


def yield_times(k):
    for _ in range(k):
        runloom.yield_now()


def spin_until(pred, limit=500000):
    """Cooperatively yield until pred() is true (bounded).  Fiber-only."""
    n = 0
    while n < limit:
        if pred():
            return True
        runloom.yield_now()
        n += 1
    return pred()


# ==========================================================================
# Chan / select  (runtime/chan_test.go)
# ==========================================================================
def test_chan_recv_empty_blocks_and_nonblocking_recv():
    """TestChan: a receive from an empty chan blocks, while try_recv / a
    select-with-default over the same empty chan does NOT block."""
    def body():
        for cap in (0, 1, 2):
            ch = rc.Chan(cap)
            got = []
            for _ in range(2):
                runloom.fiber(lambda c=ch: got.append(c.recv()))
            yield_times(50)                      # let both receivers park
            assert got == [], (cap, got)         # still blocked (no sender)
            assert ch.try_recv() is None                       # nonblocking recv
            assert rc.select([("recv", ch)], default=True) == -1
            ch.send(0)
            ch.send(0)                           # unblock both receivers
            assert spin_until(lambda: len(got) == 2)
            assert all(ok for val, ok in got), (cap, got)
        return "ok"
    with hang_guard(20, "chan recv-empty blocks"):
        assert run_body(body) == "ok"


def test_chan_send_full_blocks_and_nonblocking_send():
    """TestChan: a send to a full chan blocks, while try_send / a
    select-send-with-default over the same full chan does NOT block."""
    def body():
        for cap in (0, 1, 2):
            ch = rc.Chan(cap)
            for i in range(cap):
                assert ch.try_send(i) is True    # fill the buffer
            sent = [False]

            def sender(c=ch):
                c.send(999)
                sent[0] = True
            runloom.fiber(sender)
            yield_times(50)                      # let the sender park
            assert sent[0] is False, cap         # send blocked (chan full)
            assert ch.try_send(-1) is False                    # nonblocking send
            assert rc.select([("send", ch, -1)], default=True) == -1
            ch.recv()                            # free a slot -> sender proceeds
            assert spin_until(lambda: sent[0] is True), cap
        return "ok"
    with hang_guard(20, "chan send-full blocks"):
        assert run_body(body) == "ok"


def test_chan_recv_closed_drains_fifo_then_sentinel():
    """TestChan: after close, buffered values drain in FIFO order == i, then a
    recv yields the zero value with ok==False repeatedly (never blocks)."""
    def body():
        for cap in (0, 1, 3):
            ch = rc.Chan(cap)
            for i in range(cap):
                ch.try_send(i)
            ch.close()
            for i in range(cap):
                assert ch.recv() == (i, True), (cap, i)
            for _ in range(4):                   # zero+notok, repeatedly
                assert ch.recv() == (None, False), cap
        return "ok"
    assert run_body(body) == "ok"


def test_chan_hundred_ints_fifo():
    """TestChan: a producer sends 100 ints; the consumer receives them
    uncorrupted in FIFO order (recv2 comma-ok form), across two batches to
    exercise buffer reuse."""
    def body():
        for cap in (0, 1, 10):
            ch = rc.Chan(cap)

            def producer(c=ch):
                for i in range(100):
                    c.send(i)
            for batch in range(2):
                runloom.fiber(producer)
                for i in range(100):
                    v, ok = ch.recv()
                    assert ok and v == i, (cap, i, v, ok)
        return "ok"
    with hang_guard(20, "chan 100 ints fifo"):
        assert run_body(body) == "ok"


@mn
def test_chan_p_producers_no_loss_no_dup():
    """TestChan: P producers each send 0..L-1, C consumers drain; every value in
    0..L-1 is delivered exactly P times (no loss, no duplication) across the
    cross-hub handoff -- a multiset invariant a bare count would miss."""
    P, L, C = 4, 400, 4
    ch = rc.Chan(64)
    maps = [dict() for _ in range(C)]

    def body():
        wgp = WaitGroup(); wgp.add(P)
        wgc = WaitGroup(); wgc.add(C)

        def producer():
            try:
                for i in range(L):
                    ch.send(i)
            finally:
                wgp.done()

        def consumer(cid):
            try:
                while True:
                    v, ok = ch.recv()
                    if not ok:
                        break
                    maps[cid][v] = maps[cid].get(v, 0) + 1
            finally:
                wgc.done()

        for cid in range(C):
            runloom.fiber(lambda cid=cid: consumer(cid))
        for _ in range(P):
            runloom.fiber(producer)
        wgp.wait()
        ch.close()
        wgc.wait()
        return "ok"

    with hang_guard(40, "chan P-producers no loss/dup"):
        assert run_body(body, hubs=4) == "ok"
    counts = {}
    total = 0
    for m in maps:
        for k, c in m.items():
            counts[k] = counts.get(k, 0) + c
            total += c
    assert total == P * L, "lost/dup: got %d want %d" % (total, P * L)
    assert set(counts) == set(range(L))
    bad = [(k, v) for k, v in counts.items() if v != P]
    assert not bad, "values not delivered exactly P=%d times: %r" % (P, bad[:8])


@mn
def test_self_select_no_self_receive_no_deadlock():
    """TestSelfSelect: two goroutines each send AND recv the same chan inside one
    select; on an unbuffered chan a goroutine must never receive its own send,
    and the construct must not deadlock."""
    N = 400

    def body():
        for cap in (0, 10):
            ch = rc.Chan(cap)
            self_recv = [False]
            wg = WaitGroup(); wg.add(2)

            def worker(p):
                try:
                    for i in range(N):
                        if p == 0 or i % 2 == 0:
                            cases = [("send", ch, p), ("recv", ch)]
                            recv_idx = 1
                        else:
                            cases = [("recv", ch), ("send", ch, p)]
                            recv_idx = 0
                        idx, res = rc.select(cases)
                        if idx == recv_idx:
                            v, okflag = res
                            if cap == 0 and v == p:
                                self_recv[0] = True
                finally:
                    wg.done()

            for p in range(2):
                runloom.fiber(lambda p=p: worker(p))
            wg.wait()
            assert self_recv[0] is False, "cap=%d: goroutine self-received" % cap
        return "ok"

    with hang_guard(40, "self-select"):
        assert run_body(body, hubs=2) == "ok"


@mn
def test_select_stress_terminates_with_nil_disable():
    """TestSelectStress: 4 chans (mixed cap) with a sender+receiver goroutine
    each, plus one goroutine sending 4*N via a single select and one receiving
    4*N via a single select -- exercising the 'disable a case' idiom (runloom:
    omit the case) as each per-chan count saturates.  Must not deadlock."""
    N = 400
    caps = (0, 0, 2, 3)

    def body():
        chans = [rc.Chan(c) for c in caps]
        wg = WaitGroup(); wg.add(10)

        def plain_sender(k):
            try:
                for _ in range(N):
                    chans[k].send(0)
            finally:
                wg.done()

        def plain_receiver(k):
            try:
                for _ in range(N):
                    chans[k].recv()
            finally:
                wg.done()

        def select_sender():
            try:
                counts = [0, 0, 0, 0]
                for _ in range(4 * N):
                    cases = []
                    kmap = []
                    for k in range(4):
                        if counts[k] < N:
                            cases.append(("send", chans[k], 0))
                            kmap.append(k)
                    idx, payload = rc.select(cases)
                    counts[kmap[idx]] += 1
            finally:
                wg.done()

        def select_receiver():
            try:
                counts = [0, 0, 0, 0]
                for _ in range(4 * N):
                    cases = []
                    kmap = []
                    for k in range(4):
                        if counts[k] < N:
                            cases.append(("recv", chans[k]))
                            kmap.append(k)
                    idx, payload = rc.select(cases)
                    counts[kmap[idx]] += 1
            finally:
                wg.done()

        for k in range(4):
            runloom.fiber(lambda k=k: plain_sender(k))
            runloom.fiber(lambda k=k: plain_receiver(k))
        runloom.fiber(select_sender)
        runloom.fiber(select_receiver)
        wg.wait()
        return "ok"

    with hang_guard(60, "select stress"):
        assert run_body(body, hubs=4) == "ok"


def test_nonblock_select_always_ready_never_default():
    """TestNonblockSelectRace / Race2: a select over an always-ready case never
    falls to default, and a closed chan is permanently receive-ready (yielding
    the zero value with ok==False)."""
    def body():
        for _ in range(200):
            c1 = rc.Chan(1)
            c2 = rc.Chan(1)
            c1.try_send(1)                       # c1 ready
            r = rc.select([("recv", c1), ("recv", c2)], default=True)
            assert r != -1, "ready select fell through to default"
            # Race2: a closed chan is permanently recv-ready.
            c3 = rc.Chan(0)
            c3.close()
            r2 = rc.select([("recv", c3)], default=True)
            assert r2 != -1, "closed chan not recv-ready"
            idx, payload = r2
            assert idx == 0 and payload == (None, False)
        return "ok"
    assert run_body(body) == "ok"


@mn
def test_multi_consumer_preserves_count_and_checksum():
    """TestMultiConsumer: nwork workers range over a work chan (occasionally
    yielding to perturb FIFO), a feeder posts niter values then closes; every
    value reaches the result chan exactly once -- count and checksum preserved."""
    nwork = 23
    niter = 2000
    pn = [2, 3, 7, 11, 13, 17, 19, 23, 27, 31]

    def body():
        q = rc.Chan(nwork * 3)
        r = rc.Chan(nwork * 3)
        expect = [0]
        wgw = WaitGroup(); wgw.add(nwork)

        def worker(w):
            try:
                for v in q:                      # ranges until q closed+drained
                    if pn[w % len(pn)] == v:
                        runloom.yield_now()      # perturb the fifo-ish order
                    r.send(v)
            finally:
                wgw.done()

        def feeder():
            e = 0
            for i in range(niter):
                v = pn[i % len(pn)]
                e += v
                q.send(v)
            expect[0] = e
            q.close()                            # no more work
            wgw.wait()                           # workers drain q
            r.close()                            # ... so no more results

        for w in range(nwork):
            runloom.fiber(lambda w=w: worker(w))
        runloom.fiber(feeder)

        n = 0
        s = 0
        for v in r:                              # consume until r closed
            n += 1
            s += v
        return n, s, expect[0]

    with hang_guard(60, "multi-consumer"):
        n, s, expect = run_body(body, hubs=4)
    assert n == niter, "expected %d values, saw %d" % (niter, n)
    assert s == expect, "checksum %d != expected %d" % (s, expect)


# ==========================================================================
# Mutex  (sync/mutex_test.go)  -- runloom.sync.Lock
# ==========================================================================
@mn
def test_mutex_hammer_mutual_exclusion():
    """TestMutex: G goroutines hammer lock/unlock (with an occasional
    non-blocking TryLock, mirroring HammerMutex's i%3==0 branch); the critical
    section is never occupied by two goroutines at once."""
    G, ITERS = 8, 200
    lk = Lock()
    guard = rc.Mutex()
    peak = [0]
    cur = [0]

    def body():
        wg = WaitGroup(); wg.add(G)

        def worker():
            try:
                for i in range(ITERS):
                    if i % 3 == 0:
                        if lk.acquire(blocking=False):
                            lk.release()
                        continue
                    lk.acquire()
                    guard.lock()
                    cur[0] += 1
                    if cur[0] > peak[0]:
                        peak[0] = cur[0]
                    guard.unlock()
                    runloom.yield_now()
                    guard.lock()
                    cur[0] -= 1
                    guard.unlock()
                    lk.release()
            finally:
                wg.done()
        for _ in range(G):
            runloom.fiber(worker)
        wg.wait()
        return "ok"

    with hang_guard(30, "mutex hammer"):
        assert run_body(body, hubs=4) == "ok"
    assert peak[0] == 1, "Lock admitted %d holders concurrently" % peak[0]


def test_mutex_trylock_truth_table():
    """TestMutex: TryLock (acquire(blocking=False)) fails on a held lock and
    succeeds on a free one; locked() reflects the state."""
    def body():
        lk = Lock()
        lk.acquire()
        assert lk.locked() is True
        assert lk.acquire(blocking=False) is False    # held -> TryLock fails
        lk.release()
        assert lk.locked() is False
        assert lk.acquire(blocking=False) is True     # free -> TryLock succeeds
        lk.release()
        return "ok"
    assert run_body(body) == "ok"


def test_mutex_misuse_release_unheld_raises():
    """TestMutexMisuse ('Mutex.Unlock'): releasing an unheld Lock raises with an
    'unlocked' message, rather than silently succeeding or crashing."""
    def body():
        lk = Lock()
        with pytest.raises(RuntimeError) as ei:
            lk.release()
        assert "unlock" in str(ei.value).lower()
        # And release-past-lock (the 'Mutex.Unlock2' variant).
        lk.acquire(); lk.release()
        with pytest.raises(RuntimeError):
            lk.release()
        return "ok"
    assert run_body(body) == "ok"


# ==========================================================================
# RWMutex  (sync/rwmutex_test.go)  -- runloom.sync.RWMutex
# ==========================================================================
@mn
def test_parallel_readers_all_hold_simultaneously():
    """TestParallelReaders: N readers all acquire the read lock at once (peak
    concurrent readers reaches N); a barrier holds them all before any releases,
    proving read locks do not exclude each other."""
    N = 8
    rw = RWMutex()
    guard = rc.Mutex()
    clocked = [0]
    peak = [0]
    release_gate = rc.Chan(0)

    def body():
        wg = WaitGroup(); wg.add(N)

        def reader():
            try:
                rw.rlock()
                guard.lock()
                clocked[0] += 1
                if clocked[0] > peak[0]:
                    peak[0] = clocked[0]
                guard.unlock()
                release_gate.recv()              # hold the read lock at the barrier
                rw.runlock()
            finally:
                wg.done()
        for _ in range(N):
            runloom.fiber(reader)
        assert spin_until(lambda: peak[0] == N), "only %d readers concurrent" % peak[0]
        for _ in range(N):                       # release the barrier
            release_gate.send(None)
        wg.wait()
        return "ok"

    with hang_guard(30, "parallel readers"):
        assert run_body(body, hubs=4) == "ok"
    assert peak[0] == N, "peak concurrent readers %d != %d" % (peak[0], N)


@mn
def test_rwmutex_activity_invariant_no_overlap():
    """TestRWMutex: under a reader/writer hammer, an 'activity' counter (readers
    add 1, writers add 10000) is only ever seen as 1..<10000 by a reader and
    exactly 10000 by a writer -- i.e. a reader and a writer are never active
    together and two writers are never active together."""
    READERS, WRITERS, ITERS = 6, 2, 150
    rw = RWMutex()
    guard = rc.Mutex()
    activity = [0]
    violations = bytearray(1)

    def body():
        wg = WaitGroup(); wg.add(READERS + WRITERS)

        def reader():
            try:
                for _ in range(ITERS):
                    rw.rlock()
                    guard.lock(); activity[0] += 1; n = activity[0]; guard.unlock()
                    if n < 1 or n >= 10000:
                        violations[0] = 1
                    guard.lock(); activity[0] -= 1; guard.unlock()
                    rw.runlock()
            finally:
                wg.done()

        def writer():
            try:
                for _ in range(ITERS):
                    rw.lock()
                    guard.lock(); activity[0] += 10000; n = activity[0]; guard.unlock()
                    if n != 10000:
                        violations[0] = 1
                    guard.lock(); activity[0] -= 10000; guard.unlock()
                    rw.unlock()
            finally:
                wg.done()

        for _ in range(READERS):
            runloom.fiber(reader)
        for _ in range(WRITERS):
            runloom.fiber(writer)
        wg.wait()
        return "ok"

    with hang_guard(40, "rwmutex activity"):
        assert run_body(body, hubs=4) == "ok"
    assert violations[0] == 0, "reader/writer overlap detected"


def test_rlocker_blocks_writer_and_write_lock_blocks_reader():
    """TestRLocker: a held read lock blocks a pending writer (RLocker behaves as
    a read lock), and a held write lock blocks a new reader (RLocker respects the
    write lock).  Deterministic single-thread via the parked-waiter queues."""
    def body():
        # Part A: an active reader blocks a writer.
        rw = RWMutex()
        rw.rlock()                               # main holds the read lock
        w_got = [False]

        def writer():
            rw.lock()
            w_got[0] = True
            rw.unlock()
        runloom.fiber(writer)
        assert spin_until(lambda: len(rw._wwait) == 1)   # writer parked
        assert w_got[0] is False                 # blocked by the reader
        rw.runlock()                             # drop the read lock
        assert spin_until(lambda: w_got[0] is True)

        # Part B: an active writer blocks a new reader (writer-preference).
        rw2 = RWMutex()
        rw2.lock()                               # main holds the write lock
        r_got = [False]

        def reader():
            rw2.rlock()
            r_got[0] = True
            rw2.runlock()
        runloom.fiber(reader)
        assert spin_until(lambda: len(rw2._rwait) == 1)  # reader parked
        assert r_got[0] is False                 # blocked by the writer
        rw2.unlock()                             # drop the write lock
        assert spin_until(lambda: r_got[0] is True)
        return "ok"

    with hang_guard(20, "rlocker"):
        assert run_body(body) == "ok"


# ==========================================================================
# WaitGroup  (sync/waitgroup_test.go)  -- runloom.sync.WaitGroup
# ==========================================================================
@mn
def test_waitgroup_two_group_barrier_reusable():
    """TestWaitGroup: the wg1/wg2 barrier -- n goroutines Done wg1 then Wait wg2;
    the main Wait(wg1) must not release before all Done, and the wg2 barrier must
    unblock everyone.  Reusable: the whole dance repeats several times."""
    N = 8
    REPEATS = 6

    def body():
        wg1 = WaitGroup()
        wg2 = WaitGroup()
        for _ in range(REPEATS):
            wg1.add(N)
            wg2.add(N)
            exited = rc.Chan(N)
            reached = rc.Chan(N)                  # checkpoint: worker is at wg2 barrier

            def worker():
                wg1.done()
                reached.try_send(True)
                wg2.wait()
                exited.try_send(True)
            for _ in range(N):
                runloom.fiber(worker)
            wg1.wait()                           # all workers reached Done(wg1)
            for _ in range(N):
                reached.recv()                   # all workers are now AT the wg2 barrier
            for _ in range(N):
                # With a correct barrier every worker is parked on wg2.wait()
                # (counter N>0); none may have exited before we Done it.  Gating on
                # `reached` first makes a broken (non-blocking) wait() reliably
                # surface here instead of racing the point-in-time try_recv.
                assert exited.try_recv() is None, "barrier released too soon"
                wg2.done()
            drained = 0
            while drained < N:                   # would block/hang if a wake is lost
                v, ok = exited.recv()
                assert ok and v is True
                drained += 1
        return "ok"

    with hang_guard(40, "waitgroup two-group"):
        assert run_body(body, hubs=4) == "ok"


@mn
def test_waitgroup_race_no_spurious_wakeup():
    """TestWaitGroupRace: Add(1) x2, two goroutines each atomically bump a counter
    then Done; after Wait the counter is exactly 2 -- Wait never returns before
    both Done effects are visible (no spurious wakeup)."""
    def body():
        for _ in range(200):
            wg = WaitGroup()
            guard = rc.Mutex()
            n = [0]

            def bump():
                guard.lock(); n[0] += 1; guard.unlock()
                wg.done()
            wg.add(1); runloom.fiber(bump)
            wg.add(1); runloom.fiber(bump)
            wg.wait()
            assert n[0] == 2, "spurious wakeup from Wait: n=%d" % n[0]
        return "ok"

    with hang_guard(40, "waitgroup race"):
        assert run_body(body, hubs=4) == "ok"


# ==========================================================================
# Once  (sync/once_test.go)  -- runloom.sync.Once / once_func
# ==========================================================================
@mn
def test_once_runs_once_and_value_visible_to_all():
    """TestOnce: Do runs f exactly once across N callers, and after each caller's
    Do returns, the value produced by f is visible (== 1) to that caller."""
    N = 24
    once = Once()
    guard = rc.Mutex()
    value = [0]
    seen_not_one = bytearray(1)

    def body():
        wg = WaitGroup(); wg.add(N)

        def caller():
            try:
                def inc():
                    guard.lock(); value[0] += 1; guard.unlock()
                once.do(inc)
                guard.lock(); v = value[0]; guard.unlock()
                if v != 1:                        # Go: *o must be 1 inside run()
                    seen_not_one[0] = 1
            finally:
                wg.done()
        for _ in range(N):
            runloom.fiber(caller)
        wg.wait()
        return value[0]

    with hang_guard(30, "once runs once"):
        runs = run_body(body, hubs=4)
    assert runs == 1, "Once ran f %d times" % runs
    assert seen_not_one[0] == 0, "a caller observed value != 1 after Do"


def test_once_panic_then_second_do_is_noop():
    """TestOncePanic: the first Do's f panics (the caller sees the exception),
    and a subsequent Do is a no-op -- its function must NOT run."""
    def body():
        once = Once()

        def boom():
            raise RuntimeError("failed")
        with pytest.raises(RuntimeError):
            once.do(boom)

        second_ran = [False]

        def should_not_run():
            second_ran[0] = True
        once.do(should_not_run)                  # must be a no-op after a used Once
        assert second_ran[0] is False, "Once.Do called f a second time"
        return "ok"
    assert run_body(body) == "ok"


def test_once_func_caches_success_and_reraises_panic():
    """OnceFunc (Go 1.21 sync.OnceFunc): the returned callable runs fn exactly
    once no matter how many times it is called; a fn that panics has its panic
    re-raised on EVERY call."""
    def body():
        calls = [0]

        def work():
            calls[0] += 1
        f = once_func(work)
        for _ in range(5):
            f()
        assert calls[0] == 1, "once_func ran fn %d times" % calls[0]

        boom_calls = [0]

        def boom():
            boom_calls[0] += 1
            raise KeyError("nope")
        g = once_func(boom)
        raised = 0
        for _ in range(4):
            try:
                g()
            except KeyError:
                raised += 1
        assert boom_calls[0] == 1, "once_func re-ran a panicking fn"
        assert raised == 4, "cached panic not re-raised to every caller"
        return "ok"
    assert run_body(body) == "ok"


# ==========================================================================
# Semaphore  (x/sync semaphore_test.go)  -- runloom.sync.Semaphore (weighted)
# ==========================================================================
@mn
def test_semaphore_value1_as_mutex_hammer():
    """TestSemaphore: a value-1 semaphore hammered by G goroutines acts as a
    mutex -- the guarded section is never entered by two goroutines at once and
    the whole run terminates."""
    G, ITERS = 10, 150
    sem = Semaphore(1)
    guard = rc.Mutex()
    peak = [0]
    cur = [0]

    def body():
        wg = WaitGroup(); wg.add(G)

        def hammer():
            try:
                for _ in range(ITERS):
                    sem.acquire(1)
                    guard.lock()
                    cur[0] += 1
                    if cur[0] > peak[0]:
                        peak[0] = cur[0]
                    guard.unlock()
                    runloom.yield_now()
                    guard.lock(); cur[0] -= 1; guard.unlock()
                    sem.release(1)
            finally:
                wg.done()
        for _ in range(G):
            runloom.fiber(hammer)
        wg.wait()
        return "ok"

    with hang_guard(30, "semaphore-as-mutex"):
        assert run_body(body, hubs=4) == "ok"
    assert peak[0] == 1, "value-1 semaphore admitted %d holders" % peak[0]


@mn
def test_weighted_held_never_exceeds_cap():
    """TestWeighted: goroutines acquire/release VARYING weights; the sum of
    currently-held weight never exceeds the semaphore's capacity."""
    CAP, G, LOOPS = 4, 12, 40
    sem = Semaphore(CAP)
    guard = rc.Mutex()
    held = [0]
    over = bytearray(1)

    def body():
        wg = WaitGroup(); wg.add(G)

        def worker(w):
            try:
                for _ in range(LOOPS):
                    sem.acquire(w)
                    guard.lock()
                    held[0] += w
                    if held[0] > CAP:
                        over[0] = 1
                    guard.unlock()
                    runloom.yield_now()
                    guard.lock(); held[0] -= w; guard.unlock()
                    sem.release(w)
            finally:
                wg.done()
        for i in range(G):
            runloom.fiber(lambda i=i: worker((i % CAP) + 1))   # weights 1..CAP
        wg.wait()
        return "ok"

    with hang_guard(30, "weighted held<=cap"):
        assert run_body(body, hubs=4) == "ok"
    assert over[0] == 0, "held weight exceeded capacity %d" % CAP


def test_weighted_try_acquire_truth_table():
    """TestWeightedTryAcquire: reproduces Go's exact [true,false,true,false]
    TryAcquire truth table on a cap-2 weighted semaphore."""
    def body():
        sem = Semaphore(2)
        tries = []
        sem.acquire(1)
        tries.append(sem.try_acquire(1))         # 1 free  -> True
        tries.append(sem.try_acquire(1))         # 0 free  -> False
        sem.release(2)                           # back to full
        tries.append(sem.try_acquire(1))         # 1 free  -> True
        sem.acquire(1)                           # now full
        tries.append(sem.try_acquire(1))         # 0 free  -> False
        return tries
    assert run_body(body) == [True, False, True, False]


def test_weighted_over_release_raises():
    """TestWeightedPanic: releasing an unacquired weighted semaphore raises,
    rather than silently corrupting the token count."""
    def body():
        sem = Semaphore(1)
        with pytest.raises(ValueError):
            sem.release(1)                       # nothing held
        return "ok"
    assert run_body(body) == "ok"


@mn
def test_large_acquire_does_not_starve():
    """TestLargeAcquireDoesntStarve: with all CAP tokens initially held and CAP
    goroutines churning single acquire/release, a single Acquire(CAP) must
    eventually succeed (FIFO fairness) -- the test hangs on starvation."""
    CAP = 4
    sem = Semaphore(CAP)
    running = [True]

    def body():
        wg = WaitGroup(); wg.add(CAP)
        for _ in range(CAP):
            sem.acquire(1)                        # main holds all CAP tokens

        def churn():
            try:
                while running[0]:
                    runloom.sleep(0.001)
                    sem.release(1)
                    sem.acquire(1)
            finally:
                sem.release(1)
                wg.done()
        for _ in range(CAP):
            runloom.fiber(churn)

        sem.acquire(CAP)                          # the large acquire: must not starve
        running[0] = False
        sem.release(CAP)
        wg.wait()
        return "ok"

    with hang_guard(40, "large-acquire no starve"):
        assert run_body(body, hubs=4) == "ok"


# ==========================================================================
# Condition  (sync/cond_test.go)  -- runloom.sync.Condition
# ==========================================================================
def test_cond_signal_wakes_exactly_one():
    """TestCondSignal: with N waiters parked, no waiter wakes without a Signal,
    and each notify(1) wakes exactly one.  Deterministic single-thread via the
    parked-waiter deque."""
    def body():
        lk = Lock()
        cond = Condition(lk)
        N = 4
        awake = [0]

        def waiter():
            cond.acquire()
            cond.wait()
            awake[0] += 1
            cond.release()
        for _ in range(N):
            runloom.fiber(waiter)
        assert spin_until(lambda: len(cond._waiters) == N)   # all parked
        for k in range(N):
            assert awake[0] == k, "woke without a signal"
            cond.acquire(); cond.notify(1); cond.release()
            assert spin_until(lambda: awake[0] == k + 1)
            yield_times(5)
            assert awake[0] == k + 1, "notify(1) woke more than one"
        cond.acquire(); cond.notify(1); cond.release()       # notify with no waiters: no-op
        return awake[0]

    with hang_guard(20, "cond signal one"):
        assert run_body(body) == 4


def test_cond_broadcast_wakes_all_current_waiters():
    """TestCondBroadcast: notify_all wakes every currently-parked waiter (each
    exactly once), whereas nothing wakes without the broadcast, and a broadcast
    with no waiters is a harmless no-op."""
    def body():
        lk = Lock()
        cond = Condition(lk)
        N = 8
        woke = bytearray(N)

        def waiter(idx):
            cond.acquire()
            cond.wait()
            woke[idx] = 1
            cond.release()
        for idx in range(N):
            runloom.fiber(lambda idx=idx: waiter(idx))
        assert spin_until(lambda: len(cond._waiters) == N)
        assert sum(woke) == 0, "a waiter woke without a broadcast"
        cond.acquire(); cond.notify_all(); cond.release()
        assert spin_until(lambda: sum(woke) == N)            # ALL woke (not one)
        assert list(woke) == [1] * N
        cond.acquire(); cond.notify_all(); cond.release()    # no waiters: no-op
        return "ok"

    with hang_guard(20, "cond broadcast"):
        assert run_body(body) == "ok"


def test_cond_signal_generations_wake_in_fifo_order():
    """TestCondSignalGenerations: waiters that parked earlier are signalled
    first -- successive notify(1) calls wake the waiters in wait (FIFO) order."""
    def body():
        lk = Lock()
        cond = Condition(lk)
        N = 6
        order = []

        def waiter(i):
            cond.acquire()
            cond.wait()
            order.append(i)
            cond.release()
        # Spawn in order; under run(1) each runs to its wait() and appends to the
        # FIFO deque in spawn order before the next starts.
        for i in range(N):
            runloom.fiber(lambda i=i: waiter(i))
            assert spin_until(lambda i=i: len(cond._waiters) == i + 1)
        for i in range(N):
            cond.acquire(); cond.notify(1); cond.release()
            assert spin_until(lambda i=i: len(order) == i + 1)
        return order

    with hang_guard(20, "cond generations"):
        assert run_body(body) == list(range(6))


@mn
def test_cond_three_goroutine_ordered_handoff():
    """TestRace (cond_test.go): a 3-goroutine ordered handshake through one
    Cond+Mutex -- G1 waits then is signalled and hands to state 3, G2 drives
    1->2 and signals, G3 waits for state 2 then observes state 3.  No goroutine
    sees a stale value; the whole handshake completes."""
    lk = Lock()
    cond = Condition(lk)
    state = [0]
    errors = []

    def body():
        wg = WaitGroup(); wg.add(3)

        def g1():
            try:
                cond.acquire()
                state[0] = 1
                cond.wait()
                if state[0] != 2:
                    errors.append(("g1", state[0]))
                state[0] = 3
                cond.notify(1)
                cond.release()
            finally:
                wg.done()

        def g2():
            try:
                cond.acquire()
                while True:
                    if state[0] == 1:
                        state[0] = 2
                        cond.notify(1)
                        break
                    cond.release()
                    runloom.yield_now()
                    cond.acquire()
                cond.release()
            finally:
                wg.done()

        def g3():
            try:
                cond.acquire()
                while True:
                    if state[0] == 2:
                        cond.wait()
                        if state[0] != 3:
                            errors.append(("g3", state[0]))
                        break
                    if state[0] == 3:
                        break
                    cond.release()
                    runloom.yield_now()
                    cond.acquire()
                cond.release()
            finally:
                wg.done()

        runloom.fiber(g1)
        runloom.fiber(g2)
        runloom.fiber(g3)
        wg.wait()
        return "ok"

    with hang_guard(30, "cond 3-way handoff"):
        assert run_body(body, hubs=3) == "ok"
    assert errors == [], "ordered handoff observed stale state: %r" % errors


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
