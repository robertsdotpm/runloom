"""Foreign-thread + GIL-off contention hammer (item 12).

The foreign-thread and mn-identity bug classes (appendix 76/90/91/92/93/98/123)
are deterministic ONCE the right (caller, contention) cell executes -- no
interleaving luck needed -- but the fast/uncontended paths normal tests hit
hide them.  This hammer drives the cooperative primitives with contention FORCED
(many workers + periodic yields so contenders take the parking slow path) and
checks two oracles:

  * exactly-once accounting -- a lost update / double-grant / lost or duplicated
    channel item shows a wrong count;
  * no strand -- a stranded fiber or spinning foreign thread shows as a
    subprocess TimeoutExpired.

Two distinct cells, because the primitives have different contracts:
  * FOREIGN-SAFE primitives (monkey CoLock, sync WaitGroup/Once) are driven from
    fibers AND real OS threads together -- the cell that surfaced the CoFMutex
    strand class.  Complements test_foreign_safe_mutex by hammering them under
    heavier mixed load.
  * FIBER-ONLY primitives (runloom_c.Mutex / Chan -- which correctly REQUIRE a
    goroutine context) are driven from fibers spread across MULTIPLE hubs (mn),
    the cross-hub cell where a wake must route to the owning hub.

Each scenario runs in its own subprocess so one strand can't wedge the file.
House style: %/.format, prints kept.
"""
import os
import subprocess
import sys
import textwrap

import pytest

PY = sys.executable
ENV = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")


def run_body(body, timeout=45):
    script = ("import runloom_c as rc, runloom, threading, time, sys\n"
              + textwrap.dedent(body))
    return subprocess.run([PY, "-c", script], env=ENV, capture_output=True,
                          timeout=timeout)


def expect(body, sentinel="OK", timeout=45):
    try:
        p = run_body(body, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail("STRAND: scenario hung (a fiber or foreign thread never "
                    "made progress under contention)")
    assert sentinel.encode() in p.stdout, (p.stdout[-800:], p.stderr[-800:])


# -------- foreign-safe primitives: fibers + real OS threads together ---------

def test_colock_exclusion_fibers_and_foreign_threads():
    # monkey CoLock (CoFMutex-backed, foreign-safe) guarding a non-atomic counter
    # bumped by fibers AND foreign threads.  Real exclusion -> exact; a lost
    # update -> short count; a strand -> timeout.
    expect("""
        from runloom.monkey.locks import CoLock
        lk = CoLock(); counter = [0]; K = 3000; NFIB = 4; NFOR = 4
        go = threading.Event()
        def foreign():
            go.wait()
            for _ in range(K):
                lk.acquire(); counter[0] += 1; lk.release()
        def fiber():
            for i in range(K):
                lk.acquire(); counter[0] += 1; lk.release()
                if i % 50 == 0: rc.sched_yield()
        ts = [threading.Thread(target=foreign) for _ in range(NFOR)]
        for t in ts: t.start()
        for _ in range(NFIB): rc.fiber(fiber)
        go.set(); rc.run()
        for t in ts: t.join(30)
        exp = (NFIB + NFOR) * K
        print("OK" if counter[0] == exp else "FAIL got=%d exp=%d" % (counter[0], exp))
    """)


def test_once_exactly_once_fibers_and_threads():
    # Many fibers AND threads race Once.do(init); init must run EXACTLY once and
    # every caller observe completion (ft-check-then-act class).
    expect("""
        from runloom.sync import Once
        once = Once(); runs = [0]; seen = [0]; NFIB = 6; NFOR = 6
        go = threading.Event()
        def init(): runs[0] += 1
        def foreign(): go.wait(); once.do(init); seen[0] += 1
        def fiber(): rc.sched_yield(); once.do(init); seen[0] += 1
        ts = [threading.Thread(target=foreign) for _ in range(NFOR)]
        for t in ts: t.start()
        for _ in range(NFIB): rc.fiber(fiber)
        go.set(); rc.run()
        for t in ts: t.join(30)
        ok = (runs[0] == 1 and seen[0] == NFIB + NFOR)
        print("OK" if ok else "FAIL runs=%d seen=%d" % (runs[0], seen[0]))
    """)


def test_waitgroup_foreign_and_fiber_no_strand():
    # Foreign threads AND fibers wait on one WaitGroup while fibers do the
    # decrements; the WaitGroup guard is CoFMutex, so no waiter is stranded and
    # every wait returns exactly when the count hits zero.
    expect("""
        from runloom.sync import WaitGroup
        wg = WaitGroup(); N = 8; wg.add(N); seen = [0]; lock = threading.Lock()
        go = threading.Event()
        def waiter_foreign():
            go.wait(); wg.wait()
            with lock: seen[0] += 1
        def waiter_fiber():
            wg.wait(); seen[0] += 1
        def worker():
            for _ in range(N):
                rc.sched_yield(); wg.done()
        ts = [threading.Thread(target=waiter_foreign) for _ in range(4)]
        for t in ts: t.start()
        for _ in range(4): rc.fiber(waiter_fiber)
        rc.fiber(worker)
        go.set(); rc.run()
        for t in ts: t.join(30)
        print("OK" if seen[0] == 8 else "FAIL seen=%d" % seen[0])
    """)


# -------- fiber-only primitives: the cross-hub (mn) cell ---------------------

def test_rc_mutex_cross_hub_exclusion():
    # rc.Mutex contended by fibers spread across MULTIPLE hubs -- the cross-hub
    # wake-routing cell.  Exact count == genuine exclusion with correct wake
    # routing; a short count == a lost update / misrouted wake.
    expect("""
        m = rc.Mutex(); counter = [0]; K = 2500; NFIB = 8
        done = [0]
        def body():
            def fiber():
                for i in range(K):
                    m.lock(); counter[0] += 1; m.unlock()
                    if i % 40 == 0: rc.sched_yield()
                done[0] += 1
            for _ in range(NFIB): rc.mn_fiber(fiber)
            while done[0] < NFIB: rc.sched_sleep(0.003)
        runloom.run(4, main_fn=body)
        exp = NFIB * K
        print("OK" if counter[0] == exp else "FAIL got=%d exp=%d" % (counter[0], exp))
    """)


def test_rc_chan_exactly_once_cross_hub():
    # Producers and consumers as fibers across MULTIPLE hubs, small buffered chan
    # -> heavy cross-hub park/wake.  Every item received EXACTLY once (no dropped
    # wake losing an item, no double-grant duplicating one).
    expect("""
        ch = rc.Chan(2); PER = 300; P = 5
        got = {}; done = [0]
        def body():
            def producer(base):
                for i in range(PER): ch.send(base + i)
                done[0] += 1
            def consumer(total):
                for _ in range(total):
                    v = ch.recv(); got[v] = got.get(v, 0) + 1
                done[0] += 1
            for p in range(P): rc.mn_fiber(lambda p=p: producer(p * 1000000))
            rc.mn_fiber(lambda: consumer(PER * P))
            while done[0] < P + 1: rc.sched_sleep(0.003)
        runloom.run(4, main_fn=body)
        dups = [k for k, c in got.items() if c != 1]
        ok = (len(got) == PER * P and not dups)
        print("OK" if ok else "FAIL n=%d dups=%d" % (len(got), len(dups)))
    """)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
