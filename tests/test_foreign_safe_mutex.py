"""Regression guard for the foreign-OS-thread-safe cooperative mutex (CoFMutex).

CoFMutex (runloom.monkey._base) closes the deadlock CLASS where a foreign OS
thread holds a channel-backed cooperative mutex that a fiber also locks: the
fiber chan-parks, single-thread run() abandons it (a chan-park is deliberately
not counted as live work), and the foreign unlock's cross-thread wake lands on a
dead loop -> stranded fiber, foreign thread spins forever.  Empirically this hit
Once ~1/240, WaitGroup 58/600, CoLock 31/400 before the fix.

These tests drive the affected primitives from REAL OS threads concurrently with
fibers.  A strand manifests as a hang (subprocess.TimeoutExpired); a broken mutex
manifests as lost counter updates.  Both are asserted against.
"""
import os
import subprocess
import sys
import textwrap

import pytest

_PY = sys.executable
_ENV = dict(os.environ, PYTHON_GIL="0")


def _run(body, timeout=40):
    script = "import runloom_c as rc, threading, time, sys\n" + textwrap.dedent(body)
    return subprocess.run([_PY, "-c", script], env=_ENV,
                          capture_output=True, timeout=timeout)


def test_cofmutex_mutual_exclusion_fibers_and_foreign_threads():
    # A non-atomic counter bumped under one CoLock (CoFMutex-backed) by N fibers
    # AND M real OS threads.  With genuine mutual exclusion the final value is
    # exact; a lost update (no exclusion) shows a short count.  A strand shows a
    # TimeoutExpired.
    p = _run("""
        from runloom.monkey.locks import CoLock
        lk = CoLock(); counter = [0]; K = 2000; NFIB = 4; NFOR = 4
        go = threading.Event()
        def bump_foreign():
            go.wait()
            for _ in range(K):
                lk.acquire(); counter[0] += 1; lk.release()
        def bump_fiber():
            for i in range(K):
                lk.acquire(); counter[0] += 1; lk.release()
                if i % 64 == 0:
                    rc.sched_yield()
        ts = [threading.Thread(target=bump_foreign) for _ in range(NFOR)]
        for t in ts: t.start()
        for _ in range(NFIB): rc.fiber(bump_fiber)
        go.set(); rc.run()
        for t in ts: t.join(25)
        exp = (NFIB + NFOR) * K
        print("MX_OK" if counter[0] == exp else "MX_FAIL got=%d exp=%d" % (counter[0], exp))
    """)
    assert b"MX_OK" in p.stdout, (p.stdout, p.stderr)


def test_cofmutex_foreign_holder_does_not_strand_fiber():
    # The exact strand shape: a foreign thread repeatedly takes the lock (holding
    # the guard in windows) while the single fiber contends the SAME lock.  Before
    # CoFMutex the fiber chan-parked and was abandoned; now it parks 0-fd
    # (foreign_wakeable, keeps run() alive) and the foreign release wakes it.
    # Run several rounds to exercise the rare race.
    p = _run("""
        from runloom.monkey.locks import CoLock
        for _round in range(6):
            lk = CoLock(); out = {}
            def fiber_worker():
                for _ in range(40):
                    lk.acquire(); lk.release()
                    rc.sched_yield()
                out['fiber'] = True
            def foreign_worker():
                for _ in range(250):
                    lk.acquire(); lk.release(); time.sleep(0.0004)
                out['foreign'] = True
            t = threading.Thread(target=foreign_worker); t.start()
            time.sleep(0.003)
            rc.fiber(fiber_worker); rc.run()
            t.join(5)
            if not (out.get('fiber') and out.get('foreign')):
                print("STRAND round=%d out=%r" % (_round, out)); sys.exit(1)
        print("NO_STRAND")
    """)
    assert b"NO_STRAND" in p.stdout, (p.stdout, p.stderr)


def test_waitgroup_foreign_waiter_not_stranded():
    # A foreign thread WAITs on a WaitGroup while a fiber does the decrement; the
    # WaitGroup guard is CoFMutex, so the fiber that contends it is never stranded.
    p = _run("""
        from runloom.sync import WaitGroup
        for _round in range(6):
            wg = WaitGroup(); wg.add(1); out = {}
            def fiber_worker():
                for _ in range(40):
                    wg.add(0); rc.sched_yield()
                wg.done(); out['fiber'] = True
            def foreign_waiter():
                wg.wait(); out['waited'] = True
            t = threading.Thread(target=foreign_waiter); t.start()
            time.sleep(0.003)
            rc.fiber(fiber_worker); rc.run()
            t.join(5)
            if not (out.get('fiber') and out.get('waited')):
                print("STRAND round=%d out=%r" % (_round, out)); sys.exit(1)
        print("NO_STRAND")
    """)
    assert b"NO_STRAND" in p.stdout, (p.stdout, p.stderr)
