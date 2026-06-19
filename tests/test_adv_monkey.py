"""Adversarial QA: runloom.monkey -- foreign-OS-thread safety.

The project's sharpest invariant (CLAUDE.md "Cooperative primitives must be
FOREIGN-OS-THREAD-safe"): monkey.patch() replaces threading/select/... GLOBALLY,
so a patched Lock/Event/Queue can be taken by a thread that is NOT a fiber
and NOT a hub (a stdlib daemon thread, a foreign worker).  Such a primitive
must detect the foreign caller and fall back to REAL OS blocking -- never park a
fiber that doesn't exist, never lazily allocate scheduler state, and (the
SIGSEGV path) never wake a parked fiber through a non-foreign-safe waker.

The headline test hammers ONE patched Lock from BOTH many fibers AND a real
OS thread at once, under M:N, and checks a counter guarded by that lock: a crash
(process death) or a wrong count (lost mutual exclusion / lost wake) is the
finding.  Run this file isolated -- a SIGSEGV here is contained per-file by
tests/run_isolated.py.

NOTE: monkey.patch() is process-global and irreversible-ish, so this whole file
runs under the patch (like the existing monkey suites).
"""
import sys
import time

import runloom.monkey as monkey
monkey.patch()

import threading          # patched
import queue              # patched
import socket             # patched

import pytest

import runloom
import runloom_c as rc
from adv_util import hang_guard, needs_free_threading

# A genuinely-foreign OS thread: the monkey go-wrapper marks fiber context
# via a thread-local counter, so a thread we start that NEVER runs a fiber
# stays foreign.  threading.Thread is patched, but it still spawns a real OS
# thread (monkey runs "threads" as OS threads, not fibers, unless they run
# fiber work) -- which is exactly the foreign caller we want.
import _thread as _real_thread_mod
FT = needs_free_threading()


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.fiber(main)
    rc.run()
    return box.get("r")


# --------------------------------------------------------------------------
# patch() hygiene
# --------------------------------------------------------------------------
def test_patch_is_idempotent():
    monkey.patch()        # second call must be a harmless no-op
    monkey.patch()
    assert threading.Lock is not None


def test_patched_lock_is_cooperative_type():
    lk = threading.Lock()
    # Under patch a Lock is the cooperative CoLock, not the builtin _thread.lock.
    assert type(lk).__module__.startswith("runloom")


# --------------------------------------------------------------------------
# patched Lock mutual exclusion among fibers (single-thread + M:N)
# --------------------------------------------------------------------------
def test_patched_lock_mutual_exclusion_fibers_single_thread():
    lk = threading.Lock()
    counter = [0]
    N, ITERS = 16, 200
    def worker():
        for _ in range(ITERS):
            with lk:
                counter[0] += 1
    def main():
        for _ in range(N):
            rc.fiber(worker)
    with hang_guard(20, "lock mutex single-thread"):
        rc.fiber(main); rc.run()
    assert counter[0] == N * ITERS, "lost increments: %d != %d" % (counter[0], N * ITERS)


@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_patched_lock_mutual_exclusion_fibers_mn():
    lk = threading.Lock()
    counter = [0]
    N, ITERS = 32, 200
    def worker():
        for _ in range(ITERS):
            with lk:
                counter[0] += 1            # GIL-off: only the lock makes this safe
    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(N)
        def w():
            try:
                worker()
            finally:
                wg.done()
        for _ in range(N):
            rc.mn_fiber(w)
        wg.wait()
    with hang_guard(40, "lock mutex M:N"):
        runloom.run(4, main)
    assert counter[0] == N * ITERS, "lost increments under M:N: %d != %d" % (counter[0], N * ITERS)


# --------------------------------------------------------------------------
# HEADLINE: a patched Lock shared by fibers AND a real OS thread at once.
# Exercises the foreign-thread acquire (spin) + the foreign-thread RELEASE that
# can wake a parked fiber cross-thread -- the documented SIGSEGV surface.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_patched_lock_foreign_thread_plus_fibers():
    lk = threading.Lock()
    counter = [0]
    GOR, GOR_ITERS = 24, 150
    FOREIGN_ITERS = 4000

    # Start the foreign OS thread on the raw _thread API so it is unquestionably
    # NOT a fiber and NOT a hub.  It hammers the SAME lock while the M:N
    # scheduler runs fibers that also hammer it.
    #
    # Completion handshake: a RAW _thread lock (unpatched -- it comes from the
    # real _thread module, so it is a true OS lock and adds no cooperative
    # primitive to the verdict).  We hold it from before the thread starts; the
    # foreign thread releases it the instant it finishes its last increment.
    # The main thread then blocks on a real OS acquire -- deterministic, with a
    # generous cap that only bounds a hang, never the happy path.  This replaces
    # the old sleep-poll on the deadline, whose 30s timeout could expire under
    # CPU load and invert the verdict even though the count was correct.
    done_latch = _real_thread_mod.allocate_lock()
    done_latch.acquire()
    def foreign_body():
        try:
            for _ in range(FOREIGN_ITERS):
                with lk:
                    counter[0] += 1
        finally:
            done_latch.release()
    _real_thread_mod.start_new_thread(foreign_body, ())

    def main():
        from runloom.sync import WaitGroup
        wg = WaitGroup(); wg.add(GOR)
        def w():
            try:
                for _ in range(GOR_ITERS):
                    with lk:
                        counter[0] += 1
            finally:
                wg.done()
        for _ in range(GOR):
            rc.mn_fiber(w)
        wg.wait()

    with hang_guard(60, "lock foreign+fibers"):
        runloom.run(4, main)
        # Wait for the foreign thread to finish its remaining increments via the
        # raw-lock latch (deterministic; the 30s cap only bounds a true hang).
        acquired = done_latch.acquire(timeout=30)
        assert acquired, "foreign thread never released its completion latch (hang)"

    expected = GOR * GOR_ITERS + FOREIGN_ITERS
    assert counter[0] == expected, (
        "lock failed under mixed fiber/foreign contention: %d != %d "
        "(lost mutual exclusion or a lost cross-thread wake)" % (counter[0], expected))


# --------------------------------------------------------------------------
# patched Event fan-in + foreign-thread wait
# --------------------------------------------------------------------------
def test_patched_event_wakes_all_fiber_waiters():
    ev = threading.Event()
    woke = []
    N = 32
    def waiter():
        ev.wait()
        woke.append(1)
    def main():
        for _ in range(N):
            rc.fiber(waiter)
        rc.sched_yield()        # all park in ev.wait()
        ev.set()
    with hang_guard(20, "event fan-in"):
        rc.fiber(main); rc.run()
    assert len(woke) == N


def test_patched_event_foreign_thread_can_wait():
    ev = threading.Event()
    box = {}
    def foreign():
        # a foreign thread waiting on a patched Event must poll real-time, not
        # park a nonexistent fiber
        box["got"] = ev.wait(timeout=2.0)
    _real_thread_mod.start_new_thread(foreign, ())
    time.sleep(0.05)
    ev.set()
    deadline = time.monotonic() + 3
    while "got" not in box and time.monotonic() < deadline:
        time.sleep(0.01)
    assert box.get("got") is True


# --------------------------------------------------------------------------
# patched Queue producer/consumer
# --------------------------------------------------------------------------
def test_patched_simplequeue_producer_consumer():
    q = queue.SimpleQueue()
    N = 500
    got = []
    def producer():
        for i in range(N):
            q.put(i)
    def consumer():
        for _ in range(N):
            got.append(q.get())
    def main():
        rc.fiber(consumer)
        rc.fiber(producer)
    with hang_guard(20, "simplequeue"):
        rc.fiber(main); rc.run()
    assert got == list(range(N))


def test_patched_queue_blocking_get_across_fibers():
    q = queue.Queue()
    out = []
    def consumer():
        out.append(q.get())     # blocks until producer puts
        out.append(q.get())
    def producer():
        runloom.sleep(0.02)
        q.put("a")
        runloom.sleep(0.02)
        q.put("b")
    def main():
        rc.fiber(consumer)
        rc.fiber(producer)
    with hang_guard(20, "queue blocking get"):
        rc.fiber(main); rc.run()
    assert out == ["a", "b"]


# --------------------------------------------------------------------------
# monkey socket cooperative echo + DNS literal
# --------------------------------------------------------------------------
def test_monkey_socket_echo_roundtrip():
    result = {}
    def main():
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        port = srv.getsockname()[1]

        def server():
            conn, _ = srv.accept()
            data = conn.recv(64)
            conn.sendall(b"echo:" + data)
            conn.close()

        def client():
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(("127.0.0.1", port))
            c.sendall(b"hello")
            result["reply"] = c.recv(64)
            c.close()
            srv.close()

        rc.fiber(server)
        rc.fiber(client)
    with hang_guard(20, "monkey socket echo"):
        rc.fiber(main); rc.run()
    assert result.get("reply") == b"echo:hello"


def test_getaddrinfo_ip_literal_no_network():
    def f():
        res = socket.getaddrinfo("127.0.0.1", 80, socket.AF_INET, socket.SOCK_STREAM)
        return res[0][4][0]
    with hang_guard(15, "getaddrinfo literal"):
        assert _run_single(f) == "127.0.0.1"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
