"""Adversarial QA swarm: runloom.monkey -- the global stdlib cooperativiser.

monkey.patch() is process-global, so (like every existing monkey suite) this
file patches ONCE at module top and the WHOLE FILE runs under the patch.  The
existing tests/test_adv_monkey.py covers the headline; this file goes MUCH
deeper into the failure modes a lock-free M:N scheduler + non-blocking I/O
break under:

  * CRASH         -- a foreign-thread caller must never park a nonexistent
                     fiber nor lazily allocate scheduler state (the
                     documented SIGSEGV class).  Crash-prone scenarios run in a
                     SUBPROCESS so a SIGSEGV is contained + observed.
  * HANG/lost-wake -- every fan-in/cross-thread wake is wrapped in hang_guard
                     and/or driven with finite timeouts so a lost wake is a
                     bounded failure, never an infinite hang.
  * WRONG DATA    -- a lock-guarded counter hammered by fibers AND a real
                     OS thread must be EXACT with the GIL off; a Queue/Event/
                     Condition must deliver every item / wake every waiter.
  * REORDER       -- FIFO Queue ordering, notify(n) waking exactly n, a timed-
                     out Condition waiter not stealing a later notify().
  * SLOW RETURN   -- a foreign+fiber contended primitive that DOES complete
                     but serialized (assert_faster_than) is still a bug.
  * UNVALIDATED INPUT -- patch() unknown category, BoundedSemaphore over-release,
                     CoSemaphore negative value, DNS bogus name / family mismatch.

THE HEADLINE is FOREIGN-OS-THREAD SAFETY (CLAUDE.md "Cooperative primitives must
be FOREIGN-OS-THREAD-safe"): for each patched primitive, drive it from a genuine
foreign OS thread (raw_thread / _thread.start_new_thread) WHILE fibers also
use it under runloom.run(N), asserting no crash and correct behaviour.
"""
import os
import sys
import errno
import time
import signal as _signal
import socket as _bare_socket_for_pair      # only for socketpair in ssl test
import subprocess
import textwrap
import _thread as _real_thread_mod

import pytest

# ---- patch ONCE, at module top, before importing the patched stdlib names ----
import runloom.monkey as monkey
monkey.patch()

import threading          # patched
import queue              # patched
import socket             # patched
import selectors          # patched (via select.poll/epoll factories)
import select as _select_mod  # patched

import runloom
import runloom_c as rc
from runloom.sync import WaitGroup
from adv_util import (hang_guard, assert_faster_than, raw_thread,
                      needs_free_threading, free_tcp_port_pair)

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(REPO, "src")
mn_only = pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")


# ==========================================================================
# subprocess crash-containment helper (a SIGSEGV here is contained + observed)
# ==========================================================================
def run_child(body, extra_env=None, timeout=60):
    """Run `body` (dedented) as a fresh child, patched, return (rc, combined).

    A crash-prone scenario runs here so a SIGSEGV/abort is CONTAINED as a
    negative returncode and the crash handler's classification is observable on
    stderr instead of taking down this test process.
    """
    src = ("import sys; sys.path.insert(0, %r)\n" % _SRC +
           "import runloom, runloom_c\n"
           "import runloom.monkey as monkey\n"
           "monkey.patch()\n" +
           textwrap.dedent(body))
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = _SRC
    env.setdefault("RUNLOOM_GOROUTINE_PANIC", "silent")
    if extra_env:
        env.update(extra_env)
    p = subprocess.run([sys.executable, "-c", src],
                       capture_output=True, text=True, env=env, timeout=timeout)
    return p.returncode, (p.stdout + p.stderr)


def assert_no_signal_death(rc, out, label):
    """A clean exit (>=0) or a clean Python error is fine; a negative rc means
    death by signal (SIGSEGV/SIGABRT) -- the bug we are hunting."""
    assert rc is None or rc >= 0, (
        "%s died by signal %d (a crash/UAF, not a clean error)\n%s"
        % (label, -rc, out[-3000:]))


def _run_single(fn):
    box = {}
    def main():
        box["r"] = fn()
    rc.fiber(main); rc.run()
    return box.get("r")


def _run_mn(fn, n=4):
    """Run fn() inside a hub fiber under run(n); fn's children must use mn_go."""
    box = {}
    def main():
        box["r"] = fn()
    runloom.run(n, main)
    return box.get("r")


# ==========================================================================
# 1. patch() / unpatch() hygiene + argument validation
# ==========================================================================
def test_patch_unknown_category_raises_typeerror():
    with pytest.raises(TypeError):
        monkey.patch(nonsense_category=True)
    with pytest.raises(TypeError):
        monkey.unpatch(also_bogus=False)


def test_patch_idempotent_and_keeps_cooperative_types():
    monkey.patch()
    monkey.patch(threading=True, queue=True)   # already-applied -> no-op
    assert type(threading.Lock()).__module__.startswith("runloom")
    assert type(threading.Event()).__name__ == "CoEvent"
    assert type(threading.Condition()).__name__ == "CoCondition"
    assert type(threading.Semaphore()).__name__ == "CoSemaphore"
    assert type(threading.BoundedSemaphore()).__name__ == "CoBoundedSemaphore"
    assert type(queue.SimpleQueue()).__name__ == "CoSimpleQueue"


def test_getattr_resolves_section_internals_live():
    # The PEP 562 __getattr__ must resolve a section-internal that is rebound at
    # patch time (e.g. dns._orig_getaddrinfo) live through the package.
    assert monkey._orig_getaddrinfo is not None   # set by _patch_dns
    assert callable(monkey._patched_getaddrinfo)
    with pytest.raises(AttributeError):
        monkey.this_name_does_not_exist_anywhere


def test_fiber_wrapper_installed_marks_fiber_context():
    # After patch(), runloom_c.fiber is wrapped so _in_fiber() is true inside.
    box = {}
    def main():
        box["in_fiber"] = monkey._in_fiber()
        box["off_fiber_seen_from_here"] = True
    rc.fiber(main); rc.run()
    assert box["in_fiber"] is True
    # Outside any fiber, _in_fiber() is false.
    assert monkey._in_fiber() is False


def test_queue_uses_cooperative_condition_after_patch():
    q = queue.Queue()
    # Queue builds its internal locks from threading at __init__; under patch
    # they must be the cooperative ones, else a blocking get() freezes the hub.
    assert type(q.not_empty).__name__ == "CoCondition"
    assert type(q.mutex).__module__.startswith("runloom")


# ==========================================================================
# 2. HEADLINE: a patched Lock hammered by MANY fibers AND a foreign OS
#    thread, EXACT guarded counter (lost mutex / lost cross-thread wake / crash)
# ==========================================================================
@mn_only
def test_lock_exact_count_foreign_plus_fibers_heavy():
    lk = threading.Lock()
    counter = [0]
    GOR, GOR_ITERS = 32, 300
    FOREIGN_THREADS, FOREIGN_ITERS = 3, 4000

    foreign_done = [0]
    foreign_done_lk = _real_thread_mod.allocate_lock()  # raw lock; NOT patched obj

    def foreign_body():
        for _ in range(FOREIGN_ITERS):
            with lk:
                counter[0] += 1
        with foreign_done_lk:
            foreign_done[0] += 1

    for _ in range(FOREIGN_THREADS):
        _real_thread_mod.start_new_thread(foreign_body, ())

    def main():
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

    with hang_guard(90, "lock exact-count foreign+fibers heavy"):
        runloom.run(4, main)
        deadline = time.monotonic() + 40
        while foreign_done[0] < FOREIGN_THREADS and time.monotonic() < deadline:
            time.sleep(0.005)

    expected = GOR * GOR_ITERS + FOREIGN_THREADS * FOREIGN_ITERS
    assert foreign_done[0] == FOREIGN_THREADS, "foreign threads did not finish"
    assert counter[0] == expected, (
        "lost mutual exclusion / lost cross-thread wake: %d != %d"
        % (counter[0], expected))


@mn_only
def test_rlock_exact_count_foreign_plus_fibers():
    # RLock reentrancy + ownership identity differs between a fiber
    # (runloom.current()) and a foreign thread (get_ident); both must serialize.
    rl = threading.RLock()
    counter = [0]
    GOR, GOR_ITERS = 24, 200
    FOREIGN_ITERS = 3000
    fdone = [False]

    def foreign_body():
        for _ in range(FOREIGN_ITERS):
            with rl:
                with rl:               # reentrant acquire on the same thread
                    counter[0] += 1
        fdone[0] = True
    _real_thread_mod.start_new_thread(foreign_body, ())

    def main():
        wg = WaitGroup(); wg.add(GOR)
        def w():
            try:
                for _ in range(GOR_ITERS):
                    with rl:
                        with rl:
                            counter[0] += 1
            finally:
                wg.done()
        for _ in range(GOR):
            rc.mn_fiber(w)
        wg.wait()

    with hang_guard(70, "rlock foreign+fibers"):
        runloom.run(4, main)
        deadline = time.monotonic() + 30
        while not fdone[0] and time.monotonic() < deadline:
            time.sleep(0.005)

    expected = GOR * GOR_ITERS + FOREIGN_ITERS
    assert fdone[0], "foreign rlock thread did not finish"
    assert counter[0] == expected, "rlock lost mutual exclusion: %d != %d" % (
        counter[0], expected)


@mn_only
def test_semaphore_exact_count_foreign_plus_fibers():
    # A Semaphore(1) is a mutex; foreign + fiber contenders must serialize.
    sem = threading.Semaphore(1)
    counter = [0]
    GOR, GOR_ITERS = 20, 200
    FOREIGN_ITERS = 3000
    fdone = [False]

    def foreign_body():
        for _ in range(FOREIGN_ITERS):
            sem.acquire()
            counter[0] += 1
            sem.release()
        fdone[0] = True
    _real_thread_mod.start_new_thread(foreign_body, ())

    def main():
        wg = WaitGroup(); wg.add(GOR)
        def w():
            try:
                for _ in range(GOR_ITERS):
                    with sem:
                        counter[0] += 1
            finally:
                wg.done()
        for _ in range(GOR):
            rc.mn_fiber(w)
        wg.wait()

    with hang_guard(70, "semaphore foreign+fibers"):
        runloom.run(4, main)
        deadline = time.monotonic() + 30
        while not fdone[0] and time.monotonic() < deadline:
            time.sleep(0.005)

    expected = GOR * GOR_ITERS + FOREIGN_ITERS
    assert fdone[0]
    assert counter[0] == expected, "semaphore-1 lost mutex: %d != %d" % (
        counter[0], expected)


# ==========================================================================
# 3. The SIGSEGV class: a foreign thread taking a patched primitive must NOT
#    lazily allocate scheduler state nor park a nonexistent fiber.  Run
#    crash-prone foreign+M:N combos in a subprocess so a SIGSEGV is contained.
# ==========================================================================
def test_foreign_thread_lock_no_scheduler_alloc_subprocess():
    rc_, out = run_child("""
        import threading, time, _thread
        import runloom, runloom_c as rc
        lk = threading.Lock()
        cond = threading.Condition()
        counter = [0]
        done = [0]

        # MANY foreign threads taking patched primitives BEFORE any scheduler
        # exists in this process -- they must fall back to real OS blocking, not
        # runloom_sched_get() (which mallocs a sched on a non-hub thread -> UAF).
        def foreign():
            for _ in range(2000):
                with lk:
                    counter[0] += 1
            done[0] += 1
        for _ in range(6):
            _thread.start_new_thread(foreign, ())

        # Now also run fibers concurrently under M:N on the SAME lock.
        from runloom.sync import WaitGroup
        def main():
            wg = WaitGroup(); wg.add(8)
            def w():
                try:
                    for _ in range(500):
                        with lk:
                            counter[0] += 1
                finally:
                    wg.done()
            for _ in range(8):
                rc.mn_fiber(w)
            wg.wait()
        runloom.run(4, main)
        dl = time.monotonic() + 20
        while done[0] < 6 and time.monotonic() < dl:
            time.sleep(0.005)
        assert counter[0] == 6*2000 + 8*500, counter[0]
        print("OK", counter[0])
    """, timeout=90)
    assert_no_signal_death(rc_, out, "foreign-lock-no-alloc")
    assert "OK" in out, out


def test_foreign_condition_woken_by_fiber_notify_subprocess():
    # A foreign thread waiting on a patched Condition must make progress via
    # real OS blocking and be woken by a fiber's cross-thread notify_all.
    rc_, out = run_child("""
        import threading, time, _thread
        import runloom, runloom_c as rc
        cv = threading.Condition()
        st = {"ready": False, "woke": 0}
        NW = 4
        def foreign():
            with cv:
                while not st["ready"]:
                    cv.wait()
                st["woke"] += 1
        for _ in range(NW):
            _thread.start_new_thread(foreign, ())
        time.sleep(0.15)
        def main():
            with cv:
                st["ready"] = True
                cv.notify_all()
        runloom.run(2, main)
        dl = time.monotonic() + 5
        while st["woke"] < NW and time.monotonic() < dl:
            time.sleep(0.01)
        assert st["woke"] == NW, st["woke"]
        print("OK", st["woke"])
    """, timeout=60)
    assert_no_signal_death(rc_, out, "foreign-condition-notify")
    assert "OK" in out, out


# ==========================================================================
# 4. Event / Condition fan-in correctness (wake ALL / exactly-n / no steal)
# ==========================================================================
def test_event_wakes_all_fiber_waiters_single_thread():
    ev = threading.Event()
    woke = []
    N = 64
    def waiter():
        ev.wait()
        woke.append(1)
    def main():
        for _ in range(N):
            rc.fiber(waiter)
        rc.sched_yield()
        ev.set()
    with hang_guard(20, "event fan-in 64"):
        rc.fiber(main); rc.run()
    assert len(woke) == N


@mn_only
def test_event_fanin_mixed_foreign_and_fiber_waiters():
    ev = threading.Event()
    foreign_woke = [0]
    NG = 32
    NF = 4
    foreign_started = [0]
    fs_lk = _real_thread_mod.allocate_lock()

    def foreign():
        with fs_lk:
            foreign_started[0] += 1
        if ev.wait(timeout=10):
            with fs_lk:
                foreign_woke[0] += 1
    for _ in range(NF):
        _real_thread_mod.start_new_thread(foreign, ())
    # wait until all foreign waiters are actually parked in ev.wait()
    dl = time.monotonic() + 5
    while foreign_started[0] < NF and time.monotonic() < dl:
        time.sleep(0.005)

    # Per-fiber slot (single writer each) summed at the boundary -- a shared
    # counter += 1 from many fibers LOSES increments with the GIL off (the
    # documented race-free-counter rule), which is a test artefact, not a wake
    # bug.  We only want to prove every fiber waiter was woken exactly once.
    woke_slots = bytearray(NG)

    def main():
        wg = WaitGroup(); wg.add(NG)
        def make_waiter(i):
            def waiter():
                try:
                    ev.wait()
                    woke_slots[i] = 1
                finally:
                    wg.done()
            return waiter
        for i in range(NG):
            rc.mn_fiber(make_waiter(i))
        rc.sched_yield()
        rc.sched_yield()
        ev.set()                     # ONE set wakes fibers AND foreign
        wg.wait()

    with hang_guard(40, "event fan-in mixed"):
        runloom.run(4, main)
        dl = time.monotonic() + 12
        while foreign_woke[0] < NF and time.monotonic() < dl:
            time.sleep(0.005)
    gor_woke_total = sum(woke_slots)
    assert gor_woke_total == NG, "fiber waiters: %d != %d (a lost wake)" % (
        gor_woke_total, NG)
    assert foreign_woke[0] == NF, "foreign waiters: %d != %d" % (foreign_woke[0], NF)


def test_event_timed_wait_returns_false_on_timeout_not_slow():
    ev = threading.Event()
    out = {}
    def main():
        with assert_faster_than(2.0, "event timed wait"):
            out["r"] = ev.wait(timeout=0.2)   # never set -> times out
    with hang_guard(10, "event timed wait timeout"):
        rc.fiber(main); rc.run()
    assert out["r"] is False


def test_condition_notify_n_wakes_exactly_n():
    cv = threading.Condition()
    woke = []
    parked = [0]
    N = 16
    def make_waiter(i):
        def waiter():
            with cv:
                parked[0] += 1
                cv.wait()
                woke.append(i)
        return waiter
    def main():
        for i in range(N):
            rc.fiber(make_waiter(i))
        # let all N park
        while parked[0] < N:
            rc.sched_yield()
        with cv:
            cv.notify(5)     # wake exactly 5
        rc.sched_yield()
        # 5 should have woken; the rest stay parked -> wake them so run() ends.
        assert len(woke) == 5, "notify(5) woke %d, expected 5" % len(woke)
        with cv:
            cv.notify_all()
    with hang_guard(20, "condition notify(n)"):
        rc.fiber(main); rc.run()
    assert len(woke) == N
    assert sorted(woke[:5]) == list(range(5)), "notify is not FIFO: %r" % woke[:5]


def test_condition_timed_out_waiter_does_not_steal_later_notify():
    # A waiter that timed out must NOT remain queued to steal a notify meant for
    # a live waiter -- the documented "lingering timed-out parker" hazard.
    cv = threading.Condition()
    order = []
    def timed_waiter():
        with cv:
            r = cv.wait(timeout=0.1)    # times out (nothing notifies yet)
            order.append(("timed", r))
    def live_waiter():
        with cv:
            r = cv.wait(timeout=5.0)
            order.append(("live", r))
    def main():
        rc.fiber(timed_waiter)
        rc.sched_yield()
        # let the timed waiter actually time out
        runloom.sleep(0.25)
        rc.fiber(live_waiter)
        rc.sched_yield()
        runloom.sleep(0.05)
        with cv:
            cv.notify()     # must wake the LIVE waiter, not the dead one
    with hang_guard(20, "condition no-steal"):
        rc.fiber(main); rc.run()
    # the live waiter must have been woken by notify (r True), not timed out
    live = [r for tag, r in order if tag == "live"]
    assert live == [True], (
        "timed-out waiter stole the notify; live waiter saw %r" % live)


# ==========================================================================
# 5. Semaphore: cancel_all path, timed-acquire bound, bounded over-release,
#    negative-value validation, fair-ish hand-off correctness.
# ==========================================================================
def test_semaphore_negative_initial_value_rejected():
    with pytest.raises(ValueError):
        threading.Semaphore(-1)


def test_bounded_semaphore_over_release_raises():
    bs = threading.BoundedSemaphore(2)
    bs.acquire(); bs.acquire()
    bs.release(); bs.release()
    with pytest.raises(ValueError):
        bs.release()                # over the initial bound


def test_semaphore_limits_concurrency_exactly():
    sem = threading.Semaphore(3)
    live = [0]
    peak = [0]
    N = 40
    def worker():
        sem.acquire()
        try:
            live[0] += 1
            if live[0] > peak[0]:
                peak[0] = live[0]
            runloom.sleep(0.002)
            live[0] -= 1
        finally:
            sem.release()
    def main():
        wg = WaitGroup(); wg.add(N)
        def w():
            try:
                worker()
            finally:
                wg.done()
        for _ in range(N):
            rc.fiber(w)
        wg.wait()
    with hang_guard(30, "semaphore concurrency cap"):
        rc.fiber(main); rc.run()
    assert peak[0] <= 3, "semaphore let %d run at once (cap 3)" % peak[0]


def test_semaphore_timed_acquire_bounded_not_slow():
    sem = threading.Semaphore(0)     # never has a permit
    out = {}
    def main():
        with assert_faster_than(1.0, "sem timed acquire"):
            out["r"] = sem.acquire(timeout=0.2)
    with hang_guard(10, "sem timed acquire"):
        rc.fiber(main); rc.run()
    assert out["r"] is False


def test_semaphore_cancel_all_unblocks_waiters_without_permit():
    sem = monkey.CoSemaphore(0)
    results = []
    parked = [0]
    N = 8
    def waiter():
        parked[0] += 1
        results.append(sem.acquire())   # blocking, no timeout
    def main():
        for _ in range(N):
            rc.fiber(waiter)
        while parked[0] < N:
            rc.sched_yield()
        runloom.sleep(0.02)
        sem.cancel_all()                # wake all WITHOUT a permit -> all False
    with hang_guard(20, "semaphore cancel_all"):
        rc.fiber(main); rc.run()
    assert results == [False] * N, "cancel_all did not unblock all waiters: %r" % results


# ==========================================================================
# 6. Queue / SimpleQueue: ordering, timeouts, no lost wakeup under fan-in,
#    foreign + fiber producer/consumer.
# ==========================================================================
def test_simplequeue_fifo_and_get_nowait_empty():
    q = queue.SimpleQueue()
    out = {}
    def main():
        q.put(1); q.put(2); q.put(3)
        out["a"] = [q.get(), q.get(), q.get()]
        try:
            q.get_nowait()
            out["empty"] = False
        except queue.Empty:
            out["empty"] = True
    rc.fiber(main); rc.run()
    assert out["a"] == [1, 2, 3]
    assert out["empty"] is True


def test_simplequeue_timed_get_times_out_bounded():
    q = queue.SimpleQueue()
    out = {}
    def main():
        with assert_faster_than(1.0, "simplequeue timed get"):
            try:
                q.get(timeout=0.2)
                out["raised"] = False
            except queue.Empty:
                out["raised"] = True
    with hang_guard(10, "simplequeue timed get"):
        rc.fiber(main); rc.run()
    assert out["raised"] is True


def test_queue_many_producers_consumers_no_lost_item():
    q = queue.Queue()
    NP, NC, PER = 8, 8, 200
    produced = NP * PER
    got = []
    got_lk = threading.Lock()
    SENTINEL = object()

    def producer():
        for i in range(PER):
            q.put(i)
    def consumer():
        while True:
            item = q.get()
            if item is SENTINEL:
                return
            with got_lk:
                got.append(item)
    def main():
        wg = WaitGroup(); wg.add(NP)
        def p():
            try:
                producer()
            finally:
                wg.done()
        for _ in range(NP):
            rc.fiber(p)
        for _ in range(NC):
            rc.fiber(consumer)
        # one driver waits for producers then poisons each consumer
        def driver():
            wg.wait()
            for _ in range(NC):
                q.put(SENTINEL)
        rc.fiber(driver)
    with hang_guard(30, "queue many prod/cons"):
        rc.fiber(main); rc.run()
    assert len(got) == produced, "lost items: %d != %d" % (len(got), produced)


@mn_only
def test_simplequeue_foreign_producer_fiber_consumer():
    q = queue.SimpleQueue()
    N = 300
    got = []
    fdone = [False]

    def foreign_producer():
        for i in range(N):
            q.put(i)
            if i % 64 == 0:
                time.sleep(0)   # let the fiber drain
        fdone[0] = True
    _real_thread_mod.start_new_thread(foreign_producer, ())

    def main():
        wg = WaitGroup(); wg.add(1)
        def consumer():
            try:
                for _ in range(N):
                    got.append(q.get())
            finally:
                wg.done()
        rc.mn_fiber(consumer)
        wg.wait()
    with hang_guard(40, "simplequeue foreign producer"):
        runloom.run(4, main)
        dl = time.monotonic() + 10
        while not fdone[0] and time.monotonic() < dl:
            time.sleep(0.005)
    assert sorted(got) == list(range(N)), "lost/dup items across thread boundary"


# ==========================================================================
# 7. multiprocessing.Queue _feed-thread corpus -- spawn AND forkserver ONLY
#    (fork deadlocks under M:N per RUNTIME_GOTCHAS).  Run in a subprocess so a
#    crash is contained; the feed thread is a foreign daemon thread taking
#    patched SemLock/Condition.
# ==========================================================================
# NB: spawn/forkserver re-import the program's __main__ in the worker, so the
# target function MUST live at module top level of a real file -- a `-c` string
# has no importable __main__, so `child` is unpicklable there.  Write the corpus
# to a temp FILE and run THAT as the subprocess.
_MP_CORPUS_SCRIPT = '''
import sys
sys.path.insert(0, {src!r})
import runloom, runloom_c as rc, time
import runloom.monkey as monkey
monkey.patch()
import multiprocessing as mp
from runloom.sync import WaitGroup

def child(q, n):
    for i in range(n):
        q.put(("c", i))

def run_it(start_method):
    ctx = mp.get_context(start_method)
    q = ctx.Queue()
    N = 80
    p = ctx.Process(target=child, args=(q, N))
    box = {{"got": []}}
    def main():
        wg = WaitGroup(); wg.add(1)
        def consumer():
            try:
                for _ in range(N):
                    box["got"].append(q.get())
            finally:
                wg.done()
        p.start()
        rc.mn_fiber(consumer)
        wg.wait()
    runloom.run(4, main)
    p.join(timeout=15)
    return len(box["got"])

if __name__ == "__main__":
    got = run_it(sys.argv[1])
    assert got == 80, got
    print("OK", got)
'''


def _mp_corpus_child(start_method):
    import tempfile
    fd, path = tempfile.mkstemp(suffix="_mpcorpus.py")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(_MP_CORPUS_SCRIPT.format(src=_SRC))
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        env["PYTHONPATH"] = _SRC
        env["RUNLOOM_GOROUTINE_PANIC"] = "silent"
        p = subprocess.run([sys.executable, path, start_method],
                           capture_output=True, text=True, env=env, timeout=120)
        return p.returncode, (p.stdout + p.stderr)
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


@mn_only
def test_mp_queue_corpus_spawn():
    rc_, out = _mp_corpus_child("spawn")
    assert_no_signal_death(rc_, out, "mp-queue-spawn")
    assert "OK 80" in out, out


# A self-contained forkserver bootstrap probe: a trivial child, no queue.  Run
# in a subprocess with a HARD timeout so a hang is a bounded failure, never an
# infinite suite wedge.  Returns "OK" if Process.start()/join() completed.
_FORKSERVER_BOOTSTRAP_SCRIPT = '''
import sys, os
sys.path.insert(0, {src!r})
import runloom.monkey as monkey
monkey.patch()
import multiprocessing as mp

def trivial():
    pass

if __name__ == "__main__":
    ctx = mp.get_context("forkserver")
    p = ctx.Process(target=trivial)
    p.start()                 # <-- hangs here under monkey.patch()
    p.join(timeout=10)
    print("OK", p.exitcode)
'''


def _run_forkserver_bootstrap(timeout):
    import tempfile
    fd, path = tempfile.mkstemp(suffix="_fsboot.py")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(_FORKSERVER_BOOTSTRAP_SCRIPT.format(src=_SRC))
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        env["PYTHONPATH"] = _SRC
        env["RUNLOOM_GOROUTINE_PANIC"] = "silent"
        try:
            p = subprocess.run([sys.executable, path],
                               capture_output=True, text=True, env=env,
                               timeout=timeout)
            return p.returncode, (p.stdout + p.stderr), False
        except subprocess.TimeoutExpired as e:
            return None, (e.stdout or "") + (e.stderr or ""), True
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


@mn_only
# REGRESSION (was finding #4): monkey.patch() + multiprocessing forkserver no
# longer hangs at Process.start().  Root cause: _patched_open routed EVERY
# pollable-fd open through pure-Python _pyio, even off a fiber -- so a forked
# process with no runloom runtime (the forkserver child) wedged in the _pyio
# buffered reader while os.fdopen(pipe_fd)-reading its pickled process spec.
# _patched_open now uses the robust C io.open when not in a fiber (where _pyio
# gives no benefit anyway); the in-fiber cooperative pipe-read path is unchanged.
def test_mp_forkserver_bootstrap_should_not_hang():
    if "forkserver" not in __import__("multiprocessing").get_all_start_methods():
        pytest.skip("forkserver unavailable")
    # Asserts the CORRECT behaviour (start()/join() complete promptly).  It
    # currently HANGS -> the subprocess times out -> hung is True -> xfail.
    with hang_guard(60, "mp forkserver bootstrap"):
        rc_, out, hung = _run_forkserver_bootstrap(timeout=30)
    assert not hung, "forkserver bootstrap hung under monkey.patch()"
    assert_no_signal_death(rc_, out, "mp-forkserver-bootstrap")
    assert "OK" in out, out


# ==========================================================================
# 8. _at_fork_reinit on patched primitives (called by stdlib at-fork handlers).
# ==========================================================================
def test_at_fork_reinit_present_and_resets_lock():
    lk = threading.Lock()
    lk.acquire()
    assert lk.locked() is True
    lk._at_fork_reinit()              # child path: fresh unlocked mutex
    assert lk.locked() is False
    # event/condition reinit must not raise and must clear waiters
    ev = threading.Event(); ev.set()
    ev._at_fork_reinit()
    assert ev.is_set() is True        # flag survives a fork (per the code)
    cv = threading.Condition()
    cv._at_fork_reinit()              # no raise


def test_rlock_at_fork_reinit_resets_owner():
    rl = threading.RLock()
    out = {}
    def main():
        rl.acquire()
        rl._at_fork_reinit()
        out["owned"] = rl._is_owned()
        out["count"] = rl._recursion_count()
    rc.fiber(main); rc.run()
    assert out["owned"] is False
    assert out["count"] == 0


# ==========================================================================
# 9. DNS: IP literal, /etc/hosts, gaierror on bogus, AI_NUMERICHOST, family.
# ==========================================================================
def test_getaddrinfo_ipv4_literal_no_network():
    def f():
        return socket.getaddrinfo("127.0.0.1", 80, socket.AF_INET,
                                  socket.SOCK_STREAM)[0][4][0]
    with hang_guard(10, "getaddrinfo v4 literal"):
        assert _run_single(f) == "127.0.0.1"


def test_getaddrinfo_ipv6_literal_no_network():
    def f():
        res = socket.getaddrinfo("::1", 80, socket.AF_INET6, socket.SOCK_STREAM)
        return res[0][4][0]
    with hang_guard(10, "getaddrinfo v6 literal"):
        assert _run_single(f) == "::1"


def test_getaddrinfo_localhost_via_hosts_or_resolver():
    # localhost is in /etc/hosts on virtually every box; must resolve without a
    # network round trip and return a loopback address.
    def f():
        res = socket.getaddrinfo("localhost", 80, socket.AF_INET,
                                 socket.SOCK_STREAM)
        return [r[4][0] for r in res]
    with hang_guard(10, "getaddrinfo localhost"):
        addrs = _run_single(f)
    assert addrs, "localhost did not resolve"
    assert any(a.startswith("127.") for a in addrs), addrs


def test_getaddrinfo_ai_numerichost_on_name_raises_gaierror():
    def f():
        try:
            socket.getaddrinfo("localhost", 80, socket.AF_INET,
                               socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)
            return "no-error"
        except socket.gaierror as e:
            return e.args[0]
    with hang_guard(10, "getaddrinfo AI_NUMERICHOST"):
        r = _run_single(f)
    assert r == socket.EAI_NONAME, "AI_NUMERICHOST on a name must EAI_NONAME, got %r" % (r,)


def test_getaddrinfo_family_mismatch_literal_raises():
    def f():
        try:
            # ask for v6 but give a v4 literal
            socket.getaddrinfo("127.0.0.1", 80, socket.AF_INET6,
                               socket.SOCK_STREAM)
            return "no-error"
        except socket.gaierror as e:
            return e.args[0]
    with hang_guard(10, "getaddrinfo family mismatch"):
        r = _run_single(f)
    assert r == socket.EAI_FAMILY, r


def test_getaddrinfo_bogus_name_raises_gaierror_bounded():
    # A name that cannot resolve must raise gaierror, not hang.  Use a TLD that
    # does not exist so even a working resolver answers NXDOMAIN fast.
    def f():
        try:
            socket.getaddrinfo("nonexistent.invalid", 80, socket.AF_INET,
                               socket.SOCK_STREAM)
            return "no-error"
        except socket.gaierror:
            return "gaierror"
    with hang_guard(20, "getaddrinfo bogus name"):
        r = _run_single(f)
    assert r == "gaierror", "bogus name should raise gaierror, got %r" % (r,)


def test_gethostbyname_literal():
    def f():
        return socket.gethostbyname("127.0.0.1")
    with hang_guard(10, "gethostbyname literal"):
        assert _run_single(f) == "127.0.0.1"


# ==========================================================================
# 10. subprocess.Popen.wait cooperative + timeout -> TimeoutExpired -> kill.
# ==========================================================================
def test_subprocess_wait_cooperative_returns_code():
    out = {}
    def main():
        # a child that exits 7 quickly
        p = subprocess.Popen([sys.executable, "-c", "import sys; sys.exit(7)"])
        out["rc"] = p.wait()
    with hang_guard(30, "subprocess wait"):
        rc.fiber(main); rc.run()
    assert out["rc"] == 7


def test_subprocess_wait_yields_to_sibling():
    # While one fiber waits on a slow child, a sibling must keep running.
    out = {"sib": 0}
    def main():
        p = subprocess.Popen([sys.executable, "-c",
                              "import time; time.sleep(0.4)"])
        def sibling():
            for _ in range(50):
                out["sib"] += 1
                runloom.sleep(0.005)
        rc.fiber(sibling)
        out["rc"] = p.wait()
    with hang_guard(30, "subprocess wait yields"):
        rc.fiber(main); rc.run()
    assert out["rc"] == 0
    assert out["sib"] >= 30, "sibling starved while a fiber waited on a child: %d" % out["sib"]


def test_subprocess_wait_timeout_then_kill():
    out = {}
    def main():
        p = subprocess.Popen([sys.executable, "-c",
                              "import time; time.sleep(30)"])
        with assert_faster_than(3.0, "subprocess wait timeout"):
            try:
                p.wait(timeout=0.3)
                out["timed_out"] = False
            except subprocess.TimeoutExpired:
                out["timed_out"] = True
        p.kill()
        out["final_rc"] = p.wait()
    with hang_guard(40, "subprocess wait timeout+kill"):
        rc.fiber(main); rc.run()
    assert out["timed_out"] is True
    assert out["final_rc"] != 0   # killed by signal -> negative on POSIX


# ==========================================================================
# 11. selectors / select cooperative read-ready + timeout.
# ==========================================================================
def test_selectors_default_selector_read_ready():
    out = {}
    def main():
        a, b = _bare_socket_for_pair.socketpair()
        a.setblocking(False); b.setblocking(False)
        sel = selectors.DefaultSelector()
        sel.register(a, selectors.EVENT_READ)
        def writer():
            runloom.sleep(0.05)
            b.send(b"ping")
        rc.fiber(writer)
        events = sel.select(timeout=5.0)
        out["ready"] = bool(events)
        out["data"] = a.recv(8) if events else b""
        sel.close()
        a.close(); b.close()
    with hang_guard(20, "selectors read-ready"):
        rc.fiber(main); rc.run()
    assert out["ready"] is True
    assert out["data"] == b"ping"


def test_selectors_select_timeout_empty_bounded():
    out = {}
    def main():
        a, b = _bare_socket_for_pair.socketpair()
        a.setblocking(False); b.setblocking(False)
        sel = selectors.DefaultSelector()
        sel.register(a, selectors.EVENT_READ)
        with assert_faster_than(1.5, "selectors timeout"):
            out["events"] = sel.select(timeout=0.3)   # nothing written
        sel.close(); a.close(); b.close()
    with hang_guard(10, "selectors timeout empty"):
        rc.fiber(main); rc.run()
    assert out["events"] == []


def test_select_select_empty_lists_with_timeout_is_a_sleep():
    out = {}
    def main():
        with assert_faster_than(1.0, "select empty sleep"):
            t0 = time.monotonic()
            r = _select_mod.select([], [], [], 0.2)
            out["elapsed"] = time.monotonic() - t0
            out["r"] = r
    with hang_guard(10, "select empty sleep"):
        rc.fiber(main); rc.run()
    assert out["r"] == ([], [], [])
    assert out["elapsed"] >= 0.15, "select(timeout) returned too early: %.3f" % out["elapsed"]


def test_select_two_fds_read_ready_cooperative():
    out = {}
    def main():
        a, b = _bare_socket_for_pair.socketpair()
        c, d = _bare_socket_for_pair.socketpair()
        for s in (a, b, c, d):
            s.setblocking(False)
        def writer():
            runloom.sleep(0.05)
            d.send(b"x")
        rc.fiber(writer)
        r, w, x = _select_mod.select([a, c], [], [], 5.0)
        out["ready"] = c in r
        for s in (a, b, c, d):
            s.close()
    with hang_guard(20, "select two fds"):
        rc.fiber(main); rc.run()
    assert out["ready"] is True


# ==========================================================================
# 12. ssl cooperative handshake over a socketpair (no network).
# ==========================================================================
def _make_self_signed_cert():
    """Return (certfile, keyfile) temp paths, or None if we can't make one."""
    try:
        import tempfile
        from datetime import datetime, timedelta, timezone
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except Exception:
        return None
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(name).issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=1))
            .sign(key, hashes.SHA256()))
    import tempfile
    cf = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    kf = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    cf.write(cert.public_bytes(serialization.Encoding.PEM)); cf.close()
    kf.write(key.private_bytes(serialization.Encoding.PEM,
                               serialization.PrivateFormat.TraditionalOpenSSL,
                               serialization.NoEncryption())); kf.close()
    return cf.name, kf.name


# The SSL handshake parks on wait_fd for WANT_READ/WANT_WRITE on the socketpair
# fds; the SSLSocket close path does not run the monkey socket.close() netpoll
# unregister, so an in-process run leaves the socketpair fd NUMBERS arm-poisoned
# (the documented per-fd arm-cache staleness) and the NEXT socket test in the
# file reuses one of those fd numbers and hangs.  Running the whole handshake in
# a SUBPROCESS contains any fd-arm residue so it can't poison sibling tests
# (and a crash would be contained + observed as a negative returncode).
_SSL_HANDSHAKE_SCRIPT = '''
import sys, os
sys.path.insert(0, {src!r})
import runloom.monkey as monkey
monkey.patch()
import runloom, runloom_c as rc, socket, ssl
from datetime import datetime, timedelta, timezone
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import tempfile

def mint():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=1))
            .sign(key, hashes.SHA256()))
    cf = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    kf = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    cf.write(cert.public_bytes(serialization.Encoding.PEM)); cf.close()
    kf.write(key.private_bytes(serialization.Encoding.PEM,
             serialization.PrivateFormat.TraditionalOpenSSL,
             serialization.NoEncryption())); kf.close()
    return cf.name, kf.name

certfile, keyfile = mint()
out = {{}}
def main():
    sp_a, sp_b = socket.socketpair()
    sctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); sctx.load_cert_chain(certfile, keyfile)
    cctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    cctx.check_hostname = False; cctx.verify_mode = ssl.CERT_NONE
    def server():
        ss = sctx.wrap_socket(sp_a, server_side=True, do_handshake_on_connect=False)
        ss.do_handshake()                 # patched -> cooperative (wait_fd on WANT_*)
        data = ss.recv(64); ss.sendall(b"S:" + data)
        try: ss.unwrap()
        except Exception: pass
        sp_a.close()
    def client():
        cs = cctx.wrap_socket(sp_b, server_hostname="localhost", do_handshake_on_connect=False)
        cs.do_handshake()                 # patched -> cooperative
        cs.sendall(b"hello"); out["reply"] = cs.recv(64)
        try: cs.unwrap()
        except Exception: pass
        sp_b.close()
    rc.fiber(server); rc.fiber(client)
rc.fiber(main); rc.run()
os.unlink(certfile); os.unlink(keyfile)
assert out.get("reply") == b"S:hello", out
print("OK", out["reply"])
'''


def test_ssl_cooperative_handshake_over_socketpair():
    if _make_self_signed_cert() is None:
        pytest.skip("cryptography not available to mint a self-signed cert")
    import tempfile
    fd, path = tempfile.mkstemp(suffix="_sslhs.py")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(_SSL_HANDSHAKE_SCRIPT.format(src=_SRC))
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        env["PYTHONPATH"] = _SRC
        env["RUNLOOM_GOROUTINE_PANIC"] = "silent"
        with hang_guard(60, "ssl handshake subprocess"):
            p = subprocess.run([sys.executable, path], capture_output=True,
                               text=True, env=env, timeout=45)
        out = p.stdout + p.stderr
        assert_no_signal_death(p.returncode, out, "ssl-handshake")
        assert "OK b'S:hello'" in out, out
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# ==========================================================================
# 13. Cooperative socket echo under fault injection (FD + TCP sites): a clean
#     Python error is fine, a crash is not.  Drive a workload while injecting.
# ==========================================================================
def _socket_echo_once():
    """One in-fiber monkey-socket echo round trip; returns the reply or raises."""
    result = {}
    def main():
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        port = srv.getsockname()[1]
        def server():
            try:
                conn, _ = srv.accept()
                data = conn.recv(64)
                conn.sendall(b"echo:" + data)
                conn.close()
            except OSError as e:
                result["server_err"] = e
        def client():
            try:
                c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                c.connect(("127.0.0.1", port))
                c.sendall(b"hello")
                result["reply"] = c.recv(64)
                c.close()
            except OSError as e:
                result["client_err"] = e
            finally:
                srv.close()
        rc.fiber(server)
        rc.fiber(client)
    rc.fiber(main); rc.run()
    return result


def test_socket_echo_baseline():
    with hang_guard(20, "socket echo baseline"):
        r = _socket_echo_once()
    assert r.get("reply") == b"echo:hello", r


@pytest.mark.parametrize("site,spec", [
    ("RUNLOOM_FAULT_FD_READ",  "once:%d" % errno.EIO),
    ("RUNLOOM_FAULT_FD_WRITE", "once:%d" % errno.EIO),
    ("RUNLOOM_FAULT_SPAWN_G",  "once:%d" % errno.ENOMEM),
    ("RUNLOOM_FAULT_TCP_RECV", "once:%d" % errno.ECONNRESET),
])
def test_fault_injection_mid_monkey_workload_no_crash(site, spec):
    # Inject a single fault into the C I/O surface while a monkey socket + queue
    # + lock workload runs; assert the PROCESS does not die by signal.  A clean
    # Python OSError/RuntimeError is acceptable -- we only forbid a crash.
    rc_, out = run_child("""
        import socket, threading, queue, errno
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup

        def workload():
            lk = threading.Lock()
            q = queue.SimpleQueue()
            counter = [0]
            errs = []
            def producer():
                for i in range(50):
                    q.put(i)
            def consumer():
                for _ in range(50):
                    try:
                        q.get(timeout=2.0)
                    except queue.Empty:
                        break
                    with lk:
                        counter[0] += 1
            # plus a real socket echo to hit FD/TCP sites
            def echo():
                try:
                    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    srv.bind(("127.0.0.1", 0)); srv.listen(4)
                    port = srv.getsockname()[1]
                    def server():
                        try:
                            conn, _ = srv.accept()
                            d = conn.recv(64); conn.sendall(b"e:"+d); conn.close()
                        except OSError as e:
                            errs.append(("srv", e))
                    def client():
                        try:
                            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            c.connect(("127.0.0.1", port))
                            c.sendall(b"hi"); c.recv(64); c.close()
                        except OSError as e:
                            errs.append(("cli", e))
                        finally:
                            srv.close()
                    rc.fiber(server); rc.fiber(client)
                except OSError as e:
                    errs.append(("echo", e))
            def main():
                rc.fiber(consumer); rc.fiber(producer); rc.fiber(echo)
            try:
                rc.fiber(main); rc.run()
            except Exception as e:
                errs.append(("run", e))
            return errs
        e = workload()
        print("DONE errs=%d" % len(e))
    """, extra_env={site: spec}, timeout=60)
    assert_no_signal_death(rc_, out, "fault-%s" % site)
    assert "DONE" in out, out


def test_fault_injection_always_backs_off_bounded():
    # A persistent FD_READ fault must not busy-spin forever -- the workload
    # should surface a bounded error and finish, not hang.
    rc_, out = run_child("""
        import os, threading, queue
        import runloom, runloom_c as rc
        # use os.read on a pipe (cooperative) to hit FD_READ under always-fault
        def main():
            r, w = os.pipe()
            os.set_blocking(r, False); os.set_blocking(w, False)
            os.write(w, b"hello")
            try:
                data = os.read(r, 5)
                print("READ", data)
            except OSError as e:
                print("ERR", e.errno)
            os.close(r); os.close(w)
        rc.fiber(main); rc.run()
        print("DONE")
    """, extra_env={"RUNLOOM_FAULT_FD_READ": "always:%d" % errno.EIO},
       timeout=30)
    assert_no_signal_death(rc_, out, "fault-always-fd-read")
    assert "DONE" in out, out


# ==========================================================================
# 14. M:N env-gated stress: drive a monkey workload under sysmon / preempt /
#     handoff and assert no crash/hang (these detectors fire on a long fiber).
# ==========================================================================
@mn_only
@pytest.mark.parametrize("env", [
    {"RUNLOOM_SYSMON": "1", "RUNLOOM_SYSMON_QUIET": "1", "RUNLOOM_SYSMON_MS": "8"},
    {"RUNLOOM_PREEMPT": "1", "RUNLOOM_PREEMPT_MS": "8"},
    {"RUNLOOM_HANDOFF": "1", "RUNLOOM_HANDOFF_POOL": "2"},
])
def test_monkey_lock_workload_under_env_gated_mode(env):
    rc_, out = run_child("""
        import threading
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        lk = threading.Lock()
        counter = [0]
        N, ITERS = 24, 400
        def main():
            wg = WaitGroup(); wg.add(N)
            def w():
                try:
                    for _ in range(ITERS):
                        with lk:
                            counter[0] += 1
                        # a brief CPU burst so sysmon/preempt detectors trip
                        x = 0
                        for j in range(200):
                            x += j
                finally:
                    wg.done()
            for _ in range(N):
                rc.mn_fiber(w)
            wg.wait()
        runloom.run(4, main)
        assert counter[0] == N*ITERS, counter[0]
        print("OK", counter[0])
    """, extra_env=env, timeout=90)
    assert_no_signal_death(rc_, out, "env-gated-%s" % sorted(env)[0])
    assert "OK" in out, out


# ==========================================================================
# 15. Known-crash flags GATED OFF must warn + run the default scheduler, never
#     crash (we never set RUNLOOM_ALLOW_UNSAFE_MIGRATION).
# ==========================================================================
@mn_only
@pytest.mark.parametrize("flag", ["RUNLOOM_PER_G_TSTATE", "RUNLOOM_STEAL_WOKEN"])
def test_unsafe_migration_flag_gated_off_warns_not_crash(flag):
    rc_, out = run_child("""
        import threading
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        lk = threading.Lock()
        counter = [0]
        def main():
            wg = WaitGroup(); wg.add(8)
            def w():
                try:
                    for _ in range(300):
                        with lk:
                            counter[0] += 1
                finally:
                    wg.done()
            for _ in range(8):
                rc.mn_fiber(w)
            wg.wait()
        runloom.run(4, main)
        assert counter[0] == 8*300, counter[0]
        print("OK", counter[0])
    """, extra_env={flag: "1"}, timeout=60)   # NO RUNLOOM_ALLOW_UNSAFE_MIGRATION
    assert_no_signal_death(rc_, out, "gated-off-%s" % flag)
    assert "OK" in out, out


# ==========================================================================
# 16. Heavy offload + compile cooperative correctness under the patch.
# ==========================================================================
def test_heavy_hash_offload_yields_to_sibling():
    import hashlib
    out = {"sib": 0}
    big = b"x" * (512 * 1024)        # over the 256 KiB threshold -> offload
    # The patched sha256 keeps the original on __wrapped__; compute the stock
    # digest from it so the comparison can't tautologically pass.
    stock = getattr(hashlib.sha256, "__wrapped__", hashlib.sha256)
    expected = stock(big).hexdigest()

    def main():
        def sibling():
            for _ in range(40):
                out["sib"] += 1
                runloom.sleep(0.002)
        rc.fiber(sibling)
        out["digest"] = hashlib.sha256(big).hexdigest()
    with hang_guard(20, "heavy hash offload"):
        rc.fiber(main); rc.run()
    assert out["digest"] == expected, "offloaded hash differs from stock"
    assert out["sib"] >= 20, "sibling starved during a heavy offload: %d" % out["sib"]


def test_compile_in_fiber_offloads_and_returns_code_object():
    out = {}
    src = "a = " + " + ".join(str(i) for i in range(50))
    def main():
        code = compile(src, "<probe>", "exec")
        ns = {}
        exec(code, ns)
        out["a"] = ns["a"]
    with hang_guard(20, "compile in fiber"):
        rc.fiber(main); rc.run()
    assert out["a"] == sum(range(50))


# ==========================================================================
# ==========================================================================
# AUGMENTATION (adversarial critic pass): the gaps the first pass missed.
#
# The first pass was thorough on Lock/RLock/Semaphore/Event/Condition/Queue
# headlines + DNS happy paths + subprocess/selectors/ssl, but it SKIPPED:
#   - the SIGSEGV-class foreign-thread fallback for Semaphore / SimpleQueue /
#     RLock taken BEFORE any scheduler exists (only Lock + Condition were run
#     in a subprocess; the rest of the patched primitives a foreign thread can
#     reach were never crash-contained).
#   - os.read/os.write cooperative cross-thread correctness (only used as a
#     fault-injection vehicle, never asserted for data integrity + overlap).
#   - fcntl.flock / lockf cooperative mutual exclusion (subsystem-focus item,
#     ZERO coverage).
#   - UDP datagram recvfrom/sendto + sendmsg/recvmsg SCM_RIGHTS fd-passing
#     (subsystem-focus item, ZERO coverage).
#   - select.poll() object busy-poll (only select.select + selectors covered).
#   - signal.sigtimedwait cooperative bounded timeout (focus item, no coverage).
#   - time.sleep cooperative overlap (focus item, no coverage).
#   - concurrent.futures fiber-backed ThreadPoolExecutor (focus item, no cov).
#   - getnameinfo / gethostbyaddr / gethostbyname_ex reverse-DNS offload paths
#     (only forward getaddrinfo/gethostbyname covered).
#   - unpatch() round-trip + selective unpatch.
#   - Condition wait_for predicate + a notify(n) wake-exactly-n ACROSS the
#     foreign/fiber boundary (the first pass's notify(n) was single-thread).
#   - resource-exhaustion-scale Event fan-in (thousands of in-memory waiters)
#     with SET-EQUALITY integrity (not a count), to flush a lost wake at scale.
#   - BoundedSemaphore over-release WITH live waiters (the
#     _value+len(_waiters)+n bound branch the first pass never hit).
#   - builtins.open on a pollable pipe fd routed through cooperative _pyio.
#   - Lock timed-acquire from a FOREIGN thread (the foreign spin branch).
# Plus set-equality integrity wherever the first pass used a bare count.
# ==========================================================================
import collections as _collections


# --------------------------------------------------------------------------
# A1. SIGSEGV-class: EVERY patched primitive a foreign thread can reach, taken
#     from MANY foreign threads BEFORE any scheduler exists, concurrent with
#     M:N fibers on the SAME object.  The first pass only did this for Lock
#     + Condition; Semaphore / SimpleQueue / RLock are the untested crash
#     surface (each has its own foreign-thread fallback branch).  Subprocess-
#     contained so a SIGSEGV is a negative returncode, not a suite kill.
# --------------------------------------------------------------------------
def test_foreign_semaphore_no_scheduler_alloc_subprocess():
    rc_, out = run_child("""
        import threading, time, _thread
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        sem = threading.Semaphore(1)      # a mutex
        counter = [0]
        done = [0]
        dlk = _thread.allocate_lock()     # NOT patched? allocate_lock IS patched
        # use a bytearray slot per foreign thread to avoid its own race
        NF = 6
        fslots = bytearray(NF)
        def foreign(i):
            for _ in range(1500):
                sem.acquire(); counter[0] += 1; sem.release()
            fslots[i] = 1
        for i in range(NF):
            _thread.start_new_thread(foreign, (i,))
        def main():
            wg = WaitGroup(); wg.add(8)
            def w():
                try:
                    for _ in range(400):
                        with sem:
                            counter[0] += 1
                finally:
                    wg.done()
            for _ in range(8):
                rc.mn_fiber(w)
            wg.wait()
        runloom.run(4, main)
        dl = time.monotonic() + 20
        while sum(fslots) < NF and time.monotonic() < dl:
            time.sleep(0.005)
        assert sum(fslots) == NF, sum(fslots)
        assert counter[0] == NF*1500 + 8*400, counter[0]
        print("OK", counter[0])
    """, timeout=90)
    assert_no_signal_death(rc_, out, "foreign-semaphore-no-alloc")
    assert "OK" in out, out


def test_foreign_simplequeue_no_scheduler_alloc_subprocess():
    # A foreign thread putting AND a foreign thread getting a CoSimpleQueue with
    # no scheduler, concurrent with a fiber consumer.  put/get must fall
    # back to real OS blocking, never park a nonexistent fiber.
    rc_, out = run_child("""
        import queue, time, _thread
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        q = queue.SimpleQueue()
        N = 1200
        got_foreign = []
        fdone = [0]
        # foreign producer
        def producer():
            for i in range(N):
                q.put(i)
            for _ in range(2):
                q.put(None)     # sentinels: 1 foreign consumer + 1 fiber
            fdone[0] = 1
        # foreign consumer drains until a None
        def fconsumer():
            while True:
                v = q.get()
                if v is None:
                    break
                got_foreign.append(v)
            fdone[0] += 1
        _thread.start_new_thread(producer, ())
        _thread.start_new_thread(fconsumer, ())
        got_gor = []
        def main():
            wg = WaitGroup(); wg.add(1)
            def consumer():
                try:
                    while True:
                        v = q.get()
                        if v is None:
                            break
                        got_gor.append(v)
                finally:
                    wg.done()
            rc.mn_fiber(consumer)
            wg.wait()
        runloom.run(4, main)
        dl = time.monotonic() + 20
        while fdone[0] < 2 and time.monotonic() < dl:
            time.sleep(0.005)
        # every produced item delivered exactly once across the two consumers
        allgot = sorted(got_foreign + got_gor)
        assert allgot == list(range(N)), (len(allgot), allgot[:5], allgot[-5:])
        print("OK", len(allgot))
    """, timeout=90)
    assert_no_signal_death(rc_, out, "foreign-simplequeue-no-alloc")
    assert "OK" in out, out


def test_foreign_rlock_reentrant_no_scheduler_alloc_subprocess():
    # RLock identity uses get_ident() on a foreign thread; reentrant acquire on
    # the same thread must be granted, and must serialize vs M:N fibers.
    rc_, out = run_child("""
        import threading, time, _thread
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        rl = threading.RLock()
        counter = [0]
        NF = 5
        fslots = bytearray(NF)
        def foreign(i):
            for _ in range(1200):
                with rl:
                    with rl:
                        with rl:    # triple reentrant
                            counter[0] += 1
            fslots[i] = 1
        for i in range(NF):
            _thread.start_new_thread(foreign, (i,))
        def main():
            wg = WaitGroup(); wg.add(8)
            def w():
                try:
                    for _ in range(300):
                        with rl:
                            with rl:
                                counter[0] += 1
                finally:
                    wg.done()
            for _ in range(8):
                rc.mn_fiber(w)
            wg.wait()
        runloom.run(4, main)
        dl = time.monotonic() + 20
        while sum(fslots) < NF and time.monotonic() < dl:
            time.sleep(0.005)
        assert sum(fslots) == NF, sum(fslots)
        assert counter[0] == NF*1200 + 8*300, counter[0]
        print("OK", counter[0])
    """, timeout=90)
    assert_no_signal_death(rc_, out, "foreign-rlock-no-alloc")
    assert "OK" in out, out


# --------------------------------------------------------------------------
# A2. os.read / os.write cooperative across fibers on a pipe: data
#     integrity (every byte, in order) + cooperative overlap (the reader parks
#     on wait_fd while the writer makes progress).  Never tested directly.
# --------------------------------------------------------------------------
def test_os_read_write_pipe_cooperative_integrity_and_overlap():
    out = {}
    def main():
        r, w = os.pipe()
        os.set_blocking(r, False); os.set_blocking(w, False)
        CHUNKS = 50
        progress = {"writer": 0}
        def writer():
            for i in range(CHUNKS):
                # one byte per chunk, paced so the reader must PARK between them
                os.write(w, bytes([i % 256]))
                progress["writer"] += 1
                runloom.sleep(0.003)
            os.write(w, b"\xff")    # terminator distinct sentinel via length
            rc.netpoll_unregister(w)
            os.close(w)
        rc.fiber(writer)
        buf = bytearray()
        # read exactly CHUNKS+1 bytes back, parking between each
        while len(buf) < CHUNKS + 1:
            try:
                d = os.read(r, 64)
            except OSError:
                break
            if not d:
                break
            buf.extend(d)
        out["data"] = bytes(buf)
        out["writer_progress"] = progress["writer"]
        rc.netpoll_unregister(r)
        os.close(r)
    with hang_guard(20, "os.read/write pipe cooperative"):
        rc.fiber(main); rc.run()
    expected = bytes([i % 256 for i in range(50)]) + b"\xff"
    assert out["data"] == expected, "lost/reordered pipe bytes: %r" % out["data"][:20]
    assert out["writer_progress"] == 50


@mn_only
def test_os_write_foreign_thread_os_read_fiber_subprocess():
    # os.write from a FOREIGN thread (no fiber -> passthrough _orig_os_write on
    # a nonblocking fd), os.read from a fiber (cooperative wait_fd).  The
    # foreign side must not crash and every byte must arrive in order.
    rc_, out = run_child("""
        import os, time, _thread
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        r, w = os.pipe()
        os.set_blocking(r, False); os.set_blocking(w, False)
        N = 200
        fdone = [0]
        def foreign_writer():
            sent = 0
            while sent < N:
                try:
                    os.write(w, bytes([sent % 256]))
                    sent += 1
                except BlockingIOError:
                    time.sleep(0.001)
                if sent % 16 == 0:
                    time.sleep(0.001)
            fdone[0] = 1
        _thread.start_new_thread(foreign_writer, ())
        got = bytearray()
        def main():
            wg = WaitGroup(); wg.add(1)
            def reader():
                try:
                    while len(got) < N:
                        try:
                            d = os.read(r, 64)
                        except OSError:
                            break
                        if not d:
                            break
                        got.extend(d)
                finally:
                    wg.done()
            rc.mn_fiber(reader)
            wg.wait()
        runloom.run(4, main)
        dl = time.monotonic() + 15
        while fdone[0] == 0 and time.monotonic() < dl:
            time.sleep(0.005)
        rc.netpoll_unregister(r); rc.netpoll_unregister(w)
        os.close(r); os.close(w)
        assert bytes(got) == bytes([i % 256 for i in range(N)]), len(got)
        print("OK", len(got))
    """, timeout=60)
    assert_no_signal_death(rc_, out, "os-write-foreign-read-fiber")
    assert "OK" in out, out


# --------------------------------------------------------------------------
# A3. fcntl.flock cooperative mutual exclusion (subsystem-focus item, ZERO
#     coverage in the first pass).  A blocking LOCK_EX inside a fiber must park
#     cooperatively (LOCK_NB + backoff) so a holder yields its hub thread; the
#     guarded region must be mutually exclusive.  flock is per-OPEN-FILE-
#     DESCRIPTION, so contend across SEPARATE open() fds of the same path.
# --------------------------------------------------------------------------
def test_fcntl_flock_cooperative_mutual_exclusion():
    import tempfile
    try:
        import fcntl
    except ImportError:
        pytest.skip("no fcntl")
    path = tempfile.mktemp(suffix="_flock")
    open(path, "w").close()
    inside = [0]
    peak = [0]
    overlaps = [0]
    N = 8
    out = {}
    def main():
        wg = WaitGroup(); wg.add(N)
        def worker():
            try:
                # each worker has its OWN open fd so flock actually contends
                fd = os.open(path, os.O_RDWR)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX)   # cooperative block
                    inside[0] += 1
                    if inside[0] > 1:
                        overlaps[0] += 1
                    if inside[0] > peak[0]:
                        peak[0] = inside[0]
                    runloom.sleep(0.005)
                    inside[0] -= 1
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    os.close(fd)
            finally:
                wg.done()
        for _ in range(N):
            rc.fiber(worker)
        wg.wait()
    with hang_guard(40, "fcntl flock mutual exclusion"):
        rc.fiber(main); rc.run()
    try:
        os.unlink(path)
    except OSError:
        pass
    assert peak[0] == 1, "flock let %d hold the lock at once" % peak[0]
    assert overlaps[0] == 0, "flock mutual exclusion violated"


def test_fcntl_flock_yields_to_sibling_while_contended():
    # While one fiber holds flock(LOCK_EX) and a second is parked waiting, an
    # unrelated sibling must keep running -- proves the wait is a cooperative
    # backoff park, not an OS-thread freeze.
    import tempfile
    try:
        import fcntl
    except ImportError:
        pytest.skip("no fcntl")
    path = tempfile.mktemp(suffix="_flock2")
    open(path, "w").close()
    out = {"sib": 0}
    def main():
        fd1 = os.open(path, os.O_RDWR)
        fcntl.flock(fd1, fcntl.LOCK_EX)   # held by main
        waiter_got = [False]
        def waiter():
            fd2 = os.open(path, os.O_RDWR)
            fcntl.flock(fd2, fcntl.LOCK_EX)   # parks (contended) until main unlocks
            waiter_got[0] = True
            fcntl.flock(fd2, fcntl.LOCK_UN)
            os.close(fd2)
        def sibling():
            for _ in range(40):
                out["sib"] += 1
                runloom.sleep(0.003)
        rc.fiber(waiter)
        rc.fiber(sibling)
        # hold the lock a while so the waiter must park and the sibling must run
        runloom.sleep(0.2)
        fcntl.flock(fd1, fcntl.LOCK_UN)
        os.close(fd1)
        # give the waiter time to acquire
        for _ in range(50):
            if waiter_got[0]:
                break
            runloom.sleep(0.005)
        out["waiter_got"] = waiter_got[0]
    with hang_guard(30, "fcntl flock yields"):
        rc.fiber(main); rc.run()
    try:
        os.unlink(path)
    except OSError:
        pass
    assert out["waiter_got"] is True, "contended flock waiter never acquired"
    assert out["sib"] >= 20, "sibling starved during a contended flock: %d" % out["sib"]


# --------------------------------------------------------------------------
# A4. UDP datagram recvfrom/sendto cooperative across fibers (subsystem-
#     focus item, ZERO coverage).  A receiver parks on wait_fd; a sender on a
#     sibling fiber delivers; data + peer address must be correct.
# --------------------------------------------------------------------------
def test_udp_recvfrom_sendto_cooperative():
    out = {}
    def main():
        srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        srv.setblocking(False)
        srv.bind(("127.0.0.1", 0))
        port = srv.getsockname()[1]
        def sender():
            runloom.sleep(0.04)
            cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            cli.setblocking(False)
            cli.sendto(b"datagram-payload", ("127.0.0.1", port))
            rc.netpoll_unregister(cli.fileno())
            cli.close()
        rc.fiber(sender)
        data, addr = srv.recvfrom(64)       # parks cooperatively on wait_fd
        out["data"] = data
        out["from_loopback"] = addr[0] == "127.0.0.1"
        rc.netpoll_unregister(srv.fileno())
        srv.close()
    with hang_guard(20, "udp recvfrom/sendto"):
        rc.fiber(main); rc.run()
    assert out["data"] == b"datagram-payload", out
    assert out["from_loopback"] is True


# FINDING: socket.recvfrom / sendto / recvmsg / recvmsg_into / sendmsg /
# recvfrom_into IGNORE the socket timeout -- unlike recv/recv_into/send/sendall/
# connect/accept (which honor gettimeout() via the `t is not None` deadline
# branch), the datagram + msg variants park on a bare `wait_fd(fd, READ)` with
# NO deadline (src/runloom/monkey/sockets.py _patched_recvfrom etc.).  So a
# datagram socket with settimeout(0.2) that never receives a packet HANGS the
# fiber FOREVER instead of raising socket.timeout.  Lost-deadline class.  Run in
# a SUBPROCESS with a hard 6s timeout so the hang is bounded (it cannot wedge
# the suite), asserting the CORRECT behaviour (a bounded timeout) -- which
# currently fails because the subprocess is killed for hanging.
_UDP_TIMEOUT_PROBE = '''
import sys
sys.path.insert(0, {src!r})
import runloom, runloom_c as rc, socket, time
import runloom.monkey as monkey
monkey.patch()
out = {{}}
def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.bind(("127.0.0.1", 0))
    srv.settimeout(0.2)
    try:
        srv.recvfrom(64)              # should raise socket.timeout at ~0.2s
        out["r"] = "got"
    except (socket.timeout, OSError):
        out["r"] = "timeout"
    rc.netpoll_unregister(srv.fileno()); srv.close()
rc.fiber(main); rc.run()
print("RESULT", out)
'''


# REGRESSION (was finding #5): the cooperative socket layer now honors
# settimeout across recvfrom/sendto/recvmsg/recvmsg_into/sendmsg/recvfrom_into
# (and accept).  Root cause was deeper than the datagram methods: _make_nonblocking's
# setblocking(False) zeroed the live gettimeout() before any op read it (socket.socket
# has no __dict__ to stash on), so the WHOLE layer ignored settimeout.  Fixed by a
# per-fd side table populated before setblocking(False) and read by _coop_timeout;
# the datagram/msg/accept ops route their park through the timeout-aware _wait_io.
def test_udp_recvfrom_honors_socket_timeout_bounded():
    import tempfile
    fd, path = tempfile.mkstemp(suffix="_udpto.py")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(_UDP_TIMEOUT_PROBE.format(src=_SRC))
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"; env["PYTHONPATH"] = _SRC
        env["RUNLOOM_GOROUTINE_PANIC"] = "silent"
        hung = False
        try:
            p = subprocess.run([sys.executable, path], capture_output=True,
                               text=True, env=env, timeout=6)
            out = p.stdout + p.stderr
        except subprocess.TimeoutExpired:
            hung = True
            out = ""
        # CORRECT behaviour: it returns promptly with a timeout result.  Today
        # it hangs -> the subprocess is killed -> assertion fails -> xfail.
        assert not hung, "recvfrom ignored settimeout and HUNG (lost-deadline bug)"
        assert "RESULT" in out and "'r': 'timeout'" in out, out
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# --------------------------------------------------------------------------
# A5. sendmsg / recvmsg SCM_RIGHTS fd passing over an AF_UNIX socketpair
#     (subsystem-focus item, ZERO coverage).  The cooperative recvmsg must park
#     on wait_fd and deliver the passed fd intact; reading the dup'd fd must see
#     the data written through the original.
# --------------------------------------------------------------------------
def test_sendmsg_recvmsg_scm_rights_fd_passing():
    if not (hasattr(socket.socket, "recvmsg") and hasattr(socket.socket, "sendmsg")):
        pytest.skip("no recvmsg/sendmsg on this platform")
    import array
    out = {}
    def main():
        a, b = _bare_socket_for_pair.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        a.setblocking(False); b.setblocking(False)
        # a pipe whose READ end we pass across the socket
        pr, pw = os.pipe()
        os.write(pw, b"through-the-pipe")
        os.close(pw)
        def sender():
            runloom.sleep(0.03)
            fds = array.array("i", [pr])
            a.sendmsg([b"M"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)])
        rc.fiber(sender)
        fds_buf = array.array("i", [0])
        msg, anc, flags, addr = b.recvmsg(64, socket.CMSG_LEN(fds_buf.itemsize))
        out["msg"] = msg
        got_fd = None
        for level, ctype, cdata in anc:
            if level == socket.SOL_SOCKET and ctype == socket.SCM_RIGHTS:
                fds_buf = array.array("i")
                fds_buf.frombytes(cdata[:len(cdata) - (len(cdata) % fds_buf.itemsize)])
                got_fd = fds_buf[0]
        out["passed_data"] = os.read(got_fd, 64) if got_fd is not None else None
        if got_fd is not None:
            os.close(got_fd)
        os.close(pr)
        for s in (a, b):
            rc.netpoll_unregister(s.fileno())
            s.close()
    with hang_guard(20, "sendmsg/recvmsg scm_rights"):
        rc.fiber(main); rc.run()
    assert out["msg"] == b"M", out
    assert out["passed_data"] == b"through-the-pipe", out


# --------------------------------------------------------------------------
# A6. select.poll() object cooperative (busy-poll path).  The first pass tested
#     select.select + selectors.DefaultSelector but NOT a raw poll() object,
#     whose .poll(timeout) is a probe+yield busy loop with no backing fd.
# --------------------------------------------------------------------------
def test_select_poll_object_read_ready_cooperative():
    if not hasattr(_select_mod, "poll"):
        pytest.skip("no select.poll")
    out = {}
    def main():
        a, b = _bare_socket_for_pair.socketpair()
        a.setblocking(False); b.setblocking(False)
        po = _select_mod.poll()
        po.register(a.fileno(), _select_mod.POLLIN)
        def writer():
            runloom.sleep(0.05)
            b.send(b"poke")
        rc.fiber(writer)
        events = po.poll(5000)             # ms; cooperative busy-poll
        out["ready"] = bool(events)
        out["data"] = a.recv(8) if events else b""
        po.unregister(a.fileno())
        rc.netpoll_unregister(a.fileno())
        rc.netpoll_unregister(b.fileno())
        a.close(); b.close()
    with hang_guard(20, "select.poll object read-ready"):
        rc.fiber(main); rc.run()
    assert out["ready"] is True
    assert out["data"] == b"poke"


def test_select_poll_object_timeout_bounded():
    if not hasattr(_select_mod, "poll"):
        pytest.skip("no select.poll")
    out = {}
    def main():
        a, b = _bare_socket_for_pair.socketpair()
        a.setblocking(False); b.setblocking(False)
        po = _select_mod.poll()
        po.register(a.fileno(), _select_mod.POLLIN)
        with assert_faster_than(1.5, "poll timeout"):
            out["events"] = po.poll(300)   # ms, nothing written
        po.unregister(a.fileno())
        rc.netpoll_unregister(a.fileno())
        rc.netpoll_unregister(b.fileno())
        a.close(); b.close()
    with hang_guard(10, "select.poll timeout"):
        rc.fiber(main); rc.run()
    assert out["events"] == []


# --------------------------------------------------------------------------
# A7. signal.sigtimedwait cooperative bounded timeout (focus item, no cov).
#     With the signal blocked, a zero-arrival sigtimedwait must return None at
#     the deadline, cooperatively (not freeze the hub).
# --------------------------------------------------------------------------
def test_signal_sigtimedwait_bounded_timeout():
    if not hasattr(_signal, "sigtimedwait") or not hasattr(_signal, "pthread_sigmask"):
        pytest.skip("no sigtimedwait/pthread_sigmask")
    out = {}
    def main():
        # Block SIGUSR1 so it queues pending rather than running a handler.
        _signal.pthread_sigmask(_signal.SIG_BLOCK, {_signal.SIGUSR1})
        # sibling proves cooperative overlap during the wait
        out["sib"] = 0
        def sibling():
            for _ in range(20):
                out["sib"] += 1
                runloom.sleep(0.005)
        rc.fiber(sibling)
        with assert_faster_than(1.5, "sigtimedwait timeout"):
            out["r"] = _signal.sigtimedwait({_signal.SIGUSR1}, 0.25)
    with hang_guard(10, "sigtimedwait timeout"):
        rc.fiber(main); rc.run()
    # restore mask on the main thread
    try:
        _signal.pthread_sigmask(_signal.SIG_UNBLOCK, {_signal.SIGUSR1})
    except Exception:
        pass
    assert out["r"] is None, "sigtimedwait should time out to None, got %r" % (out["r"],)
    assert out["sib"] >= 10, "sigtimedwait starved a sibling: %d" % out["sib"]


def test_signal_sigtimedwait_reaps_pending_signal():
    # A signal already pending (blocked + raised) must be reaped by the
    # cooperative sigtimedwait, returning its siginfo.
    if not hasattr(_signal, "sigtimedwait") or not hasattr(_signal, "pthread_sigmask"):
        pytest.skip("no sigtimedwait/pthread_sigmask")
    out = {}
    def main():
        _signal.pthread_sigmask(_signal.SIG_BLOCK, {_signal.SIGUSR1})
        _signal.raise_signal(_signal.SIGUSR1)   # now pending
        info = _signal.sigtimedwait({_signal.SIGUSR1}, 1.0)
        out["signo"] = None if info is None else info.si_signo
    with hang_guard(10, "sigtimedwait reap"):
        rc.fiber(main); rc.run()
    try:
        _signal.pthread_sigmask(_signal.SIG_UNBLOCK, {_signal.SIGUSR1})
    except Exception:
        pass
    assert out["signo"] == _signal.SIGUSR1, out


# --------------------------------------------------------------------------
# A8. time.sleep cooperative overlap (focus item, no cov).  N fibers each
#     sleeping S must finish in ~S total (overlapped), not N*S (serialized).
# --------------------------------------------------------------------------
def test_time_sleep_cooperative_overlap_not_serialized():
    out = {"done": 0}
    N = 20
    S = 0.1
    def main():
        wg = WaitGroup(); wg.add(N)
        def w():
            try:
                time.sleep(S)        # patched -> cooperative
                out["done"] += 1
            finally:
                wg.done()
        # all N sleeps must overlap: wall time ~S, not N*S
        with assert_faster_than(N * S * 0.5, "time.sleep overlap"):
            for _ in range(N):
                rc.fiber(w)
            wg.wait()
    with hang_guard(15, "time.sleep overlap"):
        rc.fiber(main); rc.run()
    assert out["done"] == N


# --------------------------------------------------------------------------
# A9. concurrent.futures fiber-backed ThreadPoolExecutor (focus item, no cov).
#     submit() runs work as fibers; Future.result resolves in-domain; map
#     preserves order; max_workers bounds concurrency.
# --------------------------------------------------------------------------
def test_futures_threadpool_submit_and_result_in_fiber():
    import concurrent.futures as cf
    out = {}
    def main():
        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(lambda i=i: i * i) for i in range(16)]
            out["results"] = [f.result() for f in futs]
    with hang_guard(20, "futures submit/result"):
        rc.fiber(main); rc.run()
    assert out["results"] == [i * i for i in range(16)], out


def test_futures_threadpool_map_preserves_order():
    import concurrent.futures as cf
    out = {}
    def main():
        with cf.ThreadPoolExecutor(max_workers=3) as ex:
            # cooperative work that yields so ordering can't be trivially serial
            def work(i):
                runloom.sleep(0.002 * (5 - (i % 5)))
                return i + 100
            out["mapped"] = list(ex.map(work, range(12)))
    with hang_guard(20, "futures map order"):
        rc.fiber(main); rc.run()
    assert out["mapped"] == [i + 100 for i in range(12)], out


def test_futures_threadpool_max_workers_caps_concurrency():
    import concurrent.futures as cf
    out = {}
    live = [0]; peak = [0]
    def main():
        def work(_):
            live[0] += 1
            if live[0] > peak[0]:
                peak[0] = live[0]
            runloom.sleep(0.01)
            live[0] -= 1
            return 1
        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(work, i) for i in range(20)]
            out["sum"] = sum(f.result() for f in futs)
    with hang_guard(20, "futures max_workers cap"):
        rc.fiber(main); rc.run()
    assert out["sum"] == 20
    assert peak[0] <= 2, "ThreadPoolExecutor(max_workers=2) ran %d at once" % peak[0]


def test_futures_threadpool_propagates_exception():
    import concurrent.futures as cf
    out = {}
    def main():
        with cf.ThreadPoolExecutor(max_workers=2) as ex:
            f = ex.submit(lambda: 1 / 0)
            try:
                f.result()
                out["raised"] = False
            except ZeroDivisionError:
                out["raised"] = True
    with hang_guard(20, "futures exception"):
        rc.fiber(main); rc.run()
    assert out["raised"] is True


# --------------------------------------------------------------------------
# A10. reverse-DNS / offload DNS paths: getnameinfo, gethostbyaddr,
#      gethostbyname_ex on a literal (these all offload to the backend pool;
#      they must park the fiber + return correct data, never freeze the hub).
# --------------------------------------------------------------------------
def test_getnameinfo_loopback_literal():
    def f():
        # NUMERICHOST|NUMERICSERV avoids any reverse lookup -> deterministic,
        # but still exercises the offloaded getnameinfo path.
        return socket.getnameinfo(("127.0.0.1", 80),
                                  socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
    with hang_guard(15, "getnameinfo literal"):
        host, serv = _run_single(f)
    assert host == "127.0.0.1", (host, serv)
    assert serv == "80", (host, serv)


def test_getnameinfo_yields_to_sibling():
    # The offloaded getnameinfo must let a sibling fiber run while it parks.
    out = {"sib": 0}
    def main():
        def sibling():
            for _ in range(30):
                out["sib"] += 1
                runloom.sleep(0.002)
        rc.fiber(sibling)
        out["res"] = socket.getnameinfo(("127.0.0.1", 22),
                                        socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
    with hang_guard(15, "getnameinfo yields"):
        rc.fiber(main); rc.run()
    assert out["res"][0] == "127.0.0.1"
    assert out["sib"] >= 10, "getnameinfo offload starved a sibling: %d" % out["sib"]


def test_gethostbyname_ex_localhost():
    def f():
        name, aliases, addrs = socket.gethostbyname_ex("localhost")
        return addrs
    with hang_guard(15, "gethostbyname_ex localhost"):
        addrs = _run_single(f)
    assert any(a.startswith("127.") for a in addrs), addrs


def test_gethostbyaddr_loopback():
    def f():
        try:
            name, aliases, addrs = socket.gethostbyaddr("127.0.0.1")
            return ("ok", addrs)
        except (socket.herror, socket.gaierror, OSError) as e:
            # reverse PTR may not resolve on a minimal box -- the point is the
            # OFFLOADED call returns cleanly (no hang/crash), error or not.
            return ("err", type(e).__name__)
    with hang_guard(20, "gethostbyaddr loopback"):
        kind, val = _run_single(f)
    if kind == "ok":
        assert "127.0.0.1" in val, val
    else:
        assert kind == "err"


# --------------------------------------------------------------------------
# A11. unpatch() round-trip + selective unpatch.  The first pass only tested
#      patch() idempotency; it never reversed a category and re-applied it.
#      We restore everything at the end so the rest of the file stays patched.
# --------------------------------------------------------------------------
def test_unpatch_selective_then_repatch_restores_cooperative_type():
    # threading is the most load-bearing category; reverse it, confirm the real
    # stdlib type returns, then re-patch and confirm the cooperative type.
    assert type(threading.Lock()).__module__.startswith("runloom")
    monkey.unpatch(threading=True)
    try:
        # after unpatch, a fresh Lock is the real _thread.lock
        lk = threading.Lock()
        assert not type(lk).__module__.startswith("runloom"), type(lk)
        assert "_thread" in type(lk).__module__ or type(lk).__name__ == "lock"
    finally:
        monkey.patch(threading=True)     # restore for the rest of the file
    assert type(threading.Lock()).__module__.startswith("runloom")
    # and the cooperative type still works after the round trip
    lk2 = threading.Lock()
    lk2.acquire()
    assert lk2.locked() is True
    lk2.release()
    assert lk2.locked() is False


def test_unpatch_unknown_category_raises_after_repatch():
    with pytest.raises(TypeError):
        monkey.unpatch(definitely_not_a_category=True)


# --------------------------------------------------------------------------
# A12. Condition.wait_for predicate + a notify(n) that wakes EXACTLY n across
#      the foreign/fiber boundary.  The first pass's notify(n) was single-
#      thread only; a cross-thread notify(n) is the harder REORDER/lost-wake.
# --------------------------------------------------------------------------
def test_condition_wait_for_predicate_satisfied_by_notify():
    cv = threading.Condition()
    state = {"ready": False}
    out = {}
    def main():
        def waiter():
            with cv:
                out["r"] = cv.wait_for(lambda: state["ready"], timeout=5.0)
        rc.fiber(waiter)
        rc.sched_yield()
        runloom.sleep(0.05)
        with cv:
            state["ready"] = True
            cv.notify_all()
    with hang_guard(20, "condition wait_for"):
        rc.fiber(main); rc.run()
    assert out["r"] is True


def test_condition_wait_for_predicate_times_out_bounded():
    cv = threading.Condition()
    out = {}
    def main():
        with cv:
            with assert_faster_than(1.5, "wait_for timeout"):
                out["r"] = cv.wait_for(lambda: False, timeout=0.3)
    with hang_guard(10, "condition wait_for timeout"):
        rc.fiber(main); rc.run()
    assert out["r"] is False


@mn_only
def test_condition_notify_n_exactly_n_mixed_foreign_fiber():
    # NF foreign threads + NG fibers all park on one Condition; a single
    # notify(K) must wake EXACTLY K of them (REORDER / over-wake hunt).  We
    # cannot tell which K wake, so we assert the TOTAL woken == K after one
    # notify(K), then notify_all the rest so nothing hangs.
    cv = threading.Condition()
    NF, NG = 3, 5
    woke = bytearray(NF + NG)          # one slot each, race-free
    parked = [0]
    pk_lk = _real_thread_mod.allocate_lock()

    def foreign(slot):
        with cv:
            with pk_lk:
                parked[0] += 1
            cv.wait()
            woke[slot] = 1

    for i in range(NF):
        _real_thread_mod.start_new_thread(foreign, (i,))

    def main():
        wg = WaitGroup(); wg.add(NG)
        def make(slot):
            def g():
                try:
                    with cv:
                        with pk_lk:
                            parked[0] += 1
                        cv.wait()
                        woke[slot] = 1
                finally:
                    wg.done()
            return g
        for j in range(NG):
            rc.mn_fiber(make(NF + j))
        # wait until ALL NF+NG are parked
        dl = time.monotonic() + 8
        while parked[0] < NF + NG and time.monotonic() < dl:
            rc.sched_yield()
            runloom.sleep(0.005)
        K = 4
        with cv:
            cv.notify(K)
        # let exactly-K propagate
        runloom.sleep(0.3)
        woken_after_k = sum(woke)
        # now release the rest so foreign threads + remaining fibers finish
        with cv:
            cv.notify_all()
        wg.wait()
        return woken_after_k

    with hang_guard(40, "condition notify(n) mixed"):
        woken_after_k = _run_mn(main, n=4)
        # let foreign waiters finish their wake
        dl = time.monotonic() + 8
        while sum(woke) < NF + NG and time.monotonic() < dl:
            time.sleep(0.005)
    assert sum(woke) == NF + NG, "not all waiters woke: %d != %d" % (sum(woke), NF + NG)
    assert woken_after_k == 4, (
        "notify(4) woke %d (REORDER/over-wake/lost-wake)" % woken_after_k)


# --------------------------------------------------------------------------
# A13. resource-exhaustion-scale Event fan-in with SET-EQUALITY integrity.
#      Thousands of in-memory parkers woken by ONE set() -- a lost wake at
#      scale (the documented edge-before-park / commit-CAS window) shows up
#      here, and we assert the EXACT set of indices, not just a count.
# --------------------------------------------------------------------------
def test_event_fanin_scale_set_equality_single_thread():
    ev = threading.Event()
    N = 3000
    woke = bytearray(N)               # one writer per slot -> race-free
    def main():
        wg = WaitGroup(); wg.add(N)
        def make(i):
            def w():
                try:
                    ev.wait()
                    woke[i] = 1
                finally:
                    wg.done()
            return w
        for i in range(N):
            rc.fiber(make(i))
        # let them all park
        for _ in range(5):
            rc.sched_yield()
        ev.set()                      # ONE set wakes all N
        wg.wait()
    with hang_guard(60, "event fan-in scale 3000"):
        rc.fiber(main); rc.run()
    missing = [i for i in range(N) if not woke[i]]
    assert not missing, "lost wake for %d/%d waiters (e.g. %r)" % (
        len(missing), N, missing[:10])


# --------------------------------------------------------------------------
# A14. BoundedSemaphore over-release WITH live waiters -- the
#      _value + len(_waiters) + n > _initial bound branch the first pass's
#      over-release test (no waiters) never reached.
# --------------------------------------------------------------------------
def test_bounded_semaphore_over_release_counts_pending_waiter():
    # The CoBoundedSemaphore bound check is value + len(waiters) + n > initial:
    # a waiter holding a *pending* permit counts toward the bound.  With
    # initial=2, two permits acquired, and ONE waiter queued, a release that
    # would hand the waiter a permit is legal (value 0 + waiters 1 + 1 == 2 ==
    # initial), but a SECOND concurrent release exceeds the bound and must
    # raise -- the with-waiter branch the first pass's no-waiter test missed.
    out = {}
    def main():
        bs = threading.BoundedSemaphore(2)
        bs.acquire(); bs.acquire()         # value now 0, initial 2
        parked = [False]
        gave_back = [False]
        def waiter():
            parked[0] = True
            bs.acquire()                   # parks: no permit yet
            gave_back[0] = True            # got a permit eventually
        rc.fiber(waiter)
        while not parked[0]:
            rc.sched_yield()
        runloom.sleep(0.02)
        # state: value 0, waiters 1, initial 2.
        # release(1): 0 + 1 + 1 == 2, NOT > 2 -> legal, hands waiter its permit.
        bs.release()
        runloom.sleep(0.05)                # let the waiter wake + take the permit
        # now value 0, waiters 0; bring it back to the bound, then over-release.
        bs.release()                       # value 1
        bs.release()                       # value 2 == initial
        try:
            bs.release()                   # 2 + 0 + 1 > 2 -> must raise
            out["raised"] = False
        except ValueError:
            out["raised"] = True
        out["gave_back"] = gave_back[0]
    with hang_guard(20, "bounded semaphore over-release with waiter"):
        rc.fiber(main); rc.run()
    assert out["gave_back"] is True, "waiter never got its handed-off permit"
    assert out["raised"] is True, "over-release past the bound did not raise"


# --------------------------------------------------------------------------
# A15. builtins.open on a pollable PIPE fd routes through cooperative _pyio
#      (so a buffered .read() parks instead of wedging the hub).  Never tested.
# --------------------------------------------------------------------------
def test_open_pollable_pipe_fd_buffered_read_is_cooperative():
    out = {"sib": 0}
    def main():
        r, w = os.pipe()
        os.set_blocking(r, False)
        # open the READ end as a buffered text file object -> _pyio (cooperative)
        f = open(r, "rb", buffering=0)
        def feeder():
            for _ in range(5):
                runloom.sleep(0.01)
                out["sib"] += 1
            os.write(w, b"buffered-payload")
            os.close(w)
        rc.fiber(feeder)
        # this read must PARK cooperatively (the feeder sibling must advance)
        data = f.read(64)
        out["data"] = data
        out["sib_at_read"] = out["sib"]
        try:
            rc.netpoll_unregister(r)
        except Exception:
            pass
        f.close()
    with hang_guard(20, "open pollable pipe buffered read"):
        rc.fiber(main); rc.run()
    assert out["data"] == b"buffered-payload", out
    assert out["sib_at_read"] >= 3, (
        "buffered .read() on a pipe did NOT yield -- it wedged the hub "
        "(sib advanced only %d)" % out["sib_at_read"])


# --------------------------------------------------------------------------
# A16. Lock timed-acquire FROM A FOREIGN THREAD (the foreign-thread spin branch
#      of CoLock.acquire -- t0/timeout/_raw_time_sleep).  Never exercised: the
#      first pass's foreign-thread lock tests all used blocking acquire.
# --------------------------------------------------------------------------
@mn_only
def test_lock_timed_acquire_from_foreign_thread_bounded():
    rc_, out = run_child("""
        import threading, time, _thread
        import runloom, runloom_c as rc
        from runloom.sync import WaitGroup
        lk = threading.Lock()
        res = {}
        # A fiber grabs the lock and holds it a while; a foreign thread then
        # does a TIMED acquire that must return False at its deadline (the
        # foreign spin branch), bounded.
        held = [False]; release_it = [False]
        def main():
            wg = WaitGroup(); wg.add(1)
            def holder():
                try:
                    lk.acquire()
                    held[0] = True
                    while not release_it[0]:
                        runloom.sleep(0.005)
                    lk.release()
                finally:
                    wg.done()
            rc.mn_fiber(holder)
            # wait for the foreign thread to do its timed probe, then release
            dl = time.monotonic() + 5
            while not res.get('foreign_done') and time.monotonic() < dl:
                runloom.sleep(0.005)
            release_it[0] = True
            wg.wait()
        def foreign():
            while not held[0]:
                time.sleep(0.002)
            t0 = time.monotonic()
            got = lk.acquire(blocking=True, timeout=0.2)   # foreign spin branch
            elapsed = time.monotonic() - t0
            res['got'] = got
            res['elapsed'] = elapsed
            if got:
                lk.release()
            res['foreign_done'] = True
        _thread.start_new_thread(foreign, ())
        runloom.run(4, main)
        assert res.get('got') is False, res
        assert res['elapsed'] < 1.0, res          # bounded, not a hang
        print("OK", res['elapsed'])
    """, timeout=60)
    assert_no_signal_death(rc_, out, "lock-timed-foreign")
    assert "OK" in out, out


# --------------------------------------------------------------------------
# A17. heavy zlib offload correctness (the first pass only did hashlib).  A big
#      compress must offload (park, yield to a sibling) and round-trip exactly.
# --------------------------------------------------------------------------
def test_heavy_zlib_offload_roundtrip_and_yields():
    import zlib
    out = {"sib": 0}
    big = (b"runloom-" * 100000)       # ~800 KiB, over the 256 KiB gate
    stock_compress = getattr(zlib.compress, "__wrapped__", zlib.compress)
    expected = stock_compress(big)
    def main():
        def sibling():
            for _ in range(30):
                out["sib"] += 1
                runloom.sleep(0.002)
        rc.fiber(sibling)
        comp = zlib.compress(big)
        out["comp_ok"] = (comp == expected)
        out["roundtrip"] = (zlib.decompress(comp) == big)
    with hang_guard(20, "heavy zlib offload"):
        rc.fiber(main); rc.run()
    assert out["comp_ok"] is True, "offloaded zlib.compress differs from stock"
    assert out["roundtrip"] is True
    assert out["sib"] >= 15, "zlib offload starved a sibling: %d" % out["sib"]


# --------------------------------------------------------------------------
# A18. offload() public API: a blocking callable runs on the pool, the fiber
#      parks, a sibling advances, and the return value is correct + exceptions
#      propagate.
# --------------------------------------------------------------------------
def test_offload_runs_blocking_call_and_yields():
    out = {"sib": 0}
    def blocking_work():
        # a genuinely blocking sleep (NOT the patched time.sleep -- use the raw
        # one via os-level) so the fiber really must park on the pool worker.
        import time as _t
        _t.sleep(0.15)
        return 4242
    def main():
        def sibling():
            for _ in range(30):
                out["sib"] += 1
                runloom.sleep(0.003)
        rc.fiber(sibling)
        out["r"] = monkey.offload(blocking_work)
    with hang_guard(20, "offload yields"):
        rc.fiber(main); rc.run()
    assert out["r"] == 4242
    assert out["sib"] >= 15, "offload starved a sibling: %d" % out["sib"]


def test_offload_propagates_exception():
    out = {}
    def boom():
        raise KeyError("offloaded-boom")
    def main():
        try:
            monkey.offload(boom)
            out["raised"] = None
        except KeyError as e:
            out["raised"] = e.args[0]
    with hang_guard(20, "offload exception"):
        rc.fiber(main); rc.run()
    assert out["raised"] == "offloaded-boom", out


# --------------------------------------------------------------------------
# A19. Event clear()-then-wait re-blocks (a stale flag must not let a later
#      wait() return True early), and a spurious wake re-parks until set().
#      The first pass tested set()-wakes-all but never the clear/re-block edge.
# --------------------------------------------------------------------------
def test_event_clear_then_wait_reblocks():
    ev = threading.Event()
    out = {}
    def main():
        ev.set()
        assert ev.wait(timeout=0.1) is True
        ev.clear()                    # flag down again
        # a fresh wait must now BLOCK (and time out), not see the stale flag
        with assert_faster_than(1.5, "cleared event re-blocks"):
            out["r"] = ev.wait(timeout=0.3)
    with hang_guard(10, "event clear then wait"):
        rc.fiber(main); rc.run()
    assert out["r"] is False, "cleared Event.wait returned early (stale flag)"


def test_event_set_during_wait_window_no_lost_wake():
    # set() racing a wait() that is mid-park: the waiter must not be lost.  We
    # spawn many waiters and set() immediately after spawning (before all have
    # committed their park) to stress the edge-before-park window.
    ev = threading.Event()
    N = 200
    woke = bytearray(N)
    def main():
        wg = WaitGroup(); wg.add(N)
        def make(i):
            def w():
                try:
                    ev.wait()
                    woke[i] = 1
                finally:
                    wg.done()
            return w
        for i in range(N):
            rc.fiber(make(i))
            if i == N // 2:
                ev.set()              # set BEFORE the second half even spawns
        wg.wait()
    with hang_guard(30, "event set during wait window"):
        rc.fiber(main); rc.run()
    missing = [i for i in range(N) if not woke[i]]
    assert not missing, "lost wake in the set-during-spawn window: %r" % missing[:10]


# --------------------------------------------------------------------------
# A20. Queue.join()/task_done balance + a foreign producer with a fiber
#      consumer using set-equality integrity (the first pass's foreign-producer
#      test used sorted() == range, which we keep, but here we also stress
#      task_done/join, a separate Condition-based wait the first pass skipped).
# --------------------------------------------------------------------------
def test_queue_join_task_done_balance():
    out = {}
    def main():
        q = queue.Queue()
        N = 100
        consumed = bytearray(N)
        for i in range(N):
            q.put(i)
        def consumer():
            while True:
                try:
                    i = q.get(timeout=2.0)
                except queue.Empty:
                    return
                consumed[i] = 1
                q.task_done()
        for _ in range(6):
            rc.fiber(consumer)
        def joiner():
            q.join()                  # blocks until every task_done balances
            out["joined"] = True
            out["all_consumed"] = (sum(consumed) == N)
        rc.fiber(joiner)
    with hang_guard(30, "queue join/task_done"):
        rc.fiber(main); rc.run()
    assert out.get("joined") is True, "queue.join() never returned (lost task_done wake)"
    assert out.get("all_consumed") is True, "queue dropped items vs task_done count"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
