"""Queue-semantics tests adapted from CPython's Lib/test/test_queue.py.

A runloom ``Chan`` with capacity N is, semantically, a bounded blocking FIFO
queue: send==put, recv==get, capacity==maxsize, len()==qsize().  CPython's
test_queue.py encodes decades of blocking-queue edge cases that map straight
onto channels -- and the standout is Python 3.13's ``Queue.shutdown()``, whose
semantics are *exactly* channel close:

    Queue.shutdown()            Chan.close()
    --------------------------  --------------------------------
    put() -> ShutDown           send() -> ValueError("send on closed channel")
    get() drains then ShutDown  recv() drains buffer then (None, False)
    wakes blocked put/get       wakes parked senders (raise) / receivers (zero)

(Queue.shutdown(immediate=True), which DISCARDS buffered items, has no Chan
analog -- runloom close always drains -- so only the default/drain shutdown is
mapped.  task_done()/join() likewise have no channel analog and are skipped.)

The deterministic queue-semantics tests run in-process on the single-thread
scheduler (like test_chan.py; the conftest invariant fixture then checks
self_check + parker leak after each).  The concurrent MPMC test runs in a
fresh free-threaded subprocess so the producers/consumers genuinely race.
"""
import os
import subprocess
import sys
import unittest

sys.path.insert(0, "src")

import runloom_c

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run(*fibers):
    for g in fibers:
        runloom_c.go(g)
    runloom_c.run()


# ===========================================================================
# Deterministic queue semantics (in-process, single scheduler).
# ===========================================================================
class TestFifoOrder(unittest.TestCase):
    """test_queue.py BaseQueueTestMixin: a Queue is FIFO -- items come out in
    the order they went in."""

    def test_fifo_single_producer_consumer(self):
        N = 50
        ch = runloom_c.Chan(N)          # cap >= N: producer never blocks
        out = []

        def producer():
            for i in range(N):
                ch.send(i)
            ch.close()

        def consumer():
            for v in ch:
                out.append(v)

        _run(producer, consumer)
        self.assertEqual(out, list(range(N)))


class TestMaxsizeQsize(unittest.TestCase):
    """test_queue.py: maxsize / qsize / full() / empty() <-> capacity / len /
    try_send-on-full / try_recv-on-empty."""

    def test_qsize_full_empty(self):
        ch = runloom_c.Chan(3)          # maxsize 3
        log = []

        def runner():
            log.append(("cap", ch.capacity))
            log.append(("empty", ch.try_recv()))     # None: empty
            ch.send("a"); ch.send("b"); ch.send("c")
            log.append(("qsize", len(ch)))           # 3: full
            log.append(("full", ch.try_send("d")))   # False: no room
            log.append(("g1", ch.recv()))
            log.append(("qsize2", len(ch)))          # 2
            log.append(("room", ch.try_send("d")))   # True now

        _run(runner)
        self.assertEqual(log, [
            ("cap", 3),
            ("empty", None),
            ("qsize", 3),
            ("full", False),
            ("g1", ("a", True)),
            ("qsize2", 2),
            ("room", True),
        ])


class TestBlockingGetPut(unittest.TestCase):
    """BlockingTestMixin: get() blocks until a put(); put() blocks until a
    get() once full."""

    def test_get_blocks_until_put(self):
        ch = runloom_c.Chan()           # unbuffered
        log = []

        def consumer():
            log.append("get-wait")
            v, ok = ch.recv()
            log.append(("got", v))

        def producer():
            log.append("put")
            ch.send(42)

        _run(consumer, producer)
        self.assertEqual(log[0], "get-wait")   # getter parked first
        self.assertIn(("got", 42), log)

    def test_put_blocks_until_get_when_full(self):
        ch = runloom_c.Chan(1)          # maxsize 1
        log = []

        def producer():
            ch.send("first")            # fills the slot
            log.append("put1-done")
            ch.send("second")           # blocks: queue full
            log.append("put2-done")

        def consumer():
            runloom_c.sched_yield()     # let producer fill + block on put2
            log.append("get")
            v, _ = ch.recv()            # frees a slot -> put2 completes
            log.append(("got", v))

        _run(producer, consumer)
        # put1 completes immediately; put2 blocks until the get; the get of
        # "first" happens before put2 reports done.
        self.assertEqual(log[0], "put1-done")
        self.assertLess(log.index("get"), log.index("put2-done"))
        self.assertIn(("got", "first"), log)


# ===========================================================================
# Queue.shutdown() (Python 3.13)  ==  Chan.close().
# ===========================================================================
class TestShutdownIsClose(unittest.TestCase):
    def test_shutdown_put_raises_get_drains(self):
        """Queue.shutdown: pending items still get()-able (drain), then ShutDown;
        put() after shutdown raises.  Chan.close: recv drains buffer then
        (None, False); send on closed raises."""
        ch = runloom_c.Chan(4)
        log = []

        def runner():
            ch.send("a"); ch.send("b"); ch.send("c")
            ch.close()                              # == q.shutdown()
            try:
                ch.send("d")                        # == put after shutdown
            except ValueError as e:
                log.append(("put-raised", str(e)))
            # get() drains the buffered items, then signals shut-down:
            for _ in range(4):
                log.append(ch.recv())

        _run(runner)
        self.assertEqual(log, [
            ("put-raised", "send on closed channel"),
            ("a", True), ("b", True), ("c", True),
            (None, False),                          # drained -> ShutDown
        ])

    def test_shutdown_wakes_blocked_getter(self):
        """q.shutdown() wakes a thread blocked in get() with ShutDown ->
        close() wakes a fiber parked in recv() with (None, False)."""
        ch = runloom_c.Chan()
        log = []

        def getter():
            log.append(ch.recv())

        def shutdowner():
            runloom_c.sched_yield()      # let getter park in recv()
            ch.close()

        _run(getter, shutdowner)
        self.assertEqual(log, [(None, False)])

    def test_shutdown_wakes_blocked_putter(self):
        """q.shutdown() wakes a thread blocked in put() with ShutDown ->
        close() wakes a fiber parked in send() with ValueError."""
        ch = runloom_c.Chan()            # unbuffered: send parks immediately
        log = []

        def putter():
            try:
                ch.send("x")
                log.append("put-ok")
            except ValueError:
                log.append("put-shutdown")

        def shutdowner():
            runloom_c.sched_yield()      # let putter park in send()
            ch.close()

        _run(putter, shutdowner)
        self.assertEqual(log, ["put-shutdown"])


# ===========================================================================
# Concurrent MPMC (subprocess, real free-threaded hubs).
# ===========================================================================
def _run_mn(code, timeout=60):
    preamble = (
        "import sys; sys.path.insert(0, %r)\n"
        "import runloom_c\n" % os.path.join(REPO, "src")
    )
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_GIL"] = "0"
    try:
        p = subprocess.run(
            [sys.executable, "-c", preamble + code],
            cwd=REPO, env=env, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return 124, out, err + "\n[timed out after {0}s]".format(timeout)
    return p.returncode, p.stdout, p.stderr


class TestConcurrentMPMC(unittest.TestCase):
    def test_mpmc_conservation_and_per_producer_fifo(self):
        """test_queue.py's _doBlockingTest at scale: many producers, many
        consumers, one shared bounded queue.  Beyond conservation (every item
        received exactly once) this asserts the channel-FIFO guarantee that a
        thread-safe Queue also gives: since the buffer is FIFO and every
        consumer pulls the head, EACH consumer sees any single producer's items
        in strictly increasing order.  (Probed clean over 20 runs.)"""
        rc, out, err = _run_mn(r"""
def once(it):
    nprod, ncons, per, cap = 6, 5, 50, 8
    ch   = runloom_c.Chan(cap)
    done = runloom_c.Chan(nprod)
    res  = runloom_c.Chan(ncons)
    def prod(pid):
        def r():
            for s in range(per):
                ch.send((pid, s))
            done.send(1)
        return r
    def closer():
        for _ in range(nprod): done.recv()
        ch.close()
    def cons():
        last = {}; bad = 0; c = 0
        for (pid, s) in ch:
            if pid in last and s <= last[pid]:
                bad += 1                 # per-producer FIFO violated
            last[pid] = s; c += 1
        res.send((c, bad))
    runloom_c.mn_init(4)
    for _ in range(ncons): runloom_c.mn_go(cons)
    for p in range(nprod): runloom_c.mn_go(prod(p))
    runloom_c.mn_go(closer)
    runloom_c.mn_run()
    tot_c = tot_bad = 0
    for _ in range(ncons):
        g = res.try_recv()
        if g is None: break
        (c, bad), ok = g; tot_c += c; tot_bad += bad
    runloom_c.mn_fini()
    assert tot_c == nprod * per, ("lost/dup items", tot_c, nprod * per)
    assert tot_bad == 0, ("per-producer FIFO violated", tot_bad)
    assert runloom_c._self_check(0) == 0

for it in range(15):
    once(it)
print("PASS")
""")
        self.assertTrue(rc == 0 and "PASS" in out,
                        "rc={0}\nout={1}\nerr={2}".format(rc, out, err))


if __name__ == "__main__":
    unittest.main()
