"""big_100 / 210 -- multiprocessing.Queue (foreign _feed thread) under M:N.

`multiprocessing.Queue` runs an internal `_feed` daemon thread (a FOREIGN OS
thread to the scheduler) that takes patched locks/conditions to serialise puts
onto the underlying pipe.  Under runloom + monkey.patch() that foreign thread
must use the FOREIGN-OS-THREAD-safe fallback (real OS blocking) -- the historic
free-threaded mp.Queue SIGSEGV/UAF (CLAUDE.md "Cooperative primitives must be
FOREIGN-OS-THREAD-safe") is exactly this path.

A small number of producer/consumer goroutines push/pop a fixed total of items
through ONE shared mp.Queue.  Items are conserved: every item put is got exactly
once (sum tracked), no crash.

Uses the **forkserver** start method (NOT fork -- the fork start-method
deadlocks under runloom).  Kept deliberately small and bounded.

Stresses: mp.Queue _feed foreign thread, patched-primitive foreign fallback,
item conservation across goroutine producers/consumers.
"""
import multiprocessing

import harness
import runloom

NPRODUCERS = 8
NCONSUMERS = 8
ITEMS_PER_PRODUCER = 200       # total 1600 items -> small + bounded
SENTINEL = ("STOP", None)


def setup(H):
    # forkserver: a clean control process forks the workers, avoiding the
    # runloom fork-start-method deadlock.  We don't actually spawn worker
    # PROCESSES here (the Queue + its _feed thread are the target), but we use
    # the context's Queue so its locks come from the chosen start method.
    try:
        ctx = multiprocessing.get_context("forkserver")
    except (ValueError, OSError):
        ctx = multiprocessing.get_context("spawn")
    H.state = {"ctx": ctx}
    H.put_counts = [0] * NPRODUCERS
    H.got_counts = [0] * NCONSUMERS
    H.got_checksum = [0] * NCONSUMERS
    H.put_checksum = [0] * NPRODUCERS


def producer(H, pid):
    q = H.state["queue"]
    put = H.put_counts
    chk = H.put_checksum
    n = 0
    while n < ITEMS_PER_PRODUCER:
        if not H.running() and H.time_left() < 0:
            break
        val = (pid << 20) | n
        q.put((pid, n, val))       # _feed thread serialises this onto the pipe
        put[pid] += 1
        chk[pid] = (chk[pid] + val) & 0xFFFFFFFFFFFF
        n += 1
        if (n & 15) == 0:
            runloom.yield_now()


def consumer(H, cid, total_expected, done_event):
    q = H.state["queue"]
    got = H.got_counts
    chk = H.got_checksum
    while True:
        try:
            item = q.get(timeout=0.2)
        except Exception:
            if done_event[0]:
                break
            continue
        if item == SENTINEL:
            break
        pid, n, val = item
        got[cid] += 1
        chk[cid] = (chk[cid] + val) & 0xFFFFFFFFFFFF


def body(H):
    ctx = H.state["ctx"]
    # Bounded queue so the _feed thread is genuinely exercised (it blocks when
    # the pipe buffer fills).
    q = ctx.Queue()
    H.state["queue"] = q
    H.register_close(q)

    total = NPRODUCERS * ITEMS_PER_PRODUCER
    done_event = [False]

    # Spawn producers + consumers as goroutines.
    prod_done = [0]
    prod_lock = runloom.sync.Lock()

    def prod_wrap(pid):
        producer(H, pid)
        with prod_lock:
            prod_done[0] += 1

    for pid in range(NPRODUCERS):
        H.go(prod_wrap, pid)
    for cid in range(NCONSUMERS):
        H.go(consumer, H, cid, total, done_event)

    # Wait for all producers to finish, then send one sentinel per consumer so
    # they drain and exit cleanly (no goroutine left parked in q.get()).
    deadline = harness.REAL_MONO() + 60.0
    while prod_done[0] < NPRODUCERS and harness.REAL_MONO() < deadline:
        H.sleep(0.02)
    done_event[0] = True
    for _ in range(NCONSUMERS):
        try:
            q.put(SENTINEL)
        except Exception:
            break

    # Let consumers drain.  We count completed work via the per-slot counters in
    # post(); bump ops here so the watchdog sees progress and the metric is
    # nonzero.
    drain_deadline = harness.REAL_MONO() + 30.0
    while harness.REAL_MONO() < drain_deadline:
        if sum(H.got_counts) >= total:
            break
        H.sleep(0.02)
    # Record ops for the harness metric: one op per item moved through the queue.
    moved = sum(H.got_counts)
    H.op(0, moved)
    H.task_done(0, moved)


def post(H):
    put = sum(H.put_counts)
    got = sum(H.got_counts)
    put_chk = 0
    for c in H.put_checksum:
        put_chk = (put_chk + c) & 0xFFFFFFFFFFFF
    got_chk = 0
    for c in H.got_checksum:
        got_chk = (got_chk + c) & 0xFFFFFFFFFFFF

    H.check(put > 0, "no items were put")
    H.check(got == put,
            "items_got {0} != items_put {1} (mp.Queue lost/duplicated items)"
            .format(got, put))
    H.check(got_chk == put_chk,
            "checksum mismatch put={0} got={1} (mp.Queue corrupted values)"
            .format(put_chk, got_chk))
    H.log("put={0} got={1} put_chk={2} got_chk={3}".format(
        put, got, put_chk, got_chk))


if __name__ == "__main__":
    harness.main("p210_mp_queue_freethread", body, setup=setup, post=post,
                 default_funcs=40, max_funcs=40,
                 describe="mp.Queue _feed foreign thread under M:N; items "
                          "conserved through goroutine producers/consumers")
