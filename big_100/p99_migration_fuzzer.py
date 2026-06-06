"""big_100 / 99 -- cross-thread migration fuzzer.

Goroutines deliberately hit scheduling points (yield/sleep) so they resume on
different hub threads, and across those migrations they exercise the state that
MUST survive a PyThreadState swap: a handled exception (sys.exc_info), a lock
held across the migration, and a C-extension object (a hashlib hasher) whose
incremental state spans the switch.  Each must come out exactly right.

(contextvars / threading.local are deliberately NOT asserted here -- they are
hub-local under M:N, FINDINGS BUG #7.)

Stresses: PyThreadState snapshot/restore + C-extension state across migration.
"""
import hashlib
import sys
import threading

import harness
import runloom

# `with lock: ...; yield_now()` holds the lock for O(N/hubs) scheduler ticks.
# At 100k goroutines that is ~12.5ms/hold → throughput ~80/s → drain 1250s.
# Cap concurrent workers; cancel_all() wakes the rest when the run ends.
MAX_WORKERS = 2000


def setup(H):
    sem = threading.Semaphore(MAX_WORKERS)
    H.state = {"lock": threading.Lock(), "counter": [0], "sem": sem}


def worker(H, wid, rng, state):
    lock = state["lock"]
    sem = state["sem"]
    while H.running():
        if not sem.acquire():
            break
        try:
            # 1) exception state must survive a migration inside the handler
            try:
                raise ValueError(wid)
            except ValueError:
                runloom.sleep(0.0002)            # likely resume on another hub
                cur = sys.exc_info()[1]
                if not H.check(isinstance(cur, ValueError) and cur.args[0] == wid,
                               "exc_info lost across migration wid={0}: {1!r}"
                               .format(wid, cur)):
                    return

            # 2) a lock held across a migration still serialises the counter
            with lock:
                x = state["counter"][0]
                runloom.yield_now()
                state["counter"][0] = x + 1

            # 3) a C-extension (hashlib) object's incremental state spans a switch
            h = hashlib.sha256()
            h.update(b"a" * 100)
            runloom.yield_now()
            h.update(b"b" * 100)
            expect = hashlib.sha256(b"a" * 100 + b"b" * 100).hexdigest()
            if not H.check(h.hexdigest() == expect,
                           "hashlib state corrupted across migration wid={0}".format(
                               wid)):
                return

            H.op(wid)
            H.task_done(wid)
        finally:
            sem.release()


def body(H):
    sem = H.state["sem"]

    def _cancel_watcher():
        while H.running():
            runloom.sleep(0.05)
        sem.cancel_all()

    H.go(_cancel_watcher)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # One locked increment per completed task; the lock must not drop any
    # across a hub migration.
    counter = H.state["counter"][0]
    tasks = H.total_tasks()
    H.check(counter == tasks,
            "lock counter {0} != completed tasks {1} (increments lost across "
            "migration)".format(counter, tasks))
    H.log("migration_counter={0} tasks={1}".format(counter, tasks))


if __name__ == "__main__":
    harness.main("p99_migration_fuzzer", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="exc/lock/C-ext state survives cross-hub migration")
