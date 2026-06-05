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
import threading

import harness
import runloom


def setup(H):
    H.state = {"lock": threading.Lock(), "counter": [0]}


def worker(H, wid, rng, state):
    lock = state["lock"]
    while H.running():
        # 1) exception state must survive a migration inside the handler
        try:
            raise ValueError(wid)
        except ValueError:
            runloom.sleep(0.0002)            # likely resume on another hub
            import sys
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


def body(H):
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
