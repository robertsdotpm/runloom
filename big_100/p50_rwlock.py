"""big_100 / 50 -- reader-writer lock workload.

A reader-writer lock (built here from a Condition) guards a value plus its
checksum.  Many readers read the pair under a shared read-lock and verify the
checksum matches; a few writers update both under an exclusive write-lock.  If
the lock let a reader in during a write, the reader would observe a value whose
checksum doesn't match.

Stresses: reader/writer exclusion, fairness, data consistency.
"""
import threading

import harness
import runloom


class RWLock(object):
    def __init__(self):
        self._c = threading.Condition()
        self._readers = 0
        self._writer = False

    def racquire(self, running=None):
        with self._c:
            while self._writer:
                if running is not None and not running():
                    return False
                self._c.wait(timeout=0.05)
            self._readers += 1
            return True

    def rrelease(self):
        with self._c:
            self._readers -= 1
            if self._readers == 0:
                self._c.notify_all()

    def wacquire(self, running=None):
        with self._c:
            while self._writer or self._readers > 0:
                if running is not None and not running():
                    return False
                self._c.wait(timeout=0.05)
            self._writer = True
            return True

    def wrelease(self):
        with self._c:
            self._writer = False
            self._c.notify_all()


# Same drain problem as p43: 100k goroutines competing for a single Condition
# => drain time = O(N / throughput) >> 120s.  max_concurrent caps goroutines
# per pool so only MAX_ACTIVE compete total; drain stays well within bounds.
# wacquire/racquire take a running= arg and use wait(timeout=0.05) so goroutines
# parked inside the condition can escape without explicit cancel_all on Condition.
MAX_ACTIVE = 2000


def setup(H):
    H.state = {"rw": RWLock(), "data": {"v": 0, "sum": 0}}


def checksum(v):
    return (v * 2654435761) & 0xFFFFFFFF


def reader(H, wid, rng, state):
    rw = state["rw"]
    data = state["data"]
    while H.running():
        acquired = rw.racquire(running=H.running)
        if not acquired:
            break
        try:
            v = data["v"]
            s = data["sum"]
        finally:
            rw.rrelease()
        if not H.check(s == checksum(v),
                       "torn read: v={0} sum={1} != {2} wid={3}".format(
                           v, s, checksum(v), wid)):
            return
        H.op(wid)


def writer(H, wid, rng, state):
    rw = state["rw"]
    data = state["data"]
    while H.running():
        acquired = rw.wacquire(running=H.running)
        if not acquired:
            break
        try:
            nv = rng.randint(0, 1 << 30)
            data["v"] = nv
            runloom.yield_now()         # widen the window an unguarded reader could hit
            data["sum"] = checksum(nv)
        finally:
            rw.wrelease()
        H.op(wid)
        H.task_done(wid)


def body(H):
    writers = max(2, H.funcs // 20)
    readers = H.funcs - writers
    H.run_pool(writers, writer, H.state, max_concurrent=max(2, MAX_ACTIVE // 20))
    H.run_pool(readers, reader, H.state,
               max_concurrent=MAX_ACTIVE - max(2, MAX_ACTIVE // 20))


if __name__ == "__main__":
    harness.main("p50_rwlock", body, setup=setup, default_funcs=4000,
                 describe="reader-writer lock keeps value+checksum consistent")
