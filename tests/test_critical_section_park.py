"""Regression: a fiber that parks while holding a CPython per-object
critical section must not strand that object's mutex across the swap.

On free-threaded 3.13t a dict lookup that has to call a Python ``__eq__`` (hash
collision) runs that comparison INSIDE the dict's critical section.  If the
``__eq__`` parks (cooperative yield), the fiber holds the dict's ``ma_mutex``
across a fiber swap.  Before the fix (runloom_sched_pystate snap/load now
suspend/restore ``tstate->critical_section``, mirroring CPython's
detach/attach), every other hub doing a lookup on that dict deadlocked on the
mutex -- and the shared per-hub tstate's critical-section chain corrupted across
fibers, so the failure showed up as EITHER a hang OR a segfault.

This drove the mnweb dogfood server into a full-scheduler wedge after ~2.8 h
(all hubs blocked in ``_Py_dict_lookup_threadsafe`` on ``app.routes``).
"""
import runloom_c


class CollidingKey:
    """All instances hash-collide, so a lookup must walk the chain and call
    __eq__ -- which parks (sched_sleep) while the dict's critical section is
    held."""
    __slots__ = ("v",)

    def __init__(self, v):
        self.v = v

    def __hash__(self):
        return 1

    def __eq__(self, other):
        runloom_c.sched_sleep(0.0005)        # park INSIDE the dict critical section
        return isinstance(other, CollidingKey) and self.v == other.v


def test_park_in_dict_critical_section_no_deadlock():
    table = {CollidingKey(i): i for i in range(8)}
    n_workers = 40
    # race-free counter: with 4 hubs (GIL off) a shared `done[0] += 1` is a
    # read-modify-write race that LOSES increments -> the exact-count assert below
    # would spuriously fail (got 39 != 40).  One slot per worker, single writer each.
    done = bytearray(n_workers)

    def worker(i):
        for _ in range(30):
            table.get(CollidingKey(i % 8))
        done[i] = 1

    runloom_c.mn_init(4)
    try:
        for i in range(n_workers):
            runloom_c.mn_go(lambda i=i: worker(i))
        runloom_c.mn_run()        # would hang here pre-fix (or the process segfaults)
    finally:
        runloom_c.mn_fini()

    assert sum(done) == n_workers, "expected all workers to finish, got %d" % sum(done)


if __name__ == "__main__":
    test_park_in_dict_critical_section_no_deadlock()
    print("ok")
