"""big_100 / 141 -- GC during hub migration.

Goroutines build a (shallow, ~25-30 frame) Python call chain and a cyclic
object graph, hit yield_now()/sleep so they likely resume on a different hub,
while a driver goroutine AND a real OS thread force gc.collect() repeatedly.
The cyclic graph + a per-worker checksum (computed by walking the graph, with a
migration mid-walk) must survive the migration+GC -- the stop-the-world
collector running concurrently with the stack swap must never corrupt object
state or crash.

Stresses: stop-the-world GC + PyThreadState swap during a live recursion / cycle
walk under M:N.
"""
import gc

import harness
import runloom

# Real-thread entry points captured before monkey.patch() turns them
# cooperative -- the gc-storm thread must be a genuine OS thread.
import _thread as _real_thread
import time as _time
_REAL_SLEEP = _time.sleep

# Shallow recursion budget (FINDINGS #6: keep the per-goroutine recursion budget
# small).  25-30 deep is plenty to span a hub migration while staying well under
# any goroutine ceiling.
DEPTH = 28


class Node(object):
    __slots__ = ("nxt", "prev", "val", "owner")

    def __init__(self, val):
        self.nxt = None
        self.prev = None
        self.val = val
        self.owner = None       # back-reference -> a cycle the GC must reclaim


def build_ring(k, owner):
    """A doubly-linked ring of k nodes (a reference cycle), each pointing back
    at a shared owner object (a second cycle layer)."""
    nodes = [Node(i) for i in range(k)]
    for i in range(k):
        nodes[i].nxt = nodes[(i + 1) % k]
        nodes[i].prev = nodes[(i - 1) % k]
        nodes[i].owner = owner
    owner.ring = nodes
    return nodes


def walk_sum(node, k):
    """Walk the ring forward k steps summing vals, yielding mid-walk so the
    walk's live frame + the live Node references span a migration + a possible
    concurrent collect."""
    total = 0
    cur = node
    for i in range(k):
        total += cur.val
        if i == k // 2:
            runloom.yield_now()     # migrate with the ring graph live
        cur = cur.nxt
    return total


def recurse_build(H, depth, k, owner, acc):
    """A shallow Python call chain that, at the bottom, builds the cyclic graph
    and walks it.  The recursion frames stay live across the yields inside
    walk_sum, so a GC running during the migration sees a deep-ish live frame
    chain plus a cyclic object graph reachable from a frame local."""
    if depth == 0:
        nodes = build_ring(k, owner)
        # `nodes` is a frame local that reaches the whole cycle: the GC must
        # treat it as live.  Walk it across a migration.
        s = walk_sum(nodes[0], k)
        return acc + s
    runloom.yield_now()
    return recurse_build(H, depth - 1, k, owner, acc + depth)


class Owner(object):
    __slots__ = ("ring", "tag")

    def __init__(self, tag):
        self.ring = None
        self.tag = tag


def worker(H, wid, rng, state):
    for _ in H.round_range():
        k = rng.randint(4, 24)
        owner = Owner(wid)
        # The recursion adds `depth` on the way down for depths DEPTH .. 1
        # (acc starts at 0), then adds the ring sum at the bottom (depth 0).
        got = recurse_build(H, DEPTH, k, owner, 0)
        chain = sum(range(1, DEPTH + 1))        # depths DEPTH .. 1
        ring = k * (k - 1) // 2
        if not H.check(got == chain + ring,
                       "checksum corrupted across GC/migration wid={0}: "
                       "{1} != {2}".format(wid, got, chain + ring)):
            return
        # Drop the cyclic graph; only the GC can reclaim it (the back-references
        # from owner<->ring form cycles).
        owner.ring = None
        del owner
        if wid < 64 and rng.random() < 0.02:
            gc.collect()
        H.op(wid)
        H.task_done(wid)
        if rng.random() < 0.1:
            runloom.sleep(0.0002)


def setup(H):
    H.state = {"thread_collects": [0], "stop": [False]}


def body(H):
    state = H.state

    # A real OS thread hammering gc.collect() (stop-the-world) concurrently
    # with the goroutines doing live recursion + cycle walks across migrations.
    def gc_thread():
        while not state["stop"][0] and H.running():
            try:
                gc.collect()
                state["thread_collects"][0] += 1
            except Exception:
                pass
            _REAL_SLEEP(0.003)

    _real_thread.start_new_thread(gc_thread, ())

    def gc_driver():
        while H.running():
            H.sleep(0.05)
            gc.collect()
        state["stop"][0] = True
        H.log("thread_collects={0}".format(state["thread_collects"][0]))

    H.fiber(gc_driver)
    H.run_pool(H.funcs, worker, state)


def post(H):
    H.state["stop"][0] = True
    gc.collect()
    H.check(H.total_tasks() > 0, "no work completed")
    H.log("tasks={0} thread_collects={1}".format(
        H.total_tasks(), H.state["thread_collects"][0]))


if __name__ == "__main__":
    harness.main("p141_gc_during_migration", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="cyclic graph + recursion checksum survives GC during "
                          "hub migration")
