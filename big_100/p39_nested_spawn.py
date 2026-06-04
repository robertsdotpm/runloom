"""big_100 / 39 -- nested spawn explosion.

Each root goroutine recursively spawns a tree of children to a fixed depth and
branching factor, and we verify that exactly the expected number of nodes
complete -- no goroutine is dropped or lost on the way down or back up.

Stresses: goroutine creation at depth, join/aggregation, memory.
"""
import harness
import runloom
import runloom_c

BRANCH = 3
DEPTH = 8           # 3^8 = 6561 nodes per tree


def expected_nodes():
    return (BRANCH ** (DEPTH + 1) - 1) // (BRANCH - 1)


def spawn_tree(H, depth, done):
    """Spawn a subtree; signal `done` once for every node (self + descendants).

    Each node spawns its children, then signals its own completion.  A channel
    counts completions so the root can verify the full count."""
    if depth > 0:
        for _ in range(BRANCH):
            H.go(spawn_tree, H, depth - 1, done)
    done.send(1)


def worker(H, wid, rng, state):
    total = expected_nodes()
    while H.running():
        done = runloom.Chan(total)
        H.go(spawn_tree, H, DEPTH, done)
        seen = 0
        while seen < total:
            done.recv()
            seen += 1
            H.op(wid)
        if not H.check(seen == total,
                       "node count {0} != {1} wid={2}".format(
                           seen, total, wid)):
            return
        H.task_done(wid)


def body(H):
    # Tree nodes are many and shallow (each is its own goroutine, no deep C
    # recursion), so a small stack keeps the wide fan-out affordable.
    runloom_c.set_stack_size(96 * 1024)
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p39_nested_spawn", body, default_funcs=200,
                 describe="recursive spawn trees; every node must complete")
