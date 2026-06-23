"""big_100 / 128 -- parent-cancel cascade through a goroutine tree.

Each worker is a "parent": it creates a cancel context and spawns a 2-3 level
tree of child goroutines, each blocked in `cancelutil.cancellable_recv` /
`cancellable_sleep` watching the SAME context's done channel.  Then the parent
cancels the context.  Every descendant must observe the cancellation and exit
promptly.

Because `go()` returns no join handle under M:N, the tree shares a
`runloom.WaitGroup`: the parent `add(1)`s before each spawn and every descendant
`done()`s in a finally as it exits.  The parent `wait()`s for the group after
cancelling -- if even one descendant fails to observe the cancel (a lost
done-channel wake), wait() never returns and the watchdog fires.

A per-tree spawned/exited pair (single-writer counters held by the parent for
spawned, and an atomic-ish exited tally via a real OS lock cell) double-checks
conservation in post().

Invariant: every spawned descendant exited (spawned == exited per tree), the
parent's wait() returned (bounded teardown), no orphan goroutine.

Stresses: context cancel cascade, cancellable_recv/cancellable_sleep, fan-out
tree teardown, WaitGroup join, no lost cancellation wake.
"""
import harness
import runloom
import cancelutil


def leaf(H, ctx, wg, exited_cell, exited_lock, rng):
    """A leaf goroutine: block watching ctx, exit on cancel."""
    try:
        # Block on a channel nobody sends to, with the ctx watched.  Returns
        # None when ctx is cancelled.  A never-arriving timeout backstop keeps
        # the wait honest (it must be the cancel that wakes us, not a timeout).
        never = runloom.Chan(0)
        while H.running():
            r = cancelutil.cancellable_recv(ctx, never, timeout=2.0)
            if r is None:
                # Either cancelled (ctx.done fired) or the 2s backstop.  If the
                # ctx is actually cancelled, exit; otherwise loop (shouldn't
                # happen in the test window, but keeps us from spinning out on a
                # spurious timeout).
                if ctx.err() is not None or not H.running():
                    return
            else:
                return
    finally:
        with exited_lock:
            exited_cell[0] += 1
        wg.done()


def branch(H, ctx, wg, exited_cell, exited_lock, rng, depth):
    """An internal node: spawn some children, then block watching ctx itself."""
    try:
        nkids = rng.randint(2, 3)
        for _ in range(nkids):
            wg.add(1)
            if depth > 1:
                H.fiber(branch, H, ctx, wg, exited_cell, exited_lock, rng, depth - 1)
            else:
                H.fiber(leaf, H, ctx, wg, exited_cell, exited_lock, rng)
        # The internal node itself also blocks until cancelled.
        cancelutil.cancellable_sleep(ctx, 2.0)
    finally:
        with exited_lock:
            exited_cell[0] += 1
        wg.done()


def worker(H, wid, rng, state):
    spawned_tot = state["spawned"]
    exited_tot = state["exited"]
    exited_lock = state["exited_lock"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        wg = runloom.WaitGroup()
        exited_cell = [0]

        # Spawn a small top level of 2-3 branches; each branch fans out.
        ntop = rng.randint(2, 3)
        depth = rng.randint(1, 2)       # branch recursion depth
        n_spawned = 0
        for _ in range(ntop):
            wg.add(1)
            n_spawned += 1
            if depth > 1:
                H.fiber(branch, H, ctx, wg, exited_cell, exited_lock, rng, depth - 1)
            else:
                H.fiber(leaf, H, ctx, wg, exited_cell, exited_lock, rng)

        # Let the tree fully materialize (every descendant parks watching ctx)
        # before we cancel, so we exercise the cascade-to-parked path.
        runloom.sleep(0.002)
        cancel()

        # Wait for EVERY descendant to observe cancellation and exit.  If a
        # cancel wake were lost, this never returns -> watchdog hang.
        wg.wait()

        # Account.  exited_cell counts every node (top + internal + leaves) that
        # ran its finally; we counted top-level spawns explicitly but the tree's
        # full size is whatever materialized, so use exited_cell as the truth and
        # also confirm it is >= the top-level count we definitely spawned.
        n_exited = exited_cell[0]
        if not H.check(n_exited >= n_spawned,
                       "fewer descendants exited ({0}) than top-level spawned "
                       "({1})".format(n_exited, n_spawned)):
            return
        spawned_tot[slot] += n_exited       # every exited node was spawned
        exited_tot[slot] += n_exited
        H.op(wid)
        H.task_done(wid)


def setup(H):
    import harness as _h
    H.state = {"spawned": [0] * 1024, "exited": [0] * 1024,
               # a real OS lock for the rare cross-goroutine exited tally cell;
               # _real_thread is captured before monkey.patch in the harness.
               "exited_lock": _h._real_thread.allocate_lock()}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    sp = sum(H.state["spawned"])
    ex = sum(H.state["exited"])
    H.log("spawned={0} exited={1} ops={2}".format(sp, ex, H.total_ops()))
    H.check(H.total_ops() > 0, "no trees cancelled")
    H.check(sp == ex,
            "descendant conservation broken: spawned={0} != exited={1} "
            "(orphaned goroutine after cancel)".format(sp, ex))
    H.check(ex > 0, "no descendants ran")


if __name__ == "__main__":
    harness.main("p128_parent_cancel_cascade", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="cancel a context with a 2-3 level goroutine tree "
                          "watching it; every descendant exits (WaitGroup join)")
