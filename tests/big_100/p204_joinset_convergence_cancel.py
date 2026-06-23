"""big_100 / 204 -- JoinSet convergence under cancellation.

Each goroutine creates a `JoinSet`, spawns a batch of tasks watching a shared
cancel context, then cancels the context partway through.  Some tasks finish
their work normally; the rest observe the cancellation (via
`cancelutil.cancellable_sleep` on the ctx) and exit promptly.  Either way every
task EXITS, so `join_all()` must RETURN -- it must never hang waiting on a task
that was cancelled.

Each task returns a tagged outcome ("done" or "cancelled") so join_all (which
re-raises the first task exception) never blows up and the batch can be fully
accounted.  finished + cancelled must equal spawned.

Stresses: JoinSet/WaitGroup convergence, cooperative-cancel observed by tracked
tasks, no orphan goroutine, bounded join under cancellation.

Invariant (post): join returned for every batch (joined_batches == batches);
finished + cancelled == spawned; no task lost.
"""
import harness
import runloom
import runloom.sync as sync
import cancelutil

BATCH = 12                 # tasks per JoinSet


def setup(H):
    n = H.funcs
    H.state = {
        "spawned": [0] * n,
        "finished": [0] * n,
        "cancelled": [0] * n,
        "batches": [0] * n,
        "joined": [0] * n,
    }


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
        js = sync.JoinSet()

        # About half the tasks are "long" (they wait, so a cancel can catch
        # them mid-wait); the rest finish quickly regardless of cancel.
        plan = []
        for k in range(BATCH):
            longish = (rng.random() < 0.5)
            plan.append(longish)

        def task(longish):
            if not longish:
                # quick task: a touch of work, then done.
                runloom.yield_now()
                return "done"
            # long task: cooperatively wait on the ctx; if cancelled, exit as
            # "cancelled" -- but STILL exit (no hang).
            elapsed = cancelutil.cancellable_sleep(ctx, 0.05)
            if not elapsed:            # select chose ctx.done -> cancelled
                return "cancelled"
            return "done"

        for longish in plan:
            js.spawn(task, longish)

        # Cancel partway so some long tasks are caught waiting; a couple may
        # already be done, a few quick tasks finish regardless.
        runloom.sleep(0.001)
        cancel()

        results = js.join_all()        # MUST return -- a hung task wedges here
        state["joined"][wid] += 1
        fin = sum(1 for r in results if r == "done")
        can = sum(1 for r in results if r == "cancelled")
        # any other value would be a bug (task returned something unexpected)
        other = len(results) - fin - can
        if not H.check(other == 0,
                       "unexpected task outcome(s): {0} of {1}".format(
                           other, len(results))):
            return
        state["spawned"][wid] += len(results)
        state["finished"][wid] += fin
        state["cancelled"][wid] += can
        state["batches"][wid] += 1
        H.op(wid, len(results))
        H.task_done(wid)


def body(H):
    # Each batch spawns BATCH tracked tasks; cap concurrent parents.
    H.run_pool(H.funcs, worker, H.state, max_concurrent=3000)


def post(H):
    spawned = sum(H.state["spawned"])
    finished = sum(H.state["finished"])
    cancelled = sum(H.state["cancelled"])
    batches = sum(H.state["batches"])
    joined = sum(H.state["joined"])
    H.check(joined == batches,
            "join_all did not return for every batch: joined={0} batches={1} "
            "(a cancelled task wedged a join)".format(joined, batches))
    H.check(finished + cancelled == spawned,
            "task accounting off: finished({0}) + cancelled({1}) != "
            "spawned({2})".format(finished, cancelled, spawned))
    H.log("batches={0} spawned={1} finished={2} cancelled={3}".format(
        batches, spawned, finished, cancelled))


if __name__ == "__main__":
    harness.main("p204_joinset_convergence_cancel", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="JoinSet.join_all returns for every batch under "
                          "cancellation; finished+cancelled == spawned")
