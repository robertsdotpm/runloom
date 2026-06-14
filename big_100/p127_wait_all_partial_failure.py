"""big_100 / 127 -- wait-for-all with partial failure (JoinSet accounting).

Each goroutine spawns a small batch of child callables via a `JoinSet`.  The
children are a mix: some return a value immediately, some sleep-then-return,
some RAISE.  We must collect ALL outcomes and verify every child is accounted
for (n outcomes == n spawned), with exceptions surfaced correctly.

JoinSet.join_all() raises the FIRST child exception (like gather), which would
drop the other outcomes.  So each child returns a TAGGED outcome
`("ok", value)` / `("err", exc_type_name)` and never lets an exception escape
the callable -- then join_all() returns every outcome and we audit the full set:
the count matches, the expected number of "err" tags is present, and each value
matches what that child was asked to produce.

Invariant: outcomes == spawned for every batch (no orphan goroutine, no lost
result); the exceptions we injected all surfaced as "err" tags with the right
type; values are correct.

Stresses: JoinSet spawn/join_all, gather-style fan-out, exception propagation
out of a child goroutine, no orphan goroutine.
"""
import harness
import runloom


class ChildBoom(Exception):
    pass


def child(kind, payload):
    """Run one child.  Returns a tagged outcome; never raises out."""
    try:
        if kind == "value":
            return ("ok", payload)
        if kind == "sleep":
            runloom.sleep(payload[1])
            return ("ok", payload[0])
        if kind == "raise":
            raise ChildBoom(payload)
        return ("ok", None)
    except Exception as exc:        # noqa: BLE001 - tag, don't propagate
        return ("err", type(exc).__name__, getattr(exc, "args", ()))


def worker(H, wid, rng, state):
    accounted = state["accounted"]
    spawned_c = state["spawned"]
    err_seen = state["err_seen"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        n = rng.randint(2, 6)
        js = runloom.sync.JoinSet()
        expect = []         # what each child should produce, in spawn order
        n_err = 0
        for i in range(n):
            r = rng.random()
            if r < 0.34:
                val = (wid << 16) | i
                js.spawn(child, "value", val)
                expect.append(("ok", val))
            elif r < 0.67:
                val = (wid << 16) | i
                js.spawn(child, "sleep", (val, rng.uniform(0.0002, 0.002)))
                expect.append(("ok", val))
            else:
                tag = "boom-{0}-{1}".format(wid, i)
                js.spawn(child, "raise", tag)
                expect.append(("err", "ChildBoom"))
                n_err += 1

        results = js.join_all()     # never raises: children tag their outcome
        spawned_c[slot] += n
        accounted[slot] += len(results)
        err_seen[slot] += sum(1 for r in results if r[0] == "err")

        # Every child accounted for.
        if not H.check(len(results) == n,
                       "JoinSet lost a child: got {0} outcomes for {1} "
                       "spawned".format(len(results), n)):
            return
        # The injected exceptions all surfaced as tagged errors.
        got_err = sum(1 for r in results if r[0] == "err")
        if not H.check(got_err == n_err,
                       "exception accounting wrong: expected {0} err outcomes, "
                       "got {1}".format(n_err, got_err)):
            return
        # Every err outcome is the type we raised.
        for r in results:
            if r[0] == "err" and not H.check(
                    r[1] == "ChildBoom",
                    "wrong exception type surfaced: {0!r}".format(r[1])):
                return
        # join_all preserves spawn order, so values match position-for-position.
        for i, r in enumerate(results):
            exp = expect[i]
            if exp[0] == "ok":
                if not H.check(r[0] == "ok" and r[1] == exp[1],
                               "child {0} value mismatch: got {1!r} expected "
                               "{2!r}".format(i, r, exp)):
                    return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"accounted": [0] * 1024, "spawned": [0] * 1024,
               "err_seen": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    acc = sum(H.state["accounted"])
    spn = sum(H.state["spawned"])
    err = sum(H.state["err_seen"])
    H.log("spawned={0} accounted={1} err_outcomes={2} ops={3}".format(
        spn, acc, err, H.total_ops()))
    H.check(H.total_ops() > 0, "no batches completed")
    H.check(acc == spn,
            "child accounting broken across all batches: accounted={0} != "
            "spawned={1}".format(acc, spn))
    H.check(err > 0, "no exception children ever surfaced (raise path unused)")


if __name__ == "__main__":
    harness.main("p127_wait_all_partial_failure", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="JoinSet fan-out with value/sleep/raise children; "
                          "every child accounted, exceptions surfaced")
