"""big_100 / 37 -- yield fairness tournament.

Tens of thousands of goroutines each loop forever doing `yield_now()` and
incrementing their own counter.  A fair scheduler keeps every goroutine's
counter close to the others; a starving scheduler lets some race ahead while
others barely run.

Stresses: scheduler fairness across the M:N hubs.
"""
import harness
import runloom


def worker(H, wid, rng, state):
    # Per-worker counter lives in the harness ops shard (single-writer), which
    # IS the fairness signal we audit.
    while H.running():
        runloom.yield_now()
        H.op(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)

    def auditor():
        # Let everyone get going first.
        H.sleep(3.0)
        while H.running():
            H.sleep(2.0)
            counts = [c for c in H.ops[:H.funcs] if c > 0]
            if len(counts) < H.funcs // 2:
                continue                 # not warmed up yet
            lo, hi = min(counts), max(counts)
            # Generous tolerance -- M:N + work stealing means perfect equality
            # is not expected, but no goroutine should be ~never scheduled.
            if not H.check(lo > 0 and hi <= lo * 200 + 10000,
                           "unfair scheduling: min={0} max={1}".format(lo, hi)):
                return
        H.log("fairness min={0} max={1}".format(
            min(c for c in H.ops[:H.funcs] if c > 0) if any(H.ops[:H.funcs])
            else 0, max(H.ops[:H.funcs])))

    H.fiber(auditor)


if __name__ == "__main__":
    harness.main("p37_yield_fairness", body, default_funcs=20000,
                 describe="yield+increment tournament; counters stay balanced")
