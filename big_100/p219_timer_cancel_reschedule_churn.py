"""big_100 / 219 -- timer cancel / reschedule churn.

Goroutines repeatedly arm a timer, Stop() it before it fires, and re-arm a fresh
one -- overlapping deadlines, with a yield between so the work spans hubs.  A
Stop()'d timer must NEVER deliver onto its channel afterwards (the per-worker
'fired-after-cancel' counter must stay 0).  Some timers are deliberately left to
fire so the firing path is also exercised.

The leak angle (memory: drain_timers UAF / a timer that holds a g-ref leaks as
unbounded growth): an auditor goroutine samples RSS / live-object count / fd
count and asserts BOUNDED growth across the run -- a per-round-leaked timer
goroutine or g-ref would show as monotonic unbounded growth.

Stresses: timer arm/Stop/re-arm churn, no fire-after-cancel, timer-goroutine and
g-ref lifetime (no leak), select over racing timers.
"""
import harness
import runloom
import runloom.time as rtime


def _timer_chan(timer):
    """The channel a Timer/After result selects on (Timer.c, After is a chan)."""
    return timer.c if hasattr(timer, "c") else timer


def worker(H, wid, rng, state):
    fired_after_cancel = state["fired_after_cancel"]
    stopped = state["stopped"]
    fired = state["fired"]
    for _ in H.round_range():
        if not H.running():
            break
        # Arm several overlapping timers, Stop most of them, then verify a
        # Stop'd timer never fires.
        cancelled_timers = []
        for _ in range(rng.randint(2, 6)):
            t = rtime.NewTimer(rng.uniform(0.02, 0.08))
            # Stop it well before its (20-80ms) deadline.
            ok = t.Stop()
            if ok:
                stopped[wid & 1023] += 1
                cancelled_timers.append(t)
        runloom.yield_now()                # migrate hubs between arm and check

        # One live timer we DO let fire: a short deadline + a select that waits
        # for it (so the firing path is exercised too).
        live = rtime.NewTimer(rng.uniform(0.001, 0.005))
        idx, _ = runloom.select([("recv", _timer_chan(live))])
        if idx == 0:
            fired[wid & 1023] += 1

        # Now poll every cancelled timer's channel: a Stop'd timer must NEVER
        # have delivered.  select(default=True) returns -1 when nothing is
        # ready, or (idx, (val, ok)) when a value is waiting on the channel.
        for t in cancelled_timers:
            res = runloom.select([("recv", _timer_chan(t))], default=True)
            if res != -1:
                idx2, payload = res
                if idx2 == 0 and payload is not None and payload[1]:
                    # A value came through on a Stop'd timer's channel -> the
                    # cancellation leaked a fire.
                    fired_after_cancel[wid & 1023] += 1
        del cancelled_timers
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "fired_after_cancel": [0] * 1024,
        "stopped": [0] * 1024,
        "fired": [0] * 1024,
        "rss_samples": [],
        "obj_samples": [],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        import gc
        base_fds = harness.count_fds()
        base_rss = harness.rss_mb()
        base_obj = len(gc.get_objects())
        # Generous absolute bounds: a per-round leaked timer goroutine/g-ref
        # would blow past these; steady churn stays well under.
        while H.running():
            gc.collect()
            fds = harness.count_fds()
            rss = harness.rss_mb()
            nobj = len(gc.get_objects())
            H.state["rss_samples"].append(rss)
            H.state["obj_samples"].append(nobj)
            H.check(fds < base_fds + 2000,
                    "fd unbounded growth: {0} (base {1}) -- timer fd leak".format(
                        fds, base_fds))
            if base_rss > 0 and rss > 0:
                H.check(rss < base_rss + 1500,
                        "RSS unbounded growth: {0}MB (base {1}MB) -- timer "
                        "g-ref leak".format(rss, base_rss))
            H.check(nobj < base_obj + 4_000_000,
                    "live-object unbounded growth: {0} (base {1}) -- timer "
                    "leak".format(nobj, base_obj))
            H.sleep(0.5)
        H.log("rss base->last {0}->{1}MB objs base->last {2}->{3}".format(
            base_rss,
            H.state["rss_samples"][-1] if H.state["rss_samples"] else base_rss,
            base_obj,
            H.state["obj_samples"][-1] if H.state["obj_samples"] else base_obj))

    H.go(auditor)


def post(H):
    fac = sum(H.state["fired_after_cancel"])
    stopped = sum(H.state["stopped"])
    fired = sum(H.state["fired"])
    H.check(fac == 0,
            "{0} timers FIRED AFTER Stop() -- cancellation leaked a fire".format(
                fac))
    H.check(stopped > 0, "no timers were ever Stop'd (test did no work)")
    H.check(fired > 0, "no live timer ever fired (firing path unexercised)")
    H.log("stopped={0} fired={1} fired_after_cancel={2}".format(
        stopped, fired, fac))


if __name__ == "__main__":
    harness.main("p219_timer_cancel_reschedule_churn", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="arm/Stop/re-arm timers across hubs; zero "
                          "fire-after-cancel, bounded resource growth")
