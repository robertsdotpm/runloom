"""big_100 / 120 -- timer-heap churn (Stop must mean no second fire).

Each round a worker creates a batch of `runloom.time.NewTimer(s)` timers and
`.Stop()`s MOST of them before they can fire, letting a few short ones fire.
Per-worker counters track created / fired / stopped, classified by whether the
timer ACTUALLY delivered a value (not by Stop()'s return -- see the finding
below).

FINDING (recorded, not a crash): runloom's `Timer.Stop()` returns `True` even
when the timer has ALREADY fired and delivered into its channel -- unlike Go's
`time.Timer.Stop()`, which returns `false` if the timer already
expired/fired.  So Stop()'s return value is NOT a reliable "I prevented the
fire" signal.  This test therefore classifies a timer by draining its channel:
a timer that ever delivered a value is `fired`; otherwise it is `stopped`.

The conservation invariant that holds regardless of that quirk:
  * created == fired + stopped   (every timer is exactly one),
  * NO timer delivers more than one value (drain twice -> second is empty),
  * a STOPPED-and-drained-empty timer NEVER delivers a value later
    (we re-drain after a yield/sleep to catch a phantom late fire).

A fire-after-Stop (a second delivery, or a value appearing after a confirmed
empty drain) breaks the invariant.

Stresses: timer creation/Stop churn, the timer heap, no double/late fire.
"""
import harness
import runloom
import runloom.time as rtime

BATCH = 16


def drain_once(ch):
    """try_recv normalized: (value, True) if a value was present else
    (None, False).  Chan.try_recv returns None (not a tuple) when empty."""
    r = ch.try_recv()
    if r is None:
        return (None, False)
    return r


def worker(H, wid, rng, state):
    created = state["created"]
    fired = state["fired"]
    stopped = state["stopped"]
    double = state["double"]
    late = state["late"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        timers = []
        for _ in range(BATCH):
            if rng.random() < 0.25:
                d = rng.uniform(0.0002, 0.0015)     # short: likely to fire
                want_fire = True
            else:
                d = rng.uniform(0.3, 1.5)           # long: we will Stop it
                want_fire = False
            tm = rtime.NewTimer(d)
            created[slot] += 1
            timers.append((tm, want_fire))

        # Resolve each timer definitively.
        stopped_empty = []      # timers we Stopped and confirmed had no value
        for tm, want_fire in timers:
            if want_fire:
                # Wait for it to fire (bounded by a backstop), then drain.
                backstop = rtime.After(0.5)
                idx, _p = runloom.select([("recv", tm.c), ("recv", backstop)])
                # Whether it fired via select or not, Stop it and drain to a
                # definitive empty so it can never deliver again, then classify.
                tm.Stop()
                v, ok = drain_once(tm.c)
                if idx == 0 or ok:
                    fired[slot] += 1
                    # Must not deliver a SECOND value.
                    v2, ok2 = drain_once(tm.c)
                    if ok2:
                        double[slot] += 1
                        H.check(False, "timer delivered twice (v2={0!r})".format(v2))
                        return
                else:
                    stopped[slot] += 1
                    stopped_empty.append(tm)
            else:
                tm.Stop()
                v, ok = drain_once(tm.c)
                if ok:
                    # It fired in the window before our Stop+drain -> a real fire.
                    fired[slot] += 1
                    v2, ok2 = drain_once(tm.c)
                    if ok2:
                        double[slot] += 1
                        H.check(False, "timer delivered twice (v2={0!r})".format(v2))
                        return
                else:
                    stopped[slot] += 1
                    stopped_empty.append(tm)

        # A Stopped-and-empty timer must NEVER produce a value later.  Yield so
        # any phantom delayed delivery would have a chance to land, then re-drain.
        runloom.yield_now()
        for tm in stopped_empty:
            v, ok = drain_once(tm.c)
            if ok:
                late[slot] += 1
                H.check(False, "STOPPED timer fired LATE: v={0!r} "
                               "(fire-after-Stop)".format(v))
                return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"created": [0] * 1024, "fired": [0] * 1024,
               "stopped": [0] * 1024, "double": [0] * 1024, "late": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    c = sum(H.state["created"])
    f = sum(H.state["fired"])
    s = sum(H.state["stopped"])
    d = sum(H.state["double"])
    l = sum(H.state["late"])
    H.log("created={0} fired={1} stopped={2} (fired+stopped={3}) "
          "double={4} late={5}".format(c, f, s, f + s, d, l))
    H.check(c > 0, "no timers created")
    H.check(f + s == c,
            "timer conservation broken: fired+stopped={0} != created={1}"
            .format(f + s, c))
    H.check(d == 0, "a timer delivered more than once ({0})".format(d))
    H.check(l == 0, "a Stopped timer fired late ({0})".format(l))
    H.check(s > 0, "no timers were stopped (test did no cancellation)")


if __name__ == "__main__":
    harness.main("p120_timer_heap_churn", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="create+Stop many timers; no double/late fire; "
                          "created == fired + stopped")
