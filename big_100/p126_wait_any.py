"""big_100 / 126 -- wait-for-any (first-of-N select).

Each round a worker sets up several event sources and `select`s for the first
to fire:
  * a data Chan a helper goroutine sends to after a random delay,
  * a second data Chan a second helper may send to,
  * a `runloom.time.After` timeout.
Exactly one source wins; the losers are abandoned (their helpers' sends land in
1-buffered channels and are harmlessly dropped, or the timer self-closes).

Invariant: each round resolves exactly once -- data0_wins + data1_wins +
timeout_wins == ops -- and there is no lost wakeup (a lost wakeup would hang the
round and trip the watchdog).  All three branches must win at least sometimes.

Stresses: multi-way select, first-of-N resolution, no double-fire, no lost
wakeup across competing park/wake sources.
"""
import harness
import runloom
import runloom.time as rtime


def helper(ch, delay, tok):
    runloom.sleep(delay)
    try:
        ch.try_send(tok)
    except Exception:
        pass


def worker(H, wid, rng, state):
    w0 = state["data0_wins"]
    w1 = state["data1_wins"]
    wt = state["timeout_wins"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        ch0 = runloom.Chan(1)
        ch1 = runloom.Chan(1)
        d0 = rng.uniform(0.0005, 0.005)
        d1 = rng.uniform(0.0005, 0.005)
        H.go(helper, ch0, d0, b"0")
        H.go(helper, ch1, d1, b"1")
        to = rng.uniform(0.0005, 0.005)
        timer = rtime.After(to)
        idx, _payload = runloom.select(
            [("recv", ch0), ("recv", ch1), ("recv", timer)])
        if idx == 0:
            w0[slot] += 1
        elif idx == 1:
            w1[slot] += 1
        else:
            wt[slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"data0_wins": [0] * 1024, "data1_wins": [0] * 1024,
               "timeout_wins": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    a = sum(H.state["data0_wins"])
    b = sum(H.state["data1_wins"])
    t = sum(H.state["timeout_wins"])
    ops = H.total_ops()
    H.log("data0={0} data1={1} timeout={2} sum={3} ops={4}".format(
        a, b, t, a + b + t, ops))
    H.check(a + b + t == ops,
            "resolution conservation: sum={0} != ops={1} (double-fire or "
            "lost wakeup)".format(a + b + t, ops))
    H.check(ops > 0, "no rounds resolved")
    H.check(a > 0 and b > 0, "a data branch never won")
    H.check(t > 0, "timeout branch never won")


if __name__ == "__main__":
    harness.main("p126_wait_any", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="first-of-N select over two data chans + a timer; "
                          "exactly-once resolution, no lost wakeup")
