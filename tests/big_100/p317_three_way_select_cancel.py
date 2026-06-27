"""big_100 / 317 -- three-way select {data, After timer, cancel-broadcast}.

p213 selects over data + ONE timer.  The genuinely adversarial case is a
THREE-way select -- data channel, an After() timeout, AND a cancel-broadcast
channel -- where the cancel fires WHILE many selectors are parked across hubs.
When the cancel resolves a parked select, the runtime must cancel the LOSING
cases (the timer's After-chan and the data recv) cleanly: a data send that was
about to rendezvous with a now-cancelled selector must NOT be eaten and dropped,
and the timer fiber's buffered tick must not strand an item.  If select's
case-cancellation on a 3-way park leaks, an item is lost (consumed below
produced) or a selector both saw cancel AND ate a data value on the same select
(buckets overlap).

Closed-world per worker group so conservation is exactly checkable:

  * ONE data Chan(cap = N) -- buffered to hold all N items, so the producer
    NEVER parks on send (no send is "in flight" against a parked selector that
    could then be cancelled out from under it); the producer sends N unique
    items then close()s.  A closed channel still yields its buffered items
    (ok=True) before reporting ok=False, so the data is always fully drainable.
  * ONE cancel Chan(0), closed by a coordinator mid-flight to BROADCAST cancel
    (a closed-chan recv returns ok=False to every parked selector at once --
    the cleanest multi-waiter cancel, exactly p202's close-wakes-all pattern).
  * SELECTORS selector fibers, each LOOPING
        select([('recv', data), ('recv', After(t)), ('recv', cancel)])
      - idx 0 data  : ok=True  -> consumed += 1, loop
                      ok=False -> data closed & drained -> terminal DATA_DRAINED
      - idx 1 timer : timed_out += 1, loop (a fresh After each iteration; the
                      timeout branch NEVER consumes via try_recv -- per p213 a
                      try_recv drain there can fail to wake a parked sender)
      - idx 2 cancel: ok is always False (closed) -> terminal CANCELLED.  This
                      resolving select must NOT also carry a data value
                      (disjointness): a value on the cancel case is a PHANTOM.

After all selectors + the producer have returned, the worker drains whatever is
still buffered in the (now closed) data channel via try_recv into `residual`.

Oracle (DISJOINT-BUCKET MULTISET conservation), post:
  (1) consumed + residual == produced  -- every produced item is either
      consumed through a select or still buffered at close & drained; NONE is
      lost to a losing-then-cancelled case.
  (2) buckets disjoint & complete: every selector terminated in EXACTLY one of
      {DATA_DRAINED, CANCELLED}; the two bucket totals sum to the selector
      count (no selector vanished, none double-counted).
  (3) zero PHANTOM: no cancel-resolving select also delivered a data value
      (per-select disjointness -- a losing case was not also taken).
  (4) require_no_lost + watchdog: a missed cancel-wake leaves a selector parked
      forever (it never reaches its terminal bucket) -> the group's JoinSet
      never completes -> watchdog EXIT_HANG.

Stresses: 3-way select park, close-broadcast cancel, cancellation of the LOSING
timer + data cases under a concurrent third-source fire, After() churn, no item
eaten by a cancelled case, disjoint-bucket conservation.

Good TSan / controlled-M:N-replay target: the cancel-vs-data-rendezvous case
cancellation is a wake-ordering race; a data-race report on the select waiter
list / the dropped send is often the first signal before the conservation
oracle even fires.
"""
import random

import harness
import runloom
import runloom.sync as sync
import runloom.time as rtime

ITEMS_PER_GROUP = 96          # unique data items the producer sends per round
SELECTORS = 6                 # selector fibers parked in the 3-way select

DATA_DRAINED = 0
CANCELLED = 1


def producer(data, n, base, jitter_seed):
    """Send n unique monotonic items then close.  cap==n so no send parks; a
    small jitter sleep makes the data channel transiently empty so the timer
    branch is exercised.  Owns its OWN random.Random -- sharing one across
    goroutines under M:N with the GIL off corrupts its Mersenne state."""
    prng = random.Random(jitter_seed)
    try:
        for i in range(n):
            data.send(base + i)
            if (i & 15) == 0:
                runloom.sleep(prng.uniform(0.0, 0.0006))
    finally:
        data.close()


def selector(data, cancel, to_seed):
    """Loop the 3-way select until terminal.  Returns
    (bucket, consumed, timed_out, phantom)."""
    prng = random.Random(to_seed)
    consumed = 0
    timed_out = 0
    phantom = 0
    cases = [("recv", data), None, ("recv", cancel)]
    while True:
        # A fresh per-iteration timeout (the After fiber self-terminates after
        # firing, so each is a one-shot that does not accumulate).
        cases[1] = ("recv", rtime.After(prng.uniform(0.0008, 0.003)))
        idx, payload = runloom.select(cases)
        if idx == 0:                       # data
            _v, ok = payload
            if not ok:
                return (DATA_DRAINED, consumed, timed_out, phantom)
            consumed += 1
        elif idx == 1:                     # timer -- transiently empty; re-select
            timed_out += 1
        else:                              # idx == 2: cancel broadcast (closed)
            _v, ok = payload
            if ok:
                # A value delivered on the cancel case after close, OR the cancel
                # case eating a data item -> a losing case was wrongly taken.
                phantom += 1
            return (CANCELLED, consumed, timed_out, phantom)


def coordinator(cancel, delay_seed):
    """Broadcast cancel mid-flight by CLOSING the cap-0 cancel channel: every
    parked selector's cancel recv wakes with ok=False at once."""
    prng = random.Random(delay_seed)
    runloom.sleep(prng.uniform(0.0005, 0.004))
    cancel.close()


def worker(H, wid, rng, state):
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        n = ITEMS_PER_GROUP
        base = (wid << 24) | 0x1
        # cap == n: the producer can deposit every item without parking, so no
        # send is ever "in flight" against a selector the cancel could revoke.
        data = runloom.Chan(n)
        cancel = runloom.Chan(0)

        js = sync.JoinSet()

        pseed = rng.getrandbits(48)
        cseed = rng.getrandbits(48)

        def run_producer(data=data, base=base, n=n, pseed=pseed):
            producer(data, n, base, pseed)
            return ("producer", 0, 0, 0)

        js.spawn(run_producer)
        js.spawn(lambda cancel=cancel, cseed=cseed:
                 (coordinator(cancel, cseed), ("coord", 0, 0, 0))[1])
        for s in range(SELECTORS):
            sseed = rng.getrandbits(48)
            js.spawn(lambda data=data, cancel=cancel, sseed=sseed:
                     selector(data, cancel, sseed))

        results = js.join_all()            # a lost cancel-wake stalls here

        consumed = 0
        timed_out = 0
        phantom = 0
        drained_sel = 0
        cancelled_sel = 0
        n_sel = 0
        for kind in results:
            tag = kind[0]
            if tag in ("producer", "coord"):
                continue
            bucket, c, t, p = kind
            n_sel += 1
            consumed += c
            timed_out += t
            phantom += p
            if bucket == DATA_DRAINED:
                drained_sel += 1
            else:
                cancelled_sel += 1

        # Drain whatever is still buffered in the closed data channel -- items no
        # selector reached before cancel terminated them.  consumed+residual must
        # account for every produced item (none lost to a cancelled case).
        residual = 0
        while True:
            r = data.try_recv()
            if r is None:
                break
            _v, ok = r
            if not ok:
                break
            residual += 1

        # Disjoint-bucket completeness: every selector terminated in EXACTLY one
        # bucket; the two totals must sum to the selector count.
        if drained_sel + cancelled_sel != n_sel:
            H.fail("buckets not disjoint/complete: drained={0} cancelled={1} "
                   "sum != selectors={2}".format(drained_sel, cancelled_sel,
                                                  n_sel))
            return
        if phantom:
            H.fail("PHANTOM: {0} cancel-resolving select(s) also carried a data "
                   "value (a losing case was wrongly taken)".format(phantom))
            return
        if consumed + residual != n:
            H.fail("conservation broken: consumed={0} + residual={1} != "
                   "produced={2} (item lost to a cancelled case)".format(
                       consumed, residual, n))
            return

        state["produced"][slot] += n
        state["consumed"][slot] += consumed
        state["residual"][slot] += residual
        state["timed_out"][slot] += timed_out
        state["drained_sel"][slot] += drained_sel
        state["cancelled_sel"][slot] += cancelled_sel
        state["selectors"][slot] += n_sel
        H.op(wid, consumed)
        H.task_done(wid)


def setup(H):
    H.state = {
        "produced": [0] * 1024,
        "consumed": [0] * 1024,
        "residual": [0] * 1024,
        "timed_out": [0] * 1024,
        "drained_sel": [0] * 1024,
        "cancelled_sel": [0] * 1024,
        "selectors": [0] * 1024,
    }


def body(H):
    # Each worker round spawns SELECTORS+2 sub-fibers; cap concurrent workers so
    # the live-fiber count stays bounded at scale.
    H.run_pool(H.funcs, worker, H.state, max_concurrent=3000)


def post(H):
    p = sum(H.state["produced"])
    c = sum(H.state["consumed"])
    r = sum(H.state["residual"])
    t = sum(H.state["timed_out"])
    ds = sum(H.state["drained_sel"])
    cs = sum(H.state["cancelled_sel"])
    sel = sum(H.state["selectors"])
    H.log("produced={0} consumed={1} residual={2} timed_out={3} "
          "drained_sel={4} cancelled_sel={5} selectors={6}".format(
              p, c, r, t, ds, cs, sel))
    H.check(p > 0, "no items produced")
    H.check(c + r == p,
            "conservation broken: consumed={0} + residual={1} != produced={2} "
            "(item lost or double-consumed through the 3-way select)".format(
                c, r, p))
    H.check(ds + cs == sel,
            "disjoint-bucket law broken: drained_sel={0} + cancelled_sel={1} "
            "!= selectors={2}".format(ds, cs, sel))
    H.require_no_lost("three-way-select-cancel")


if __name__ == "__main__":
    harness.main("p317_three_way_select_cancel", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="select{data, After timer, cancel-broadcast}; "
                          "disjoint-bucket multiset: consumed+residual==produced, "
                          "buckets sum to selectors, no item eaten by a "
                          "cancelled case")
