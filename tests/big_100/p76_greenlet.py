"""big_100 / 76 -- greenlet-style stack switch comparison.

If greenlet is installed, each goroutine runs a greenlet ping-pong -- a SECOND
stack-switching mechanism nested inside runloom's goroutine stack swap.  The
greenlet sequence runs to completion ATOMICALLY (no runloom scheduling point
between two greenlet switches), then the goroutine yields.  This is the only
safe ordering: interleaving the two stack switchers crashes (FINDINGS BUG #8).

Stresses: greenlet's C-stack switching coexisting with goroutine stacks at the
boundary; verifies the non-interleaving case works.

Preemption is disabled here: a preemptive goroutine switch can also fall in the
middle of a greenlet switch and crash (FINDINGS BUG #8), so greenlet coexistence
requires preemption off AND no cooperative yield mid-greenlet-sequence.
"""
import os
os.environ.setdefault("RUNLOOM_PREEMPT", "0")   # must be set before mn_init

import harness          # noqa: E402
import runloom          # noqa: E402

try:
    import greenlet
    HAVE = True
except ImportError:
    HAVE = False


def worker(H, wid, rng, state):
    while H.running():
        n = rng.randint(3, 12)
        produced = []
        received = []

        def gbody():
            for i in range(n):
                produced.append(i)
                main.switch(i)          # hand control back with the value

        main = greenlet.getcurrent()
        g = greenlet.greenlet(gbody)
        # Drive the greenlet to completion WITHOUT any runloom switch in
        # between -- the two stack switchers must not interleave (BUG #8).
        r = g.switch()
        while not g.dead:
            if r is not None:
                received.append(r)
            r = g.switch()
        if not H.check(produced == list(range(n)) == received,
                       "greenlet sequence wrong wid={0}: {1} / {2}".format(
                           wid, produced, received)):
            return
        H.op(wid)
        H.task_done(wid)         # only NOW is it safe to yield (in run_pool loop)


def body(H):
    if not HAVE:
        H.log("greenlet not installed -- nothing to compare")
        return
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p76_greenlet", body, default_funcs=1500,
                 describe="greenlet switches nested inside runloom goroutines")
