"""big_100 / 66 -- ContextVars isolation probe.

Each goroutine sets a contextvar to its own unique value, then does deep nested
calls, yields and sleeps, and reads the var back.  In an asyncio-style model the
value would always be its OWN; runloom's M:N goroutines share the hub thread's
context with no per-goroutine copy, so the value can leak from a sibling (see
FINDINGS BUG #7).  This project MEASURES that leak rate instead of failing, and
fails only on corruption -- a value that was never any goroutine's id.

Stresses: contextvars behaviour across yields/sleeps under M:N.
"""
import contextvars

import harness
import runloom

CV = contextvars.ContextVar("big100_cv")


def deep(rng, depth):
    if depth <= 0:
        if rng.random() < 0.5:
            runloom.yield_now()
        else:
            runloom.sleep(0.0005)
        return CV.get(None)
    return deep(rng, depth - 1)


def setup(H):
    H.state = {"checks": [0] * 1024, "leaks": [0] * 1024}


def worker(H, wid, rng, state):
    while H.running():
        CV.set(wid)
        got = deep(rng, rng.randint(2, 12))     # depth stays well under ceiling
        state["checks"][wid & 1023] += 1
        if got != wid:
            state["leaks"][wid & 1023] += 1
            if got is not None and not (0 <= got < H.funcs):
                H.fail("contextvar CORRUPTION: read {0!r} (wid {1})".format(
                    got, wid))
                return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    leaks = sum(H.state["leaks"])
    pct = (100.0 * leaks / checks) if checks else 0.0
    H.log("contextvars: {0} checks, {1} cross-goroutine leaks ({2:.1f}%) -- "
          "runloom goroutines share the hub context (FINDINGS BUG #7); only "
          "corruption fails".format(checks, leaks, pct))


if __name__ == "__main__":
    harness.main("p66_contextvars", body, setup=setup, post=post,
                 default_funcs=5000,
                 describe="measure contextvar cross-goroutine leakage under M:N")
