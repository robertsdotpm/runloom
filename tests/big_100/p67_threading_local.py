"""big_100 / 67 -- threading.local compatibility probe.

threading.local() is keyed by OS thread.  Under M:N a goroutine can run on
different hub threads over its life, so a value it stashed in a threading.local
may NOT be there after a scheduling point (a sibling on the same hub may have
overwritten it, or the goroutine migrated to a hub that never saw it).  This
project measures that: it sets tls.value, yields, and checks what it reads.

It does not fail on a migration/leak (that is a documented consequence of the
M:N model -- threading.local is hub-local, not goroutine-local); it fails only
on CORRUPTION (a value that was never any goroutine's id), and reports the
observed leak rate so the semantics are explicit.

Stresses: PyThreadState / TLS assumptions, OS-thread migration.
"""
import threading

import harness
import runloom

TLS = threading.local()


def setup(H):
    H.state = {"checks": [0] * 1024, "leaks": [0] * 1024, "valid": [0]}


def worker(H, wid, rng, state):
    while H.running():
        TLS.value = wid
        runloom.yield_now()
        if rng.random() < 0.5:
            runloom.sleep(0.0003)
        try:
            got = TLS.value
        except AttributeError:
            got = None                  # migrated to a hub that never set it
        state["checks"][wid & 1023] += 1
        if got != wid:
            state["leaks"][wid & 1023] += 1
            # CORRUPTION check: a leaked value must still be a plausible worker
            # id (someone's wid) -- never garbage.
            if got is not None and not (0 <= got < H.funcs):
                H.fail("threading.local CORRUPTION: read {0!r} (wid {1})".format(
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
    H.log("threading.local: {0} checks, {1} migration/leaks ({2:.1f}%) -- "
          "this is EXPECTED (TLS is hub-local under M:N), only corruption "
          "fails".format(checks, leaks, pct))


if __name__ == "__main__":
    harness.main("p67_threading_local", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="probe threading.local semantics under hub migration")
