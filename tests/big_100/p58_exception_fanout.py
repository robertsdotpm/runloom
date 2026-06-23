"""big_100 / 58 -- exception fanout.

Each parent goroutine spawns a batch of children; each child either returns a
value or raises one of several exception types (chosen deterministically from
the seed).  Children report their outcome over a channel and the parent
aggregates -- verifying it sees exactly the expected number of failures and
that each exception's type and payload survived the trip.

Stresses: exception state across goroutines, traceback/payload integrity,
aggregation.
"""
import harness
import runloom

KIDS = 16


class WidgetError(Exception):
    pass


class GadgetError(Exception):
    pass


def child(idx, rng_seed, out):
    import random
    r = random.Random(rng_seed)
    try:
        roll = r.random()
        if roll < 0.5:
            out.send(("ok", idx, idx * idx))
        elif roll < 0.75:
            raise WidgetError("widget {0}".format(idx))
        else:
            raise GadgetError("gadget {0}".format(idx))
    except Exception as exc:                    # noqa: BLE001
        out.send(("err", idx, (type(exc).__name__, str(exc))))


def worker(H, wid, rng, state):
    while H.running():
        out = runloom.Chan(KIDS)
        base = rng.getrandbits(48)
        expected_err = 0
        import random
        for i in range(KIDS):
            seed = base + i
            r = random.Random(seed)
            if r.random() >= 0.5:
                expected_err += 1
            H.fiber(child, i, seed, out)
        errs = 0
        for _ in range(KIDS):
            tag, idx, payload = out.recv()[0]
            if tag == "err":
                errs += 1
                name, msg = payload
                if not H.check(name in ("WidgetError", "GadgetError")
                               and str(idx) in msg,
                               "corrupt exception payload: {0} {1}".format(
                                   name, msg)):
                    return
            else:
                if not H.check(payload == idx * idx,
                               "bad child result {0} for {1}".format(
                                   payload, idx)):
                    return
            H.op(wid)
        if not H.check(errs == expected_err,
                       "fanout error count {0} != expected {1} wid={2}".format(
                           errs, expected_err, wid)):
            return
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p58_exception_fanout", body, default_funcs=1000,
                 describe="children raise; parent aggregates exact failures")
