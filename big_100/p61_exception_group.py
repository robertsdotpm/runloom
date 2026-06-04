"""big_100 / 61 -- ExceptionGroup workload.

Each parent goroutine fans out children that may raise one of two exception
types, collects the raised exceptions, and wraps them in a Python 3.11+
ExceptionGroup.  It then splits the group with `except*` and verifies each
sub-group has exactly the expected members -- exercising the modern exception
machinery on the goroutine scheduler.

Stresses: ExceptionGroup construction/splitting, traceback integrity.
"""
import harness
import runloom

KIDS = 12


class AlphaError(Exception):
    pass


class BetaError(Exception):
    pass


def child(idx, kind, out):
    try:
        if kind == 0:
            raise AlphaError("alpha-{0}".format(idx))
        elif kind == 1:
            raise BetaError("beta-{0}".format(idx))
        else:
            out.send(("ok", None))
            return
    except Exception as exc:                # noqa: BLE001
        out.send(("err", exc))


def worker(H, wid, rng, state):
    while H.running():
        out = runloom.Chan(KIDS)
        kinds = [rng.randrange(3) for _ in range(KIDS)]
        exp_alpha = kinds.count(0)
        exp_beta = kinds.count(1)
        for i, k in enumerate(kinds):
            H.go(child, i, k, out)
        excs = []
        for _ in range(KIDS):
            tag, payload = out.recv()[0]
            if tag == "err":
                excs.append(payload)
            H.op(wid)
        got_alpha = got_beta = 0
        if excs:
            try:
                raise ExceptionGroup("fanout", excs)
            except* AlphaError as eg:
                got_alpha = len(eg.exceptions)
            except* BetaError as eg:
                got_beta = len(eg.exceptions)
        if not H.check(got_alpha == exp_alpha and got_beta == exp_beta,
                       "group split wrong wid={0}: alpha {1}/{2} beta {3}/{4}"
                       .format(wid, got_alpha, exp_alpha, got_beta, exp_beta)):
            return
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, None)


if __name__ == "__main__":
    harness.main("p61_exception_group", body, default_funcs=1000,
                 describe="ExceptionGroup build + except* split from child tasks")
