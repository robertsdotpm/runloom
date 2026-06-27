"""big_100 / 218 -- exception relay up a goroutine chain.

Each round builds a chain of D goroutines connected by request/result channels:
goroutine k forwards a request to k+1 and awaits its result; the DEEPEST raises
a tagged exception.  Exceptions don't auto-cross goroutines, so each level
passes the exception object (or its (type, args)) up through the result channel
and re-raises/wraps it.  At the top the worker verifies the original exception's
type + tagged args survived the D-hop relay and that a re-raised traceback is
non-empty.

Stresses: exception object identity + traceback across a multi-goroutine relay
under M:N; no frame leak.
"""
import gc

import harness
import runloom

# Chain depth.  ~8 hops as specced.
DEPTH = 8


class RelayError(Exception):
    """The tagged exception raised at the bottom of the chain."""


def setup(H):
    # Per-worker race-free slots: chains completed, and how many correctly
    # carried the exception identity to the top.
    H.state = {"relayed": [0] * H.funcs, "ok": [0] * H.funcs}


def chain_link(level, tag, req_ch, res_ch):
    """One goroutine in the chain.

    Waits for a request on req_ch, then:
      * if it is the deepest level, raises the tagged exception and sends it
        (wrapped as a value) up res_ch;
      * otherwise forwards a request down to the next link and relays whatever
        result/exception comes back UP through res_ch, re-raising-then-catching
        so the traceback is real (exceptions can't cross a channel as a raise).

    The result is sent up as ('ok', value) or ('exc', exc_object).
    """
    _v, ok = req_ch.recv()                  # wait until our parent kicks us
    if not ok:
        return                              # channel closed at teardown
    if level == DEPTH - 1:
        # Deepest: raise the tagged exception, catch it (so __traceback__ is
        # populated), and pass the live exception object up.
        try:
            raise RelayError(("tag", tag, level))
        except RelayError as exc:
            res_ch.send(("exc", exc))
        return

    # Spawn the child link + its channels, kick it, await its result.
    child_req = runloom.Chan(0)
    child_res = runloom.Chan(0)
    runloom.fiber(chain_link, level + 1, tag, child_req, child_res)
    child_req.send(None)                    # kick the child
    result, ok = child_res.recv()           # recv() -> (value, ok)
    if not ok:
        return
    kind, payload = result
    if kind == "exc":
        # Re-raise the relayed exception so THIS frame appears in its traceback,
        # then catch + forward it up (exceptions don't auto-cross goroutines).
        try:
            raise payload
        except RelayError as exc:
            res_ch.send(("exc", exc))
    else:
        res_ch.send(("ok", payload))


def worker(H, wid, rng, state):
    relayed = state["relayed"]
    ok = state["ok"]
    for _ in H.round_range():
        tag = wid * 1_000_003 + (relayed[wid] & 0xFFFF)
        top_req = runloom.Chan(0)
        top_res = runloom.Chan(0)
        runloom.fiber(chain_link, 0, tag, top_req, top_res)
        top_req.send(None)                  # kick the top of the chain
        result, rok = top_res.recv()        # recv() -> (value, ok)
        if not rok:
            return
        kind, payload = result
        relayed[wid] += 1
        # The relay must surface the tagged exception at the top with identity
        # intact: same type, same tagged args, and a non-empty traceback (it
        # was re-raised at every level).
        good = (
            kind == "exc"
            and isinstance(payload, RelayError)
            and isinstance(payload.args[0], tuple)
            and payload.args[0][0] == "tag"
            and payload.args[0][1] == tag
            and payload.__traceback__ is not None
        )
        if not H.check(good,
                       "exception identity lost after {0} hops wid={1}: "
                       "{2!r} args={3!r}".format(
                           DEPTH, wid, payload,
                           getattr(payload, "args", None))):
            return
        ok[wid] += 1
        # Drop the exception (and its traceback frame chain) so no frame leaks;
        # an occasional collect proves the relay frames are reclaimable.
        del payload
        if wid < 64 and rng.random() < 0.05:
            gc.collect()
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    gc.collect()
    relayed = sum(H.state["relayed"])
    ok = sum(H.state["ok"])
    H.check(relayed > 0, "no chains relayed")
    H.check(ok == relayed,
            "{0}/{1} chains preserved exception identity across {2} hops".format(
                ok, relayed, DEPTH))
    H.log("relayed={0} identity_ok={1} depth={2}".format(relayed, ok, DEPTH))


if __name__ == "__main__":
    harness.main("p218_exception_unwind_chain", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=2000,   # 9-deep chains -> design-tier cap (slow finishers past ~design)
                 describe="tagged exception relayed up a chain of D goroutines; "
                          "type+tag+traceback intact")
