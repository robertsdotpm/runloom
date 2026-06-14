"""big_100 / 208 -- offload pool: cancel near completion, then reuse.

Each goroutine submits blocking work via runloom.blocking(fn, payload) where fn
derives a deterministic result from its OWN payload (sha256 hexdigest).  Each
submission is wrapped in a cancellable scope: a timer races the offload, and
SOME submissions are abandoned (the goroutine moves on to a NEW offload) the
instant the timer wins -- modelling a cancel near completion.  Immediately after,
the goroutine submits a FRESH offload and verifies its result.

The hazards (FINDINGS #4 family):
  * a completed offload's result MUST match ITS OWN payload -- a cancelled
    sibling's result must never be delivered to the wrong owner;
  * the offload pool must not wedge (lost wakeup) -- forward progress never
    stalls (the harness watchdog catches a wedge).

Because runloom.blocking parks the goroutine until the worker returns, a "cancel"
here means: arrange for the offload to be slow, race it with a timer in a
select, and when the timer wins, DROP that result and submit a new one.  We run
the slow offload in a child goroutine that publishes its result on a channel, so
the parent can select(result, timeout) and abandon cleanly.

Stresses: runloom.blocking offload pool, park/wake on the blockpool wake, result
ownership across cancel+resubmit, no lost wakeup.
"""
import hashlib
import struct

import harness
import runloom
import runloom.time as rtime


def hash_payload(payload):
    """Run on the offload pool.  No scheduler ops allowed in here."""
    h = hashlib.sha256()
    # A handful of update rounds so the C work is non-trivial (it releases the
    # GIL), making the offload genuinely slow enough to sometimes lose the race.
    for _ in range(64):
        h.update(payload)
    return h.hexdigest()


def reference(payload):
    h = hashlib.sha256()
    for _ in range(64):
        h.update(payload)
    return h.hexdigest()


def offload_into(ch, payload):
    """Child goroutine: run the blocking hash, publish (payload, result)."""
    try:
        res = runloom.blocking(hash_payload, payload)
        ch.try_send((payload, res))
    except Exception:
        try:
            ch.try_send((payload, None))
        except Exception:
            pass


def worker(H, wid, rng, state):
    completed = state["completed"]
    cancelled = state["cancelled"]
    base = struct.pack("<I", wid)
    seq = 0
    for _ in H.round_range():
        if not H.running():
            break
        seq += 1
        payload = base + struct.pack("<I", seq) + bytes((seq * 7) & 0xFF
                                                         for _ in range(48))
        # cap-1 result channel; the child publishes there, we race a timer.
        ch = runloom.Chan(1)
        H.go(offload_into, ch, payload)

        # Sometimes give the offload a tiny deadline so the timer frequently
        # wins (a cancel near completion); other times a generous one so it
        # completes.  Either way the result, IF taken, must match this payload.
        deadline = 0.0005 if (seq & 3) == 0 else 0.05
        timer = rtime.After(deadline)
        idx, payload_ok = runloom.select([("recv", ch), ("recv", timer.c
                                                          if hasattr(timer, "c")
                                                          else timer)])
        if idx == 0:
            got_payload, res = payload_ok[0]
            if res is None:
                # offload errored; just count progress and move on
                cancelled[wid & 1023] += 1
            else:
                # The result MUST be this exact payload's digest -- never a
                # sibling's delivered to the wrong owner.
                if not H.check(got_payload == payload,
                               "offload owner mismatch wid={0} seq={1}".format(
                                   wid, seq)):
                    return
                if not H.check(res == reference(payload),
                               "offload result wrong wid={0} seq={1}".format(
                                   wid, seq)):
                    return
                completed[wid & 1023] += 1
                H.op(wid)
        else:
            # Timer won: abandon this offload's result. The child goroutine
            # still finishes and try_send's into the cap-1 channel (no block,
            # no leak); we just never read it.  Submit a FRESH offload now.
            cancelled[wid & 1023] += 1
            fresh = base + struct.pack("<I", seq) + b"FRESH-RESUBMIT-PAYLOAD"
            res = runloom.blocking(hash_payload, fresh)
            if not H.check(res == reference(fresh),
                           "resubmit result wrong wid={0} seq={1}".format(
                               wid, seq)):
                return
            H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"completed": [0] * 1024, "cancelled": [0] * 1024}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    completed = sum(H.state["completed"])
    cancelled = sum(H.state["cancelled"])
    H.check(completed + cancelled > 0,
            "no offloads ran at all (pool may have wedged)")
    H.check(completed > 0,
            "no offload ever completed correctly")
    H.log("completed={0} cancelled={1}".format(completed, cancelled))


if __name__ == "__main__":
    harness.main("p208_offload_cancel_reuse", body, setup=setup, post=post,
                 default_funcs=1000,
                 describe="offload, cancel near completion, resubmit; each "
                          "result matches its own payload, pool never wedges")
