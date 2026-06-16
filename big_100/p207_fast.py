"""p207 + the FULL Axis-A fix: immortalize shared instances AND bind the per-op
shared-instance lookups to locals.

After immortalizing H + the channels (p207_immortal.py) the residual bottleneck
was per-op ATTRIBUTE LOOKUP on the shared H instance -- every H.op()/H.running()/
H.check()/a.send()/b.recv() probes H's (or the channel's) instance dict each
iteration (_PyObject_GenericGetAttrWithDict / _Py_dict_lookup_threadsafe) and
returns a fresh reference to the looked-up method value (still cross-hub).

The classic hot-loop fix: bind each shared-instance method to a LOCAL once at
worker entry.  The per-op path then calls a local bound-method object -- no dict
probe, and the bound method (created once, owned by the running hub) is local so
its refcount stays on the biased fast path.  Identical workload + identical
correctness check to p207; only the lookup site moves out of the loop.

RUNLOOM_IMMORTALIZE_SHARED=1 (default here) immortalizes H + channels in setup.
"""
import os
import struct

import harness
import runloom
import runloom_c

import p207_park_wake_pingpong as p207

PINGS_PER_ROUND = p207.PINGS_PER_ROUND


def pinger(H, wid, rng, pairs):
    a, b = pairs[wid >> 1]
    a_send = a.send                 # bind shared-instance methods ONCE
    b_recv = b.recv
    H_running = H.running
    H_op = H.op
    H_check = H.check
    H_task_done = H.task_done
    round_range = H.round_range
    pack = struct.pack
    half = wid >> 1
    seq = 0
    for _ in round_range():
        for _ in range(PINGS_PER_ROUND):
            if not H_running():
                break
            seq += 1
            tok = pack("<II", half, seq)
            try:
                a_send(tok)
            except Exception:
                return
            val, ok = b_recv()
            if not ok:
                return
            if not H_check(val == tok,
                           "ping-pong cross-talk pair={0} seq={1}: sent {2!r} "
                           "got {3!r}".format(half, seq, tok, val)):
                return
            H_op(wid)
        H_task_done(wid)


def ponger(H, wid, rng, pairs):
    a, b = pairs[wid >> 1]
    a_recv = a.recv
    b_send = b.send
    H_running = H.running
    H_op = H.op
    while H_running():
        val, ok = a_recv()
        if not ok:
            return
        try:
            b_send(val)
        except Exception:
            return
        H_op(wid)


def worker(H, wid, rng, pairs):
    if (wid & 1) == 0:
        pinger(H, wid, rng, pairs)
    else:
        ponger(H, wid, rng, pairs)


def setup(H):
    p207.setup(H)
    if os.environ.get("RUNLOOM_IMMORTALIZE_SHARED", "1") == "1":
        runloom_c.immortalize(H)
        for a, b in H.state:
            runloom_c.immortalize(a)
            runloom_c.immortalize(b)


def body(H):
    n = (H.funcs // 2) * 2
    H.run_pool(n, worker, H.state)


if __name__ == "__main__":
    harness.main("p207_fast", body, setup=setup,
                 default_funcs=4000,
                 describe="p207 ping-pong: immortalized shared instances + "
                          "per-op lookups bound to locals")
