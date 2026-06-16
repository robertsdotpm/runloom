"""p207 + A1b experiment: immortalize the shared hub-0-owned instances.

Identical workload to p207_park_wake_pingpong, but when RUNLOOM_IMMORTALIZE_SHARED=1
the per-op shared instances -- the harness H and every channel -- are made
immortal in setup(), BEFORE the worker pool fans out across hubs.  Each per-op
H.op()/H.running()/H.check()/a.send()/b.recv() pushes `self` as a new reference;
on a shared hub-0-owned instance that is a cross-hub _Py_TryIncRefShared /
_Py_DecRefShared atomic.  Immortalizing freezes those refcounts so the
incref/decref become no-ops.  This is the lever the hub-scaling experiments
showed actually removes the profile's ~20% _Py_DecRefShared (pair affinity did
not -- the token handoff is negligible; the shared-instance method calls are the
cost).  See docs/dev/HUB_SCALING.md.
"""
import os

import harness
import runloom_c

import p207_park_wake_pingpong as p207


def setup(H):
    p207.setup(H)                       # builds pairs into H.state, registers channels
    if os.environ.get("RUNLOOM_IMMORTALIZE_SHARED") == "1":
        runloom_c.immortalize(H)
        for a, b in H.state:
            runloom_c.immortalize(a)
            runloom_c.immortalize(b)


if __name__ == "__main__":
    harness.main("p207_immortal", p207.body, setup=setup,
                 default_funcs=4000,
                 describe="p207 ping-pong with the shared H + channels "
                          "immortalized (RUNLOOM_IMMORTALIZE_SHARED=1)")
