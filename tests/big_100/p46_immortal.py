"""p46_mutex_torture + A1b probe: immortalize the shared monkey Lock + counter.

The monkey/aio analog of the p207 finding: thousands of goroutines across hubs
hammer ONE shared monkey-patched threading.Lock (`with lock:` pushes the shared
lock instance per op) + a shared counter list + the shared H.  All are hub-0-
owned instances -> cross-hub refcount per op.  RUNLOOM_IMMORTALIZE_SHARED=1
(default) freezes their refcounts.  Answers: does the monkey layer carry the
same shared-instance cross-hub refcount tax, and does immortalizing fix it?
"""
import os

import harness
import runloom_c

import p46_mutex_torture as p46


def setup(H):
    p46.setup(H)
    if os.environ.get("RUNLOOM_IMMORTALIZE_SHARED", "1") == "1":
        runloom_c.immortalize(H)
        runloom_c.immortalize(H.state["lock"])
        runloom_c.immortalize(H.state["counter"])


if __name__ == "__main__":
    harness.main("p46_immortal", p46.body, setup=setup, post=p46.post,
                 default_funcs=5000,
                 describe="p46 mutex torture with shared lock+counter+H immortalized")
