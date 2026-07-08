#!/usr/bin/env python3
"""cover_check.py -- Sometimes()/reachability assertion (QA-steal rank 6).

pygo has deep recovery code (work-stealing, deque-overflow fallback, cross-hub
g-slab balance, cold coro allocation) but a green fuzz/soak run gives no proof
the chaos actually DROVE execution into those paths -- it may be vacuous.  The
named counters (src/runloom_c/runloom_cover.h, exposed as runloom_c._cover_*)
make each interesting state an atom; this harness runs workloads shaped to reach
them, then FAILS if any REQUIRED state has zero hits.  The runtime analog of
pygo's model-mutation "teeth".

Build the ext with RUNLOOM_COVER=1 (add RUNLOOM_SHRINK=1 so the cap-based states
fire with far less work):
    RUNLOOM_COVER=1 RUNLOOM_SHRINK=1 python setup.py build_ext --inplace --force
    PYTHONPATH=src python tools/verify/cover_check.py

Wire runloom_c._cover_report() into lifefuzz / hang_hunter / soak the same way to
fail a session that never reached one of these rescue paths.
"""
import os
import sys

import runloom
import runloom_c as rc

# Reached by the workloads below in any real multi-hub run.
REQUIRED = ["steal_hit", "deque_full_fallback", "g_slab_spill",
            "g_slab_refill", "coro_pool_miss"]
# Config-gated: only fires under RUNLOOM_STEAL_WOKEN / RUNLOOM_PER_G_TSTATE.
OPTIONAL = ["global_runq_pull"]


def _work(i):
    s = 0
    for _ in range(30):
        s += i
    return s


def phase_regular(n):
    """Non-bulk spawns exercise the coro pool-miss cold path."""
    def main():
        for i in range(n):
            rc.fiber(lambda i=i: _work(i))
    runloom.run(4, main)


def phase_bulk(n):
    """A big imbalanced bulk pile makes idle hubs steal + slabs spill/refill."""
    def main():
        rc.fiber_n(lambda: _work(1), n)
    runloom.run(4, main)


def main():
    if not rc._cover_enabled():
        print("cover_check: ext NOT built with RUNLOOM_COVER=1 -- SKIP "
              "(rebuild: RUNLOOM_COVER=1 RUNLOOM_SHRINK=1 python setup.py build_ext --inplace --force)")
        return 0
    reg = int(os.environ.get("COVER_REG", "5000"))
    bulk = int(os.environ.get("COVER_BULK", "40000"))
    rc._cover_reset()
    phase_regular(reg)
    phase_bulk(bulk)
    rep = rc._cover_report()
    print("cover report (regular=%d, bulk=%d, 4 hubs):" % (reg, bulk))
    for k in sorted(rep):
        tag = "REQ" if k in REQUIRED else ("opt" if k in OPTIONAL else "   ")
        print("  [%s] %-22s %d" % (tag, k, rep[k]))
    missing = [k for k in REQUIRED if rep.get(k, 0) == 0]
    if missing:
        print("cover_check: FAIL -- states never reached: %s" % ", ".join(missing))
        return 1
    print("cover_check: PASS -- all %d required states reached" % len(REQUIRED))
    return 0


if __name__ == "__main__":
    sys.exit(main())
