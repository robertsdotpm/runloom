"""Record a concurrent operation history for one primitive, on the REAL M:N
scheduler, then emit it as JSON for the linearizability checker.

Two timing modes:
  * --seeded S : run under RUNLOOM_MN_SEED=S (DST Plane 1 -- the seeded baton
      serialises fiber segments into one deterministic grant order) and timestamp
      events with a LOGICAL clock (a shared monotonic counter, safe to bump under
      the baton exactly as the mn-sim determinism suite bumps a shared completion
      list).  The whole history is then a pure function of S, so a non-linearizable
      finding reduces to a single integer -- replay it with the same S.
  * --wallclock : no seed; genuine real-time overlap across hub OS threads,
      timestamped with time.monotonic_ns().  This is the original
      record_history.py regime, kept for real-overlap stress.

Every goroutine appends only to its OWN event list (never shared-mutated); only
the logical clock counter is shared, and only touched at cooperative points under
the baton.  Observables are ints only (contract #9: object ids / addresses are
not seed-stable).

Usage:
  record.py <primitive> [--seeded S | --wallclock] [--hubs H] [--procs K]
            [--ops M] [--cap C] [--out FILE]
  primitive in: chan mutex rwmutex semaphore waitgroup event
"""
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "..", "src"))

import runloom_c  # noqa: E402


class Recorder(object):
    def __init__(self, seeded):
        self.seeded = seeded
        self.clock = [0]
        self.t0 = time.monotonic_ns()
        self.logs = {}

    def register(self, gid):
        self.logs[gid] = []

    def stamp(self):
        if self.seeded:
            v = self.clock[0]
            self.clock[0] = v + 1
            return v
        return time.monotonic_ns() - self.t0

    def timed(self, gid, op, args, fn, classify):
        """Record one op: stamp the call, run fn (which may park), stamp the
        return, classify fn's result into (res_tag, ret_values)."""
        call = self.stamp()
        result = fn()
        ret = self.stamp()
        res, rets = classify(result)
        self.logs[gid].append({
            "proc": gid, "op": op, "args": [int(a) for a in args],
            "res": res, "rets": [int(x) for x in rets],
            "call": call, "ret": ret})

    def events(self):
        evs = []
        for gid in sorted(self.logs):
            evs.extend(self.logs[gid])
        evs.sort(key=lambda e: e["call"])
        return evs


# --------------------------------------------------------------------- run

def record(primitive, seed, seeded, hubs, procs, ops, cap, out_path):
    import workloads  # local import: needs sys.path set above
    if seeded:
        os.environ["RUNLOOM_MN_SEED"] = str(seed)
    else:
        os.environ.pop("RUNLOOM_MN_SEED", None)

    rec = Recorder(seeded)
    thunks, meta = workloads.build(primitive, seed, hubs, procs, ops, cap, rec)

    runloom_c.mn_init(hubs)
    for gid, fn in thunks:
        runloom_c.mn_fiber(fn)
    runloom_c.mn_run()
    runloom_c.mn_fini()
    assert runloom_c._self_check(0) == 0, "self_check failed after run"

    events = rec.events()
    meta.update({"primitive": primitive, "seed": seed, "seeded": bool(seeded),
                 "hubs": hubs, "nevents": len(events)})
    payload = {"events": events, "meta": meta}
    if out_path:
        with open(out_path, "w") as fh:
            json.dump(payload, fh)
    else:
        json.dump(payload, sys.stdout)
        sys.stdout.write("\n")
    return payload


def main(argv):
    primitive = argv[1] if len(argv) > 1 else "chan"
    seed = 0
    seeded = True
    hubs = None
    procs = None
    ops = None
    cap = None
    out = None
    i = 2
    while i < len(argv):
        a = argv[i]
        if a == "--seeded":
            seeded = True
            seed = int(argv[i + 1])
            i += 2
        elif a == "--wallclock":
            seeded = False
            i += 1
        elif a == "--hubs":
            hubs = int(argv[i + 1])
            i += 2
        elif a == "--procs":
            procs = int(argv[i + 1])
            i += 2
        elif a == "--ops":
            ops = int(argv[i + 1])
            i += 2
        elif a == "--cap":
            cap = int(argv[i + 1])
            i += 2
        elif a == "--out":
            out = argv[i + 1]
            i += 2
        else:
            i += 1
    if hubs is None:
        hubs = 2 + (seed % 3)          # {2,3,4}: always multi-hub (H>=2)
    record(primitive, seed, seeded, hubs, procs, ops, cap, out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
