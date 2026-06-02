"""Record a concurrent channel operation history for linearizability checking.

Runs producers/consumers as REAL goroutines on the multi-hub M:N scheduler
(multiple OS threads, GIL off on 3.13t) so operations genuinely overlap in
real time, and timestamps each op's call/return.  Writes the history as JSON
for the Porcupine checker (tools/lincheck/porcupine), which decides whether
some linearization consistent with those real-time intervals satisfies the
sequential FIFO-channel spec -- the gold-standard correctness property for a
concurrent object.

Each goroutine writes only to its OWN list (pre-created, never shared-mutated)
so recording itself races nothing; the lists are merged after mn_run.

Usage:  record_history.py [out.json] [nhubs] [nprod] [nperprod] [cap]
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import pygo_core

T0 = time.monotonic_ns()


def now():
    return time.monotonic_ns() - T0


def record(out_path, nhubs, nprod, nper, cap, nselect=0):
    ch = pygo_core.Chan(cap)
    done = pygo_core.Chan(nprod)             # buffered barrier: producers never block on it
    nconsumers = nprod                       # balanced
    nselect = min(nselect, nconsumers)       # how many consumers receive via select(...)
    logs = {}                                # gid -> list of op records (per-goroutine)
    ngor = nprod + nconsumers + 1
    for g in range(ngor):
        logs[g] = []

    def producer(gid, base):
        log = logs[gid]
        for i in range(nper):
            v = base * 1000 + i              # globally unique value
            t = now()
            ch.send(v)
            log.append({"proc": gid, "op": "send", "value": v, "result": "ok",
                        "call": t, "ret": now()})
        done.send(1)

    def closer(gid):
        log = logs[gid]
        for _ in range(nprod):               # wait for every producer to finish
            done.recv()
        t = now()
        ch.close()
        log.append({"proc": gid, "op": "close", "value": -1, "result": "ok",
                    "call": t, "ret": now()})

    def consumer(gid):
        log = logs[gid]
        while True:
            t = now()
            v, ok = ch.recv()
            r = now()
            log.append({"proc": gid, "op": "recv", "value": v if ok else -1,
                        "result": "ok" if ok else "closed", "call": t, "ret": r})
            if not ok:
                break

    def select_consumer(gid):
        # Receive via select() over [the real channel, a private never-ready
        # channel].  The private case never fires, so every received value is
        # still a FIFO recv on `ch` -- recorded as op="recv" so the SAME
        # sequential-FIFO Porcupine spec validates it unchanged.  The point is
        # the *path*: each blocking select installs Phase-2 waiters on BOTH
        # channels and must abort/clean up the never-firing one, which is
        # exactly where chan.c's four historical select bugs lived.
        log = logs[gid]
        idle = pygo_core.Chan(1)             # never sent to / never closed
        while True:
            t = now()
            idx, res = pygo_core.select([("recv", ch), ("recv", idle)])
            r = now()
            assert idx == 0, "idle select case fired (idx={0})".format(idx)
            v, ok = res
            log.append({"proc": gid, "op": "recv", "value": v if ok else -1,
                        "result": "ok" if ok else "closed", "call": t, "ret": r})
            if not ok:
                break

    pygo_core.mn_init(nhubs)
    for p in range(nprod):
        pygo_core.mn_go(lambda gid=p, base=p: producer(gid, base))
    for c in range(nconsumers):
        fn = select_consumer if c < nselect else consumer
        pygo_core.mn_go(lambda gid=nprod + c, fn=fn: fn(gid))
    pygo_core.mn_go(lambda gid=nprod + nconsumers: closer(gid))
    pygo_core.mn_run()
    pygo_core.mn_fini()
    assert pygo_core._self_check(0) == 0, "self_check failed after run"

    events = []
    for g in range(ngor):
        events.extend(logs[g])
    events.sort(key=lambda e: e["call"])

    sent = [e["value"] for e in events if e["op"] == "send"]
    recv = [e["value"] for e in events if e["op"] == "recv" and e["result"] == "ok"]
    meta = {"nhubs": nhubs, "nprod": nprod, "nper": nper, "cap": cap,
            "nselect": nselect,
            "total_sent": len(sent), "total_recv": len(recv),
            "lost": sorted(set(sent) - set(recv)),
            "dup": len(recv) != len(set(recv))}
    with open(out_path, "w") as fh:
        json.dump({"events": events, "meta": meta}, fh)
    print("[record] {0} events ({1} send, {2} recv via {3} plain + {4} select consumers) -> {5}".format(
        len(events), len(sent), len(recv), nconsumers - nselect, nselect, out_path))
    print("[record] sanity: lost={0} dup={1}".format(meta["lost"], meta["dup"]))
    return meta


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "history.json")
    nhubs = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    nprod = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    nper = int(sys.argv[4]) if len(sys.argv) > 4 else 6
    cap = int(sys.argv[5]) if len(sys.argv) > 5 else 2
    nselect = int(sys.argv[6]) if len(sys.argv) > 6 else 0
    record(out, nhubs, nprod, nper, cap, nselect)
    return 0


if __name__ == "__main__":
    sys.exit(main())
