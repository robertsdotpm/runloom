#!/usr/bin/env python3
"""repro_select.py -- harder deterministic-replay probe for the controlled M:N
scheduler.  Stresses paths the single-channel demo does not: select() over
several channels, multiple producers, AND a goroutine that spawns more
consumers mid-run (dynamic spawn determinism).  No sched_sleep -- timers are
wall-clock and belong to a separate logical-clock lever, not this one.

Each consumer selects over all channels and records its (chan, value) stream;
the run signature is every consumer's stream in cid order.  Same RUNLOOM_MN_SEED
must reproduce one signature.  House style: .format().

Usage: repro_select.py [seeds] [reps]   (defaults 8 seeds, 6 reps)
"""
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

WORKLOAD = r"""
import sys; sys.path.insert(0, 'src'); import runloom_c
HUBS, NCH, NPROD_EACH = 3, 3, 4
NPROD = NCH * 2
runloom_c.mn_init(HUBS)
chans = [runloom_c.Chan() for _ in range(NCH)]
done = runloom_c.Chan()      # producers signal completion here
rec = {}                     # cid -> list of (chan_idx, val)

def consumer(cid):
    seen = []
    rec[cid] = seen
    openidx = list(range(NCH))
    while openidx:
        idx, (val, ok) = runloom_c.select([("recv", chans[i]) for i in openidx])
        ci = openidx[idx]
        if not ok:
            openidx.pop(idx)     # that channel closed -> stop selecting on it
            continue
        seen.append((ci, val))

def producer(ch_idx, base):
    c = chans[ch_idx]
    for k in range(NPROD_EACH):
        c.send(base * 1000 + k)
    done.send(1)                 # join signal

# initial consumers
for cid in range(4):
    runloom_c.mn_go(lambda cid=cid: consumer(cid))

# a spawner goroutine that adds two more consumers mid-run
def spawner():
    runloom_c.mn_go(lambda: consumer(100))
    runloom_c.mn_go(lambda: consumer(101))
runloom_c.mn_go(spawner)

# producers: each channel gets two producers with distinct bases
def boot():
    for ci in range(NCH):
        runloom_c.mn_go(lambda ci=ci: producer(ci, ci * 2 + 1))
        runloom_c.mn_go(lambda ci=ci: producer(ci, ci * 2 + 2))
    # join ALL producers (deterministic) before closing -- no send/close race
    for _ in range(NPROD):
        done.recv()
    for c in chans:
        c.close()
runloom_c.mn_go(boot)

runloom_c.mn_run(); runloom_c.mn_fini()

# signature: every consumer's stream, cid-sorted; plus conservation check
parts = []
allvals = []
for cid in sorted(rec):
    stream = rec[cid]
    parts.append("{}:{}".format(cid, ",".join("{}/{}".format(i, v) for i, v in stream if v >= 0)))
    allvals.extend(v for _, v in stream if v >= 0)
sig = "|".join(parts)
# expected values: NCH channels, each 2 producers, NPROD_EACH each
expect = []
for ci in range(NCH):
    for base in (ci * 2 + 1, ci * 2 + 2):
        for k in range(NPROD_EACH):
            expect.append(base * 1000 + k)
ok = sorted(allvals) == sorted(expect)
import hashlib
h = hashlib.sha1(sig.encode()).hexdigest()[:12]
print(h + ("|OK" if ok else "|LOST"))
"""


def run_once(seed):
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["PYTHONPATH"] = os.path.join(ROOT, "src")
    env["RUNLOOM_MN_SEED"] = str(seed)
    try:
        out = subprocess.run([sys.executable, "-c", WORKLOAD], env=env, cwd=ROOT,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    line = out.stdout.decode(errors="replace").strip().splitlines()
    last = line[-1] if line else "ERR"
    sig, sep, status = last.partition("|")
    if sep == "" or status != "OK":
        return "ERR(" + last + ")"
    return sig


def main():
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 6
    print("repro_select: {} seeds x {} reps (select + multi-chan + mid-run spawn)".format(seeds, reps))
    stable = 0
    for s in range(1, seeds + 1):
        sigs = [run_once(s) for _ in range(reps)]
        uniq = sorted(set(sigs))
        ok = len(uniq) == 1 and not uniq[0].startswith("ERR") and uniq[0] not in ("TIMEOUT",)
        stable += ok
        if ok:
            print("  seed {:>3}: STABLE  {}".format(s, uniq[0]))
        else:
            print("  seed {:>3}: VARIES  {} distinct: {}".format(s, len(uniq), uniq))
    print("-" * 60)
    print("  {}/{} seeds reproduce identically across {} reps".format(stable, seeds, reps))
    return 0 if stable == seeds else 1


if __name__ == "__main__":
    sys.exit(main())
