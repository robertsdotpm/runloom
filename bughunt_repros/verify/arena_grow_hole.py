"""Verify: copy-on-grow munmaps a hole out of the RUNLOOM_STACK_ARENA arena.

Env (set before import): RUNLOOM_STACK_ARENA=1, RUNLOOM_STACK_ARENA_N=256.
A fiber on a SMALL arena-carved stack recurses through a C boundary (map)
with a yield at every level, so maybe_grow sees a deep saved sp at a resume
boundary and copy-grows the stack; runloom_coro_grow then munmaps the OLD
stack, which is a slice of the shared arena mapping -> a hole.

Detection: snapshot /proc/self/maps anonymous rw regions before and after;
a hole splits a previously-contiguous arena mapping into two pieces.
"""
import os
os.environ["RUNLOOM_STACK_ARENA"] = "1"
os.environ["RUNLOOM_STACK_ARENA_N"] = "256"

import sys
sys.setrecursionlimit(200000)

import runloom
import runloom_c as rc

STACK = 32768
GUARD = 4096


def anon_maps(minsize):
    out = []
    with open("/proc/self/maps") as f:
        for line in f:
            parts = line.split()
            rng, perms = parts[0], parts[1]
            path = parts[5] if len(parts) > 5 else ""
            if path:
                continue
            lo, hi = (int(x, 16) for x in rng.split("-"))
            if hi - lo >= minsize and "rw" in perms:
                out.append((lo, hi))
    return out


def rec(n):
    runloom.yield_now()
    if n == 0:
        for _ in range(6):
            runloom.yield_now()
        return 0
    return next(map(rec, (n - 1,))) + 1


result = []


def worker():
    try:
        result.append(rec(60))
    except RecursionError:
        result.append("recursion-budget")


def companion():
    # keep every hub's ready queue non-empty so yields are REAL swaps
    # (the trivial-switch fast path skips the resume boundary otherwise)
    while not result:
        runloom.yield_now()


def main():
    before = anon_maps(1024 * 1024)
    for _ in range(8):
        rc.mn_fiber(companion)
    rc.mn_fiber(worker, STACK)
    for _ in range(200000):
        rc.sched_yield()
        if result:
            break
    for _ in range(50):
        rc.sched_yield()
    after = anon_maps(1024 * 1024)
    print("worker result:", result)
    holes = 0
    for lo, hi in before:
        inside = sorted([(a, b) for a, b in after if lo <= a and b <= hi])
        if len(inside) > 1:
            for (a1, b1), (a2, b2) in zip(inside, inside[1:]):
                holes += 1
                print("HOLE punched in formerly-contiguous mapping "
                      "[%x-%x): gap %d bytes at %x" % (lo, hi, a2 - b1, b1))
    print("holes:", holes)


runloom.run(2, main)
print("DONE")
