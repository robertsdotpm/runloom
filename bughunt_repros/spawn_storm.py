"""Spawn storms: 100k fibers, recursive spawning, counters verified."""
import sys, itertools, threading
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
N = int(sys.argv[2]) if len(sys.argv) > 2 else 100_000

counter = itertools.count()   # thread-safe-ish? no -- use bytearray per docs
slots = bytearray(N)


def main():
    def w(i):
        slots[i] = 1
    for i in range(N):
        runloom.fiber(w, i)


runloom.run(HUBS, main)
missing = N - sum(slots)
assert missing == 0, "flat storm: %d fibers never ran" % missing
print("flat storm hubs=%d N=%d OK" % (HUBS, N))

# recursive fan-out: each fiber spawns 2 children until depth d; total 2^d - 1
DEPTH = 15
total = 2 ** DEPTH - 1
slots2 = bytearray(total)
idx_lock = threading.Lock()
next_idx = [0]


def main2():
    def node(depth):
        with idx_lock:
            i = next_idx[0]
            next_idx[0] += 1
        slots2[i] = 1
        if depth + 1 < DEPTH:
            runloom.fiber(node, depth + 1)
            runloom.fiber(node, depth + 1)
    runloom.fiber(node, 0)


runloom.run(HUBS, main2)
got = sum(slots2)
assert got == total, "recursive storm: ran %d expected %d" % (got, total)
print("recursive storm hubs=%d total=%d OK" % (HUBS, total))
