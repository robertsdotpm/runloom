"""Verify pair-affinity placement EMPIRICALLY: a goroutine runs pinned to its
hub's OS thread, so threading.get_native_id() inside it is ground truth for
which hub actually ran it (captures any work-steal re-distribution too).

Spawn 2*H*reps goroutines via fiber_n(indexed=True) -- the same indexed path
run_pool uses under RUNLOOM_HARNESS_GON=1, where py_index == wid -- and check
whether adjacent (2k, 2k+1) pairs land on the SAME hub thread.

Run twice: RUNLOOM_PAIR_AFFINITY unset (expect pairs split) and =1 (expect
co-located).
"""
import os
import sys
import threading

sys.path.insert(0, "src")
import runloom
import runloom_c

H = int(os.environ.get("VH", "8"))
REPS = int(os.environ.get("VREPS", "20"))
N = 2 * H * REPS
slots = [0] * N


def w(i):
    # busy a touch so the pair actually rendezvous-runs, not just spawns
    s = 0
    for _ in range(2000):
        s += i
    slots[i] = threading.get_native_id()


def root():
    runloom_c.fiber_n(w, N, indexed=True)


runloom.run(H, root)

pairs_same = sum(1 for k in range(N // 2) if slots[2 * k] == slots[2 * k + 1])
total_pairs = N // 2
hubs_used = len(set(slots))
print("RUNLOOM_PAIR_AFFINITY={0!r}  H={1}  pairs={2}".format(
    os.environ.get("RUNLOOM_PAIR_AFFINITY"), H, total_pairs))
print("  pairs co-located on ONE hub: {0}/{1}  ({2:.0f}%)".format(
    pairs_same, total_pairs, 100.0 * pairs_same / total_pairs))
print("  distinct hub threads used  : {0} (expect {1})".format(hubs_used, H))
