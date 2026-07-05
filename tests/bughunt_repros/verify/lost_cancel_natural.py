"""Natural (no settrace) stress for the WithCancel vs parent-cancel race.

Many threads concurrently create children of one parent while another thread
cancels it.  Contention on parent._children's per-object lock can stall an
append between the _err check and the append, so the child misses both the
immediate-propagation branch and the fanout snapshot.
After the run, every child whose creation STARTED must be either cancelled or
never created; a child with err() None while parent.err() is set = lost cancel.
"""
import sys
import threading
import time

from runloom import context

NTHREADS = 24
TRIALS = 400

def one_trial(trial):
    parent, pcancel = context.WithCancel(context.Background())
    start = threading.Barrier(NTHREADS + 1)
    stop = threading.Event()
    made = [[] for _ in range(NTHREADS)]

    def maker(i):
        start.wait()
        lst = made[i]
        while not stop.is_set():
            c, _ = context.WithCancel(parent)
            lst.append(c)

    def canceller():
        start.wait()
        # let makers get going, then cancel mid-storm
        time.sleep(0.0005)
        pcancel()
        stop.set()

    ts = [threading.Thread(target=maker, args=(i,)) for i in range(NTHREADS)]
    tc = threading.Thread(target=canceller)
    for t in ts: t.start()
    tc.start()
    for t in ts: t.join(20)
    tc.join(20)

    assert parent.err() is not None
    lost = []
    for lst in made:
        for c in lst:
            if c.err() is None:
                lost.append(c)
    return lost, sum(len(l) for l in made)

total = 0
for trial in range(TRIALS):
    lost, n = one_trial(trial)
    total += n
    if lost:
        print(f"trial {trial}: LOST CANCELLATION on {len(lost)} child(ren) "
              f"(out of {n} created this trial, {total} total)")
        c = lost[0]
        print("  parent cancelled, child.err() =", c.err())
        sys.exit(1)
print(f"no natural hit in {TRIALS} trials, {total} children created")
sys.exit(0)
