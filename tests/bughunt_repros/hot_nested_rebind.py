"""@runloom.hot promises: "If the handler REBINDS a captured name (nonlocal
x; x = ...), per-core copies could drift, so runloom leaves it shared."
But _rebinds_capture() only scans the handler's OWN bytecode; a rebind done
in a NESTED function (which shares the same cell) is invisible, so hot()
splits the cell anyway and per-core copies silently diverge.
optimize("throughput") applies this automatically with NO decorator."""
import threading
from runloom._hot import hot

def make_counter_handler():
    count = 0
    def handler():
        def bump():
            nonlocal count      # rebind happens HERE, in the nested fn
            count += 1
        bump()
        return count
    return handler

h = make_counter_handler()
hh = hot(h)
print("hot() wrapped it (split cells)?", getattr(hh, "__runloom_hot__", False))

results = {}
def worker(tid, n):
    last = 0
    for _ in range(n):
        last = hh()
    results[tid] = last

ts = [threading.Thread(target=worker, args=(i, 1000)) for i in range(4)]
for t in ts: t.start()
for t in ts: t.join()
print("per-thread final counts:", results)
total_expected = 4000
print("a SHARED counter must end at %d on some thread; per-core copies "
      "each count independently" % total_expected)
if max(results.values()) < total_expected:
    print("BUG: captured counter silently became per-thread (cells were "
          "split despite a nonlocal rebind)")
