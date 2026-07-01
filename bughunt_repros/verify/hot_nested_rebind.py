import threading
from runloom._hot import hot
def make():
    count = 0
    def handler():
        def bump():
            nonlocal count
            count += 1
        bump()
        return count
    return handler
h = make()
hh = hot(h)
print("wrapped (cells split)?", hh is not h, getattr(hh, "__runloom_hot__", False))
results = {}
def worker(tid):
    last = None
    for _ in range(1000):
        last = hh()
    results[tid] = last
ts = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
[t.start() for t in ts]; [t.join() for t in ts]
print("per-thread final counts:", results)
# expected (shared cell, undecorated): total across threads = 4000, max ~4000
plain = make()
results2 = {}
def worker2(tid):
    last = None
    for _ in range(1000):
        last = plain()
    results2[tid] = last
ts = [threading.Thread(target=worker2, args=(i,)) for i in range(4)]
[t.start() for t in ts]; [t.join() for t in ts]
print("undecorated per-thread final counts (shared cell):", results2)
