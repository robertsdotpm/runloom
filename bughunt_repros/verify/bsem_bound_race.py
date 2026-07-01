"""Repro: CoBoundedSemaphore.release TOCTOU on the bound check.

sem = BoundedSemaphore(2); one acquire -> _value == 1 == initial-1.
Two concurrent release() calls: correct (stdlib) behavior is that EXACTLY ONE
raises ValueError (only one permit is owed).  With the racy unlocked check,
both can pass and _value ends at 3 > initial with no ValueError.
"""
import sys, threading as _pre  # noqa

USE_MONKEY = "--stock" not in sys.argv
if USE_MONKEY:
    import runloom.monkey as monkey
    monkey.patch()

import threading

ITERS = 20000
races = 0
first = None
for it in range(ITERS):
    sem = threading.BoundedSemaphore(2)
    sem.acquire()          # live permit count -> 1 (initial-1)
    start = threading.Event()
    results = []

    def rel():
        start.wait()
        try:
            sem.release()
            results.append("ok")
        except ValueError:
            results.append("err")

    t1 = threading.Thread(target=rel)
    t2 = threading.Thread(target=rel)
    t1.start(); t2.start()
    start.set()
    t1.join(); t2.join()

    if results.count("err") != 1:
        races += 1
        if first is None:
            val = getattr(sem, "_value", "?")
            first = (it, results[:], val)

print("mode:", "monkey" if USE_MONKEY else "stock")
print("iterations:", ITERS, "over-bound races (no ValueError):", races)
if first:
    print("first race at iter %d results=%s final _value=%r (bound=2)" % first)
sys.exit(1 if races else 0)
