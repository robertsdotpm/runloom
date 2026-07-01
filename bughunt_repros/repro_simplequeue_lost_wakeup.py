"""CoSimpleQueue lost wakeup: a parked getter whose waiter record is consumed
by put()'s wake_one(), but whose item is stolen by a fast-path get from another
fiber, re-parks WITHOUT re-registering in _waiters.  A later put() then sees no
waiters and never wakes it -> the getter hangs forever while the item sits in
the queue.  Deterministic on the single-thread scheduler.

Expected (stdlib SimpleQueue semantics): consumer returns "y".
Observed (bug): hang -> the watchdog prints DEADLOCK and os._exit(2).
"""
import os
import sys
import threading as _th_pre   # real threading captured before patch for watchdog

import runloom.monkey as monkey
monkey.patch()

import time
import queue
import runloom_c as rc

q = queue.SimpleQueue()
results = []


def consumer():
    results.append(q.get())      # untimed get -> must eventually return "y"


def villain():
    time.sleep(0.05)             # (patched -> cooperative) let consumer park
    q.put("x")                   # wake_one() pops consumer's waiter record
    got = q.get()                # fast-path steal of "x" before consumer runs
    assert got == "x"
    time.sleep(0.05)             # consumer wakes, finds queue empty, RE-PARKS
                                 # (its record was consumed -> unregistered)
    q.put("y")                   # _waiters empty -> no wake; "y" sits in queue
    time.sleep(0.3)
    print("qsize after second put:", q.qsize(), "results:", results)


def watchdog():
    time.sleep(5)
    if not results:
        print("DEADLOCK: consumer never got item; qsize=%d" % q.qsize())
        sys.stdout.flush()
        os._exit(2)


_wd = _th_pre.Thread(target=watchdog, daemon=True)
_wd.start()

rc.fiber(consumer)
rc.fiber(villain)
rc.run()
print("DONE, results:", results)
