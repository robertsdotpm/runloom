"""CoEvent lost registration: set() snapshots-and-empties _waiters, then
clear() resets the flag before the woken waiter runs.  The waiter's re-check
loop sees _flag False and RE-PARKS -- but it is no longer in _waiters and never
re-appends itself, so EVERY LATER set() misses it: wait() hangs forever.

stdlib semantics (Event built on Condition.wait_for): the waiter re-registers
in the condition each time, so a later set() always wakes it.

Expected: waiter returns after the second set().
Observed (bug): hang -> watchdog prints DEADLOCK, exit 2.
"""
import os
import sys
import threading as _th

import runloom.monkey as monkey
monkey.patch()

import time
import runloom_c as rc

ev = _th.Event()          # patched -> CoEvent
results = []


def waiter():
    ev.wait()             # untimed wait
    results.append(True)


def setter():
    time.sleep(0.05)      # let waiter park
    ev.set()              # snapshot pops waiter's parker, wakes it...
    ev.clear()            # ...but flag is False again before the waiter runs
    time.sleep(0.05)      # waiter runs: flag False -> re-parks, UNREGISTERED
    ev.set()              # should wake the waiter; _waiters is empty -> lost
    time.sleep(0.3)
    print("flag:", ev.is_set(), "results:", results)


def watchdog():
    time.sleep(5)
    if not results:
        print("DEADLOCK: waiter never woke despite final set()")
        sys.stdout.flush()
        os._exit(2)


_wd = _th.Thread(target=watchdog, daemon=True)
_wd.start()

rc.fiber(waiter)
rc.fiber(setter)
rc.run()
print("DONE, results:", results)
