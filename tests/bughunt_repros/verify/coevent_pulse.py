import os, sys, threading as _th
import runloom.monkey as monkey
monkey.patch()
import time
import runloom_c as rc

ev = _th.Event()   # CoEvent
results = []

def waiter():
    ev.wait(); results.append(True)

def setter():
    time.sleep(0.05)
    ev.set(); ev.clear()   # waiter's parker consumed; flag false again
    time.sleep(0.05)       # waiter re-parks, unregistered
    ev.set()               # lost: _waiters empty
    time.sleep(0.3)

def watchdog():
    time.sleep(5)
    if not results:
        print("DEADLOCK: waiter never woke despite final set()"); sys.stdout.flush(); os._exit(2)

_th.Thread(target=watchdog, daemon=True).start()
rc.fiber(waiter); rc.fiber(setter); rc.run()
print("DONE", results)
