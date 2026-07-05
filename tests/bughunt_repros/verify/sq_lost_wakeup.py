import os, sys, threading as _th_pre
import runloom.monkey as monkey
monkey.patch()
import time, queue
import runloom_c as rc

q = queue.SimpleQueue()
results = []

def consumer():
    results.append(q.get())

def villain():
    time.sleep(0.05)       # let consumer park
    q.put("x")            # wake_one() pops consumer's waiter record
    assert q.get() == "x" # fast-path steal before consumer runs
    time.sleep(0.05)       # consumer wakes, finds empty, re-parks unregistered
    q.put("y")            # no waiter registered -> lost wakeup
    time.sleep(0.3)
    print("qsize after second put:", q.qsize(), "results:", results)

def watchdog():
    time.sleep(5)
    if not results:
        print("DEADLOCK: consumer never got item; qsize=%d" % q.qsize()); sys.stdout.flush(); os._exit(2)

_th_pre.Thread(target=watchdog, daemon=True).start()
rc.fiber(consumer); rc.fiber(villain); rc.run()
print("DONE, results:", results)
