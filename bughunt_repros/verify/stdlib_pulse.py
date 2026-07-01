import os, sys, threading, time
ev = threading.Event()
results = []
def waiter():
    ev.wait(); results.append(True)
def setter():
    time.sleep(0.05)
    ev.set(); ev.clear()
    time.sleep(0.05)
    ev.set()
def watchdog():
    time.sleep(5)
    if not results:
        print("DEADLOCK (stdlib)"); sys.stdout.flush(); os._exit(2)
threading.Thread(target=watchdog, daemon=True).start()
w = threading.Thread(target=waiter); s = threading.Thread(target=setter)
w.start(); s.start(); w.join(); s.join()
print("DONE", results)
