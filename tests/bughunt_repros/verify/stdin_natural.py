# Run as: printf 'a\nb\n' | python stdin_natural.py  (with a held-open pipe)
import os, sys, time, threading
import runloom.monkey as monkey
monkey.patch()
import runloom_c as rc
results = []

def reader():
    results.append(sys.stdin.readline())
    results.append(sys.stdin.readline())

def watchdog():
    time.sleep(5)
    if len(results) < 2:
        print("DEADLOCK: got %r" % (results,)); sys.stdout.flush(); os._exit(2)

threading.Thread(target=watchdog, daemon=True).start()
rc.fiber(reader); rc.run()
print("DONE:", results)
