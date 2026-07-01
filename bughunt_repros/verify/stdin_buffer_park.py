import os, sys, threading as _th_mod
r, w = os.pipe()
os.write(w, b"line-one\nline-two\n")
os.dup2(r, 0)
sys.stdin = os.fdopen(0, "r")
import runloom.monkey as monkey
monkey.patch()
import time
import runloom_c as rc
results = []

def reader():
    results.append(input())
    results.append(input())   # parks: kernel pipe empty, data in io buffer

def watchdog():
    time.sleep(5)
    if len(results) < 2:
        print("DEADLOCK: got %r" % (results,)); sys.stdout.flush(); os._exit(2)

_th_mod.Thread(target=watchdog, daemon=True).start()
rc.fiber(reader); rc.run()
print("DONE:", results)
