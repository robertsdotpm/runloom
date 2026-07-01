"""SIGINT delivered to a busy scheduler: should raise KeyboardInterrupt promptly, not hang."""
import os, sys, signal, threading, time
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 4

def main():
    def busy():
        i = 0
        while True:
            i += 1
            if i % 10000 == 0:
                runloom.yield_now()
    for _ in range(8):
        runloom.fiber(busy)

def killer():
    time.sleep(1.0)
    os.kill(os.getpid(), signal.SIGINT)

threading.Thread(target=killer, daemon=True).start()
t0 = time.time()
try:
    runloom.run(HUBS, main)
    print("run returned without KeyboardInterrupt after %.1fs" % (time.time() - t0))
except KeyboardInterrupt:
    print("KeyboardInterrupt after %.2fs (sent at 1.0s)" % (time.time() - t0))
