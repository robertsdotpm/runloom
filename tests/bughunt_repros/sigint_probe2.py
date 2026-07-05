"""SIGINT variants under run(1): sleeping fibers, repeated SIGINTs."""
import os, sys, signal, threading, time
import runloom

mode = sys.argv[1]

if mode == "sleepers":
    def main():
        def sleeper():
            runloom.sleep(30)
        for _ in range(4):
            runloom.fiber(sleeper)
    def killer():
        time.sleep(1.0)
        os.kill(os.getpid(), signal.SIGINT)
    threading.Thread(target=killer, daemon=True).start()
    t0 = time.time()
    try:
        runloom.run(1, main)
        print("run returned normally after %.1fs" % (time.time() - t0))
    except KeyboardInterrupt:
        print("KeyboardInterrupt after %.2fs" % (time.time() - t0))

elif mode == "repeat":
    def main():
        def busy():
            i = 0
            while True:
                i += 1
                if i % 10000 == 0:
                    runloom.yield_now()
        for _ in range(3):
            runloom.fiber(busy)
    def killer():
        for k in range(10):
            time.sleep(0.5)
            os.kill(os.getpid(), signal.SIGINT)
    threading.Thread(target=killer, daemon=True).start()
    t0 = time.time()
    try:
        runloom.run(1, main)
        print("run returned normally after %.1fs" % (time.time() - t0))
    except KeyboardInterrupt:
        print("KeyboardInterrupt after %.2fs (3 busy fibers, SIGINT every 0.5s)" % (time.time() - t0))
