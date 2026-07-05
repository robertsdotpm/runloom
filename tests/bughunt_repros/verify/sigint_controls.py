import os, signal, sys, threading, time
import runloom

mode = sys.argv[1]

def killer(n=1):
    def k():
        for _ in range(n):
            time.sleep(0.5); os.kill(os.getpid(), signal.SIGINT)
    threading.Thread(target=k, daemon=True).start()

t0 = time.time()
try:
    if mode == 'run4_busy':
        def main():
            def busy():
                i = 0
                while True:
                    i += 1
                    if i % 10000 == 0: runloom.yield_now()
            for _ in range(3): runloom.fiber(busy)
        killer(3)
        runloom.run(4, main)
    elif mode == 'run1_parked':
        def main():
            def sleeper():
                while True: runloom.sleep(0.1)
            for _ in range(3): runloom.fiber(sleeper)
        killer(3)
        runloom.run(1, main)
    elif mode == 'run1_one_sigint':
        def main():
            def busy():
                i = 0
                while True:
                    i += 1
                    if i % 10000 == 0: runloom.yield_now()
            for _ in range(8): runloom.fiber(busy)
        killer(1)
        runloom.run(1, main)
    print('%s: run returned normally after %.1fs' % (mode, time.time()-t0))
except KeyboardInterrupt:
    print('%s: KeyboardInterrupt propagated after %.1fs' % (mode, time.time()-t0))
