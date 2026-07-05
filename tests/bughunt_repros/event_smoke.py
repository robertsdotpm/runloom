import threading, time, sys
import runloom
def main():
    def f():
        ev = threading.Event()
        t0 = time.monotonic()
        r = ev.wait(0.3)
        print("timed wait ->", r, "after %.2fs" % (time.monotonic()-t0), flush=True)
        ev2 = threading.Event()
        def setter():
            time.sleep(0.2); ev2.set()
        th = threading.Thread(target=setter); th.start()
        t0 = time.monotonic()
        r = ev2.wait(5)
        print("foreign-set wait ->", r, "after %.2fs" % (time.monotonic()-t0), flush=True)
        th.join()
    runloom.fiber(f)
runloom.monkey.patch(); runloom.run(2, main); print("OK", flush=True)
