import runloom_c, time, sys
from runloom import sync
ev = sync.Event()
state = {"done": False}
def A():
    r = ev.wait(timeout=0.05)
    state["done"] = True
    print("Event.wait returned", r, "after", round(time.monotonic() - t0, 4), "s")
def B():
    while not state["done"]:
        runloom_c.sched_yield()
        if time.monotonic() - t0 > 3.0:
            print("BUG: Event.wait(timeout=0.05) starved for 3s by yielding fiber")
            sys.exit(1)
t0 = time.monotonic()
runloom_c.fiber(A)
runloom_c.fiber(B)
runloom_c.run()
