import runloom_c, time, sys
state = {"woke": False}
def A():
    r = runloom_c.park(timeout=0.05)
    state["woke"] = True
    print("A resumed, timed_out =", r, "after", round(time.monotonic() - t0, 4), "s")
def B():
    while not state["woke"]:
        runloom_c.yield_()
        if time.monotonic() - t0 > 3.0:
            print("BUG with yield_ too"); sys.exit(1)
t0 = time.monotonic()
runloom_c.fiber(A)
runloom_c.fiber(B)
runloom_c.run()
