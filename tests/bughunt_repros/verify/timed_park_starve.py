import runloom_c, time, sys
state = {"woke": False, "b_iters": 0}
def A():
    r = runloom_c.park(timeout=0.05)
    state["woke"] = True
    print("A resumed, timed_out =", r, "after", time.monotonic() - t0, "s")
def B():
    while not state["woke"]:
        state["b_iters"] += 1
        runloom_c.sched_yield()
        if time.monotonic() - t0 > 3.0:
            print("BUG: A's 50ms park timeout never fired after 3s of B yielding (b_iters=%d)" % state["b_iters"])
            sys.exit(1)
t0 = time.monotonic()
runloom_c.fiber(A)
runloom_c.fiber(B)
runloom_c.run()
