# Repro: single-thread sched_yield fast path ignores the TIMER heap.
# Fiber A parks with a timeout (runloom_c.park(timeout=0.05) -- the primitive
# runloom.sync Lock/Event timeouts build on).  Fiber B poll-loops on
# runloom_c.sched_yield().  The yield fast path (runloom_sched_parkwake.c.inc:93)
# checks ready/sleep/netpoll/blockpool but NOT s->timer_size, so it never
# returns to the drain loop and A's timeout never fires -> hang.
import runloom_c, time, sys

state = {"woke": False, "b_iters": 0}

def A():
    r = runloom_c.park(timeout=0.05)   # should time out after 50ms
    state["woke"] = True
    print("A resumed, timed_out =", r, "after", time.monotonic() - t0, "s")

def B():
    # cooperative poll loop: yields every iteration -- SHOULD let the
    # scheduler fire A's 50ms timer.
    while not state["woke"]:
        state["b_iters"] += 1
        runloom_c.sched_yield()
        if time.monotonic() - t0 > 3.0:
            print("BUG: A's 50ms park timeout never fired after 3s of B yielding",
                  "(b_iters=%d)" % state["b_iters"])
            sys.exit(1)

t0 = time.monotonic()
runloom_c.fiber(A)
runloom_c.fiber(B)
runloom_c.run()
print("OK: total", time.monotonic() - t0)
