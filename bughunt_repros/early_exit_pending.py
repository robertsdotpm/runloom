# Repro: runloom_mn_run() can return while a fiber is still running.
# Root cause: runloom_mn_fiber_core does runloom_mn_hub_submit(h, g) BEFORE
# runloom_mn_pending_inc(h).  A spawned trivial fiber can be drained, run and
# pending_complete'd (h->pending -= 1) by another hub before the spawner's
# pending_inc lands, so sum(pending) transiently reads 0 with the SPAWNER
# fiber still running -> mn_run's `total == 0` exit fires early.
import runloom, time, sys, threading

TRIALS = int(sys.argv[1]) if len(sys.argv) > 1 else 200

def noop():
    pass

bugs = 0
for t in range(TRIALS):
    runloom.mn_init(8)
    state = {"done": False, "spawned": 0}
    def root():
        # spawn many trivial fibers; each spawn opens the submit->inc window
        for i in range(20000):
            runloom.mn_fiber(noop)
        state["done"] = True
    runloom.mn_fiber(root)
    runloom.mn_run()
    done_at_return = state["done"]
    if not done_at_return:
        # give the still-running root fiber time to finish -> proves mn_run
        # returned while it was mid-flight
        deadline = time.time() + 2.0
        while not state["done"] and time.time() < deadline:
            time.sleep(0.005)
        print(f"trial {t}: BUG mn_run returned early; root done after wait={state['done']}")
        bugs += 1
        runloom.mn_fini()
        break
    runloom.mn_fini()
print("bugs:", bugs)
