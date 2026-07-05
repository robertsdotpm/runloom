# Amplified proof of the submit-before-pending_inc under-count:
# RUNLOOM_TEST_SPAWN_GAP_US widens the (real, otherwise sub-us) window between
# runloom_mn_hub_submit(h,g) and runloom_mn_pending_inc(h) in mn_fiber_core.
# The spawned noop completes on another hub inside the window, so
# sum(hubs.pending) dips to 0 while the SPAWNER fiber is still running ->
# runloom_mn_run()'s `total == 0` exit fires early (the exact invariant the
# code comments claim cannot happen).
import sys, time
sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo_patch/src")
import runloom
print("using", runloom.__file__)

def noop():
    pass

runloom.mn_init(4)
state = {"done": False, "i": 0}
def root():
    for i in range(50):
        state["i"] = i
        runloom.mn_fiber(noop)   # each spawn: submit .. [5ms gap] .. pending_inc
    state["done"] = True
runloom.mn_fiber(root)
t0 = time.monotonic()
runloom.mn_run()
dt = time.monotonic() - t0
done_at_return = state["done"]
i_at_return = state["i"]
time.sleep(2.0)
if not done_at_return:
    print(f"BUG CONFIRMED: mn_run returned after {dt*1000:.1f}ms at spawn #{i_at_return} "
          f"while the root fiber was still running; root finished later: {state['done']}")
    sys.exit(1)
print("no early exit (root done at mn_run return)")
