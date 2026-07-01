# Brute-force the unamplified window on the STOCK build: root spawns one noop
# per mn_run cycle; if mn_run returns before root set done, the race fired.
import sys, time
import runloom

def noop():
    pass

runloom.mn_init(8)
hits = 0
trials = 0
t_end = time.monotonic() + 15
state = {"done": False}
def root():
    for _ in range(200):
        runloom.mn_fiber(noop)
    state["done"] = True
while time.monotonic() < t_end:
    trials += 1
    state["done"] = False
    runloom.mn_fiber(root)
    runloom.mn_run()
    if not state["done"]:
        hits += 1
        print("EARLY EXIT on stock build, trial", trials)
        break
print(f"trials={trials} hits={hits}")
