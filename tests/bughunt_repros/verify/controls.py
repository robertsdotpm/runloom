import runloom, runloom.sync as gsync

# Control 1: runloom.fiber (runtime._fiber_full) under M:N run(2)
ran1 = {"v": False}
def w1(): ran1["v"] = True
def main1():
    runloom.fiber(w1)
    runloom.sleep(0.3)
runloom.run(2, main1)
print("control runloom.fiber under run(2):", ran1["v"], "(expect True)")
