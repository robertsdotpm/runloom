import time
import runloom, runloom.sync as gsync
ran = {"v": False}
def work(): ran["v"] = True
def main():
    gsync.fiber(work)
    runloom.sleep(0.5)
t0 = time.monotonic()
runloom.run(2, main)
print("run(2) done in %.2fs; sync.fiber target ran?" % (time.monotonic()-t0), ran["v"], "(expected True)")
