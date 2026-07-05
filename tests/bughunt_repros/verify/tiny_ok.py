import os
os.environ["RUNLOOM_STACK_ARENA"] = "0"
import runloom, runloom_c as rc
res=[]
def worker():
    res.append(sum(range(10)))
def main():
    rc.mn_fiber(worker, 32768)
    for _ in range(1000):
        rc.sched_yield()
        if res: break
    print("res:", res)
runloom.run(2, main)
print("DONE")
