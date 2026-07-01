import runloom
from runloom.sync import Semaphore
state = {"b": False, "b2": False}
def main():
    sem = Semaphore(2)
    sem.acquire(1)
    def a():
        r = sem.acquire(2, timeout=0.3)
        print("a acquire ->", r)
    def b():
        sem.acquire(1)
        state["b"] = True
    runloom.fiber(a)
    runloom.sleep(0.05)
    runloom.fiber(b)
    runloom.sleep(3.0)
    print("after 3s, b_acquired =", state["b"])
    sem.release(1)          # a future release finally grants B
    runloom.sleep(0.1)
    print("after release, b_acquired =", state["b"])
runloom.run(1, main)
