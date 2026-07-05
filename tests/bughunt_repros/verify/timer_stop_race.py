import runloom, runloom.time
violations = 0
def main():
    global violations
    for i in range(3000):
        t = runloom.time.NewTimer(0.001)
        runloom.sleep(0.001)
        stopped = t.Stop()
        runloom.sleep(0.002)
        if stopped and t.c.try_recv() is not None:
            violations += 1
    print(violations, "violations")
runloom.run(4, main)
