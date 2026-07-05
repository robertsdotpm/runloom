"""Timer.Stop() vs the fire fiber under M:N (GIL off): _stopped/_fired/_gen
are plain attributes with no lock/atomic.  Go guarantees: Stop() == True =>
the timer will NOT fire.  Here the fire fiber can pass its _stopped/_gen
check on hub A while Stop() runs on hub B -> Stop returns True AND the
channel still receives a value."""
import runloom
import runloom.time

violations = 0
TRIALS = 3000

def main():
    global violations
    for i in range(TRIALS):
        t = runloom.time.NewTimer(0.001)
        runloom.sleep(0.001)         # land Stop() right at the fire instant
        stopped = t.Stop()
        runloom.sleep(0.002)         # give a racing try_send time to land
        got = t.c.try_recv()
        if stopped and got is not None:
            violations += 1
    print("Stop()==True but timer fired anyway: %d / %d trials" %
          (violations, TRIALS))

runloom.run(4, main)
if violations:
    print("BUG confirmed: Go's Stop()->True 'will not fire' contract violated")
