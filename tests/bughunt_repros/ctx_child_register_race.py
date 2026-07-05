"""_CancelCtx.__init__ registration vs parent._cancel() fanout has no lock.
Aligns a WithCancel(parent) call against pcancel() on two hubs and sweeps
relative offsets; a child that observes err() is None after the parent was
cancelled proves a lost cancellation."""
import runloom
import runloom.context as ctx

TRIALS = 800

def main():
    missed = 0
    for i in range(TRIALS):
        parent, pcancel = ctx.WithCancel(ctx.Background())
        go = [False]
        child_box = [None]
        done = [0]
        off_a = i % 37
        off_b = (i * 11) % 37

        def creator(parent=parent, go=go, child_box=child_box, done=done, k=off_a):
            while not go[0]:
                runloom.yield_now()
            x = 0
            for _ in range(k):
                x += 1
            child_box[0] = ctx.WithCancel(parent)[0]
            done[0] += 1

        def canceller(pcancel=pcancel, go=go, done=done, k=off_b):
            while not go[0]:
                runloom.yield_now()
            x = 0
            for _ in range(k):
                x += 1
            pcancel()
            done[0] += 1

        runloom.fiber(creator)
        runloom.fiber(canceller)
        runloom.sleep(0.0002)
        go[0] = True
        while done[0] < 2:
            runloom.yield_now()
        child = child_box[0]
        if child.err() is None:
            missed += 1
    print("children that missed the parent's cancellation: %d / %d" %
          (missed, TRIALS))
    if missed:
        print("BUG confirmed: lost cancellation (child.done never closes)")

runloom.run(4, main)
