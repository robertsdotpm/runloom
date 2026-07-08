"""A depth-2 target-order bug on ONE channel, amid POS_NOISE INDEPENDENT noise
channels -- the workload that separates POS from PCT (QA-steal-V2 #18).

Target channel `cht` (buffered): producer P0 sends "a0" then "a1"; producer P1
sends "b"; consumer C receives all three (FIFO => the scheduled SEND order).  The
BUG order is ["a0","b","a1"] -- P1's send landing BETWEEN P0's two sends, which
needs the baton to leave P0 after a0 and return after b: exactly ONE preemption
(PCT depth 2).  A fixed priority (depth 1) can only make [a0,a1,b] or [b,a0,a1].
Every op on cht touches obj=cht (dpor_id 1), so POS keys the bug's operations.

POS_NOISE (K) independent producer/consumer channel pairs add 2K fibers doing
benign, bug-irrelevant work on DISJOINT objects (dpor_id 2..K+1).  They inflate
the number of baton grant steps -- so PCT's single change point must land at the
right step among MANY, diluting its hit probability as K grows -- WITHOUT touching
cht, so POS never re-rolls the target operations for them: POS's hit probability
is (ideally) independent of K.  Sweep K to see PCT's samples-to-bug climb while
POS's stays ~flat.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
import runloom_c

K = int(os.environ.get("POS_NOISE", "0"))        # independent noise channels
MN = int(os.environ.get("POS_NOISE_M", "2"))     # ops per noise channel

runloom_c.mn_init(3 + 2 * K)                      # one fiber per hub
cht = runloom_c.Chan(8)                           # target (buffered): dpor_id 1
st = {"seen": None}


def p0():
    cht.send("a0")
    runloom_c.sched_sleep(0)     # force a baton grant point between the two sends
    cht.send("a1")               # (a buffered send does not block, so would not)


def p1():
    cht.send("b")


def cons():
    st["seen"] = [cht.recv()[0] for _ in range(3)]   # recv() -> (value, ok); FIFO


# Noise channels are UNBUFFERED so each op rendezvous-blocks -> yields the baton
# -> a real grant STEP.  More noise => more steps => PCT's single change point
# must land at the right one among more, diluting its hit rate.  cht is untouched.
noise = [runloom_c.Chan(0) for _ in range(K)]        # dpor_id 2..K+1


def nprod(ch):
    for i in range(MN):
        ch.send(i)


def ncons(ch):
    for _ in range(MN):
        ch.recv()


runloom_c.mn_fiber(p0)
runloom_c.mn_fiber(p1)
runloom_c.mn_fiber(cons)
for ch in noise:
    runloom_c.mn_fiber(lambda c=ch: nprod(c))
    runloom_c.mn_fiber(lambda c=ch: ncons(c))
runloom_c.mn_run()
runloom_c.mn_fini()

seen = st["seen"]
print("BUG order=%s" % seen if seen == ["a0", "b", "a1"] else "OK order=%s" % seen)
