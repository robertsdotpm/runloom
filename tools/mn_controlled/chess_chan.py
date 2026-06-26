"""Two INDEPENDENT channel pairs, for the DPOR partial-order-reduction demo.

producer/consumer on ch1 and producer/consumer on ch2.  A ch1 segment and a ch2
segment touch DISJOINT objects, so their relative order does not affect the
outcome -- interleavings differing only by swapping a ch1 op past a ch2 op are
EQUIVALENT.  DPOR collapses those; full enumeration counts them all.  Every run
is clean (every value delivered) -- the point is the REDUCTION, not a bug.

4 fibers on 4 hubs so each hub == exactly one fiber (so the toucher-hub is a
sound conflict identity for the Mazurkiewicz key).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
import runloom_c

M = int(os.environ.get("CHESS_M", "1"))
runloom_c.mn_init(4)
ch1 = runloom_c.Chan()
ch2 = runloom_c.Chan()
got = [0]


def prod(ch):
    for v in range(M):
        ch.send(v)


def cons(ch):
    for _ in range(M):
        ch.recv()
        got[0] += 1


runloom_c.mn_fiber(lambda: prod(ch1))
runloom_c.mn_fiber(lambda: cons(ch1))
runloom_c.mn_fiber(lambda: prod(ch2))
runloom_c.mn_fiber(lambda: cons(ch2))
runloom_c.mn_run()
runloom_c.mn_fini()

print("OK delivered=%d" % got[0])
