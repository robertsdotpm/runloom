"""Token-conservation invariant over the SELECT + concurrent-CLOSE path -- the
historically bug-prone scheduler-core seam (README Finding A: NULL-on-close ->
SIGSEGV, bare -1 unpack, dropped/duplicated values).  Closed-world (channels
only), so chess_explore can drive it exhaustively.

A producer sends distinct tokens; a closer closes the channel concurrently; a
consumer drains via select().  INVARIANT: every value the consumer receives with
ok=1 is a real, not-yet-seen sent token (no phantom/NULL, no duplicate), and the
process does not crash.  Prints "BUG ..." on a violation, else "OK ...".
A SIGSEGV/abort under some interleaving shows up to the explorer as CRASH.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
import runloom_c

N = int(os.environ.get("CHESS_N", "2"))
SENT = [10 + i for i in range(N)]
runloom_c.mn_init(3)
ch = runloom_c.Chan()
recvd = []


def producer():
    for t in SENT:
        try:
            ch.send(t)
        except ValueError:          # send on closed -> correct, not a bug
            break


def consumer():
    for _ in range(N):
        try:
            idx, (val, ok) = runloom_c.select([("recv", ch)])
        except Exception:
            break
        if ok:
            recvd.append(val)
        else:
            break                   # channel closed-and-drained


def closer():
    ch.close()                      # races the sends + the select recv


runloom_c.mn_fiber(producer)
runloom_c.mn_fiber(consumer)
runloom_c.mn_fiber(closer)
runloom_c.mn_run()
runloom_c.mn_fini()

# conservation: received values are distinct + all real sent tokens
seen = set()
bug = None
for v in recvd:
    if v not in SENT:
        bug = "PHANTOM value %r received (not sent); recvd=%r" % (v, recvd)
        break
    if v in seen:
        bug = "DUPLICATE value %r received; recvd=%r" % (v, recvd)
        break
    seen.add(v)

if bug:
    print("BUG", bug)
else:
    print("OK recvd=%s" % recvd)
