"""Contended SELECT + concurrent CLOSE -- the exact shape of README Finding A's 4
bugs (select under contention with a racing close: SIGSEGV, bare -1 unpack,
dropped/duplicated values).  TWO consumers each select() over TWO channels while
a closer closes both, racing two producers.

INVARIANT (cross-consumer token conservation): each sent token is received by AT
MOST ONE consumer (no duplicate delivery across consumers), every received value
is a real sent token (no phantom/NULL), and no crash.  Prints "BUG ..." on a
violation, else "OK ...".
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
import runloom_c

runloom_c.mn_init(5)
ch1 = runloom_c.Chan()
ch2 = runloom_c.Chan()
SENT = {10, 20}
got = []                 # (consumer_id, value) pairs


def prod(ch, tok):
    try:
        ch.send(tok)
    except ValueError:
        pass


def consumer(cid):
    try:
        idx, (val, ok) = runloom_c.select([("recv", ch1), ("recv", ch2)])
    except Exception:
        return
    if ok:
        got.append((cid, val))


def closer():
    ch1.close()
    ch2.close()


runloom_c.mn_fiber(lambda: prod(ch1, 10))
runloom_c.mn_fiber(lambda: prod(ch2, 20))
runloom_c.mn_fiber(lambda: consumer(0))
runloom_c.mn_fiber(lambda: consumer(1))
runloom_c.mn_fiber(closer)
runloom_c.mn_run()
runloom_c.mn_fini()

# conservation: no token to two consumers; every value real
bug = None
vals = [v for _cid, v in got]
seen = set()
for cid, v in got:
    if v not in SENT:
        bug = "PHANTOM value %r to consumer %d; got=%r" % (v, cid, got)
        break
    if v in seen:
        bug = "DUPLICATE delivery of %r (to two consumers); got=%r" % (v, got)
        break
    seen.add(v)

if bug:
    print("BUG", bug)
else:
    print("OK got=%s" % got)
