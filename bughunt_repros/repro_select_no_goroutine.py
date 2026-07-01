"""Blocking select() with no goroutine context: chan.recv()/send() raise a
clean RuntimeError immediately, but select() has no chan_no_goroutine guard.
Expected-good behavior: immediate RuntimeError like ch.recv().
Observed: busy-spin (CPU pegged) in the phase1/phase2/park-noop retry loop.
"""
import sys, time, os
import runloom_c as rc

ch = rc.Chan()          # unbuffered, empty, nobody on the other side

# sanity: plain recv raises immediately
t0 = time.time()
try:
    ch.recv()
    print("recv: NO ERROR (bug)")
except RuntimeError as e:
    print("recv raised RuntimeError in %.3fs (good)" % (time.time() - t0))

t0 = time.time()
try:
    r = rc.select([("recv", ch)])       # blocking select, no default
    print("select returned", r)
except RuntimeError as e:
    print("select raised RuntimeError after %.3fs: %s" % (time.time() - t0, e))
except BaseException as e:
    print("select raised %r after %.3fs" % (e, time.time() - t0))
