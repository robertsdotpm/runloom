"""Refcount conservation: unique object instances pushed through plain ops and
select (including heavy abort/retry churn on SEND cases), plus close with
buffered objects.  After the run + gc, every weakref must be dead."""
import gc
import sys
import weakref
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8

class Box:
    __slots__ = ("v", "__weakref__")
    def __init__(self, v):
        self.v = v

N = 2000
boxes = [Box(i) for i in range(N)]
refs = [weakref.ref(b) for b in boxes]
chans = [rc.Chan(2) for _ in range(4)]
out = []
out_mu = rc.Mutex()

def main():
    wg = WaitGroup(); wg.add(4)
    def producer(pid):
        try:
            for i in range(pid, N, 4):
                b = boxes[i]
                if i % 2 == 0:
                    rc.select([("send", c, b) for c in chans])
                else:
                    chans[i % 4].send(b)
        finally:
            wg.done()
    def consumer(cid):
        while True:
            idx, res = rc.select([("recv", c) for c in chans])
            v, ok = res
            if not ok:
                return
            out_mu.lock()
            out.append(v.v)
            done = len(out) >= N
            out_mu.unlock()
            if done:
                for c in chans:
                    try: c.close()
                    except ValueError: pass
                return
    for c in range(6):
        rc.mn_fiber(lambda cid=c: consumer(cid))
    for p in range(4):
        rc.mn_fiber(lambda pid=p: producer(pid))
    wg.wait()

runloom.run(HUBS, main)

assert sorted(out) == list(range(N)), "lost/dup values: %d unique of %d" % (len(set(out)), len(out))
del boxes
gc.collect(); gc.collect()
alive = [r for r in refs if r() is not None]
assert not alive, "LEAKED %d/%d objects through channel refcounting" % (len(alive), N)

# close with buffered objects frees them
b = Box(-1); wb = weakref.ref(b)
c = rc.Chan(4); c.send(b); del b
c.close()
del c
gc.collect()
assert wb() is None, "buffered object leaked after close+dealloc"
print("OK: refcount conserved through %d select/send/recv handoffs" % N)
