"""Isolate the _Py_DecRefShared source: cross-hub refcounting of a SHARED
hub-0-owned INSTANCE vs a per-goroutine LOCAL instance.

Each goroutine runs a tight loop of method calls.  A method call pushes `self`
as a NEW reference (incref) then decrefs it after CALL.  If `self` is a shared
instance owned by hub 0, every call on hubs 1..H-1 is a cross-thread
incref/decref -> _Py_TryIncRefShared CAS + _Py_DecRefShared.  If `self` is local
(created on the running hub), the refcount stays on the biased local fast path.

MODE=shared : all goroutines call methods on ONE instance built on hub 0.
MODE=local  : each goroutine builds its own instance.

Prediction: shared -> _Py_DecRefShared high; local -> ~0.  This is the same
shape as p207's per-op H.op()/a.send()/b.recv() on the shared harness+channels.
"""
import os
import sys

sys.path.insert(0, "src")
import runloom
import runloom_c


class Obj:
    __slots__ = ("x",)

    def __init__(self):
        self.x = 0

    def bump(self):
        return self.x


H = int(os.environ.get("VH", "32"))
G = int(os.environ.get("VG", "64000"))
ITERS = int(os.environ.get("VITERS", "20000"))
MODE = os.environ.get("MODE", "shared")
SHARED = Obj()
if os.environ.get("IMMORTAL") == "1":
    runloom_c.immortalize(SHARED)        # A1b: freeze the shared instance's refcount


def worker(i):
    o = SHARED if MODE == "shared" else Obj()
    n = 0
    for _ in range(ITERS):
        n += o.bump()
        n += o.bump()
        n += o.bump()
        n += o.bump()
    return n


def root():
    runloom_c.go_n(worker, G, indexed=True)


runloom.run(H, root)
sys.stderr.write("done MODE={0} H={1} G={2} ITERS={3}\n".format(MODE, H, G, ITERS))
