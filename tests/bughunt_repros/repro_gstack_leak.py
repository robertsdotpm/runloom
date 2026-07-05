"""G.stack() leaks one str object per call (PyDict_SetItemString does not steal)."""
import sys
import runloom_c

h = runloom_c.fiber(lambda: None)   # spawn (not yet run) -> G handle
runloom_c.run()                     # let it finish; handle stays valid

# warm up
for _ in range(1000):
    h.stack()

before = sys.getallocatedblocks()
N = 100_000
for _ in range(N):
    h.stack()
after = sys.getallocatedblocks()
print("allocated block delta after", N, "G.stack() calls:", after - before)
