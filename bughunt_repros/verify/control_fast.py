"""Control: identical mutator, but spawn via fiber_fast which does NOT read
fn.__dict__ / the grow-down store.  If this does NOT crash while the grow-down
version does, the crash is attributable to the borrowed store read in
m_fiber_grow, not to concurrent dict mutation per se.
"""
import threading
import runloom

K = "runloom_stack"

def fn():
    pass

STOP = threading.Event()

def mutator():
    d = fn.__dict__
    while not STOP.is_set():
        d[K] = [1 << 16, 64]

def spawner(n):
    f = runloom.fiber_fast
    for _ in range(n):
        f(fn)

def main():
    fn.__dict__[K] = [1 << 16, 64]
    for _ in range(8):
        runloom.fiber_fast(lambda: spawner(200000))

muts = [threading.Thread(target=mutator, daemon=True) for _ in range(2)]
for t in muts:
    t.start()
try:
    runloom.run(8, main)
finally:
    STOP.set()
print("control completed without crash")
