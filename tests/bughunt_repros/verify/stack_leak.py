import sys, runloom_c
h = runloom_c.fiber(lambda: None)
runloom_c.run()
for _ in range(1000):
    h.stack()
before = sys.getallocatedblocks()
N = 100_000
for _ in range(N):
    h.stack()
print('block delta after', N, 'calls:', sys.getallocatedblocks() - before)
