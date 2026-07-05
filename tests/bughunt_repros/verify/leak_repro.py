import runloom_c, gc
def rss():
    with open('/proc/self/status') as f:
        for l in f:
            if l.startswith('VmRSS'): return int(l.split()[1])
def noop(): pass
def cycle(nfib):
    runloom_c.mn_init(4)
    for _ in range(nfib): runloom_c.mn_fiber(noop)
    runloom_c.mn_run(); runloom_c.mn_fini()
for _ in range(20): cycle(100)
gc.collect(); r0 = rss()
for _ in range(300): cycle(100)
gc.collect(); r1 = rss()
print('rss %d->%d kB, %.1f kB/cycle' % (r0, r1, (r1-r0)/300.0))
