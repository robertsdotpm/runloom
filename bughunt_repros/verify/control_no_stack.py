"""Variant: brand-new foreign threads every cycle, so EACH one takes the lazy
runloom_sched_get() malloc + runloom_netpoll_wake_pump_arm() path, timed to
overlap mn_run()/mn_fini() teardown. Also measures RSS growth to size the
claimed per-thread sched 'leak'."""
import threading
import time
import os
import runloom_c as rc


def rss_kb():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])
    return -1


handles = []
hlock = threading.Lock()

deadline = time.time() + 12
cycle = 0
rss_start = None
total_threads = 0
while time.time() < deadline:
    cycle += 1

    go = threading.Event()

    def poller():
        go.wait()
        # first g.stack() on THIS thread = lazy sched malloc + pump arm
        for _ in range(200):
            with hlock:
                hs = list(handles)
            for h in hs:
                pass

    # fresh threads each cycle -> fresh lazy sched_get + arm on each
    ts = [threading.Thread(target=poller) for _ in range(4)]
    for t in ts:
        t.start()

    def make(idx):
        def body():
            g = rc.current_g()
            with hlock:
                handles.append(g)
                del handles[:-16]
            if idx == 0:
                go.set()          # release pollers mid-run, near teardown
            rc.sched_yield()
        return body

    rc.mn_init(3)
    for i in range(6):
        rc.mn_fiber(make(i))
    rc.mn_run()
    rc.mn_fini()           # teardown races the pollers' first sched_get/arm
    go.set()
    for t in ts:
        t.join()
    total_threads += len(ts)
    if cycle == 5:
        rss_start = rss_kb()

print("cycles:", cycle, "fresh foreign threads:", total_threads)
print("RSS after cycle5: %d kB, at end: %d kB, delta: %d kB over %d threads"
      % (rss_start, rss_kb(), rss_kb() - rss_start, total_threads))
print("OK: no crash")
