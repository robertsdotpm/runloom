"""Workload: parallel producer/consumer channel storm + a gc collector.

PAIRS independent (producer, consumer) goroutines hammer buffered channels in
parallel across hubs while a collector loops gc.collect (stop-the-world).  This
exercises cross-hub channel park/wake (the wake_safe / call_soon-FIFO paths) and
the chan/scheduler interaction with the world periodically stopped.  Every
producer sends exactly MSGS and every consumer receives exactly MSGS over a
buffered channel, so the program always terminates -- any hang is a real bug.
Params from env; prints PASS on clean completion.

Note: runloom_c.Chan.recv() returns (value, ok) (Go-style); the value is unused
here.
"""
import gc
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", "src"))
import runloom_c

import os as _crashos
if _crashos.environ.get("RUNLOOM_CRASH"):
    runloom_c.install_crash_handler(_crashos.environ["RUNLOOM_CRASH"],
                                 _crashos.environ.get("RUNLOOM_CRASH_FILE"))

NHUB = int(os.environ.get("HH_NHUB", "4"))
PAIRS = int(os.environ.get("HH_PAIRS", "16"))          # producer/consumer pairs
MSGS = int(os.environ.get("HH_MSGS", "200"))           # messages per pair
CAP = int(os.environ.get("HH_CHAN_CAP", "1"))          # channel buffer
GC = os.environ.get("HH_GC", "1") != "0"

done = runloom_c.Chan(2 * PAIRS + 1)
stop = [False]


def mk(cap):
    ch = runloom_c.Chan(cap)

    def producer():
        for _ in range(MSGS):
            ch.send(1)
        done.send(1)

    def consumer():
        for _ in range(MSGS):
            ch.recv()
        done.send(1)
    return producer, consumer


def collector():
    n = 0
    while not stop[0]:
        gc.collect()
        n += 1
        runloom_c.sched_yield_classic()
    done.send(("gc", n))


runloom_c.mn_init(NHUB)
for _ in range(PAIRS):
    prod, cons = mk(CAP)
    runloom_c.mn_fiber(prod)
    runloom_c.mn_fiber(cons)
if GC:
    runloom_c.mn_fiber(collector)


def reaper():
    for _ in range(2 * PAIRS):
        done.recv()
    stop[0] = True
    if GC:
        done.recv()


runloom_c.mn_fiber(reaper)
runloom_c.mn_run()
runloom_c.mn_fini()
assert runloom_c._self_check(0) == 0, "self_check failed"
print("PASS")
