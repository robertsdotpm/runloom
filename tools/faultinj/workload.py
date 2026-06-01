"""Compact pygo workload for fault-injection sweeps.

Exercises the paths whose cleanup branches the coverage report flagged as
untested: channel send/recv, the single-thread scheduler, the M:N scheduler
(hub threads -> eventfd/epoll), and a timed park (timerfd / deadline heap).
Prints WORKLOAD_OK on success; any crash/hang on an injected failure is a
cleanup-path bug.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import pygo
import pygo_core


def producer_consumer():
    ch = pygo_core.Chan(4)

    def prod():
        for i in range(8):
            ch.send(i)
        ch.close()

    def cons():
        total = 0
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            total += v
        return total

    pygo_core.go(prod)
    pygo_core.go(cons)
    pygo_core.run()


def mn_round():
    pygo_core.mn_init(2)
    for i in range(16):
        pygo_core.mn_go(lambda n=i: n * 2)
    pygo_core.mn_run()
    pygo_core.mn_fini()


def timed_park():
    def g():
        pygo.sleep(0.001)
    pygo.go(g)
    pygo.run()


def main():
    producer_consumer()
    mn_round()
    timed_park()
    assert pygo_core._self_check(0) == 0, "self_check failed after injected fault"
    print("WORKLOAD_OK")


if __name__ == "__main__":
    main()
