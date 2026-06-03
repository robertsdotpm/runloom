"""Offload — keep a hub alive across a non-cooperative call.

A goroutine that enters a long pure-C / pure-compute call has no yield
point, so on its own it would monopolise its hub until it returns.
pygo.blocking(fn, ...) hands such a call to a worker pool and parks the
goroutine until it's done, so the other goroutines on that hub keep
running.  (pygo.monkey.offload is the same thing under the monkey API,
and the monkey `heavy` category does this automatically for big
hashlib/zlib/... calls.)

The heartbeat goroutine keeps ticking while the heavy compute runs —
that's the proof the scheduler wasn't wedged.

Run:
    python3 examples/offload_blocking.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo_core


def crunch(rounds):
    # A tight, yield-free loop — exactly what would otherwise stall a hub.
    total = 0
    for i in range(rounds):
        total += i * i
    return total


def heavy_worker(results):
    # Without pygo.blocking this loop would block every other goroutine
    # sharing this hub for its whole duration.
    result = pygo.blocking(crunch, 8_000_000)
    results.send(result)


def heartbeat(stop):
    n = 0
    while not stop[0]:
        print("heartbeat", n)
        n += 1
        pygo.sleep(0.01)


def main():
    results = pygo_core.Chan(1)
    stop = [False]

    pygo.go(heartbeat, stop)
    pygo.go(heavy_worker, results)

    result = results.recv()[0]
    stop[0] = True
    print("crunch result:", result)


if __name__ == "__main__":
    pygo.run(main)
