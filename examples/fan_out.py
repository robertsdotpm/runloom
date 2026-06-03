"""Fan-out — one producer, many consumers sharing a channel.

The mirror image of fan-in: a single producer fills one channel and
several consumers pull from it.  The runtime hands each value to
whichever consumer is ready, so work spreads out automatically.

Run:
    python3 examples/fan_out.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo_core

NUM_CONSUMERS = 4
NUM_ITEMS = 40


def producer(out):
    for i in range(NUM_ITEMS):
        out.send(i)
    out.close()                    # tells every consumer's for-loop to stop


def consumer(cid, jobs, done):
    handled = 0
    for _ in jobs:                 # competes with the other consumers
        handled += 1
    done.send((cid, handled))


def main():
    jobs = pygo_core.Chan(8)
    done = pygo_core.Chan(NUM_CONSUMERS)

    pygo.go(producer, jobs)
    for cid in range(NUM_CONSUMERS):
        pygo.go(consumer, cid, jobs, done)

    total = 0
    for _ in range(NUM_CONSUMERS):
        cid, handled = done.recv()[0]
        total += handled
        print("consumer {0} handled {1} items".format(cid, handled))
    print("total handled:", total)            # == NUM_ITEMS


if __name__ == "__main__":
    pygo.run(main)
