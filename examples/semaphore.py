"""Semaphore — bound concurrency with a buffered channel.

A buffered channel of N tokens is a counting semaphore: receive a token
to acquire, send it back to release.  At most N goroutines hold a token
at once, so this caps how many run the protected section simultaneously
even though 10 are spawned.

Run:
    python3 examples/semaphore.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo_core

MAX_CONCURRENT = 3
NUM_TASKS = 10


def main():
    sem = pygo_core.Chan(MAX_CONCURRENT)
    for _ in range(MAX_CONCURRENT):
        sem.try_send(None)                 # fill with tokens

    active = [0]
    peak = [0]
    done = pygo_core.Chan(NUM_TASKS)

    def task(tid):
        sem.recv()                         # acquire (blocks if no token)
        try:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            print("task {0} running (active={1})".format(tid, active[0]))
            pygo.sleep(0.02)               # the protected "slow" section
            active[0] -= 1
        finally:
            sem.send(None)                 # release
            done.send(tid)

    for tid in range(NUM_TASKS):
        pygo.go(task, tid)
    for _ in range(NUM_TASKS):
        done.recv()

    print("peak concurrency was {0} (cap {1})".format(peak[0], MAX_CONCURRENT))


if __name__ == "__main__":
    pygo.run(main)
