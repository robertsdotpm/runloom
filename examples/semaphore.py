"""Semaphore — bound concurrency with a buffered channel.

A buffered channel of N tokens is a counting semaphore: receive a token
to acquire, send it back to release.  At most N goroutines hold a token
at once, so this caps how many run the protected section simultaneously
even though 10 are spawned.

The channel needs no lock.  The active/peak *counter* below does, though:
with the GIL off, tasks holding tokens run on different hubs in parallel,
so `active[0] += 1` is a real read-modify-write race -- a runloom.sync.Lock
makes it correct.  (Go's lesson: share memory by communicating; when you
do share raw state across goroutines, guard it.)

Run:
    python3 examples/semaphore.py
"""

import os

import runloom
from runloom.sync import Lock

# Free-threaded build: fan goroutines across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

MAX_CONCURRENT = 3
NUM_TASKS = 10

def main():
    sem = runloom.Chan(MAX_CONCURRENT)
    for _ in range(MAX_CONCURRENT):
        sem.try_send(None)                 # fill with tokens

    active = [0]
    peak = [0]
    lock = Lock()                          # guards the active/peak counter
    done = runloom.Chan(NUM_TASKS)

    def task(tid):
        sem.recv()                         # acquire (blocks if no token)
        try:
            with lock:
                active[0] += 1
                cur = active[0]
                peak[0] = max(peak[0], cur)
            print("task {0} running (active={1})".format(tid, cur))
            runloom.sleep(0.02)               # the protected "slow" section
            with lock:
                active[0] -= 1
        finally:
            sem.send(None)                 # release
            done.send(tid)

    for tid in range(NUM_TASKS):
        runloom.go(task, tid)
    for _ in range(NUM_TASKS):
        done.recv()

    print("peak concurrency was {0} (cap {1})".format(peak[0], MAX_CONCURRENT))

if __name__ == "__main__":
    runloom.run(HUBS, main)
