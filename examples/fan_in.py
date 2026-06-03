"""Fan-in — many producers, one consumer, one channel.

Several producer goroutines write into the same channel; a single
consumer multiplexes their output.  The channel does the merging for
you — no locks, no shared list.

Run:
    python3 examples/fan_in.py
"""

import runloom

NUM_PRODUCERS = 4
ITEMS_EACH = 5

def producer(pid, out):
    for i in range(ITEMS_EACH):
        out.send((pid, i))

def main():
    merged = runloom.Chan(16)
    for pid in range(NUM_PRODUCERS):
        runloom.go(producer, pid, merged)

    for _ in range(NUM_PRODUCERS * ITEMS_EACH):
        pid, item = merged.recv()[0]
        print("from producer {0}: item {1}".format(pid, item))

if __name__ == "__main__":
    runloom.run(main)
