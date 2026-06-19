"""Hello, fibers — spawn, yield, sleep.

The "go fn(args)" of Python: runloom.go schedules a function to run
cooperatively and returns immediately, just like Go's `go`.  runloom.run
drives the scheduler until every fiber has finished.

Run:
    python3 examples/hello_fibers.py
"""

import os

import runloom

# Free-threaded build: fan fibers across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

def greeter(name, steps):
    for i in range(steps):
        print("{0}: step {1}".format(name, i))
        runloom.yield_now()       # hand the CPU to the other fibers

def napper():
    runloom.sleep(0.01)           # cooperative sleep — others run meanwhile
    print("napper: woke up")

def main():
    # Three greeters interleave because each yields after every step.
    for name in ("alice", "bob", "carol"):
        runloom.fiber(greeter, name, 3)
    runloom.fiber(napper)

if __name__ == "__main__":
    # runloom.run(HUBS, main) spawns main() first, then drains the scheduler —
    # the moral equivalent of Go's func main().
    runloom.run(HUBS, main)
