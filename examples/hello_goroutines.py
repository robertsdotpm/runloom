"""Hello, goroutines — spawn, yield, sleep.

The "go fn(args)" of Python: pygo.go schedules a function to run
cooperatively and returns immediately, just like Go's `go`.  pygo.run
drives the scheduler until every goroutine has finished.

Run:
    python3 examples/hello_goroutines.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo


def greeter(name, steps):
    for i in range(steps):
        print("{0}: step {1}".format(name, i))
        pygo.yield_()          # hand the CPU to the other goroutines


def napper():
    pygo.sleep(0.01)           # cooperative sleep — others run meanwhile
    print("napper: woke up")


def main():
    # Three greeters interleave because each yields after every step.
    for name in ("alice", "bob", "carol"):
        pygo.go(greeter, name, 3)
    pygo.go(napper)


if __name__ == "__main__":
    # pygo.run(main) spawns main() first, then drains the scheduler —
    # the moral equivalent of Go's func main().
    pygo.run(main)
