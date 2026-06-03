"""Hello, goroutines — spawn, yield, sleep.

The "go fn(args)" of Python: runloom.go schedules a function to run
cooperatively and returns immediately, just like Go's `go`.  runloom.run
drives the scheduler until every goroutine has finished.

Run:
    python3 examples/hello_goroutines.py
"""

import runloom

def greeter(name, steps):
    for i in range(steps):
        print("{0}: step {1}".format(name, i))
        runloom.yield_()          # hand the CPU to the other goroutines

def napper():
    runloom.sleep(0.01)           # cooperative sleep — others run meanwhile
    print("napper: woke up")

def main():
    # Three greeters interleave because each yields after every step.
    for name in ("alice", "bob", "carol"):
        runloom.go(greeter, name, 3)
    runloom.go(napper)

if __name__ == "__main__":
    # runloom.run(main) spawns main() first, then drains the scheduler —
    # the moral equivalent of Go's func main().
    runloom.run(main)
