"""Timeouts — race a result against a timer with select.

runloom.time.After(d) returns a channel that fires once after d seconds.
select-ing on both the real work and the timer gives you a timeout for
free: whichever is ready first wins.  This is Go's canonical timeout
idiom, no special API required.

Run:
    python3 examples/timeout.py
"""

import os

import runloom

# Free-threaded build: fan goroutines across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

def slow_op(out, delay):
    runloom.sleep(delay)
    out.send("result after {0}s".format(delay))

def with_timeout(delay, limit):
    result = runloom.Chan(1)
    runloom.go(slow_op, result, delay)
    idx, payload = runloom.select([
        ("recv", result),                 # case 0: the work finished
        ("recv", runloom.time.After(limit)), # case 1: the deadline fired
    ])
    if idx == 1:
        return "TIMEOUT after {0}s".format(limit)
    return payload[0]

def main():
    print(with_timeout(delay=0.05, limit=0.20))   # finishes in time
    print(with_timeout(delay=0.30, limit=0.10))   # times out

if __name__ == "__main__":
    runloom.run(HUBS, main)
