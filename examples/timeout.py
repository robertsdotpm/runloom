"""Timeouts — race a result against a timer with select.

pygo.time.After(d) returns a channel that fires once after d seconds.
select-ing on both the real work and the timer gives you a timeout for
free: whichever is ready first wins.  This is Go's canonical timeout
idiom, no special API required.

Run:
    python3 examples/timeout.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo.time
import pygo_core


def slow_op(out, delay):
    pygo.sleep(delay)
    out.send("result after {0}s".format(delay))


def with_timeout(delay, limit):
    result = pygo_core.Chan(1)
    pygo.go(slow_op, result, delay)
    idx, payload = pygo_core.select([
        ("recv", result),                 # case 0: the work finished
        ("recv", pygo.time.After(limit)), # case 1: the deadline fired
    ])
    if idx == 1:
        return "TIMEOUT after {0}s".format(limit)
    return payload[0]


def main():
    print(with_timeout(delay=0.05, limit=0.20))   # finishes in time
    print(with_timeout(delay=0.30, limit=0.10))   # times out


if __name__ == "__main__":
    pygo.run(main)
