"""Pipeline — stages connected by channels.

Each stage is a goroutine that reads from an input channel, does one
job, and writes to an output channel; closing an output propagates the
"done" signal downstream.  Here: generate 1..N -> square -> sum.

Run:
    python3 examples/pipeline.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import runloom
import runloom_c


def generate(out, n):
    for i in range(1, n + 1):
        out.send(i)
    out.close()


def square(inp, out):
    for v in inp:
        out.send(v * v)
    out.close()


def sum_all(inp, result):
    total = 0
    for v in inp:
        total += v
    result.send(total)


def main():
    nums = runloom_c.Chan(10)
    squares = runloom_c.Chan(10)
    result = runloom_c.Chan(1)

    runloom.go(generate, nums, 10)
    runloom.go(square, nums, squares)
    runloom.go(sum_all, squares, result)

    print("sum of squares 1..10 =", result.recv()[0])   # 385


if __name__ == "__main__":
    runloom.run(main)
