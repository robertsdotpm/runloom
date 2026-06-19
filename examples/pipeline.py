"""Pipeline — stages connected by channels.

Each stage is a fiber that reads from an input channel, does one
job, and writes to an output channel; closing an output propagates the
"done" signal downstream.  Here: generate 1..N -> square -> sum.

Run:
    python3 examples/pipeline.py
"""

import os

import runloom

# Free-threaded build: fan fibers across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

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
    nums = runloom.Chan(10)
    squares = runloom.Chan(10)
    result = runloom.Chan(1)

    runloom.fiber(generate, nums, 10)
    runloom.fiber(square, nums, squares)
    runloom.fiber(sum_all, squares, result)

    print("sum of squares 1..10 =", result.recv()[0])   # 385

if __name__ == "__main__":
    runloom.run(HUBS, main)
