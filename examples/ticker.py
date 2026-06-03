"""Ticker — a channel that fires on a fixed interval.

pygo.time.NewTicker(d) delivers a value on its `.c` channel every d
seconds until you Stop() it (pygo.time also has NewTimer for one-shot,
and After/Tick helpers).  Like Go's time.Ticker, ticks are dropped if
the consumer falls behind — the buffer is size 1.

Run:
    python3 examples/ticker.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo.time
import pygo_core


def main():
    ticker = pygo.time.NewTicker(0.05)
    try:
        for n in range(1, 6):
            ticker.c.recv()               # blocks ~50 ms between ticks
            print("tick", n)
    finally:
        ticker.Stop()                     # halt the backing goroutine
    print("stopped after 5 ticks")


if __name__ == "__main__":
    pygo.run(main)
