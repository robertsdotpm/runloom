"""Ticker — a channel that fires on a fixed interval.

runloom.time.NewTicker(d) delivers a value on its `.c` channel every d
seconds until you Stop() it (runloom.time also has NewTimer for one-shot,
and After/Tick helpers).  Like Go's time.Ticker, ticks are dropped if
the consumer falls behind — the buffer is size 1.

Run:
    python3 examples/ticker.py
"""

import os

import runloom

# Free-threaded build: fan fibers across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

def main():
    ticker = runloom.time.NewTicker(0.05)
    try:
        for n in range(1, 6):
            ticker.c.recv()               # blocks ~50 ms between ticks
            print("tick", n)
    finally:
        ticker.Stop()                     # halt the backing fiber
    print("stopped after 5 ticks")

if __name__ == "__main__":
    runloom.run(HUBS, main)
