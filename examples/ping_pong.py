"""Ping-pong — two fibers bouncing a message over two channels.

The classic concurrency hello-world: two fibers take turns, each
blocked on the other, synchronised purely by unbuffered channels (no
locks).  This is the cooperative hand-off the scheduler is built for —
each rendezvous is a ~few-hundred-nanosecond stack swap.

Run:
    python3 examples/ping_pong.py
"""

import os

import runloom

# Free-threaded build: fan fibers across all cores (M:N scheduler).
HUBS = os.cpu_count() or 4

ROUNDS = 5

def ping(to_pong, from_pong):
    for i in range(ROUNDS):
        to_pong.send("ping {0}".format(i))
        reply, _ = from_pong.recv()
        print("ping received:", reply)
    to_pong.close()            # tell pong we're done

def pong(from_ping, to_ping):
    while True:
        msg, ok = from_ping.recv()
        if not ok:             # ping closed the channel -> stop
            return
        print("pong received:", msg)
        to_ping.send("pong")

def main():
    a = runloom.Chan()       # ping -> pong  (unbuffered rendezvous)
    b = runloom.Chan()       # pong -> ping
    runloom.fiber(ping, a, b)
    runloom.fiber(pong, a, b)

if __name__ == "__main__":
    runloom.run(HUBS, main)
