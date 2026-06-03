"""Ping-pong — two goroutines bouncing a message over two channels.

The classic concurrency hello-world: two goroutines take turns, each
blocked on the other, synchronised purely by unbuffered channels (no
locks).  This is the cooperative hand-off the scheduler is built for —
each rendezvous is a ~few-hundred-nanosecond stack swap.

Run:
    python3 examples/ping_pong.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo_core

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
    a = pygo_core.Chan()       # ping -> pong  (unbuffered rendezvous)
    b = pygo_core.Chan()       # pong -> ping
    pygo.go(ping, a, b)
    pygo.go(pong, a, b)


if __name__ == "__main__":
    pygo.run(main)
