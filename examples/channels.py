"""Channels — buffered, unbuffered, close, and range.

A runloom.Chan is a Go channel: a typed-agnostic, goroutine-safe queue.
Buffered channels hold up to `capacity` values before a send blocks;
an unbuffered channel (capacity 0) is a rendezvous — each send blocks
until a receiver takes the value.  `recv()` returns (value, ok); `ok`
is False once the channel is closed and drained.  `for v in ch` ranges
until close.

Run:
    python3 examples/channels.py
"""

import runloom

def main():
    # --- Buffered: sends up to capacity don't block, no receiver yet. ---
    buf = runloom.Chan(3)
    buf.send("a")
    buf.send("b")
    buf.send("c")
    buf.close()                # closing lets `for v in buf` terminate
    for v in buf:              # drains the 3 buffered values, then stops
        print("buffered:", v)

    # --- Unbuffered: each send rendezvous with a receiver. ---
    pipe = runloom.Chan()    # capacity 0

    def producer():
        for i in range(3):
            pipe.send(i)       # blocks until main's loop receives
        pipe.close()

    runloom.go(producer)
    for v in pipe:             # receives 0, 1, 2 then sees the close
        print("unbuffered:", v)

    # --- recv() spells out the (value, ok) close signal explicitly. ---
    done = runloom.Chan(1)
    done.send(99)
    done.close()
    value, ok = done.recv()
    print("recv ->", value, ok)        # 99 True
    value, ok = done.recv()
    print("recv ->", value, ok)        # None False  (closed + drained)

if __name__ == "__main__":
    runloom.run(1, main)
