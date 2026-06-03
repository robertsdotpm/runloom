"""select — wait on several channel operations at once.

pygo_core.select takes a list of cases: ("recv", ch) or
("send", ch, value).  It blocks until exactly one is ready, then
returns (index, payload) — payload is (value, ok) for a recv, or None
for a send.  With default=True it never blocks: it returns (-1, None)
when no case is immediately ready (Go's `select { ... default: }`).

Run:
    python3 examples/select.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import pygo
import pygo_core


def main():
    a = pygo_core.Chan(1)
    b = pygo_core.Chan(1)

    pygo.go(lambda: a.send("from a"))
    pygo.go(lambda: b.send("from b"))

    # Receive from whichever is ready first; do it twice to drain both.
    for _ in range(2):
        idx, payload = pygo_core.select([
            ("recv", a),
            ("recv", b),
        ])
        value, ok = payload
        print("case {0} fired -> {1}".format(idx, value))

    # A send case: parks until a receiver shows up, then completes.
    sink = pygo_core.Chan()            # unbuffered
    pygo.go(lambda: print("received:", sink.recv()[0]))
    idx, payload = pygo_core.select([("send", sink, "hello")])
    print("send case {0} completed (payload={1})".format(idx, payload))

    # Non-blocking probe with default — nothing is ready here.
    empty = pygo_core.Chan(1)
    idx, _ = pygo_core.select([("recv", empty)], default=True)
    print("default fired" if idx == -1 else "got a value")


if __name__ == "__main__":
    pygo.run(main)
