"""select — wait on several channel operations at once.

runloom.select takes a list of cases: ("recv", ch) or
("send", ch, value).  It blocks until exactly one is ready, then
returns (index, payload) — payload is (value, ok) for a recv, or None
for a send.  With default=True it never blocks: it returns (-1, None)
when no case is immediately ready (Go's `select { ... default: }`).

Run:
    python3 examples/select_demo.py
"""

import runloom

def main():
    a = runloom.Chan(1)
    b = runloom.Chan(1)

    runloom.go(lambda: a.send("from a"))
    runloom.go(lambda: b.send("from b"))

    # Receive from whichever is ready first; do it twice to drain both.
    for _ in range(2):
        idx, payload = runloom.select([
            ("recv", a),
            ("recv", b),
        ])
        value, ok = payload
        print("case {0} fired -> {1}".format(idx, value))

    # A send case: parks until a receiver shows up, then completes.
    sink = runloom.Chan()            # unbuffered
    runloom.go(lambda: print("received:", sink.recv()[0]))
    idx, payload = runloom.select([("send", sink, "hello")])
    print("send case {0} completed (payload={1})".format(idx, payload))

    # Non-blocking probe with default — nothing is ready here.
    empty = runloom.Chan(1)
    idx, _ = runloom.select([("recv", empty)], default=True)
    print("default fired" if idx == -1 else "got a value")

if __name__ == "__main__":
    runloom.run(main)
