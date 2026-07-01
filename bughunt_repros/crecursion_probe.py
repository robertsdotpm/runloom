"""Deep C recursion (repr of nested lists) inside a fiber.
Stock Python raises RecursionError; a 512KiB fiber stack may guard-page SEGV."""
import sys
import runloom

mode = sys.argv[1] if len(sys.argv) > 1 else "fiber"
HUBS = int(sys.argv[2]) if len(sys.argv) > 2 else 1
DEPTH = int(sys.argv[3]) if len(sys.argv) > 3 else 100000

def build(depth):
    x = []
    for _ in range(depth):
        x = [x]
    return x

if mode == "main":
    x = build(DEPTH)
    try:
        repr(x)
        print("main: repr survived")
    except RecursionError:
        print("main: RecursionError (expected)")
else:
    def main():
        def f():
            x = build(DEPTH)
            try:
                repr(x)
                print("fiber: repr survived")
            except RecursionError:
                print("fiber: RecursionError (clean)")
        runloom.fiber(f)
    runloom.run(HUBS, main)
    print("run returned")
