"""Pooled-stack data-hygiene regression test (security campaign).

pygo recycles a completed goroutine's coro WITH its stack attached and, in
steady state (stack painting calibrated off for perf), does NOT scrub it -- so
the next goroutine to reuse that stack can read the previous one's leftovers
(TLS keys / request bodies; the aio bridge runs OpenSSL on these stacks).
Only reachable via a C extension reading uninitialised stack (Python objects
live on the heap), but real defense-in-depth.

This verifies set_stack_scrub(True) / PYGO_STACK_SCRUB=1 closes it: goroutine A
writes a sentinel across its stack and records the exact addresses; goroutine B
reuses the stack and reads those addresses back.
"""
import ctypes
import os
import sys

sys.path.insert(0, "src")
import pygo_core

HERE = os.path.dirname(os.path.abspath(__file__))
lib = ctypes.CDLL(os.path.join(HERE, "stack_scrub_helper.so"))
lib.write_sentinel.argtypes = [ctypes.c_uint64]
lib.read_recorded.argtypes = [ctypes.c_uint64]
lib.read_recorded.restype = ctypes.c_int
SENTINEL = 0xDEADBEEFCAFEF00D


def leaked_points(scrub):
    pygo_core.set_stack_scrub(scrub)
    pygo_core.set_stack_size(128 * 1024)
    pygo_core.go(lambda: lib.write_sentinel(SENTINEL))
    pygo_core.run()                                  # A done -> stack recycled
    r = {}
    pygo_core.go(lambda: r.__setitem__("h", lib.read_recorded(SENTINEL)))
    pygo_core.run()                                  # B reuses the stack
    return r["h"]


def main():
    off = leaked_points(False)
    on = leaked_points(True)
    print("scrub OFF (default): %2d/16 probe points leaked across goroutines" % off)
    print("scrub ON           : %2d/16 probe points leaked" % on)
    if on != 0:
        print("FAIL: set_stack_scrub(True) did not prevent the leak")
        return 1
    if off == 0:
        print("WARN: expected a leak with scrub off (test may not be reusing the stack)")
    print("OK: stack scrub prevents cross-goroutine stack data leakage")
    return 0


if __name__ == "__main__":
    sys.exit(main())
