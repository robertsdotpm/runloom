#!/usr/bin/env python3
"""
Baseline: the UNINSTRUMENTED extension on a small stack overflows in
*hardware* -- you get a SIGSEGV (a crash), not a clean software report.
This is the situation a per-fiber guard page would normally have to catch.

We run recurse_c on a thread created with a deliberately tiny stack so the
C recursion blows it. With the uninstrumented .so there is NO software
check, so the process dies with a fatal signal.

Usage: python3 baseline_overflow.py <path-to-uninstrumented.so>
"""
import os
import sys
import threading
import importlib.util

PYTHON_GIL = os.environ.setdefault("PYTHON_GIL", "0")


def load_ext(path):
    # The init symbol is PyInit_stacktest, so the module name must be
    # 'stacktest' regardless of file name.
    spec = importlib.util.spec_from_file_location("stacktest", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    so = sys.argv[1]
    print(f"[baseline] loading uninstrumented ext: {so}")
    mod = load_ext(so)

    # Tiny thread stack so deep C recursion overflows the hardware stack.
    tiny = 256 * 1024  # 256 KiB
    threading.stack_size(tiny)
    print(f"[baseline] thread stack size set to {tiny} bytes ({tiny//1024} KiB)")

    result = {}

    def worker():
        # Each recurse_c frame burns ~4KB; 256KiB / 4KB ~= 64 frames before
        # the real stack is exhausted. Ask for far more than fits.
        depth = 100000
        print(f"[baseline] calling recurse({depth}) on tiny stack "
              f"(expect HARDWARE crash, no software report)...")
        try:
            r = mod.recurse(depth)
            result["ok"] = r
            print(f"[baseline] returned normally?! r={r}")
        except RecursionError as e:
            result["rec"] = str(e)
            print(f"[baseline] RecursionError: {e}")

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    print("[baseline] worker joined (if you see this, no crash happened)")


if __name__ == "__main__":
    main()
