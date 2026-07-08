#!/usr/bin/env python3
"""fibersan_teeth.py -- planted-bug teeth control for the fiber-aware sanitizer
annotations (see docs/dev/soak/fiber_sanitizer_annotations.md).

Run under the ASan-instrumented ext (tools/run_asan_ext.sh conventions):

  mode "uaf"   goroutine A frees a heap buffer, then hands the now-dangling
               pointer to goroutine B THROUGH A CHANNEL -- a real park/wake that
               crosses at least one arch/swap_*.S stack switch.  B writes through
               it -> heap-use-after-free.  ASan MUST fire.  If it does not, either
               the ext is not ASan-built OR the fiber start/finish_switch_fiber
               brackets broke ASan's tracking across the swap.

  mode "clean" identical control-flow, but B writes through a STILL-VALID buffer.
               ASan MUST stay SILENT -- the negative control proving the brackets
               do not manufacture a false positive across the switch.

The harness (run_fibersan_teeth.sh) decides pass/fail from ASan's own output:
uaf -> expect a "heap-use-after-free" abort; clean -> expect a clean exit + the
completion line below.  This mirrors the project's model-mutation teeth: a check
that only passes when the tool it validates is actually working.
"""
import ctypes
import sys

import runloom_c

libc = ctypes.CDLL(None, use_errno=True)
libc.malloc.restype = ctypes.c_void_p
libc.malloc.argtypes = [ctypes.c_size_t]
libc.free.restype = None
libc.free.argtypes = [ctypes.c_void_p]
libc.memset.restype = ctypes.c_void_p
libc.memset.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_size_t]

BUF = 64


def run(mode):
    ch = runloom_c.Chan(1)

    def producer():
        p = libc.malloc(BUF)
        libc.memset(p, 0x41, BUF)
        if mode == "uaf":
            libc.free(p)              # p now dangles
        ch.send(p)                    # park/wake + fiber switch, hand p to B

    def consumer():
        p, ok = ch.recv()             # Go idiom: (value, ok)
        libc.memset(p, 0x42, BUF)     # write through p -- UAF in "uaf" mode
        if mode == "clean":
            libc.free(p)

    runloom_c.fiber(producer)
    runloom_c.fiber(consumer)
    runloom_c.run()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "uaf"
    if mode not in ("uaf", "clean"):
        print("usage: fibersan_teeth.py [uaf|clean]", file=sys.stderr)
        sys.exit(2)
    run(mode)
    # Reached only when ASan did NOT abort (expected for "clean"; a FAILURE of
    # the teeth for "uaf").
    print("fibersan_teeth[%s]: completed with no ASan abort" % mode)
