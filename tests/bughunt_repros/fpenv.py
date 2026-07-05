import sys, ctypes
sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")
import runloom_c

libc = ctypes.CDLL(None, use_errno=True)
fesetround = libc.fesetround
fegetround = libc.fegetround
fesetround.argtypes = [ctypes.c_int]
fegetround.restype = ctypes.c_int

FE_TONEAREST = 0x000
FE_DOWNWARD  = 0x400
FE_UPWARD    = 0x800
FE_TOWARDZERO= 0xc00

print("main rounding before:", hex(fegetround()))

def child():
    # change the FP rounding mode inside the fiber, then yield back
    rc = fesetround(FE_UPWARD)
    print("  child set FE_UPWARD rc=", rc, "now:", hex(fegetround()))
    runloom_c.yield_()
    print("  child resumed, rounding:", hex(fegetround()))

c = runloom_c.Coro(child)
c.resume()   # child sets FE_UPWARD then yields
print("main rounding AFTER resume (should be FE_TONEAREST=0x0):", hex(fegetround()))
if fegetround() != FE_TONEAREST:
    print("BUG: FP rounding mode leaked out of the fiber across the context switch")
else:
    print("OK: rounding preserved")
