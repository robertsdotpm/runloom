import sys, ctypes
sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")
import runloom_c

libc = ctypes.CDLL(None)
libc.fesetround.argtypes = [ctypes.c_int]
libc.fegetround.restype = ctypes.c_int
FE_TONEAREST = 0x000
FE_UPWARD    = 0x800

# runtime values so nothing is constant-folded at compile time
one   = float(1.0)
tiny  = 2.0 ** -53      # half an ULP at 1.0

def correct_add():
    # round-to-nearest (ties to even): 1 + 2^-53 == 1.0 exactly
    return one + tiny

print("baseline (default rounding):", repr(correct_add()))

def child():
    libc.fesetround(FE_UPWARD)   # fiber legitimately sets a rounding mode
    runloom_c.yield_()           # ... and yields (parks) mid-work

c = runloom_c.Coro(child)
c.resume()   # child sets FE_UPWARD, yields back to us

# We are a DIFFERENT execution context that never touched rounding.
val = correct_add()
print("after fiber yielded (our result):", repr(val))
if val != 1.0:
    print("BUG: our arithmetic is now wrong (%r) because the fiber's rounding "
          "mode leaked into us via the context switch" % val)
