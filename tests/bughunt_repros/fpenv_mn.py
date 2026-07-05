import sys, ctypes
sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")
import runloom

libc = ctypes.CDLL(None)
libc.fesetround.argtypes = [ctypes.c_int]
libc.fegetround.restype = ctypes.c_int
FE_UPWARD = 0x800

one  = float(1.0)
tiny = 2.0 ** -53

bad = [0]
rounds = [0]

def setter():
    # a fiber that (legitimately) uses a hardware rounding mode
    for _ in range(200):
        libc.fesetround(FE_UPWARD)
        runloom.yield_now()

def victim():
    # a DIFFERENT fiber that never touches rounding; its float math must be exact
    for _ in range(200):
        runloom.yield_now()
        v = one + tiny        # round-to-nearest => exactly 1.0
        rounds[0] += 1
        if v != 1.0:
            bad[0] += 1

def main():
    runloom.fiber(setter)
    runloom.fiber(victim)

runloom.run(1, main)   # single hub so the two fibers interleave on one OS thread
print("victim rounds:", rounds[0], "corrupted results:", bad[0])
if bad[0]:
    print("BUG: %d/%d of the victim fiber's float computations were wrong "
          "because another fiber's FP rounding mode leaked through the "
          "context switch" % (bad[0], rounds[0]))
