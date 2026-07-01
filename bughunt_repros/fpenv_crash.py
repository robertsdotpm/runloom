import sys, ctypes
sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")
import runloom_c

libc = ctypes.CDLL(None)
# glibc: int feenableexcept(int excepts)
libc.feenableexcept.argtypes = [ctypes.c_int]
FE_OVERFLOW = 0x08

big = float(1e308)

def child():
    libc.feenableexcept(FE_OVERFLOW)   # fiber unmasks the HW overflow trap
    runloom_c.yield_()

c = runloom_c.Coro(child)
c.resume()   # child unmasks FE_OVERFLOW, yields back into us

print("about to overflow a double in the *caller* context...")
sys.stdout.flush()
x = big * big     # overflow -> SIGFPE because the fiber's unmasked trap leaked in
print("no trap, result:", x)
