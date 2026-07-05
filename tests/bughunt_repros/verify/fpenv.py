import sys, ctypes
sys.path.insert(0, 'src')
import runloom_c
libc = ctypes.CDLL(None)
libc.fesetround.argtypes=[ctypes.c_int]; libc.fegetround.restype=ctypes.c_int
libc.feenableexcept.argtypes=[ctypes.c_int]
FE_UPWARD=0x800; FE_OVERFLOW=0x08
one=float(1.0); tiny=2.0**-53; big=float(1e308)

# (1) rounding-mode leak -> wrong arithmetic
def child_round():
    libc.fesetround(FE_UPWARD); runloom_c.yield_()
c=runloom_c.Coro(child_round); c.resume()
print('after fiber yield 1+2**-53 =', repr(one+tiny))   # -> 1.0000000000000002 (bug)

# (2) exception-mask leak -> whole-process SIGFPE crash
def child_crash():
    libc.feenableexcept(FE_OVERFLOW); runloom_c.yield_()
c2=runloom_c.Coro(child_crash); c2.resume()
print('about to overflow...'); sys.stdout.flush()
print(big*big)   # process dies with SIGFPE here
