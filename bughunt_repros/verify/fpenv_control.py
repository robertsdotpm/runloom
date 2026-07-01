import ctypes, threading
libc = ctypes.CDLL(None)
libc.fesetround.argtypes=[ctypes.c_int]
FE_UPWARD=0x800
one=float(1.0); tiny=2.0**-53
print('baseline main thread:', repr(one+tiny))
def worker():
    libc.fesetround(FE_UPWARD)
t=threading.Thread(target=worker); t.start(); t.join()
print('after OS thread set FE_UPWARD, main:', repr(one+tiny))
