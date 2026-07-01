"""A user socket set non-blocking must raise BlockingIOError from recv()
when no data is ready.  Patched runloom parks the fiber forever instead."""
import socket, sys

def scenario(tag):
    a, b = socket.socketpair()
    b.setblocking(False)
    try:
        data = b.recv(10)
        print(tag, "recv returned", data, "(unexpected)")
    except BlockingIOError:
        print(tag, "raised BlockingIOError (correct)")
    a.close(); b.close()

def scenario2(tag):
    """settimeout(0) AFTER a timed op: stale side-table timeout applies."""
    a, b = socket.socketpair()
    b.settimeout(5.0)
    a.sendall(b"x"); b.recv(1)          # records 5.0 in side table, forces nonblocking
    b.settimeout(0)                     # user now wants non-blocking
    import time
    t0 = time.monotonic()
    try:
        b.recv(10)
        print(tag, "recv returned (unexpected)")
    except BlockingIOError:
        print(tag, "raised BlockingIOError after %.2fs (correct)" % (time.monotonic()-t0))
    except socket.timeout:
        print(tag, "raised socket.timeout after %.2fs (WRONG: stale 5s timeout)" % (time.monotonic()-t0))
    a.close(); b.close()

if sys.argv[1] == "stock":
    scenario("stock nb-recv:"); scenario2("stock timeout0:")
else:
    import runloom
    def main():
        def f():
            scenario2("patched timeout0:")
            scenario("patched nb-recv:")   # runs second: expected to hang
        runloom.fiber(f)
    runloom.monkey.patch()
    runloom.run(2, main)
