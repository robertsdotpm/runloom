"""Exceptions: SystemExit / KeyboardInterrupt in a fiber; exception in main fiber."""
import sys
import runloom

mode = sys.argv[1]

if mode == "plain":
    # unhandled exception in a non-main fiber: others must keep running
    ran = []
    def bad():
        raise ValueError("boom")
    def good():
        runloom.sleep(0.01)
        ran.append(1)
    def main():
        runloom.fiber(bad)
        runloom.fiber(good)
    n = runloom.run(4, main)
    print("plain: ran=%r n=%r" % (ran, n))

elif mode == "sysexit":
    def bad():
        raise SystemExit(3)
    def good():
        runloom.sleep(0.01)
        print("good ran")
    def main():
        runloom.fiber(bad)
        runloom.fiber(good)
    runloom.run(4, main)
    print("sysexit: run returned (should we have exited?)")

elif mode == "kbi":
    def bad():
        raise KeyboardInterrupt
    def good():
        runloom.sleep(0.01)
        print("good ran")
    def main():
        runloom.fiber(bad)
        runloom.fiber(good)
    runloom.run(4, main)
    print("kbi: run returned")

elif mode == "main_exc":
    def main():
        runloom.fiber(lambda: runloom.sleep(0.05))
        raise RuntimeError("main fiber blew up")
    try:
        runloom.run(4, main)
        print("main_exc: run returned normally")
    except Exception as e:
        print("main_exc: propagated %r" % e)

elif mode == "main_exc1":
    def main():
        runloom.fiber(lambda: runloom.sleep(0.05))
        raise RuntimeError("main fiber blew up")
    try:
        runloom.run(1, main)
        print("main_exc1: run returned normally")
    except Exception as e:
        print("main_exc1: propagated %r" % e)

elif mode == "sysexit1":
    def bad():
        raise SystemExit(3)
    def main():
        runloom.fiber(bad)
        runloom.fiber(lambda: print("good ran"))
    runloom.run(1, main)
    print("sysexit1: run returned")
