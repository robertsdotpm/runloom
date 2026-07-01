"""Misc semantic probes: double close, send-on-closed, recv from closed with buffer,
try_* from foreign threads, foreign-thread fiber spawn into live hubs."""
import sys, threading, time
import runloom

# 1. double close
def main():
    ch = runloom.Chan(1)
    ch.close()
    try:
        ch.close()
        print("double close: OK (idempotent)")
    except Exception as e:
        print("double close: raises %r" % e)
    # send on closed
    try:
        ch.send(1)
        print("send on closed: silently OK (BUG)")
    except Exception as e:
        print("send on closed: raises %s" % type(e).__name__)
    # buffered values then close: drain semantics
    ch2 = runloom.Chan(4)
    ch2.send(1); ch2.send(2)
    ch2.close()
    a = ch2.recv(); b = ch2.recv(); c = ch2.recv()
    print("drain after close:", a, b, c)  # expect (1,True),(2,True),(None,False)
runloom.run(1, main)

# 2. try_send/try_recv from a foreign thread while hubs live
res = []
def main2():
    ch = runloom.Chan(2)
    hold = runloom.Chan(0)
    def foreign():
        ok1 = ch.try_send("x")
        ok2 = ch.try_send("y")
        ok3 = ch.try_send("z")   # full -> False
        r = ch.try_recv()
        res.append((ok1, ok2, ok3, r))
    t = threading.Thread(target=foreign)
    t.start()
    t.join()
    print("foreign try_*:", res)

runloom.run(4, main2)

# 3. foreign-thread fiber() while hubs are live (round-robins a non-hub caller)
ran = []
evt = threading.Event()
def main3():
    gate = runloom.Chan(0)
    def spawner():
        def newfiber():
            ran.append(1)
            evt.set()
        runloom.fiber(newfiber)   # from a plain OS thread while hubs run
    t = threading.Thread(target=spawner)
    t.start()
    t.join()
    ok = evt.wait(5)
    print("foreign-thread fiber spawn ran:", ok, len(ran))
runloom.run(4, main3)
