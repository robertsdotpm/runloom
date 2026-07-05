"""Foreign OS thread does ch.try_send() (documented foreign-safe: never parks)
while a fiber is parked in ch.recv().  wake_waiter then runs on the foreign
thread.  Check delivery in (a) single-thread run(), (b) M:N run(N)."""
import sys
import threading
import time
import runloom
import runloom_c as rc

mode = sys.argv[1] if len(sys.argv) > 1 else "mn"

got = []

def scenario():
    ch = rc.Chan()
    alive = [True]

    def receiver():
        v, ok = ch.recv()
        got.append((v, ok))
        alive[0] = False

    def keepalive():
        # keep run() alive with sleep-work so the chan-parked fiber isn't abandoned
        while alive[0]:
            runloom.sleep(0.01)

    def foreign():
        time.sleep(0.3)
        for _ in range(1000):
            if ch.try_send("from-foreign"):
                return
            time.sleep(0.01)

    t = threading.Thread(target=foreign, daemon=True)
    t.start()
    if mode == "mn":
        def main():
            rc.mn_fiber(receiver)
            rc.mn_fiber(keepalive)
        runloom.run(4, main)
    else:
        rc.fiber(receiver)
        rc.fiber(keepalive)
        rc.run()
    t.join(timeout=5)

scenario()
print("mode=%s got=%r" % (mode, got))
assert got == [("from-foreign", True)], "foreign try_send wake failed: %r" % (got,)
print("OK")
