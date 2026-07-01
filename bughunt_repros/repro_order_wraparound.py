"""Buffered channel FIFO order across wraparound + parked-sender refill order."""
import runloom_c as rc

def main():
    rc.run()
    return box.get("ok")

# simpler: do everything in fibers
import runloom
res = []
def main2():
    ch = rc.Chan(3)
    seq = list(range(50))
    def producer():
        for v in seq:
            ch.send(v)
        ch.close()
    rc.fiber(producer)
    got = []
    while True:
        v, ok = ch.recv()
        if not ok:
            break
        got.append(v)
    assert got == seq, "FIFO broken: %r" % (got[:10],)

    ch2 = rc.Chan(1)
    ch2.send(100)
    for v in (101, 102, 103):
        rc.fiber(lambda v=v: ch2.send(v))
    # let senders park
    for _ in range(10):
        rc.sched_yield_classic()
    drained = []
    for _ in range(4):
        v, ok = ch2.recv()
        drained.append(v)
    assert drained == [100, 101, 102, 103], "parked-sender refill order: %r" % drained
    res.append("ok")

rc.fiber(main2)
rc.run()
assert res == ["ok"]
print("OK: FIFO order + wraparound + parked-sender refill order")
