"""Edge shapes: (1) select with send+recv on the SAME channel with racing
counterparties; (2) select listing the same channel twice for recv; (3) pairs
of selects ping-ponging.  Looking for hangs, double-delivery, crashes."""
import sys
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8

# --- shape 1: self-select send+recv on one channel, 2 parties (lockstep:
# every rendezvous consumes one round from each, so they always finish
# together -- no legitimate stranding) ---
ch = rc.Chan(0)
ROUNDS = 300
tally = {"sent": 0, "got": 0}
mu = rc.Mutex()

def main():
    wg = WaitGroup(); wg.add(2)
    def party(pid):
        try:
            for i in range(ROUNDS):
                idx, res = rc.select([("send", ch, (pid, i)), ("recv", ch)])
                mu.lock()
                if idx == 0:
                    tally["sent"] += 1
                else:
                    v, ok = res
                    assert ok
                    assert v[0] != pid, "SELF-PAIRED: got own value back %r" % (v,)
                    tally["got"] += 1
                mu.unlock()
        finally:
            wg.done()
    for p in range(2):
        rc.mn_fiber(lambda pid=p: party(pid))
    wg.wait()

runloom.run(HUBS, main)
assert tally["sent"] == tally["got"], "sent %d != got %d (lost or dup delivery)" % (tally["sent"], tally["got"])
print("shape1 OK: %d rendezvous, sent==got" % tally["sent"])

# --- shape 2: duplicate recv cases on one channel ---
ch2 = rc.Chan(0)
got = []

def main2():
    wg = WaitGroup(); wg.add(1)
    def producer():
        try:
            for i in range(500):
                ch2.send(i)
        finally:
            wg.done()
    def consumer():
        for _ in range(500):
            idx, (v, ok) = rc.select([("recv", ch2), ("recv", ch2)])
            assert ok
            got.append(v)
    rc.mn_fiber(consumer)
    rc.mn_fiber(producer)
    wg.wait()

runloom.run(HUBS, main2)
assert sorted(got) == list(range(500)), "dup/lost with duplicate cases: %d unique" % len(set(got))
print("shape2 OK: duplicate recv cases, 500 values exactly once")

# --- shape 3: two selects as sole counterparties, ping-pong across two chans ---
a, b = rc.Chan(0), rc.Chan(0)
count = [0, 0]

def main3():
    wg = WaitGroup(); wg.add(2)
    def left():
        try:
            for i in range(400):
                idx, res = rc.select([("send", a, i), ("recv", b)])
                count[0] += 1
        finally:
            wg.done()
    def right():
        try:
            for i in range(400):
                idx, res = rc.select([("recv", a), ("send", b, i)])
                count[1] += 1
        finally:
            wg.done()
    rc.mn_fiber(left)
    rc.mn_fiber(right)
    wg.wait()

runloom.run(HUBS, main3)
print("shape3 OK: select-vs-select ping-pong", count)
