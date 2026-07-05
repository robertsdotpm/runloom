"""Hammer: many selects (SEND and RECV cases mixed, multiple consumers)
racing across hubs.  Integrity: every produced value received exactly once."""
import sys
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
K = 4            # channels
P = 8            # select-producers
C = 8            # select-consumers
PER = 400        # values per producer

chans = [rc.Chan(0) for _ in range(K)]   # unbuffered: maximum rendezvous racing
done = rc.Chan(4)                        # consumers signal exit
sink = [list() for _ in range(C)]

TOTAL = P * PER

def main():
    wg = WaitGroup(); wg.add(P)

    def producer(pid):
        try:
            for j in range(PER):
                v = pid * PER + j
                # select-SEND across all channels (hits install/abort/retry)
                rc.select([("send", ch, v) for ch in chans])
        finally:
            wg.done()

    remaining = [TOTAL]
    cnt_mu = rc.Mutex()

    def consumer(cid):
        while True:
            cases = [("recv", ch) for ch in chans] + [("recv", done)]
            idx, res = rc.select(cases)
            if idx == K:
                return          # done signal
            val, ok = res
            if not ok:
                return
            sink[cid].append(val)
            cnt_mu.lock()
            remaining[0] -= 1
            r = remaining[0]
            cnt_mu.unlock()
            if r == 0:
                for _ in range(C):   # wake everyone else
                    done.send(None)
                return

    for c in range(C):
        rc.mn_fiber(lambda cid=c: consumer(cid))
    for p in range(P):
        rc.mn_fiber(lambda pid=p: producer(pid))
    wg.wait()

runloom.run(HUBS, main)

got = [v for slot in sink for v in slot]
expected = set(range(TOTAL))
assert len(got) == TOTAL, "lost/dup count: got %d want %d" % (len(got), TOTAL)
assert set(got) == expected, "value set mismatch: missing=%r extra=%r" % (
    sorted(expected - set(got))[:10], sorted(set(got) - expected)[:10])
print("OK: %d values, no loss, no dup" % TOTAL)
