"""Hammer: close racing parked senders/receivers/selects across hubs.
Integrity: every value RECEIVED must have been SENT-without-error exactly once
is not assertable (send may succeed into buffer then drain), but we assert:
 - no crash, no hang
 - received set is a subset of sent-ok set... actually Go guarantees:
   a send that returned without error had its value delivered to the buffer
   or a receiver.  After close+drain, received == sent_ok exactly.
"""
import sys
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
ROUNDS = int(sys.argv[2]) if len(sys.argv) > 2 else 30

for rnd in range(ROUNDS):
    ch = rc.Chan(rnd % 4)          # mix unbuffered + small buffers
    P, C, PER = 6, 6, 50
    sent_ok = [list() for _ in range(P)]
    got = [list() for _ in range(C)]

    def main():
        wg = WaitGroup(); wg.add(P + C)

        def producer(pid):
            try:
                for j in range(PER):
                    v = pid * PER + j
                    try:
                        if j % 3 == 0:
                            rc.select([("send", ch, v)])
                        else:
                            ch.send(v)
                        sent_ok[pid].append(v)
                    except ValueError:
                        return    # closed
            finally:
                wg.done()

        def consumer(cid):
            try:
                while True:
                    if cid % 2 == 0:
                        v, ok = ch.recv()
                    else:
                        _i, (v, ok) = rc.select([("recv", ch)])
                    if not ok:
                        return
                    got[cid].append(v)
            finally:
                wg.done()

        def closer():
            runloom.sleep(0.001)
            try:
                ch.close()
            except ValueError:
                pass

        for c in range(C):
            rc.mn_fiber(lambda cid=c: consumer(cid))
        for p in range(P):
            rc.mn_fiber(lambda pid=p: producer(pid))
        rc.mn_fiber(closer)
        wg.wait()

    runloom.run(HUBS, main)

    s = [v for x in sent_ok for v in x]
    g = [v for x in got for v in x]
    assert len(g) == len(set(g)), "round %d: duplicate delivery! %d vs %d" % (rnd, len(g), len(set(g)))
    assert set(g) == set(s), (
        "round %d: sent-ok vs received mismatch: sent_ok-not-received=%r received-not-sent=%r"
        % (rnd, sorted(set(s) - set(g))[:10], sorted(set(g) - set(s))[:10]))
print("OK: %d rounds close-race, delivered == sent-ok, no dup/loss" % ROUNDS)
