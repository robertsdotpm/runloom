"""select() torture: fibers select over several channels while others close them,
plus default-case polling under churn. Verify counts."""
import sys
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
NCH = 6
PER = 400


def main():
    chans = [runloom.Chan(2) for _ in range(NCH)]
    out = runloom.Chan(64)

    def producer(i):
        ch = chans[i]
        for seq in range(PER):
            ch.send((i, seq))
        ch.close()

    def selector(sid):
        live = list(range(NCH))
        got = 0
        while live:
            cases = [("recv", chans[i]) for i in live]
            idx, res = runloom.select(cases)
            v, ok = res
            if not ok:
                live.pop(idx)
            else:
                got += 1
        out.send(got)

    NSEL = 8
    for i in range(NCH):
        runloom.fiber(producer, i)
    for i in range(NSEL):
        runloom.fiber(selector, i)

    def collect():
        total = 0
        for _ in range(NSEL):
            v, ok = out.recv()
            total += v
        expect = NCH * PER
        assert total == expect, "select lost/dup values: got %d want %d" % (total, expect)
        print("select churn hubs=%d total=%d OK" % (HUBS, total))
    runloom.fiber(collect)


runloom.run(HUBS, main)


def main2():
    # default-case polling + send-select while a closer races
    ch = runloom.Chan(1)
    done = runloom.Chan(0)

    def poller():
        seen = 0
        while True:
            r = runloom.select([("recv", ch)], default=True)
            if r == -1 or (isinstance(r, tuple) and r[0] == -1):
                runloom.yield_now()
                continue
            idx, (v, ok) = r
            if not ok:
                break
            seen += 1
        done.send(seen)

    def sender():
        n = 0
        try:
            for i in range(1000):
                idx, _ = runloom.select([("send", ch, i)])
                n += 1
        except Exception:
            pass
        ch.close()

    runloom.fiber(poller)
    runloom.fiber(sender)

    def collect():
        v, ok = done.recv()
        assert v == 1000, "poller saw %d != 1000" % v
        print("select default-poll OK (%d)" % v)
    runloom.fiber(collect)


runloom.run(HUBS, main2)
