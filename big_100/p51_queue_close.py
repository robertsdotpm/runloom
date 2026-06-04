"""big_100 / 51 -- queue close semantics.

Rounds of producers and consumers share a channel that is CLOSED while
operations are still pending.  Producers may be parked in send and consumers
parked in recv at the moment of close; every one of them must wake (a send on a
closed channel raises; a recv drains the buffer then reports closed) and the
round must always complete -- a lost close-wakeup would hang the join.

Stresses: cancellation/wakeup on close, channel teardown correctness.
"""
import harness
import runloom

PRODUCERS = 3
CONSUMERS = 3


def producer(H, ch, sent, done):
    n = 0
    try:
        while True:
            ch.send(1)
            n += 1
    except Exception:
        pass                    # send on closed channel -> stop
    finally:
        sent[0] += n            # single-writer per producer slot
        done.send(1)


def consumer(H, ch, got, done):
    n = 0
    try:
        while True:
            _val, ok = ch.recv()
            if not ok:
                break
            n += 1
    except Exception:
        pass
    finally:
        got[0] += n
        done.send(1)


def session(H, wid, rng, state):
    while H.running():
        ch = runloom.Chan(8)
        done = runloom.Chan(PRODUCERS + CONSUMERS)
        sent = [[0] for _ in range(PRODUCERS)]
        got = [[0] for _ in range(CONSUMERS)]
        for i in range(PRODUCERS):
            H.go(producer, H, ch, sent[i], done)
        for i in range(CONSUMERS):
            H.go(consumer, H, ch, got[i], done)
        # Let some traffic build up, then close mid-flight.
        runloom.sleep(rng.uniform(0.0005, 0.005))
        ch.close()
        # Join everyone -- this only returns if every parked op woke on close.
        for _ in range(PRODUCERS + CONSUMERS):
            done.recv()
        total_sent = sum(s[0] for s in sent)
        total_got = sum(g[0] for g in got)
        if not H.check(total_sent == total_got,
                       "close lost items: sent={0} consumed={1} wid={2}".format(
                           total_sent, total_got, wid)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, session, None)


if __name__ == "__main__":
    harness.main("p51_queue_close", body, default_funcs=600,
                 describe="close a channel mid-flight; all parked ops must wake")
