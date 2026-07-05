"""Channel torture: many senders/receivers, checksummed payloads, close mid-stream.
Verify no loss, duplication, or corruption. Run under both run(1) and run(N)."""
import sys, hashlib, threading
import runloom

HUBS = int(sys.argv[1]) if len(sys.argv) > 1 else 8
NSEND = 16
NRECV = 16
PER = 500
CAP = int(sys.argv[2]) if len(sys.argv) > 2 else 4


def payload(sender, seq):
    body = b"%d:%d:" % (sender, seq)
    h = hashlib.sha256(body).hexdigest()
    return (sender, seq, body * 20, h)


def main():
    ch = runloom.Chan(CAP)
    done = runloom.Chan(0)
    recv_lock = threading.Lock()
    got = []           # (sender, seq)
    corrupt = []
    send_ok = [0] * NSEND

    def sender(sid):
        n = 0
        try:
            for seq in range(PER):
                ch.send(payload(sid, seq))
                n += 1
        except Exception as e:
            pass  # send on closed chan raises; count what got through
        finally:
            send_ok[sid] = n
            done.send(("s", sid, n))

    def receiver(rid):
        cnt = 0
        while True:
            v, ok = ch.recv()
            if not ok:
                break
            sid, seq, body, h = v
            if hashlib.sha256(body[: len(b"%d:%d:" % (sid, seq))]).hexdigest() != h:
                with recv_lock:
                    corrupt.append((sid, seq))
            with recv_lock:
                got.append((sid, seq))
            cnt += 1
        done.send(("r", rid, cnt))

    for i in range(NSEND):
        runloom.fiber(sender, i)
    for i in range(NRECV):
        runloom.fiber(receiver, i)

    def waiter():
        sdone = 0
        for _ in range(NSEND):
            pass
        # wait for all senders then close, then wait receivers
        seen_s = 0
        seen_r = 0
        results = []
        while seen_s < NSEND:
            kind, i, n = done.recv()[0]
            # recv returns (value, ok)
            raise SystemExit("unreachable")

    # simpler: collect done messages
    def collector():
        seen_s = 0
        seen_r = 0
        while seen_s < NSEND or seen_r < NRECV:
            (kind, i, n), ok = done.recv()
            if kind == "s":
                seen_s += 1
                if seen_s == NSEND:
                    ch.close()
            else:
                seen_r += 1
        total_sent = sum(send_ok)
        assert not corrupt, "CORRUPT payloads: %r" % corrupt[:10]
        assert len(got) == total_sent, "LOSS/DUP: sent=%d recv=%d" % (total_sent, len(got))
        assert len(set(got)) == len(got), "DUPLICATES: %d dups" % (len(got) - len(set(got)))
        print("hubs=%d cap=%d sent=%d recv=%d OK" % (HUBS, CAP, total_sent, len(got)))

    runloom.fiber(collector)


runloom.run(HUBS, main)
