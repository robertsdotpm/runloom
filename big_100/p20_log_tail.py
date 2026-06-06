"""big_100 / 20 -- log tail fanout.

One writer goroutine appends monotonically increasing sequence numbers to a
log file.  Tens of thousands of reader goroutines tail the file and verify the
sequence numbers they observe never go backwards and never repeat (a duplicate
or out-of-order line would mean a broken tail/offset).

Stresses: sleep/poll loops, file reads, many readers fanning out from one
writer, scheduler fairness.
"""
import os

import harness


def setup(H):
    base = H.make_tmpdir("big100_logtail_")
    path = os.path.join(base, "log.txt")
    open(path, "wb").close()
    H.state = {"path": path}


def writer(H):
    path = H.state["path"]
    seq = 0
    with open(path, "ab", buffering=0) as f:
        while H.running():
            for _ in range(64):
                f.write("seq {0}\n".format(seq).encode())
                seq += 1
            H.sleep(0.002)
    H.log("writer wrote {0} lines".format(seq))


def reader(H, wid, rng, state):
    path = state["path"]
    H.sleep(rng.random() * 0.5)
    last = -1
    offset = 0
    buf = b""
    try:
        f = open(path, "rb")
    except OSError:
        return
    try:
        while H.running():
            f.seek(offset)
            data = f.read(65536)
            offset = f.tell()
            if not data:
                H.sleep(0.01)
                continue
            buf += data
            while b"\n" in buf:
                if not H.running():
                    break
                line, buf = buf.split(b"\n", 1)
                if not line:
                    continue
                seq = int(line.split()[1])
                if not H.check(seq > last,
                               "non-monotonic tail wid={0}: {1} after {2}"
                               .format(wid, seq, last)):
                    return
                last = seq
                H.op(wid)
            H.task_done(wid)
    finally:
        try:
            f.close()
        except OSError:
            pass


def body(H):
    H.go(writer, H)
    H.run_pool(H.funcs, reader, H.state)


if __name__ == "__main__":
    harness.main("p20_log_tail", body, setup=setup, default_funcs=10000,
                 describe="one writer, many tailing readers verify monotonic seq")
