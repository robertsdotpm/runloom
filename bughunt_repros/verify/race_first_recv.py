"""Claim (b): two fibers on different hubs race the FIRST recv() on a shared
TCPConn in iouring multishot mode.  Unsynchronized lazy init of self->ms can
arm TWO kernel multishot recvs on one fd; bytes delivered to the leaked
handle's queue are unreachable -> data loss / hung recv.

Detection: per iteration, peer sends a known byte total; all reader fibers
count what they get.  Shortfall after deadline => lost bytes.
"""
import os
os.environ["RUNLOOM_TCPCONN_IOURING"] = "1"
import socket
import sys
import threading
import time
import runloom_c

runloom_c.mn_init(8)

ITERS = 150
CHUNKS = 30
CHUNK = 100

summary = {"ok": 0, "loss": 0, "raceshort": 0}


def sleep_f(s):
    runloom_c.sched_sleep(s)


def one_iteration(idx):
    lst = socket.socket()
    lst.bind(("127.0.0.1", 0)); lst.listen(1)
    cli = socket.socket(); cli.connect(lst.getsockname())
    srv, _ = lst.accept(); lst.close()
    fd = os.dup(srv.fileno()); srv.close()
    conn = runloom_c.TCPConn(fd)

    lock = threading.Lock()
    st = {"arrived": 0, "got": 0, "done": 0}

    # 2 bytes already buffered in the socket before the racing first recvs.
    cli.sendall(b"XY")
    sleep_f(0.005)

    def racer():
        with lock:
            st["arrived"] += 1
        deadline = time.monotonic() + 1.0
        while st["arrived"] < 2 and time.monotonic() < deadline:
            pass
        try:
            b = conn.recv(1)
            with lock:
                st["got"] += len(b)
        except OSError:
            pass
        with lock:
            st["done"] += 1

    runloom_c.mn_fiber(racer)
    runloom_c.mn_fiber(racer)

    # Wait for both racers (bounded).
    dl = time.monotonic() + 1.5
    while time.monotonic() < dl:
        with lock:
            if st["done"] == 2:
                break
        sleep_f(0.001)

    # Phase 2: stream CHUNKS*CHUNK bytes; a single collector fiber reads.
    def collector():
        while True:
            try:
                b = conn.recv(4096)
            except OSError:
                break
            if not b:
                break
            with lock:
                st["got"] += len(b)

    runloom_c.mn_fiber(collector)
    for _ in range(CHUNKS):
        cli.sendall(b"a" * CHUNK)
        sleep_f(0.001)
    cli.close()  # EOF unblocks collector when stream fully delivered

    total_sent = 2 + CHUNKS * CHUNK
    dl = time.monotonic() + 2.0
    while time.monotonic() < dl:
        with lock:
            if st["got"] >= total_sent:
                break
        sleep_f(0.002)

    with lock:
        got = st["got"]
        racers_done = st["done"]
    conn.close()  # unblock anything still parked (best effort)
    if got < total_sent:
        summary["loss"] += 1
        print("iter %d: LOST %d of %d bytes (racers done=%d)"
              % (idx, total_sent - got, total_sent, racers_done), flush=True)
    else:
        if racers_done < 2:
            summary["raceshort"] += 1
        summary["ok"] += 1


def main():
    for i in range(ITERS):
        one_iteration(i)
        if i % 25 == 0:
            print("progress iter", i, summary, flush=True)
    print("SUMMARY", summary, flush=True)
    os._exit(0 if summary["loss"] == 0 else 1)


runloom_c.mn_fiber(main)
runloom_c.mn_run()
