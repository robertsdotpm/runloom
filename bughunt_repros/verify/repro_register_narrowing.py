"""Verify: concurrent netpoll_register on one fd from two hubs races the
out-of-lock epoll_ctl -> spurious EEXIST from wait_fd, or kernel-set narrowing
(lost WRITE wakeup).  Run with RUNLOOM_PERHUB_EPOLL=0."""
import os, sys, socket, time
import runloom
import runloom_c as rc

ROUNDS = int(os.environ.get("ROUNDS", "1500"))
PARK_MS = int(os.environ.get("PARK_MS", "600"))

READ, WRITE = 1, 2

failures = []


def main():
    lost = 0
    eexist = 0
    for rnd in range(ROUNDS):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        # fill a's send buffer so a is NOT writable
        try:
            a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        except OSError:
            pass
        filler = b"x" * 65536
        while True:
            try:
                a.send(filler)
            except BlockingIOError:
                break
        fd = a.fileno()

        state = {"arrived": 0, "go": False, "res_r": None, "res_w": None}

        def waiter(ev, key):
            state["arrived"] += 1
            while not state["go"]:
                pass
            try:
                state[key] = rc.wait_fd(fd, ev, PARK_MS)
            except OSError as e:
                state[key] = "exc:%d" % e.errno

        runloom.fiber(lambda: waiter(READ, "res_r"))
        runloom.fiber(lambda: waiter(WRITE, "res_w"))
        while state["arrived"] != 2:
            pass
        state["go"] = True
        # give the two registers a moment to collide, then make fd
        # readable AND writable
        t0 = time.monotonic()
        while time.monotonic() - t0 < 0.002:
            pass
        # drain b (frees a's send buffer -> a writable), send to a (readable)
        while True:
            try:
                if not b.recv(65536):
                    break
            except BlockingIOError:
                break
        try:
            b.send(b"y")
        except BlockingIOError:
            pass
        deadline = time.monotonic() + (PARK_MS / 1000.0) + 1.5
        while (state["res_r"] is None or state["res_w"] is None) and \
                time.monotonic() < deadline:
            rc.sched_sleep(0.001)
        rr, rw = state["res_r"], state["res_w"]
        bad = (not isinstance(rr, int)) or (not isinstance(rw, int)) or \
              rr == 0 or rw == 0 or rr is None or rw is None
        if bad:
            import select
            rd, wr, _ = select.select([fd], [fd], [], 0)
            print("round %d: res_r=%r res_w=%r readable=%r writable=%r"
                  % (rnd, rr, rw, bool(rd), bool(wr)), flush=True)
            if isinstance(rr, str) or isinstance(rw, str):
                eexist += 1
            else:
                lost += 1
        rc.netpoll_unregister(fd)
        a.close()
        b.close()
    print("rounds=%d eexist_failures=%d lost_wakeups=%d" % (ROUNDS, eexist, lost),
          flush=True)
    if eexist or lost:
        sys.exit(1)


runloom.run(4, main)
