# Stress repro: runloom_netpoll_register computes the target direction set under
# runloom_pool.lock but issues the epoll_ctl OUTSIDE the lock.  Two concurrent
# registers on the same fd (READ from one hub, WRITE from another) can execute
# their syscalls in the opposite order of their target computation:
#   A: cur=0 -> armed=R, unlock
#   B: cur=R -> armed=RW, unlock, MOD(RW) -> ENOENT -> ADD(RW)  [wins first]
#   A: ADD(R) -> EEXIST -> MOD(R)          [narrows kernel set to R]
# leaving the kernel registered for READ only while the arm cache says RW.
# The WRITE parker never gets its event -> spurious timeout / infinite hang.
#
# Run with RUNLOOM_PERHUB_EPOLL=0 to isolate from the (separate) cross-pool
# migration bug.
import os, socket, sys, time, threading
import runloom
import runloom_c as rc

READ, WRITE = 1, 2
N_ROUNDS = int(os.environ.get("ROUNDS", "400"))

state = {"go_r": 0, "go_w": 0, "arr_r": 0, "arr_w": 0,
         "res_r": None, "res_w": None, "fd": -1, "stop": False,
         "tid_r": 0, "tid_w": 0}

def worker(role):
    my = 1
    go_k, arr_k, other_arr, res_k, tid_k = (
        ("go_r", "arr_r", "arr_w", "res_r", "tid_r") if role == "r"
        else ("go_w", "arr_w", "arr_r", "res_w", "tid_w"))
    ev = READ if role == "r" else WRITE
    while not state["stop"]:
        if state[go_k] != my:
            rc.yield_()
            continue
        fd = state["fd"]
        state[tid_k] = threading.get_ident()
        state[arr_k] = my
        spins = 0
        while state[other_arr] != my:      # tight barrier; yield if starved
            spins += 1
            if spins > 200000:
                rc.yield_(); spins = 0
        try:
            r = rc.wait_fd(fd, ev, 800)
        except OSError as e:
            r = "exc:%s" % e.errno
        state[res_k] = r
        my += 1

def main():
  try:
    runloom.fiber(worker, "r")
    runloom.fiber(worker, "w")
    runloom.sleep(0.2)
    bad = []
    cross = 0
    for rnd in range(1, N_ROUNDS + 1):
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        # fill a's send buffer so WRITE genuinely parks
        try:
            while True:
                a.send(b"\0" * 4096)
        except BlockingIOError:
            pass
        state["res_r"] = None; state["res_w"] = None
        state["fd"] = a.fileno()
        state["go_r"] = rnd; state["go_w"] = rnd
        # wait for both to arrive at the barrier (they then race into wait_fd)
        t0 = time.monotonic()
        while (state["arr_r"] != rnd or state["arr_w"] != rnd):
            runloom.sleep(0.001)
            if time.monotonic() - t0 > 5: raise RuntimeError("barrier wedge")
        runloom.sleep(0.005)               # let both link+register+park
        if state["tid_r"] != state["tid_w"]:
            cross += 1
        b.send(b"!")                       # a readable
        try:                               # drain b -> a writable
            while b.recv(65536):
                pass
        except BlockingIOError:
            pass
        t0 = time.monotonic()
        while state["res_r"] is None or state["res_w"] is None:
            runloom.sleep(0.001)
            if time.monotonic() - t0 > 3: break
        rr, rw = state["res_r"], state["res_w"]
        if rr != READ or rw != WRITE:
            # ground-truth: is the fd actually ready in the lost direction?
            import select as _sel
            pr, pw, _ = _sel.select([a], [a], [], 0)
            bad.append((rnd, rr, rw, bool(pr), bool(pw)))
            print("round %d: res_r=%r res_w=%r fd_readable=%s fd_writable=%s"
                  % (rnd, rr, rw, bool(pr), bool(pw)), flush=True)
        rc.netpoll_unregister(a.fileno())
        a.close(); b.close()
    state["summary"] = (len(bad), cross)
  finally:
    state["stop"] = True

runloom.run(3, main)
nbad, cross = state["summary"]
print("rounds=%d cross_hub=%d lost_wakeups=%d" % (N_ROUNDS, cross, nbad))
sys.exit(1 if nbad else 0)
