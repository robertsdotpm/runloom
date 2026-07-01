# Instrumented: record OS thread idents to confirm reader/writers cross hubs.
import socket, sys, time, threading
import runloom
import runloom_c as rc

READ, WRITE = 1, 2
res = {}

def main():
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    def reader():
        res["r_tid"] = threading.get_ident()
        t0 = time.monotonic()
        r = rc.wait_fd(a.fileno(), READ, 8000)
        res["r"] = r
        res["rt"] = time.monotonic() - t0
    runloom.fiber(reader)
    runloom.sleep(0.3)
    def writer(i):
        res["w%d_tid" % i] = threading.get_ident()
        res["w%d" % i] = rc.wait_fd(a.fileno(), WRITE, 2000)
    for i in range(8):
        runloom.fiber(writer, i)
    runloom.sleep(0.5)
    res["send_t"] = time.monotonic()
    b.send(b"x")
    runloom.sleep(1.5)
    res["socks"] = (a, b)

t_start = time.monotonic()
runloom.run(4, main)
r_tid = res.get("r_tid")
w_tids = set(res.get("w%d_tid" % i) for i in range(8))
print("reader tid:", r_tid)
print("writer tids:", w_tids)
print("cross-hub writer existed:", any(t != r_tid for t in w_tids))
print("reader result:", res.get("r"), "elapsed %.3fs" % res.get("rt", -1))
ok = res.get("r") == READ and res.get("rt", 99) < 1.2
print("OK" if ok else "BUG")
sys.exit(0 if ok else 1)
