# Repro: RUNLOOM_PERHUB_EPOLL cross-pool migration drops the already-armed
# direction (netpoll_register.c.inc: migration sets target = need, not cur|need).
#
# Scenario: fiber R parks on READ (fd armed IN in hub H1's epoll, owner=H1).
# A fiber on another hub H2 then parks on WRITE for the SAME fd ->
# cross-pool migration: EPOLL_CTL_DEL from H1's epoll, fresh ADD into the
# shared epoll with ONLY the new caller's direction (OUT).  The IN arm is
# gone from every epoll, so when the peer sends data the reader never wakes:
# it burns its full timeout and returns 0 (or hangs forever with timeout=-1).
import socket, sys, time
import runloom
import runloom_c as rc

READ, WRITE = 1, 2
res = {}

def main():
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    def reader():
        t0 = time.monotonic()
        r = rc.wait_fd(a.fileno(), READ, 8000)
        res["r"] = r
        res["rt"] = time.monotonic() - t0
    runloom.fiber(reader)
    runloom.sleep(0.3)            # reader parked; IN armed, owner = reader's hub pool
    def writer(i):
        # socketpair is immediately writable; this returns fast, but its
        # REGISTER already ran (and, cross-hub, migrated the fd).
        res["w%d" % i] = rc.wait_fd(a.fileno(), WRITE, 2000)
    for i in range(8):            # 8 writers over 4 hubs: >=1 lands off-hub
        runloom.fiber(writer, i)
    runloom.sleep(0.5)            # writers done
    t_send = time.monotonic()
    b.send(b"x")                  # fd is now READABLE
    res["sent"] = True
    runloom.sleep(0.2)
    a2, b2 = a, b                 # keep sockets alive until run() drains

runloom.run(4, main)
w = [res.get("w%d" % i) for i in range(8)]
print("writers:", w)
print("reader result:", res.get("r"), "elapsed: %.2fs" % res.get("rt", -1))
if res.get("r") == READ and res.get("rt", 99) < 2.0:
    print("OK: reader woke on data")
    sys.exit(0)
else:
    print("BUG: reader lost its READ arm (timed out / stale wake)")
    sys.exit(1)
