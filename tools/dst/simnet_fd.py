"""Socketpair-backed sim connections (Slice 3, RUNLOOM_SIM) -- the byte/readiness
plane over REAL fds.

Slice 0/1 (simnet.py) modelled protocol logic over Chans -- it never touched
fds/netpoll.  This runs a REAL socket workload under RUNLOOM_SIM: real send()/recv()
on a real socketpair (so EAGAIN / short-read / byte semantics are the kernel's, and
the full real park/commit/deadline/wake path is exercised), while the WAKE is
model-driven via the per-scheduler ready ledger -- runloom_c.sim_deliver_ready,
dispatched by the sim pump in a seed-stable (deliver_at, conn_id, dir) order.  So
readiness ORDER is a function of the seed, not the kernel's epoll ordering.

Under sim the pump never epoll_waits, so a socketpair reader parked on EAGAIN is
woken ONLY by the ledger.  Off sim the same wrapper still works (the real epoll
pump wakes it and sim_deliver_ready is a no-op) -- a nice belt-and-suspenders.

Increment 1: zero-delay, no-fault, ONE socketpair per connection.  Delay / loss /
reorder / partition (a MITM model goroutine) and WRITE-readiness are later
increments of the SAME ledger.  H=1 / single-thread only.  See
docs/dev/soak/SIM_IO_DST.md.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")     # sim shares one clock
import runloom_c

READ = 0x1
WRITE = 0x2
# Pin the socketpair buffer sizes so the residual (kernel-driven) EAGAIN / short-
# write cadence is a fixed host constant -- within-host replay stays bit-exact.
_SNDBUF = _RCVBUF = 1 << 16


class SimFdEndpoint(object):
    """One end of a socketpair-backed sim connection.  send/recv are REAL on my
    fd; send appends a READ delivery for the PEER fd so a reader parked there is
    woken by the ledger at the current logical instant."""

    def __init__(self, conn, my_fd, peer_fd):
        self._conn = conn
        self._fd = my_fd
        self._peer_fd = peer_fd

    def send(self, data):
        n = runloom_c.tcp_send(self._fd, data)               # real send; WRITE-park on EAGAIN
        runloom_c.sim_deliver_ready(self._conn.conn_id, self._peer_fd, READ)
        return n

    def sendall(self, data):
        mv = memoryview(bytes(data))
        while mv:
            sent = self.send(mv)
            if sent <= 0:
                runloom_c.sched_yield()
                continue
            mv = mv[sent:]

    def recv(self, n):
        return runloom_c.tcp_recv_alloc(self._fd, n)         # real recv; READ-park on EAGAIN, ledger wakes

    def recv_exact(self, n):
        """Loop recv until n bytes or EOF (kernel short-reads are real)."""
        buf = b""
        while len(buf) < n:
            chunk = self.recv(n - len(buf))
            if not chunk:
                break
            buf += chunk
        return buf


class SimFdConn(object):
    """A socketpair-backed sim connection: two endpoints (`a`, `b`) over ONE real
    socketpair, registered so its conn_id is the ready-ledger ordering key."""

    def __init__(self):
        s_a, s_b = socket.socketpair()                       # AF_UNIX SOCK_STREAM
        s_a.setblocking(False)
        s_b.setblocking(False)
        for s in (s_a, s_b):
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _SNDBUF)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _RCVBUF)
            except OSError:
                pass
        self._s_a, self._s_b = s_a, s_b                      # hold refs; else GC closes the fds
        self.conn_id = runloom_c.sim_conn_register(s_a.fileno(), s_b.fileno())
        self.a = SimFdEndpoint(self, s_a.fileno(), s_b.fileno())
        self.b = SimFdEndpoint(self, s_b.fileno(), s_a.fileno())

    def close(self):
        for s in (self._s_a, self._s_b):
            try:
                runloom_c.netpoll_release_if_idle(s.fileno())
            except Exception:
                pass
            try:
                s.close()
            except OSError:
                pass
