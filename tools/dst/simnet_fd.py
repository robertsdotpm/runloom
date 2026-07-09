"""Socketpair-backed sim connections (Slice 3, RUNLOOM_SIM) -- the byte/readiness
plane over REAL fds.

Slice 0/1 (simnet.py) modelled protocol logic over Chans -- it never touched
fds/netpoll.  This runs a REAL socket workload under RUNLOOM_SIM: real send()/recv()
on real socketpairs (so EAGAIN / short-read / byte semantics are the kernel's, and
the full real park/commit/deadline/wake path is exercised), while the WAKE is
model-driven via the per-scheduler ready ledger -- runloom_c.sim_deliver_ready,
dispatched by the sim pump in a seed-stable (deliver_at, conn_id, dir) order.

Two topologies:

  * DIRECT (delay_fn=None, increment 1): ONE socketpair per connection; a send
    writes the peer's end directly and wakes the peer reader via the ledger.
    Zero delay, no faults.

  * MITM (delay_fn given, increment 2): TWO socketpairs per connection
    (app <-> model, on each side) plus a per-direction SHUTTLER fiber.  A send
    writes the app<->model socketpair and wakes the MODEL; the shuttler recv's it,
    holds it for a seed-drawn delay (a sched_sleep on the LOGICAL clock -- so a
    delivery scheduled at logical T is dispatched at T, order-independently),
    then writes the reader's model<->app socketpair and wakes the reader.  Bytes
    do not reach the reader until after the delay, so the delay is honoured
    regardless of send/recv interleaving.  Stream order is preserved (one shuttler
    per direction, serialized); reorder/loss/partition are later increments.

Under sim the pump never epoll_waits, so a socketpair reader parked on EAGAIN is
woken ONLY by the ledger.  Off sim the same wrapper still works (the real epoll
pump wakes it; sim_deliver_ready is a no-op).  H=1 / single-thread only.  See
docs/dev/soak/SIM_IO_DST.md.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
os.environ.setdefault("RUNLOOM_SIM", "1")               # this IS a sim module
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")     # sim shares one clock
import runloom_c

READ = 0x1
WRITE = 0x2
# Pin the socketpair buffer sizes so the residual (kernel-driven) EAGAIN / short-
# write cadence is a fixed host constant -- within-host replay stays bit-exact.
_SNDBUF = _RCVBUF = 1 << 16
_CHUNK = 1 << 16


def _setup(s):
    s.setblocking(False)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _SNDBUF)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _RCVBUF)
    except OSError:
        pass


class SimFdEndpoint(object):
    """One app-facing end of a sim connection.  send/recv are REAL on my fd;
    send wakes `wake_fd`'s parker via the ledger (the peer reader in DIRECT mode,
    the model shuttler in MITM mode)."""

    def __init__(self, conn, my_fd, wake_fd):
        self._conn = conn
        self._fd = my_fd
        self._wake_fd = wake_fd

    def send(self, data):
        n = runloom_c.tcp_send(self._fd, data)               # real send; WRITE-park on EAGAIN
        runloom_c.sim_deliver_ready(self._conn.conn_id, self._wake_fd, READ)
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
    """A socketpair-backed sim connection with app endpoints `a` and `b`.

    delay_fn: None -> DIRECT (zero-delay).  A callable returning a delay in
    seconds -> MITM: each message is held for delay_fn() logical seconds by a
    shuttler fiber before delivery.  delay_fn is called once per message per
    direction; draw it off the scenario's ONE seeded rng for determinism.

    loss_fn: optional callable returning True to DROP a chunk (a modelled loss --
    protocol logic, like a lost segment with no retransmit; the bytes never
    arrive).  Requires MITM (a delay_fn).  Disruptive: it breaks byte-conservation
    by design, so a conservation workload leaves it off (mirrors sim_program's
    P_LOSS=0)."""

    def __init__(self, delay_fn=None, loss_fn=None):
        self._delay_fn = delay_fn
        self._loss_fn = loss_fn
        self._socks = []
        if loss_fn is not None and delay_fn is None:
            # loss needs the MITM to hold+drop bytes; a direct socketpair can't.
            delay_fn = self._delay_fn = (lambda: 0.0)
        if delay_fn is None:
            a_app, b_app = socket.socketpair()
            _setup(a_app)
            _setup(b_app)
            self._socks = [a_app, b_app]
            self.conn_id = runloom_c.sim_conn_register(a_app.fileno(), b_app.fileno())
            # DIRECT: a send wakes the PEER app reader.
            self.a = SimFdEndpoint(self, a_app.fileno(), b_app.fileno())
            self.b = SimFdEndpoint(self, b_app.fileno(), a_app.fileno())
        else:
            a_app, a_mid = socket.socketpair()
            b_app, b_mid = socket.socketpair()
            for s in (a_app, a_mid, b_app, b_mid):
                _setup(s)
            self._socks = [a_app, a_mid, b_app, b_mid]
            self.conn_id = runloom_c.sim_conn_register(a_app.fileno(), b_app.fileno())
            # MITM: a send wakes the MODEL on its OWN mid fd.
            self.a = SimFdEndpoint(self, a_app.fileno(), a_mid.fileno())
            self.b = SimFdEndpoint(self, b_app.fileno(), b_mid.fileno())
            # A->B: model reads a_mid, delivers to b_app (write b_mid, wake b_app).
            self._spawn_shuttle(a_mid.fileno(), b_mid.fileno(), b_app.fileno())
            # B->A: model reads b_mid, delivers to a_app (write a_mid, wake a_app).
            self._spawn_shuttle(b_mid.fileno(), a_mid.fileno(), a_app.fileno())

    def _spawn_shuttle(self, read_fd, write_fd, wake_fd):
        conn_id = self.conn_id
        delay_fn = self._delay_fn
        loss_fn = self._loss_fn

        def shuttle():
            # Loops until EOF (peer app closed) or the settled-deadlock reap
            # terminates it (OSError) once nothing is left to shuttle -- so an
            # idle shuttler never wedges run(); it is netpoll-parked, not counted
            # by the count_deadlocked (chan/safe) census.
            while True:
                try:
                    chunk = runloom_c.tcp_recv_alloc(read_fd, _CHUNK)
                except OSError:
                    break
                if not chunk:
                    break
                if loss_fn is not None and loss_fn():
                    continue                         # DROP: the chunk never arrives
                d = delay_fn()
                if d and d > 0:
                    runloom_c.sched_sleep(d)          # logical-clock delay (a sleeper)
                try:
                    runloom_c.tcp_send(write_fd, chunk)
                except OSError:
                    break
                runloom_c.sim_deliver_ready(conn_id, wake_fd, READ)

        runloom_c.fiber(shuttle)

    def close(self):
        for s in self._socks:
            try:
                runloom_c.netpoll_release_if_idle(s.fileno())
            except Exception:
                pass
            try:
                s.close()
            except OSError:
                pass


# --------------------------------------------------------------------------- #
#  A self-contained deterministic byte-plane WORKLOAD, for the soak fleet.     #
# --------------------------------------------------------------------------- #
def simfd_program(seed, timeout=20.0):
    """A pure-function-of-seed socketpair workload: K client/server pairs over MITM
    sim connections (seed-drawn logical delay); each client sends M token bytes,
    the server echoes them, every client must get its own bytes back.

    Unlike simnet.py's sim_program (Chan-based -- protocol logic only), this runs
    over REAL socketpairs + wait_fd, so it exercises the C park/commit FSM +
    deadline heap + wake routing DETERMINISTICALLY -- genuinely different coverage.
    Oracle: exact per-client CONSERVATION (a lost/mis-delivered byte, or a reader
    reaped as a settled deadlock -> a missing/wrong result), plus _self_check.
    (count_deadlocked censuses only chan/safe parks, so a netpoll-plane deadlock
    surfaces as a CONSERVATION miss here until the PARKED_NETPOLL census lands.)
    Returns (ok, reason)."""
    import random
    runloom_c.sim_reset()                                # fresh clock/ledger/registry
    rng = random.Random(seed)
    k = rng.randint(1, 4)
    m = rng.randint(1, 6)
    delay_max = rng.random() * 0.02
    conns = [SimFdConn(delay_fn=lambda: rng.random() * delay_max) for _ in range(k)]
    results = {}

    def server(cid):
        try:
            got = conns[cid].b.recv_exact(m)
            conns[cid].b.sendall(got)                    # echo this client's bytes
        except OSError:
            pass                                         # reaped -> client sees a miss

    def client(cid):
        try:
            payload = bytes([(cid * 13 + i) & 0xff for i in range(m)])
            conns[cid].a.sendall(payload)
            back = conns[cid].a.recv_exact(m)
            results[cid] = sum(back)
        except OSError:
            pass

    runloom_c.set_deadlock_mode(1)
    for cid in range(k):
        runloom_c.fiber(lambda cid=cid: server(cid))
    for cid in range(k):
        runloom_c.fiber(lambda cid=cid: client(cid))
    runloom_c.run()
    for conn in conns:
        conn.close()

    for cid in range(k):
        want = sum((cid * 13 + i) & 0xff for i in range(m))
        if results.get(cid) != want:
            return False, ("CONSERVATION client={0} got={1} want={2} seed={3}"
                           .format(cid, results.get(cid), want, seed))
    if runloom_c._self_check(0) != 0:
        return False, "SELF_CHECK seed={0}".format(seed)
    return True, "ok"

