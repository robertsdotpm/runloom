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


class SimError(OSError):
    """A modelled socket error (ECONNRESET), analogous to simnet.py's SimError."""


def _setup(s):
    s.setblocking(False)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _SNDBUF)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _RCVBUF)
    except OSError:
        pass


class SimFdEndpoint(object):
    """One app-facing end of a sim connection.

    The SYMMETRIC DRAINER-POSTS rule (increment W): every send posts READ for the
    peer end (`wake_fd`); every recv posts WRITE for the peer end.  A post with no
    parker is dropped harmlessly by the ledger.  This is what discharges
    backpressure both ways -- a sender that WRITE-parks inside the C send when its
    socketpair fills is woken by the DRAINER (the consumer's recv posting WRITE),
    and a reader that READ-parks is woken by the producer's send posting READ.
    Without it, a >socketpair-buffer send strands (the C send parks WRITE before
    the wrapper can post, so nobody wakes -- the confirmed pre-W bug)."""

    def __init__(self, conn, my_fd, wake_fd):
        self._conn = conn
        self._fd = my_fd
        self._wake_fd = wake_fd

    def send(self, data):
        # SINGLE-SHOT (partial-write): tcp_send_once does ONE send syscall and
        # cooperatively WRITE-parks on EAGAIN; when the buffer is full it parks
        # BEFORE any bytes go in and returns only after a send succeeded, so the
        # n>0 post can never post for bytes that did not enter the buffer.  Post
        # READ for the peer after EVERY successful chunk, before the next
        # (potentially parking) call -- so any bytes in the pipe always have a
        # pending wake for their consumer.  Use sendall() for the full-write surface.
        if self._conn.reset_flag:                            # RST-discard: raise before touching the fd
            raise SimError("ECONNRESET on send")
        try:
            n = runloom_c.tcp_send_once(self._fd, data)
        except OSError:
            if self._conn.reset_flag:                        # cancel_fd woke us out of a WRITE park
                raise SimError("ECONNRESET on send")
            raise
        if n > 0:
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
        if self._conn.reset_flag:                            # RST-discard: raise even if buffered data exists
            raise SimError("ECONNRESET on recv")
        try:
            chunk = runloom_c.tcp_recv_alloc(self._fd, n)    # real recv; READ-park on EAGAIN, ledger wakes
        except OSError:
            if self._conn.reset_flag:                        # cancel_fd woke us out of a READ park
                raise SimError("ECONNRESET on recv")
            raise
        if chunk:
            # drained my end -> freed the peer sender's buffer -> wake a WRITE-parked peer
            runloom_c.sim_deliver_ready(self._conn.conn_id, self._wake_fd, WRITE)
        return chunk

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
        self.reset_flag = False
        self.partition_until = 0.0        # logical seconds; deliveries held until then
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
            # A->B: model reads a_mid, delivers to b_app (write b_mid, wake b_app);
            # frees the a-side sender by posting WRITE to a_app after each drain.
            self._spawn_shuttle(a_mid.fileno(), b_mid.fileno(), b_app.fileno(),
                                a_app.fileno())
            # B->A: model reads b_mid, delivers to a_app (write a_mid, wake a_app).
            self._spawn_shuttle(b_mid.fileno(), a_mid.fileno(), a_app.fileno(),
                                b_app.fileno())

    def _spawn_shuttle(self, read_fd, write_fd, wake_fd, sender_fd):
        conn_id = self.conn_id
        delay_fn = self._delay_fn
        loss_fn = self._loss_fn
        conn = self

        def shuttle():
            # Loops until EOF (peer app closed), a reset (conn.reset_flag), or the
            # settled-deadlock reap (OSError) once nothing is left to shuttle -- so
            # an idle shuttler never wedges run(); it is netpoll-parked, not counted
            # by the count_deadlocked (chan/safe) census.  The symmetric
            # drainer-posts rule applies here too: after draining read_fd, post
            # WRITE to the SENDER's app fd (it may be WRITE-parked on a full
            # app<->mid pipe); forward via single-shot chunks posting READ to the
            # reader per chunk (a full mid<->app pipe WRITE-parks us here, woken
            # by the reader's recv posting WRITE to write_fd).  reset() wakes an
            # fd-parked shuttler via cancel_fd (-> CANCELLED -> OSError -> break);
            # a shuttler mid-sched_sleep is not fd-parked, so it checks reset_flag
            # after the sleep and breaks BEFORE touching any (possibly-being-torn-
            # down) fd -- the fd-reuse-safety hinge.
            while True:
                if conn.reset_flag:
                    break
                try:
                    chunk = runloom_c.tcp_recv_alloc(read_fd, _CHUNK)
                except OSError:
                    break
                if not chunk:
                    break
                # drained read_fd -> freed the sender's app<->mid buffer
                runloom_c.sim_deliver_ready(conn_id, sender_fd, WRITE)
                if loss_fn is not None and loss_fn():
                    continue                         # DROP: the chunk never arrives
                d = delay_fn()
                if conn.partition_until > 0.0:
                    # PARTITION: hold this chunk until the (logical) heal time.  The
                    # hold is a logical sleeper, so it keeps the system unsettled --
                    # a reader parked through the partition is NOT reaped; time
                    # compresses to the heal instant.  Chunks recv'd during the
                    # partition all deliver at/after partition_until, in order (one
                    # serialized shuttler per direction).
                    now = runloom_c._logical_ns() / 1e9
                    gap = conn.partition_until - now
                    if gap > d:
                        d = gap
                if d and d > 0:
                    runloom_c.sched_sleep(d)          # logical-clock delay (a sleeper)
                    if conn.reset_flag:              # woke into a reset -> do not touch fds
                        break
                mv = memoryview(chunk)
                broke = False
                while mv:
                    try:
                        n = runloom_c.tcp_send_once(write_fd, mv)   # WRITE-park on full pipe
                    except OSError:
                        broke = True
                        break
                    if n <= 0:
                        runloom_c.sched_yield()
                        continue
                    runloom_c.sim_deliver_ready(conn_id, wake_fd, READ)
                    mv = mv[n:]
                if broke:
                    break

        runloom_c.fiber(shuttle)

    def reset(self):
        """Modelled connection reset (protocol logic, not a kernel RST -- an
        AF_UNIX socketpair cannot emit one).  RST-DISCARD semantics: both
        directions die, and both endpoints observe SimError(ECONNRESET) on their
        next op even if kernel-buffered pre-reset data exists.

        Mechanism: set reset_flag, then netpoll_cancel_fd every fd -- which wakes
        every fd-parked fiber (reader, writer, shuttler; any direction) with the
        CANCELLED sentinel, so wait_fd returns -1 and the op raises WITHOUT
        retrying (fd-reuse-safe by construction -- unlike a normal-mask ledger
        wake, which would retry the op).  A shuttler mid-sched_sleep is not
        fd-parked; it checks reset_flag after the sleep and exits.  The fds are
        NOT closed here (close() does that) -- so no fd number is freed for reuse
        while a stale parker or sleeper could still reference it; the whole
        fd-reuse teardown hazard is dissolved by never closing on reset.  Wakes
        are observable only after the next scheduler turn, not synchronously."""
        self.reset_flag = True
        for s in self._socks:
            try:
                runloom_c.netpoll_cancel_fd(s.fileno())
            except Exception:
                pass

    def partition_until_t(self, t_logical_seconds):
        """Withhold every delivery until logical time `t_logical_seconds` (a
        partition); the heal is that instant passing.  Deliveries are HELD (not
        dropped -- compose with loss_fn for a loss-partition) and arrive at/after
        the heal, in order.  Requires MITM (a shuttler) -- a stale value < now is
        inert.  Re-call with a later t to extend.  Idempotent to set from a
        scenario in seeded/program order for determinism."""
        self.partition_until = t_logical_seconds

    def logical_now(self):
        """Current logical time in seconds (for computing a partition heal time)."""
        return runloom_c._logical_ns() / 1e9

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
    reaps = runloom_c.sim_reap_count()
    for conn in conns:
        conn.close()

    # REAP oracle (increment O): in a clean run the ONLY parkers left at settle are
    # the 2 shuttlers per MITM conn (all conns here are MITM); every workload fiber
    # completed.  Any EXCESS is a stranded client/server -- a netpoll-plane lost
    # wake the chan/safe count_deadlocked census cannot see.  Structural, instant.
    expected_reaps = 2 * k
    if reaps != expected_reaps:
        return False, ("REAP reaps={0} expected={1} (stranded fiber -- netpoll "
                       "lost wake) seed={2}".format(reaps, expected_reaps, seed))
    for cid in range(k):
        want = sum((cid * 13 + i) & 0xff for i in range(m))
        if results.get(cid) != want:
            return False, ("CONSERVATION client={0} got={1} want={2} seed={3}"
                           .format(cid, results.get(cid), want, seed))
    if runloom_c._self_check(0) != 0:
        return False, "SELF_CHECK seed={0}".format(seed)
    return True, "ok"

