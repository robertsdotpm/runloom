"""Deterministic simulated network for DST -- the third pillar (Slice 0).

runloom already has the other two DST pillars: deterministic SCHEDULING (the
controlled baton) and deterministic TIME (the logical clock).  This adds
deterministic I/O, but at the level the WHOLE thing can be validated cheaply and
correctly first: a pure-Python cooperative sim-socket over the single-thread
scheduler + logical clock.  Every connect / accept / send / recv outcome, and
every delivery delay / loss / reset, is drawn from ONE seeded rng -- so a whole
network scenario is a pure function of its seed, and any lost-wake / deadlock is
reproducible from a single integer.

WHAT IT IS: a determinism amplifier for runloom's INTERNAL I/O plumbing -- the
scheduler-to-I/O boundary where the documented lost-wake / park-commit / deadlock
lineage lives.  It models protocol LOGIC (byte streams, connect/accept/close,
loss/delay/reorder/reset).  WHAT IT IS NOT: it does NOT model kernel/wire quirks
(Nagle, delayed-ACK, cwnd, TIME_WAIT, half-close, NAT rebinding, CGNAT port
allocation, simul-open RST).  It will not catch the NAT-traversal/hole-punch bug
class -- the real-network suites (tests/net, the netns chaos tools) own that.

Transport: each connection direction is a runloom Chan carrying byte chunks, so a
blocked recv PARKS on the real scheduler (not a spin) and an unfed recv surfaces
as a real runloom deadlock -- the instant, wall-clock-free hang oracle.  Delivery
latency is modeled by a delivery fiber that sched_sleep()s on the LOGICAL clock,
so "arrival timing" -- the open-system limit the baton header names -- becomes a
function of the seed.

Slice 0 runs under the single-thread scheduler.  Slice 1 composes it with the
baton at H=1; Slice 2 is the additive C `sim` netpoll backend; Slice 3 the real
socketpair-backed byte plane.  See docs/dev/soak/SIM_IO_DST.md.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
os.environ.setdefault("PYTHON_GIL", "0")
os.environ.setdefault("RUNLOOM_LOGICAL_CLOCK", "1")     # sched_sleep on logical time
import runloom_c


class SimError(OSError):
    """A modeled socket error (ECONNRESET / ECONNREFUSED / ETIMEDOUT)."""


class _Dir(object):
    """One direction of a connection: a byte-chunk Chan + a leftover buffer.  A
    closed Chan is EOF; a reset flag turns the next op into ECONNRESET."""

    def __init__(self, cap):
        self.ch = runloom_c.Chan(cap)
        self.buf = b""
        self.reset = False
        self.closed = False


class SimSocket(object):
    """Cooperative sim socket: the subset of the socket API the DST scenarios use.
    Blocking ops park on the transport Chan (real scheduler); faults are drawn
    from the owning SimNet's seeded rng."""

    def __init__(self, net):
        self._net = net
        self._addr = None            # bound/listen address
        self._accept = None          # accept queue Chan (listeners only)
        self._rx = None              # _Dir this socket reads from
        self._tx = None              # _Dir this socket writes to
        self._peer = None            # peer SimSocket (for close propagation)

    # --- server side ---
    def bind(self, addr):
        self._addr = addr

    def listen(self, backlog=8):
        self._accept = runloom_c.Chan(max(1, backlog))
        self._net._register_listener(self._addr, self._accept)

    def accept(self):
        conn, ok = self._accept.recv()
        if not ok:
            raise SimError("accept on closed listener")
        return conn, conn._peer_addr

    # --- client side ---
    def connect(self, addr):
        self._net._connect(self, addr)      # sets _rx/_tx/_peer or raises

    # --- data ---
    def send(self, data):
        d = self._tx
        if d is None or d.reset:
            raise SimError("ECONNRESET on send")
        n = self._net._faults.on_send(len(data))        # short-write draw
        if n <= 0:
            return 0
        chunk = bytes(data[:n])
        self._net._deliver(d, chunk)                     # loss / delay / rst inside
        return n

    def sendall(self, data):
        mv = memoryview(bytes(data))
        while mv:
            sent = self.send(mv)
            if sent == 0:
                runloom_c.sched_yield()
                continue
            mv = mv[sent:]

    def recv(self, n):
        d = self._rx
        if d is None:
            raise SimError("recv on unconnected socket")
        while not d.buf:
            if d.reset:
                raise SimError("ECONNRESET on recv")
            chunk, ok = d.ch.recv()                      # PARKS here if empty
            if not ok:
                return b""                               # clean EOF (peer closed)
            if chunk is _RESET:
                d.reset = True
                raise SimError("ECONNRESET on recv")
            d.buf += chunk
        take = self._net._faults.on_recv(len(d.buf), n)  # partial-read draw
        out, d.buf = d.buf[:take], d.buf[take:]
        return out

    def close(self):
        if self._tx is not None and not self._tx.closed:
            self._tx.closed = True
            try:
                self._tx.ch.close()
            except Exception:
                pass
        if self._accept is not None:
            try:
                self._accept.close()
            except Exception:
                pass


_RESET = object()                                        # in-band reset sentinel


class _Faults(object):
    """All I/O-outcome draws come through here, off ONE seeded rng, so the whole
    run is a pure function of the seed.  Probabilities are modest so most runs are
    clean (the interesting interleavings), with faults sprinkled for coverage."""

    def __init__(self, rng, cfg=None):
        self.rng = rng
        c = dict(P_CONNECT_FAIL=0.03, P_LOSS=0.02, P_RESET=0.02,
                 P_DELAY=0.30, DELAY_MAX=0.050, P_SHORTWRITE=0.15, P_PARTIAL=0.25)
        if cfg:
            c.update(cfg)
        self.c = c

    def connect_fails(self):
        return self.rng.random() < self.c["P_CONNECT_FAIL"]

    def on_send(self, n):
        if n > 1 and self.rng.random() < self.c["P_SHORTWRITE"]:
            return self.rng.randint(1, n - 1)
        return n

    def on_recv(self, avail, want):
        take = min(avail, want)
        if take > 1 and self.rng.random() < self.c["P_PARTIAL"]:
            return self.rng.randint(1, take - 1)
        return take

    def delivery(self):
        """Returns ('drop'|'reset'|'ok', delay_seconds)."""
        r = self.rng.random()
        if r < self.c["P_LOSS"]:
            return "drop", 0.0
        if r < self.c["P_LOSS"] + self.c["P_RESET"]:
            return "reset", 0.0
        delay = (self.rng.random() * self.c["DELAY_MAX"]
                 if self.rng.random() < self.c["P_DELAY"] else 0.0)
        return "ok", delay


class SimNet(object):
    """The in-memory network: a listener table + a fault model, both driven by one
    seeded rng.  `record(event)` receives a stream of structural events so the
    scenario's signature captures the byte trace."""

    def __init__(self, rng, record=None, cap=64, cfg=None):
        self.rng = rng
        self.record = record or (lambda ev: None)
        self.cap = cap
        self._faults = _Faults(rng, cfg)
        self._listeners = {}                             # addr -> accept Chan

    def socket(self):
        return SimSocket(self)

    def _register_listener(self, addr, accept_ch):
        self._listeners[addr] = accept_ch

    def _connect(self, client, addr):
        if self._faults.connect_fails() or addr not in self._listeners:
            self.record(("connect_fail", addr))
            raise SimError("ECONNREFUSED to %r" % (addr,))
        # a fresh full-duplex connection: two directions
        c2s = _Dir(self.cap)                             # client -> server
        s2c = _Dir(self.cap)                             # server -> client
        server = SimSocket(self)
        server._rx, server._tx, server._peer = c2s, s2c, client
        client._rx, client._tx, client._peer = s2c, c2s, server
        server._peer_addr = ("client", id(client) & 0xffff)
        self._listeners[addr].send(server)               # hand to accept()
        self.record(("connect_ok", addr))

    def _deliver(self, direction, chunk):
        """Apply the seed-drawn delivery outcome, timed on the LOGICAL clock."""
        kind, delay = self._faults.delivery()
        if kind == "drop":
            self.record(("drop", len(chunk)))
            return
        if kind == "reset":
            direction.reset = True
            try:
                direction.ch.send(_RESET)
            except Exception:
                pass
            self.record(("reset", len(chunk)))
            return
        if delay > 0.0:
            # arrival timing: a delivery fiber sleeps on the LOGICAL clock, then
            # delivers -- so late/early interleavings are a function of the seed.
            def deliver():
                runloom_c.sched_sleep(delay)
                if not direction.closed and not direction.reset:
                    try:
                        direction.ch.send(chunk)
                    except Exception:
                        pass
            runloom_c.fiber(deliver)
        else:
            direction.ch.send(chunk)
        self.record(("deliver", len(chunk)))
