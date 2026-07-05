"""Soak workload shapes (docs/dev/RELIABILITY_PROGRAM.md R1).

Each workload runs the runloom scheduler continuously until the shared Ctx
deadline passes, bumping ctx.progress once per unit of work so the sampler can
prove the scheduler is still making progress (a frozen counter = a wedge).

The point of a soak is CYCLES, not wall-clock: long-uptime bugs are about how
many times the create/destroy cycle ran, so every workload maximizes object
lifecycle turnover (spawn/join, connect/close, park/wake) rather than sitting
idle.  --churn-compress (see soak.py) simply removes the inter-unit yields so a
day of it ages the runtime like months of steady traffic.

A workload MUST fully drain each unit (no stranded fiber -- that leaks a stack
by construction and would false-positive the slope oracle); the negative
control `leak_control` is the ONE that deliberately leaks, to prove the oracle
has teeth.
"""
import gc
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import runloom
import runloom.monkey
import runloom_c


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _spin(ctx, unit, concurrency=1):
    """Drive `unit()` in `concurrency` parallel fibers, each looping until the
    ctx deadline, bumping progress per unit.  Returns when the deadline passes
    (every worker fiber has returned -> run() drains)."""
    def worker():
        while not ctx.expired():
            unit()
            ctx.bump()
            if not ctx.compress:
                runloom_c.sched_yield()
    def root():
        for _ in range(concurrency):
            runloom_c.fiber(worker)
    runloom_c.fiber(root)
    runloom_c.run()


# ---------------------------------------------------------------------------
# workload units
# ---------------------------------------------------------------------------
def _wl_spawn_churn(ctx):
    # Goroutine create/die churn -- ages the g slab + coro-stack depot.
    def unit():
        done = [0]
        def child():
            done[0] += 1
        for _ in range(16):
            runloom_c.fiber(child)
        for _ in range(3):
            runloom_c.sched_yield()
    _spin(ctx, unit, concurrency=4)


def _wl_chan_select(ctx):
    # Producer -> consumer over a chan, joined through a done chan (fully
    # drains).  Ages chan waiter park/unpark + select case order.
    def unit():
        ch = runloom_c.Chan()
        done = runloom_c.Chan()
        def consumer():
            n = 0
            while True:
                v, ok = ch.recv()
                if not ok:
                    break
                n += 1
            done.send(n)
        runloom_c.fiber(consumer)
        for i in range(24):
            ch.send(i)
        ch.close()
        done.recv()
    _spin(ctx, unit, concurrency=3)


def _wl_timer(ctx):
    # Timer storm -- ages the sleep heap + timed parkers.
    def unit():
        def sleeper():
            runloom_c.sched_sleep(0.001)
        for _ in range(8):
            runloom_c.fiber(sleeper)
        runloom_c.sched_sleep(0.005)
    _spin(ctx, unit, concurrency=2)


def _wl_tcp_churn(ctx):
    # Connect / serve one echo / close storm -- maximize the socket
    # create/destroy cycle (netpoll parkers + fd arm cache + monkey socket
    # side tables).  A fresh listener + client per unit, both fully closed.
    def unit():
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(4)
        port = srv.getsockname()[1]
        result = [None]
        def server():
            conn, _ = srv.accept()
            try:
                data = conn.recv(64)
                conn.sendall(data)
            finally:
                conn.close()
        def client():
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                c.connect(("127.0.0.1", port))
                c.sendall(b"ping")
                result[0] = c.recv(64)
            finally:
                c.close()
        gs = runloom.fiber(server)
        gc_ = runloom.fiber(client)
        # both spawned; the outer unit fiber yields until they finish
        for _ in range(200):
            if result[0] is not None:
                break
            runloom_c.sched_yield()
        srv.close()
    # tcp_churn uses monkey sockets, so drive it through runloom.run via _spin
    _spin(ctx, unit, concurrency=4)


def _wl_keepalive(ctx):
    # Many idle connections held open with a periodic ping -- the N=1M keepalive
    # shape (parkers dwelling, low arrival rate).  Establishes a pool of echo
    # connections once, then pings them in a loop; ages long-dwell parkers +
    # the stack-park sweep.
    NCONN = int(os.environ.get("SOAK_KEEPALIVE_CONNS", "64"))
    def body():
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(NCONN)
        port = srv.getsockname()[1]
        conns = []
        def echo(conn):
            try:
                while True:
                    d = conn.recv(64)
                    if not d:
                        break
                    conn.sendall(d)
            except OSError:
                pass
            finally:
                conn.close()
        def acceptor():
            for _ in range(NCONN):
                conn, _ = srv.accept()
                runloom.fiber(lambda c=conn: echo(c))
        runloom.fiber(acceptor)
        for _ in range(NCONN):
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(("127.0.0.1", port))
            conns.append(c)
        # ping loop until deadline
        while not ctx.expired():
            for c in conns:
                c.sendall(b"p")
                c.recv(1)
                ctx.bump()
            if not ctx.compress:
                runloom_c.sched_sleep(0.05)
        for c in conns:
            c.close()
        srv.close()
    runloom.fiber(body)
    runloom_c.run()


def _wl_offload(ctx):
    # Blocking-pool offload churn -- ages the offload backend + its parkers.
    def unit():
        def blocking():
            return sum(range(256))
        runloom.blocking(blocking)
    _spin(ctx, unit, concurrency=4)


def _wl_mixed(ctx):
    # All of the above interleaved in one process -- the default and highest-
    # signal shape (a real service does all of these at once).  Each unit does
    # a bit of every kind so no single subsystem's slope hides behind another.
    def unit():
        # spawn
        done = [0]
        for _ in range(8):
            runloom_c.fiber(lambda: done.__setitem__(0, done[0] + 1))
        # chan
        ch = runloom_c.Chan()
        dch = runloom_c.Chan()
        def cons():
            while True:
                _, ok = ch.recv()
                if not ok:
                    break
            dch.send(1)
        runloom_c.fiber(cons)
        for i in range(12):
            ch.send(i)
        ch.close()
        dch.recv()
        # timer
        runloom_c.sched_sleep(0.001)
        # offload
        runloom.blocking(lambda: sum(range(128)))
    _spin(ctx, unit, concurrency=3)


# ---------------------------------------------------------------------------
# NEGATIVE CONTROL -- proves the slope oracle has teeth
# ---------------------------------------------------------------------------
_LEAK_SINK = []


def _wl_leak_control(ctx):
    # Deliberately leaks: append a growing object to a module-global list every
    # unit and NEVER release it.  RSS + Python object counts must climb, so the
    # slope oracle MUST fail this workload.  If it passes, the oracle is blind.
    def unit():
        _LEAK_SINK.append(bytearray(4096))   # 4 KB/unit, retained forever
    _spin(ctx, unit, concurrency=2)


WORKLOADS = {
    "spawn_churn": _wl_spawn_churn,
    "chan_select": _wl_chan_select,
    "timer": _wl_timer,
    "tcp_churn": _wl_tcp_churn,
    "keepalive": _wl_keepalive,
    "offload": _wl_offload,
    "mixed": _wl_mixed,
    "leak_control": _wl_leak_control,
}
