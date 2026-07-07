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


def _wl_cserve_echo(ctx):
    # The production server shape, verbatim from the benchmark suite's fastest
    # regular-fiber tier (benchmark/suite/servers/runloom_epoll_py_tcpcon.py,
    # "runloom_c": 624K rps peak, above Go): runloom_c.serve's C scaffold
    # (SO_REUSEPORT listeners + C accept loops) spawning a plain-Python handler
    # fiber per connection that echoes via the C-level TCPConn recv_into/
    # send_all -- on the DEFAULT epoll backend (no io_uring env), under
    # runloom.run(hubs) M:N.  In-process client fibers provide the load:
    # connect, a burst of echo round-trips, close, reconnect -- so both the
    # steady path (recv/send parkers) and the lifecycle path (accept/spawn/
    # close) accrue cycles.  Everything is joined: clients via a Chan, handler
    # fibers end when their conn closes, acceptors end when the listeners are
    # closed -- run() then drains (no stranded fiber, per the module contract).
    hubs = int(os.environ.get("RUNLOOM_SOAK_HUBS", "4"))
    conc = int(os.environ.get("RUNLOOM_SOAK_CONC", "8"))
    RTRIPS = 64          # echo round-trips per connection before reconnecting
    CHUNK = 65536

    def handle(conn):
        # the benchmark tier's handler, verbatim
        buf = bytearray(CHUNK)
        mv = memoryview(buf)
        try:
            while True:
                n = conn.recv_into(buf)
                if not n:
                    break
                conn.send_all(mv[:n])
        except OSError:
            pass

    def client(port, done):
        buf = bytearray(64)
        try:
            while not ctx.expired():
                try:
                    c = runloom_c.TCPConn.connect("127.0.0.1", port)
                except OSError:
                    continue
                try:
                    for _ in range(RTRIPS):
                        c.send_all(b"ping-payload-64b" * 4)
                        got = 0
                        while got < 64:
                            n = c.recv_into(buf)
                            if not n:
                                raise OSError("early EOF")
                            got += n
                        ctx.bump()
                        if ctx.expired():
                            break
                except OSError:
                    pass
                finally:
                    c.close()
                if not ctx.compress:
                    runloom_c.sched_yield()
        finally:
            done.send(1)

    def root():
        # RUNLOOM_SOAK_CECHO_ALLC=1 -> handler=None runs each connection ENTIRELY
        # in C (runloom_mn_fiber_c: no Python tstate, no PyObjects in the recv/send
        # loop) -- soaks the pure C serve primitive.  Default keeps the Python echo
        # handler (C accept scaffold + a Python handler fiber per conn).
        srv_handler = None if os.environ.get("RUNLOOM_SOAK_CECHO_ALLC") == "1" else handle
        port, listeners = runloom_c.serve(
            "127.0.0.1", 0, srv_handler, acceptors=hubs, backlog=1024)
        done = runloom_c.Chan(conc)
        for _ in range(conc):
            runloom.fiber(lambda: client(port, done))
        for _ in range(conc):
            done.recv()                  # join every client
        for l in listeners:
            l.close()                    # stops the C accept loops (serve doc)

    runloom.run(hubs, main_fn=root)


def _wl_iouring_churn(ctx):
    # R7 item 1 aging: connect / echo / close churn on runloom_c.TCPConn under
    # M:N with the io_uring backend, so the soak ages the per-hub cancel-by-fd /
    # dup-fd close path just landed (docs/dev/DESIGN_mn_iouring_cancel_fd.md).
    # Every 3rd unit closes a connection WHILE a recv is parked on the hub ring
    # -- the exact cancel-by-fd path -- so the dup-fd lifecycle is exercised, not
    # only happy-path echo.  Watches fds / iouring_inflight / netpoll_fd_armed for
    # a leak over hours.  REQUIRES --env RUNLOOM_TCPCONN_IOURING=1 to hit the
    # io_uring path (else it ages the epoll TCPConn path, still useful).  Each
    # worker owns ONE listener reused across units (no ephemeral-port churn) and
    # every unit fully joins its server+client fibers via a Chan (race-free under
    # M:N) -- no stranded fiber, per the module contract.
    hubs = int(os.environ.get("RUNLOOM_SOAK_HUBS", "2"))
    conc = int(os.environ.get("RUNLOOM_SOAK_CONC", "4"))

    def bound_port(l):
        fd = l.fileno(); sk = socket.socket(fileno=socket.dup(fd))
        p = sk.getsockname()[1]; sk.close(); return p

    def unit(L, port, cancel_variant):
        join = runloom_c.Chan(2)   # server + client each send one token on exit
        def server():
            try:
                conn = L.accept()
                try:
                    d = conn.recv(64)        # EOF (b'') when the client closes
                    if d:
                        conn.send(d)
                finally:
                    conn.close()
            except OSError:
                pass
            finally:
                join.send(1)
        def client():
            try:
                c = runloom_c.TCPConn.connect("127.0.0.1", port)
                if cancel_variant:
                    # Park a recv on the hub ring, then close it -> the cancel-by-
                    # fd broadcast wakes it -ECANCELED (server sees EOF + closes).
                    # Join rd via a Chan (a cooperative PARK); never a sched_yield
                    # spin -- a busy-spin waiting on a sibling fiber starves the hub
                    # it runs on under M:N, a livelock (not a leak) that wedges the
                    # soak.  A short bounded yield first lets rd actually park on the
                    # recv, so close() exercises the cancel-a-parked-recv path.
                    rddone = runloom_c.Chan(1)
                    def rd(cc=c, ch=rddone):
                        try:
                            cc.recv(64, socket.MSG_WAITALL)
                        except OSError:
                            pass
                        finally:
                            ch.send(1)
                    runloom_c.mn_fiber(rd)
                    for _ in range(10):
                        runloom_c.sched_yield()
                    c.close()                # cancels the parked recv
                    rddone.recv()            # cooperative join -- no spin/starve
                else:
                    c.send(b"ping")
                    c.recv(64)
                    c.close()
            except OSError:
                pass
            finally:
                join.send(1)
        runloom_c.mn_fiber(server)
        runloom_c.mn_fiber(client)
        join.recv(); join.recv()             # join BOTH -- no abandoned fiber

    def worker(w, wdone):
        L = runloom_c.TCPConn.listen("127.0.0.1", 0)
        port = bound_port(L)
        i = 0
        try:
            while not ctx.expired():
                unit(L, port, (i + w) % 3 == 0)
                i += 1
                ctx.bump()
                if not ctx.compress:
                    runloom_c.sched_yield()
        finally:
            L.close()
            wdone.send(1)

    def body():
        wdone = runloom_c.Chan(conc)
        for w in range(conc):
            runloom_c.mn_fiber(lambda w=w: worker(w, wdone))
        for _ in range(conc):
            wdone.recv()                     # join every worker before teardown

    runloom_c.mn_init(hubs)
    runloom_c.mn_fiber(body)
    runloom_c.mn_run()
    runloom_c.mn_fini()


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


_ERR_LEAK_SINK = []


def _wl_leak_on_error(ctx):
    # R3 NEGATIVE CONTROL: the happy path fully drains, but the ERROR path
    # leaks -- exactly the bug class chaos exists to age.  A fraction of units
    # hit a simulated error and, on that branch, "forget" to close a socketpair
    # (append one end to a module global forever).  The fd count + RSS +
    # py_sock_timeouts climb, so the slope oracle MUST fail this under chaos.
    # If it passes, error-path aging is not catching leaks.
    n = [0]
    def unit():
        n[0] += 1
        a, b = socket.socketpair()
        if n[0] % 4 == 0:
            # ERROR branch: leak `a` (never closed), close only b.  This is the
            # "cleanup dropped a resource on the failure path" shape.
            _ERR_LEAK_SINK.append(a)
            b.close()
        else:
            a.close()
            b.close()
    _spin(ctx, unit, concurrency=2)


WORKLOADS = {
    "spawn_churn": _wl_spawn_churn,
    "chan_select": _wl_chan_select,
    "timer": _wl_timer,
    "tcp_churn": _wl_tcp_churn,
    "keepalive": _wl_keepalive,
    "offload": _wl_offload,
    "cserve_echo": _wl_cserve_echo,
    "iouring_churn": _wl_iouring_churn,
    "mixed": _wl_mixed,
    "leak_control": _wl_leak_control,
    "leak_on_error": _wl_leak_on_error,
}
