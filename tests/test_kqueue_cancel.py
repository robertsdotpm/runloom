"""kqueue (macOS/BSD) netpoll CANCEL-path unit tests.

Focus: the cancel machinery that wakes a fiber parked in wait_fd
(recv/accept/connect) with the POSITIVE WAIT_FD_CANCELLED sentinel (0x40000000),
and the cooperative fast-path mapping of that sentinel to OSError(ECANCELED) so a
parked socket op unwinds instead of re-parking forever.  Code under test:

  - runloom_netpoll_cancel_fd(fd)               (netpoll_wake_iouring.c.inc:191)
        wake EVERY parker on `fd` with RUNLOOM_NETPOLL_CANCELLED.  Bound to the
        monkey socket close hook (monkey/sockets.py:_patched_close) and the
        TCPConn close/dealloc path (runloom_tcp_conn_io.c.inc:71,96).
  - runloom_netpoll_cancel_all_parked() -> int  (netpoll_wake_iouring.c.inc:256,
        finding B3) -- teardown backstop; cancel every by_fd parker across all
        pools, return the count.  C binding: runloom_c.cancel_all_parked().
  - runloom_netpoll_cancel_g(g)                 (netpoll_wake_iouring.c.inc:90)
        cancel ONE parked g.  C binding: G.cancel_wait_fd() (module_g.c.inc:84).
  - the WAIT_FD_CANCELLED sentinel itself        (netpoll_wait_fd.c.inc:107,227)
        stored into ready_out by every cancel path; wait_fd returns it verbatim.
  - runloom_netpoll_wait_fd_coop (runloom_tcp.c:164, finding B3) -- the C socket
        FAST PATH (TCPConn.recv at runloom_tcp_conn_io.c.inc:214) maps the
        positive sentinel to errno=ECANCELED/-1 so recv raises OSError instead of
        re-parking on the still-OPEN socket.
  - monkey _wait_fd_coop (monkey/sockets.py:37) -- same OSError(ECANCELED) map
        for the patched socket.recv loop.
  - the dispatch_event claim states: a committed (PARKED) parker is re-queued by
    the cancel; an ARMED parker (mid-commit) aborts its own park and returns the
    sentinel itself (netpoll_wait_fd.c.inc:301-320).  PARKED is forced
    deterministically on the single-thread scheduler (the parker yields before
    the canceller runs); the ARMED race is exercised under M:N.

Run from the repo root (sys.path.insert "src" below).  kqueue-only: the cancel
paths above are identical on epoll/iocp, but this module asserts them on the
kqueue backend specifically (the wake_all=1 per-fd fan-out + the one-shot re-arm
model are kqueue-specific, finding B2), so it skips elsewhere.
"""
import errno
import socket
import sys
import time

import pytest

sys.path.insert(0, "src")

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

import runloom_c          # noqa: E402
import runloom            # noqa: E402  (high-level go/sleep/run for the M:N driver)

READ = 1
WRITE = 2

# The positive cancel sentinel wait_fd returns; mirror the module constant but
# pin the literal so a drift in the C #define is caught here too.
CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", 0x40000000)


# ------------------------------------------------------------------ helpers ----

def _pair():
    """A non-blocking, never-readable/never-writable-blocked socketpair.

    Neither end has pending data, so a READ park never fires on its own; only a
    cancel (or a deadline, which we don't set) can wake it -- exactly the
    "stranded forever" condition the cancel paths exist to break."""
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _drive(*fibers):
    """Single-thread driver: spawn each callable as a fiber in the given ORDER,
    run the single-thread scheduler to completion, re-raise the first exception
    so asserts surface.  Spawn order is the run order on the single-thread
    scheduler (FIFO ready ring), which is what lets a parker fiber commit to
    PARKED and yield BEFORE a later canceller fiber runs -- deterministic."""
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:   # noqa: BLE001
                box.append(e)
        return runner

    for g in fibers:
        runloom_c.go(wrap(g))
    runloom_c.run()
    if box:
        raise box[0]


def _reset_netpoll_registration():
    """These tests raw-close their socketpairs, bypassing the unregister hook
    that real close paths run.  Clear the per-fd arm/registration cache around
    each test so a reused fd number re-registers cleanly (the convention the
    conformance suite uses)."""
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:                # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _netpoll_reset():
    _reset_netpoll_registration()
    # Drain any parker stranded by a prior test before this one starts.
    try:
        runloom_c.cancel_all_parked()
    except Exception:                    # noqa: BLE001
        pass
    yield
    try:
        runloom_c.cancel_all_parked()
    except Exception:                    # noqa: BLE001
        pass
    _reset_netpoll_registration()


def test_backend_is_kqueue():
    """Guard: this whole module asserts kqueue-backend cancel behaviour."""
    assert runloom_c.netpoll_backend() == "kqueue"


# ============================================================================
# 1. netpoll_cancel_fd -> wait_fd returns the WAIT_FD_CANCELLED sentinel
#    Branch: netpoll_wake_iouring.c.inc:191 (cancel_fd) +
#            netpoll_wait_fd.c.inc:107/227 (sentinel stored) ; PARKED claim
#            (single-thread: parker yields before canceller runs).
# ============================================================================

@pytest.mark.parametrize("direction", [READ, WRITE | READ],
                         ids=["read", "read+write"])
def test_cancel_fd_returns_sentinel_single_thread(direction):
    """A fiber parked in wait_fd(fd, ...) that ANOTHER fiber cancel_fd's wakes
    with the exact WAIT_FD_CANCELLED sentinel -- not 0 (timeout), not a mask."""
    a, b = _pair()
    got = []

    def parker():
        # No timeout: only the cancel can wake this (READ never fires; b never
        # writes).  Recorded raw so we assert the sentinel value precisely.
        got.append(runloom_c.wait_fd(a.fileno(), direction))

    def canceller():
        # Runs after parker has committed PARKED + yielded (spawn order).
        runloom_c.netpoll_cancel_fd(a.fileno())

    try:
        _drive(parker, canceller)
    finally:
        a.close()
        b.close()
    assert got == [CANCELLED], "expected the cancel sentinel, got %r" % got


@pytest.mark.parametrize("n_waiters", [2, 3, 5],
                         ids=["w2", "w3", "w5"])
def test_cancel_fd_wakes_every_parker_on_fd(n_waiters):
    """wake_all=1 (kqueue) fans the single per-fd one-shot knote out to EVERY
    same-direction parker on the fd -- finding B2.  All N READ waiters on one fd
    must each get the sentinel from ONE cancel_fd (a first-match-only wake would
    strand the siblings: no re-arm path for a one-shot)."""
    a, b = _pair()
    got = []

    def parker():
        got.append(runloom_c.wait_fd(a.fileno(), READ))

    def canceller():
        runloom_c.netpoll_cancel_fd(a.fileno())

    # Spawn all parkers first (they each commit PARKED + yield), then the
    # single canceller.
    fibers = [parker] * n_waiters + [canceller]
    try:
        _drive(*fibers)
    finally:
        a.close()
        b.close()
    assert got == [CANCELLED] * n_waiters, \
        "expected %d sentinels, got %r" % (n_waiters, got)


# ============================================================================
# 2. cancel_all_parked() -> count cancelled, every waiter sees the sentinel,
#    idempotent second call returns 0.
#    Branch: netpoll_wake_iouring.c.inc:256 (B3) + sentinel store.
# ============================================================================

@pytest.mark.parametrize("k", [1, 2, 4, 8], ids=["k1", "k2", "k4", "k8"])
def test_cancel_all_parked_count_and_sentinel_single_thread(k):
    """K fibers parked on K distinct never-ready fds: cancel_all_parked() returns
    exactly K, every wait_fd returns the sentinel, and a second call returns 0
    (nothing left parked -- idempotent)."""
    pairs = [_pair() for _ in range(k)]
    got = []
    counts = []

    def make_parker(sock):
        def parker():
            got.append(runloom_c.wait_fd(sock.fileno(), READ))
        return parker

    def canceller():
        counts.append(runloom_c.cancel_all_parked())     # first: cancels K
        counts.append(runloom_c.cancel_all_parked())     # second: nothing parked

    fibers = [make_parker(a) for (a, _b) in pairs] + [canceller]
    try:
        _drive(*fibers)
    finally:
        for a, b in pairs:
            a.close()
            b.close()
    assert counts[0] == k, "cancel_all_parked returned %r, want %d" % (counts[0], k)
    assert counts[1] == 0, "second cancel_all_parked returned %r, want 0" % counts[1]
    assert got == [CANCELLED] * k, "expected %d sentinels, got %r" % (k, got)


def test_cancel_all_parked_idempotent_when_nothing_parked():
    """cancel_all_parked() on a quiescent runtime (the clean-drain common case)
    returns 0 and does not crash -- B3's "cheap when nothing is parked"."""
    out = []

    def worker():
        out.append(runloom_c.cancel_all_parked())

    _drive(worker)
    assert out == [0]


# ============================================================================
# 3. G.cancel_wait_fd() -> cancel ONE specific parked fiber.
#    Branch: module_g.c.inc:84 -> runloom_netpoll_cancel_g
#            (netpoll_wake_iouring.c.inc:90).
# ============================================================================

def test_cancel_wait_fd_one_fiber_returns_true_and_sentinel():
    """A fiber parked in wait_fd, cancelled by its own G handle from a sibling:
    cancel_wait_fd() returns True (it WAS netpoll-parked) and the parked wait_fd
    returns the sentinel.  A second fiber on the SAME fd stays parked (cancel_g
    is per-g, not per-fd) -- so we cancel it too to let the run drain."""
    a, b = _pair()
    box = {}
    got = []

    def parker_one():
        box["g"] = runloom_c.current_g()      # publish our handle for the canceller
        got.append(("one", runloom_c.wait_fd(a.fileno(), READ)))

    def parker_two():
        got.append(("two", runloom_c.wait_fd(a.fileno(), READ)))

    def canceller():
        woke = box["g"].cancel_wait_fd()       # cancels ONLY parker_one
        box["woke"] = woke
        # parker_two is still parked on the same fd -> drain it via cancel_fd.
        runloom_c.netpoll_cancel_fd(a.fileno())

    try:
        _drive(parker_one, parker_two, canceller)
    finally:
        a.close()
        b.close()
    assert box["woke"] is True, "cancel_wait_fd should report it woke a parker"
    assert ("one", CANCELLED) in got
    assert ("two", CANCELLED) in got


def test_cancel_wait_fd_false_when_not_parked():
    """cancel_wait_fd() on a fiber that is NOT netpoll-parked (here: the running
    fiber's own handle, never having parked) returns False."""
    out = []

    def worker():
        g = runloom_c.current_g()
        out.append(g.cancel_wait_fd())          # running, not parked -> False

    _drive(worker)
    assert out == [False]


# ============================================================================
# 4. monkey socket close hook: a cross-fiber close() of a socket a sibling is
#    parked in recv() on wakes the sibling (cancel_fd) and it unwinds with
#    OSError(EBADF) on the now-closed fd.
#    Branch: monkey/sockets.py:_patched_close -> netpoll_cancel_fd ;
#            _patched_recv retry on fileno==-1 -> EBADF.
# ============================================================================

def test_monkey_recv_cross_fiber_close_raises():
    """Under monkey.patch(), a fiber blocked in socket.recv() whose socket is
    close()d by ANOTHER fiber wakes and raises OSError (EBADF / EBADF-family),
    instead of hanging forever (BUG #5).  Driven on the single-thread scheduler
    via runloom.run(1)."""
    runloom.monkey.patch()
    result = {}
    a, b = socket.socketpair()       # patched close hook needs the patched type

    def reader():
        try:
            a.recv(64)               # parks (nothing sent); sibling closes a
            result["outcome"] = ("ok", None)
        except OSError as e:
            result["outcome"] = ("oserror", e.errno)
        except BaseException as e:   # noqa: BLE001
            result["outcome"] = ("err", type(e).__name__)

    def closer():
        runloom.sleep(0.02)          # let reader park first
        a.close()                    # cross-fiber close -> cancel_fd + EBADF

    def main():
        runloom.go(reader)
        runloom.go(closer)
        runloom.sleep(0.2)

    try:
        runloom.run(1, main)
    finally:
        runloom.monkey.unpatch()
        try:
            b.close()
        except OSError:
            pass
    assert result.get("outcome") is not None, "reader never returned (hang)"
    kind, val = result["outcome"]
    assert kind == "oserror", "recv should raise OSError on cross-fiber close, got %r" % (result["outcome"],)
    assert val in (errno.EBADF, errno.ECANCELED, errno.ECONNRESET), \
        "unexpected errno %r from a cross-fiber close" % val


# ============================================================================
# 5. monkey _wait_fd_coop fast path: cancel an OPEN socket's parked recv via
#    cancel_all_parked from a sibling -> OSError(ECANCELED), NOT a re-park hang.
#    Branch: monkey/sockets.py:37 _wait_fd_coop (sentinel -> ECANCELED).
# ============================================================================

def test_monkey_recv_cancel_all_open_socket_raises_ecanceled():
    """The B3 teardown backstop: a fiber parked in socket.recv() on a still-OPEN
    socket, cancelled by cancel_all_parked() from a sibling, must raise
    OSError(ECANCELED) (the _wait_fd_coop map of the positive sentinel) rather
    than ignore the wake and re-park on the open fd."""
    runloom.monkey.patch()
    result = {}
    a, b = socket.socketpair()

    def reader():
        try:
            a.recv(64)               # parks on the OPEN socket
            result["outcome"] = ("ok", None)
        except OSError as e:
            result["outcome"] = ("oserror", e.errno)
        except BaseException as e:   # noqa: BLE001
            result["outcome"] = ("err", type(e).__name__)

    def canceller():
        runloom.sleep(0.02)          # let reader park
        runloom_c.cancel_all_parked()

    def main():
        runloom.go(reader)
        runloom.go(canceller)
        runloom.sleep(0.2)

    try:
        runloom.run(1, main)
    finally:
        runloom.monkey.unpatch()
        a.close()
        b.close()
    assert result.get("outcome") is not None, "reader never returned (re-park hang)"
    kind, val = result["outcome"]
    assert kind == "oserror" and val == errno.ECANCELED, \
        "open-socket cancel should raise OSError(ECANCELED), got %r" % (result["outcome"],)


# ============================================================================
# 6. TCPConn.recv coop fast path (runloom_netpoll_wait_fd_coop): cancel an OPEN
#    socket's parked recv -> OSError(ECANCELED), not a hang.
#    Branch: runloom_tcp.c:164 (wait_fd_coop) via
#            runloom_tcp_conn_io.c.inc:214 (TCPConn.recv).
# ============================================================================

@pytest.mark.parametrize("how", ["cancel_all", "cancel_fd"],
                         ids=["cancel_all", "cancel_fd"])
def test_tcpconn_recv_cancel_open_socket_raises_ecanceled(how):
    """runloom_c.TCPConn(fd).recv() parks via the C coop fast path.  Cancelling
    it on a STILL-OPEN socket (so the retry-recv would EAGAIN -> re-park forever
    without the coop map) via cancel_all_parked() OR netpoll_cancel_fd() must
    raise OSError(ECANCELED) -- the bare positive sentinel mapped to -1/errno."""
    a, b = _pair()
    conn = runloom_c.TCPConn(a.fileno())       # wraps + steals a's fd
    result = {}

    def reader():
        try:
            conn.recv(64)                       # C coop recv: parks (no data)
            result["outcome"] = ("ok", None)
        except OSError as e:
            result["outcome"] = ("oserror", e.errno)
        except BaseException as e:              # noqa: BLE001
            result["outcome"] = ("err", type(e).__name__)

    def canceller():
        if how == "cancel_all":
            runloom_c.cancel_all_parked()
        else:
            runloom_c.netpoll_cancel_fd(a.fileno())

    try:
        _drive(reader, canceller)
    finally:
        # conn stole a's fd; closing conn closes it.  b is independent.
        try:
            conn.close()
        except Exception:                       # noqa: BLE001
            pass
        b.close()
    assert result.get("outcome") is not None, "TCPConn.recv never returned (hang)"
    kind, val = result["outcome"]
    assert kind == "oserror" and val == errno.ECANCELED, \
        "TCPConn.recv cancel should raise OSError(ECANCELED), got %r" % (result["outcome"],)


# ============================================================================
# 7. M:N: cancel_all_parked() across hubs.  Park K fibers (on K never-ready
#    fds) spread over N hubs, wait until they are actually PARKED (poll the
#    global netpoll_parked stat), cancel_all_parked() returns K, every fiber
#    sees the sentinel, a second call returns 0.
#    Branch: netpoll_wake_iouring.c.inc:256 walking EVERY pool (per-hub kqueue
#            pools) -- the multi-pool fan-out the single-thread test can't reach;
#            exercises both PARKED and (racily) ARMED claim states.
# ============================================================================

def _wait_until_parked(target, timeout_s=4.0):
    """Poll the GLOBAL netpoll_parked count from the main thread until it
    reaches `target` (or time out).  stats()['netpoll_parked'] is the
    cross-thread global parked count, so this is a deterministic barrier for
    "all K fibers have committed their wait_fd park" under M:N."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if runloom_c.stats().get("netpoll_parked", 0) >= target:
            return True
        time.sleep(0.005)
    return runloom_c.stats().get("netpoll_parked", 0) >= target


@pytest.mark.parametrize("hubs,k", [(2, 4), (4, 8), (8, 16)],
                         ids=["h2k4", "h4k8", "h8k16"])
def test_cancel_all_parked_mn(hubs, k):
    """Under run(N), K fibers parked on K distinct never-ready fds across the
    hubs are all cancelled by ONE cancel_all_parked() (return == K), each sees
    the sentinel, and a follow-up call returns 0."""
    pairs = [_pair() for _ in range(k)]
    seen = bytearray(k)          # one writer slot per fiber: race-free under M:N
    counts = []

    def make_parker(idx, sock):
        def parker():
            r = runloom_c.wait_fd(sock.fileno(), READ)
            # 1 if cancelled-sentinel, 2 otherwise (single distinct writer slot).
            seen[idx] = 1 if r == CANCELLED else 2
        return parker

    def main():
        for i, (a, _b) in enumerate(pairs):
            runloom.go(make_parker(i, a))
        # Wait (cooperatively) until every fiber has parked, then cancel.  We
        # poll the global stat from inside the root fiber via short sleeps.
        deadline = time.monotonic() + 4.0
        while (runloom_c.stats().get("netpoll_parked", 0) < k
               and time.monotonic() < deadline):
            runloom.sleep(0.005)
        counts.append(runloom_c.cancel_all_parked())     # cancels all K
        runloom.sleep(0.05)                              # let woken fibers run
        counts.append(runloom_c.cancel_all_parked())     # nothing left -> 0

    try:
        runloom.run(hubs, main)
    finally:
        for a, b in pairs:
            a.close()
            b.close()
    assert counts and counts[0] == k, \
        "cancel_all_parked returned %r, want %d (hubs=%d)" % (counts[:1], k, hubs)
    assert counts[1] == 0, "second cancel_all_parked returned %r, want 0" % counts[1]
    assert all(v == 1 for v in seen), \
        "not every parked fiber saw the cancel sentinel: %r" % (list(seen),)


@pytest.mark.parametrize("hubs", [2, 4], ids=["h2", "h4"])
def test_cancel_fd_mn_wakes_all_on_one_fd(hubs):
    """Under run(N), several fibers parked on ONE shared never-ready fd are all
    woken with the sentinel by a single netpoll_cancel_fd() -- the wake_all=1
    per-fd fan-out (B2) holds across the per-hub kqueue pools."""
    n_waiters = 4
    a, b = _pair()
    seen = bytearray(n_waiters)

    def make_parker(idx):
        def parker():
            r = runloom_c.wait_fd(a.fileno(), READ)
            seen[idx] = 1 if r == CANCELLED else 2
        return parker

    def main():
        for i in range(n_waiters):
            runloom.go(make_parker(i))
        deadline = time.monotonic() + 4.0
        while (runloom_c.stats().get("netpoll_parked", 0) < n_waiters
               and time.monotonic() < deadline):
            runloom.sleep(0.005)
        runloom_c.netpoll_cancel_fd(a.fileno())
        runloom.sleep(0.05)

    try:
        runloom.run(hubs, main)
    finally:
        a.close()
        b.close()
    assert all(v == 1 for v in seen), \
        "cancel_fd under M:N did not wake every parker: %r" % (list(seen),)


# ============================================================================
# 8. M:N TCPConn.recv coop cancel: the coop OSError(ECANCELED) map holds under
#    real multi-hub parallelism too (the ARMED-vs-PARKED claim race is live).
#    Branch: runloom_tcp.c:164 wait_fd_coop under run(N).
# ============================================================================

@pytest.mark.parametrize("hubs", [2, 4], ids=["h2", "h4"])
def test_tcpconn_recv_cancel_mn_raises_ecanceled(hubs):
    """A C TCPConn.recv parked on an OPEN socket under run(N), cancelled by
    cancel_all_parked() from the root fiber, raises OSError(ECANCELED) -- the
    coop map survives the M:N claim race (PARKED re-queue OR ARMED self-abort,
    both routed to the sentinel)."""
    n = 3
    conns = []
    socks = []
    for _ in range(n):
        a, b = _pair()
        conns.append(runloom_c.TCPConn(a.fileno()))
        a.detach()                  # TCPConn owns the fd now; detach so a's GC
                                    # can't close it (-> recv ENOTSOCK/EBADF race)
        socks.append(b)             # keep peer open so the fd stays OPEN
    outcomes = [None] * n

    def make_reader(idx):
        def reader():
            try:
                conns[idx].recv(64)
                outcomes[idx] = ("ok", None)
            except OSError as e:
                outcomes[idx] = ("oserror", e.errno)
            except BaseException as e:   # noqa: BLE001
                outcomes[idx] = ("err", type(e).__name__)
        return reader

    def main():
        for i in range(n):
            runloom.go(make_reader(i))
        deadline = time.monotonic() + 4.0
        while (runloom_c.stats().get("netpoll_parked", 0) < n
               and time.monotonic() < deadline):
            runloom.sleep(0.005)
        runloom_c.cancel_all_parked()
        runloom.sleep(0.05)

    try:
        runloom.run(hubs, main)
    finally:
        for c in conns:
            try:
                c.close()
            except Exception:           # noqa: BLE001
                pass
        for s in socks:
            s.close()
    for i, o in enumerate(outcomes):
        assert o is not None, "reader %d never returned (hang) hubs=%d" % (i, hubs)
        assert o == ("oserror", errno.ECANCELED), \
            "reader %d got %r, want OSError(ECANCELED)" % (i, o)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
