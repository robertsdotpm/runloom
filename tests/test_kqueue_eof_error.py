"""kqueue EV_EOF / EV_ERROR fold-into-both-directions tests (audit finding B1).

THE CODE UNDER TEST
-------------------
src/runloom_c/netpoll_pump.c.inc:202-215 -- the kqueue drain loop's event
classify + fold.  kqueue arms ONLY the requested direction one-shot
(EV_ADD|EV_ONESHOT, netpoll_register.c.inc:139-142), so a teardown event
(EV_EOF on a peer close / SHUT_WR, EV_ERROR on a refused connect / RST) is
delivered on whatever filter the kernel chose -- which may NOT be the filter a
surviving waiter armed on.  Before the fix a WRITE-only parker whose peer reset
the connection got an EV_EOF reported on (ident, EVFILT_READ) that matched no
WRITE parker -> a LOST WAKEUP (the strongest teardown-tail-hang candidate).

The fix (netpoll_pump.c.inc:212-213):
    if (evs[i].flags & (EV_EOF | EV_ERROR))
        mask |= RUNLOOM_NETPOLL_READ | RUNLOOM_NETPOLL_WRITE;
folds the event into BOTH directions, so dispatch_event wakes EVERY parker on
the dead fd regardless of the direction it armed -- mirroring epoll/Go/libuv/mio.
Combined with wake_all=1 (netpoll_pump.c.inc:215 / dispatch_event finding B2)
every same-fd waiter on a dead fd becomes runnable and its next syscall observes
the error.

These tests assert that BEHAVIOUR through real sockets (socketpair / loopback
connect) + runloom_c.wait_fd -- never a backend internal -- and compare to a
plain blocking socket where it clarifies intent.  Single-thread (runloom_c.go/
run) AND a couple under M:N (runloom.run(4, ...), per-hub kqueue delivers EOF).

kqueue only; run from the repo root.
"""
import errno as _errno
import socket
import sys

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

sys.path.insert(0, "src")

import runloom_c                       # noqa: E402
import runloom                         # noqa: E402

READ = 1
WRITE = 2
RW = READ | WRITE
# wait_fd returns this (0x40000000) when a parked fiber is cancelled; an EOF/
# error fold must NEVER produce it -- it must produce a real readiness mask.
CANCELLED = 0x40000000


# --------------------------------------------------------------------------
# single-thread driver (the proven conformance-suite pattern)
# --------------------------------------------------------------------------
def _drive(*fibers):
    """Spawn each callable as a fiber, run the single-thread scheduler, and
    re-raise the first exception any fiber hit so asserts surface."""
    box = []

    def wrap(fn):
        def runner():
            try:
                fn()
            except BaseException as e:      # noqa: BLE001
                box.append(e)
        return runner

    for g in fibers:
        runloom_c.go(wrap(g))
    runloom_c.run()
    if box:
        raise box[0]


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _reset_registration():
    """These tests raw-close their sockets, bypassing the unregister hook the
    real close paths run.  Clear the per-fd registration cache around each test
    so a reused fd NUMBER re-registers cleanly (netpoll_register's fd-bit gate,
    netpoll_register.c.inc:123)."""
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:                   # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _registration_reset():
    _reset_registration()
    yield
    _reset_registration()


def test_backend_is_kqueue():
    # Guard: every test below asserts the *kqueue* fold path.  If some other
    # backend were selected the assertions would be vacuous.
    assert runloom_c.netpoll_backend() == "kqueue"


# --------------------------------------------------------------------------
# helpers: fill a socket's send buffer so WRITE is NOT immediately ready
# --------------------------------------------------------------------------
def _make_write_blocked_pair():
    """A connected socketpair where `a` is NOT writable: shrink both buffers and
    stuff `a`'s send path until EAGAIN.  Returns (a, b, bytes_written)."""
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    try:
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    except OSError:
        pass
    chunk = b"\x00" * 4096
    total = 0
    # Bounded: a few MB at most before the kernel refuses (EAGAIN/EWOULDBLOCK).
    for _ in range(4096):
        try:
            n = a.send(chunk)
            total += n
        except BlockingIOError:
            break
        except OSError as e:
            if e.errno in (_errno.EAGAIN, _errno.EWOULDBLOCK):
                break
            raise
    return a, b, total


# ==========================================================================
# B1 case 1: peer FULLY closes -> EV_EOF on the READ filter -> a READ-parked
# waiter wakes and recv() returns b"" (EOF).
# Targets netpoll_pump.c.inc:202-215 (classify READ + EOF fold) reaching a
# READ parker (the direction it armed); compares to a blocking socket's recv.
# ==========================================================================
@pytest.mark.parametrize("payload", [b"", b"tail-bytes-then-eof"],
                         ids=["pure-eof", "data-then-eof"])
def test_read_parker_wakes_on_peer_close_eof(payload):
    a, b = _pair()
    out = []

    def reader():
        out.append(runloom_c.wait_fd(a.fileno(), READ, 2000))

    def closer():
        runloom_c.sched_yield()             # let the reader park first
        if payload:
            b.sendall(payload)              # data is still delivered before EOF
        b.close()                           # full close -> EV_EOF on READ

    _drive(reader, closer)
    assert out == [READ], "EOF did not wake the READ parker"
    # The buffered tail (if any) is readable, then EOF (b"").
    if payload:
        assert a.recv(len(payload) + 8) == payload
    assert a.recv(16) == b""                # EOF, like a blocking socket
    a.close()


def test_read_parker_eof_matches_blocking_socket():
    # Control: a plain BLOCKING socket sees b"" after the peer closes.  The
    # kqueue path must produce the identical observable EOF.
    ba, bb = socket.socketpair()
    bb.close()
    assert ba.recv(16) == b""               # blocking baseline
    ba.close()

    a, b = _pair()
    out = []

    def reader():
        out.append(runloom_c.wait_fd(a.fileno(), READ, 2000))

    def closer():
        runloom_c.sched_yield()
        b.close()

    _drive(reader, closer)
    assert out == [READ]
    assert a.recv(16) == b""
    a.close()


# ==========================================================================
# B1 case 2 (THE BUG): a WRITE-ONLY parker on a socket whose peer closes the
# connection.  The send buffer is filled first so WRITE is NOT immediately
# ready -> the fiber actually parks on EVFILT_WRITE only.  The peer close
# reports EV_EOF on a filter the waiter is NOT armed on; the fold into BOTH
# directions (netpoll_pump.c.inc:212-213) is what wakes it.  Without the fold
# this parker hangs to its deadline (the lost-wakeup B1 fixes).
# ==========================================================================
@pytest.mark.parametrize("hint", [WRITE, RW],
                         ids=["armed-write-only", "armed-read|write"])
def test_write_parker_wakes_on_peer_close_eof_fold(hint):
    a, b, _total = _make_write_blocked_pair()
    out = []

    def writer():
        # WRITE not ready (buffer full) -> real park on EVFILT_WRITE.  A long
        # deadline so a *timeout* can't masquerade as the fold-wake: if the
        # fold is broken this hangs ~3 s then returns 0, which the assert below
        # rejects (and the value would be 0, not a WRITE-bearing mask).
        out.append(runloom_c.wait_fd(a.fileno(), hint, 3000))

    def closer():
        runloom_c.sched_yield()             # let the writer commit its park
        b.close()                           # EV_EOF -> fold into READ|WRITE

    _drive(writer, closer)
    assert len(out) == 1
    r = out[0]
    assert r != 0, "WRITE parker hung to its deadline -- EOF fold lost (B1)"
    assert r != CANCELLED, "spurious cancel, not an EOF fold"
    # The fold makes the requested direction(s) ready; WRITE was requested in
    # both parametrize cases, so the result must carry WRITE.
    assert r & WRITE, "fold did not wake the WRITE-armed direction (mask=%#x)" % r
    a.close()


# ==========================================================================
# B1 case 3: half-close.  Peer shutdown(SHUT_WR) -> our READ side sees EOF
# while the WRITE side stays open.  A READ parker wakes (EV_EOF on READ);
# recv() returns b"" and the socket is still writable afterwards.
# Targets the EOF classify+fold reaching a READ waiter on a half-dead fd.
# ==========================================================================
def test_half_close_shut_wr_wakes_read_parker():
    a, b = _pair()
    out = []

    def reader():
        out.append(runloom_c.wait_fd(a.fileno(), READ, 2000))

    def half_closer():
        runloom_c.sched_yield()
        b.shutdown(socket.SHUT_WR)          # our read side -> EOF; write open

    _drive(reader, half_closer)
    assert out == [READ], "SHUT_WR did not wake the READ parker"
    assert a.recv(16) == b""                # read side EOF
    # Write side is still open: a fresh WRITE wait_fd must report WRITE-ready,
    # not block (the half-close only killed the read direction).
    out2 = []
    _drive(lambda: out2.append(runloom_c.wait_fd(a.fileno(), WRITE, 1000)))
    assert out2 == [WRITE], "write side wrongly reported dead after SHUT_WR"
    a.close(); b.close()


# ==========================================================================
# B1 case 4: connect() to a REFUSED port -> EV_EOF|EV_ERROR on the connecting
# socket's WRITE filter -> the connect waiter wakes and the error surfaces
# (SO_ERROR == ECONNREFUSED).  The waiter must WAKE, not hang.
# Targets the EV_ERROR classify+fold on the connect-WRITE path.
# ==========================================================================
def _refused_addr():
    """A loopback address with NOTHING listening: bind+listen to claim a port,
    capture it, then close the listener so a connect there is refused
    immediately (RST)."""
    lst = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lst.bind(("127.0.0.1", 0))
    lst.listen(1)
    addr = lst.getsockname()
    lst.close()                             # port now refuses
    return addr


def test_connect_refused_wakes_write_waiter_with_error():
    addr = _refused_addr()
    out = []
    res = []

    def connector():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setblocking(False)
        try:
            ec = s.connect_ex(addr)         # EINPROGRESS (async connect)
            assert ec in (0, _errno.EINPROGRESS, _errno.EWOULDBLOCK), ec
            r = runloom_c.wait_fd(s.fileno(), WRITE, 3000)
            out.append(r)
            so_err = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            res.append(so_err)
        finally:
            s.close()

    _drive(connector)
    assert len(out) == 1
    r = out[0]
    assert r != 0, "connect waiter hung to its deadline -- error fold lost"
    assert r != CANCELLED
    assert r & WRITE, "connect failure did not wake the WRITE filter (mask=%#x)" % r
    # The fold delivered the error; SO_ERROR reports the refusal.
    assert res == [_errno.ECONNREFUSED], (
        "SO_ERROR=%r, expected ECONNREFUSED after refused connect" % res)


# ==========================================================================
# B1 case 5: an EV_ERROR event that carries data(errno) -- a peer RST on an
# established connection.  We RST by setting SO_LINGER{on,0} on the peer and
# closing it (a hard reset rather than an orderly FIN).  A WRITE-blocked
# parker must WAKE (the fold), not hang -- the next syscall then observes the
# error (EPIPE/ECONNRESET) exactly as a blocking socket would.
# ==========================================================================
def _rst_close(sock):
    """Force an RST close: SO_LINGER with a zero timeout discards queued data
    and sends a reset instead of a FIN."""
    import struct
    linger = struct.pack("ii", 1, 0)        # l_onoff=1, l_linger=0
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
    except OSError:
        pass
    sock.close()


def test_write_parker_wakes_on_peer_rst_error_fold():
    a, b, _total = _make_write_blocked_pair()
    out = []

    def writer():
        out.append(runloom_c.wait_fd(a.fileno(), WRITE, 3000))

    def resetter():
        runloom_c.sched_yield()
        _rst_close(b)                       # EV_EOF|EV_ERROR -> fold

    _drive(writer, resetter)
    assert len(out) == 1
    r = out[0]
    assert r != 0, "WRITE parker hung after peer RST -- error fold lost (B1)"
    assert r != CANCELLED
    assert r & WRITE, "RST did not wake the WRITE-armed direction (mask=%#x)" % r
    # After the wake the socket is dead: a send/recv now errors, never blocks
    # silently -- the whole point of waking it.
    try:
        a.send(b"x" * 65536)
        # If the RST hasn't been fully processed a single send may still queue;
        # a second observes it.  Either way it must not hang.
        a.recv(16)
    except OSError:
        pass
    a.close()


# ==========================================================================
# B1 reinforcement: TWO waiters in DIFFERENT directions on the SAME dead fd.
# A READ parker AND a WRITE parker on one socket; the peer closes.  The single
# (ident,filter) one-shot knote that the kernel reports must, via the fold +
# wake_all=1 (dispatch_event, B2), wake BOTH -- the cross-direction sibling is
# exactly the one the old first-match/no-fold path stranded.
# ==========================================================================
def test_both_direction_waiters_wake_on_close():
    a, b, _total = _make_write_blocked_pair()  # a: WRITE-blocked, READ-empty
    woke = {}

    def reader():
        woke["read"] = runloom_c.wait_fd(a.fileno(), READ, 3000)

    def writer():
        woke["write"] = runloom_c.wait_fd(a.fileno(), WRITE, 3000)

    def closer():
        runloom_c.sched_yield()
        runloom_c.sched_yield()             # let BOTH park
        b.close()

    _drive(reader, writer, closer)
    assert "read" in woke and "write" in woke
    assert woke["read"] != 0, "READ waiter stranded on close"
    assert woke["write"] != 0, "WRITE waiter stranded on close (cross-dir B1)"
    assert woke["read"] & READ
    assert woke["write"] & WRITE
    a.close()


# ==========================================================================
# M:N coverage -- per-hub kqueue delivers EOF/error.  Run the two highest-value
# scenarios (READ-EOF and the WRITE-only cross-direction fold) under
# runloom.run(n, main) so the fold is exercised on the live hub kqueues, not
# just the single-thread default pool.  Results land in single-writer dict
# slots (no shared counter RMW -- mandatory with the GIL off).
# Targets the SAME netpoll_pump.c.inc:202-215 fold reached from a hub pump.
# ==========================================================================
def _run_mn_eof_read(hubs):
    box = {}

    def reader(slot):
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        runloom.go(_peer_closer, b)         # spawn the closer fiber
        r = runloom_c.wait_fd(a.fileno(), READ, 4000)
        box[slot] = (r, a.recv(16))
        a.close()

    def main():
        runloom.go(reader, "r")
        # No sleep: runloom.run() waits for ALL goroutines, so the reader's
        # full round-trip (park -> EOF fold -> recv) completes before run()
        # returns -- deterministic regardless of load.
    runloom.run(hubs, main)
    return box


def _peer_closer(b):
    # Deterministic ordering: the close must land AFTER the sibling fiber has
    # COMMITTED its wait_fd park, so the EV_EOF/EV_ERROR is delivered through the
    # kqueue drain loop's fold while a real park exists (the path under test) --
    # not consumed at registration by a peer that closed first.  Under CPU load a
    # fixed sleep can let the close win that race, which silently tests the wrong
    # path and, for a write-blocked parker, can invert the wake (a close-before-
    # register lost wakeup -> the parker hangs to its deadline).  Poll the GLOBAL
    # netpoll_parked stat (module_run.c.inc:214, summed across hubs) until the one
    # socket parker in this run() is committed; the cap only bounds a true hang.
    i = 0
    while runloom_c.stats()["netpoll_parked"] < 1 and i < 1000000:
        runloom_c.sched_yield()
        i += 1
    b.close()


@pytest.mark.parametrize("hubs", [2, 4], ids=["hubs2", "hubs4"])
def test_mn_read_eof_fold(hubs):
    box = _run_mn_eof_read(hubs)
    assert box.get("r") is not None, "M:N reader never woke (EOF lost on a hub)"
    mask, data = box["r"]
    assert mask == READ, "M:N EOF did not wake READ parker (mask=%#x)" % mask
    assert data == b"", "M:N reader did not observe EOF"


def _run_mn_write_fold(hubs):
    box = {}

    def writer(slot):
        a, b, _ = _make_write_blocked_pair()
        runloom.go(_peer_closer, b)
        r = runloom_c.wait_fd(a.fileno(), WRITE, 4000)
        box[slot] = r
        a.close()

    def main():
        runloom.go(writer, "w")
        # No sleep: run() waits for the writer goroutine's full round-trip.
    runloom.run(hubs, main)
    return box


@pytest.mark.parametrize("hubs", [2, 4], ids=["hubs2", "hubs4"])
def test_mn_write_only_close_fold(hubs):
    box = _run_mn_write_fold(hubs)
    r = box.get("w")
    assert r is not None, "M:N WRITE-only parker never woke (cross-dir fold lost)"
    assert r != 0, "M:N WRITE parker hung to its deadline -- B1 fold lost on a hub"
    assert r != CANCELLED
    assert r & WRITE, "M:N fold did not wake the WRITE direction (mask=%#x)" % r


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
