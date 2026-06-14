"""kqueue WAKE-ALL one-shot dispatch -- audit finding B2 (RECENTLY ADDED).

The kqueue backend arms exactly ONE (ident, filter) knote per direction per fd,
EV_ADD | EV_ONESHOT (src/runloom_c/netpoll_register.c.inc:139-142).  That single
knote stands in for EVERY same-direction fiber parked on the fd: when the fd
becomes ready the pump collects ONE kevent, then calls
runloom_pump_dispatch_event(fd, mask, wake_all=1)
(src/runloom_c/netpoll_pump.c.inc:215).  With wake_all=1 the dispatcher walks the
WHOLE pool->by_fd[fd] bucket and wakes every parker whose events & mask -- it does
NOT stop at first match (netpoll_pump_helpers.c.inc:80-91).  The one-shot knote
auto-disabled after firing once and there is no re-arm path for it, so a
first-match-only wake (the wake_all=0 epoll/iocp behaviour) would STRAND every
sibling parker on that fd forever.  These tests prove all siblings wake.

Because wake_all walks by_fd[fd] keyed on the fd NUMBER, the N siblings must all
park on the SAME fd number (one shared read-end fileno).  Each fiber's wait_fd()
re-issues EV_ADD|EV_ONESHOT (idempotent re-arm) and links its own parker into the
shared bucket; the pump's single fired event must wake them all.

Covered branches (file:line):
  - netpoll_pump_helpers.c.inc:80-91  wake_all=1 keeps walking the bucket (B2)
  - netpoll_pump_helpers.c.inc:53-79  per-parker claim + events&mask gate +
                                      ready_out = mask & p->events
  - netpoll_pump.c.inc:202-215        READ vs WRITE filter -> mask, dispatch
                                      with wake_all=1
  - netpoll_register.c.inc:139-142    EV_ADD|EV_ONESHOT per requested direction,
                                      re-armed every park

Asserted through real sockets/pipes + per-fiber flags summed at the end -- never
an internal.  Runs both single-thread (runloom_c.run) and M:N (runloom.run(n)).
"""
import os
import socket
import sys

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

sys.path.insert(0, "src")

import runloom          # noqa: E402  high-level M:N entry (run / go / sleep)
import runloom_c        # noqa: E402

READ = 1
WRITE = 2


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _reset_netpoll_registration():
    """Clear the per-fd 'registered' bit cache around each test.

    These tests raw-close their sockets/pipes, bypassing the netpoll_unregister
    that all real runloom close paths run.  Under kqueue's EV_ONESHOT re-arm a
    reused fd NUMBER whose stale fd-bit is set would have its registration
    rolled differently; clearing here mimics the real close hook so fds don't
    leak registration state into each other (the convention from
    test_netpoll_conformance)."""
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:       # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _clean_registration():
    _reset_netpoll_registration()
    yield
    _reset_netpoll_registration()


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _drive(*fibers):
    """Single-thread scheduler: spawn each callable, run, re-raise the first
    exception any fiber hit (so asserts surface)."""
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


def _backend_is_kqueue():
    return runloom_c.netpoll_backend() == "kqueue"


# ==========================================================================
# 1) N fibers all park READ on the SAME fd; one peer write must wake ALL N.
#    Single-thread driver.  (B2: wake_all keeps walking the bucket.)
#    netpoll_pump_helpers.c.inc:80-91
# ==========================================================================
@pytest.mark.parametrize("n", [2, 3, 8, 32], ids=lambda n: "n%d" % n)
def test_wake_all_same_fd_single_thread(n):
    assert _backend_is_kqueue()
    a, b = _pair()
    fd = a.fileno()
    woke = bytearray(n)          # one slot per fiber (race-free, single writer)

    def reader(i):
        def run():
            # All N share the SAME fd number, so all land in by_fd[fd].  The
            # one-shot READ knote stands in for all of them; the single peer
            # write must wake EVERY parker (not just first-match).
            r = runloom_c.wait_fd(fd, READ, 3000)
            if r == READ:
                woke[i] = 1
        return run

    def writer():
        runloom_c.sched_yield()              # let all N readers park first
        b.send(b"x")                         # ONE write -> all N must wake

    _drive(*[reader(i) for i in range(n)], writer)

    assert sum(woke) == n, (
        "only %d/%d siblings woke on one fd (first-match strands the rest, B2)"
        % (sum(woke), n))
    a.close()
    b.close()


# ==========================================================================
# 2) N readers parked on ONE os.pipe read-end; ONE byte must wake all N.
#    A pipe (not a socket) exercises the same by_fd bucket walk on a different
#    kind of fd.  Single-thread driver.  netpoll_pump_helpers.c.inc:80-91
# ==========================================================================
@pytest.mark.parametrize("n", [2, 4, 8], ids=lambda n: "n%d" % n)
def test_wake_all_pipe_readers_single_thread(n):
    assert _backend_is_kqueue()
    rfd, wfd = os.pipe()
    os.set_blocking(rfd, False)
    woke = bytearray(n)

    def reader(i):
        def run():
            r = runloom_c.wait_fd(rfd, READ, 3000)
            if r == READ:
                woke[i] = 1
        return run

    def writer():
        runloom_c.sched_yield()
        os.write(wfd, b"\x01")               # one byte -> wake every reader

    _drive(*[reader(i) for i in range(n)], writer)

    assert sum(woke) == n, (
        "pipe: %d/%d readers woke on one write (B2 first-match strand)"
        % (sum(woke), n))
    os.close(rfd)
    os.close(wfd)


# ==========================================================================
# 3) MIXED directions on the SAME fd: some park READ, some park WRITE.
#    A socketpair end is ALWAYS writable, so the WRITE knote fires immediately
#    and must wake EVERY WRITE parker; the READ parkers wake only when the peer
#    writes.  The ready direction wakes exactly its set.
#    netpoll_pump_helpers.c.inc:53 (events & mask gate) + :80-91 (wake_all).
#    netpoll_pump.c.inc:202-203 (EVFILT_READ vs EVFILT_WRITE -> mask).
# ==========================================================================
@pytest.mark.parametrize("n_each", [2, 4], ids=lambda k: "each%d" % k)
def test_wake_all_mixed_directions_same_fd(n_each):
    assert _backend_is_kqueue()
    a, b = _pair()
    fd = a.fileno()
    n = 2 * n_each
    got = [None] * n             # got[i] = mask each fiber's wait_fd returned

    def waiter(i, events):
        def run():
            got[i] = runloom_c.wait_fd(fd, events, 3000)
        return run

    fibers = []
    # even indices: WRITE waiters (fire immediately, socket is writable)
    # odd  indices: READ  waiters (fire when the peer writes)
    for i in range(n):
        ev = WRITE if (i % 2 == 0) else READ
        fibers.append(waiter(i, ev))

    def feeder():
        runloom_c.sched_yield()              # let everyone park
        b.send(b"hello")                     # make a readable too

    _drive(*fibers, feeder)

    writers = [got[i] for i in range(n) if i % 2 == 0]
    readers = [got[i] for i in range(n) if i % 2 == 1]
    assert all(m == WRITE for m in writers), (
        "WRITE set: every writable-waiter must wake WRITE, got %r" % (writers,))
    assert all(m == READ for m in readers), (
        "READ set: every readable-waiter must wake READ, got %r" % (readers,))
    a.close()
    b.close()


# ==========================================================================
# 4) wake_all wakes ALL same-direction parkers but NOT the wrong direction.
#    All park READ on a fd that is writable-but-not-readable; nobody parks
#    WRITE.  The READ parkers must NOT wake on writability -- they wake only on
#    their deadline (0).  Proves dispatch masks to the fired direction even
#    while walking the whole bucket.  netpoll_pump_helpers.c.inc:53.
# ==========================================================================
@pytest.mark.parametrize("n", [2, 4], ids=lambda n: "n%d" % n)
def test_wake_all_does_not_cross_direction(n):
    assert _backend_is_kqueue()
    a, b = _pair()                           # a is writable, never readable
    fd = a.fileno()
    out = [None] * n

    def reader(i):
        def run():
            out[i] = runloom_c.wait_fd(fd, READ, 250)    # short deadline
        return run

    _drive(*[reader(i) for i in range(n)])

    assert all(m == 0 for m in out), (
        "READ waiter(s) fired on writability (cross-direction wake): %r" % (out,))
    a.close()
    b.close()


# ==========================================================================
# 5) M:N: N fibers park READ on the SAME shared fd across n hubs; one write
#    must wake ALL N regardless of which hub each landed on.  dispatch_event
#    searches EVERY pool's by_fd, so a delivered fd resolves to every sibling
#    parker no matter the hub (netpoll_pump_helpers.c.inc:45-48 loop + B2 walk).
# ==========================================================================
@pytest.mark.parametrize("hubs", [2, 4, 8], ids=lambda h: "hubs%d" % h)
@pytest.mark.parametrize("n", [4, 16], ids=lambda n: "n%d" % n)
def test_wake_all_same_fd_mn(hubs, n):
    assert _backend_is_kqueue()
    box = {"woke": 0}

    def main():
        a, b = _pair()
        fd = a.fileno()
        woke = bytearray(n)              # per-fiber slot, single writer each

        def reader(i):
            r = runloom_c.wait_fd(fd, READ, 4000)
            if r == READ:
                woke[i] = 1

        for i in range(n):
            runloom.go(reader, i)
        runloom.sleep(0.2)              # let all N park (spread across hubs)
        b.send(b"x")                    # ONE write -> all N must wake
        runloom.sleep(0.4)             # give every hub time to drain + resume
        box["woke"] = sum(woke)
        a.close()
        b.close()

    runloom.run(hubs, main)
    assert box["woke"] == n, (
        "M:N hubs=%d: only %d/%d siblings woke on one fd (B2 strand)"
        % (hubs, box["woke"], n))


# ==========================================================================
# 6) M:N pipe: N readers parked on one os.pipe read-end across n hubs; one
#    write wakes them all.  netpoll_pump_helpers.c.inc:80-91 under M:N.
# ==========================================================================
@pytest.mark.parametrize("hubs", [2, 8], ids=lambda h: "hubs%d" % h)
def test_wake_all_pipe_readers_mn(hubs):
    assert _backend_is_kqueue()
    n = 12
    box = {"woke": 0}

    def main():
        rfd, wfd = os.pipe()
        os.set_blocking(rfd, False)
        woke = bytearray(n)

        def reader(i):
            r = runloom_c.wait_fd(rfd, READ, 4000)
            if r == READ:
                woke[i] = 1

        for i in range(n):
            runloom.go(reader, i)
        runloom.sleep(0.2)
        os.write(wfd, b"\x01")
        runloom.sleep(0.4)
        box["woke"] = sum(woke)
        os.close(rfd)
        os.close(wfd)

    runloom.run(hubs, main)
    assert box["woke"] == n, (
        "M:N pipe hubs=%d: only %d/%d readers woke on one write" % (
            hubs, box["woke"], n))


# ==========================================================================
# 7) M:N mixed directions on the SAME fd across hubs: the ready direction wakes
#    exactly its set.  WRITE waiters wake immediately (socket writable); READ
#    waiters wake on the peer write.  netpoll_pump_helpers.c.inc:53 + :80-91.
# ==========================================================================
@pytest.mark.parametrize("hubs", [2, 4], ids=lambda h: "hubs%d" % h)
def test_wake_all_mixed_directions_mn(hubs):
    assert _backend_is_kqueue()
    n_each = 6
    n = 2 * n_each
    box = {"writers_ok": 0, "readers_ok": 0}

    def main():
        a, b = _pair()
        fd = a.fileno()
        got = [None] * n

        def waiter(i, events):
            got[i] = runloom_c.wait_fd(fd, events, 4000)

        for i in range(n):
            ev = WRITE if (i % 2 == 0) else READ
            runloom.go(waiter, i, ev)
        runloom.sleep(0.2)
        b.send(b"hello")               # make a readable too
        runloom.sleep(0.5)
        box["writers_ok"] = sum(
            1 for i in range(n) if i % 2 == 0 and got[i] == WRITE)
        box["readers_ok"] = sum(
            1 for i in range(n) if i % 2 == 1 and got[i] == READ)
        a.close()
        b.close()

    runloom.run(hubs, main)
    assert box["writers_ok"] == n_each, (
        "M:N hubs=%d: %d/%d WRITE waiters woke WRITE" % (
            hubs, box["writers_ok"], n_each))
    assert box["readers_ok"] == n_each, (
        "M:N hubs=%d: %d/%d READ waiters woke READ" % (
            hubs, box["readers_ok"], n_each))


# ==========================================================================
# 8) RE-ARM STORM: one fiber loops park->wake->re-park rapidly on a fd while
#    OTHER fibers also park READ on the SAME fd.  Each peer write must wake the
#    looping fiber AND every co-parked sibling (no lost/stranded waiter across
#    re-arm churn).  The looper re-issues EV_ADD|EV_ONESHOT each round
#    (netpoll_register.c.inc:139-142) and the wake_all dispatch must re-wake the
#    whole bucket every round (netpoll_pump_helpers.c.inc:80-91).  Single-thread
#    so the round handshake is deterministic.
# ==========================================================================
@pytest.mark.parametrize("rounds", [10, 30], ids=lambda r: "rounds%d" % r)
def test_wake_all_rearm_storm_single_thread(rounds):
    assert _backend_is_kqueue()
    a, b = _pair()
    fd = a.fileno()
    ready = runloom_c.Chan()           # looper -> writer "re-parked" handshake
    sibling_n = 3
    sib_wakes = bytearray(sibling_n)   # times each sibling observed a wake
    looper_wakes = [0]

    def looper():
        for _ in range(rounds):
            r = runloom_c.wait_fd(fd, READ, 3000)
            if r == READ:
                looper_wakes[0] += 1
            try:
                a.recv(64)             # drain so the next write is a fresh edge
            except OSError:
                pass
            ready.send(1)              # "consumed + about to re-park"

    def sibling(i):
        def run():
            # Park once on the shared fd.  The FIRST write that wakes the looper
            # must ALSO wake this sibling (wake_all walks the whole bucket).
            r = runloom_c.wait_fd(fd, READ, 3000)
            if r == READ:
                sib_wakes[i] = 1
        return run

    def writer():
        runloom_c.sched_yield()        # let looper + siblings park
        for _ in range(rounds):
            b.send(b"x")               # wakes looper + (round 1) every sibling
            ready.recv()               # looper drained + re-parked; next edge

    _drive(looper, *[sibling(i) for i in range(sibling_n)], writer)

    assert looper_wakes[0] == rounds, (
        "looper lost a wake across re-arm storm: %d/%d" % (
            looper_wakes[0], rounds))
    assert sum(sib_wakes) == sibling_n, (
        "re-arm storm stranded co-parked siblings: %d/%d woke" % (
            sum(sib_wakes), sibling_n))
    a.close()
    b.close()


# ==========================================================================
# 9) RE-ARM STORM under M:N: a looping re-parker plus co-parked siblings on the
#    SAME fd, driven across hubs.  Same invariant -- no stranded waiter -- but
#    exercising the cross-hub pump timing.  netpoll_pump_helpers.c.inc:80-91 +
#    netpoll_register.c.inc:139-142 under run(n).
# ==========================================================================
@pytest.mark.parametrize("hubs", [2, 4], ids=lambda h: "hubs%d" % h)
def test_wake_all_rearm_storm_mn(hubs):
    assert _backend_is_kqueue()
    rounds = 20
    sibling_n = 4
    box = {"looper": 0, "siblings": 0}

    def main():
        a, b = _pair()
        fd = a.fileno()
        ready = runloom_c.Chan()
        sib_wakes = bytearray(sibling_n)
        looper_wakes = [0]

        def looper():
            for _ in range(rounds):
                r = runloom_c.wait_fd(fd, READ, 4000)
                if r == READ:
                    looper_wakes[0] += 1
                try:
                    a.recv(64)
                except OSError:
                    pass
                ready.send(1)

        def sibling(i):
            r = runloom_c.wait_fd(fd, READ, 4000)
            if r == READ:
                sib_wakes[i] = 1

        runloom.go(looper)
        for i in range(sibling_n):
            runloom.go(sibling, i)
        runloom.sleep(0.2)             # everyone parked across hubs
        for _ in range(rounds):
            b.send(b"x")
            ready.recv()
        runloom.sleep(0.3)
        box["looper"] = looper_wakes[0]
        box["siblings"] = sum(sib_wakes)
        a.close()
        b.close()

    runloom.run(hubs, main)
    assert box["looper"] == rounds, (
        "M:N hubs=%d: looper lost a wake: %d/%d" % (
            hubs, box["looper"], rounds))
    assert box["siblings"] == sibling_n, (
        "M:N hubs=%d: re-arm storm stranded siblings: %d/%d" % (
            hubs, box["siblings"], sibling_n))


# ==========================================================================
# 10) wake_all + losers re-park: ALL N readers wake on one write, but only one
#     can consume the single byte; the LOSERS (recv EAGAIN) re-park and must
#     wake AGAIN on the next write.  Proves wake_all delivers to every sibling
#     each round AND the one-shot re-arms cleanly for the losers.
#     netpoll_pump_helpers.c.inc:80-91 + netpoll_register.c.inc:139-142.
# ==========================================================================
@pytest.mark.parametrize("n", [2, 3], ids=lambda n: "n%d" % n)
def test_wake_all_losers_recheck_and_repark(n):
    assert _backend_is_kqueue()
    a, b = _pair()
    fd = a.fileno()
    # Each reader must successfully recv exactly one byte; with N bytes total
    # (one per round) and the loser re-parking, all N must eventually consume.
    consumed = bytearray(n)
    wake_counts = bytearray(n)         # >=1 means it observed at least one wake

    def reader(i):
        def run():
            while True:
                r = runloom_c.wait_fd(fd, READ, 3000)
                if r != READ:
                    return                   # timeout: give up (failure shows up)
                wake_counts[i] = 1
                try:
                    data = a.recv(1)
                except OSError:
                    continue                 # loser this round: re-park
                if data:
                    consumed[i] = 1
                    return                   # got my byte, done
        return run

    def writer():
        runloom_c.sched_yield()
        for _ in range(n):
            b.send(b"x")                     # one byte per round
            # let the woken readers run: one consumes, losers re-park, next write
            for _ in range(4):
                runloom_c.sched_yield()

    _drive(*[reader(i) for i in range(n)], writer)

    assert sum(wake_counts) == n, (
        "not every reader observed a wake: %d/%d" % (sum(wake_counts), n))
    assert sum(consumed) == n, (
        "losers did not re-park + eventually consume: %d/%d" % (
            sum(consumed), n))
    a.close()
    b.close()


if __name__ == "__main__":
    print("netpoll backend under test:", runloom_c.netpoll_backend())
    raise SystemExit(pytest.main([__file__, "-v"]))
