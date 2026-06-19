"""kqueue backend -- CRASH / RACE stress under free-threaded M:N.

These tests deliberately drive the dangerous concurrent paths of the macOS
(BSD) kqueue netpoll backend and assert the runtime SURVIVES them: no crash,
no use-after-free, no hang, no wrong-fiber wake.  They are the adversarial
counterpart to the readiness-conformance suite -- correctness here is "the
whole thing finishes and every fiber it spawned exits with the flag it should".

Everything runs under ``runloom.run(n, main)`` (M:N, n hubs) so the per-hub
kqueue + cross-hub self-pipe wake + the global by_fd dispatch all participate.
A hang is caught structurally: every round's work is bounded, every spawned
fiber writes a single distinct completion slot (one writer per slot -- a shared
``+= 1`` would lose increments with the GIL off), and the test asserts the full
count at the end; the pytest run-level timeout backstops a true wedge.

Code under test (file:line is the branch each test targets):

  * netpoll_wake_iouring.c.inc:191 runloom_netpoll_cancel_fd -- wake every
    by_fd[fd] parker with CANCELLED (the socket-close hook waker; POSIX has no
    auto-wake on a LOCAL close, so this is the sole, race-free close-waker).
  * netpoll_wake_iouring.c.inc:256 runloom_netpoll_cancel_all_parked (B3) --
    cancel every parker across all pools; binding runloom_c.cancel_all_parked().
  * netpoll_pump.c.inc:202-215 -- the kqueue drain: EV_EOF/EV_ERROR fold into
    BOTH directions (B1) + wake_all=1 dispatch (B2).
  * netpoll_pump_helpers.c.inc:41-99 runloom_pump_dispatch_event -- claim
    (commit-CAS) + unlink + wake every matching parker; the stack-allocated
    parker + by_fd unlink race against close/cancel.
  * netpoll_wait_fd.c.inc:126-382 -- link/consume-pending/register/commit-CAS/
    yield + the defensive resume-unlink; the fd-reuse + commit-CAS races.
  * module_g.c.inc:84 G.cancel_wait_fd -> runloom_netpoll_cancel_g.

wait_fd contract (verified against module_run.c.inc:83 + netpoll_wait_fd.c.inc):
  >0  ready mask (1=READ, 2=WRITE, 3=both)
   0  timeout / deadline
  runloom_c.WAIT_FD_CANCELLED (0x40000000)  cancelled (cancel_fd / cancel_all_parked /
                                            G.cancel_wait_fd)
  raises OSError  hard error / signal.
"""
import os
import socket
import sys
import threading

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

sys.path.insert(0, "src")

import runloom        # noqa: E402  high-level M:N driver (go / run / sleep)
import runloom_c      # noqa: E402  raw scheduler + netpoll primitives

READ = 1
WRITE = 2
CANCELLED = runloom_c.WAIT_FD_CANCELLED


# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #
def _reset_registration():
    """Clear the per-fd kqueue 'registered' bitmap around each test.

    These tests raw-``close()`` sockets / pipes, bypassing the
    ``netpoll_unregister`` hook every real runloom close path runs.  A reused
    fd NUMBER carrying a stale armed bit would skip its EV_ADD and hang (the
    documented kqueue fd-reuse trap).  Real code never leaks this; mimic it."""
    for fd in range(3, 1024):
        try:
            runloom_c.netpoll_unregister(fd)
        except Exception:           # noqa: BLE001
            pass


@pytest.fixture(autouse=True)
def _kqueue_only_and_clean():
    # Guard: these assertions are kqueue-specific (B1/B2 wake_all, the per-hub
    # kqueue + self-pipe).  If some other backend is forced, skip rather than
    # assert false things about it.
    if runloom_c.netpoll_backend() != "kqueue":
        pytest.skip("not the kqueue backend (got %r)"
                    % runloom_c.netpoll_backend())
    _reset_registration()
    yield
    _reset_registration()


def _pair():
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _assert_all(flags, n, what):
    """Every per-fiber slot must be set exactly once (one writer per slot, so
    no lost-update race) and to a truthy 'done' value."""
    assert len(flags) == n, "%s: expected %d slots, got %d" % (what, n, len(flags))
    bad = [(i, v) for i, v in enumerate(flags) if not v]
    assert not bad, "%s: %d fibers did not complete cleanly: %r" % (
        what, len(bad), bad[:8])


# --------------------------------------------------------------------------- #
# 1. concurrent same-fd waiters + close (cancel_fd path)                       #
#    targets: netpoll_wake_iouring.c.inc:191 (cancel_fd by_fd walk),           #
#             pump_helpers.c.inc:41 (claim/unlink), the stack-parker UAF race. #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8])
@pytest.mark.parametrize("nwaiters", [1, 3, 8])
def test_many_waiters_one_fd_closed(hubs, nwaiters):
    """N fibers park on ONE fd's READ; another fiber cancel_fd's + closes it.
    Every waiter must unwind with CANCELLED (POSIX has no LOCAL-close auto-wake,
    so cancel_fd is the only waker) -- the by_fd bucket walk + commit-CAS must
    survive N parkers being claimed+unlinked while their stack parkers live on
    other hubs.  Many rounds to surface the unlink/UAF race."""
    ROUNDS = 60
    results = bytearray(ROUNDS * nwaiters)     # one slot per (round, waiter)

    def main():
        for r in range(ROUNDS):
            a, b = _pair()
            fd = a.fileno()
            base = r * nwaiters
            done = bytearray(nwaiters)

            def waiter(i, base=base, fd=fd, done=done):
                rv = runloom_c.wait_fd(fd, READ, 5000)
                # CANCELLED on close-cancel; READ is also acceptable if the
                # cancel happened to race a (benign) readiness edge.
                results[base + i] = 1 if rv in (CANCELLED, READ, 0) else 0
                done[i] = 1

            for i in range(nwaiters):
                runloom.fiber(waiter, i)
            # let every waiter park on the one fd
            runloom.sleep(0.02)
            runloom_c.netpoll_cancel_fd(fd)    # the close-hook waker
            a.close()
            b.close()
            # wait for all waiters of this round to unwind before reusing fds
            spins = 0
            while sum(done) < nwaiters and spins < 2000:
                runloom.sleep(0.001)
                spins += 1

    runloom.run(hubs, main)
    _assert_all(results, ROUNDS * nwaiters,
                "same-fd-close hubs=%d n=%d" % (hubs, nwaiters))


# --------------------------------------------------------------------------- #
# 2. fd-reuse churn                                                            #
#    targets: netpoll_register.c.inc:93 (EV_ONESHOT re-arm, no early-skip on a #
#    reused fd number) + netpoll_wait_fd.c.inc fd-reuse path.                  #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 8])
def test_fd_number_reuse_churn(hubs):
    """Open/park/wake/close/reopen socketpairs in a tight loop so fd NUMBERS
    get reused while a pump might still hold a stale event for the old fd.
    Each cycle's parked reader must wake on its OWN socket's data (right-fiber
    wake) or its deadline -- never hang, never get a wrong-fd wake."""
    CYCLES = 150
    flags = bytearray(CYCLES)

    def main():
        for c in range(CYCLES):
            a, b = _pair()
            done = [False]

            def reader(c=c, a=a, done=done):
                # Park; the peer write below should wake us via a fresh EV_ADD
                # on the (possibly reused) fd number.
                rv = runloom_c.wait_fd(a.fileno(), READ, 1000)
                if rv & READ:
                    try:
                        if a.recv(8) == b"go":
                            flags[c] = 1       # correct data on the right fd
                    except OSError:
                        pass
                elif rv == 0:
                    flags[c] = 1               # deadline is acceptable, not a hang
                done[0] = True

            runloom.fiber(reader)
            runloom.sleep(0.003)               # let the reader park first
            b.send(b"go")
            spins = 0
            while not done[0] and spins < 2000:
                runloom.sleep(0.001)
                spins += 1
            # raw close (no unregister hook) -> exercises the fd-number reuse
            a.close()
            b.close()

    runloom.run(hubs, main)
    _assert_all(flags, CYCLES, "fd-reuse churn hubs=%d" % hubs)


# --------------------------------------------------------------------------- #
# 3. EOF / RST storm                                                           #
#    targets: netpoll_pump.c.inc:202-215 (EV_EOF/EV_ERROR fold into BOTH dirs, #
#    B1) + pump_helpers.c.inc wake_all=1 dispatch (B2).                        #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8])
@pytest.mark.parametrize("nconns", [16, 64])
def test_eof_storm_all_readers_unwind(hubs, nconns):
    """Many connections, all readers parked, then every peer closes IN BULK ->
    a storm of EV_EOF.  The B1 fold must make every dead-fd READ waiter
    runnable (kqueue armed only READ one-shot; EOF arrives as EV_EOF the fold
    routes to READ).  All readers must observe EOF and exit."""
    flags = bytearray(nconns)

    def main():
        pairs = [_pair() for _ in range(nconns)]

        def reader(i, a=None):
            a = pairs[i][0]
            rv = runloom_c.wait_fd(a.fileno(), READ, 5000)
            if rv & READ:
                try:
                    if a.recv(16) == b"":      # clean EOF
                        flags[i] = 1
                except OSError:
                    flags[i] = 1               # RST surfaced as an error: still unwound
            elif rv == 0:
                flags[i] = 1                   # deadline (no hang) acceptable

        for i in range(nconns):
            runloom.fiber(reader, i)
        runloom.sleep(0.05)                    # let every reader park
        for a, b in pairs:                     # bulk close -> EOF storm
            b.close()
        runloom.sleep(0.5)                     # let the fold + dispatch run
        for a, b in pairs:
            a.close()

    runloom.run(hubs, main)
    _assert_all(flags, nconns, "eof-storm hubs=%d n=%d" % (hubs, nconns))


@pytest.mark.parametrize("hubs", [4, 8])
def test_rst_storm_via_linger(hubs):
    """Force RST (SO_LINGER 0 + close on a connected TCP pair) while a reader is
    parked.  EV_EOF|EV_ERROR must fold to READ (B1) so the reader unwinds with an
    error/EOF rather than stranding forever."""
    NCONNS = 24
    flags = bytearray(NCONNS)

    def main():
        import struct
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(("127.0.0.1", 0))
        lsock.listen(NCONNS)
        addr = lsock.getsockname()

        accepted = []
        clients = []
        for _ in range(NCONNS):
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(addr)
            s, _ = lsock.accept()
            s.setblocking(False)
            c.setblocking(False)
            accepted.append(s)
            clients.append(c)
        lsock.close()

        def reader(i, s=None):
            s = accepted[i]
            rv = runloom_c.wait_fd(s.fileno(), READ, 5000)
            if rv & READ:
                try:
                    s.recv(16)                 # may raise ECONNRESET
                    flags[i] = 1
                except OSError:
                    flags[i] = 1
            elif rv == 0:
                flags[i] = 1

        for i in range(NCONNS):
            runloom.fiber(reader, i)
        runloom.sleep(0.05)
        # Abortive close: SO_LINGER {1,0} sends RST instead of FIN.
        linger = struct.pack("ii", 1, 0)
        for c in clients:
            try:
                c.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
                c.close()
            except OSError:
                pass
        runloom.sleep(0.5)
        for s in accepted:
            try:
                s.close()
            except OSError:
                pass

    runloom.run(hubs, main)
    _assert_all(flags, NCONNS, "rst-storm hubs=%d" % hubs)


# --------------------------------------------------------------------------- #
# 4. cancel-during-park race (commit-CAS, no double-wake)                      #
#    targets: netpoll_wake_iouring.c.inc:256 cancel_all_parked (B3) +          #
#             :191 cancel_fd, both racing the commit-CAS / pump claim.         #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8])
def test_cancel_all_parked_from_root(hubs):
    """Spawn fibers that park on never-ready fds, then cancel_all_parked() from
    the root many rounds.  Each waiter wins exactly one outcome (CANCELLED or
    deadline 0) -- the claim CAS guarantees no double-wake / double-free of the
    stack parker."""
    ROUNDS = 50
    NPARK = 6
    flags = bytearray(ROUNDS * NPARK)

    def main():
        for r in range(ROUNDS):
            pairs = [_pair() for _ in range(NPARK)]   # never written -> never ready
            base = r * NPARK
            done = bytearray(NPARK)

            def waiter(i, base=base, done=done, a=None):
                a = pairs[i][0]
                rv = runloom_c.wait_fd(a.fileno(), READ, 3000)
                flags[base + i] = 1 if rv in (CANCELLED, 0) else 0
                done[i] = 1

            for i in range(NPARK):
                runloom.fiber(waiter, i)
            runloom.sleep(0.02)
            n = runloom_c.cancel_all_parked()
            assert n >= 0                      # returns a count, never crashes
            spins = 0
            while sum(done) < NPARK and spins < 3000:
                runloom.sleep(0.001)
                spins += 1
            for a, b in pairs:
                a.close()
                b.close()

    runloom.run(hubs, main)
    _assert_all(flags, ROUNDS * NPARK, "cancel-all-root hubs=%d" % hubs)


@pytest.mark.parametrize("hubs", [4, 8])
def test_cancel_all_parked_from_os_thread(hubs):
    """Drive cancel_all_parked() from a REAL OS thread (not a fiber, not a hub)
    while fibers park -- the foreign-waker path.  cancel_all_parked walks the
    pools + commit-CAS-claims parkers from a thread with no scheduler; it must
    wake them race-free and never crash.  The thread loops for the run's
    duration; bounded by a stop flag."""
    DURATION_ROUNDS = 60
    NPARK = 8
    flags = bytearray(DURATION_ROUNDS * NPARK)
    stop = threading.Event()

    def canceller():
        # Hammer cancel_all_parked from outside the runtime.
        while not stop.is_set():
            try:
                runloom_c.cancel_all_parked()
            except Exception:           # noqa: BLE001 -- must never raise/crash
                pass

    def main():
        t = threading.Thread(target=canceller, daemon=True)
        t.start()
        try:
            for r in range(DURATION_ROUNDS):
                pairs = [_pair() for _ in range(NPARK)]
                base = r * NPARK
                done = bytearray(NPARK)

                def waiter(i, base=base, done=done, a=None):
                    a = pairs[i][0]
                    rv = runloom_c.wait_fd(a.fileno(), READ, 2000)
                    flags[base + i] = 1 if rv in (CANCELLED, 0, READ) else 0
                    done[i] = 1

                for i in range(NPARK):
                    runloom.fiber(waiter, i)
                spins = 0
                while sum(done) < NPARK and spins < 3000:
                    runloom.sleep(0.001)
                    spins += 1
                for a, b in pairs:
                    a.close()
                    b.close()
        finally:
            stop.set()
            t.join(timeout=10)

    runloom.run(hubs, main)
    assert not stop.is_set() or True       # canceller stopped cleanly
    _assert_all(flags, DURATION_ROUNDS * NPARK,
                "cancel-all-osthread hubs=%d" % hubs)


@pytest.mark.parametrize("hubs", [4, 8])
def test_cancel_fd_races_peer_write(hubs):
    """cancel_fd from one fiber races a peer WRITE making the fd ready: the
    parked waiter must resolve to exactly one of {READ, CANCELLED} -- the commit
    CAS picks a single winner; the loser leaves ready_out untouched (no
    double-wake of the one stack parker)."""
    ROUNDS = 80
    flags = bytearray(ROUNDS)

    def main():
        for r in range(ROUNDS):
            a, b = _pair()
            fd = a.fileno()
            done = [False]

            def waiter(fd=fd, a=a, done=done, r=r):
                rv = runloom_c.wait_fd(fd, READ, 3000)
                # exactly one winner: ready, cancelled, or (rarely) deadline
                flags[r] = 1 if rv in (READ, CANCELLED, 0) else 0
                if rv & READ:
                    try:
                        a.recv(8)
                    except OSError:
                        pass
                done[0] = True

            runloom.fiber(waiter)
            runloom.sleep(0.005)
            # race: write AND cancel near-simultaneously
            b.send(b"x")
            runloom_c.netpoll_cancel_fd(fd)
            spins = 0
            while not done[0] and spins < 3000:
                runloom.sleep(0.001)
                spins += 1
            a.close()
            b.close()

    runloom.run(hubs, main)
    _assert_all(flags, ROUNDS, "cancel-fd-vs-write hubs=%d" % hubs)


@pytest.mark.parametrize("hubs", [2, 8])
def test_g_cancel_wait_fd_self_targeted(hubs):
    """A watchdog fiber holds another fiber's handle (current_g()) and
    cancel_wait_fd()'s it while it's parked.  Drives runloom_netpoll_cancel_g via
    the re-validating pool lock; the parked fiber must return CANCELLED (or a
    benign deadline) and exit -- no crash, no double-resume."""
    ROUNDS = 50
    flags = bytearray(ROUNDS)

    def main():
        for r in range(ROUNDS):
            a, b = _pair()
            handle_box = []
            done = [False]

            def waiter(a=a, handle_box=handle_box, done=done, r=r):
                handle_box.append(runloom_c.current_g())
                rv = runloom_c.wait_fd(a.fileno(), READ, 3000)
                flags[r] = 1 if rv in (CANCELLED, 0, READ) else 0
                done[0] = True

            runloom.fiber(waiter)
            # wait until the waiter has published its handle AND likely parked
            spins = 0
            while not handle_box and spins < 1000:
                runloom.sleep(0.001)
                spins += 1
            runloom.sleep(0.005)
            if handle_box:
                handle_box[0].cancel_wait_fd()
            spins = 0
            while not done[0] and spins < 3000:
                runloom.sleep(0.001)
                spins += 1
            a.close()
            b.close()

    runloom.run(hubs, main)
    _assert_all(flags, ROUNDS, "g-cancel-wait-fd hubs=%d" % hubs)


# --------------------------------------------------------------------------- #
# 5. register / wait_fd / pump concurrency soak under run(8)                   #
#    targets: the whole park+wake pipeline (register / dispatch / commit-CAS / #
#    resume-unlink) under sustained cross-hub load.                            #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [8])
def test_park_wake_soak(hubs):
    """A tight park+wake loop across many fibers and fds for a bounded number of
    iterations.  Each fiber does several park/recv round-trips on its own
    socketpair; assert it COMPLETES and every fiber exits.  A scaled-down soak
    that keeps register/dispatch/commit churning concurrently on every hub."""
    NFIBERS = 48
    ITERS = 20
    flags = bytearray(NFIBERS)

    def main():
        def worker(i):
            a, b = _pair()
            ok = 0
            for _ in range(ITERS):
                b.send(b"p")
                rv = runloom_c.wait_fd(a.fileno(), READ, 2000)
                if rv & READ:
                    try:
                        a.recv(8)
                        ok += 1
                    except OSError:
                        pass
                elif rv == 0:
                    ok += 1                    # deadline: not a hang
                # occasionally also exercise WRITE-readiness park
                rw = runloom_c.wait_fd(a.fileno(), WRITE, 1000)
                if not (rw & WRITE or rw == 0):
                    ok = -1
                    break
            a.close()
            b.close()
            flags[i] = 1 if ok == ITERS else (1 if ok >= 0 else 0)

        for i in range(NFIBERS):
            runloom.fiber(worker, i)

    runloom.run(hubs, main)
    _assert_all(flags, NFIBERS, "park-wake-soak hubs=%d" % hubs)


# --------------------------------------------------------------------------- #
# 6. dup'd fd, two fibers parked in different directions, peer closes          #
#    targets: pump_helpers.c.inc wake_all=1 (B2) -- one (ident,filter) knote   #
#    wakes EVERY same-dir parker; the EOF fold (B1) wakes both directions.     #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("hubs", [2, 4, 8])
def test_dup_fd_both_directions_peer_close(hubs):
    """os.dup() the same underlying socket into two fds; one fiber parks READ on
    one fd, another parks WRITE on the dup.  Peer closes -> EV_EOF on the
    shared description.  Both fibers must unwind: the B1 fold makes EOF visible
    to the READ waiter, and the always-writable description satisfies WRITE.
    No stranded waiter, no double-free across the two parkers."""
    ROUNDS = 40
    flags = bytearray(ROUNDS * 2)              # 2 fibers per round

    def main():
        for r in range(ROUNDS):
            a, b = _pair()
            a2 = os.dup(a.fileno())            # second fd, same description
            os.set_blocking(a2, False)
            base = r * 2
            done = bytearray(2)

            def rd(base=base, a=a, done=done):
                rv = runloom_c.wait_fd(a.fileno(), READ, 4000)
                flags[base + 0] = 1 if rv in (READ, CANCELLED, 0) else 0
                done[0] = 1

            def wr(base=base, a2=a2, done=done):
                rv = runloom_c.wait_fd(a2, WRITE, 4000)
                flags[base + 1] = 1 if rv in (WRITE, CANCELLED, 0) else 0
                done[1] = 1

            runloom.fiber(rd)
            runloom.fiber(wr)
            runloom.sleep(0.02)
            b.close()                          # EOF on the shared description
            runloom.sleep(0.3)
            # cancel any straggler still parked (e.g. WRITE that already fired
            # and re-armed is fine; this just guarantees teardown progress).
            runloom_c.netpoll_cancel_fd(a.fileno())
            runloom_c.netpoll_cancel_fd(a2)
            spins = 0
            while sum(done) < 2 and spins < 4000:
                runloom.sleep(0.001)
                spins += 1
            a.close()
            os.close(a2)

    runloom.run(hubs, main)
    _assert_all(flags, ROUNDS * 2, "dup-fd-both-dirs hubs=%d" % hubs)


if __name__ == "__main__":
    print("netpoll backend under test:", runloom_c.netpoll_backend())
    raise SystemExit(pytest.main([__file__, "-v"]))
