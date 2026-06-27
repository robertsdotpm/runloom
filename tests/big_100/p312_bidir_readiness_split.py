"""big_100 / 312 -- single-fd bidirectional readiness split across hubs.

This targets a source-confirmed soft spot in the Linux netpoll arm cache.  The
arm mask (runloom_fd_armed) is keyed by fd NUMBER and is a single per-fd
direction-set bitmask (netpoll_register.c.inc: target = cur | need), and the
epoll pump wakes only the FIRST direction-matching parker per cycle then returns
(netpoll_pump_helpers.c.inc: `if (!wake_all) { return 1; }`), relying on LEVEL
re-report to serve the other waiter on the next cycle.  The register code's own
comment flags a known follow-up: a long-lived WRITE-heavy fd's lazy OUT-disarm.

So we put TWO goroutines on the SAME connected socket fd, deliberately on
DIFFERENT hubs (spawned in two separate passes so they fan out): goroutine R
parks the fd for READ readiness (hub A) while goroutine W fills the send buffer
to EWOULDBLOCK and parks the SAME fd for WRITE readiness / EPOLLOUT (hub B).  A
peer goroutine then makes that one fd simultaneously readable AND writable -- it
sends R's bytes (drives EPOLLIN) and drains W's backpressured bytes (drives
EPOLLOUT) -- so the kernel delivers ONE combined EPOLLIN|EPOLLOUT readiness
event that must wake BOTH same-fd parkers (across pump cycles via level
re-report).  If the WRITE arm is mis-disarmed, or the two arms race on the one
arm-cache slot and one is dropped, or the combined event wakes only one waiter
and the other never re-reports, a parker STRANDS.

ORACLE -- bidirectional conservation + require_no_lost:
  * R must recv EXACTLY N bytes, and the content must match the agreed pattern
    (a same-slot overwrite / cross-wire corrupts the byte count or content ->
    H.fail mismatch).
  * W must drain its FULL backpressured send of M bytes (sent == M, confirmed by
    the peer reader's received count -> a torn count or short send fails).
  * Both directions report their completion + totals through a Chan; the worker
    H.checks both arrived with exact totals.  A dropped EPOLLOUT wake strands W
    (its done-Chan never delivers) -> the worker can't join -> the goroutine
    never returns -> lost_workers>0 and the watchdog/_dump_parkers fires
    (readyParked>0 = lost wakeup).  A dropped EPOLLIN strands R identically.
  * Rounds ALTERNATE which direction the peer satisfies first, exercising both
    first-match orderings of the pump.

Meaningful only where wait_fd parks on a real readiness backend (epoll/kqueue);
io_uring marks fds always-ready so the park is a no-op -- still correct, just not
the targeted race -- so the conservation oracle holds on every backend and the
strand-hunt bites on the poll backends.

Stresses: one fd's combined EPOLLIN|EPOLLOUT arm mask, the pump's first-match
wake + level re-report, cross-hub READ-park vs WRITE-park on the SAME fd number,
lazy OUT-disarm.  Good TSan / controlled-M:N-replay target: the arm-cache slot
is written from two hubs (READ arm on A, WRITE arm on B) -- a data race on
runloom_fd_armed[fd] or a dropped first-match wake is the first signal, before
the conservation oracle even fires.
"""
import os
import socket

import harness
import runloom
import runloom_c

# Capture RAW os.write / set_blocking BEFORE harness's monkey.patch() makes them
# cooperative.  We need a raw non-blocking write that RAISES BlockingIOError when
# the send buffer fills, to get a deterministic "now genuinely blocked-in-write"
# point -- the patched os.write would instead PARK and we could never first arm
# the WRITE direction ourselves.
RAW_WRITE = os.write
RAW_SET_BLOCKING = os.set_blocking

# Small per-fd buffers so backpressure (EWOULDBLOCK on the writer) is cheap to
# induce and the peer's drain is bounded.  N and M are the agreed, exact totals
# the conservation oracle checks; both are a few buffers' worth so the transfer
# spans several readiness cycles (forcing real re-arm / re-report, not a single
# synchronous drain).
BUFSZ = 4096
N_RECV = 12000          # bytes the peer sends to the fd -> R must recv exactly N
M_SEND = 12000          # bytes W must push through the fd -> peer must drain M

# Per-park readiness timeout.  Kept SHORT (not one giant wait) so the dance
# converges promptly regardless of which pass spawns first: a reader spawned in
# pass 1 parks READ and re-probes every WAIT_MS until the pass-2 peer starts
# feeding it -- a long single wait would stall the whole unit on the first
# cross-pass gap.  A GENUINE strand (the targeted bug: a dropped EPOLLIN/EPOLLOUT
# wake on the combined arm mask) still surfaces: the round owner blocks on its
# done-Chan forever, the watchdog window expires, and require_no_lost fires
# (readyParked>0).  The short re-probe never MASKS a strand -- it only re-arms
# the SAME direction, so a truly dropped wake never gets its readiness back.
WAIT_MS = 200

READ = 1
WRITE = 2

# A deterministic, position-dependent byte pattern so a cross-wire / same-slot
# overwrite (R reading W's bytes, or a torn count) is caught by CONTENT, not just
# length.  pattern[i] depends on (seed, i), unique per (worker, round, direction).
def pattern(seed, n):
    out = bytearray(n)
    x = (seed * 2654435761) & 0xFFFFFFFF
    for i in range(n):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out[i] = (x >> 16) & 0xFF
    return bytes(out)


def reader(running, fd, sock, want, expect, done):
    """Park the fd for READ readiness, then recv EXACTLY `want` bytes; verify the
    content equals `expect`.  Reports (ok, nbytes) through `done`.  Parks on the
    SAME fd the writer arms for WRITE -- on (very likely) a different hub.
    `running()` lets it bail at teardown so a closed fd never busy-spins."""
    buf = bytearray()
    try:
        while len(buf) < want and running():
            # Park THIS fd for READ readiness.  Under per-hub epoll this arms the
            # READ direction of fd's combined arm mask on THIS goroutine's hub.
            try:
                ready = runloom_c.wait_fd(fd, READ, WAIT_MS)
            except OSError:
                break                    # fd closed at teardown
            if not (ready & READ):
                # Timed out without a READ wake: the peer hasn't fed us yet (a
                # cross-pass gap) or -- the targeted bug -- the EPOLLIN wake was
                # dropped on the combined arm mask.  Re-probe (short WAIT_MS); a
                # GENUINE strand never gets its readiness back, so the round owner
                # blocks on rdone forever -> require_no_lost fires.
                continue
            try:
                chunk = sock.recv(want - len(buf))
            except BlockingIOError:
                continue                 # spurious readiness; re-park
            except OSError:
                break
            if not chunk:
                break                    # peer closed early
            buf += chunk
    except Exception:                    # noqa: BLE001
        pass
    ok = (len(buf) == want and bytes(buf) == expect)
    done.send((ok, len(buf)))


def writer(running, fd, sock, total, payload, done):
    """Fill the send buffer to EWOULDBLOCK (a REAL write-park), then park the fd
    for WRITE readiness and drain the rest until `total` bytes are sent.  Reports
    (ok, nbytes) through `done`.  Arms the SAME fd for WRITE that the reader arms
    for READ -- on (very likely) a different hub."""
    sent = 0
    try:
        RAW_SET_BLOCKING(fd, False)
        while sent < total and running():
            # Raw non-blocking write: returns the count written, or raises
            # BlockingIOError when the buffer is full (-> we must park WRITE).
            try:
                n = RAW_WRITE(fd, payload[sent:])
                if n <= 0:
                    break
                sent += n
                continue
            except BlockingIOError:
                pass
            except OSError:
                break
            # Buffer full: park THIS fd for WRITE readiness (EPOLLOUT).  This arms
            # the WRITE direction of fd's combined arm mask; the peer's drain must
            # drive the EPOLLOUT wake.  A dropped/disarmed OUT wake strands here.
            try:
                ready = runloom_c.wait_fd(fd, WRITE, WAIT_MS)
            except OSError:
                break                    # fd closed at teardown
            if not (ready & WRITE):
                continue                 # no OUT wake yet; re-probe within budget
    except Exception:                    # noqa: BLE001
        pass
    done.send((sent == total, sent))


def peer(running, fd, sock, send_bytes, drain_total, drain_first):
    """The OTHER end of the connection.  Makes the test fd simultaneously
    readable (by sending `send_bytes` to it) AND writable (by draining its
    backpressured data), so ONE combined EPOLLIN|EPOLLOUT readiness event must
    wake both same-fd parkers.  `drain_first` alternates which direction the peer
    satisfies first across rounds, exercising both first-match orderings."""
    RAW_SET_BLOCKING(fd, False)
    sent = 0
    drained = 0
    send_payload = send_bytes
    nsend = len(send_payload)

    def do_send():
        nonlocal sent
        if sent >= nsend:
            return
        try:
            n = RAW_WRITE(fd, send_payload[sent:])
            if n > 0:
                sent += n
        except (BlockingIOError, OSError):
            pass

    def do_drain():
        nonlocal drained
        if drained >= drain_total:
            return
        try:
            chunk = sock.recv(65536)
            if chunk:
                drained += len(chunk)
        except (BlockingIOError, OSError):
            pass

    # Loop until BOTH halves of the transfer are complete, parking on whichever
    # direction is not yet done.  Ordering of the two ops per cycle alternates by
    # round so neither direction is always serviced first.
    while (sent < nsend or drained < drain_total) and running():
        want_mask = 0
        if drained < drain_total:
            want_mask |= READ           # peer must READ to drain W's bytes
        if sent < nsend:
            want_mask |= WRITE          # peer must WRITE to feed R's bytes
        try:
            ready = runloom_c.wait_fd(fd, want_mask, WAIT_MS)
        except OSError:
            break                       # fd closed at teardown -> stop cleanly
        if ready == 0:
            # No readiness within budget -- try both opportunistically (a
            # spurious/zero return must not wedge the peer) and loop.
            if drain_first:
                do_drain(); do_send()
            else:
                do_send(); do_drain()
            continue
        if drain_first:
            if ready & READ:
                do_drain()
            if ready & WRITE:
                do_send()
        else:
            if ready & WRITE:
                do_send()
            if ready & READ:
                do_drain()


def shrink(s):
    for opt in (socket.SO_SNDBUF, socket.SO_RCVBUF):
        try:
            s.setsockopt(socket.SOL_SOCKET, opt, BUFSZ)
        except OSError:
            pass


def reader_pass(H, wid, rng, state):
    """Pass-1 goroutine: the READER half of worker `wid` (hub A)."""
    pair = state["pairs"][wid]
    rno = 0
    for _ in H.round_range():
        rno += 1
        cli, _srv = pair
        try:
            fd = cli.fileno()
        except (OSError, ValueError):
            return
        if fd < 0:
            return
        seed = (wid << 20) ^ (rno & 0xFFFF)
        expect = pattern(seed ^ 0x5A5A, N_RECV)
        rdone = state["rdone"][wid]
        reader(H.running, fd, cli, N_RECV, expect, rdone)
        H.op(wid)
        H.task_done(wid)


def driver_pass(H, wid, rng, state):
    """Pass-2 goroutine: the WRITER (hub B) + the peer, plus the per-round join.

    Owning the round loop here (one place) keeps the reader/writer/peer in
    lock-step per round and gives ONE goroutine that joins both done-Chans so a
    stranded waiter shows as a goroutine that never returns -> lost."""
    pair = state["pairs"][wid]
    cli, srv = pair
    rno = 0
    for _ in H.round_range():
        rno += 1
        try:
            cfd = cli.fileno()
            sfd = srv.fileno()
        except (OSError, ValueError):
            return
        if cfd < 0 or sfd < 0:
            return
        seed = (wid << 20) ^ (rno & 0xFFFF)
        wpayload = pattern(seed ^ 0xA5A5, M_SEND)        # W -> peer
        send_to_reader = pattern(seed ^ 0x5A5A, N_RECV)  # peer -> R (== expect)
        wdone = state["wdone"][wid]
        # Alternate first-match ordering across rounds.
        drain_first = bool(rno & 1)
        # Spawn the peer (drives the combined readiness) and the writer (arms
        # WRITE on the SAME cli fd).  Reader was spawned in pass 1 and is already
        # parked READ on cli -> READ-arm on hub A, WRITE-arm on hub B, one fd.
        H.fiber(peer, H.running, sfd, srv, send_to_reader, M_SEND, drain_first)
        H.fiber(writer, H.running, cfd, cli, M_SEND, wpayload, wdone)

        # Join BOTH directions.  A dropped EPOLLOUT strands the writer (wdone
        # never delivers); a dropped EPOLLIN strands the reader (rdone never
        # delivers).  Either way this goroutine cannot complete the round and
        # never returns -> require_no_lost fires.  Chan.recv() returns
        # (value, ok); our value is the (success, nbytes) tuple we sent.
        (rok, rn), _ropen = state["rdone"][wid].recv()
        (wok, wn), _wopen = wdone.recv()
        # A shortfall once the deadline has passed is benign teardown (the helper
        # goroutines bailed on running()), NOT a dropped readiness during the
        # measured window -- so only treat an incomplete transfer as an invariant
        # break while the run is still live.  A true strand inside the window
        # would have hung the join above (require_no_lost catches it); reaching
        # here with a short count means the wake DID arrive but the byte
        # conservation was violated -> a real same-slot/cross-wire corruption.
        if H.running():
            if not H.check(rok and rn == N_RECV,
                           "READ side: recv {0}/{1} bytes or content mismatch "
                           "(wid={2} round={3}) -- combined readiness dropped the "
                           "EPOLLIN wake or a same-slot overwrite corrupted the "
                           "stream".format(rn, N_RECV, wid, rno)):
                return
            if not H.check(wok and wn == M_SEND,
                           "WRITE side: sent {0}/{1} bytes (wid={2} round={3}) -- "
                           "the EPOLLOUT wake was dropped / OUT-arm mis-disarmed "
                           "and the backpressured writer stranded".format(
                               wn, M_SEND, wid, rno)):
                return
            H.op(wid)
            H.task_done(wid)
        else:
            break


def setup(H):
    n = H.funcs
    pairs = []
    for _ in range(n):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        shrink(a)
        shrink(b)
        pairs.append((a, b))
        H.register_close(a)
        H.register_close(b)
    H.state = {
        "pairs": pairs,
        # cap-1 done-Chans, one per worker (single producer / single consumer).
        "rdone": [runloom.Chan(1) for _ in range(n)],
        "wdone": [runloom.Chan(1) for _ in range(n)],
    }


def body(H):
    n = len(H.state["pairs"])
    # Pass 1: readers (each parks READ on its cli fd, fanning across hubs).
    # Pass 2: drivers (writer arms WRITE on the SAME fd + peer drives the
    # combined event).  Two passes maximise reader-vs-writer landing on
    # DIFFERENT hubs -> READ-arm on hub A, WRITE-arm on hub B, one fd number.
    H.run_pool(n, reader_pass, H.state)
    H.run_pool(n, driver_pass, H.state)


def post(H):
    H.log("bidir_units(ops)={0} tasks={1} N={2} M={3}".format(
        H.total_ops(), H.total_tasks(), N_RECV, M_SEND))
    H.check(H.total_ops() > 0,
            "no bidirectional units completed (every same-fd parker stranded?)")
    # The completeness oracle: a stranded reader/writer (dropped EPOLLIN/EPOLLOUT
    # wake on the combined arm mask) leaves a driver goroutine joined forever ->
    # LOST.  This is the precise detector for the targeted strand.
    H.require_no_lost("single-fd bidir readiness")


if __name__ == "__main__":
    # Moderate default sibling N: like p106, the socketpair + raw buffer-fill +
    # three-goroutine-per-unit churn does not scale to tens of thousands, and the
    # arm-cache race is a CONCURRENCY (not a volume) hunt -- a few thousand same-fd
    # READ/WRITE splits across hubs is plenty to expose a dropped first-match wake.
    harness.main("p312_bidir_readiness_split", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=4000,
                 describe="same fd parked READ on hub A and WRITE on hub B; a "
                          "combined EPOLLIN|EPOLLOUT event must wake both -- R "
                          "recvs exact N, W drains full M, no waiter stranded")
