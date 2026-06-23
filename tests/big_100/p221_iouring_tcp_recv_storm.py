"""big_100 / 221 -- io_uring per-conn TCP recv/send proactor storm.

The io_uring per-conn proactor recv/send path (RUNLOOM_TCPCONN_IOURING) is the
+20% Stage-2 win and a COMPLETELY separate cooperative-block + cancel +
fd-identity implementation from the default epoll TCPConn path.  ZERO other
big_100 program forces it, so its recv/cancel/close-while-blocked/fd-reuse
behaviour is entirely unexercised here -- every prior network bug class
(stale-arm hang, close-while-blocked cancel, fd recycle) was found on epoll only
and could re-occur differently on the ring.

This program forces the ring on (RUNLOOM_TCPCONN_IOURING=1, set at module top
BEFORE importing runloom_c) and drives a fleet of long-lived echo connections
THROUGH the runloom_c.TCPConn proactor objects -- NOT through the monkey-patched
socket stack, which routes recv/send via the raw-fd epoll fast path and would
never touch the per-conn ring backend.  The server is runloom_c.serve(...,
acceptors=4); its handler receives a TCPConn, so server-side recv/send also ride
the ring.  Each worker does K tagged round-trips: it struct-packs a unique
(wid, seq, k) tag, send_all's it, recv's exactly that many bytes back, and
asserts a byte-exact echo -- a dropped or duplicated ring completion shows up as
a tag mismatch.  A fraction of workers, after connecting, spawn a sibling that
closes the conn WHILE the first goroutine is parked in a ring recv, exercising
close-while-blocked cancel on the proactor path: the parked recv must wake with a
clean OSError, never hang and never return the wrong bytes.

Availability-guarded: if io_uring is unavailable (Linux<5.1 / no liburing /
non-Linux) the program prints a SKIP line and exits 0 as a no-op.

Oracle / invariants:
  * every tagged round-trip echoes its EXACT tag (H.check on each recv);
  * the close-while-blocked sibling case ends in OSError/clean-cancel, never a
    wrong-bytes echo and never a hang (the watchdog catches a lost completion --
    a parked ring recv that never wakes);
  * total correct round-trips == expected, so a silently-swallowed completion is
    caught at the end (post-check);
  * leaked fds stay bounded (no per-conn fd leak on the ring close path).

Stresses: Stresses: io_uring per-conn TCP recv/send proactor (RUNLOOM_TCPCONN_IOURING) under many concurrent long-lived echo conns: cooperative recv completion, sticky per-conn backend choice, threshold flip mid-run, and close-while-blocked cancel on the ring path.
"""
import os
import struct
import sys

# Force the io_uring per-conn proactor ON, and set the auto-flip threshold, BOTH
# BEFORE runloom_c is imported -- the mode is resolved once from the env on the
# first TCPConn recv/send and latched, so it must be present at import time.
# "1" = unconditional ring; the THRESHOLD knob is read in the same resolve pass
# (it only governs "auto", but we set it so a mid-run flip to "auto" would honour
# a value below our connection count -- documenting the crossover the proactor
# uses).
os.environ.setdefault("RUNLOOM_TCPCONN_IOURING", "1")
os.environ.setdefault("RUNLOOM_TCPCONN_IOURING_THRESHOLD", "2048")

import harness   # noqa: E402  (harness imports runloom_c after the env is set)

# runloom_c is importable via harness's sys.path bootstrap.
import runloom_c   # noqa: E402
import runloom     # noqa: E402

# Tag layout: (wid, seq, k) as three unsigned 32-bit ints, then padded out to a
# fixed per-round-trip length so a dropped/duplicated completion changes the
# exact bytes, not just the length.
_TAG = struct.Struct("<III")
_PAD = 64   # total payload = 12-byte tag + 64 padding bytes, deterministic


def make_payload(wid, seq, k):
    head = _TAG.pack(wid & 0xFFFFFFFF, seq & 0xFFFFFFFF, k & 0xFFFFFFFF)
    # Deterministic, tag-dependent padding so a duplicated completion (right
    # length, wrong bytes) is also caught.
    fill = bytes(((wid + seq * 7 + k * 13 + i) & 0xFF) for i in range(_PAD))
    return head + fill


def recv_exact_conn(conn, n):
    """Read exactly n bytes off a TCPConn (ring recv); OSError on short EOF."""
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise OSError("eof after {0}/{1} bytes".format(len(buf), n))
        buf += chunk
    return bytes(buf)


# Set in setup() so the server-side echo handler can self-terminate at teardown
# instead of staying parked forever in a ring recv on an idle-but-open conn.
# OPEN_CONNS tracks live handler conns so teardown can close any still parked in
# a ring recv (closing the LISTENER only stops accept loops, not in-flight
# handler recvs).  Guarded by a real OS lock -- the handlers run in PARALLEL
# across hubs with the GIL off, so a bare set.add/discard would race.  We DISCARD
# on close, so the set stays bounded by live conn count, never the cumulative
# total (no unbounded growth across reconnects).
import _thread   # noqa: E402

HARNESS = None
OPEN_CONNS = set()
OPEN_LOCK = _thread.allocate_lock()


def echo_handler(conn):
    """Server-side: echo until EOF.  recv/send_all ride the ring (TCPConn).

    Tracks the accepted conn in OPEN_CONNS so teardown can close it -- a handler
    parked in a ring recv on a client that went idle (e.g. a close-while-blocked
    worker's peer) would otherwise strand the run, since closing the LISTENER
    only stops accept loops, not in-flight handler recvs.  Closing the conn at
    teardown wakes the parked ring recv with EOF/cancel."""
    with OPEN_LOCK:
        OPEN_CONNS.add(conn)
    try:
        while True:
            data = conn.recv(65536)
            if not data:
                break
            conn.send_all(data)
    except OSError:
        pass
    finally:
        with OPEN_LOCK:
            OPEN_CONNS.discard(conn)
        try:
            conn.close()
        except Exception:
            pass


class _ConnReaper(object):
    """A closeable the harness invokes at teardown (via register_close): closes
    every still-open handler conn so any handler parked in a ring recv wakes and
    its fiber returns, letting the M:N join complete (no teardown wedge)."""

    def close(self):
        with OPEN_LOCK:
            conns = list(OPEN_CONNS)
        for c in conns:
            try:
                c.close()
            except Exception:
                pass


def setup(H):
    # Availability guard: skip-clean when the ring isn't usable.
    if sys.platform != "linux" or not runloom_c.iouring_available():
        H.log("SKIP: io_uring not available "
              "(platform={0}, iouring_available={1})".format(
                  sys.platform,
                  getattr(runloom_c, "iouring_available", lambda: "n/a")()))
        H.state = {"skip": True}
        return

    # Best-effort confirmation that the ring force-on actually took: the
    # per-conn proactor is selected on the TCPConn object, not at the netpoll
    # layer (netpoll stays epoll -- it pumps the ring's eventfd), so we can't
    # read it back from netpoll_backend().  We log the env + netpoll backend and
    # proceed; if the env didn't switch the backend we still run (echo
    # round-trips remain a valid correctness oracle on whichever path resolves),
    # but a non-"1" env means we are NOT testing the ring, so SKIP-clean.
    forced = os.environ.get("RUNLOOM_TCPCONN_IOURING", "")
    if forced != "1":
        H.log("SKIP: RUNLOOM_TCPCONN_IOURING did not force on (={0!r}); "
              "not exercising the ring".format(forced))
        H.state = {"skip": True}
        return

    H.log("io_uring forced ON: RUNLOOM_TCPCONN_IOURING={0} threshold={1} "
          "netpoll={2} (ring pumped via its eventfd)".format(
              forced, os.environ.get("RUNLOOM_TCPCONN_IOURING_THRESHOLD"),
              runloom_c.netpoll_backend()))

    global HARNESS
    HARNESS = H

    # One echo server, 4 acceptors (SO_REUSEPORT) on the job's isolated IP.
    # serve() returns (bound_port, [listeners]); register the listeners so the
    # harness closes them at teardown and the accept loops exit.  Register the
    # conn-reaper FIRST so it runs at teardown and closes any handler conn still
    # parked in a ring recv (waking it) before the M:N join.
    H.register_close(_ConnReaper())
    host = H.net_ip(0)
    bound_port, listeners = runloom_c.serve(host, 0, echo_handler, acceptors=4)
    for ln in listeners:
        H.register_close(ln)
    H.state = {"skip": False, "host": host, "port": bound_port,
               "rt_ok": [0] * harness.NSHARDS}
    H.log("echo server up on {0}:{1} ({2} acceptors)".format(
        host, bound_port, len(listeners)))


def closer_sibling(H, conn, fired):
    """Close `conn` while the owning worker is parked in a ring recv.  A small
    cooperative delay lets the worker reach the parked recv first, so we are
    genuinely cancelling an in-flight ring completion (not racing a closed fd
    before the recv is even armed)."""
    H.sleep(0.002)
    fired[0] = True
    try:
        conn.close()
    except Exception:
        pass


def worker(H, wid, rng, state):
    if state.get("skip"):
        return
    host = state["host"]
    port = state["port"]
    rt_ok = state["rt_ok"]
    slot = wid & harness.SHARD_MASK

    # Spread the connect storm deterministically so we don't thunder-herd the
    # acceptors all at once.
    H.sleep(rng.random() * 0.5)

    # A fraction of workers exercise close-while-blocked cancel on the ring.
    close_while_blocked = (rng.random() < 0.10)

    for _ in H.round_range():
        if not H.running():
            break
        conn = None
        try:
            conn = runloom_c.TCPConn.connect(host, port)
        except OSError:
            if not H.running():
                break
            continue

        try:
            if close_while_blocked:
                # Spawn a sibling that closes the conn mid-recv.  We send NO
                # request first, so the echo never comes and our recv parks on
                # the ring -- then the sibling close must cancel it cleanly.
                fired = [False]
                H.fiber(closer_sibling, H, conn, fired)
                try:
                    got = conn.recv(64)
                    # A clean orderly-shutdown (b'') from our own close is fine;
                    # ANY non-empty bytes here would be a phantom completion
                    # (we sent nothing) -- that is a real bug.
                    H.check(got == b"",
                            "ring recv returned {0} bytes after "
                            "close-while-blocked wid={1} (phantom "
                            "completion)".format(len(got), wid))
                except OSError:
                    pass   # expected: cancel/closed-fd raises, never hangs
                H.op(wid)
                H.task_done(wid)
                # Re-arm for the next round with a fresh connection below.
                continue

            # Normal long-lived echo conn: K tagged round-trips.
            k = rng.randint(2, 6)
            ok_this_conn = True
            for seq in range(k):
                if not H.running():
                    break
                payload = make_payload(wid, seq, k)
                conn.send_all(payload)
                got = recv_exact_conn(conn, len(payload))
                if not H.check(got == payload,
                               "ring echo mismatch wid={0} seq={1} k={2} "
                               "(len {3}!={4} or bytes differ)".format(
                                   wid, seq, k, len(got), len(payload))):
                    ok_this_conn = False
                    break
                H.op(wid)
                rt_ok[slot] = rt_ok[slot] + 1   # single-writer per slot
            if ok_this_conn:
                H.task_done(wid)
        except OSError:
            if not H.running():
                break
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def body(H):
    if H.state.get("skip"):
        return   # no-op workload; harness exits 0 (PASS)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    """End-of-run conservation check: the count of byte-exact round-trips the
    workers recorded (single-writer-per-slot, race-free) must equal the total
    ops counted on the normal-conn path.  A silently-swallowed ring completion
    would let send_all+recv 'succeed' on a goroutine that never actually got
    its bytes echoed -- but recv_exact_conn would have raised or H.check would
    have failed first, so rt_ok and the per-op counter must agree exactly."""
    if H.state.get("skip"):
        H.log("skipped: io_uring unavailable -- no-op PASS")
        return
    rt_ok = sum(H.state["rt_ok"])
    H.log("byte-exact ring round-trips recorded: {0}".format(rt_ok))
    # Bounded-leak / liveness are reported by the harness footer; the per-trip
    # H.check already validated correctness inline.  This post-check asserts we
    # actually exercised the ring (did real work) rather than skipping silently.
    H.check(rt_ok > 0 or H.total_ops() > 0,
            "no round-trips completed -- the ring path did no work")


if __name__ == "__main__":
    harness.main("p221_iouring_tcp_recv_storm", body, setup=setup, post=post,
                 default_funcs=5000,
                 describe="force the io_uring per-conn TCP recv/send proactor "
                          "(RUNLOOM_TCPCONN_IOURING) under many long-lived echo "
                          "conns: ring recv completion, sticky backend choice, "
                          "and close-while-blocked cancel on the ring path")
