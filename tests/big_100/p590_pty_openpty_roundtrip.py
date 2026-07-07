"""big_100 / 590 -- pty.openpty() single-owner round-trip identity under M:N.

The `pty` stdlib module's public workhorse is pty.openpty(): it returns a fresh
(master_fd, slave_fd) pseudo-terminal PAIR.  Bytes written to the master appear,
after the slave tty's line discipline, on the slave for reading; with the slave
put into RAW mode (tty.setraw -> cfmakeraw: ICANON/ECHO/ISIG/OPOST/IXON/ICRNL all
OFF) the discipline is a transparent passthrough, so a byte written to the master
comes back BYTE-IDENTICAL from the slave.  Each pty pair is a private kernel
object owned by exactly ONE fiber -- nothing about it is shared -- so on a correct
runtime the round trip is a pure identity.

WHERE M:N COULD BREAK IT (the gap this program probes).  The interesting hazard
is NOT the pty device (the kernel owns that correctly) but the RUNTIME's
cooperative I/O path: under monkey.patch(), os.read()/os.write() on a tty fd are
turned cooperative, so a read on an EMPTY slave PARKS the fiber on netpoll and is
woken when the master side is written.  This program makes the reader genuinely
park: the worker calls os.read(slave) on an empty pty (parks), and a sibling
fiber it spawned -- scheduled on any hub, GIL off -- then writes THIS worker's
uniquely tagged payload to the master, which must wake exactly THIS parked reader
with exactly THOSE bytes.  A lost wakeup strands the reader (watchdog HANG /
require_no_lost), and a cross-fiber fd delivery -- another worker's bytes arriving
on this worker's slave -- is caught byte-exactly because every payload is tagged
with its own wid and round.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Round-trip identity on a SINGLE-OWNER pty
pair: the exact bytes a worker's own sibling writes to its own master must be the
exact bytes that worker reads back from its own slave.  This holds on plain OS
threads (verified precedent: p33 runs the same write-master/read-tty round trip
through `cat` and asserts echo identity).  Because the pair is private to one
fiber, there is no documented shared-object semantics to muddy it: a mismatch, a
truncation, or a hang can only be a runtime fault (cross-fiber fd delivery, a torn
cooperative os.read/os.write, or a lost netpoll wakeup on the parked slave read).

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP IDENTITY (worker, HARD, fail-fast).  Each round the
    worker opens its OWN pty pair via pty.openpty(), sets the slave raw, builds a
    fiber-local payload tagged "W<wid>R<round>-<random>", spawns a sibling that
    (after a yield, so the reader parks first) writes that payload to the master,
    and reads exactly len(payload) bytes back from the slave across the park.  The
    bytes MUST equal the payload.  Single-owner: the pair is created in a fiber-
    local variable and never shared; the only other fiber that touches it is this
    worker's own writer sibling, writing this worker's own bytes.

  * COMPLETENESS (post, HARD): require_no_lost -- a reader parked on os.read(slave)
    that is never woken (lost netpoll wakeup) never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually completed round trips
    (rt_checks > 0), else the park/wake hazard was never exercised.

FAIL ON: a byte mismatch or truncation on a single-owner pty round trip (a cross-
fiber fd delivery or torn cooperative read/write), or a stranded reader (lost
wakeup on the parked slave read).  There is no shared-mutable arm: the pty pair is
strictly single-owner, so nothing here can mislabel documented Python semantics as
a bug.

RESOURCE CAP: each concurrent worker holds a master+slave fd pair (2 pty devices)
plus a transient writer sibling.  The kernel caps pseudo-terminals at
/proc/sys/kernel/pty/max (default 4096), so max_funcs is set to 512 (<=1024 pty
devices live) to keep the forever-loop's --funcs 1000000 well under the ceiling.

Stresses: pty.openpty() pair creation/teardown churn, cooperative os.read()
parking on an empty pty slave and its netpoll wakeup by a cross-hub sibling write
to the master, byte-exact round-trip identity of a single-owner tty under M:N,
raw-mode line-discipline passthrough, and fd lifecycle (open/close every round).
"""
import os
import sys

# ---- availability guard (POSIX-only: ptys + pollable tty fds) --------------
# The pty module is POSIX-only, and cooperative os.read() parking needs the tty
# fd to be pollable by the netpoll backend (epoll/kqueue on Linux/BSD/mac).
_POSIX = sys.platform.startswith(("linux", "darwin", "freebsd"))
if not _POSIX:
    print("SKIP: POSIX-only (pty/openpty + pollable tty fds unavailable on "
          "{0})".format(sys.platform))
    sys.exit(0)

if not hasattr(os, "openpty"):
    print("SKIP: os.openpty unavailable on this platform")
    sys.exit(0)

import pty
import tty

import harness
import runloom

# Each concurrent worker holds master+slave (2 pty devices); the kernel caps
# ptys at /proc/sys/kernel/pty/max (default 4096).  Cap so <=1024 devices are
# ever live at once, well under the ceiling, and so the forever loop's
# --funcs 1000000 does not try to allocate a million ptys.
MAX_SESSIONS = 512

# Printable-ASCII payload alphabet.  Raw mode already passes arbitrary bytes,
# but restricting the RANDOM body to printable characters (and using only
# printable header bytes) keeps the payload trivially human-readable in a
# failure message and sidesteps any surprise from a stray control byte.
ALPHABET = (bytes(range(48, 58))       # 0-9
            + bytes(range(65, 91))     # A-Z
            + bytes(range(97, 123)))   # a-z


def write_all(fd, data):
    """Write every byte of `data` to `fd` (os.write may write short).  Payloads
    here are small enough (<= ~110 bytes) that a single write returns them all,
    but loop defensively so no byte is ever dropped."""
    view = memoryview(data)
    off = 0
    while off < len(data):
        off += os.write(fd, view[off:])


def read_exact(fd, n):
    """Read EXACTLY n bytes from `fd`, parking cooperatively (monkey os.read) as
    long as the pty slave is empty.  Raises OSError on premature EOF (a closed
    master), which the worker treats as a shutdown-edge condition."""
    buf = bytearray()
    while len(buf) < n:
        chunk = os.read(fd, n - len(buf))
        if not chunk:
            raise OSError("pty slave EOF after {0}/{1} bytes".format(len(buf), n))
        buf += chunk
    return bytes(buf)


def make_payload(wid, rnd, rng):
    """Build this fiber's uniquely tagged payload for this round.  The
    "W<wid>R<round>-" header makes any cross-fiber fd delivery (another worker's
    bytes on this worker's slave) decode to the WRONG wid, and the random body
    gives every round distinct content so a stale-buffer replay is also caught."""
    header = "W{0}R{1}-".format(wid, rnd).encode("ascii")
    body_len = rng.randint(8, 96)
    body = bytes(ALPHABET[rng.randrange(len(ALPHABET))] for _ in range(body_len))
    return header + body


def run_session(H, wid, rnd, rng, state):
    """One single-owner pty round trip.  Open a private pair via pty.openpty(),
    set the slave raw, park the worker on os.read(slave), have a sibling write
    THIS worker's tagged payload to the master (waking the parked reader across
    hubs), and assert the bytes read back are byte-identical."""
    master, slave = pty.openpty()
    try:
        try:
            tty.setraw(slave)              # transparent passthrough: no echo/OPOST
        except Exception:
            pass                           # raw is best-effort; identity still holds

        payload = make_payload(wid, rnd, rng)

        wg = runloom.WaitGroup()
        wg.add(1)

        def writer():
            # Yield first so the worker reaches os.read(slave) and PARKS on the
            # empty pty before we write -- the write is what must wake it.  Always
            # deliver (do NOT gate on H.running): a parked reader must never be
            # left stranded by a writer that bailed out at the shutdown edge.
            try:
                runloom.yield_now()
                write_all(master, payload)
            except OSError:
                pass                       # shutdown edge: fd went bad; swallow
            finally:
                wg.done()

        H.fiber(writer)

        # Parks here on the empty slave until the sibling's master write wakes it.
        got = read_exact(slave, len(payload))
        wg.wait()                          # writer fully retired before we read state

        if got != payload:
            H.fail("pty round-trip CORRUPT wid={0} round={1}: read {2!r} but "
                   "wrote {3!r} -- a cross-fiber fd delivery, a torn cooperative "
                   "os.read/os.write, or a stale pty buffer under M:N".format(
                       wid, rnd, got[:48], payload[:48]))
            return False

        state["rt_checks"][wid & 1023] += 1
        state["rt_bytes"][wid & 1023] += len(got)
        return True
    finally:
        for fd in (slave, master):
            try:
                os.close(fd)
            except OSError:
                pass


def worker(H, wid, rng, state):
    rnd = 0
    for _ in H.round_range():
        if not H.running():
            break
        if not run_session(H, wid, rnd, rng, state):
            return                          # fail-fast: oracle already recorded
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)
        rnd += 1


def setup(H):
    H.state = {
        "rt_checks": [0] * 1024,            # non-vacuity tally (sharded; report)
        "rt_bytes": [0] * 1024,             # bytes round-tripped (sharded; report)
    }


def body(H):
    # Hard PTY resource cap: never spawn more concurrent pty-holding workers than
    # MAX_SESSIONS regardless of --funcs (2 pty devices/worker vs kernel pty/max).
    H.run_pool(H.funcs, worker, H.state, max_concurrent=MAX_SESSIONS)


def post(H):
    checks = sum(H.state["rt_checks"])
    nbytes = sum(H.state["rt_bytes"])
    H.log("pty round-trip identity: {0} single-owner round trips ({1} bytes) "
          "all byte-identical (fail-fast); ops={2}".format(
              checks, nbytes, H.total_ops()))

    # NON-VACUITY: the park/wake round-trip hazard was actually exercised.
    H.check(checks > 0,
            "no pty round trips completed -- the single-owner openpty round-trip "
            "park/wake hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no reader parked-then-vanished on a pty slave read (lost
    # netpoll wakeup) -- would never return.
    H.require_no_lost("pty round-trip identity")


if __name__ == "__main__":
    harness.main(
        "p590_pty_openpty_roundtrip", body, setup=setup, post=post,
        default_funcs=4000, max_funcs=MAX_SESSIONS,
        describe="pty.openpty() gives a private (master, slave) pseudo-terminal "
                 "pair; with the slave in raw mode a byte written to the master "
                 "comes back byte-identical from the slave.  LOAD-BEARING: each "
                 "fiber opens its OWN pair, parks on a cooperative os.read(slave), "
                 "and a sibling writes this fiber's uniquely tagged payload to the "
                 "master -- waking the parked reader across hubs GIL-off.  The "
                 "bytes read back MUST equal the payload; a mismatch (cross-fiber "
                 "fd delivery / torn cooperative read-write) or a stranded reader "
                 "(lost netpoll wakeup) is the runtime bug.  Single-owner pty pair, "
                 "so no shared-object semantics can mislabel a documented behavior "
                 "as a fault")
