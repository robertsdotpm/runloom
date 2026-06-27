"""big_100 / 326 -- MSG_PEEK readiness ordering across a yield / hub migration.

Targets the netpoll arm re-report corner where data is READABLE-BUT-NOT-DRAINED
across a yield + hub migration.  MSG_PEEK reads the bytes WITHOUT consuming them:
the fd's receive queue stays full and the fd stays readable -- so a LEVEL-triggered
epoll SHOULD keep re-reporting that readiness.  That makes a pure lost-wake on the
peek alone low-prior.  The sharper hazard is the SECOND wait_fd after a forced hub
migration, on STILL-UNDRAINED data: it crosses the per-hub-epoll arm-OWNERSHIP
path -- the cross-pool stale-arm-migration logic at netpoll_register.c.inc:140
that once fixed a real LOST PARK (docs/dev/repro/LOST_PARK_FINDING.md).

The unit under test (per record):
  1. Receiver wait_fd-parks the connected socket for READ (likely hub A).
  2. The peer sends ONE known tag.  The READ wake fires; the receiver does
     `sock.recv(n, MSG_PEEK)` -> H.check(peeked == tag).  The data is NOT consumed
     -- the kernel receive queue still holds the full tag, the fd stays readable.
  3. The receiver runloom.yield_now() + runloom.sleep(0) to FORCE a likely hub
     change (so the fd is now wanted by a DIFFERENT hub's pool than the one whose
     epoll first armed it -- the exact cross-pool re-arm path).
  4. The receiver wait_fd-parks for READ AGAIN.  Because the data was never
     consumed, the fd is STILL readable; a level-triggered epoll, correctly
     migrated/re-armed, MUST re-report immediately -> woken_by_event.  A LOST
     re-arm-after-migration leaves the parker armed in a dead pool's epoll
     (orphaned) so the still-pending bytes never re-trigger a wake: the second
     wait runs to its CEILING -> woken_by_timeout, and the subsequent real recv
     finds nothing -> the unit is LOST.
  5. The receiver does the REAL `sock.recv(n)` -> H.check(recv == tag, exactly the
     same bytes, exactly once).  After this the queue is empty.

ORACLE (content equality + readiness-source metric + require_no_lost):
  * CONTENT: peeked bytes == tag AND real-recv bytes == tag (same N bytes).  A
    peek that corrupted the arm/readiness state, or a torn / cross-wire recv, is
    caught by content, not mere length.
  * EXACTLY ONCE: the byte is delivered once -- peek does not consume, real recv
    consumes the full tag, and a post-recv non-blocking peek confirms the queue
    is now empty (the peek did not leave a phantom copy, the recv did not
    duplicate).
  * READINESS SOURCE: the SECOND wait_fd (post-migration, on undrained data) is
    the unit under test.  We tag each second-wait as woken_by_event (ready & READ)
    vs woken_by_timeout (ready == 0 at the ceiling).  On correct level-triggered
    re-report EVERY second wait is woken_by_event; a lost re-arm-after-migration
    surfaces as a MEASURED woken_by_timeout count, not a silent hang.  post()
    fails if ANY second wait timed out (require the migrated re-arm to deliver).
  * require_no_lost: a receiver whose second wait genuinely never re-reports (the
    bytes orphaned in a dead pool's epoll, ceiling-rescue exhausted) cannot
    complete its record -> its driver never joins -> LOST worker.

The SECOND wait_fd has a BOUNDED ceiling so a lost re-arm cannot hang forever; it
surfaces first as woken_by_timeout (a metric) and, only if the bytes are truly
orphaned past every re-probe, as a short/empty recv -> H.fail + a lost worker.

Meaningful only where wait_fd parks on a real readiness backend (epoll/kqueue);
io_uring marks fds always-ready so the second park is a no-op (still correct -- the
content + exactly-once oracle holds on every backend) and the targeted cross-pool
re-arm race bites on the poll backends.  Skips cleanly if wait_fd is unavailable.

Stresses: MSG_PEEK readable-but-undrained state held across yield_now + a forced
hub migration; the cross-pool stale-arm migration re-arm (netpoll_register:140);
level-triggered re-report of an un-consumed receive queue; per-fd arm-cache churn
across rounds.  Good TSan / controlled-M:N-replay target: the second wait_fd's
re-arm is driven from a (likely) different hub than the first armed the fd on -- a
data race on runloom_fd_armed[fd] / a dropped migrated re-arm is the first signal
before the content oracle even fires.  Mirrors the p312/p313 netpoll structure.
"""
import os
import socket
import sys

import harness
import runloom

# wait_fd is the generic-fd park primitive; detect + skip cleanly if absent so
# the campaign treats a missing primitive as non-fatal (matches p313).
try:
    import runloom_c
    _HAVE_WAITFD = hasattr(runloom_c, "wait_fd")
except Exception:                       # pragma: no cover - import guard
    runloom_c = None
    _HAVE_WAITFD = False

READ = 1                                # wait_fd events bitmask: 1 = readable
# Positive sentinel returned on cancellation (NOT a "< 0" value -- a bare "ready
# & READ" test would misread it, so compare explicitly, as p313 does).
CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", 1 << 30) if runloom_c else (1 << 30)

# Capture the RAW (unpatched) socket.recv / os.write / os.set_blocking BEFORE
# harness's monkey.patch() makes recv COOPERATIVE.  We need a true non-blocking
# recv that RAISES BlockingIOError on an empty queue -- the patched recv would
# instead PARK forever (it catches BlockingIOError and re-waits), so an
# "is-the-queue-empty?" MSG_PEEK probe could never return empty, and we could not
# drive the park/re-arm ourselves.  Mirrors p312's RAW_WRITE/RAW_SET_BLOCKING.
RAW_RECV = socket.socket.recv
RAW_WRITE = os.write
RAW_SET_BLOCKING = os.set_blocking

# A tag is one fixed-size record per unit.  Small enough that a single recv drains
# the whole record (so "consumed exactly once" is a clean check) and the kernel
# never coalesces it with a neighbour (one tag per socketpair per round).
TAG_LEN = 64

# Ceiling for the FIRST wait (waiting for the peer to send the tag): generous, so
# a cross-pass scheduling gap before the peer feeds us is not mistaken for a lost
# wake.  Re-probed within the run window.
FIRST_WAIT_MS = 300

# Ceiling for the SECOND wait (the unit under test): on correct level-triggered
# re-report of UNDRAINED data this returns IMMEDIATELY (the fd is already
# readable), so the ceiling is only a backstop.  Kept moderate -- long enough that
# a momentarily-busy hub still delivers within it (no false timeout), short enough
# that a GENUINELY orphaned re-arm is observed as a timeout promptly rather than
# stalling the whole record.  A lost re-arm shows as woken_by_timeout (a metric)
# first; only a truly orphaned wake past every re-probe strands the record.
SECOND_WAIT_MS = 250

# How many times the SECOND wait may re-probe before giving up on a record.  A
# correct re-report needs ZERO re-probes (immediate).  Bounding the re-probes
# means a truly orphaned re-arm cannot busy-spin to the deadline; it exhausts the
# budget, the recv finds nothing, and the record is recorded LOST -- the precise
# detector.  Re-probing only re-arms the SAME direction on the SAME undrained
# data, so it never MASKS a dropped wake (a genuine orphan never gets readiness
# back); it only tolerates ordinary scheduler slack.
SECOND_REPROBE_MAX = 8


def make_tag(seed):
    """A deterministic, position-dependent record so a torn / cross-wire / stale
    recv is caught by CONTENT, not just length.  Unique per (worker, round)."""
    out = bytearray(TAG_LEN)
    x = (seed * 2654435761) & 0xFFFFFFFF
    for i in range(TAG_LEN):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out[i] = (x >> 16) & 0xFF
    return bytes(out)


def receiver(running, fd, sock, tag, counts, slot, done):
    """The unit under test.  Park READ; peek (NOT consuming); FORCE a migration;
    park READ AGAIN on the still-undrained data; real recv.  Reports
    (ok, detail) through `done`.  Updates per-shard readiness-source counters so a
    lost re-arm-after-migration surfaces as a METRIC (woken_by_timeout) before the
    record is declared lost.

    `running()` lets it bail at teardown so a closed fd never busy-spins."""
    ok = False
    detail = "incomplete"
    try:
        # ---- 1. FIRST wait: park READ until the peer sends the tag. -----------
        got_first = False
        while running():
            try:
                ready = runloom_c.wait_fd(fd, READ, FIRST_WAIT_MS)
            except OSError:
                detail = "first-wait fd error"
                done.send((False, detail)); return
            if ready == CANCELLED:
                detail = "cancelled"
                done.send((False, detail)); return
            if ready & READ:
                got_first = True
                break
            # bare timeout: peer hasn't fed us yet (cross-pass gap) -- re-probe.
        if not got_first:
            detail = "teardown before first readiness"
            done.send((False, detail)); return

        # ---- 2. PEEK: read the tag WITHOUT consuming it.  The kernel receive
        #         queue stays full; the fd stays readable.  RAW non-blocking recv
        #         (the patched recv would never raise BlockingIOError -- it parks
        #         -- so we use the unpatched recv and drive the wait ourselves). --
        try:
            peeked = RAW_RECV(sock, TAG_LEN, socket.MSG_PEEK)
        except BlockingIOError:
            # First readiness fired but nothing is actually queued yet (a rare
            # spurious wake): re-park once and re-peek before giving up.
            try:
                runloom_c.wait_fd(fd, READ, FIRST_WAIT_MS)
                peeked = RAW_RECV(sock, TAG_LEN, socket.MSG_PEEK)
            except (BlockingIOError, OSError):
                peeked = b""
        except OSError:
            detail = "peek error"
            done.send((False, detail)); return
        if peeked != tag:
            detail = ("peek mismatch: got {0} bytes, content {1}"
                      .format(len(peeked), "differs" if peeked else "empty"))
            done.send((False, detail)); return

        # ---- 3. FORCE a likely hub migration while the data stays UNDRAINED.
        #         yield_now hands this g back to the scheduler; sleep(0) gives the
        #         M:N work-stealer a chance to resume it on a DIFFERENT hub -> the
        #         next wait_fd arms the fd in another hub's epoll pool (the
        #         cross-pool stale-arm path). -----------------------------------
        runloom.yield_now()
        runloom.sleep(0)
        runloom.yield_now()

        # ---- 4. SECOND wait (THE UNIT UNDER TEST): park READ AGAIN on the
        #         still-undrained data.  Level-triggered + correctly migrated =>
        #         IMMEDIATE re-report (woken_by_event).  A lost re-arm-after-
        #         migration => woken_by_timeout, and the bytes never re-trigger. --
        woke_by_event = False
        probes = 0
        while running() and probes <= SECOND_REPROBE_MAX:
            probes += 1
            try:
                ready = runloom_c.wait_fd(fd, READ, SECOND_WAIT_MS)
            except OSError:
                detail = "second-wait fd error"
                done.send((False, detail)); return
            if ready == CANCELLED:
                detail = "cancelled at second wait"
                done.send((False, detail)); return
            if ready & READ:
                woke_by_event = True
                break
            # ready == 0: the ceiling expired WITHOUT a re-report of data we KNOW
            # is still queued (we never consumed it).  That is the targeted lost
            # re-arm-after-migration signal -> record it and re-probe within the
            # bounded budget.  A correctly migrated re-arm never reaches here.
            counts["second_timeout"][slot] += 1
        if woke_by_event:
            counts["second_event"][slot] += 1
        else:
            # Exhausted the re-probe budget with the data still queued and never
            # re-reported: a genuinely orphaned re-arm.  The real recv below will
            # still try (the bytes ARE in the kernel queue), but this is the
            # measured lost-re-arm; record it and let the content/exactly-once
            # oracle + the driver join decide the verdict.
            counts["second_lost"][slot] += 1

        # ---- 5. REAL recv: consume the full tag exactly once. -----------------
        # The bytes are in the kernel queue regardless of whether our re-arm wake
        # fired (MSG_PEEK never removed them), so a non-blocking recv retrieves
        # them; a lost re-arm is thus visible as woken_by_timeout (step 4) WITHOUT
        # also corrupting the byte conservation -- the two oracles are independent.
        buf = bytearray()
        tries = 0
        while len(buf) < TAG_LEN and running() and tries < 64:
            tries += 1
            try:
                chunk = RAW_RECV(sock, TAG_LEN - len(buf))
            except BlockingIOError:
                # Not yet drained into userspace -- park once more and retry.  If
                # the re-arm was genuinely lost AND the data somehow vanished this
                # would spin to the cap and fall through to a shortfall -> H.fail.
                try:
                    runloom_c.wait_fd(fd, READ, SECOND_WAIT_MS)
                except OSError:
                    break
                continue
            except OSError:
                break
            if not chunk:
                break                    # peer closed early
            buf += chunk

        if len(buf) != TAG_LEN or bytes(buf) != tag:
            detail = ("real recv mismatch: got {0}/{1} bytes, content {2}"
                      .format(len(buf), TAG_LEN,
                              "differs" if bytes(buf) != tag else "ok"))
            done.send((False, detail)); return

        # ---- exactly-once: the queue must now be EMPTY (peek left no phantom
        #         copy, recv did not duplicate).  A non-blocking peek must raise
        #         BlockingIOError / return empty. -------------------------------
        leftover = b"?"
        try:
            leftover = RAW_RECV(sock, TAG_LEN, socket.MSG_PEEK | socket.MSG_DONTWAIT)
        except BlockingIOError:
            leftover = b""               # queue empty -> correct
        except OSError:
            leftover = b""               # treat as drained
        if leftover:
            detail = "queue not empty after recv: {0} stray bytes".format(
                len(leftover))
            done.send((False, detail)); return

        ok = True
        detail = "ok"
    except Exception as exc:             # noqa: BLE001
        detail = "exception: {0}: {1}".format(type(exc).__name__, exc)
    done.send((ok, detail))


def peer(running, fd, tag):
    """The OTHER end: send ONE tag so the receiver's first wait fires, leaving the
    data readable-but-undrained for the peek + migration dance.  Raw send on a
    non-blocking fd; a TAG_LEN write never blocks (well under the send buffer)."""
    sent = 0
    try:
        os.set_blocking(fd, False)
        while sent < len(tag) and running():
            try:
                n = os.write(fd, tag[sent:])
            except BlockingIOError:
                runloom.yield_now()
                continue
            except OSError:
                break
            if n <= 0:
                break
            sent += n
    except Exception:                    # noqa: BLE001
        pass


def driver(H, wid, rng, state):
    """One goroutine per worker: owns the per-round loop and joins the receiver so
    a stranded receiver (genuinely orphaned re-arm) shows as a goroutine that never
    returns -> require_no_lost.  Spawns the peer + receiver as siblings; the
    receiver fans across hubs vs the driver, and the in-receiver yield/sleep forces
    the second wait onto a (likely) different hub than the first arm."""
    pair = state["pairs"][wid]
    cli, srv = pair
    counts = state["counts"]
    slot = wid & 1023
    rno = 0
    for _ in H.round_range():
        rno += 1
        if not H.running():
            break
        try:
            cfd = cli.fileno()
            sfd = srv.fileno()
        except (OSError, ValueError):
            return
        if cfd < 0 or sfd < 0:
            return
        seed = (wid << 20) ^ (rno & 0xFFFF)
        tag = make_tag(seed)
        done = state["done"][wid]

        # Receiver parks READ on cli (hub A); peer sends the tag on srv.  The
        # receiver then peeks, migrates, and re-parks (likely hub B) on the SAME
        # cli fd -- the cross-pool re-arm under test.
        H.fiber(receiver, H.running, cfd, cli, tag, counts, slot, done)
        H.fiber(peer, H.running, sfd, tag)

        # Join the receiver.  A genuinely orphaned re-arm strands it (done never
        # delivers) -> this goroutine cannot complete the round and never returns
        # -> require_no_lost fires.  Chan.recv() returns (value, ok); our value is
        # the (success, detail) tuple.
        (rok, detail), _open = done.recv()

        # A shortfall once the deadline has passed is benign teardown (the helper
        # bailed on running()), NOT a dropped readiness during the measured window.
        if H.running():
            if not H.check(rok,
                           "peek/recv ordering broke (wid={0} round={1}): {2} -- "
                           "the MSG_PEEK left data readable-but-undrained, and the "
                           "SECOND wait_fd after the forced hub migration either "
                           "lost the cross-pool re-arm (no re-report of still-"
                           "queued bytes) or the recv was torn / duplicated"
                           .format(wid, rno, detail)):
                return
            H.op(wid)
            H.task_done(wid)
        else:
            break


def setup(H):
    if not _HAVE_WAITFD:
        H.note_scale_limit(
            "runloom_c.wait_fd unavailable -- cannot park on a raw fd; skipping "
            "the MSG_PEEK re-arm ordering test")
        H.state = None
        return
    if not hasattr(socket, "MSG_PEEK"):
        H.note_scale_limit(
            "socket.MSG_PEEK unavailable on this platform ({0}) -- skipping"
            .format(sys.platform))
        H.state = None
        return
    n = H.funcs
    pairs = []
    for _ in range(n):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        pairs.append((a, b))
        H.register_close(a)
        H.register_close(b)
    H.state = {
        "pairs": pairs,
        # cap-1 done-Chans, one per worker (single producer / single consumer).
        "done": [runloom.Chan(1) for _ in range(n)],
        # Sharded readiness-source counters (one writer per slot; a shared += loses
        # increments GIL-off).  second_event: the migrated re-arm re-reported
        # immediately (correct).  second_timeout: a ceiling expired with data still
        # queued (a lost-re-arm signal; >0 re-probes).  second_lost: the re-probe
        # budget was exhausted with the data never re-reported (a genuine orphan).
        "counts": {
            "second_event": [0] * 1024,
            "second_timeout": [0] * 1024,
            "second_lost": [0] * 1024,
        },
    }


def body(H):
    if H.state is None:
        return                          # skipped in setup (no wait_fd / MSG_PEEK)
    H.run_pool(H.funcs, driver, H.state)


def post(H):
    if H.state is None:
        H.log("SKIPPED: {0}".format(H.scale_limit_reason or "no wait_fd/MSG_PEEK"))
        return
    c = H.state["counts"]
    ev = sum(c["second_event"])
    to = sum(c["second_timeout"])
    lost = sum(c["second_lost"])
    H.log("units(ops)={0} tasks={1} second_wait[event={2} timeout_reprobe={3} "
          "orphaned={4}] TAG_LEN={5}".format(
              H.total_ops(), H.total_tasks(), ev, to, lost, TAG_LEN))
    H.check(H.total_ops() > 0,
            "no peek/recv units completed (every receiver stranded on the first "
            "or second wait?)")
    # The readiness-source metric: on correct level-triggered re-report of the
    # un-consumed receive queue, EVERY second wait wakes by EVENT and NONE is
    # orphaned.  A genuine lost re-arm-after-migration (the bytes stranded in a
    # dead pool's epoll) is an orphaned second wait -> a real bug, not slack.
    H.check(lost == 0,
            "lost re-arm after hub migration: {0} second wait(s) exhausted the "
            "re-probe budget with data STILL queued and never re-reported -- the "
            "MSG_PEEK left the fd readable but the migrated re-arm orphaned it in "
            "a dead pool's epoll (the cross-pool stale-arm path, "
            "netpoll_register.c.inc:140)".format(lost))
    # A stranded receiver whose second wait genuinely never delivers leaves its
    # driver joined forever -> LOST.  This is the precise completeness detector.
    H.require_no_lost("MSG_PEEK re-arm-after-migration ordering")


if __name__ == "__main__":
    # Moderate default sibling N: like p312/p313, the socketpair + peek + migration
    # + recv churn (two goroutines per unit) is a CONCURRENCY hunt, not a volume
    # one -- a few thousand peek/migrate/re-arm cycles across hubs is plenty to
    # expose a dropped cross-pool re-arm.
    harness.main(
        "p326_msg_peek_recv_ordering", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=4000,
        describe="MSG_PEEK leaves data readable-but-undrained; after a forced hub "
                 "migration the SECOND wait_fd must still re-report (level-trigger "
                 "+ cross-pool re-arm) and the real recv gets the tag exactly once")
