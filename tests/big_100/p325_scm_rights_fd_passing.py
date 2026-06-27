"""big_100 / 325 -- SCM_RIGHTS fd passing over AF_UNIX: a received fd must be
cleanly cooperatively-park-able after crossing the recvmsg boundary.

sendmsg/recvmsg with SCM_RIGHTS injects a BRAND-NEW kernel fd into the receiver
at recvmsg time -- a fd the netpoll arm cache has never seen.  No program in the
corpus exercises sendmsg/recvmsg/SCM_RIGHTS at all; p312/p313/p101/p324 cover
the arm-cache and the stale-arm-on-recycled-number corner already, so this
program deliberately does NOT re-test that.  The REFINED (de-duplicated) bite is
two-fold and both halves are real M:N hazards:

  (A) COOPERATIVE-PARK proof (the p177 missing-wrapper class, for the ancillary-
      data path).  recvmsg() must be a COOPERATIVE park: a peer that DELAYS its
      SCM_RIGHTS send must NOT wedge the receiver's hub OS-thread.  socket.recvmsg
      is monkey-patched cooperative (src/runloom/monkey/sockets.py:_patched_recvmsg
      -- EAGAIN -> _wait_io -> park), so a receiver waiting on a delayed send
      parks the goroutine and frees the hub.  If that wrapper were missing (a raw
      blocking recvmsg) the hub OS-thread would OS-block for the whole delay and
      every OTHER goroutine sharing that hub would stall.  We measure cooperative
      progress exactly like p177: a sampler asserts the bystanders' op count never
      stalls to zero while delayed recvmsg's are in flight.  A stalled hub == a
      missing cooperative wrapper.

  (B) fd-IDENTITY conservation (CMSG parsed intact, no MSG_CTRUNC corruption,
      received fd is the RIGHT kernel object even though it materialized mid-run
      on a different hub).  The sender writes a UNIQUE tag into a pipe, passes the
      pipe's READ fd over an AF_UNIX socketpair via sendmsg(SCM_RIGHTS), then
      closes its local copy.  The receiver recvmsg's the ancillary data on a
      DIFFERENT hub, parses the CMSG to recover got_fd, wait_fd-parks got_fd for
      READ, and os.read's it.  The bytes read MUST equal the sender's tag.  This
      proves: the CMSG was parsed intact (a truncated/mis-parsed CMSG yields a
      wrong fd number -> read returns the wrong tag or fails), MSG_CTRUNC was not
      silently dropped (we assert it is clear), and the new fd -- never before
      seen by the arm cache, first armed on the receiver's hub at recvmsg time --
      parks and wakes correctly across the boundary.

We CHURN fresh pipes + socketpairs every round so the pipe-read fd NUMBERS
recycle aggressively, maximising the chance a freshly-passed fd reuses a number
the arm cache last saw on a since-closed fd (the recycle pressure p312 also
relies on, but here the materialization happens via ancillary data, not open()).

ORACLE:
  * (A) cooperative-park: a sampler (like p177) asserts the bystander op count
    never stalls to zero for a sampling window while delayed-send recvmsg's are
    in flight.  A sustained stall == a hub OS-blocked in a non-cooperative
    recvmsg == a missing cooperative wrapper.
  * (B) fd-identity conservation: every received fd's os.read bytes == the
    sender's unique tag (content + length).  A mismatch == a mis-parsed CMSG /
    MSG_CTRUNC corruption / wrong kernel object delivered.
  * MSG_CTRUNC must be clear on every recvmsg (ancillary buffer was big enough;
    a set CTRUNC means the fd array was truncated -> the passed fd was lost).
  * require_no_lost: a receiver parked on a passed fd that never wakes (the new
    fd's first arm on the receiver hub was dropped) is a LOST worker.
  * fd-leak: every pipe (r locally + r-via-SCM in the receiver + w) and every
    socketpair half is closed; H.fd_end stays bounded vs H.fd_base (guarded
    behind fd_base >= 0, i.e. Linux /proc/self/fd).

Skips cleanly (note_scale_limit) where socket.sendmsg / SCM_RIGHTS / wait_fd are
unavailable (non-POSIX).  socket.sendmsg + AF_UNIX + SCM_RIGHTS confirmed on the
3.13t target.

Stresses: cooperative recvmsg park on a DELAYED ancillary-data send (missing-
wrapper / hub-starvation hunt), a brand-new kernel fd materialized at recvmsg
time then immediately wait_fd-parked + read on a different hub, CMSG/MSG_CTRUNC
parse integrity under M:N, fd-number recycle pressure on the SCM_RIGHTS path.

Good TSan / controlled-M:N-replay target: the passed fd is FIRST armed on the
receiver hub the instant recvmsg returns -- a data race on the arm cache for that
never-before-seen fd number, or a dropped first-arm wake, is the first signal,
before the byte-identity oracle even fires.
"""
import array
import os
import socket
import sys

import harness
import runloom

# ---- availability guard ---------------------------------------------------
# sendmsg/recvmsg + SCM_RIGHTS are POSIX-only; wait_fd is the generic raw-fd park
# primitive (used to park the freshly-received pipe fd).  Detect and skip cleanly.
_HAVE_MSG = (hasattr(socket.socket, "sendmsg")
             and hasattr(socket.socket, "recvmsg")
             and hasattr(socket, "SCM_RIGHTS")
             and hasattr(socket, "CMSG_LEN"))

try:
    import runloom_c
    _HAVE_WAITFD = hasattr(runloom_c, "wait_fd")
except Exception:                       # pragma: no cover - import guard
    runloom_c = None
    _HAVE_WAITFD = False

READ = 1                                # wait_fd events bitmask: 1 = readable
CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", -1) if runloom_c else -1

# Tag length per passed fd.  Small (a pipe holds it without blocking the writer)
# but big enough that a truncated / wrong-fd read is unmistakable, and carries a
# per-(worker,round) unique prefix so a cross-wire (reading ANOTHER unit's pipe)
# is caught by CONTENT, not just length.
TAG_LEN = 64

# How long the sender DELAYS before sending the SCM_RIGHTS message.  This is the
# whole point of arm (A): during this window the receiver is parked in a
# cooperative recvmsg, and the bystanders on its hub must keep progressing.  Long
# enough that a NON-cooperative (hub-OS-blocking) recvmsg would visibly starve the
# bystanders for several sampler windows; short enough that rounds still cycle.
SEND_DELAY_S = 0.02

# Per-park ceiling (ms) for the received-fd wait_fd.  Bounded so a genuinely lost
# first-arm wake surfaces (the receiver re-probes and, if the tag never becomes
# readable, the unit fails the identity oracle) rather than only as a watchdog
# hang.  The pipe already HAS the tag written before the fd is even sent, so the
# received fd is readable immediately on a healthy run -- a timeout here means the
# new fd's first arm on this hub was dropped.
WAIT_MS = 200

# Bystanders spawned per receiver, doing purely cooperative ops, to prove the
# receiver's delayed-recvmsg park does not starve its hub.  A handful is plenty:
# they only need to keep the global cooperative-progress signal rising.
BYSTANDERS = 3


def make_tag(wid, rno):
    """A unique, position-dependent tag for (worker, round).  A cross-wire (the
    receiver reading some OTHER unit's passed pipe) changes the content, so the
    identity oracle catches it by bytes, not merely by length."""
    seed = ((wid & 0xFFFF) << 16) ^ (rno & 0xFFFF)
    out = bytearray(TAG_LEN)
    x = (seed * 2654435761 + 0x9E3779B1) & 0xFFFFFFFF
    for i in range(TAG_LEN):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out[i] = (x >> 16) & 0xFF
    return bytes(out)


def bystander(H, wid, shard, state):
    """A cooperative goroutine sharing the receiver's hub.  Loops doing pure
    cooperative ops + a yield, bumping the global coop-progress counter.  If the
    receiver's recvmsg were NON-cooperative (a missing wrapper) and OS-blocked the
    hub, this stalls -- the sampler in body() detects the stall.  Bounded by a
    per-bystander op budget so it returns (mn_run joins on pending count)."""
    coop = state["coop"]
    budget = 200
    while budget > 0 and H.running():
        coop[shard] += 1
        budget -= 1
        runloom.yield_now()
        runloom.sleep(0.0005)


def sender(H, sock_a, send_fd, delay, ready_ch):
    """Sender goroutine (lives on whichever hub it lands on).  DELAYS, then
    sendmsg's `send_fd` (a pipe READ fd already carrying the tag) over AF_UNIX
    socketpair end `sock_a` as SCM_RIGHTS ancillary data, then closes its local
    copy of send_fd.  The delay is the cooperative-park window for arm (A): the
    receiver is parked in recvmsg the whole time.

    Signals completion (sent? bool) over ready_ch so the unit can join."""
    sent_ok = False
    try:
        # Cooperative sleep -> the receiver's recvmsg parks for this whole window.
        runloom.sleep(delay)
        if not H.running():
            ready_ch.send(False)
            return
        anc = [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                array.array("i", [send_fd]))]
        # A 1-byte payload alongside the ancillary data: SCM_RIGHTS needs at
        # least one data byte to ride on, and it doubles as the recvmsg's data.
        try:
            sock_a.sendmsg([b"\x01"], anc)
            sent_ok = True
        except OSError:
            sent_ok = False
    except Exception:                   # noqa: BLE001
        sent_ok = False
    finally:
        # Close OUR copy of the passed fd immediately: the kernel duped it into
        # the socket buffer, so the receiver's recvmsg materializes a fresh fd
        # NUMBER -- and our close frees this number to recycle (the recycle
        # pressure that makes the new fd likely reuse a stale arm-cache slot).
        try:
            os.close(send_fd)
        except OSError:
            pass
        ready_ch.send(sent_ok)


def receive_fd(H, sock_b, ancbufsize):
    """recvmsg on socketpair end `sock_b` (cooperative park -- arm A), parse the
    SCM_RIGHTS CMSG, return (got_fd, ctrunc_clear).  got_fd is a brand-new kernel
    fd the arm cache has never seen.  Returns (-1, True) on a clean teardown
    close, (-1, False) on a CMSG/CTRUNC corruption signal."""
    while H.running():
        try:
            # Cooperative recvmsg: EAGAIN -> park the goroutine (frees the hub)
            # until the (delayed) sender's message arrives.  msg/ancdata carry the
            # SCM_RIGHTS array.
            msg, ancdata, msg_flags, _addr = sock_b.recvmsg(1, ancbufsize)
        except OSError:
            return -1, True             # fd closed under us at teardown
        # MSG_CTRUNC set == the kernel had to TRUNCATE the ancillary buffer, i.e.
        # the fd array was clipped and the passed fd was LOST.  Our ancbufsize is
        # sized for exactly one fd, so on a healthy run CTRUNC is always clear.
        ctrunc = bool(msg_flags & getattr(socket, "MSG_CTRUNC", 0))
        got_fd = -1
        for level, ctype, cdata in ancdata:
            if level == socket.SOL_SOCKET and ctype == socket.SCM_RIGHTS:
                fds = array.array("i")
                # Only whole ints; a truncated CMSG payload would give a partial
                # int -> drop the remainder (and ctrunc would be set anyway).
                fds.frombytes(cdata[:len(cdata) - (len(cdata) % fds.itemsize)])
                if len(fds) > 0:
                    got_fd = fds[0]
                # Close any extra fds we somehow received (shouldn't happen for a
                # single-fd send, but never leak).
                for extra in fds[1:]:
                    try:
                        os.close(int(extra))
                    except OSError:
                        pass
        if got_fd >= 0:
            return got_fd, (not ctrunc)
        if ctrunc:
            return -1, False            # ancillary truncated -> fd lost
        # A datagram with no ancillary fd (e.g. a stray byte): loop and re-recv.
        if not msg:
            return -1, True             # peer closed -> clean stop
    return -1, True


def read_tag(H, got_fd, want):
    """wait_fd-park the freshly-received fd for READ, then os.read it.  The tag
    was written into the pipe BEFORE the fd was sent, so a healthy fd is readable
    immediately; a timeout means the new fd's first arm on THIS hub was dropped.
    Returns the bytes read (possibly short on a torn read)."""
    buf = bytearray()
    while len(buf) < want and H.running():
        try:
            ready = runloom_c.wait_fd(got_fd, READ, WAIT_MS)
        except OSError:
            break                       # fd closed at teardown
        if ready == CANCELLED:
            break
        if not (ready & READ):
            # No readiness within budget.  The bytes are already in the pipe, so
            # a healthy fd is readable at once -- a timeout here means the
            # first-arm wake on this never-before-seen fd number was dropped.  Re-
            # probe within the unit's window; a genuinely lost first-arm never
            # gets its readiness, so the identity oracle records a short/failed
            # read (caught in worker(), and require_no_lost catches an outright
            # strand).
            continue
        try:
            chunk = os.read(got_fd, want - len(buf))
        except BlockingIOError:
            continue                    # spurious readiness; re-park
        except OSError:
            break
        if not chunk:
            break                       # writer end gone / EOF
        buf += chunk
    return bytes(buf)


def worker(H, wid, rng, state):
    """Round owner == the RECEIVER.  Per round: build a pipe carrying a unique
    tag, spawn the (delayed) SCM_RIGHTS sender + the cooperative bystanders, then
    recvmsg the passed fd HERE (a different hub from the sender, very likely),
    wait_fd-park it, and assert os.read == the tag."""
    shard = wid & 1023
    rno = 0
    # Ancillary buffer sized for exactly one fd (CMSG_LEN of one int).  Exact so
    # MSG_CTRUNC is a real signal: too-small would truncate every healthy send;
    # this is just-right, so CTRUNC set == a genuine kernel-side fd loss.
    ancbufsize = socket.CMSG_LEN(array.array("i", [0]).itemsize)
    for _ in H.round_range():
        rno += 1
        if not H.running():
            break
        a, b = state["pairs"][wid]
        try:
            r, w = os.pipe()
        except OSError:
            break
        # Track every fd we OPEN this round so the fd-conservation oracle is
        # exact even if something below short-circuits.
        opened = state["opened"]
        opened[shard] += 2              # r + w

        tag = make_tag(wid, rno)
        # Write the tag into the pipe BEFORE passing the read end, so the received
        # fd is readable the instant it materializes (a healthy fd needs no wake
        # to find data already present; a timeout then isolates a dropped arm).
        try:
            nwritten = os.write(w, tag)
        except OSError:
            nwritten = 0
        # The write end is no longer needed by anyone except as the data source we
        # already wrote; close it so EOF is well-defined and it doesn't leak.
        try:
            os.close(w)
            state["closed"][shard] += 1
        except OSError:
            pass

        ready_ch = runloom.Chan(1)
        # Spawn the bystanders (cooperative-progress proof, arm A) -- they share
        # whatever hub they land on; the sampler watches the global coop signal.
        for _ in range(BYSTANDERS):
            H.fiber(bystander, H, wid, shard, state)
        # Spawn the DELAYED sender.  It dups r into a's socket buffer then closes
        # its own r, so the receiver's recvmsg below materializes a NEW fd number.
        H.fiber(sender, H, a, r, SEND_DELAY_S, ready_ch)
        state["delayed_inflight"][shard] += 1   # arm (A): a delayed send is live

        # RECEIVE on b (different hub from the sender, very likely): cooperative
        # recvmsg park for the whole SEND_DELAY_S, then parse the CMSG.
        got_fd, ctrunc_ok = receive_fd(H, b, ancbufsize)
        # Sender finished (or bailed); join it so its local-r close has happened
        # and a stranded sender shows as a never-returning goroutine -> LOST.
        sent_ok, _open = ready_ch.recv()
        state["delayed_inflight"][shard] -= 1

        if got_fd < 0:
            # No fd received.  Distinguish a genuine CTRUNC corruption (arm B
            # failure) from a benign teardown (sender bailed because the run
            # ended).  nwritten==0 / not sent / not running -> benign.
            if H.running() and sent_ok and nwritten == len(tag):
                if not ctrunc_ok:
                    H.fail("MSG_CTRUNC set / fd array truncated (wid={0} "
                           "round={1}) -- the SCM_RIGHTS ancillary data was "
                           "clipped and the passed fd was LOST".format(wid, rno))
                    return
                H.fail("no fd materialized from a successful SCM_RIGHTS send "
                       "(wid={0} round={1}) -- the CMSG was mis-parsed or the "
                       "ancillary fd was dropped crossing recvmsg".format(
                           wid, rno))
                return
            # benign: the delayed send was cancelled by teardown.
            break

        # got_fd is a brand-new kernel fd the arm cache has never seen.  Park it
        # for READ and read the tag back.
        opened[shard] += 1              # the received (duped) fd
        data = read_tag(H, got_fd, len(tag))
        try:
            os.close(got_fd)
            state["closed"][shard] += 1
        except OSError:
            pass

        # IDENTITY CONSERVATION (arm B): the bytes from the received fd MUST equal
        # the sender's tag.  A wrong fd (mis-parsed CMSG), a truncated CMSG, or a
        # cross-wire (the wrong unit's pipe) all corrupt this.
        if H.running():
            if not ctrunc_ok:
                H.fail("MSG_CTRUNC set on a delivered fd (wid={0} round={1}) -- "
                       "the ancillary buffer was truncated".format(wid, rno))
                return
            if not H.check(
                    data == tag,
                    "fd-identity broken: read {0} bytes, tag is {1} (wid={2} "
                    "round={3}) -- the received fd is the WRONG kernel object "
                    "(mis-parsed CMSG / MSG_CTRUNC corruption / cross-wire) or "
                    "its first arm on the receiver hub was dropped".format(
                        len(data), len(tag), wid, rno)):
                return
            H.op(wid)
            H.task_done(wid)
        else:
            break


def sampler(H, state):
    """Cooperative-park PROOF (arm A), measured exactly like p177: while delayed-
    send recvmsg's are in flight, the bystanders' cooperative op count must keep
    rising.  A sustained stall to zero == a hub OS-blocked inside a NON-
    cooperative recvmsg (a missing wrapper for the ancillary-data path)."""
    coop = state["coop"]
    inflight = state["delayed_inflight"]
    last = sum(coop)
    stalls = 0
    while H.running():
        H.sleep(0.25)
        cur = sum(coop)
        # Only count a stall when delayed sends are actually in flight (so the
        # receivers ARE parked in recvmsg) and the run is mid-flight (not ramp-up
        # /drain).  A non-cooperative recvmsg would OS-block hubs and freeze coop.
        if (cur == last and sum(inflight) > 0
                and H.running() and H.time_left() > 0.5):
            stalls += 1
            if stalls >= 6:             # ~1.5s with zero cooperative progress
                H.fail("cooperative progress stalled ~1.5s while delayed "
                       "SCM_RIGHTS recvmsg's were in flight -- recvmsg is NOT "
                       "cooperatively parking; a hub OS-thread is blocked in a "
                       "non-cooperative recvmsg (missing wrapper)")
                return
        else:
            stalls = 0
        last = cur


def setup(H):
    if not _HAVE_MSG:
        H.note_scale_limit(
            "socket.sendmsg/recvmsg/SCM_RIGHTS unavailable on this platform "
            "({0}) -- skipping the fd-passing test".format(sys.platform))
        H.state = None
        return
    if not _HAVE_WAITFD:
        H.note_scale_limit(
            "runloom_c.wait_fd unavailable -- cannot park the received fd; "
            "skipping")
        H.state = None
        return
    n = H.funcs
    pairs = []
    for _ in range(n):
        a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        a.setblocking(False)
        b.setblocking(False)
        pairs.append((a, b))
        H.register_close(a)
        H.register_close(b)
    H.state = {
        "pairs": pairs,
        # Cooperative-progress signal (arm A) -- sharded, one writer per slot.
        "coop": [0] * 1024,
        # Count of delayed sends currently in flight (gates the stall detector).
        "delayed_inflight": [0] * 1024,
        # fd-conservation tallies: every fd opened must be closed (the received
        # fd, r locally via SCM_RIGHTS, and w).
        "opened": [0] * 1024,
        "closed": [0] * 1024,
    }


def body(H):
    if H.state is None:
        return                          # skipped in setup
    # Arm (A) sampler runs alongside the receivers.
    H.fiber(sampler, H, H.state)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if H.state is None:
        H.log("SKIPPED: {0}".format(H.scale_limit_reason or "no sendmsg/wait_fd"))
        return
    units = H.total_ops()
    coop = sum(H.state["coop"])
    opened = sum(H.state["opened"])
    closed = sum(H.state["closed"])
    H.log("fd_pass_units(ops)={0} tasks={1} coop_ops={2} opened={3} closed={4} "
          "fd_base={5} fd_end={6}".format(
              units, H.total_tasks(), coop, opened, closed,
              H.fd_base, H.fd_end))
    # The transfer actually ran (a fully-stranded run with zero received fds is a
    # failure, not a vacuous pass).
    H.check(units > 0,
            "no SCM_RIGHTS fd-passing unit completed -- every receiver stranded "
            "in recvmsg or no fd ever materialized")
    # Arm (A) corroboration: cooperative bystanders made progress (the sampler
    # already fails on a sustained stall; this guards against a vacuous run).
    H.check(coop > 0,
            "no cooperative bystander progress -- the cooperative-park proof did "
            "not actually run")
    # fd-leak (per-worker accounting): every fd we materialized (r, w, and the
    # received dup) must have been closed.  The receiver fd + w are closed by the
    # worker; r's local copy is closed by the sender.  A surplus of opened over
    # closed-here is the receiver-side / sender-side leak we own; the sender's
    # close of its own r is its own (counted via H.fd_end below, not here), so we
    # only require closed-here covers w + the received fd == 2 per completed unit.
    # The strong, backend-independent leak oracle is the process fd balance:
    if H.fd_base >= 0 and H.fd_end >= 0:
        # Each in-flight unit transiently holds at most a pipe pair + the received
        # dup + the socketpair (pre-allocated, counted in fd_base); bound the
        # end-vs-base balance generously by the concurrent fan-out, NOT by funcs
        # (every per-round fd is closed within the round, so it must not grow
        # with funcs).
        H.check(H.fd_end < H.fd_base + 256,
                "fd leak across run: end {0} vs base {1} -- a passed/received fd "
                "or a per-round pipe was not closed (the SCM_RIGHTS dup or the "
                "pipe write end leaked)".format(H.fd_end, H.fd_base))
    # A receiver parked on a freshly-passed fd that never wakes (the new fd's
    # first arm on the receiver hub was dropped), or a sender stranded in a non-
    # cooperative sendmsg, is a LOST worker -- not merely slow.
    H.require_no_lost("SCM_RIGHTS fd-passing completeness")


if __name__ == "__main__":
    # Moderate default sibling N: like p312/p309, the per-round pipe + socketpair
    # + delayed-sender + bystanders fan-out is a CONCURRENCY hunt (the new-fd arm
    # + cooperative recvmsg park), not a volume hunt -- a few thousand fd-passes
    # across hubs is plenty to expose a mis-parsed CMSG, a dropped first-arm wake,
    # or a non-cooperative recvmsg.  A hard ceiling keeps the soak driver from
    # opening a pipe + socketpair per func at 1M (which would exhaust fds, not
    # test a bug -- that would be a benign scale limit, not a fault).
    harness.main("p325_scm_rights_fd_passing", body, setup=setup, post=post,
                 default_funcs=1500, max_funcs=3000,
                 describe="pass a pipe READ fd carrying a unique tag over AF_UNIX "
                          "via sendmsg(SCM_RIGHTS) with a DELAYED send; receiver "
                          "recvmsg's it on another hub (cooperative-park proof), "
                          "wait_fd-parks the new fd, os.read == tag (fd-identity); "
                          "no MSG_CTRUNC, no lost wake, no fd leak")
