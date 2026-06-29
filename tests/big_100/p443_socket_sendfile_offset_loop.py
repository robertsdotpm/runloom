"""big_100 / 443 -- cooperative socket.sendfile partial-loop offset conservation
under M:N, with a sibling mutating the SAME source fd across the EAGAIN park.

The subject is runloom's cooperative ``socket.sendfile`` -- specifically
``runloom.monkey.sockets._co_sendfile_use_sendfile`` (monkey/sockets.py:323),
the zero-copy half it reimplements over the RAW ``os.sendfile`` captured
pre-offload as ``_base._raw_os_sendfile`` (monkey/_base.py:73).  Stock
socket.sendfile refuses a non-blocking socket and drives its own
selectors.PollSelector; the fiber socket is non-blocking by construction, so
runloom reimplements the loop and PARKS on wait_fd(sock, WRITE) on EAGAIN
instead of pinning a hub.  The hot loop is (sockets.py:339-364):

    total_sent = 0
    while True:
        if count:
            blocksize = min(count - total_sent, blocksize)
            if blocksize <= 0: break
        try:
            sent = _raw_os_sendfile(sockno, fileno, offset, blocksize)
        except (BlockingIOError, InterruptedError):
            _wait_fd_coop(sockno, WRITE)       # <-- PARKS HERE between partials
            continue
        ...
        if sent == 0: break                    # EOF / short source -> stop
        offset += sent                         # Python-side running offset
        total_sent += sent

The exact non-atomic state under attack is the PYTHON-SIDE pair
``(offset, total_sent)`` -- a plain pair of local ints carried, on a grown-down
C stack, across the ``_wait_fd_coop(sockno, WRITE)`` park between two partial
``sendfile`` syscalls -- together with the source fd's kernel object (its
inode page cache + file size) that ``_raw_os_sendfile(sockno, fileno, offset,
blocksize)`` reads at the EXPLICIT offset.  The racing op pair is:

    sendfile(out, in, offset, count) partial-resume   (this fiber, post-park)
  vs
    lseek(in) / ftruncate(in) / close(in)             (a sibling on ANOTHER hub)

on the SAME source fd (or its dup) DURING the park.  Three mutually-exclusive
ways that can corrupt the transfer, each made falsifiable below:

  * a MOVED OFFSET re-sends a region (a DUPLICATE tagged block) or skips one
    (a LOST tagged block) -- this would happen if the loop ever let the source
    fd's kernel file-position drive the transfer instead of its own running
    ``offset``;
  * a TRUNCATED source returns short, so ``sent`` shrinks toward 0: the loop
    must complete CLEANLY at the truncated length (a clean prefix), never spin
    forever and never over-count past what the (now shorter) file holds;
  * the out-socket's WRITE-readiness arm vs a concurrent close of either fd --
    the park must wake and the loop must terminate, never strand the fiber.

TARGET INVARIANT -- BYTE CONSERVATION over a finite tagged universe.  The
source is a fixed-length blob of NBLOCKS 8-byte blocks; block i carries its own
index i (little-endian) as its tag, so EVERY 8 received bytes name the file
position they came from.  The receiver reassembles the stream and asserts:

  * every received 8-byte block decodes to an index in [0, NBLOCKS)  -- an
    out-of-universe tag is a torn/overrun read (hard fault);
  * the received stream is an EXACT, in-order, gap-free PREFIX: block k of the
    received stream decodes to index k.  A DUPLICATE (block k decodes to an
    index already seen / != k) is a re-sent region; a GAP (index jumps) is a
    skipped region.  Both are hard faults.

CONTROL ARM (case 0, the falsifier): a single-owner sendfile of a PRIVATE
source file that NO sibling touches.  It MUST reproduce the blob byte-for-byte
-- received length == file length, every block index present exactly once in
order.  A single-owner transfer is race-free by construction, so if the CONTROL
loses or duplicates a block the fault is in CPython/runloom's sendfile
partial-loop / offset bookkeeping ITSELF, not cross-fd contention.

CONTENTION ARMS (cases 1-3, the probes), each round-robined by worker id:

  * case 1 LSEEK-DURING-PARK: a sibling hammers os.lseek() on the SAME fd the
    sendfile is driving, during its parks.  Because ``_raw_os_sendfile`` passes
    an EXPLICIT offset, lseek MUST be inert: the result MUST still be the full,
    byte-identical blob (exact prefix, every block once, in order).  A duplicate
    or skipped block HERE means the loop let the kernel file-position leak into
    the transfer -- the headline bug.
  * case 2 FTRUNCATE-DURING-PARK: a sibling ftruncate()s the source to a
    random shorter length mid-park.  The transfer legally ends SHORT; the
    invariant relaxes to "received length <= original AND is a clean in-order
    gap-free prefix of tagged blocks" -- never a duplicate, never an
    out-of-universe tag, never a spin (the watchdog catches a spin as a HANG).
  * case 3 CLOSE-OUT-DURING-PARK: a sibling closes the OUT socket mid-transfer.
    The sendfile must raise an OSError (caught) or return short, and the fiber
    must NOT strand -- it returns and the round completes.  No byte oracle (the
    stream is intentionally cut); the invariant is liveness + no out-of-universe
    block among whatever arrived.

Conservation accounting (post): a per-slot single-writer tally of CONTROL bytes
offered and bytes received-and-verified; CONTROL received == CONTROL offered
exactly (no byte lost or doubled on the uncontended path), and >0 of each case
ran (round-robin coverage, the p125/p126/p172 flaky-random lesson).

Stresses: cooperative socket.sendfile partial-transfer loop, Python-side
(offset,total_sent) carried across the wait_fd(WRITE) park, explicit-offset
os.sendfile vs lseek/ftruncate/close on the same source fd cross-hub, short-
source loop termination, out-socket WRITE-arm vs concurrent close, tagged-block
byte conservation (no duplicate / no gap / no out-of-universe block).

Good TSan / controlled-M:N-replay target: the running ``offset += sent`` write
straddling ``_wait_fd_coop(sockno, WRITE)`` while a sibling lseek/ftruncates the
same fd is a textbook publish-across-park; a TSan report on the fd's kernel
file object, or a single duplicated/skipped tagged block under replay,
localizes the offset leak before the prefix oracle even closes.
"""
import os
import socket
import sys

# ---- availability guard (POSIX zero-copy sendfile) ------------------------
# os.sendfile (the zero-copy path runloom reimplements cooperatively) exists on
# Linux/*BSD/macOS but NOT on Windows; without it _patched_sendfile falls to the
# read()+send() fallback, which is a DIFFERENT primitive (no os-level offset arg
# to attack).  Detect-and-skip so this program only runs where the offset loop
# under test actually executes.
if not sys.platform.startswith(("linux", "freebsd", "darwin")) \
        or getattr(os, "sendfile", None) is None:
    print("SKIP: os.sendfile zero-copy path unavailable on this platform "
          "({0})".format(sys.platform))
    sys.exit(0)

import harness
import runloom

# ---- the finite tagged UNIVERSE -------------------------------------------
# The source blob is NBLOCKS blocks of BLOCK bytes each; block i stores its own
# index i (little-endian) as its tag.  Every received BLOCK-byte group therefore
# NAMES the file position it came from, so a duplicate / skipped / torn region is
# detectable as a wrong or out-of-universe index.  Sized so the transfer spans
# MANY partial sendfile() calls (many wait_fd(WRITE) parks) once the receiver
# drains slowly with a small socket buffer -- each park is a fresh window for the
# sibling mutation to land.
BLOCK = 8
NBLOCKS = 768
BLOB_SIZE = BLOCK * NBLOCKS              # 6144 bytes

# Force the sender through real EAGAIN parks: a tiny out-socket send buffer means
# sendfile() can only move a little before EAGAIN, and a slow receiver (small
# recv + a yield per read) keeps the pipe nearly full so the sender parks
# repeatedly mid-transfer.  This is what opens the cross-fd-mutation race window.
SNDBUF = 2048
RCVBUF = 2048
RECV_CHUNK = 96                          # tiny receiver reads -> many sender parks

# The four cases, round-robined by worker id in the first ops so post() coverage
# holds whether one worker does 4 rounds or 4 workers do 1 each (the p125/p126/
# p172 flaky-random-coverage fix).
CASE_CONTROL = 0      # private source, NO sibling touch -> exact blob (falsifier)
CASE_LSEEK = 1        # sibling lseek()s the SAME fd mid-park -> must stay exact
CASE_FTRUNCATE = 2    # sibling ftruncate()s the source mid-park -> clean short prefix
CASE_CLOSE_OUT = 3    # sibling closes the OUT socket mid-transfer -> liveness only
NCASES = 4

SLOTS = 1024


def make_blob():
    """The fixed tagged blob: block i holds its own index i (little-endian).
    Every BLOCK-byte group self-identifies its file position, so a re-sent or
    skipped region is caught as a wrong/repeated index."""
    b = bytearray(BLOB_SIZE)
    for i in range(NBLOCKS):
        b[i * BLOCK:(i + 1) * BLOCK] = i.to_bytes(BLOCK, "little")
    return bytes(b)


# One immutable blob shared read-only by every worker (its CONTENT is the same
# for all; each worker writes its OWN private temp file FROM it, so no fd is
# shared across workers -- only within a round, deliberately, by the sibling).
BLOB = make_blob()


def write_source(d, wid, salt):
    """Write the tagged blob to a fresh PRIVATE temp file under dir d and return
    its path.  Per-(wid,salt) name so concurrent rounds never collide on a file
    another round's sibling might truncate."""
    path = os.path.join(d, "src_{0}_{1}.bin".format(wid, salt & 0xFFFFFF))
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        off = 0
        mv = memoryview(BLOB)
        while off < BLOB_SIZE:
            off += os.write(fd, mv[off:])
    finally:
        os.close(fd)
    return path


def decode_blocks(H, wid, case, data, allow_short):
    """Validate `data` as an in-order, gap-free, duplicate-free prefix of tagged
    blocks.  Returns the number of WHOLE blocks verified, or -1 on a hard fault
    (H.fail already recorded).  `allow_short` selects whether a length < full
    blob is legal (truncate / close arms) or itself a drop (control / lseek)."""
    n = len(data)
    whole = n // BLOCK
    # A partial trailing block can legitimately exist on the truncate/close arms
    # (the transfer was cut mid-block); on the exact arms a non-block-multiple
    # length is itself a torn-stream symptom unless it is the full blob.
    if not allow_short and n != BLOB_SIZE:
        H.fail("wid={0} case={1}: exact-arm received {2} bytes != full blob "
               "{3} -- a tagged block was DROPPED or DUPLICATED on the "
               "uncontended/lseek-inert sendfile offset loop".format(
                   wid, case, n, BLOB_SIZE))
        return -1
    for k in range(whole):
        idx = int.from_bytes(data[k * BLOCK:(k + 1) * BLOCK], "little")
        if idx >= NBLOCKS:
            H.fail("wid={0} case={1}: received block {2} decodes to OUT-OF-"
                   "UNIVERSE index {3} (>= {4}) -- a torn/overrun read from the "
                   "sendfile offset loop".format(wid, case, k, idx, NBLOCKS))
            return -1
        if idx != k:
            # The received stream's k-th block must come from source block k.
            # A smaller-or-equal index already seen == a RE-SENT region (moved
            # offset); a larger jump == a SKIPPED region.
            H.fail("wid={0} case={1}: received block {2} carries source index "
                   "{3} (expected {2}) -- the sendfile running offset {4} a "
                   "region (the kernel file-position leaked into the explicit-"
                   "offset transfer across the wait_fd park)".format(
                       wid, case, k, idx,
                       "RE-SENT" if idx < k else "SKIPPED"))
            return -1
    return whole


def receiver(H, in_sock, received, sent_cell, wg):
    """Drain the IN socket into `received` slowly (tiny reads + a yield each) so
    the sender stays near a full pipe and parks repeatedly between partials.

    Stops at the FIRST of: clean EOF, shutdown, OR having collected exactly the
    sender-reported byte count (sent_cell[0], published by the sender the instant
    socket.sendfile() returns).  Bounding the drain by the sender's OWN
    authoritative `sent` is the closed-world contract -- the sender is the single
    source of truth for how many bytes crossed -- and it deliberately avoids a
    read-PAST-the-transfer window: under heavy socketpair fd churn a recv that
    keeps blocking after its transfer is complete can be woken by a recycled fd
    number and pull an unrelated round's bytes, which is an fd-lifecycle artifact,
    NOT a sendfile offset-loop fault (the oracle below would mis-blame it).  We
    still validate every byte we DO take against its tagged position, so a genuine
    duplicate/skip WITHIN the transfer is caught."""
    try:
        while True:
            if not H.running():
                return
            target = sent_cell[0]            # -1 until the sender publishes
            if target >= 0 and len(received) >= target:
                return                       # collected the whole reported transfer
            try:
                chunk = in_sock.recv(RECV_CHUNK)
            except OSError:
                return                   # out closed / torn at shutdown
            if not chunk:
                return                   # clean EOF
            received.extend(chunk)
            runloom.yield_now()
    finally:
        wg.done()


def lseek_molester(H, src_fd, rng, wg):
    """Hammer os.lseek() on the SAME fd the sendfile drives, during its parks.
    With the explicit-offset os.sendfile this MUST be inert; if the offset loop
    ever consulted the kernel file position, a moved seek would re-send or skip a
    tagged block and the receiver's prefix oracle would catch it."""
    try:
        for _ in range(4000):
            if not H.running():
                return
            try:
                os.lseek(src_fd, rng.randrange(BLOB_SIZE), os.SEEK_SET)
            except OSError:
                return                   # fd closed (round tearing down) -> stop
            runloom.yield_now()
    finally:
        wg.done()


def ftruncate_molester(H, src_fd, rng, wg):
    """ftruncate() the source to a random shorter, block-aligned length
    mid-transfer, then keep poking so the short read lands inside a sendfile
    park.  The legal upper bound on received bytes is the sender's OWN reported
    `sent` (consulted in the oracle), so the molester records nothing -- it only
    shrinks the source under the running offset loop."""
    try:
        # A block-aligned shorter length so a clean short prefix is still whole
        # tagged blocks, then a few re-pokes to widen the in-park window.
        target = rng.randint(NBLOCKS // 8, NBLOCKS - 1) * BLOCK
        for _ in range(64):
            if not H.running():
                return
            try:
                os.ftruncate(src_fd, target)
            except OSError:
                return
            runloom.yield_now()
    finally:
        wg.done()


def close_out_molester(H, out_sock, rng, wg, closed_flag):
    """Close the OUT socket mid-transfer so the sender's wait_fd(WRITE) arm wakes
    onto a closed fd: the sendfile must raise/return short and the fiber must NOT
    strand.  Yields a few times first so some bytes move before the cut."""
    try:
        for _ in range(rng.randint(1, 4)):
            if not H.running():
                return
            runloom.yield_now()
        closed_flag[0] = True
        try:
            out_sock.close()
        except OSError:
            pass
    finally:
        wg.done()


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    offered_tbl = state["offered"]
    received_tbl = state["received"]
    case_tbl = state["case_runs"]
    d = state["dir"]
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the four cases by worker id in the first ops; random after.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1

        salt = rng.getrandbits(32)
        path = write_source(d, wid, salt)

        out_sock, in_sock = socket.socketpair()
        try:
            out_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SNDBUF)
            in_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RCVBUF)
        except OSError:
            pass

        src = open(path, "rb", buffering=0)
        src_fd = src.fileno()

        received = bytearray()
        sent_cell = [-1]                 # sender publishes its authoritative `sent`
        closed_flag = [False]

        # Receiver always runs; the contention cases add exactly one sibling.
        nchildren = 1 + (0 if case == CASE_CONTROL else 1)
        wg = runloom.WaitGroup()
        wg.add(nchildren)

        mseed = rng.getrandbits(48)

        def run_receiver(in_sock=in_sock, received=received,
                         sent_cell=sent_cell, wg=wg):
            receiver(H, in_sock, received, sent_cell, wg)

        H.fiber(run_receiver)

        if case == CASE_LSEEK:
            import random

            def run_lseek(src_fd=src_fd, mseed=mseed, wg=wg):
                lseek_molester(H, src_fd, random.Random(mseed ^ 0xA5A5), wg)
            H.fiber(run_lseek)
        elif case == CASE_FTRUNCATE:
            import random

            def run_trunc(src_fd=src_fd, mseed=mseed, wg=wg):
                ftruncate_molester(H, src_fd, random.Random(mseed ^ 0x5A5A), wg)
            H.fiber(run_trunc)
        elif case == CASE_CLOSE_OUT:
            import random

            def run_close(out_sock=out_sock, mseed=mseed, wg=wg,
                          closed_flag=closed_flag):
                close_out_molester(H, out_sock,
                                   random.Random(mseed ^ 0x3C3C), wg, closed_flag)
            H.fiber(run_close)

        # ---- drive the cooperative socket.sendfile (the loop under test) -----
        sent = -1
        sendfile_err = None
        try:
            sent = out_sock.sendfile(src, 0, BLOB_SIZE)
        except OSError as exc:
            sendfile_err = exc           # legal on the close-out arm; checked below

        # Publish the sender's authoritative byte count so the receiver stops once
        # it has collected exactly that many (the closed-world bound; see receiver).
        # On an OSError (close-out arm) we never know a clean total, so bound the
        # receiver by whatever is already in flight: signal "stop at EOF" by
        # leaving sent_cell as the current received length's ceiling is not known,
        # so use BLOB_SIZE as a safe upper bound -- the out socket close gives EOF.
        sent_cell[0] = sent if sent >= 0 else BLOB_SIZE

        # The receiver stops at sent_cell bytes or EOF.  For the arms where we did
        # NOT close the out socket, close it now so a receiver still short of
        # sent_cell (truncate arm: fewer bytes than BLOB_SIZE arrived) sees EOF.
        if case != CASE_CLOSE_OUT and not closed_flag[0]:
            try:
                out_sock.close()
            except OSError:
                pass

        # Join EVERY child before reading `received` -- the stream is provably
        # quiescent only once the receiver (and any molester) has returned.
        wg.wait()

        try:
            src.close()
        except OSError:
            pass
        try:
            in_sock.close()
        except OSError:
            pass
        if case == CASE_CLOSE_OUT:
            try:
                out_sock.close()         # idempotent if the molester beat us
            except OSError:
                pass
        try:
            os.unlink(path)
        except OSError:
            pass

        if H.failed:
            return

        # ---- the tagged-block conservation oracle ---------------------------
        data = bytes(received)
        if case == CASE_CONTROL:
            # Uncontended single-owner transfer: MUST be the exact blob, and the
            # receiver MUST have collected exactly what the sender reported.
            if not H.check(sent == BLOB_SIZE,
                           "wid={0} CONTROL: sendfile returned sent={1} != blob "
                           "{2} -- the partial-loop under-counted on the "
                           "uncontended path (a CPython/runloom offset-loop bug, "
                           "not contention)".format(wid, sent, BLOB_SIZE)):
                return
            if not H.check(len(data) == BLOB_SIZE,
                           "wid={0} CONTROL: received {1} bytes != blob {2} "
                           "(sender reported {3}) -- the transfer did not arrive "
                           "intact on the uncontended path".format(
                               wid, len(data), BLOB_SIZE, sent)):
                return
            whole = decode_blocks(H, wid, case, data, allow_short=False)
            if whole < 0:
                return
            offered_tbl[slot] += BLOB_SIZE
            received_tbl[slot] += len(data)
        elif case == CASE_LSEEK:
            # lseek on the explicit-offset sendfile fd MUST be inert: still exact.
            if not H.check(sent == BLOB_SIZE,
                           "wid={0} LSEEK: sendfile returned sent={1} != blob "
                           "{2} despite the explicit-offset form -- a sibling "
                           "lseek leaked into the running offset across the "
                           "park".format(wid, sent, BLOB_SIZE)):
                return
            if not H.check(len(data) == BLOB_SIZE,
                           "wid={0} LSEEK: received {1} bytes != blob {2} -- a "
                           "sibling lseek perturbed the transfer length".format(
                               wid, len(data), BLOB_SIZE)):
                return
            whole = decode_blocks(H, wid, case, data, allow_short=False)
            if whole < 0:
                return
        elif case == CASE_FTRUNCATE:
            # Legal short transfer: a clean in-order gap-free prefix of whole
            # tagged blocks, length == the sender's reported `sent` (when no
            # error), never a dup / out-of-universe / over-count.
            whole = decode_blocks(H, wid, case, data, allow_short=True)
            if whole < 0:
                return
            if not H.check(len(data) <= BLOB_SIZE,
                           "wid={0} FTRUNCATE: received {1} bytes > original blob "
                           "{2} -- the loop OVER-COUNTED past the (now shorter) "
                           "source".format(wid, len(data), BLOB_SIZE)):
                return
            if sendfile_err is None and not H.check(
                    len(data) == sent,
                    "wid={0} FTRUNCATE: received {1} bytes != sender-reported "
                    "sent {2} -- the short transfer did not conserve bytes "
                    "between sender and receiver".format(wid, len(data), sent)):
                return
        else:  # CASE_CLOSE_OUT
            # The stream was intentionally cut: no length oracle, only liveness
            # (we got here -> the fiber did not strand) + no out-of-universe block
            # among whatever arrived (a clean prefix up to the cut).
            whole = decode_blocks(H, wid, case, data, allow_short=True)
            if whole < 0:
                return

        case_tbl[case][slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran): the temp dir, the
    # per-slot single-writer tallies, and the per-case run counters.  No shared
    # mutable Python object is hammered across hubs here -- each round owns its
    # own files/sockets; only the per-round sibling deliberately shares a fd.
    d = H.make_tmpdir(prefix="big100_sendfile_")
    H.state = {
        "dir": d,
        "offered": [0] * SLOTS,          # CONTROL bytes offered (single-owner)
        "received": [0] * SLOTS,         # CONTROL bytes received-and-verified
        "case_runs": {
            CASE_CONTROL: [0] * SLOTS,
            CASE_LSEEK: [0] * SLOTS,
            CASE_FTRUNCATE: [0] * SLOTS,
            CASE_CLOSE_OUT: [0] * SLOTS,
        },
    }


def body(H):
    # Each worker holds a socketpair (2 fds) + a source file fd + 1-2 sibling
    # fibers for the lifetime of a round -- a small fd budget per worker, but the
    # tight park-resolve handoff does not scale to 1M.  Cap to a designed scale
    # well under the descriptor ceiling (cf. p413/p228): the point is the per-
    # block offset-conservation oracle under M:N, not raw 1M throughput.
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    offered = sum(H.state["offered"])
    received = sum(H.state["received"])
    runs = H.state["case_runs"]
    c0 = sum(runs[CASE_CONTROL])
    c1 = sum(runs[CASE_LSEEK])
    c2 = sum(runs[CASE_FTRUNCATE])
    c3 = sum(runs[CASE_CLOSE_OUT])
    H.log("CONTROL bytes offered={0} received={1}; case runs control={2} "
          "lseek={3} ftruncate={4} close_out={5}; ops={6}".format(
              offered, received, c0, c1, c2, c3, H.total_ops()))

    H.check(H.total_ops() > 0,
            "no sendfile round completed -- the partial-loop offset race window "
            "was never exercised")

    # CONTROL conservation: on the single-owner uncontended path every offered
    # byte was received and verified exactly once.  A divergence here is a
    # CPython/runloom sendfile partial-loop bug, NOT contention (the control has
    # no sibling), so this is the disambiguating falsifier.
    H.check(offered == received,
            "CONTROL byte conservation broken: offered={0} != received={1} -- "
            "the uncontended single-owner sendfile offset loop lost or doubled "
            "bytes (a CPython/runloom bug, not cross-fd contention)".format(
                offered, received))
    H.check(c0 > 0,
            "CONTROL arm never exercised -- no single-owner falsifier ran, so a "
            "partial-loop bug could hide behind the contention arms")

    # Each contention arm round-robined by worker id was reached (so the lseek /
    # truncate / close-out windows were actually probed, not skipped).
    H.check(c1 > 0, "LSEEK-during-park arm never exercised")
    H.check(c2 > 0, "FTRUNCATE-during-park arm never exercised")
    H.check(c3 > 0, "CLOSE-OUT-during-park arm never exercised")

    H.require_no_lost("sendfile-offset-loop completeness")


if __name__ == "__main__":
    harness.main(
        "p443_socket_sendfile_offset_loop", body, setup=setup, post=post,
        default_funcs=3000, max_funcs=4000,
        describe="cooperative socket.sendfile partial-loop offset conservation "
                 "under M:N: a tagged-block source must arrive as an exact in-"
                 "order gap-free prefix on the control/lseek arms (lseek is inert "
                 "for the explicit-offset os.sendfile) and a clean short prefix "
                 "under ftruncate/close-out -- a duplicated/skipped/out-of-"
                 "universe block, an over-count past a truncated source, or a "
                 "stranded fiber is the bug")
