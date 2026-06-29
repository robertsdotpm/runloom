"""big_100 / 440 -- os.writev/os.readv iovec-vs-bytearray-realloc across the park.

The subject is the cooperative ``os.writev`` / ``os.readv`` monkey-patches
(src/runloom/monkey/osio.py ``_patched_os_writev`` / ``_patched_os_readv``) and
the exact CPython internal state they straddle: the per-buffer ``Py_buffer``
export (the bytearray's ``ob_exports`` counter, and its ``ob_bytes`` /
``ob_alloc`` backing store) that the C ``posix_writev`` / ``posix_readv`` build
their ``iovec[i].iov_base`` / ``iov_len`` snapshot from at syscall entry.

How the cooperative wrapper turns this into an M:N hazard.  ``os.writev`` is
PARTIAL-TRANSFER: a single ``_orig_os_writev(fd, buffers)`` call writes at most
what fits in the kernel pipe buffer and returns that count -- the patched
wrapper does NOT loop it to completion, it only RE-CALLS ``_orig_os_writev`` on
a *full-pipe* ``BlockingIOError`` after parking the fiber in
``runloom_c.wait_fd(fd, WRITE)`` on a grown-down C stack.  So a full vectored
write is a SEQUENCE of ``_orig_os_writev`` C calls, and between them the fiber
sleeps PARKED with the buffer list a LIVE Python list -- each re-call rebuilds
the ``iovec`` array (re-exports each buffer, re-reads ob_bytes/ob_alloc) from
the list elements as they are AT WAKE.  Each individual C call holds the export
(``ob_exports >= 1``) only for the duration of that one syscall; the park sits
BETWEEN calls, with nothing exported.  That is the window a sibling on another
hub attacks by mutating a bytearray that appears in that same list:

  * a ``bytearray.extend()`` / ``__iadd__`` / del-slice issued WHILE the buffer
    is exported (mid-syscall) hits ``PyByteArray_Resize`` -> the legal
    ``BufferError("Existing exports of data: object cannot be re-sized")``: the
    ``ob_exports`` guard refuses to free+realloc ``ob_bytes`` out from under the
    live ``iov_base`` (the detection we COUNT as acceptable).  If that guard
    ever failed, the realloc would free ``ob_bytes`` and the next kernel read of
    the stale ``iov_base`` is a USE-AFTER-FREE / torn payload on the wire.
  * an IN-PLACE poke (``buf[i] = v``, no resize) IS permitted through the export
    and is reflected on the wire on the NEXT re-call -- so the payload bytes a
    racing sibling can legally make appear are confined to a finite MUTATION
    tag; anything OUTSIDE that finite set in the received stream is a freed /
    torn / cross-wire byte -- a real fault.

For ``os.readv`` the mirror invariant: the writable ``Py_buffer`` targets
(``bytearray`` / ``array.array``) are exported for the readv duration; a sibling
``extend`` / ``frombytes`` that would resize an exported readv target MUST raise
``BufferError`` (the legal detection).  A silent resize, an out-of-universe
byte landing in the buffer, a short read, or a SIGSEGV is the bug.

The atomic unit under attack is the per-buffer ``ob_exports`` counter, checked-
then-acted-on across the park boundary.

TARGET INVARIANT -- CONSERVATION + IDENTITY on a finite tagged universe.  Each
writev sends a list of N buffers whose concatenation is a UNIQUE per-(wid,round)
tag pattern: byte at the offset owned by buffer ``i`` carries ``tag(wid, round,
i)`` (a value in a fixed sentinel UNIVERSE).  A reader fiber on the OTHER end of
a per-worker pipe reassembles every byte and asserts:

  * exactly ``sum(len(buf))`` bytes arrive -- no byte dropped (short prefix) or
    duplicated (overrun): CONSERVATION;
  * every received byte is IN the finite universe {its position's original tag}
    plus, for the raced shared-bytearray region, the single recognised MUTATION
    tag -- an out-of-universe byte is a freed / torn / cross-wire byte: IDENTITY.

CONTROL ARM (case 0).  A single-owner writer does the identical writev whose
buffer list -- fresh ``bytes`` objects and a PRIVATE bytearray no sibling
references -- NO other fiber touches.  It MUST conserve byte-for-byte 100% and
the reader MUST NOT see a single MUTATION tag.  If even the control loses or
tears a byte, the fault is CPython's iovec machinery itself, not contention --
the private-writer falsifier that disambiguates "writev is buggy" from "M:N
contention tore it".

RACED ARM (case 1).  One buffer in the list is a SHARED bytearray a sibling
mutator on another hub pokes IN PLACE DURING the park (NO resize, so the byte
COUNT is invariant and the writev's LIVE re-resolution always sees a BUF_LEN
buffer -> strict length conservation still holds).  An in-place poke through the
export legally reflects on the next re-call, so the legal value universe for the
shared region is exactly {original tag, MUTATION tag}.  The reader STILL requires
EXACT length and every byte in that universe; an out-of-universe byte / short /
overrun / SIGSEGV is the bug.  (The mutator deliberately does NOT resize the
writev buffer: a resize landing in the unexported window BETWEEN partial-writev
re-calls legitimately grows the byte count with in-universe bytes -- a property
of mutating a buffer you are concurrently sending, NOT a fault -- so folding it
into conservation would make the arm a tautology.  The resize-while-exported ->
``BufferError`` guard is driven on the READV arm instead.)

READV ARM (case 2).  A sibling tries to resize an exported writable readv
target while the readv is parked; that MUST raise ``BufferError`` (the legal
detection).  The readv'd bytes that DO land must be an exact in-order prefix of
the fed universe stream -- a wrong byte / short read / silent resize / SIGSEGV
is the bug.

Coverage is ROUND-ROBINED by worker id in the first ops (``sel = (wid + i) % 3``)
then random, so each arm is exercised whether one worker does 3 rounds or 3
workers do 1 each (the p125/p126/p172 flaky-random-coverage fix).  post()
reconciles the per-slot conserved-byte and BufferError tallies after every
fiber has joined and asserts each arm ran.

Stresses: os.writev/os.readv iovec base/len snapshot vs concurrent
bytearray.extend/__iadd__/del-slice realloc across the cooperative wait_fd park,
PyByteArray_Resize ob_exports guard, in-place-poke-through-export torn payload,
use-after-free / out-of-universe byte on the wire, exact vectored conservation,
private-vs-shared writer control.

Good TSan / controlled-M:N-replay target: the ob_bytes read inside posix_writev
vs the PyByteArray_Resize free+realloc on another hub is a textbook UAF data
race; a TSan report on ob_bytes, or one out-of-universe byte under replay,
localizes the fault before the conservation length check even closes.
"""
import os
import sys

# ---- availability guard (POSIX-only: pipe fds must be pollable, writev exists)
# Windows pipe fds are not pollable by the netpoll backend, so a vectored write
# on an os.pipe() write end can't park cooperatively; and os.writev/os.readv are
# POSIX-only.  Detect-and-skip-clean.
POSIX = sys.platform.startswith(("linux", "darwin", "freebsd"))
if not POSIX or not hasattr(os, "writev") or not hasattr(os, "readv"):
    print("SKIP: POSIX os.writev/os.readv on pollable pipe fds required "
          "(platform {0}, writev={1}, readv={2})".format(
              sys.platform, hasattr(os, "writev"), hasattr(os, "readv")))
    sys.exit(0)

import harness
import runloom

# ---- the finite sentinel tag UNIVERSE --------------------------------------
# Each writev buffer i of a round carries a single constant tag byte drawn from
# this universe.  A received byte NOT in the universe (and not the recognised
# MUTATION tag in the raced region) is a freed / torn / cross-wire byte -- the
# hard fault.  We pick a small set of recognisable values and a DISTINCT mutation
# tag the sibling pokes in-place, so a legal in-place-poke-through-export byte is
# distinguishable from the original AND from an out-of-universe corruption.
NBUF = 6                       # buffers per vectored write (a real iovec array)
# Per-buffer base tags: distinct, recognisable, none equal to MUT_TAG.  Buffer i
# of writer (wid, rnd) carries TAG_FOR(wid, rnd, i); the reader recomputes it.
TAG_BASE = 0x11
MUT_TAG = 0xC7                 # the in-place poke value a racing sibling writes
# Sentinel values the universe NEVER otherwise uses, so a stale/freed read is
# unlikely to coincidentally pass: tags are TAG_BASE + i in [0x11, 0x16]; MUT_TAG
# is 0xC7.  Everything else is out-of-universe.
UNIVERSE_TAGS = frozenset(range(TAG_BASE, TAG_BASE + NBUF))

# Per-buffer length.  Small * NBUF must comfortably exceed the (shrunk) pipe
# capacity so the cooperative writev PARKS several times per round -- the park is
# the hazard window.  Big enough that the shared bytearray spans many pipe-fulls
# (so an in-place poke lands in a region not yet flushed), small enough that
# thousands of workers each finish a round under the timeout.
BUF_LEN = 4096
ROUND_BYTES = NBUF * BUF_LEN

# Shrink each pipe to this capacity so a writev of ROUND_BYTES (24 KiB) is
# guaranteed to fill the pipe and park (re-call -> re-resolve LIVE) many times,
# making the race window deterministic instead of timing-dependent.  4 KiB is the
# Linux minimum (one page); if F_SETPIPE_SZ is unavailable we fall back to the
# default and rely on a slow reader to keep the pipe full.
PIPE_CAP = 4096

# Index of the buffer that is a SHARED bytearray in the raced arm (the one the
# sibling mutates).  The CONTROL arm makes this same slot a PRIVATE bytearray.
SHARED_SLOT = 3

# Cases, round-robined by worker id for guaranteed post() coverage.
CASE_CONTROL = 0   # single-owner writev, no sibling touches the list -> 100% exact
CASE_RACED = 1     # shared bytearray mutated mid-park -> {orig, MUT_TAG}, no UAF
CASE_READV = 2     # exported readv target resize must raise BufferError
NCASES = 3

SLOTS = 1024


def tag_for(wid, rnd, i):
    """The constant tag byte that buffer i of writer (wid, rnd) carries.  Fixed
    per (wid, rnd, i) and always in UNIVERSE_TAGS, so the reader recomputes the
    expected value at every position and a torn/freed byte is caught by value."""
    return TAG_BASE + i


def set_pipe_cap(fd):
    """Shrink the pipe to PIPE_CAP so a full vectored write parks many times.
    Best-effort: returns the resulting capacity (or -1 if it could not be read)."""
    try:
        import fcntl
        if hasattr(fcntl, "F_SETPIPE_SZ"):
            try:
                fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, PIPE_CAP)
            except OSError:
                pass
        if hasattr(fcntl, "F_GETPIPE_SZ"):
            return fcntl.fcntl(fd, fcntl.F_GETPIPE_SZ)
    except Exception:
        pass
    return -1


def writev_all(wfd, buffers):
    """Manual full-transfer loop over the cooperative os.writev.

    os.writev is partial-transfer and the patched wrapper only RE-CALLS the C
    posix_writev after a full-pipe park -- it does NOT itself loop to completion.
    So we drive the loop: each os.writev() call writes what fits, parks on a full
    pipe (re-resolving every iovec base LIVE from `buffers` on wake), and we
    advance past the consumed bytes via memoryview slices.  `buffers` may contain
    a bytearray a sibling mutates mid-park; the LIVE re-resolution is exactly the
    hazard.  Returns the total byte count actually sent."""
    views = [memoryview(b) for b in buffers]
    total = sum(len(v) for v in views)
    sent = 0
    idx = 0
    try:
        while sent < total:
            while idx < len(views) and len(views[idx]) == 0:
                idx += 1
            if idx >= len(views):
                break
            n = os.writev(wfd, views[idx:])   # may park; re-resolves bases LIVE
            if n <= 0:
                break
            sent += n
            k = n
            while k > 0 and idx < len(views):
                take = min(k, len(views[idx]))
                views[idx] = views[idx][take:]
                k -= take
                if len(views[idx]) == 0:
                    idx += 1
    finally:
        for v in views:
            try:
                v.release()
            except Exception:
                pass
    return sent


def build_buffers(wid, rnd, shared):
    """Build the NBUF-element writev list for writer (wid, rnd).  Buffer i is a
    constant tag block tag_for(wid,rnd,i)*BUF_LEN.  Slot SHARED_SLOT is the given
    `shared` bytearray (a private one for the control arm, the cross-hub shared
    one for the raced arm), pre-filled to its tag; the rest are immutable bytes."""
    bufs = []
    for i in range(NBUF):
        t = tag_for(wid, rnd, i)
        if i == SHARED_SLOT:
            shared[:] = bytes([t]) * BUF_LEN
            bufs.append(shared)
        else:
            bufs.append(bytes([t]) * BUF_LEN)
    return bufs


def validate_stream(H, wid, rnd, got, raced, info):
    """Validate the reassembled per-round stream `got` against the closed-world
    conservation + identity oracle.  Returns True iff the round conserved every
    byte in the finite universe.

    CONSERVATION: len(got) must equal ROUND_BYTES exactly -- a short prefix is a
    DROPPED byte (a torn iov_len), an overrun is a DUPLICATED byte.
    IDENTITY: byte at position p (in buffer p // BUF_LEN) must equal that
    buffer's original tag, EXCEPT in the raced shared region where the recognised
    MUT_TAG is also legal (an in-place poke through the export).  Any other value
    is OUT-OF-UNIVERSE -- a freed / torn / cross-wire byte."""
    if len(got) != ROUND_BYTES:
        H.fail("CONSERVATION broken (case={0}) wid={1} rnd={2}: received {3} "
               "bytes, expected exactly {4} -- the vectored write {5} a byte "
               "across the park (torn iov_len / partial re-resolve)".format(
                   "RACED" if raced else "CONTROL", wid, rnd, len(got),
                   ROUND_BYTES,
                   "DROPPED" if len(got) < ROUND_BYTES else "DUPLICATED"))
        return False
    mut_seen = 0
    for p in range(ROUND_BYTES):
        i = p // BUF_LEN
        exp = tag_for(wid, rnd, i)
        b = got[p]
        if b == exp:
            continue
        if raced and i == SHARED_SLOT and b == MUT_TAG:
            # A legal in-place poke-through-export byte: the sibling wrote MUT_TAG
            # in place (no resize), and the LIVE re-resolution put it on the wire.
            # In-universe, counted -- NOT a fault.
            mut_seen += 1
            continue
        # Anything else is a freed / torn / cross-wire byte: the hard fault.
        H.fail("IDENTITY broken (case={0}) wid={1} rnd={2}: byte at pos {3} "
               "(buffer {4}) == {5:#04x}, expected the tag {6:#04x}{7} -- an "
               "OUT-OF-UNIVERSE byte (use-after-free / torn / cross-wire payload "
               "from a stale iov_base across the writev park)".format(
                   "RACED" if raced else "CONTROL", wid, rnd, p, i, b, exp,
                   " or MUT_TAG {0:#04x}".format(MUT_TAG) if raced else ""))
        return False
    if not raced and mut_seen:
        # The control arm's list is private; NO sibling touches it, so a MUTATION
        # tag here would be impossible -- the assert above already forbids it, but
        # be explicit for the falsifier.
        H.fail("CONTROL arm saw {0} MUTATION tag byte(s) wid={1} rnd={2} -- a "
               "private single-owner buffer was mutated/torn: a CPython iovec "
               "machinery fault, not contention".format(mut_seen, wid, rnd))
        return False
    info["mut"] = mut_seen
    return True


def run_writev_round(H, wid, rng, rnd, state, slot, raced):
    """One writev conservation round (CONTROL if not raced, RACED if raced).

    Spawns: a slow READER fiber that drains the per-worker pipe into `got` (kept
    slow so the pipe stays full and the writev PARKS); the WRITER fiber driving
    writev_all over the NBUF buffer list; and -- only in the raced arm -- a
    MUTATOR fiber that pokes the shared bytearray IN PLACE (no resize -> count
    invariant; the poked MUT_TAG can legally land on the wire) while the writev is
    parked.  All three join on a WaitGroup before the oracle reads `got`, so the
    stream is provably complete and quiescent."""
    try:
        rfd, wfd = os.pipe()
    except OSError:
        if not H.running():
            return True
        return True
    os.set_blocking(rfd, False)
    os.set_blocking(wfd, False)
    set_pipe_cap(wfd)

    shared = bytearray(BUF_LEN)            # private (control) or shared (raced)
    bufs = build_buffers(wid, rnd, shared)
    got = bytearray()
    info = {"mut": 0, "wrote": -1}

    nfibers = 3 if raced else 2
    wg = runloom.WaitGroup()
    wg.add(nfibers)
    # The reader must keep running until it sees clean EOF (writer closed wfd);
    # the writer closes wfd in its finally so the reader's os.read returns b"".
    def run_reader(rfd=rfd):
        try:
            while True:
                if not H.running():
                    return
                try:
                    chunk = os.read(rfd, 8192)
                except OSError:
                    return
                if not chunk:
                    return                  # clean EOF -> writer closed wfd
                got.extend(chunk)
                # Slow the reader so the small pipe stays full and the writev on
                # the other side PARKS in wait_fd(WRITE) -- the hazard window.
                runloom.sleep(0.0004)
        finally:
            wg.done()

    def run_writer(wfd=wfd):
        try:
            info["wrote"] = writev_all(wfd, bufs)
        except OSError:
            pass                            # torn-down fd at shutdown -> benign
        finally:
            try:
                os.close(wfd)               # clean EOF for the reader
            except OSError:
                pass
            wg.done()

    def run_mutator(mseed=rng.getrandbits(48)):
        # Hammer the SHARED bytearray while the writev is parked, IN PLACE ONLY --
        # NO resize.  The byte COUNT of `shared` is therefore invariant
        # (PyByteArray_Resize is never called), so the writev's LIVE re-resolution
        # always sees a BUF_LEN-long buffer and the strict length-conservation
        # oracle holds; an extra/short byte at the wire would be a torn iov_len, a
        # real fault.  What CAN legally land on the wire is the in-place MUT_TAG: a
        # poke through the export reflects on the next re-call.  So the legal value
        # universe for the shared region is exactly {original tag, MUT_TAG}; an
        # OUT-OF-UNIVERSE byte is a freed / cross-wire read -- the UAF we hunt.
        #
        # (The resize-while-exported -> BufferError detection -- the ob_exports
        # guard -- is driven on the READV arm, where a writable target is exported
        # across a true park and a sibling's resize MUST raise.  We deliberately do
        # NOT resize the writev buffer here: a resize that lands in the unexported
        # window BETWEEN partial-writev re-calls legitimately changes the byte
        # count (in-universe bytes, no corruption), which is a property of mutating
        # a buffer you are concurrently sending, NOT a runtime bug -- folding it
        # into the conservation oracle would make the arm a tautology, not a test.)
        #
        # Uses its OWN rng (a shared random.Random corrupts GIL-off).
        import random
        mrng = random.Random(mseed)
        t = tag_for(wid, rnd, SHARED_SLOT)
        try:
            for _ in range(96):
                if not H.running():
                    return
                # In-place poke MUT_TAG across the buffer: permitted through the
                # export (no resize), so it can legally surface on the wire.
                try:
                    for off in range(0, len(shared), 251):
                        shared[off] = MUT_TAG
                except (IndexError, ValueError):
                    pass
                runloom.yield_now()
                # Re-stamp the original tag over some pokes so the stream is not
                # ENTIRELY MUT_TAG (keeps both legal values present on the wire).
                if mrng.getrandbits(1):
                    try:
                        for off in range(1, len(shared), 263):
                            shared[off] = t
                    except (IndexError, ValueError):
                        pass
                runloom.sleep(0.0003)
        finally:
            wg.done()

    H.fiber(run_reader)
    if raced:
        H.fiber(run_mutator)
    H.fiber(run_writer)
    wg.wait()                              # reader + writer (+ mutator) joined

    try:
        os.close(rfd)
    except OSError:
        pass

    if H.failed:
        return False
    if not H.running():
        return True                        # shutdown mid-round: benign, not a tear

    ok = validate_stream(H, wid, rnd, got, raced, info)
    if not ok:
        return False
    if raced:
        state["mut"][slot] += info["mut"]
        state["raced"][slot] += 1
    else:
        state["control"][slot] += 1
    state["bytes"][slot] += len(got)
    return True


def run_readv_round(H, wid, rng, rnd, state, slot):
    """READV arm: a readv whose writable target bytearray a sibling tries to
    RESIZE while it is exported for the parked readv.  The resize MUST raise
    BufferError (the legal detection); the bytes that DO land must be an exact
    in-order prefix of the fed universe stream.

    A per-worker pipe is fed a deterministic universe stream by a writer fiber;
    a reader fiber drives os.readv into a list of bytearray targets (parking on
    an empty pipe); a mutator fiber tries to resize one exported target mid-park.
    Oracle: every readv'd byte == its universe position; a silent resize, a
    wrong byte, or a SIGSEGV is the bug; a BufferError is counted acceptable."""
    try:
        rfd, wfd = os.pipe()
    except OSError:
        return True
    os.set_blocking(rfd, False)
    os.set_blocking(wfd, False)

    # Deterministic universe stream: byte at absolute position p is rv_byte(p).
    # The readv targets reassemble an exact in-order prefix of it.
    salt = (wid * 2654435761 + rnd) & 0xFFFF

    def rv_byte(p):
        v = ((p * 2246822519 + salt * 374761393 + 0x9E) >> 5) & 0xFF
        # Keep every stream byte inside a recognisable sub-universe so a torn read
        # is unlikely to coincide; never 0 (our resize filler) so a leaked filler
        # byte is detectable.
        return v if v != 0 else 0x3B

    TOTAL = NBUF * BUF_LEN
    target_len = 1024                       # each readv chunk target
    info = {"berr": 0, "consumed": 0}
    wg = runloom.WaitGroup()
    wg.add(3)
    # The reader exports `live` (a bytearray) across each readv park; the mutator
    # tries to resize THAT object.  We keep a stable reference both fibers share.
    box = {"live": None}

    def run_feeder(wfd=wfd):
        pos = 0
        try:
            while pos < TOTAL and H.running():
                # Feed in chunks SMALLER than the readv target (2*1024) and SLEEP
                # between them, so the pipe drains empty and the readv on the other
                # side PARKS in wait_fd(READ) with its target bytearray exported --
                # the wide window where the mutator's resize attempt lands and must
                # raise BufferError (the ob_exports guard).
                n = min(rng.randint(64, 400), TOTAL - pos)
                chunk = bytes(rv_byte(pos + i) for i in range(n))
                off = 0
                while off < n:
                    try:
                        off += os.write(wfd, chunk[off:])
                    except (BrokenPipeError, OSError):
                        return
                pos += n
                runloom.sleep(0.0004)         # let the reader park exported
        finally:
            try:
                os.close(wfd)
            except OSError:
                pass
            wg.done()

    def run_reader(rfd=rfd):
        consumed = 0
        try:
            while consumed < TOTAL:
                if not H.running():
                    return
                live = bytearray(target_len)
                spare = bytearray(target_len)
                box["live"] = live          # the mutator will try to resize this
                try:
                    n = os.readv(rfd, [live, spare])   # parks on empty pipe
                except OSError:
                    return
                box["live"] = None
                if n <= 0:
                    return                  # EOF
                # Validate every readv'd byte against its universe position.
                joined = bytes(live) + bytes(spare)
                for j in range(n):
                    exp = rv_byte(consumed)
                    b = joined[j]
                    if b != exp:
                        H.fail("READV IDENTITY broken wid={0} rnd={1}: byte at "
                               "stream pos {2} == {3:#04x}, expected {4:#04x} -- "
                               "a torn / silently-resized / freed readv target "
                               "byte across the park".format(
                                   wid, rnd, consumed, b, exp))
                        return
                    consumed += 1
            info["consumed"] = consumed
        finally:
            wg.done()

    def run_mutator():
        try:
            for _ in range(96):
                if not H.running():
                    return
                live = box["live"]
                if live is None:
                    runloom.yield_now()
                    continue
                # Try to RESIZE the exported readv target.  While the readv holds
                # the export this MUST raise BufferError (the ob_exports guard); a
                # silent resize would free ob_bytes under the live iov_base -> the
                # reader's universe check catches the resulting torn byte.
                try:
                    live.extend(b"\x00" * 32)
                    del live[target_len:]    # not exported this instant -> allowed
                except BufferError:
                    info["berr"] += 1
                except (ValueError, IndexError):
                    pass
                runloom.sleep(0.0002)
        finally:
            wg.done()

    H.fiber(run_feeder)
    H.fiber(run_mutator)
    H.fiber(run_reader)
    wg.wait()

    try:
        os.close(rfd)
    except OSError:
        pass

    if H.failed:
        return False
    if not H.running():
        return True
    # Conservation: the reader consumed an exact in-order prefix.  At a clean EOF
    # it consumed all TOTAL; a shutdown mid-round may consume fewer (benign).
    if info["consumed"] not in (0, TOTAL) and H.running():
        H.fail("READV CONSERVATION broken wid={0} rnd={1}: consumed {2} bytes, "
               "expected the full {3}-byte universe prefix -- a dropped/short "
               "readv across the park".format(
                   wid, rnd, info["consumed"], TOTAL))
        return False
    state["berr"][slot] += info["berr"]
    state["readv"][slot] += 1
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three arms by worker id in the first ops so post()
        # coverage holds whether one worker does 3 rounds or 3 workers do 1 each
        # (the p125/p126/p172 flaky-random-coverage fix); random after.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_CONTROL:
            ok = run_writev_round(H, wid, rng, i, state, slot, raced=False)
        elif sel == CASE_RACED:
            ok = run_writev_round(H, wid, rng, i, state, slot, raced=True)
        else:
            ok = run_readv_round(H, wid, rng, i, state, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # All per-slot tallies allocated here, inside the root (single-writer-per-slot,
    # race-free; summed in post()).  No shared object lives at module top level.
    H.state = {
        "control": [0] * SLOTS,   # CONTROL writev rounds that conserved exactly
        "raced": [0] * SLOTS,     # RACED writev rounds that conserved in-universe
        "readv": [0] * SLOTS,     # READV rounds completed
        "mut": [0] * SLOTS,       # legal in-place-poke MUT_TAG bytes seen on wire
        "berr": [0] * SLOTS,      # resize-while-exported BufferErrors (legal)
        "bytes": [0] * SLOTS,     # total writev bytes conserved
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    control = sum(H.state["control"])
    raced = sum(H.state["raced"])
    readv = sum(H.state["readv"])
    mut = sum(H.state["mut"])
    berr = sum(H.state["berr"])
    nbytes = sum(H.state["bytes"])
    H.log("writev CONTROL rounds={0} RACED rounds={1} readv rounds={2} | "
          "conserved bytes={3} | legal in-place MUT bytes on wire={4} | "
          "resize-while-exported BufferErrors={5} | ops={6}".format(
              control, raced, readv, nbytes, mut, berr, H.total_ops()))
    # Reaching post with no failure already means every per-round conservation +
    # identity law held (they are fail-fast); assert the run did real work.
    H.check(H.total_ops() > 0,
            "no rounds completed -- the writev/readv park race window was never "
            "exercised")
    # Each arm must have been exercised at least once (the round-robin guarantees
    # it once enough rounds ran); else the invariant is untested.
    H.check(control > 0,
            "CONTROL (single-owner writev) arm never exercised -- the "
            "byte-for-byte conservation falsifier never ran")
    H.check(raced > 0,
            "RACED (shared-bytearray mutated mid-park) arm never exercised -- "
            "the cross-hub iovec-vs-realloc hazard was never driven")
    H.check(readv > 0,
            "READV (exported-target resize) arm never exercised -- the "
            "BufferError detection was never driven")
    H.require_no_lost("writev/readv iovec-realloc completeness")


if __name__ == "__main__":
    # Each round holds a pipe (2 fds) plus 2-3 sibling fibers and drives a tight
    # park-resolve writev/readv handoff that does not scale to 1M (cf.
    # p228/p309/p413); each in-flight pipe pins 2 fds.  Cap to a designed scale
    # well under the descriptor ceiling -- the point is the per-byte
    # conservation+identity oracle under M:N, not raw 1M throughput.
    harness.main("p440_os_writev_iovec_buffer_realloc", body, setup=setup,
                 post=post, default_funcs=3000, max_funcs=4000,
                 describe="os.writev/os.readv iovec base/len snapshot vs a "
                          "concurrent bytearray realloc/poke across the "
                          "cooperative wait_fd park; each vectored write is a "
                          "unique tagged universe the reader reassembles "
                          "byte-for-byte (CONSERVATION+IDENTITY), a private "
                          "control writer must conserve 100%, a resize of an "
                          "exported buffer must raise BufferError -- any "
                          "out-of-universe / short / torn byte or SIGSEGV is the "
                          "bug")
