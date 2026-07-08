"""big_100 / 413 -- cooperative _pyio.BufferedReader over a per-worker pipe;
exact in-order prefix conservation across the refill park.

Nothing in the corpus drives a BUFFERED file object across a cooperative refill
park: p228/p309 read raw pipe fds with os.read (no Python-level buffer), and the
C io.BufferedReader can't be made cooperative at all (io.FileIO issues its
read() syscall directly in C, OS-blocking the hub -- see monkey/__init__.py).
The interesting object is the PURE-PYTHON _pyio.BufferedReader, which runloom's
patched open(pollable_fd) hands back exactly so a buffered read on a pipe parks
the fiber instead of wedging the hub.  _pyio.BufferedReader keeps ONE internal
`_read_buf` bytearray and a `_read_pos` cursor, and it mutates BOTH across each
cooperative `os.read` park inside `_peek_unlocked` / `_read_unlocked`:

    current = self.raw.read(to_read)      # <-- parks here (cooperative os.read)
    self._read_buf = self._read_buf[self._read_pos:] + current
    self._read_pos = 0

So a preempt or a cross-fiber touch landing in that park can tear the
(_read_buf, _read_pos) pair and double-return, drop, or reorder bytes -- the
classic buffer-export-vs-mutation / preempt-mid-refill FT hazard.

We make that EXACTLY falsifiable with a per-worker pipe and a closed-world
stream oracle.  Each worker:

  * owns its OWN os.pipe() and wraps the READ end in a cooperative
    _pyio.BufferedReader (CoopRaw routes readinto through the PATCHED os.read so
    a refill parks on the pipe's netpoll arm -- the raw _pyio.FileIO would use
    os.readinto, which runloom does NOT patch, so it would NOT park);
  * spawns a sibling FEEDER fiber on the hubs that writes a deterministic byte
    stream -- byte at absolute position p is stream_byte(p, salt) -- in small
    chunks with a yield/sleep between them, so each write lands while the reader
    is parked mid-refill;
  * drains via a per-round mix of peek()+read / read1(n) / read(n) / readline(),
    accumulating bytes, and CHECKS every drained byte against stream_byte(its
    absolute position) -- because the pipe is per-worker, the drained bytes must
    be an EXACT, in-order, gap-free PREFIX of the fed stream.

A torn _read_buf / _read_pos across the refill park shows up immediately as:
  * a VALUE mismatch (byte at position p != stream_byte(p))  -> reorder / torn,
  * the prefix running PAST what was fed                     -> duplicated byte,
  * the prefix ending SHORT with the feeder long done + EOF  -> dropped byte.
Each is a hard, content-checksummed invariant break, not a heuristic.

The four drain methods are ROUND-ROBINED by worker id in the first ops (then
random) so post() can assert each was exercised without the flaky-random-
coverage bug the suite hit in p125/p126/p172.

Invariant (hot, fail-fast): every drained byte == stream_byte(absolute pos);
total drained == bytes fed at clean EOF (no drop, no dup); post: each of the 4
drain methods exercised, no worker LOST.

Stresses: pure-Python _pyio.BufferedReader _read_buf/_read_pos mutation across a
cooperative os.read refill park, buffer-export-vs-mutation, preempt-mid-refill,
cross-hub feeder vs reader on one buffered stream, exact prefix conservation.

Good TSan / controlled-M:N-replay target: the _read_buf rebuild + _read_pos
reset straddling the os.read park is a textbook publish-after-park; a TSan report
on the bytearray write/read usually localizes a tear before the position oracle
even fires.
"""
import os
import sys

# ---- availability guard (POSIX-only: pipe fds must be pollable) ------------
# Windows pipe fds are not pollable by the netpoll backend, so a buffered read
# on an os.pipe() read end can't park cooperatively there.  Detect-and-skip.
POSIX = sys.platform.startswith(("linux", "darwin", "freebsd"))
if not POSIX:
    print("SKIP: POSIX-only (raw pipe fds not pollable on this platform: "
          "{0})".format(sys.platform))
    sys.exit(0)

import _pyio

import harness
import runloom

# Per-worker fed-stream length in bytes.  Big enough to force the buffered
# reader through MANY refills (each refill is a fresh os.read park where a tear
# can land), small enough that thousands of concurrent workers each finish a
# round inside the deadline.  buffer_size below is deliberately smaller so the
# stream spans several buffer rebuilds.
STREAM_LEN = 4096
BUFFER_SIZE = 256

# Feeder chunk sizes: small + mixed so writes straddle buffer boundaries and the
# reader parks mid-refill between them.  A chunk smaller than the buffer means
# _peek_unlocked/_read_unlocked must loop+park to satisfy a larger read.
CHUNK_MIN = 7
CHUNK_MAX = 61

# Drain methods, round-robined by worker id for guaranteed post() coverage.
M_PEEK_READ = 0       # peek(k) then read(k): peek refills, read drains the peeked bytes
M_READ1 = 1           # read1(n): at most one raw read per call
M_READ_N = 2          # read(n): may loop+park across several refills to get n bytes
M_READLINE = 3        # readline(): drains to the next NEWLINE_BYTE (refills as needed)
NUM_METHODS = 4

# A sentinel newline value injected at known positions so readline() has real
# line boundaries to find.  We pick a byte value the stream generator never
# produces on its own, then OVERLAY it at fixed-period positions, and teach the
# oracle about it -- so a readline drain is still validated byte-for-byte.
NEWLINE_BYTE = 0x0A
NEWLINE_PERIOD = 37   # every Nth absolute position is forced to NEWLINE_BYTE


def stream_byte(pos, salt):
    """The deterministic byte at absolute stream position `pos` for a worker
    whose stream `salt` is fixed for the round.  Every NEWLINE_PERIOD-th position
    is forced to NEWLINE_BYTE so readline() finds line breaks; otherwise it is a
    salted mix that NEVER equals NEWLINE_BYTE (so a non-boundary byte can't be
    mistaken for a boundary).  A drained byte at position p that does not equal
    this is a TORN/REORDERED byte; this is the whole oracle."""
    if pos % NEWLINE_PERIOD == NEWLINE_PERIOD - 1:
        return NEWLINE_BYTE
    v = ((pos * 1103515245 + salt * 12345 + 0x2B) >> 3) & 0xFF
    if v == NEWLINE_BYTE:
        v ^= 0x80           # keep non-boundary bytes distinct from the newline
    return v


class CoopRaw(_pyio.RawIOBase):
    """A raw IO layer whose readinto() routes through the PATCHED cooperative
    os.read, so a buffered refill PARKS the fiber on the pipe's netpoll arm
    (wait_fd) instead of OS-blocking the hub or returning short on EAGAIN.

    This is the cooperative analogue of _pyio.FileIO: _pyio.FileIO.readinto uses
    os.readinto (which runloom does NOT patch -> would not park / would return
    None on EAGAIN), so we substitute os.read here.  Wrapped in a
    _pyio.BufferedReader, this gives the exact pure-Python _read_buf/_read_pos
    mutate-across-park the test is about."""

    def __init__(self, fd):
        self.rfd = fd

    def readable(self):
        return True

    def readinto(self, buf):
        # Patched os.read parks until >=1 byte is readable, then returns it
        # (b"" only at real EOF -- the write end closed).  Copy into the caller's
        # buffer; returning 0 signals EOF to _pyio.BufferedReader.
        data = os.read(self.rfd, len(buf))
        n = len(data)
        if n:
            buf[:n] = data
        return n

    def fileno(self):
        return self.rfd

    def close(self):
        # RawIOBase.close() has no fd to close, so own rfd here: br.close() ->
        # raw.close() -> os.close(rfd).  Without this the read end leaks one fd
        # per round (the buffered reader never owns the raw fd).
        if self.rfd >= 0:
            try:
                os.close(self.rfd)
            except OSError:
                pass
            self.rfd = -1
        super().close()


def feeder(H, wfd, salt, total, rng):
    """Write the deterministic stream [0, total) into the pipe in small chunks,
    yielding between them so each write lands while the reader is parked
    mid-refill, then close the write end so the reader sees a clean EOF.  Uses
    its OWN rng stream (a shared random.Random corrupts GIL-off)."""
    pos = 0
    try:
        while pos < total and H.running():
            n = min(rng.randint(CHUNK_MIN, CHUNK_MAX), total - pos)
            chunk = bytes(stream_byte(pos + i, salt) for i in range(n))
            off = 0
            while off < n:
                try:
                    off += os.write(wfd, chunk[off:])
                except BrokenPipeError:
                    return                  # reader gone (shutdown) -> stop
                except OSError:
                    return
            pos += n
            # Yield so the reader is the one parked in os.read when the NEXT
            # write arrives -- this is what lands the feed inside the refill park.
            runloom.yield_now()
    finally:
        try:
            os.close(wfd)                   # clean EOF -> reader's os.read -> b""
        except OSError:
            pass


def drain_round(H, wid, br, salt, total, rng, method, counts, slot):
    """Drain the whole per-worker stream through `br` using one drain METHOD,
    validating every byte against stream_byte(absolute position).  Returns True
    on a clean, exact, gap-free prefix that consumed exactly `total` bytes;
    H.fail (and returns False) on any torn/dropped/duplicated byte."""
    consumed = 0                            # == next expected absolute position
    while consumed < total:
        if not H.running():
            return True                     # shutdown mid-drain: benign, not a tear
        try:
            if method == M_PEEK_READ:
                # peek(k) refills the buffer without advancing _read_pos; read(k)
                # then drains exactly those bytes.  A tear between the peek's
                # buffer rebuild and the read's cursor advance double-returns or
                # drops.
                want = rng.randint(1, 96)
                pk = br.peek(want)
                k = min(want, len(pk))
                if k <= 0:
                    chunk = br.read(want)   # buffer empty -> force a refill+drain
                else:
                    chunk = br.read(k)
            elif method == M_READ1:
                chunk = br.read1(rng.randint(1, 128))
            elif method == M_READ_N:
                # read(n) may loop+park across SEVERAL refills to assemble n
                # bytes -- the deepest mutate-across-park path.
                chunk = br.read(rng.randint(1, 200))
            else:  # M_READLINE
                chunk = br.readline()       # drains to the next NEWLINE_BYTE
        except OSError:
            if not H.running():
                return True                 # fd torn out at shutdown -> benign
            H.fail("drain raised OSError mid-stream wid={0} method={1} pos={2}"
                   .format(wid, method, consumed))
            return False
        if not chunk:
            if not H.running():
                return True             # deadline expired mid-round: the feeder's
                                        # `while pos < total and H.running()` loop
                                        # stops and closes wfd, so this EOF is a
                                        # benign truncation, NOT a dropped byte.
                                        # Mirrors the OSError branch above.
            # Empty before reaching `total` WHILE THE RUN IS LIVE: the feeder only
            # closes wfd after feeding all `total` bytes (its loop exits on
            # pos>=total), so an EOF/empty here means a byte was genuinely lost
            # across the refill park (a real tear), not a deadline truncation.
            H.fail("drain hit EOF/empty at pos {0} of {1} wid={2} method={3} -- "
                   "the buffered prefix ended SHORT (a DROPPED byte across the "
                   "refill park)".format(consumed, total, wid, method))
            return False
        # Validate every byte of `chunk` against its absolute stream position.
        for b in chunk:
            if consumed >= total:
                H.fail("drain ran PAST the fed stream: extra byte {0!r} at pos "
                       "{1} (total {2}) wid={3} method={4} -- a DUPLICATED byte "
                       "from a torn _read_buf/_read_pos".format(
                           b, consumed, total, wid, method))
                return False
            exp = stream_byte(consumed, salt)
            if b != exp:
                H.fail("drained byte MISMATCH at pos {0}: got {1!r} want {2!r} "
                       "wid={3} method={4} -- a REORDERED/torn byte across the "
                       "cooperative refill park (buffer-export-vs-mutation)"
                       .format(consumed, b, exp, wid, method))
                return False
            consumed += 1
    # Exact prefix: we consumed precisely `total` bytes, every one in order.
    counts[method][slot] += 1
    return True


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & 1023
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the 4 drain methods by worker id in the first ops so post()
        # coverage holds whether one worker does 4 rounds or 4 workers do 1 each;
        # random after that to preserve the concurrent mix (the suite's
        # p125/p126/p172 flaky-random-coverage fix).
        if i < NUM_METHODS:
            method = (wid + i) % NUM_METHODS
        else:
            method = rng.randint(0, NUM_METHODS - 1)
        i += 1

        try:
            rfd, wfd = os.pipe()
        except OSError:
            if not H.running():
                break
            continue
        os.set_blocking(rfd, False)         # cooperative os.read parks via wait_fd

        # Per-round stream salt: distinct streams across rounds/workers so a
        # cross-talk byte from another pipe (which must not happen on a per-worker
        # pipe) would be detectable as an out-of-stream value.
        salt = rng.getrandbits(24) ^ (wid * 2654435761 & 0xFFFFFF)
        fseed = rng.getrandbits(48)

        # Cooperative buffered reader over the read end.  BUFFER_SIZE < STREAM_LEN
        # forces many refills (parks); peek/read1/read/readline all mutate the one
        # _read_buf/_read_pos across those parks.
        br = _pyio.BufferedReader(CoopRaw(rfd), buffer_size=BUFFER_SIZE)

        wg = runloom.WaitGroup()
        wg.add(1)

        def run_feeder(wfd=wfd, salt=salt, fseed=fseed):
            import random
            frng = random.Random(fseed)
            try:
                feeder(H, wfd, salt, STREAM_LEN, frng)
            finally:
                wg.done()

        H.fiber(run_feeder)

        ok = drain_round(H, wid, br, salt, STREAM_LEN, rng, method, counts, slot)

        # Drain reader done; make sure the feeder has closed its write end before
        # we tear down so we don't leave a parked write or an orphan fiber.
        wg.wait()
        try:
            br.close()                      # closes rfd (CoopRaw owns it)
        except OSError:
            pass

        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {"counts": {
        M_PEEK_READ: [0] * 1024,
        M_READ1: [0] * 1024,
        M_READ_N: [0] * 1024,
        M_READLINE: [0] * 1024,
    }}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    counts = H.state["counts"]
    pk = sum(counts[M_PEEK_READ])
    r1 = sum(counts[M_READ1])
    rn = sum(counts[M_READ_N])
    rl = sum(counts[M_READLINE])
    H.log("clean drains: peek+read={0} read1={1} read(n)={2} readline={3} "
          "ops={4}".format(pk, r1, rn, rl, H.total_ops()))
    # Coverage/reachability asserts: use require_coverage (completion-aware) so a
    # CPU-starved run that couldn't complete enough rounds to exercise a method
    # is a benign SCALE LIMIT (exit 4), not a false CRASH.  A run WITH the budget
    # (>=half the workers finished in-window) that still misses a method is a real
    # coverage gap and FAILs.
    H.require_coverage(H.total_ops() > 0,
                       "no round completed -- the buffered refill-park drain never ran")
    H.require_coverage(pk > 0, "peek+read drain method never exercised")
    H.require_coverage(r1 > 0, "read1 drain method never exercised")
    H.require_coverage(rn > 0, "read(n) drain method never exercised")
    H.require_coverage(rl > 0, "readline drain method never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    # Each worker holds a pipe (2 fds) + a buffered reader + a feeder sibling for
    # the lifetime of a round, and the drain is a tight park-resolve handoff that
    # does not scale to 1M.  Cap to a designed scale well under the descriptor
    # ceiling (cf. p228/p309) -- the point is the per-byte tear oracle under M:N,
    # not raw 1M throughput.
    harness.main("p413_bufferedreader_pipe_shared_buf", body, setup=setup,
                 post=post, default_funcs=3000, max_funcs=4000,
                 describe="cooperative _pyio.BufferedReader over a per-worker "
                          "pipe; every drained byte == stream_byte(its absolute "
                          "position) -- an exact in-order gap-free prefix, or a "
                          "torn _read_buf/_read_pos (drop/dup/reorder) under M:N")
