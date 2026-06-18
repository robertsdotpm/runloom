"""big_100 / 222 -- io_uring file read/write funnel.

The file-I/O primitives runloom_c.file_read / runloom_c.file_write route through
io_uring on Linux>=5.1 (pread/pwrite fallback otherwise).  Every existing file
program (p16-p23, p92) drives only the blocking-offload pool via the monkey
layer -- the io_uring file submission/completion path and its documented global
ring sub_lock (R7) funnel are never exercised.  This program drives them
directly.

setup() pre-creates N data files, each filled with a deterministic per-file byte
pattern (file i, block b -> a repeated 16-byte struct of (file_id, block_idx)).
Each goroutine opens ITS file (os.open) and runs two phases:
  * read phase  -- R rounds of file_read(fd, buf, n, offset) at random block
                   offsets, verifying every byte matches the deterministic
                   pattern for that (file, offset) -- catching a misrouted
                   completion (wrong g gets the CQE), a wrong offset, or a
                   cross-g buffer mixup;
  * write phase -- file_write(fd, payload, offset) into the goroutine's OWN
                   reserved tail region, then file_read it back and assert the
                   read-back equals what was written.
Reads can be short (file_read returns < n at EOF), so the oracle compares
buf[:got] against the expected prefix and re-issues for the remainder.  Run with
many --hubs to load the R7 sub_lock with concurrent submissions.

Stresses: Stresses: runloom_c.file_read/file_write io_uring submission +
completion routing and the global-ring sub_lock (R7) funnel at high hub count;
offset correctness, short-read/short-write handling, and per-g data integrity
under concurrent file I/O.
"""
import os
import struct
import sys

import harness
import runloom_c


# Per-file deterministic pattern.  Each 16-byte block at block index b in file
# `fid` is the little-endian struct (fid, b) repeated -> 16 bytes.  The whole
# file is BLOCKS such blocks, so any byte at absolute offset `off` is fully
# determined by (fid, off) with no goroutine-local state -- which is exactly
# what lets the read oracle catch a completion routed to the wrong goroutine.
BLOCK = 16
BLOCKS = 256                       # 256 * 16 = 4096-byte read region per file
READ_REGION = BLOCK * BLOCKS       # bytes of fixed pattern (read-only phase)
WRITE_REGION = 4096                # per-file tail region the worker owns/writes
FILE_SIZE = READ_REGION + WRITE_REGION

_FILES = "files"


def pattern_block(fid, b):
    """The deterministic 16-byte content of block b in file fid."""
    return struct.pack("<qq", fid, b)


def expected_bytes(fid, off, n):
    """The expected READ-region bytes for file fid over [off, off+n)."""
    out = bytearray()
    pos = off
    end = off + n
    while pos < end:
        b = pos // BLOCK
        blk = pattern_block(fid, b)
        within = pos % BLOCK
        take = min(BLOCK - within, end - pos)
        out += blk[within:within + take]
        pos += take
    return bytes(out)


def setup(H):
    base = H.make_tmpdir("big100_iouring_file_")
    fdir = os.path.join(base, _FILES)
    os.makedirs(fdir, exist_ok=True)

    avail = bool(runloom_c.iouring_available())
    H.log("io_uring file path: {0}".format(
        "io_uring (Linux>=5.1)" if avail else "pread/pwrite fallback"))

    # Pre-create one data file per goroutine, filled with its read-region
    # pattern followed by a zeroed write region.  Written here with plain
    # os.write (setup runs once, no need to drive the feature) so the workload
    # phase reads a known-good baseline.
    nfiles = max(1, H.funcs)
    for fid in range(nfiles):
        path = os.path.join(fdir, "f{0}.dat".format(fid))
        buf = bytearray()
        for b in range(BLOCKS):
            buf += pattern_block(fid, b)
        buf += b"\x00" * WRITE_REGION
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            mv = memoryview(buf)
            done = 0
            while done < len(mv):
                done += os.write(fd, mv[done:])
        finally:
            os.close(fd)

    H.state = {"dir": fdir, "nfiles": nfiles, "iouring": avail}


def read_full(fd, buf, n, offset):
    """file_read into buf[:n] at offset, re-issuing on short reads until n
    bytes are read or EOF.  Returns the number of bytes actually read."""
    got = 0
    mv = memoryview(buf)
    while got < n:
        r = runloom_c.file_read(fd, mv[got:n], n - got, offset + got)
        if r == 0:                 # genuine EOF / no progress
            break
        got += r
    return got


def write_full(fd, payload, offset):
    """file_write the whole payload at offset, re-issuing on short writes."""
    done = 0
    mv = memoryview(payload)
    total = len(payload)
    while done < total:
        w = runloom_c.file_write(fd, bytes(mv[done:]), offset + done)
        if w <= 0:
            break
        done += w
    return done


def worker(H, wid, rng, state):
    # One pre-created data file per goroutine (setup made H.funcs of them), so
    # each goroutine OWNS its file exclusively -- the write-phase readback oracle
    # needs no cross-g coordination and a misrouted completion shows up as this
    # g reading bytes it never wrote.  Guard the modulo for the rare case where
    # --funcs was capped below the spawned pool.
    nfiles = state["nfiles"]
    fid = wid if wid < nfiles else wid % nfiles
    path = os.path.join(state["dir"], "f{0}.dat".format(fid))

    # Stagger the opening connect-storm-equivalent so the ring sub_lock sees a
    # realistic concurrent submission mix rather than a single instantaneous
    # spike at t0.
    H.sleep(rng.random() * 0.5)

    fd = None
    try:
        fd = os.open(path, os.O_RDWR)
        buf = bytearray(READ_REGION)
        for _ in H.round_range():
            # ---- read phase: random block-aligned offset + random length ----
            # Block-aligned offsets keep the expected-pattern check simple; the
            # length spans several blocks and may run to EOF (short read).
            blk = rng.randrange(BLOCKS)
            off = blk * BLOCK
            maxn = READ_REGION - off
            n = rng.randint(1, maxn)
            got = read_full(fd, buf, n, off)
            # Within the read region the file is fully populated, so a read that
            # starts inside it must return exactly n bytes.
            if not H.check(got == n,
                           "short read wid={0} fid={1} off={2} "
                           "got={3}!={4}".format(wid, fid, off, got, n)):
                return
            expect = expected_bytes(fid, off, n)
            if not H.check(bytes(buf[:n]) == expect,
                           "read pattern mismatch wid={0} fid={1} off={2} "
                           "n={3}".format(wid, fid, off, n)):
                return
            H.op(wid)

            # ---- write phase: write into THIS file's reserved tail region ----
            # The worker owns its file, so the whole tail region [READ_REGION,
            # FILE_SIZE) is private.  Payload encodes (wid, seq) so a cross-g
            # buffer mixup or a misrouted write completion (another g's CQE
            # landing here) is caught on readback.  A random in-region offset
            # exercises varied io_uring submission offsets.
            seq = rng.randrange(1 << 30)
            payload = struct.pack("<II", wid & 0xFFFFFFFF, seq) * 4   # 32 bytes
            woff = READ_REGION + rng.randrange(WRITE_REGION - len(payload) + 1)
            wrote = write_full(fd, payload, woff)
            if not H.check(wrote == len(payload),
                           "short write wid={0} fid={1} woff={2} "
                           "wrote={3}".format(wid, fid, woff, wrote)):
                return
            rb = bytearray(len(payload))
            rgot = read_full(fd, rb, len(payload), woff)
            if not H.check(rgot == len(payload) and bytes(rb) == payload,
                           "write-readback mismatch wid={0} fid={1} woff={2} "
                           "rgot={3}".format(wid, fid, woff, rgot)):
                return
            H.op(wid)
            H.task_done(wid)
    except OSError as e:
        if not H.running():
            return
        H.fail("file I/O error wid={0} fid={1}: {2}".format(wid, fid, e))
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    # Guard: the file_read/file_write symbols only exist on a runloom build that
    # has the fd-I/O module; on non-Linux the io_uring path is absent but the
    # pread/pwrite fallback is still the correctness oracle, so we run anywhere
    # the symbols exist.  Only SKIP when the API itself is missing.
    if not hasattr(runloom_c, "file_read") or \
            not hasattr(runloom_c, "file_write"):
        print("SKIP: runloom_c.file_read/file_write unavailable in this build")
        sys.exit(0)
    harness.main("p222_iouring_file_rw_funnel", body, setup=setup,
                 default_funcs=4000,
                 describe="io_uring file_read/file_write submission + R7 "
                          "sub_lock funnel; offset/short-IO/per-g integrity")
