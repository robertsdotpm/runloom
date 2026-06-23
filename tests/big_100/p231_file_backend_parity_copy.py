"""big_100 / 231 -- file-I/O backend parity copy+verify.

Beyond raw file_read/file_write coverage, this flips the file-I/O
*implementation* and re-asserts a baseline file invariant across both backends:
the io_uring file path (runloom_c.file_read / file_write, used on Linux >=5.1)
vs the blockpool pread/pwrite offload path that p16-p23 ride
(runloom_c.blocking(os.pread / os.pwrite, ...)).  Each goroutine owns a source
file pre-filled with a deterministic per-file pattern and copies it to a dest in
fixed-size chunks, advancing an explicit offset and looping over short
reads/writes.  It runs the IDENTICAL workload through BOTH backends and asserts:

  * the io_uring-copied dest is byte-identical to the known source pattern AND
    exactly the right size (a wrong offset or a dropped tail = mismatch),
  * the pread/pwrite-copied dest is likewise byte-identical, and
  * the two backends produce byte-identical dest files (the parity oracle),

so any divergence in offset semantics, partial-I/O handling, or error mapping
between the two file paths is caught.  When io_uring isn't built the file_read
path simply falls back to pread, so both modes use the same syscall and parity
holds trivially -- we log that and still run the correctness half.  If the
file_read attribute is missing entirely the program SKIPs clean (exit 0).

Stresses: Stresses: file-I/O backend parity -- the same large-file copy+verify
workload run through runloom_c.file_read/file_write (io_uring on >=5.1) and
re-asserted against the blockpool/os.pread path, checking identical bytes,
offset semantics, and partial-read/write handling.
"""
import os

import harness
import runloom_c


# Per-file deterministic pattern: a wid-keyed byte stream so a wrong offset or a
# dropped tail shifts/truncates the bytes and the verify catches it.  Cheap to
# regenerate (no big buffer kept around between phases).
def make_pattern(wid, size):
    seed = (wid * 2654435761 + 0x9E3779B9) & 0xFFFFFFFF
    out = bytearray(size)
    x = seed | 1
    for i in range(size):
        # xorshift32 -> one byte; deterministic + position-sensitive.
        x ^= (x << 13) & 0xFFFFFFFF
        x ^= x >> 17
        x ^= (x << 5) & 0xFFFFFFFF
        out[i] = (x ^ (i * 31 + wid)) & 0xFF
    return bytes(out)


def file_size_for(rng):
    # Varied sizes incl. ones that don't divide the chunk size, so the tail is a
    # short read/write that must be handled by the copy loop.  Bounded (<=256KB)
    # so memory stays flat at scale.
    pick = rng.random()
    if pick < 0.25:
        return rng.randint(0, 4096)              # tiny / sub-chunk / empty
    if pick < 0.75:
        return rng.randint(4097, 65536)          # mid
    return rng.randint(65537, 262144)            # large, multi-chunk


CHUNK = 8192


def copy_iouring(src_fd, dst_fd, size):
    """Copy size bytes src->dst via the io_uring file path, advancing an
    explicit offset and looping over short reads/writes."""
    buf = bytearray(CHUNK)
    off = 0
    while off < size:
        want = min(CHUNK, size - off)
        # Read up to `want` at offset `off`; loop on short reads.
        got = runloom_c.file_read(src_fd, buf, want, off)
        if got <= 0:
            raise OSError("short/EOF read at off={0} (got {1})".format(off, got))
        # Write exactly the bytes we read at the same offset; loop on shorts.
        wtotal = 0
        while wtotal < got:
            mv = bytes(buf[wtotal:got])
            wrote = runloom_c.file_write(dst_fd, mv, off + wtotal)
            if wrote <= 0:
                raise OSError("short write at off={0}".format(off + wtotal))
            wtotal += wrote
        off += got


def copy_blockpool(src_fd, dst_fd, size):
    """Same copy via the blockpool offload path (os.pread / os.pwrite run
    through runloom_c.blocking so they don't stall a hub thread)."""
    off = 0
    while off < size:
        want = min(CHUNK, size - off)
        chunk = runloom_c.blocking(os.pread, src_fd, want, off)
        if not chunk:
            raise OSError("short/EOF pread at off={0}".format(off))
        wtotal = 0
        while wtotal < len(chunk):
            wrote = runloom_c.blocking(os.pwrite, dst_fd, chunk[wtotal:], off + wtotal)
            if wrote <= 0:
                raise OSError("short pwrite at off={0}".format(off + wtotal))
            wtotal += wrote
        off += len(chunk)


def read_back_iouring(fd, size):
    """Read the whole dest back via the io_uring file path into one bytes."""
    out = bytearray(size)
    buf = bytearray(CHUNK)
    off = 0
    while off < size:
        want = min(CHUNK, size - off)
        got = runloom_c.file_read(fd, buf, want, off)
        if got <= 0:
            raise OSError("short/EOF readback at off={0}".format(off))
        out[off:off + got] = buf[:got]
        off += got
    return bytes(out)


def setup(H):
    base = H.make_tmpdir("big100_parity_")
    for sub in ("src", "iou", "blk"):
        os.makedirs(os.path.join(base, sub), exist_ok=True)
    H.state = {
        "base": base,
        # If file_read is missing entirely we SKIP (handled in body); record the
        # io_uring availability so workers can log/relax the parity expectation.
        "iouring": bool(getattr(runloom_c, "iouring_available", lambda: False)()),
    }


def worker(H, wid, rng, state):
    base = state["base"]
    src = os.path.join(base, "src", "f{0}".format(wid))
    iou = os.path.join(base, "iou", "f{0}".format(wid))
    blk = os.path.join(base, "blk", "f{0}".format(wid))
    H.sleep(rng.random() * 0.3)
    for _ in H.round_range():
        size = file_size_for(rng)
        pattern = make_pattern(wid, size)
        src_fd = iou_fd = blk_fd = -1
        try:
            # Lay down the source file via the io_uring write path.
            src_fd = os.open(src, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            woff = 0
            while woff < size:
                w = runloom_c.file_write(src_fd, pattern[woff:woff + CHUNK], woff)
                if w <= 0:
                    H.fail("src seed short write wid={0} off={1}".format(wid, woff))
                    return
                woff += w

            # --- backend A: io_uring file_read/file_write ---
            iou_fd = os.open(iou, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            copy_iouring(src_fd, iou_fd, size)
            iou_bytes = read_back_iouring(iou_fd, size)
            iou_real = os.fstat(iou_fd).st_size

            # --- backend B: blockpool os.pread/os.pwrite ---
            blk_fd = os.open(blk, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            copy_blockpool(src_fd, blk_fd, size)
            blk_bytes = runloom_c.blocking(os.pread, blk_fd, size, 0) if size else b""
            blk_real = os.fstat(blk_fd).st_size

            # --- invariants ---
            # 1. io_uring copy is byte-exact AND exactly the right size.
            if not H.check(iou_real == size and iou_bytes == pattern,
                           "io_uring copy mismatch wid={0} size={1} "
                           "(got {2} bytes, fsize={3})".format(
                               wid, size, len(iou_bytes), iou_real)):
                return
            # 2. blockpool copy is byte-exact AND exactly the right size.
            if not H.check(blk_real == size and blk_bytes == pattern,
                           "blockpool copy mismatch wid={0} size={1} "
                           "(got {2} bytes, fsize={3})".format(
                               wid, size, len(blk_bytes), blk_real)):
                return
            # 3. parity oracle: the two backends agree byte-for-byte.
            if not H.check(iou_bytes == blk_bytes,
                           "BACKEND PARITY DIVERGENCE wid={0} size={1}".format(
                               wid, size)):
                return

            H.op(wid, max(1, size))   # count copied bytes for the throughput line
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("file error wid={0}: {1}".format(wid, e))
            return
        finally:
            for fd in (src_fd, iou_fd, blk_fd):
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        pass


def body(H):
    # Availability guard.  file_read missing entirely -> SKIP clean.
    if not hasattr(runloom_c, "file_read") or not hasattr(runloom_c, "file_write"):
        H.log("SKIP: runloom_c.file_read/file_write not available "
              "(no io_uring file path built); nothing to compare")
        return
    if not H.state.get("iouring"):
        H.log("NOTE: io_uring not available; runloom_c.file_read falls back to "
              "pread, so both modes exercise pread/pwrite and the parity oracle "
              "holds trivially -- still running the correctness half")
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    # File-handle / fd-bound: each worker holds up to 3 open fds at once, so cap
    # the pool well below the 1M-goroutine socket regime.
    harness.main("p231_file_backend_parity_copy", body, setup=setup,
                 default_funcs=4000, max_funcs=20000,
                 describe="file-I/O backend parity: io_uring file_read/write "
                          "vs blockpool pread/pwrite, copy+verify+parity oracle")
