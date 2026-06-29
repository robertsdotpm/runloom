"""big_100 / 417 -- struct.pack_into / unpack_from over ONE shared arena.

Every existing struct program in the suite uses struct.pack() (returns a fresh
immutable bytes) and struct.unpack() (reads an immutable bytes) -- nothing ever
drives struct.pack_into / unpack_from, which write/read THROUGH a live Py_buffer
over a pre-allocated mutable backing object.  That is a different, sharper FT
hazard: pack_into acquires a Py_buffer via the buffer protocol, computes the
target byte range, then memcpy's the packed fields in PLACE.  Under M:N two
distinct things can corrupt it:

  * out-of-bounds / neighbour-clobber -- pack_into computes its target range from
    a live Py_buffer; if a concurrent in-place write to the ADJACENT bytes bleeds
    past its own range, or pack_into miscomputes the range under preempt, a
    worker's record cell is torn by a write that should have stayed in a guard;
  * stale Py_buffer / use-after-realloc -- a fiber holds an exported buffer over
    the backing bytearray (or a memoryview slice of it) across a park while a
    sibling resizes the SAME object; on resume pack_into writes through a pointer
    into freed/moved storage.

CRUCIAL framing: two fibers pack_into the SAME overlapping bytes with no lock is
an INHERENT, expected data race under free-threading (two hubs memcpy the same
bytes on different cores) -- it tears, and that is NOT a bug.  So this program
NEVER lets two writers touch the same bytes.  Each worker owns an EXCLUSIVE
3-cell region (low-guard | record | high-guard); the owner writes ONLY the record
cell and its sibling writes ONLY the two guard cells of the SAME worker -- all
within the worker's private region, no cross-worker writes.  The only way the
record cell tears, or a guard is found corrupted, is a real memory-safety fault:
an in-place write bleeding past its byte range, an out-of-range pack_into, or a
stale/realloc'd buffer.  That keeps the invariant falsifiable only by a bug.

The arena is one shared bytearray of N_SLOTS * CELLS_PER_REGION * REC_SIZE bytes.
The pool is capped so `funcs <= N_SLOTS` (region ownership is exclusive).  Each
round the owner does pack_into('<QQI', arena, rec_off, wid, seq, crc) where
crc == record_crc(wid, seq), a sibling pack_into's the two guard cells with a
guard sentinel, then the owner reads its region back and asserts:

  * record cell: wid_field == wid; crc_field == record_crc(wid, seq_field) (a
    guard write bleeding into the record splices a foreign wid / breaks the crc);
  * per-owner seq monotonic non-decreasing (a stale-buffer write of an OLD value
    into a moved cell regresses it);
  * both guard cells decode to the guard sentinel for THIS wid (a record write
    bleeding into a guard, or vice-versa, corrupts the guard's crc).

To drive the hazards rather than hope, each worker ROUND-ROBINS its mode by id
over its first ops (the suite's flaky-random-coverage lesson -- pure random misses
a mode at low op-count under load):

  * mode 0 GUARD-CONCURRENT: owner writes the record cell while a sibling writes
    the two guard cells, yields interleaving -- probes in-place writes honour
    their byte ranges (record vs guards never bleed across the cell boundary).
  * mode 1 MV-SLICE: owner pack_into through a memoryview SLICE of its record cell
    (a second live buffer export over the arena) while the sibling writes the
    guards -- exercises pack_into via a memoryview's buffer.
  * mode 2 RESIZE-CHURN: a PRIVATE per-worker bytearray that a sibling RESIZES
    (extend/del) across the owner's pack_into yield -- a stale-buffer / use-after-
    realloc probe.  Legal outcomes: a clean self-consistent record, OR a
    BufferError (CPython correctly refusing to resize while a buffer export is
    live) -- caught and counted.  A SIGSEGV / wrong-crc / non-BufferError
    exception is the bug.

Invariant (hot + post, fail-fast): every record cell decodes wid==owner,
crc==record_crc(owner, seq), per-owner seq monotonic; both guard cells decode to
the owner's guard sentinel; the only tolerated exception is BufferError; at least
one record was verified across the run, and no worker LOST.

Stresses: buffer-protocol pack_into/unpack_from over a SHARED arena, cell-bounds
respect under concurrent in-place writes, stale Py_buffer / use-after-realloc
across a park, memoryview-slice buffer export during mutation, BufferError-on-
export.
"""
import struct
import zlib

import harness
import runloom

# Record layout: <QQI = (wid:u64, seq:u64, crc:u32) little-endian.  20 bytes.
REC_FMT = "<QQI"
REC_SIZE = struct.calcsize(REC_FMT)            # 20

# Each worker owns an EXCLUSIVE region of 3 cells: [low-guard | record | high-
# guard].  The owner writes the record cell; its sibling writes the two guards.
# A write bleeding past a cell boundary is the only way a guard or the record
# ends up corrupt -- a real out-of-bounds in-place write.  No cross-worker writes.
CELLS_PER_REGION = 3
REGION_SIZE = CELLS_PER_REGION * REC_SIZE

# Cap on live workers; region ownership is exclusive so no two workers ever write
# the same bytes (an unsynchronised same-byte overlap is an expected FT data race,
# not a bug).  Big enough to span many cache lines / several pages.
N_SLOTS = 4096
ARENA_SIZE = N_SLOTS * REGION_SIZE

NMODES = 3

# Guard cells carry a sentinel record keyed off the owner's wid so a record write
# that bleeds into a guard (or a foreign write) is detectable.  seq field is a
# fixed magic so a guard always decodes to the same recognisable value.
GUARD_SEQ = 0xCAFEF00D


def record_crc(wid, seq):
    """crc bound to the (wid, seq) it is stored with.  Computed over the packed
    8+8 bytes of (wid, seq) so a record whose crc field does NOT equal
    record_crc(its wid_field, its seq_field) is a TORN splice -- the crc came
    from one write and the wid/seq from another (e.g. a guard write bled in)."""
    head = struct.pack("<QQ", wid & 0xFFFFFFFFFFFFFFFF, seq & 0xFFFFFFFFFFFFFFFF)
    return zlib.crc32(head) & 0xFFFFFFFF


def guard_wid(wid):
    """A distinct, recognisable wid value for this owner's guard cells (so a guard
    record never collides with the owner's real record, yet is tied to the owner
    so a foreign bleed is still caught)."""
    return (wid << 1) | 0x4000000000000000


def write_guards(arena, lo_off, hi_off, wid):
    gw = guard_wid(wid)
    gc = record_crc(gw, GUARD_SEQ)
    struct.pack_into(REC_FMT, arena, lo_off, gw, GUARD_SEQ, gc)
    runloom.yield_now()
    struct.pack_into(REC_FMT, arena, hi_off, gw, GUARD_SEQ, gc)


def check_guards(H, arena, lo_off, hi_off, wid, slot, counts):
    """Both guard cells must decode to this owner's guard sentinel; a corrupted
    guard means a write bled across a cell boundary (out-of-bounds in-place)."""
    gw = guard_wid(wid)
    for goff, name in ((lo_off, "low"), (hi_off, "high")):
        wf, sf, cf = struct.unpack_from(REC_FMT, arena, goff)
        if wf != gw or sf != GUARD_SEQ or cf != record_crc(wf, sf):
            H.fail("GUARD CORRUPT ({0}) at off {1}: owner wid={2} expected guard "
                   "wid={3} seq=0x{4:08x} but read wid={5} seq={6} crc=0x{7:08x} "
                   "-- an in-place write bled across the cell boundary (record<->"
                   "guard bounds bleed under M:N)".format(
                       name, goff, wid, gw, GUARD_SEQ, wf, sf, cf))
            return False
    return True


def verify_record_cell(H, arena, rec_off, wid, last_seq, slot, counts):
    """unpack_from the owner's record cell and assert self-consistency.  Returns
    the seq read (for the monotonic check) or None on a fault (already failed)."""
    wid_f, seq_f, crc_f = struct.unpack_from(REC_FMT, arena, rec_off)
    if wid_f != wid:
        H.fail("RECORD CLOBBER at off {0}: owner wid={1} read back wid_field={2} "
               "-- a concurrent in-place write bled a FOREIGN wid into this "
               "worker's record cell (out-of-bounds write under M:N)".format(
                   rec_off, wid, wid_f))
        return None
    if crc_f != record_crc(wid_f, seq_f):
        H.fail("TORN record at off {0}: owner wid={1} read seq={2} crc=0x{3:08x} "
               "but record_crc(wid,seq)=0x{4:08x} -- the record cell was spliced "
               "by a concurrent write (guard bounds bleed / stale Py_buffer)"
               .format(rec_off, wid, seq_f, crc_f, record_crc(wid_f, seq_f)))
        return None
    if seq_f < last_seq:
        H.fail("SEQ REGRESSION at off {0}: owner wid={1} read seq={2} < "
               "previously-observed {3} -- a stale Py_buffer wrote an OLD value "
               "through a moved/realloc'd cell".format(
                   rec_off, wid, seq_f, last_seq))
        return None
    return seq_f


def verify_region(H, arena, rec_off, lo_off, hi_off, wid, last_seq, slot, counts):
    seq_r = verify_record_cell(H, arena, rec_off, wid, last_seq, slot, counts)
    if seq_r is None:
        return None
    if not check_guards(H, arena, lo_off, hi_off, wid, slot, counts):
        return None
    counts["verified"][slot] += 1
    return seq_r


def do_guard_concurrent(H, wid, rng, arena, offs, seq, last_seq, slot, counts):
    """mode 0: owner writes the record cell while a sibling writes the two guard
    cells, yields interleaving.  Probes in-place writes honour their cell ranges
    (record vs guards never bleed across the boundary)."""
    rec_off, lo_off, hi_off = offs
    crc = record_crc(wid, seq)
    wg = runloom.WaitGroup()
    wg.add(1)

    def sibling():
        try:
            write_guards(arena, lo_off, hi_off, wid)
        finally:
            wg.done()

    H.fiber(sibling)
    runloom.yield_now()
    struct.pack_into(REC_FMT, arena, rec_off, wid, seq, crc)
    runloom.yield_now()
    wg.wait()
    return verify_region(H, arena, rec_off, lo_off, hi_off, wid, last_seq, slot,
                         counts)


def do_mvslice(H, wid, rng, arena, offs, seq, last_seq, slot, counts):
    """mode 1: owner pack_into through a memoryview SLICE of its record cell (a
    second live buffer export over the arena) while the sibling writes the
    guards.  Exercises pack_into via a memoryview's exported buffer."""
    rec_off, lo_off, hi_off = offs
    crc = record_crc(wid, seq)
    mv = memoryview(arena)
    wg = runloom.WaitGroup()
    wg.add(1)

    def sibling():
        try:
            write_guards(arena, lo_off, hi_off, wid)
        finally:
            wg.done()

    H.fiber(sibling)
    try:
        sl = mv[rec_off:rec_off + REC_SIZE]
        runloom.yield_now()
        struct.pack_into(REC_FMT, sl, 0, wid, seq, crc)
        sl.release()
    finally:
        mv.release()
    wg.wait()
    return verify_region(H, arena, rec_off, lo_off, hi_off, wid, last_seq, slot,
                         counts)


def do_resize_churn(H, wid, rng, seq, slot, counts):
    """mode 2: a PRIVATE per-worker bytearray that a sibling RESIZES (extend/del)
    across the owner's pack_into yield -- stale Py_buffer / use-after-realloc
    probe.  Legal: a clean self-consistent record, OR a BufferError (CPython
    refusing to resize while a buffer export is live).  Anything else is the bug.

    Returns ("ok", seq_read) | ("buffererror", None) | ("fail", None)."""
    buf = bytearray(REC_SIZE + 64)             # padding so a resize moves storage
    crc = record_crc(wid, seq)
    wg = runloom.WaitGroup()
    wg.add(1)

    def resizer(buf=buf):
        try:
            try:
                buf.extend(b"\x00" * 128)      # grow -> may move storage
                del buf[REC_SIZE + 32:]        # shrink back
            except BufferError:
                # CPython refused the resize because a buffer export is live --
                # legal; the worker's pack_into then writes through a valid buffer.
                pass
        finally:
            wg.done()

    H.fiber(resizer)
    try:
        mv = memoryview(buf)                   # live export -> resize may refuse
        runloom.yield_now()                    # sibling tries to resize HERE
        struct.pack_into(REC_FMT, mv, 0, wid, seq, crc)
        mv.release()
    except BufferError:
        # A release/export race could legally surface BufferError here.
        wg.wait()
        counts["buffererror"][slot] += 1
        return ("buffererror", None)
    wg.wait()
    wid_f, seq_f, crc_f = struct.unpack_from(REC_FMT, buf, 0)
    if wid_f != wid or crc_f != record_crc(wid_f, seq_f):
        H.fail("RESIZE-CHURN torn/stale record: unpack_from gave wid={0} seq={1} "
               "crc=0x{2:08x} (expected wid={3}, crc=0x{4:08x}) -- pack_into wrote "
               "through a stale Py_buffer after the backing bytearray was resized "
               "(use-after-realloc)".format(
                   wid_f, seq_f, crc_f, wid, record_crc(wid_f, seq_f)))
        return ("fail", None)
    counts["verified"][slot] += 1
    return ("ok", seq_f)


def worker(H, wid, rng, state):
    arena = state["arena"]
    counts = state["counts"]
    slot = wid & 1023
    region = wid % N_SLOTS
    base = region * REGION_SIZE
    lo_off = base                              # low guard cell
    rec_off = base + REC_SIZE                  # record cell (middle)
    hi_off = base + 2 * REC_SIZE               # high guard cell
    offs = (rec_off, lo_off, hi_off)
    # Pre-seed the guards once so the first record verify sees valid guards even
    # if the very first sibling write is still in flight (it never is -- wg.wait
    # joins it -- but this also makes the region well-formed before any mode runs).
    write_guards(arena, lo_off, hi_off, wid)
    last_seq = 0
    seq = 0
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        seq += 1
        # Round-robin the three modes by worker id over the first ops so post()'s
        # coverage holds whether one worker does 3 ops or 3 workers do 1 each --
        # pure random misses a mode at low op-count under load (the suite's
        # p125/p126/p172 flaky-coverage lesson).  Random after, for the mix.
        if i < NMODES:
            mode = (wid + i) % NMODES
        else:
            mode = rng.randrange(NMODES)
        i += 1

        if mode == 0:
            r = do_guard_concurrent(H, wid, rng, arena, offs, seq, last_seq,
                                    slot, counts)
            if r is None:
                return
            last_seq = r
        elif mode == 1:
            r = do_mvslice(H, wid, rng, arena, offs, seq, last_seq, slot, counts)
            if r is None:
                return
            last_seq = r
        else:
            kind, r = do_resize_churn(H, wid, rng, seq, slot, counts)
            if kind == "fail":
                return
            # mode 2 uses a private buffer; check only that it wrote/read its own
            # seq back (independent of the shared region's last_seq).
            if kind == "ok" and r is not None and r != seq:
                H.fail("RESIZE-CHURN wrong seq: wrote {0} read {1}".format(seq, r))
                return

        H.op(wid)
        H.task_done(wid)


def setup(H):
    # One shared arena for the whole pool -- modes 0/1 pack_into disjoint regions
    # of it concurrently across hubs.  Pre-allocated; region ownership exclusive.
    H.state = {
        "arena": bytearray(ARENA_SIZE),
        "counts": {
            "verified": [0] * 1024,
            "buffererror": [0] * 1024,
        },
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    counts = H.state["counts"]
    verified = sum(counts["verified"])
    buffererror = sum(counts["buffererror"])
    H.log("records verified self-consistent={0} buffererror(legal resize "
          "refusals)={1} ops={2}".format(verified, buffererror, H.total_ops()))
    # Every recorded record passed the torn/clobber/guard/regression checks (those
    # fail fast), so the only post check is that the windows were exercised at all.
    H.check(verified > 0,
            "no records were verified -- the pack_into/unpack_from race windows "
            "were never exercised")
    H.require_no_lost()


if __name__ == "__main__":
    # Memory-safety / correctness test: in-place struct.pack_into / unpack_from
    # over a SHARED arena under M:N preempt -- a write bleeding across a cell
    # boundary (record<->guard) and stale-Py_buffer writes after a sibling resize.
    # Capped to N_SLOTS so region ownership is EXCLUSIVE (no two workers write the
    # same bytes; an unsynchronised same-byte overlap is an expected FT data race,
    # not a bug).
    harness.main("p417_struct_pack_into_shared_buf", body, setup=setup,
                 post=post, default_funcs=3000, max_funcs=N_SLOTS,
                 describe="struct.pack_into/unpack_from over one shared bytearray "
                          "arena across M:N hubs; each worker owns a disjoint "
                          "guard|record|guard region that must decode self-"
                          "consistent (crc bound to wid,seq) or a legal "
                          "BufferError -- a bounds bleed / stale-buffer write fails")
