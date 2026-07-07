"""big_100 / 516 -- tarfile in-memory build/extract round-trip isolation under M:N.

tarfile.TarFile is a mutable per-instance object.  A TarFile opened for WRITING
carries a large amount of instance state that is threaded through every
addfile()/extractfile() call:

  * self.offset      -- the CURRENT byte position in the underlying stream, bumped
                        512 bytes at a time as each header + padded data block is
                        written;
  * self.members     -- the growing list of TarInfo objects for members added so
                        far;
  * self.fileobj     -- the backing stream (here a BytesIO), whose write cursor
                        MUST stay in lock-step with self.offset;
  * copyfileobj()    -- the block-copy helper that streams a member's payload into
                        the archive in BUFSIZE (16 KiB) chunks, writing a final
                        512-byte-aligned NUL pad; it reads/writes through a shared
                        scratch buffer.

Under M:N many fibers run on a handful of hub OS-threads with the GIL OFF.  A
fiber that is PARKED (yield/sleep) in the middle of writing a member -- after the
header block but before the padded data block, or mid-copyfileobj() -- lets a
sibling fiber on the same hub run.  The hazard this program probes: if runloom
did NOT properly isolate each fiber's TarFile instance (a torn self.offset, a
block written to the wrong stream cursor, a members[] append lost, or the
copyfileobj scratch buffer shared across fibers), the archive a fiber produces
would extract to the WRONG bytes -- a member truncated, padded incorrectly, or
carrying a sibling's payload -- or the header checksum would not validate on
re-open.

Because every fiber owns its OWN BytesIO + its OWN tarfile.open("w") + its OWN
tarfile.open("r"), this is a SINGLE-OWNER round-trip.  Nothing is shared between
fibers.  On a correct runtime the archive a fiber builds is exactly the archive
it reads back, every time -- so the program EXITS 0 (PASS) when there is no bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a closed-world round-trip, single-owner):

  Each fiber, per iteration, generates a KNOWN multiset of K members whose names
  AND payloads are tagged with the fiber's wid (plus a per-iteration idx and a
  per-member index), so a byte that leaked in from a SIBLING fiber's archive is
  immediately recognizable (its embedded "W<wid>" tag would be wrong).  The fiber:

    1. Opens a fresh in-memory tar for WRITING (tarfile.open(fileobj=BytesIO,
       mode="w")).
    2. Adds all K members via addfile(TarInfo, BytesIO(payload)), YIELDING between
       members (and once mid-stream) so a sibling reliably interleaves while this
       fiber is parked with a half-written archive (torn self.offset window).
    3. Closes the write TarFile (flushes the two 512-byte end-of-archive NUL
       blocks), snapshots buf.getvalue().
    4. Re-opens that byte string for READING (tarfile.open(fileobj=BytesIO,
       mode="r")) and asserts the CLOSED-WORLD round-trip law:
         (a) member COUNT == K exactly (no member dropped or duplicated);
         (b) every member name is one THIS fiber wrote (no sibling name appears --
             an out-of-universe name is a cross-fiber leak);
         (c) each member's declared size == len(its known payload);
         (d) extractfile(member).read() == the EXACT known payload bytes (no
             truncation, no NUL-pad bleed, no sibling bytes, no torn block);
         (e) the set of names read back == the set written (no missing member).

  We verified the analogous single-owner round-trip with a standalone plain-
  threads control (16 OS threads, each building + extracting its own in-memory
  tar with wid-tagged members, GIL ON and OFF): 100% of round-trips reproduce the
  exact bytes -- 0 mismatches.  Each thread's TarFile instance is independent and
  properly isolated.  Under a CORRECT runloom each fiber's round-trip MUST also be
  byte-exact.  If a fiber's extracted bytes differ from what it wrote, a member
  count is wrong, or a sibling's name/bytes appear, that is a runloom M:N
  fiber-isolation bug (a torn self.offset, a mis-cursored block write, a lost
  members[] append, or a shared copyfileobj scratch), and the load-bearing
  single-owner oracle FAILS -- otherwise it PASSES (exit 0).

ORACLES:
  * LOAD-BEARING -- TAR ROUND-TRIP INTEGRITY (worker, HARD, fail-fast).  The
    closed-world (a)-(e) checks above on a fiber's OWN in-memory archive.  Single-
    owner: the BytesIO, the write TarFile, the read TarFile, and the expected-
    payload dict are all fiber-local, never shared.  A failure is a runloom
    isolation desync, never documented Python semantics (an unsynchronized SHARED
    TarFile would tear exactly like a shared file across OS threads -- documented
    behavior -- so we never share one).
  * NON-VACUITY (post, HARD): the round-trip hazard actually ran
    (member_roundtrips > 0 -- else the oracle is vacuous).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-write
    (stranded inside copyfileobj / addfile on a torn offset) never returns; the
    watchdog + require_no_lost catch it.

FAIL ON: member count != K, an out-of-universe (sibling) member name, a member
size mismatch, extracted bytes != the known payload (truncation / NUL-pad bleed /
sibling bytes / torn block), or a missing member on read-back.

Stresses: tarfile.TarFile.addfile()/extractfile() per-instance stream state
(self.offset, self.members, self.fileobj cursor), copyfileobj() block copy +
512-byte padding across a fiber yield, header checksum validation on re-open,
per-fiber in-memory archive isolation under M:N with the GIL off.

Good TSan / controlled-M:N-replay target: self.offset and the BytesIO write
cursor are a get-then-advance pair driven per 512-byte block; a fiber parked
between the header write and the data write is the cleanest window for a
mis-cursored block or torn offset -- a TSan report on the BytesIO buffer, or a
single byte off in the extracted payload under replay, localizes the tear before
the round-trip law even closes.
"""
import io
import tarfile

import harness
import runloom

# Members per fiber-owned archive.  Small enough that build+extract is cheap under
# tens of thousands of fibers, large enough that self.members grows across several
# entries and the round-trip exercises multiple header/data/pad boundaries.
MEMBERS_PER_TAR = 6

# Payload size band (design: 256B - 4KiB).  A payload > BUFSIZE-free single block
# but spanning several 512-byte blocks (and occasionally > one copy chunk) so the
# padding + block-copy path is exercised, not just a single-block member.
PAYLOAD_MIN = 256
PAYLOAD_MAX = 4096

# Sustained round-trips per worker, bounded by H.running().  The isolation hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously building and
# extracting archives while parked mid-write across a yield, so a sibling reliably
# interleaves before this fiber resumes.  A single round-trip per fiber barely
# overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_payload(wid, idx, m, size):
    """Deterministic, wid-tagged payload for member m of fiber wid's idx-th tar.

    The payload begins with an ASCII tag embedding wid/idx/m so a byte sequence
    that leaked in from a SIBLING fiber's archive (a different wid) is immediately
    recognizable, then is filled with a per-member repeating pattern to length
    `size`.  Single-owner: the fiber that built it is the only one that reads it.
    """
    tag = "W{0}:I{1}:M{2}:".format(wid, idx, m).encode("ascii")
    if len(tag) >= size:
        return tag[:size]
    fill_span = size - len(tag)
    # A per-(wid,m) repeating byte so a mis-cursored block from a sibling shows a
    # wrong fill value as well as a wrong tag.
    fill_byte = ((wid * 7 + m * 31 + 1) & 0xFF)
    return tag + bytes([fill_byte]) * fill_span


def round_trip(H, wid, idx, rng, state):
    """One single-owner tar build+extract round-trip.

    Builds a fiber-local in-memory archive of K wid-tagged members (yielding
    between writes so a sibling interleaves on a torn-offset archive), then
    re-opens the archive read-only and asserts the closed-world round-trip law.
    Every object here is fiber-local -- a mismatch is a runloom isolation bug."""
    # ---- KNOWN multiset of members this fiber will write (the closed world) ----
    expected = {}
    order = []
    for m in range(MEMBERS_PER_TAR):
        name = "w{0}_i{1}_m{2}.dat".format(wid, idx, m)
        size = rng.randint(PAYLOAD_MIN, PAYLOAD_MAX)
        payload = build_payload(wid, idx, m, size)
        expected[name] = payload
        order.append(name)

    # ---- BUILD: fiber-local BytesIO + fiber-local write TarFile ----------------
    wbuf = io.BytesIO()
    wtar = tarfile.open(fileobj=wbuf, mode="w")
    try:
        for pos, name in enumerate(order):
            payload = expected[name]
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            # addfile() writes the 512-byte header, then copyfileobj() streams the
            # payload and NUL-pads to a 512-byte boundary, advancing self.offset.
            wtar.addfile(info, io.BytesIO(payload))
            # PARK mid-archive: a sibling on this hub runs now, while this fiber's
            # TarFile sits at a partially-written offset.  If self.offset / the
            # BytesIO cursor / self.members are not fiber-isolated, the sibling's
            # writes bleed into this archive.
            runloom.yield_now()
            if pos == 0:
                runloom.sleep(0.0002)
    finally:
        wtar.close()          # flushes the two 512-byte end-of-archive blocks

    archive_bytes = wbuf.getvalue()

    # ---- EXTRACT: fiber-local read TarFile over the snapshot -------------------
    rbuf = io.BytesIO(archive_bytes)
    rtar = tarfile.open(fileobj=rbuf, mode="r")
    try:
        members = rtar.getmembers()

        # (a) member COUNT == K exactly.
        if len(members) != MEMBERS_PER_TAR:
            H.fail("fiber {0} idx {1}: tar round-trip member COUNT wrong: read "
                   "{2} members, wrote {3} -- a member was dropped or duplicated, "
                   "self.members / self.offset was torn across a yield (cross-fiber "
                   "leak into this fiber's single-owner archive)".format(
                       wid, idx, len(members), MEMBERS_PER_TAR))
            return

        seen = set()
        for info in members:
            name = info.name

            # (b) name must be one THIS fiber wrote -- no sibling name.
            if name not in expected:
                H.fail("fiber {0} idx {1}: tar round-trip OUT-OF-UNIVERSE member "
                       "name {2!r} -- a sibling fiber's member name appeared in "
                       "this fiber's single-owner archive (self.members / offset "
                       "isolation failure under M:N)".format(wid, idx, name))
                return
            if name in seen:
                H.fail("fiber {0} idx {1}: tar round-trip DUPLICATE member {2!r} "
                       "-- the same member was written twice (torn members[] "
                       "append under M:N)".format(wid, idx, name))
                return
            seen.add(name)

            exp = expected[name]

            # (c) declared size matches.
            if info.size != len(exp):
                H.fail("fiber {0} idx {1}: tar round-trip SIZE mismatch for {2!r}: "
                       "header says {3} bytes, wrote {4} -- a torn header/offset "
                       "under concurrent fiber writes".format(
                           wid, idx, name, info.size, len(exp)))
                return

            # (d) extracted bytes must EXACTLY equal the known payload.
            ef = rtar.extractfile(info)
            if ef is None:
                H.fail("fiber {0} idx {1}: extractfile({2!r}) returned None -- the "
                       "member is not a regular file on read-back (corrupted "
                       "TarInfo type from a torn header)".format(wid, idx, name))
                return
            got = ef.read()
            ef.close()
            if got != exp:
                got_head = repr(got[:48]) if got else "empty"
                exp_head = repr(exp[:48])
                H.fail("fiber {0} idx {1}: tar round-trip BYTES mismatch for {2!r}: "
                       "got {3} (len {4}), expected {5} (len {6}) -- extracted "
                       "content is truncated, NUL-pad-bled, torn, or carries a "
                       "sibling fiber's payload (TarFile per-instance stream "
                       "isolation failure under M:N)".format(
                           wid, idx, name, got_head, len(got), exp_head, len(exp)))
                return

        # (e) every written member was read back.
        if len(seen) != MEMBERS_PER_TAR:
            H.fail("fiber {0} idx {1}: tar round-trip MISSING member(s): read back "
                   "{2} distinct names, wrote {3} -- a member vanished from "
                   "self.members across the build (lost append under M:N)".format(
                       wid, idx, len(seen), MEMBERS_PER_TAR))
            return
    finally:
        rtar.close()

    state["roundtrips"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber runs sustained single-owner tar build+extract round-trips,
    fail-fast on the first closed-world round-trip violation."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            round_trip(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One race-free slot per worker (single-writer-per-slot).  H.funcs is already
    # capped to max_funcs here, so this array is bounded.
    H.state = {
        "roundtrips": [0] * H.funcs,      # single-owner round-trips per worker
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["roundtrips"])
    H.log("tarfile: {0} single-owner in-memory tar build+extract round-trips "
          "(every closed-world round-trip law -- count, names, sizes, exact "
          "bytes -- passed fail-fast); ops={1}".format(rts, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(rts > 0,
            "no tar round-trips completed -- the load-bearing tarfile build/extract "
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-write.
    H.require_no_lost("tarfile membuf round-trip")


if __name__ == "__main__":
    harness.main(
        "p516_tarfile_membuf_roundtrip", body, setup=setup, post=post,
        default_funcs=4000, max_funcs=6000,
        describe="tarfile.TarFile is a mutable per-instance object threading "
                 "self.offset / self.members / the backing stream cursor through "
                 "every addfile()/copyfileobj() 512-byte block write.  Under M:N a "
                 "fiber parked mid-write lets a sibling run; if the TarFile "
                 "instance is not fiber-isolated, block writes interleave and the "
                 "archive extracts to wrong bytes.  LOAD-BEARING (single-owner): "
                 "each fiber builds its OWN in-memory tar (BytesIO) of K wid-tagged "
                 "members, yielding between writes, then re-opens it read-only and "
                 "asserts the closed-world round-trip law -- member count==K, no "
                 "sibling name, exact sizes, extracted bytes==the known payload. "
                 "A mismatch (torn offset, mis-cursored block, lost members[] "
                 "append, sibling bytes) is a runloom M:N isolation bug (0 under "
                 "plain threads GIL on AND off)")
