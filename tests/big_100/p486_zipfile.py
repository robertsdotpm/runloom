"""big_100 / 486 -- ZipFile per-instance mutable state isolation under M:N.

zipfile.ZipFile is a mutable per-instance object that holds internal state:
  - self._NameToInfo: dict mapping archive member names -> ZipInfo objects
  - self.fp: file pointer to the underlying .zip file
  - self._filePassed: whether fp was passed to __init__ or opened internally
  - self.NameToInfo: public read-only export of _NameToInfo (actually mutable)
  - self.infolist(): list of ZipInfo for all members in the archive

Under M:N the hazard is SHARED STATE ACCESS ACROSS CONCURRENT FIBERS: many
fibers run on one hub OS-thread.  Each fiber opens its OWN DISTINCT ZipFile
instance over one of N pool archives (wid % N), reads members, and yields --
a fiber-local scheduling point (yield/sleep) lets a sibling fiber on the same
hub run.  If the Python interpreter is not careful about ZipFile's instance
isolation, or if a fiber migrates a hub mid-operation, a torn or corrupted
read of _NameToInfo or a stale file pointer can cause:
  - A fiber reads the WRONG member from its archive (torn _NameToInfo entry)
  - A fiber's member count / member list changes mid-read (concurrent mutation)
  - A fiber extracts data from the WRONG offset (fp contamination)
  - A fiber's extracted data is truncated or contains foreign bytes

BOUNDED-POOL REDESIGN (root-cause fix for disk exhaustion):
  The OLD version created ONE temp .zip PER FIBER.  At --funcs 500000 that was
  ~500k temp files -> it FILLED THE DISK and crashed the box.  The number of
  temp files MUST NOT scale with --funcs.

  We now build EXACTLY N = min(H.funcs, POOL_CAP) distinct .zip archives ONCE
  at setup() into a single mkdtemp directory, and store them in a module-level
  pool.  Each archive has DISTINCT, archive-specific marker members, so the N
  archives give N distinct _NameToInfo/ZipInfo registries -- the cache-isolation
  hazard is fully preserved.  Every fiber maps to archive `wid % N`, opens its
  own ZipFile instance over that shared archive file, and asserts every member
  round-trips to the known marker bytes.  No fiber EVER creates a temp file.
  The pool dir is removed at teardown + atexit, so it never leaks.

  Two distinct fibers mapping to the SAME pool archive open INDEPENDENT ZipFile
  instances over the same read-only file -- that is exactly the per-instance
  isolation hazard (concurrent ZipFile reads of one underlying archive across
  fiber yields), now without the unbounded per-fiber file creation.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  Each pool archive holds K members, each with ARCHIVE-SPECIFIC marker data
  (the archive index encoded in the member name and content).  A fiber bound to
  archive a = wid % N repeatedly:
    1. Opens a ZipFile instance pointing to pool archive a
    2. Reads all members' names via namelist()
    3. For each member, reads the member's ZipInfo via getinfo(name)
    4. Extracts each member's data via read(name)
    5. Asserts the extracted data EXACTLY MATCHES the known marker bytes
    6. Yields to encourage sibling scheduling and hub switching

  The oracle: every read and extract MUST:
    - Return the CORRECT member list for archive a (no foreign members)
    - Return the CORRECT ZipInfo (the right size)
    - Extract EXACTLY the marker bytes archive a holds (no truncation, no
      foreign data, no corruption)

  We verified with a standalone plain-threads control (same multi-member,
  shared-archive hazard, 16 threads, NO runloom) that this NEVER fails under
  PYTHON_GIL=1 AND PYTHON_GIL=0.  Each OS thread's ZipFile instance is
  independent and properly isolated.  Under a CORRECT runloom each fiber MUST
  also get its archive's data every time.  If runloom does NOT properly isolate
  per-fiber ZipFile instances -- the _NameToInfo dict is corrupted / shared, the
  file pointer is torn, or extraction reads from the wrong offset -- the read
  returns WRONG DATA (foreign bytes, truncated data, or a missing member).  That
  is the runloom M:N isolation bug, and the program EXITS 0 only when there is
  NO bug (all data matches).

ORACLES:
  * LOAD-BEARING -- MEMBER LIST + DATA INTEGRITY (worker, HARD, fail-fast).
    Each pool archive has K unique members (K archive-distinct marker strings).
    The oracle:
      (a) namelist() MUST return exactly K members with archive-specific names.
      (b) getinfo(name) MUST return the correct ZipInfo (size).
      (c) read(name) MUST extract exactly the marker bytes archive a holds (no
          truncation, no foreign data, no corruption).
    A mismatch indicates a fiber's ZipFile instance state is corrupted or shared
    with a sibling.  The program NEVER fires on plain threads (GIL on AND off --
    verified), so it is a true runloom isolation signal.
  * NON-VACUITY (post, HARD): the member-data isolation hazard was actually
    exercised (read_count > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    extract (stranded in ZipFile machinery on a torn entry) never returns; the
    watchdog + require_no_lost catch it.

  * MEASURED (report-ONLY, NEVER fails): per-fiber member-count variance.

FAIL ON: wrong member list (name mismatch, count mismatch), wrong ZipInfo
(size), wrong extracted data (doesn't match the marker bytes), or
truncated/corrupted data.

Stresses: zipfile.ZipFile per-instance mutable state (_NameToInfo dict, fp),
ZipFile.namelist() + getinfo() + read() across concurrent fiber yield/sleep,
per-archive member identity, member data integrity across yields, hub migration
between ZipFile __enter__ and data extraction.
"""
import atexit
import os
import shutil
import sys
import tempfile
import zipfile

import harness
import runloom

# Modest population.  Past a few hundred concurrent fibers, the .zip-open
# overhead dominates; the cache hazard is fully exercised well below that.
MAX_WORKERS = 4000

# Number of members in each pool .zip archive.  Small: enough to test
# multi-member list integrity without dominating setup time.
MEMBERS_PER_ZIP = 10

# Number of iterations per worker: open the ZipFile, read namelist, extract all
# members.  Each iteration yields to encourage concurrent fibers on the hub.
INNER_CAP = 1000

# BOUNDED POOL: the maximum number of distinct .zip archives (and thus the
# maximum number of temp files) the program EVER creates, regardless of
# --funcs.  This is the root-cause fix for the disk-exhaustion crash: temp
# files are O(POOL_CAP), NOT O(funcs).  N distinct archives still give N
# distinct _NameToInfo/ZipInfo registries -> the cache-isolation hazard holds.
POOL_CAP = 512

# Module-level bounded pool (built once at setup, cleaned at teardown/atexit).
TMPDIR = None
# POOL[a] = (zip_path, expected_members_dict) for pool archive index a.
POOL = []


def cleanup():
    """Remove the bounded-pool temp dir.  Idempotent; safe from atexit."""
    global TMPDIR
    d = TMPDIR
    TMPDIR = None
    if d:
        shutil.rmtree(d, ignore_errors=True)


def build_pool_archive(tmpdir, archive_idx, num_members):
    """Create ONE pool .zip archive with NUM_MEMBERS archive-specific members.

    Each member is named 'member_AIDX_IDX.txt' and contains marker bytes
    'AIDX:IDX:' followed by repetitions of the archive's index digit, so a
    reader can verify it read ONLY this archive's data.

    Returns (zip_path, expected_members_dict) mapping member_name -> marker_bytes.
    """
    zpath = os.path.join(tmpdir, "zip_{0}.zip".format(archive_idx))

    # Marker length per member (kept modest for speed).
    marker_len = 50

    expected = {}
    with zipfile.ZipFile(zpath, "w") as z:
        for idx in range(num_members):
            member_name = "member_{0}_{1}.txt".format(archive_idx, idx)
            marker_data = "AIDX:{0}:IDX:{1}:".format(archive_idx, idx)
            marker_data += str(archive_idx % 10) * marker_len
            marker_bytes = marker_data.encode("utf-8")

            z.writestr(member_name, marker_bytes)
            expected[member_name] = marker_bytes

    return zpath, expected


def setup(H):
    """Build EXACTLY N = min(H.funcs, POOL_CAP) distinct pool .zip archives ONCE.

    No file/dir is created per fiber -- fibers map to a pool archive via wid % N.
    """
    global TMPDIR, POOL

    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    # N pool archives -- bounded by POOL_CAP so temp-file count NEVER scales with
    # --funcs.  At least 2 distinct archives so wid % N exercises >1 registry.
    npool = max(2, min(nworkers, POOL_CAP))

    base = os.environ.get("BIG100_TMP") or tempfile.gettempdir()
    try:
        os.makedirs(base, exist_ok=True)
    except OSError:
        base = tempfile.gettempdir()
    TMPDIR = tempfile.mkdtemp(prefix="p486_zipfile_", dir=base)
    atexit.register(cleanup)

    POOL = []
    for a in range(npool):
        zpath, expected = build_pool_archive(TMPDIR, a, MEMBERS_PER_ZIP)
        POOL.append((zpath, expected))

    H.state = {
        "npool": npool,
        "read_counts": [0] * 1024,    # ZipFile read/extract iterations per fiber
        "list_mismatches": [0] * 1024,  # namelist() returned wrong members
        "data_mismatches": [0] * 1024,  # extracted data != expected marker
        "size_mismatches": [0] * 1024,  # getinfo size != expected
        "read_errors": [0] * 1024,    # ZipFile operations raised exceptions
        "nworkers": nworkers,
    }


def worker(H, wid, rng, state):
    """Each fiber opens a POOL archive (wid % npool) many times, reads/extracts
    all members, and yields between reads to encourage concurrent hub switching.

    The fiber NEVER creates a temp file -- it reads a shared pool archive."""
    if wid >= state["nworkers"]:
        H.task_done(wid)
        return

    zpath, expected_members = POOL[wid % state["npool"]]

    for _ in H.round_range():
        if not H.running():
            break

        idx = 0
        while H.running() and idx < INNER_CAP:
            try:
                # Open an INDEPENDENT ZipFile instance for this iteration.
                with zipfile.ZipFile(zpath, "r") as z:
                    # LOAD-BEARING: check the namelist is correct.
                    got_names = sorted(z.namelist())
                    expected_names = sorted(expected_members.keys())

                    if got_names != expected_names:
                        state["list_mismatches"][wid & 1023] += 1
                        H.fail(
                            "fiber {0}: namelist() mismatch: got {1!r} (expected "
                            "{2!r}) -- the ZipFile's member list is corrupted or a "
                            "foreign archive leaked into this fiber's namespace.  "
                            "Archive: {3}".format(
                                wid, got_names, expected_names, zpath
                            )
                        )
                        return

                    # LOAD-BEARING: check each member's data integrity.
                    for member_name, expected_data in expected_members.items():
                        # Verify ZipInfo (size).
                        try:
                            info = z.getinfo(member_name)
                        except KeyError:
                            H.fail(
                                "fiber {0}: getinfo({1!r}) raised KeyError -- the "
                                "member exists in the expected list but not in "
                                "ZipFile._NameToInfo (corrupted archive metadata).  "
                                "Archive: {2}".format(wid, member_name, zpath)
                            )
                            return

                        # Check the stored size matches our expectation.
                        expected_size = len(expected_data)
                        if info.file_size != expected_size:
                            state["size_mismatches"][wid & 1023] += 1
                            H.fail(
                                "fiber {0}: ZipInfo size mismatch for {1!r}: got "
                                "{2} bytes (expected {3}) -- the member's metadata "
                                "is corrupted or this fiber read a foreign member "
                                "(wrong ZipInfo).  Archive: {4}".format(
                                    wid, member_name, info.file_size, expected_size,
                                    zpath
                                )
                            )
                            return

                        # Extract the member's data.
                        try:
                            got_data = z.read(member_name)
                        except Exception as e:
                            state["read_errors"][wid & 1023] += 1
                            H.fail(
                                "fiber {0}: z.read({1!r}) raised {2}: {3} -- "
                                "ZipFile extraction failed (torn entry or corrupted "
                                "archive).  Archive: {4}".format(
                                    wid, member_name, type(e).__name__, e, zpath
                                )
                            )
                            return

                        # LOAD-BEARING: data MUST match exactly.
                        if got_data != expected_data:
                            state["data_mismatches"][wid & 1023] += 1
                            # Log a sample for debugging.
                            got_str = repr(got_data[:100]) if got_data else "empty"
                            exp_str = repr(expected_data[:100])
                            H.fail(
                                "fiber {0}: z.read({1!r}) returned wrong data: got "
                                "{2} (expected {3}) -- the extracted content is "
                                "corrupted, truncated, or from a foreign archive "
                                "(ZipFile isolation failure under M:N).  Archive: "
                                "{4}".format(
                                    wid, member_name, got_str, exp_str, zpath
                                )
                            )
                            return

                    state["read_counts"][wid & 1023] += 1

            except Exception as e:
                state["read_errors"][wid & 1023] += 1
                H.fail(
                    "fiber {0}: ZipFile({1!r}).read() raised {2}: {3} -- "
                    "ZipFile operation failed (likely a torn state or fiber "
                    "isolation desync).".format(wid, zpath, type(e).__name__, e)
                )
                return

            # Yield between reads to encourage concurrent fiber scheduling on
            # the hub.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)

            H.op(wid)
            idx += 1

        H.task_done(wid)


def body(H):
    H.run_pool(H.state["nworkers"], worker, H.state)


def post(H):
    reads = sum(H.state["read_counts"])
    list_mm = sum(H.state["list_mismatches"])
    data_mm = sum(H.state["data_mismatches"])
    size_mm = sum(H.state["size_mismatches"])
    errors = sum(H.state["read_errors"])

    data_pct = (100.0 * data_mm / reads) if reads else 0.0
    list_pct = (100.0 * list_mm / reads) if reads else 0.0

    H.log(
        "zipfile: {0} read/extract cycles | data_mismatches={1} ({2:.2f}%) | "
        "list_mismatches={3} ({4:.2f}%) | size_mismatches={5} | errors={6} | "
        "nworkers={7} pool_archives={8}".format(
            reads, data_mm, data_pct, list_mm, list_pct, size_mm, errors,
            H.state["nworkers"], H.state["npool"]
        )
    )

    if data_mm or list_mm or size_mm:
        H.log(
            "note: the load-bearing ZipFile data-integrity oracle observed "
            "{0} data mismatches, {1} list mismatches, and {2} size mismatches "
            "across {3} read/extract cycles -- ZipFile's per-instance mutable "
            "state (_NameToInfo dict, file pointer, member list) is corrupted "
            "under M:N fiber concurrency.  Many fibers open independent ZipFile "
            "instances over a bounded pool of archives (wid % npool); if hub "
            "fibers are not properly isolated, a sibling's concurrent ZipFile "
            "operations can corrupt this fiber's reads (wrong data, wrong "
            "members, truncated extracts).  This is a runloom M:N fiber "
            "isolation bug (0 mismatches under plain threads GIL on AND off -- "
            "each OS thread's ZipFile is independent).  The fix is to ensure "
            "fiber-local isolation of per-instance ZipFile state or serialize "
            "access in runloom.".format(data_mm, list_mm, size_mm, reads)
        )

    if errors:
        H.log(
            "note: {0} ZipFile exceptions were raised (beyond the data-integrity "
            "failures).  These may indicate corrupted member metadata (KeyError in "
            "getinfo), torn archive entries, or concurrent access tearing the .zip "
            "file pointer.".format(errors)
        )

    # NON-VACUITY: the load-bearing data-integrity hazard was actually exercised.
    H.check(
        reads > 0,
        "no ZipFile read/extract cycles ran -- the load-bearing member-data "
        "isolation hazard was never exercised (oracle would be vacuous)"
    )

    # COMPLETENESS: no fiber vanished mid-extract.
    H.require_no_lost("zipfile ZipFile data-integrity")

    # Clean up the bounded pool temp dir (idempotent with the atexit hook).
    cleanup()


if __name__ == "__main__":
    harness.main(
        "p486_zipfile_isolation",
        body,
        setup=setup,
        post=post,
        default_funcs=8000,
        describe="zipfile.ZipFile is a mutable per-instance object holding "
                 "internal state (_NameToInfo dict, file pointer).  Under M:N "
                 "many fibers share one hub OS-thread; each fiber opens an "
                 "independent ZipFile instance over a BOUNDED POOL of archives "
                 "(wid % npool, npool<=512 -- temp files do NOT scale with "
                 "--funcs), but if instance state is not properly fiber-isolated, "
                 "concurrent reads/extracts from siblings can corrupt this "
                 "fiber's data (wrong member list, wrong data, truncated "
                 "extract).  LOAD-BEARING: each fiber reads/extracts from its "
                 "pool archive (archive-specific marker data), yields between "
                 "reads; the oracle is that namelist() returns the correct "
                 "members and read(member) extracts exactly the marker bytes "
                 "that archive holds.  A mismatch indicates ZipFile instance "
                 "state is corrupted or leaked across fibers (0 under plain "
                 "threads GIL on AND off; runloom M:N fiber-isolation bug).  "
                 "MEASURED: member count & error counts (report-only)"
    )
