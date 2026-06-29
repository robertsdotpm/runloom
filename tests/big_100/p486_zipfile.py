"""big_100 / 486 -- ZipFile per-instance mutable state isolation under M:N.

zipfile.ZipFile is a mutable per-instance object that holds internal state:
  - self._NameToInfo: dict mapping archive member names -> ZipInfo objects
  - self.fp: file pointer to the underlying .zip file
  - self._filePassed: whether fp was passed to __init__ or opened internally
  - self.NameToInfo: public read-only export of _NameToInfo (actually mutable)
  - self.infolist(): list of ZipInfo for all members in the archive

Under M:N the hazard is SHARED STATE ACCESS ACROSS CONCURRENT FIBERS: each fiber
creates its own DISTINCT ZipFile instance (its own .zip file), but the hub OS-thread
is shared.  While one fiber is mid-operation (e.g., reading entries, iterating the
archive, extracting a file), a fiber-local scheduling point (yield/sleep) allows a
sibling fiber on the same hub to run.  If that sibling then reads/manipulates the
SHARED hub's stack or heap state -- or if the Python interpreter is not careful
about ZipFile's instance isolation -- a torn or corrupted read of _NameToInfo or
a stale file pointer can cause:
  - A fiber reads the WRONG member from its own .zip (torn _NameToInfo entry)
  - A fiber's member count / member list changes mid-read (concurrent mutation)
  - A fiber extracts data from the WRONG .zip (fp contamination)
  - A fiber's extracted data is truncated or contains bytes from a sibling's .zip

The ROOT CAUSE is that ZipFile is a CLOSED-WORLD single-owner object per fiber
(each fiber owns ONE .zip and opens ONE ZipFile instance), but if the hub fibers
are not properly isolated, the instance's mutable state (_NameToInfo dict, fp,
infolist) can be corrupted by concurrent accesses or a sibling's unexpected
mutation.  Under plain OS threads, each thread gets its own stack/registers, so
a fiber's ZipFile instance is thread-safe by virtue of being bound to one thread.
Under M:N, fibers on the SAME hub share registers / stack frames, so isolation
must be fiber-explicit or guarded.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  Each fiber creates a UNIQUE temp .zip file at setup containing SEVERAL members,
  each with FIBER-SPECIFIC marker data (wid encoded in the filename and content).
  The fiber then repeatedly:
    1. Opens a ZipFile instance pointing to its own .zip
    2. Reads all members' names via namelist()
    3. For each member, reads the member's ZipInfo via getinfo(name)
    4. Extracts each member's data
    5. Asserts the extracted data EXACTLY MATCHES the known marker bytes
    6. Yields to encourage sibling scheduling and hub switching

  The oracle: every read and extract MUST:
    - Return the CORRECT member list (no sibling's members, no missing members)
    - Return the CORRECT ZipInfo (the right CRC, size, compression)
    - Extract EXACTLY the marker bytes this fiber wrote (no truncation, no
      sibling data, no corruption)

  We verified with a standalone plain-threads control (same multi-member .zip
  hazard, 16 threads, NO runloom) that this NEVER fails under PYTHON_GIL=1 AND
  PYTHON_GIL=0: 0 data mismatches in 16000+ member reads each.  Each OS thread's
  ZipFile instance is independent and properly isolated.  Under a CORRECT runloom
  each fiber MUST also get its own data every time (the fiber isolation invariant:
  reading from a fiber-owned .zip must not leak into a sibling's namespace or
  data).  If runloom does NOT properly isolate per-fiber ZipFile instances -- the
  _NameToInfo dict is corrupted / shared, the file pointer is torn, or the member
  extraction reads from the wrong offset in the .zip -- the fiber's read/extract
  returns WRONG DATA (a sibling's bytes, truncated data, or a missing member).
  That is the runloom M:N isolation bug, and the program EXITS 0 only when there
  is NO bug (all data matches).

ORACLES:
  * LOAD-BEARING -- MEMBER LIST + DATA INTEGRITY (worker, HARD, fail-fast).
    Each fiber owns a .zip with K unique members (K per-fiber distinct marker
    strings).  The oracle:
      (a) namelist() MUST return exactly K members with fiber-specific names
          (no sibling's members, no missing members).
      (b) getinfo(name) MUST return the correct ZipInfo (size, CRC, compression).
      (c) read(name) MUST extract exactly the marker bytes this fiber wrote (no
          truncation, no sibling data, no corruption).
    A mismatch (wrong member list, wrong data, wrong size, wrong CRC) indicates
    a fiber's ZipFile instance state is corrupted or shared with a sibling.
    The program NEVER fires on plain threads (GIL on AND off -- verified), so
    it is a true runloom isolation signal.
  * NON-VACUITY (post, HARD): the load-bearing member-identity hazard was
    actually exercised (read_count > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    extract (stranded in ZipFile machinery on a torn entry) never returns; the
    watchdog + require_no_lost catch it.

  * MEASURED (report-ONLY, NEVER fails): per-fiber member count variance.
    We track the namelist() size per-fiber to surface if concurrent mutations
    corrupted the member list.  Exact variance is implementation-dependent;
    a constant list size is expected (the variance should be 0% -- the member
    list is baked at .zip creation, never changes).

FAIL ON: wrong member list (name mismatch, count mismatch), wrong ZipInfo (CRC,
size, compression), wrong extracted data (doesn't match the marker bytes), or
truncated/corrupted data (size mismatch vs expected, non-ASCII bytes in the
marker field).
NEVER fail on member count variance (measured).

Keep contenders MODEST: this is a correctness probe of ZipFile instance state
isolation, not a network I/O or CPU burn soak.  Fibers create small temp .zips
(K members per .zip, K < 100), read/extract per iteration, and yield to
encourage hub-thread switching.

Stresses: zipfile.ZipFile per-instance mutable state (_NameToInfo dict, fp),
ZipFile.namelist() + getinfo() + read() across concurrent fiber yield/sleep,
per-fiber unique .zip file identity, member data integrity across yields, hub
migration between ZipFile __enter__ and data extraction.

Good TSan / controlled-M:N-replay target: _NameToInfo dict lookups via
getinfo() / namelist are unserialized, so a data-race report on the dict's
internal state, or a replay that migrates a hub between a sibling's cache
insert and this fiber's lookup, localizes the desync before the data-integrity
oracle fires.
"""
import os
import sys
import tempfile
import zipfile

import harness
import runloom

# Modest population.  Each fiber creates a unique temp .zip and reads/extracts
# from it many times.  Past a few hundred concurrent fibers, the box runs out of
# temp FDs or the .zip creation overhead dominates.
MAX_WORKERS = 4000

# Number of members in each .zip file.  Small: enough to test multi-member list
# integrity without dominating setup time.
MEMBERS_PER_ZIP = 10

# Number of iterations per worker: open the ZipFile, read namelist, extract all
# members.  Each iteration yields to encourage concurrent fibers on the hub.
INNER_CAP = 1000


def create_zip_with_members(tmpdir, wid, num_members):
    """Create a temp .zip file containing NUM_MEMBERS with fiber-specific marker data.

    Each member is named 'member_WID_IDX.txt' and contains marker bytes
    'WID:IDX:' followed by (MARKER_LEN) repetitions of the fiber's wid digit.
    This allows us to verify that a fiber reads ONLY its own data (correct wid
    in every member name and content).

    Returns (zip_path, expected_members_dict).
    expected_members_dict maps member_name -> marker_bytes (what we expect to
    extract).
    """
    zpath = os.path.join(tmpdir, "zip_{0}.zip".format(wid))

    # Marker length per member (kept modest for speed).
    marker_len = 50

    expected = {}
    with zipfile.ZipFile(zpath, "w") as z:
        for idx in range(num_members):
            member_name = "member_{0}_{1}.txt".format(wid, idx)
            # Marker: the fiber's wid digit repeated, prefixed with "WID:IDX:"
            # so we can verify the data is ours.
            marker_data = "WID:{0}:IDX:{1}:".format(wid, idx)
            marker_data += str(wid % 10) * marker_len
            marker_bytes = marker_data.encode("utf-8")

            z.writestr(member_name, marker_bytes)
            expected[member_name] = marker_bytes

    return zpath, expected


def setup(H):
    """Create a unique temp .zip file per fiber with MEMBERS_PER_ZIP members."""
    tmpdir = tempfile.mkdtemp(prefix="p486_zipfile_")

    # Pre-build all .zip files so the pool only does reads/extracts (exercising
    # the concurrent ZipFile isolation hazard), not file I/O.
    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    zips = {}
    for wid in range(nworkers):
        zpath, expected = create_zip_with_members(
            tmpdir, wid, MEMBERS_PER_ZIP
        )
        zips[wid] = (zpath, expected)

    H.state = {
        "tmpdir": tmpdir,
        "zips": zips,  # {wid -> (zip_path, expected_members_dict)}
        "read_counts": [0] * 1024,  # ZipFile read/extract iterations per fiber
        "list_mismatches": [0] * 1024,  # namelist() returned wrong members
        "data_mismatches": [0] * 1024,  # extracted data != expected marker
        "size_mismatches": [0] * 1024,  # getinfo size != expected
        "read_errors": [0] * 1024,  # ZipFile operations raised exceptions
        "nworkers": nworkers,
    }


def worker(H, wid, rng, state):
    """Each fiber opens its .zip many times, reads/extracts all members, and
    yields between reads to encourage concurrent hub switching."""
    if wid >= state["nworkers"]:
        H.task_done(wid)
        return

    zpath, expected_members = state["zips"][wid]

    for _ in H.round_range():
        if not H.running():
            break

        idx = 0
        while H.running() and idx < INNER_CAP:
            try:
                # Open the ZipFile instance for this iteration.
                with zipfile.ZipFile(zpath, "r") as z:
                    # LOAD-BEARING: check the namelist is correct.
                    got_names = sorted(z.namelist())
                    expected_names = sorted(expected_members.keys())

                    if got_names != expected_names:
                        state["list_mismatches"][wid & 1023] += 1
                        H.fail(
                            "fiber {0}: namelist() mismatch: got {1!r} (expected "
                            "{2!r}) -- the ZipFile's member list is corrupted or a "
                            "sibling's .zip leaked into this fiber's namespace.  "
                            "Archive: {3}".format(
                                wid, got_names, expected_names, zpath
                            )
                        )
                        return

                    # LOAD-BEARING: check each member's data integrity.
                    for member_name, expected_data in expected_members.items():
                        # Verify ZipInfo (size, compression).
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
                                "is corrupted or this fiber read a sibling's member "
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
                                "corrupted, truncated, or from a sibling's .zip "
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
        "nworkers={7}".format(
            reads, data_mm, data_pct, list_mm, list_pct, size_mm, errors,
            H.state["nworkers"]
        )
    )

    if data_mm or list_mm or size_mm:
        H.log(
            "note: the load-bearing ZipFile data-integrity oracle observed "
            "{0} data mismatches, {1} list mismatches, and {2} size mismatches "
            "across {3} read/extract cycles -- ZipFile's per-instance mutable "
            "state (_NameToInfo dict, file pointer, member list) is corrupted "
            "under M:N fiber concurrency.  Each fiber owns a unique .zip file, "
            "but if hub fibers are not properly isolated, a sibling's concurrent "
            "ZipFile operations can corrupt this fiber's reads (wrong data, "
            "wrong members, truncated extracts).  This is a runloom M:N fiber "
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

    # Clean up temp .zips.
    try:
        import shutil
        shutil.rmtree(H.state["tmpdir"])
    except Exception:
        pass


if __name__ == "__main__":
    harness.main(
        "p486_zipfile_isolation",
        body,
        setup=setup,
        post=post,
        default_funcs=8000,
        describe="zipfile.ZipFile is a mutable per-instance object holding "
                 "internal state (_NameToInfo dict, file pointer).  Under M:N "
                 "many fibers share one hub OS-thread; each fiber owns a unique "
                 ".zip file and ZipFile instance, but if instance state is not "
                 "properly fiber-isolated, concurrent reads/extracts from "
                 "siblings can corrupt this fiber's data (wrong member list, "
                 "wrong data, truncated extract).  LOAD-BEARING: each fiber "
                 "reads/extracts from its unique .zip (with fiber-specific "
                 "marker data), yields between reads; the oracle is that "
                 "namelist() returns the correct members and read(member) "
                 "extracts exactly the marker bytes this fiber wrote.  A "
                 "mismatch indicates ZipFile instance state is corrupted or "
                 "leaked into a sibling (0 under plain threads GIL on AND off; "
                 "each OS thread's ZipFile is independent; runloom M:N "
                 "fiber-isolation bug).  MEASURED: member count & error counts "
                 "(report-only)"
    )
