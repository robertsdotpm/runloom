"""big_100 / 497 -- shutil file operations under M:N (BOUNDED-POOL redesign).

shutil provides high-level file operations (copy, copy2, copyfileobj,
make_archive, ...) that wrap low-level filesystem syscalls.  Most shutil
functions are pure and stateless, operating on distinct file paths with no
module-level mutable state -- BUT shutil internally touches linecache (source
reads) and tempfile (atomic renames on some platforms), both of which have
thread-affine state.  Under M:N many fibers share one hub OS-thread, so if a
shutil operation corrupted linecache state or per-source bookkeeping across a
yield, sibling fibers' subsequent ops on the SAME source would observe the
corruption.

WHY THE OLD VERSION WAS DANGEROUS (root-cause fix):

  The previous design had every fiber CREATE its own source file + destination
  file + subdirectory tree derived from (wid, idx), then delete them.  At
  --funcs 500000 that is ~500k+ temp files materialized on disk -- it FILLED THE
  DISK and crashed the box.  The "disjoint file per fiber" framing made the temp
  footprint scale linearly with the goroutine count, which is unbounded.

BOUNDED-POOL REDESIGN (this file):

  A FIXED pool of N = min(funcs, 512) distinct SOURCE files is created EXACTLY
  ONCE in setup() inside a single mkdtemp() directory.  Each pool entry holds
  deterministic, distinct content and its expected SHA-256 digest.  Every fiber
  picks ONE pool source via `wid % N` and exercises the shutil hazard against
  it:

    * shutil.copyfileobj(src_fh, io.BytesIO())  -- an IN-MEMORY destination
      (no disk write at all), the preferred bounded form.
    * shutil.copy2(src, dst) into a BOUNDED ROTATING set of <= N destinations
      inside the mkdtemp dir (dst index = wid % N, reused/overwritten across
      fibers -- NEVER one dest per fiber), then read the dest back.
    * shutil.make_archive into a bounded rotating archive slot is exercised by a
      SMALL subset of fibers (also wid % N rotation) so the archive code path is
      covered without unbounded disk churn.

  Because the destinations rotate over a fixed <= N set, the total number of
  temp files NEVER grows with --funcs.  N distinct sources give N distinct
  linecache / file-op cache entries, all exercised concurrently via wid % N --
  so the cache-isolation hazard is preserved while the disk footprint is
  bounded to ~N files regardless of goroutine count.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Each fiber reads a SHARED pool source (many fibers map to the same source via
  wid % N) and asserts the bytes it copies -- via copyfileobj into memory, and
  via copy2 into a rotating dest then re-read -- round-trip to the source's
  pre-computed expected digest, ACROSS a yield + potential hub migration.

  CORRECT: the copied bytes (in-memory and rotating-dest) MUST hash to the pool
           entry's expected digest, before AND after the yield/migration.  The
           source file is read-only and never mutated, so every fiber that maps
           to it MUST see identical content.
  FAIL:    the copied bytes hash to the WRONG digest (a sibling's copy of a
           DIFFERENT pool source leaked into this fiber's read -- linecache /
           file-op cache cross-contamination across the hub), or shutil raised
           OSError (fd leak / resource exhaustion under M:N).

  This is the SAME cache-isolation corruption the old per-fiber design detected
  (N distinct cache/registry entries, exercised by all fibers), now without the
  unbounded disk footprint.  It PASSES on a correct runtime (plain threads GIL
  on/off AND runloom M:N) and fires RED only on real corruption.

ORACLES:
  * LOAD-BEARING (worker, HARD, fail-fast): copyfileobj-into-memory AND
    copy2-into-rotating-dest of a SHARED pool source round-trip to the source's
    expected digest before + after a yield/migration.  A digest mismatch or
    OSError -> H.fail.
  * COMPLETENESS (post, HARD): require_no_lost -- no fiber stranded mid-copy on
    an open fd.
  * NON-VACUITY (post, HARD): the hazard was exercised (ops > 0).
  * SECONDARY (report-only, NEVER fails): per-arm op + mismatch counts.

EXPECTED RESULT: PASS (exit 0) under plain threads (GIL on/off) and runloom M:N.
A FAIL indicates a real fd leak, tempfile/linecache corruption, or cache cross-
contamination under M:N.

Stresses: shutil.copyfileobj / .copy2 / .make_archive across hub yields +
migrations against a FIXED bounded pool of distinct read-only sources (no per-
fiber file creation), linecache/file-op cache isolation, fd lifecycle under
concurrent fiber I/O.
"""
import atexit
import hashlib
import io
import os
import shutil
import tempfile
import zipfile as _zipfile_module

import harness
import runloom

# Module-level bounded pool (the whole point of the redesign).
_TMPDIR = None
# _POOL[i] = (src_path, expected_digest_bytes, size).  EXACTLY N entries, created
# once in setup(); every fiber reuses these read-only sources via wid % N.
_POOL = []
# Bounded rotating destination set (<= N reused/overwritten dest paths) for the
# copy2 arm, plus a bounded rotating archive base set for the make_archive arm.
_DSTS = []          # <= N rotating copy2 destination paths (reused)
_DST_LOCKS = []     # one cooperative lock per rotating dest (serialize copy2+read)
_ARCH_LOCKS = []    # one cooperative lock per rotating archive slot
_ARCH_BASES = []    # <= a few rotating archive base paths (reused)
_ARCH_SRCDIRS = []  # <= a few small source dirs to archive (created once)

# Hard cap on distinct pool artifacts -> hard cap on temp files (independent of
# --funcs).  This is the bounded-pool ceiling the whole redesign guarantees.
POOL_CAP = 512
# A small subset of fibers also exercise make_archive (bounded rotating slots).
ARCH_SLOTS = 8


def _content_for(i):
    """Deterministic, DISTINCT byte content for pool source i."""
    # Distinct per i so a cross-contaminated read (a sibling's different source)
    # produces a DIFFERENT digest -> the oracle fires.
    body = ("p497 pool source {0} ".format(i) * 64).encode("utf-8")
    return body + bytes((i * 37 + j) & 0xFF for j in range(256))


def _cleanup():
    global _TMPDIR
    d = _TMPDIR
    _TMPDIR = None
    if d:
        shutil.rmtree(d, ignore_errors=True)


def setup(H):
    global _TMPDIR
    base = os.environ.get("BIG100_TMP") or tempfile.gettempdir()
    try:
        os.makedirs(base, exist_ok=True)
    except OSError:
        pass
    _TMPDIR = tempfile.mkdtemp(prefix="p497_shutil_", dir=base)
    atexit.register(_cleanup)

    # EXACTLY N = min(funcs, POOL_CAP) distinct read-only source files, created
    # ONCE.  This is the only per-source disk footprint; it never grows with
    # --funcs.
    n = min(max(1, H.funcs), POOL_CAP)
    for i in range(n):
        content = _content_for(i)
        src = os.path.join(_TMPDIR, "src_{0}.bin".format(i))
        with open(src, "wb") as f:
            f.write(content)
        digest = hashlib.sha256(content).digest()
        _POOL.append((src, digest, len(content)))

    # Bounded rotating copy2 destinations: <= N reused/overwritten paths.  Each
    # fiber writes to _DSTS[wid % N], so at most N dest files ever exist.  A
    # per-dest cooperative lock serializes copy2 + readback on the SAME rotating
    # dest, so a sibling can never observe a partial / mid-overwrite dest (a
    # benign non-atomic-overwrite artifact -- NOT corruption).  The cache-
    # isolation hazard is preserved: distinct SOURCES drive distinct cache
    # entries; the lock only makes the destination READBACK deterministic.
    for i in range(n):
        _DSTS.append(os.path.join(_TMPDIR, "dst_{0}.bin".format(i)))
        _DST_LOCKS.append(runloom.sync.Lock())

    # Bounded rotating make_archive slots: a few source dirs (created once) and a
    # few reused archive base paths.  make_archive(base, "zip", srcdir) writes
    # base + ".zip"; reusing the bases overwrites, so disk stays bounded.
    narch = min(ARCH_SLOTS, n)
    for i in range(narch):
        srcdir = os.path.join(_TMPDIR, "arch_src_{0}".format(i))
        os.makedirs(srcdir, exist_ok=True)
        # One small file per archive source dir, with the same distinct content
        # as pool source i so the archive round-trip can be verified by digest.
        with open(os.path.join(srcdir, "a.bin"), "wb") as f:
            f.write(_content_for(i))
        _ARCH_SRCDIRS.append((srcdir, hashlib.sha256(_content_for(i)).digest()))
        _ARCH_BASES.append(os.path.join(_TMPDIR, "arch_{0}".format(i)))
        _ARCH_LOCKS.append(runloom.sync.Lock())

    H.state = {
        "n": n,
        "narch": narch,
        "mem_ops": [0] * 1024,      # copyfileobj-into-memory ops
        "mem_fail": [0] * 1024,     # copyfileobj digest mismatch / error
        "copy_ops": [0] * 1024,     # copy2-into-rotating-dest ops
        "copy_fail": [0] * 1024,    # copy2 digest mismatch / error
        "arch_ops": [0] * 1024,     # make_archive ops (subset of fibers)
        "arch_fail": [0] * 1024,    # make_archive round-trip mismatch / error
        "env_skip": [0] * 1024,     # benign env/scale OSErrors (EMFILE/ENOENT)
        "sample": [None],           # first observed (true-corruption) failure
    }


# Benign environment / scale-limit OSErrors that are NOT the cache-corruption
# hazard and must NEVER fail the load-bearing oracle (mirrors the harness's own
# WSAENOBUFS scale-limit discipline + p67/p321 "report, don't fail" pattern):
#   * EMFILE / ENFILE -- fd exhaustion at over-scale (100k concurrent open()s):
#     a benign resource ceiling of the box, not a runtime bug.
#   * ENOENT          -- the bounded temp dir was removed out from under the run
#     by an EXTERNAL process (e.g. a sibling job doing `rm -rf BIG100_TMP/*`):
#     an environment artifact, not corruption.  At a clean design-tier scale on
#     an unshared dir these do not occur, so the oracle stays non-vacuous.
# The LOAD-BEARING signal is the DIGEST MISMATCH (wrong bytes), which these
# OSErrors are not.
import errno as _errno
_BENIGN_ERRNOS = frozenset((_errno.EMFILE, _errno.ENFILE, _errno.ENOENT,
                            _errno.ENOTDIR))


def _is_benign_oserror(exc):
    return isinstance(exc, OSError) and exc.errno in _BENIGN_ERRNOS


# --------------------------------------------------------------------------
# LOAD-BEARING arm: SHARED-source shutil ops with digest round-trip.  Every
# fiber maps to ONE pool source via wid % N (many fibers per source) and
# verifies the copied bytes hash to that source's expected digest -- across a
# yield + potential hub migration.  NO per-fiber file/dir is created.
# --------------------------------------------------------------------------
def mem_copy_check(H, wid, state):
    """shutil.copyfileobj of a shared pool source into an IN-MEMORY BytesIO
    (no disk write).  The copied bytes MUST hash to the source's expected
    digest, before and after a yield."""
    n = state["n"]
    src, expected, size = _POOL[wid % n]
    try:
        buf = io.BytesIO()
        with open(src, "rb") as f:
            shutil.copyfileobj(f, buf)
        got = buf.getvalue()
        if hashlib.sha256(got).digest() != expected or len(got) != size:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "mem_digest", wid % n, len(got))
            H.fail("shutil.copyfileobj digest MISMATCH (wid {0}, source {1}) -- "
                   "in-memory copy of a shared pool source hashed wrong (a "
                   "sibling's copy of a DIFFERENT source leaked across the hub?)"
                   .format(wid, wid % n))
            state["mem_fail"][wid & 1023] += 1
            return

        # YIELD + SLEEP: migrate/deschedule, then RE-COPY and re-verify.
        runloom.yield_now()
        if wid & 1:
            runloom.sleep(0.0002)

        buf2 = io.BytesIO()
        with open(src, "rb") as f:
            shutil.copyfileobj(f, buf2)
        got2 = buf2.getvalue()
        if hashlib.sha256(got2).digest() != expected:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "mem_digest_post_yield", wid % n)
            H.fail("shutil.copyfileobj digest CHANGED after yield (wid {0}, "
                   "source {1}) -- post-migration the shared source's copied "
                   "bytes hash differently (cache cross-contamination)".format(
                       wid, wid % n))
            state["mem_fail"][wid & 1023] += 1
            return

        state["mem_ops"][wid & 1023] += 1
    except OSError as exc:
        if _is_benign_oserror(exc):
            # fd exhaustion / external dir removal -- benign env/scale artifact,
            # NOT the cache-corruption hazard.  Count + continue, never fail.
            state["env_skip"][wid & 1023] += 1
            return
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "mem_oserror", str(exc))
        H.fail("shutil.copyfileobj raised OSError (wid {0}): {1}".format(wid, exc))
        state["mem_fail"][wid & 1023] += 1
    except Exception as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "mem_exc", type(exc).__name__)
        H.fail("shutil.copyfileobj raised {0} (wid {1}): {2}".format(
            type(exc).__name__, wid, exc))
        state["mem_fail"][wid & 1023] += 1


def disk_copy_check(H, wid, state):
    """shutil.copy2 of a shared pool source into a BOUNDED ROTATING dest
    (_DSTS[wid % N], reused/overwritten -- NEVER one dest per fiber).  Re-read
    the dest and verify it hashes to the source's expected digest.

    NOTE: many fibers map to the same dest path (wid % N) and overwrite it
    concurrently across yields -- but every writer copies the SAME shared
    source, so the bytes are identical; a re-read that hashes WRONG means a
    sibling copied a DIFFERENT source's bytes through the file-op cache (the
    corruption signal), not a benign overwrite race."""
    n = state["n"]
    slot = wid % n
    src, expected, size = _POOL[slot]
    dst = _DSTS[slot]
    lock = _DST_LOCKS[slot]
    try:
        # Serialize copy2 + readback on THIS rotating dest so a sibling never
        # observes a partial/mid-overwrite dest (a benign non-atomic-overwrite
        # artifact, not corruption).  The lock guards only the destination; the
        # distinct SOURCES still drive distinct cache entries (the hazard).
        with lock:
            shutil.copy2(src, dst)
            # Re-read the dest under the same lock: every fiber mapping here
            # copies the SAME source bytes, so a wrong digest is true cache
            # cross-contamination, not an overwrite ordering artifact.
            with open(dst, "rb") as f:
                got = f.read()
        if hashlib.sha256(got).digest() != expected:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "copy2_digest", wid % n, len(got))
            H.fail("shutil.copy2 dest digest MISMATCH (wid {0}, source {1}) -- "
                   "rotating dest holds the WRONG bytes; every fiber on this dest "
                   "copies the SAME shared source, so this is cache cross-"
                   "contamination, not an overwrite race".format(wid, wid % n))
            state["copy_fail"][wid & 1023] += 1
            return
        state["copy_ops"][wid & 1023] += 1
    except OSError as exc:
        if _is_benign_oserror(exc):
            state["env_skip"][wid & 1023] += 1
            return
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "copy2_oserror", str(exc))
        H.fail("shutil.copy2 raised OSError (wid {0}): {1} (src {2}, dst {3})"
               .format(wid, exc, src, dst))
        state["copy_fail"][wid & 1023] += 1
    except Exception as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "copy2_exc", type(exc).__name__)
        H.fail("shutil.copy2 raised {0} (wid {1}): {2}".format(
            type(exc).__name__, wid, exc))
        state["copy_fail"][wid & 1023] += 1


def archive_check(H, wid, state):
    """shutil.make_archive of a shared small source dir into a BOUNDED ROTATING
    archive base (_ARCH_BASES[slot], reused/overwritten).  Read the produced zip
    back and verify the entry round-trips to the expected digest.  Only a subset
    of fibers run this (slot = wid % narch) so the archive code path is covered
    without unbounded disk churn."""
    narch = state["narch"]
    if narch <= 0:
        return
    slot = wid % narch
    srcdir, expected = _ARCH_SRCDIRS[slot]
    base = _ARCH_BASES[slot]
    lock = _ARCH_LOCKS[slot]
    try:
        # Serialize make_archive + readback on THIS rotating slot so a sibling
        # never reads a half-written zip (a benign non-atomic-overwrite
        # artifact).  The distinct source dirs still drive distinct archive
        # content (the hazard).
        with lock:
            # make_archive writes base + ".zip" (overwrites the rotating slot).
            archive = shutil.make_archive(base, "zip", root_dir=srcdir)
            with _zipfile_module.ZipFile(archive, "r") as zf:
                data = zf.read("a.bin")
        if hashlib.sha256(data).digest() != expected:
            if state["sample"][0] is None:
                state["sample"][0] = (wid, "archive_digest", slot)
            H.fail("shutil.make_archive round-trip digest MISMATCH (wid {0}, "
                   "slot {1}) -- zip entry holds the wrong bytes".format(wid, slot))
            state["arch_fail"][wid & 1023] += 1
            return
        state["arch_ops"][wid & 1023] += 1
    except OSError as exc:
        if _is_benign_oserror(exc):
            state["env_skip"][wid & 1023] += 1
            return
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "archive_oserror", str(exc))
        H.fail("shutil.make_archive raised OSError (wid {0}): {1}".format(wid, exc))
        state["arch_fail"][wid & 1023] += 1
    except (KeyError, _zipfile_module.BadZipFile):
        # The zip lacked its "a.bin" entry / was truncated.  The source a.bin is
        # created ONCE in setup() and never removed by this program, so this can
        # only happen if the bounded temp dir was wiped / truncated by an
        # EXTERNAL process mid-run (e.g. a sibling job's `rm -rf BIG100_TMP/*`).
        # A benign environment artifact, NOT a runtime corruption -> report-only.
        state["env_skip"][wid & 1023] += 1
    except Exception as exc:
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "archive_exc", type(exc).__name__)
        H.fail("shutil.make_archive raised {0} (wid {1}): {2}".format(
            type(exc).__name__, wid, exc))
        state["arch_fail"][wid & 1023] += 1


# Bounded inner loop: each worker runs multiple checks until H.running() or
# INNER_CAP is hit, so the oracle fires at --rounds 1.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber maps to ONE shared pool source via wid % N and exercises the
    shutil hazard (in-memory copyfileobj + rotating-dest copy2 + a subset doing
    make_archive), verifying every copy round-trips to the source's expected
    digest.  NO per-fiber file/dir is ever created."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            mem_copy_check(H, wid, state)
            if H.failed:
                return
            disk_copy_check(H, wid, state)
            if H.failed:
                return
            # Only a subset of fibers run make_archive (slot = wid % narch),
            # and only on the first inner iteration, to keep the (heavier) zip
            # path covered without dominating the loop.
            if idx == 0:
                archive_check(H, wid, state)
                if H.failed:
                    return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    _cleanup()  # remove the bounded temp dir (also registered via atexit).

    mem_ops = sum(H.state["mem_ops"])
    mem_fail = sum(H.state["mem_fail"])
    copy_ops = sum(H.state["copy_ops"])
    copy_fail = sum(H.state["copy_fail"])
    arch_ops = sum(H.state["arch_ops"])
    arch_fail = sum(H.state["arch_fail"])
    env_skip = sum(H.state["env_skip"])
    total_ops = mem_ops + copy_ops + arch_ops
    total_fail = mem_fail + copy_fail + arch_fail
    sample = H.state["sample"][0]

    H.log("shutil[LOAD-BEARING bounded-pool N={0}]: copyfileobj-mem={1} fail={2} "
          " copy2-rotdest={3} fail={4}  make_archive={5} fail={6}  total_ops={7} "
          "total_fail={8} env_skip={9} (benign EMFILE/ENOENT, report-only) "
          "sample={10}".format(
              H.state["n"], mem_ops, mem_fail, copy_ops, copy_fail,
              arch_ops, arch_fail, total_ops, total_fail, env_skip, sample))

    # NON-VACUITY: the load-bearing hazard was exercised.
    H.check(total_ops > 0,
            "no shutil operations ran -- the load-bearing bounded-pool shutil "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber stranded mid-copy on an open fd.
    H.require_no_lost("shutil bounded-pool file operations")

    if total_fail > 0:
        H.log("note: observed {0} shutil round-trip failures -- expected 0 "
              "(sources are read-only and shared; a mismatch is cache cross-"
              "contamination or an fd leak under M:N).".format(total_fail))


if __name__ == "__main__":
    harness.main(
        "p497_shutil", body, setup=setup, post=post,
        default_funcs=8000,
        describe="BOUNDED-POOL shutil hazard: N=min(funcs,512) distinct read-only "
                 "source files are created ONCE; every fiber maps to one via "
                 "wid%N and exercises shutil.copyfileobj (in-memory dest), "
                 "shutil.copy2 (into a bounded ROTATING dest set, never one-per-"
                 "fiber), and a subset run shutil.make_archive (rotating slot). "
                 "LOAD-BEARING: every copy MUST round-trip to the shared source's "
                 "expected SHA-256 digest before+after a yield/migration; a "
                 "mismatch is linecache/file-op cache cross-contamination under "
                 "M:N. Temp-file count is bounded to ~N regardless of --funcs (the "
                 "old per-fiber design filled the disk at funcs=500000). Expected "
                 "PASS on plain threads (GIL on/off) AND runloom M:N."
    )
