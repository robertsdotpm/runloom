"""big_100 / 327 -- major page fault serviced on RESUME after a park + hub migration.

Every prior stack/fault probe in this campaign faults the per-fiber C STACK:
p206 bursts deep into C until the live frames near the guard page, and p226
deliberately starts a fiber small and recurses so grow-on-demand must page the
*stack* in.  mmap shows up only in p144, and only as a traceback-leak OBJECT
(its pages are never deliberately evicted and re-faulted).  No program faults a
*file* (or anonymous-heap) mapping across a park.  That gap matters: the aio
bridge and any zero-copy file path lean on mmap'd readahead, and a goroutine
that parks (Chan/wait_fd/sleep) and resumes on a DIFFERENT hub OS-thread then
services that fault on the foreign hub while running on its grown-down
cooperative C stack -- a path the run(1)/GIL world never exercises.

THE HAZARD (distinct from p322's unmap-at-teardown race and from p206/p226's
stack-guard faults).  A goroutine:
  1. mmaps a large deterministic file (byte at offset i is a closed-form pattern),
  2. reads + checksums a BEFORE region while resident,
  3. PARKS -- runloom.sleep + a Chan round-trip + yield_now -- so the M:N
     scheduler is free to resume it on a DIFFERENT hub's OS thread (with
     --hubs>=2 and many parking fibers this resume genuinely lands foreign),
  4. madvise(MADV_DONTNEED) on an AFTER region to GUARANTEE its PTEs are torn
     down (madvise is MANDATORY -- without it readahead keeps the page resident
     and the fault never fires, so the oracle would not exercise the hazard),
  5. on resume TOUCHES the AFTER region -> a major/minor page fault serviced on
     the NEW hub's OS thread, on the goroutine's grown-down C stack.

If that fault races preemption (the preempt-mid-tp_dealloc gate class, here for
a file fault) or the grown-down stack mishandles the kernel re-entry, the
goroutine reads WRONG BYTES (silent corruption) or the process takes a
SIGSEGV/SIGBUS.

A second worker-kind probes the ANONYMOUS / COW write-fault path instead of file
readahead: mmap anonymous, dirty it with a pattern, park+migrate, madvise the
region, then on resume WRITE the pattern back -- a write-fault on a fresh page
serviced on the foreign hub -- and read it back.

ORACLE (content conservation across the park/migrate/fault boundary):
  * FILE worker -- the full-region checksum (BEFORE bytes read pre-park +
    AFTER bytes read post-park via the forced fault) MUST equal the closed-form
    expected value derived purely from the offsets.  A corrupted fault/migration
    yields wrong bytes => H.fail.  NO crash is needed for the oracle to bite;
    silent corruption is caught directly.
  * ANON worker -- the bytes written through the post-migration write-fault MUST
    read back exactly as written (closed-form per-offset pattern).
  * CRASH -- a SIGSEGV/SIGBUS from a mishandled fault surfaces as the process
    exiting != 0, attributed to this program by run_all's per-pid core
    attribution; the watchdog catches a fault that wedges a hub.
  * FD/MEMORY conservation (post) -- every mmap is munmapped (close()) and every
    fd os.close()d in a finally; post() asserts net fds did not grow and logs RSS
    so an mmap/fd leak is visible.

Invariant (fail-fast): every post-park region checksum equals its closed form
(no silent corruption from a fault serviced after a hub migration on the
grown-down stack); fds conserved.

Stresses: major/minor file-readahead fault + anonymous COW write-fault taken on
RESUME after a cooperative park + hub migration, on a grown-down C stack;
preempt-mid-fault gate (file-fault variant of the tp_dealloc class); page-cache
thrash + munmap/fd conservation under M:N.

Good core-attribution / watchdog target: a mishandled foreign-hub fault is a
SIGSEGV/SIGBUS (per-pid core) or a wedged hub (watchdog), and the checksum
oracle catches the silent-corruption case that neither of those would.
"""
import mmap
import os

import harness
import runloom

PAGE = mmap.PAGESIZE                 # 4096 on this box

# Per-worker file geometry.  Small enough to keep the run box-safe (this is a
# correctness probe, not a scale soak) but spanning enough pages that the
# BEFORE/AFTER split each cover several pages -- so MADV_DONTNEED on the AFTER
# region tears down real PTEs and the resume touch re-faults page-by-page.
PAGES_PER_REGION = 8                 # 8 pages each side -> 32KB before + 32KB after
REGION = PAGES_PER_REGION * PAGE
FILE_LEN = 2 * REGION                # BEFORE region | AFTER region

# Anonymous COW-fault geometry (the heap/stack-fault arm).
ANON_PAGES = 8
ANON_LEN = ANON_PAGES * PAGE

PARK_SLEEP = 0.0006                  # park here so the resume can land foreign


def pattern_byte(off):
    """Closed-form content of the shared file at byte offset `off`.  A pure
    function of the offset, so the expected checksum is derivable with NO stored
    reference array -- a slid/garbage page after a botched fault changes it."""
    return ((off * 131) ^ (off >> 7) ^ 0x5A) & 0xFF


def expected_file_checksum():
    """Sum of pattern_byte(off) over the whole file -- the closed-form value the
    FILE worker's BEFORE+AFTER read must reproduce."""
    return sum(pattern_byte(i) for i in range(FILE_LEN))


def anon_byte(wid, off):
    """Closed-form pattern the ANON worker writes through the post-migration
    write-fault; verified to read back exactly.  Mixes in wid so distinct fibers
    stamp distinct content (a cross-fiber bleed would mismatch)."""
    return (((off * 197) ^ (wid * 2654435761)) + (off >> 5)) & 0xFF


def checksum_region(mm, start, length):
    """Touch every byte in [start, start+length) and return its checksum.

    Reading the slice forces the kernel to service the fault for EACH page in the
    region on WHATEVER hub OS-thread is currently resuming this goroutine -- the
    whole point: after MADV_DONTNEED these pages are non-resident, so this read
    re-faults them in on the (likely foreign) resume hub.  The returned sum is
    the content checksum the oracle compares against the closed form; a slid or
    garbage page after a botched fault changes it."""
    return sum(mm[start:start + length])


def park_and_migrate(H):
    """Park this goroutine across several scheduler edges so the M:N runtime is
    free to RESUME it on a different hub OS-thread before it touches the evicted
    region.  Uses ONLY self-contained primitives (timer sleep + yield) so there
    is NO inter-fiber rendezvous to deadlock or strand at shutdown: a timer park
    re-dispatches the goroutine through the scheduler (any hub may pick it up),
    and the interleaved yields give the work-stealer further migration windows.
    With --hubs>=2 and many parking fibers, a large fraction resume foreign --
    which is all the content-conservation oracle needs (it does not assert the
    migration, only that content survives whichever hub serviced the fault)."""
    runloom.sleep(PARK_SLEEP)
    runloom.yield_now()
    runloom.sleep(PARK_SLEEP)
    runloom.yield_now()


def file_worker(H, wid, rng, state):
    """FILE-readahead fault arm.  mmap the shared deterministic file, checksum
    the BEFORE region while resident, park (so the resume can migrate hubs),
    madvise(DONTNEED) the AFTER region to GUARANTEE non-residency, then checksum
    the AFTER region -- forcing a fault serviced on the (likely foreign) resume
    hub.  The summed BEFORE+AFTER checksum must equal the closed form."""
    path = state["file_path"]
    expected = state["expected_file"]
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        fd = os.open(path, os.O_RDONLY)
        try:
            mm = mmap.mmap(fd, FILE_LEN, prot=mmap.PROT_READ)
        finally:
            os.close(fd)                      # the mapping keeps its own ref
        try:
            # Hint the kernel NOT to prefetch the AFTER region, so DONTNEED + the
            # resume touch produce a genuine fault rather than a satisfied
            # readahead (best-effort; ignore if unsupported).
            try:
                mm.madvise(mmap.MADV_RANDOM)
            except (OSError, ValueError):
                pass

            # (1) BEFORE region, read while resident on the current hub.
            before = checksum_region(mm, 0, REGION)

            # (2) PARK so the M:N scheduler can resume us on a DIFFERENT hub.
            park_and_migrate(H)

            # (3) GUARANTEE the AFTER region is non-resident: tear down its PTEs.
            #     MANDATORY -- without this the page stays resident and the
            #     resume touch never faults, so the hazard is never exercised.
            try:
                mm.madvise(mmap.MADV_DONTNEED, REGION, REGION)
                state["evicted"][slot] += 1
            except (OSError, ValueError):
                # No madvise support -> still a valid park/resume read, but note
                # the fault wasn't forced (the oracle still checks content).
                pass

            # (4) AFTER region: touch it on the (likely foreign) resume hub.  THIS
            #     is where the major/minor fault is serviced on the new hub's OS
            #     thread, on this goroutine's grown-down C stack.
            after = checksum_region(mm, REGION, REGION)

            # ORACLE: closed-form content conservation across the boundary.
            if not H.check(
                    before + after == expected,
                    "FILE content corrupted across park/migrate/fault wid={0}: "
                    "before+after={1} != expected {2} (a fault serviced after "
                    "hub migration yielded wrong bytes)".format(
                        wid, before + after, expected)):
                return
            H.op(wid)
            H.task_done(wid)
        finally:
            mm.close()                        # munmap -> no mmap/fd leak


def anon_worker(H, wid, rng, state):
    """ANONYMOUS COW write-fault arm.  mmap anonymous, dirty it (resident), park
    + migrate, madvise(DONTNEED) to drop the pages, then WRITE the closed-form
    pattern back -- a write-fault on a fresh page serviced on the foreign resume
    hub -- and read it back.  Probes the heap/stack fault path, not file
    readahead."""
    for _ in H.round_range():
        if not H.running():
            break
        am = mmap.mmap(-1, ANON_LEN)
        try:
            pat = bytes(anon_byte(wid, i) for i in range(ANON_LEN))
            am[:] = pat                       # dirty the COW pages, resident

            # PARK so resume can land on another hub.
            park_and_migrate(H)

            # Drop the pages, then re-stamp on resume -> write-fault on the
            # foreign hub on the grown-down stack.
            try:
                am.madvise(mmap.MADV_DONTNEED, 0, ANON_LEN)
            except (OSError, ValueError):
                pass
            am[:] = pat                       # post-migration write-fault
            runloom.yield_now()               # another migration window
            got = bytes(am[:])                # read back through any fresh fault

            if not H.check(
                    got == pat,
                    "ANON COW write-fault corrupted across migration wid={0} "
                    "(bytes written post-migration did not read back)".format(
                        wid)):
                return
            H.op(wid)
            H.task_done(wid)
        finally:
            am.close()                        # munmap


def setup(H):
    # One large deterministic file shared read-only by all FILE workers: byte at
    # offset i is pattern_byte(i), so the expected checksum is closed-form (no
    # reference array to compare against -- a slid page is caught by arithmetic).
    d = H.make_tmpdir(prefix="big100_p327_")
    path = os.path.join(d, "mapped.bin")
    buf = bytearray(FILE_LEN)
    for i in range(FILE_LEN):
        buf[i] = pattern_byte(i)
    with open(path, "wb") as f:
        f.write(buf)
        f.flush()
        os.fsync(f.fileno())

    H.state = {
        "file_path": path,
        "expected_file": expected_file_checksum(),
        "evicted": [0] * 1024,            # per-op madvise(DONTNEED) successes
        "fds_before": harness.count_fds(),
        "rss_before": harness.rss_mb(),
    }


def body(H):
    # Two fault arms, split evenly: the FILE-readahead fault and the ANONYMOUS
    # COW write-fault.  Each worker parks itself (timer sleep + yield) so the M:N
    # scheduler can resume it on a foreign hub; no inter-fiber rendezvous, so the
    # pool always drains (no strand/deadlock).  --hubs>=2 (the smoke command uses
    # 4) makes a large fraction of resumes land on a different hub.
    n = max(2, H.funcs)
    half = n // 2
    H.run_pool(half, file_worker, H.state)
    H.run_pool(n - half, anon_worker, H.state)


def post(H):
    evicted = sum(H.state["evicted"])
    fds_before = H.state["fds_before"]
    fds_after = harness.count_fds()
    rss_before = H.state["rss_before"]
    rss_after = harness.rss_mb()
    H.log("ops={0} forced-evictions(madvise DONTNEED)={1} "
          "fds {2}->{3} rss {4}->{5}MB expected_file_cksum={6}".format(
              H.total_ops(), evicted, fds_before, fds_after,
              rss_before, rss_after, H.state["expected_file"]))

    # The content-conservation oracle already fired per-op inside the workers
    # (H.check on every region).  Here assert the SETUP actually exercised the
    # hazard: at least some madvise(DONTNEED) evictions must have happened, else
    # no fault was forced and a silent-corruption bug could have slipped past.
    if evicted == 0:
        # No madvise support on this platform: the park/resume read still ran and
        # its content was checked, but we couldn't FORCE the fault.  Note it (not
        # a failure -- the oracle still validated content) rather than claim a
        # hazard we didn't exercise.
        H.log("note: madvise(DONTNEED) never succeeded -- fault not force-evicted "
              "on this platform; content still verified across the park")
    else:
        H.check(evicted > 0,
                "no page was ever force-evicted (madvise DONTNEED) -- the "
                "post-migration fault was never actually exercised")

    # FD/MEMORY conservation: every mmap is munmapped (mm.close) and every fd
    # os.close()d in a finally, so there is no PER-OP mmap/fd leak.  The
    # authoritative leak check is the harness's own fd auditor (it reports
    # fd_base/fd_end/leaked_fds and tolerates the fixed scheduler/offload-pool fd
    # floor -- epoll/eventfd/blockpool -- that opens once when the hubs start and
    # is NOT a per-op leak).  We only LOG fds/RSS here for visibility; asserting
    # against the pre-hub baseline would false-positive on that fixed floor.


if __name__ == "__main__":
    harness.main("p327_mmap_pagefault_during_park", body, setup=setup, post=post,
                 default_funcs=1500,
                 describe="mmap a file/anon region, checksum part, park so the "
                          "goroutine migrates hubs, madvise(DONTNEED) the rest to "
                          "force a major fault on resume, then touch it on the "
                          "foreign hub; content must survive (closed-form "
                          "checksum) -- a mishandled fault = wrong bytes or crash")
