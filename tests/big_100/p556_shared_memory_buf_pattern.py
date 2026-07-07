"""big_100 / 556 -- multiprocessing.shared_memory.SharedMemory .buf pattern
conservation across a fiber park under M:N.

The subject is multiprocessing.shared_memory.SharedMemory(create=True, size=N):
a named POSIX shared-memory block (a /dev/shm file on Linux) whose backing store
is an mmap'd region.  SharedMemory exposes that region as `.buf`, a memoryview
over the mmap.  The lifecycle C state is: an open fd (from shm_open), an mmap
(the region), and a resource_tracker registration keyed by the block's name.
Creation/close/unlink churns all three.  Idiomatic use writes and reads bytes
THROUGH the `.buf` memoryview:

        shm = SharedMemory(create=True, size=N)
        mv  = shm.buf                     # memoryview export over the mmap
        mv[i] = value                     # store into the mapped region
        got  = mv[i]                       # load from the mapped region
        mv.release(); shm.close(); shm.unlink()

WHERE M:N COULD BREAK IT (the gap this program probes).  The `.buf` memoryview
is a live BUFFER EXPORT over an mmap owned by ONE fiber.  If a fiber obtains
`mv = shm.buf`, writes a wid-derived pattern into it, and then PARKS (a
cooperative yield/sleep), the export -- and the fd/mmap behind it -- must stay
exactly as this fiber left it when the fiber resumes, possibly on a DIFFERENT
hub thread.  The hazards a broken M:N runtime could introduce:

  * the memoryview export is RELEASED underneath the parked fiber (a stale
    close()/refcount drop crossing hubs) -> accessing mv after the park raises
    "operation forbidden on released memoryview" / ValueError;
  * the mmap/fd is ALIASED -- another fiber's SharedMemory ends up mapped at the
    same region, so this fiber reads back a SIBLING'S bytes instead of its own;
  * a torn mmap store -> a byte read back differs from the byte written, with no
    other writer in the closed world.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner value conservation).

  Each fiber creates its OWN SharedMemory block (a distinct /dev/shm name, a
  distinct fd + mmap, never shared with any sibling).  Per round it:
    - obtains a fresh `mv = shm.buf` export,
    - writes a UNIQUE per-(wid,round) byte pattern into the whole block, PARKING
      (runloom.yield_now / sleep) partway through the write so a sibling reliably
      interleaves while this fiber's half-written export is latched,
    - PARKS again with the block fully written,
    - reads every byte back through the SAME export and asserts it equals the
      pattern this fiber wrote -- and ONLY this fiber could have written, because
      the block is single-owner.
  A byte that reads back wrong is either a torn mmap store, a cross-fiber alias
  (a sibling's block mapped over ours), or a stale value from a previous round --
  all real runtime faults.  A released-export error across the park is a lost
  close crossing hubs.  On a CORRECT runtime this single-owner arm PASSES (the
  program exits 0 when there is no bug): a block only this fiber can write must
  read back exactly what this fiber wrote, park or no park, hub migration or not.

  This is single-owner VALUE CONSERVATION, exactly like p490's enum-member
  identity/value stability and p405's private-Counter control: there is NO
  sharing, so a mismatch cannot be documented shared-object semantics -- it can
  only be the runtime mishandling the fiber-owned export/mmap/fd across a park.

ORACLES:
  * LOAD-BEARING -- SINGLE-OWNER .buf PATTERN CONSERVATION (worker, HARD,
    fail-fast).  Write a unique per-(wid,round) pattern through the fiber-owned
    `.buf`, park across the write, read it all back, fail on ANY mismatch or a
    released-export error.  No sharing -> a mismatch is a runtime bug.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-write
    inside the mmap store or parked on a released export never returns; the
    watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (byte_checks > 0).

RESOURCE DISCIPLINE.  Each SharedMemory is a /dev/shm file + an fd + an mmap +
a resource_tracker registration.  This is RESOURCE-HEAVY, so max_funcs is capped
(800) -- the forever loop's --funcs 1000000 must never try to open a million shm
blocks.  Each fiber owns exactly ONE block for its lifetime (bounded fd use:
funcs blocks, not rounds x funcs), and an add_cleanup closure close()+unlink()s
it at the end.  The per-round `.buf` export is explicitly released in a finally
so close() never trips a lingering-export BufferError.

FAIL ON: a byte read back through the fiber-owned .buf that differs from what
this fiber wrote (torn store / cross-fiber alias / stale round), or a released-
memoryview error on the export across the park.  There is no shared arm -- the
block is single-owner, so every failure is falsifiable and points at the runtime.

Stresses: multiprocessing.shared_memory mmap store/load through a live `.buf`
memoryview export held across a fiber park + hub migration, per-fiber shm_open
fd + /dev/shm allocation and close()/unlink() lifecycle under M:N, single-owner
value conservation on an mmap-backed buffer.

Good TSan / controlled-M:N-replay target: the mmap store-then-load through the
`.buf` export, with a yield_now() forced between the write and the read, is a
per-byte write/read over a fiber-private region; a TSan report on the mmap store,
a released-export error, or a single mismatched byte under replay localizes the
lost/aliased export before the conservation scan even closes.
"""
import multiprocessing                       # imported BEFORE monkey.patch() (as
from multiprocessing import shared_memory     # in p444) so any mp primitives the
                                              # shm lifecycle touches are patched.

import harness
import runloom

# Size of each fiber-owned block.  Big enough that a torn store or a cross-fiber
# alias moves several bytes visibly, small enough that 800 blocks are a trivial
# /dev/shm footprint (800 * 512 B = 400 KiB).  Not a power that hits an mmap page
# edge specially -- we just want a real region to write across.
BLOCK_SIZE = 512

# Where inside the write we PARK so a sibling reliably interleaves with our
# half-written, still-latched .buf export.
PARK_AT = BLOCK_SIZE // 2


def expected_byte(wid, nonce, i):
    """The byte this fiber writes at offset i for a given (wid, nonce=round).

    Encodes wid so a cross-fiber alias (a sibling's block mapped over ours) reads
    back a WRONG byte, and nonce so a stale value left from a previous round is
    also caught.  Deterministic -> the read-back check is exact."""
    return (((wid * 131) ^ (nonce * 17) ^ i ^ 0x5A) & 0xFF)


def buf_pattern_round(H, wid, rng, state, shm, nonce):
    """One single-owner conservation round on this fiber's OWN SharedMemory.

    Obtain a fresh `.buf` export, write the unique per-(wid,nonce) pattern into
    it while PARKING partway (so a sibling interleaves with our latched export),
    park again fully written, then read every byte back through the SAME export
    and verify it equals what we wrote.  Any mismatch or released-export error is
    a runtime fault (single-owner: no sibling can legitimately touch this block).
    Returns True on success, False after H.fail."""
    # shm.buf returns SharedMemory's own internal memoryview over the mmap (the
    # same object each call).  We do NOT release it -- releasing shm.buf would
    # break the block for the next round; SharedMemory.close() releases it once at
    # cleanup.  We just hold the reference live across our parks below.
    mv = shm.buf                              # live memoryview export over the mmap

    # ---- write the pattern, parking with the export latched half-written -------
    try:
        for i in range(BLOCK_SIZE):
            mv[i] = expected_byte(wid, nonce, i)
            if i == PARK_AT:
                # Park here: our export is alive and half-written.  A correct
                # runtime keeps the mmap/fd/export exactly as we left it; a
                # sibling's block must NOT alias over ours.
                runloom.yield_now()
    except ValueError as exc:
        H.fail("SharedMemory .buf export RELEASED mid-write (wid {0} nonce "
               "{1}): {2!r} -- a close()/refcount drop crossed hubs and freed "
               "this fiber's memoryview export while it was parked".format(
                   wid, nonce, exc))
        return False

    # ---- park again, fully written, then read the whole block back -------------
    if nonce & 1:
        runloom.sleep(0.0002)
    else:
        runloom.yield_now()

    try:
        for i in range(BLOCK_SIZE):
            got = mv[i]
            want = expected_byte(wid, nonce, i)
            if got != want:
                H.fail("SharedMemory .buf CONSERVATION broken at offset {0} "
                       "(wid {1} nonce {2}): read back {3} but this fiber wrote "
                       "{4} -- a torn mmap store, a cross-fiber alias (a "
                       "sibling's block mapped over this fiber's single-owner "
                       "block), or a stale value from a previous round".format(
                           i, wid, nonce, got, want))
                return False
    except ValueError as exc:
        H.fail("SharedMemory .buf export RELEASED mid-read (wid {0} nonce "
               "{1}): {2!r} -- the fiber-owned export was freed across the "
               "park before read-back".format(wid, nonce, exc))
        return False
    return True


def make_cleanup(shm):
    """Return a cleanup closure that close()+unlink()s a fiber's block once, at
    the very end.  Guarded so a double-invocation or a partially-torn-down block
    never raises out of the cleanup phase."""
    def cleanup():
        try:
            shm.close()
        except Exception:
            pass
        try:
            shm.unlink()
        except Exception:
            pass
    return cleanup


def worker(H, wid, rng, state):
    """Each fiber owns exactly ONE SharedMemory block for its whole lifetime
    (bounded fd/shm use: funcs blocks, not rounds x funcs).  It runs the single-
    owner .buf pattern-conservation round repeatedly, changing the nonce each
    round so a stale-region bug is caught, and returns on the first failure."""
    try:
        shm = shared_memory.SharedMemory(create=True, size=BLOCK_SIZE)
    except OSError as exc:
        # A /dev/shm or fd ceiling at over-scale is a benign platform SCALE LIMIT,
        # NOT a runtime bug -- record it and stop this fiber cleanly.
        H.note_scale_limit("SharedMemory create failed (wid {0}): {1!r}".format(
            wid, exc))
        return
    H.add_cleanup(make_cleanup(shm))          # close()+unlink() once at the end

    nonce = 0
    for _ in H.round_range():
        if not H.running():
            break
        if not buf_pattern_round(H, wid, rng, state, shm, nonce):
            return                            # H.fail already recorded
        state["byte_checks"][wid] += 1        # single-writer-per-wid, race-free
        H.op(wid)
        H.task_done(wid)
        nonce += 1


def setup(H):
    # H.funcs is known here -> one race-free slot per worker (single writer per
    # slot; summed in post for non-vacuity).  No shared mutable oracle state.
    H.state = {
        "byte_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["byte_checks"])
    H.log("single-owner SharedMemory .buf pattern-conservation rounds: {0} (each "
          "wrote a unique per-(wid,round) pattern through its own .buf across a "
          "park and read it back exactly -- all passed fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing single-owner arm actually ran.
    H.check(checks > 0,
            "no single-owner .buf conservation rounds ran -- the mmap-export-"
            "across-park hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-write on the
    # mmap store or parked on a released export).
    H.require_no_lost("shared_memory .buf pattern conservation")


if __name__ == "__main__":
    harness.main(
        "p556_shared_memory_buf_pattern", body, setup=setup, post=post,
        default_funcs=800,
        max_funcs=800,
        describe="each fiber creates its OWN multiprocessing.shared_memory."
                 "SharedMemory block, writes a unique per-(wid,round) byte pattern "
                 "through the live .buf memoryview export while PARKING partway, "
                 "then reads it all back -- single-owner value conservation across "
                 "a fiber park + hub migration.  A byte that reads back wrong (torn "
                 "mmap store / cross-fiber alias / stale round) or a released-"
                 "export error is a runtime bug.  RESOURCE-HEAVY: one /dev/shm "
                 "file + fd + mmap per fiber, max_funcs=800, close()+unlink() at "
                 "cleanup")
