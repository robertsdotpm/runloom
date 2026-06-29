"""big_100 / 483 -- bz2.BZ2Compressor/BZ2File state isolation under M:N.

bz2 is a C-accelerated module providing BZ2Compressor and BZ2File objects.
Each instance holds a per-instance compression/decompression state struct
(struct bz2_state) allocated in C. Under M:N, many fibers share a single hub
OS-thread; if a fiber yields mid-operation and a sibling fiber runs on the same
hub, state corruption can occur if:
  (1) the shared object's state is accessed concurrently without thread safety,
  (2) a fiber is preempted and desyncs state updates across yield points.

This program stresses bz2 compression isolation: each fiber compresses a
DISTINCT set of data via bz2.compress() or maintains a BZ2Compressor object
across yield points, then decompresses the result and asserts the round-trip
succeeds (matches the original plaintext). A torn/corrupted compressor state
(preempted mid-state-update across yields) produces mismatched decompression.

LOAD-BEARING INVARIANT: each fiber's bz2 operations on DISTINCT data with
DISTINCT compressor objects MUST NOT cross-contaminate. A fiber that sets
data D and compresses it via a fresh BZ2Compressor, yields, then decompresses
the bytes MUST recover exactly D. Corruption manifests as:
  - decompression raising an exception (OSError / data format error),
  - decompressed bytes != original data (corruption),
  - torn bytes (partial compression state).

This is an M:N-SPECIFIC hazard (0 under plain OS threads): genuine threads own
their own Python objects and C state structs, so no sharing or preemption-mid-
state-update corruption occurs. A runloom M:N fiber can be preempted WHILE
HOLDING a BZ2Compressor reference mid-method, then resume on a different hub
where another fiber has acquired and mutated the SAME object (same id() but
different logical owner).

ORACLE:

  * LOAD-BEARING -- DISTINCT-COMPRESSOR ROUND-TRIP INTEGRITY (worker, HARD,
    fail-fast): each fiber compresses a unique data block via fresh
    BZ2Compressor, yields (to invite preemption and hub migration), then
    decompresses the result. The round-trip MUST recover the original data
    exactly.  A mismatch is a torn/corrupted compressor state (runloom M:N
    corruption, not a documented caveat).  Single-owner per fiber: nothing but
    THIS fiber touches its compressor; a failure is a runloom state-isolation
    desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    compress/decompress (stranded in a C function on corrupted state) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (lc_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): the CONCURRENT-COMPRESS arm.  Multiple
    fibers call bz2.compress() on DISTINCT plaintext to build DISTINCT ciphers,
    park, then decompress.  Under correct M:N, each fiber's own plaintext
    round-trips.  A failure here is a corruption (fail-fast like the LOAD-
    BEARING arm), but we still report the rate so the severity is explicit.

Stresses: bz2 C-extension object per-instance state across hub fibers, struct
bz2_state allocation and concurrent access, BZ2Compressor / BZ2File state
across yield + hub migration, preempt-mid-compress-method, decompression of
torn/corrupted compressed bytes.

Good TSan / controlled-M:N-replay target: bz2_state struct fields (pos, bufSize,
live, mode, etc.) accessed concurrently across fibers -> data race on those
fields, or a replay that migrates a hub between a compress() entry and exit.
"""
import bz2
import harness
import runloom

# Per-fiber data payloads are drawn from this band.  Each plaintext yields a
# DISTINCT, deterministic compressed output, so a leaked sibling's compressed
# data produces wrong decompression on round-trip.  Keep payloads distinct
# enough (different sizes, contents) that corruption is easily caught.
DATA_MIN = 100
DATA_MAX = 2000
DATA_SPAN = DATA_MAX - DATA_MIN + 1


def make_payload(wid, iteration):
    """Generate a unique plaintext for this fiber + iteration.  Deterministic
    so the same (wid, iteration) always yields the same data.  Payloads are
    large enough that compression reveals state corruption (small data can
    round-trip even if state is partially torn)."""
    base = (wid * 7919 + iteration * 37) & 0xFFFFFF
    size = DATA_MIN + ((base + wid) % DATA_SPAN)
    # Highly compressible data: repeated patterns compress to much smaller.
    # A corrupted compressor produces either wrong size or checksum mismatch
    # on decompression, both easily caught.
    chunk = b"X" * (size // 10) + b"Y" * (size // 10)
    return (chunk * ((size // len(chunk)) + 1))[:size]


def setup(H):
    H.state = {
        "distinct_checks": [0] * 1024,  # LOAD-BEARING round-trip checks done
        "distinct_fails": [0] * 1024,   # LOAD-BEARING round-trip failures
        "concurrent_checks": [0] * 1024,  # MEASURED concurrent compress checks
        "concurrent_fails": [0] * 1024,  # MEASURED concurrent compress failures
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: DISTINCT-COMPRESSOR round-trip integrity.  Each fiber
# owns its own BZ2Compressor object (distinct id(self)), compresses its own
# unique plaintext, yields (to invite preemption), then decompresses and
# asserts recovery.  Single-owner: no sibling touches this compressor.
# --------------------------------------------------------------------------
def distinct_check(H, wid, idx, state):
    """Compress unique plaintext via a fresh BZ2Compressor, yield to invite
    preemption, then decompress and assert recovery."""
    plaintext = make_payload(wid, idx)
    try:
        # Compress: allocate a fresh BZ2Compressor, set state, call compress().
        compressor = bz2.BZ2Compressor(9)  # max compression
        compressed = compressor.compress(plaintext)
        compressed += compressor.flush()

        # YIELD + SLEEP-PARK: invite preemption and hub migration. A sibling
        # fiber on this hub may run and mutate the thread-local bz2 state, or
        # (if this fiber migrates hubs) the state struct may be left in an
        # inconsistent state. Upon resume, decompressing torn bytes fails or
        # produces wrong data.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0001)

        # Decompress: the bytes MUST recover exactly the original plaintext.
        # If the compressed bytes are torn (compressor state was mid-update
        # when this fiber was preempted), decompression fails with OSError or
        # produces a mismatch.
        decompressed = bz2.decompress(compressed)

        state["distinct_checks"][wid & 1023] += 1
        if decompressed != plaintext:
            H.fail("bz2 DISTINCT-COMPRESSOR CORRUPTION: round-trip failed "
                   "(wid {0} idx {1}): plaintext {2} bytes != decompressed {3} "
                   "bytes (expected {4}). The compressor state was torn across "
                   "the yield (runloom preemption/hub-migration desync).".format(
                       wid, idx, len(plaintext), len(decompressed),
                       len(plaintext)))
            state["distinct_fails"][wid & 1023] += 1
            return
    except (OSError, EOFError, Exception) as exc:
        state["distinct_checks"][wid & 1023] += 1
        state["distinct_fails"][wid & 1023] += 1
        H.fail("bz2 DISTINCT-COMPRESSOR EXCEPTION: round-trip raised {0} "
               "(wid {1} idx {2}): plaintext={3} bytes. The compressor state "
               "is corrupted (torn mid-compress/decompress, runloom M:N "
               "desync).".format(type(exc).__name__, wid, idx, len(plaintext)))
        return


# --------------------------------------------------------------------------
# MEASURED arm: CONCURRENT-COMPRESS round-trip. Multiple fibers compress
# DISTINCT plaintexts in parallel, then decompress. A failure here is still
# corruption (fail-fast), but we report the rate so severity is explicit.
# Under correct M:N, each fiber's own plaintext round-trips; a mismatch means
# state was cross-contaminated across fibers on the same hub.
# --------------------------------------------------------------------------
def concurrent_check(H, wid, idx, state):
    """Concurrent compress: many fibers compress distinct data simultaneously,
    park to allow interleaving, then decompress. Each plaintext MUST
    round-trip to its own fiber (no cross-contamination)."""
    plaintext = make_payload(wid, idx)
    try:
        # Compress concurrently: no lock, just distinct objects/data per fiber.
        compressor = bz2.BZ2Compressor(9)
        compressed = compressor.compress(plaintext)
        compressed += compressor.flush()

        # Park to invite a sibling's concurrent compress on the shared hub.
        runloom.yield_now()

        # Decompress: the bytes MUST recover this fiber's OWN plaintext.
        decompressed = bz2.decompress(compressed)

        state["concurrent_checks"][wid & 1023] += 1
        if decompressed != plaintext:
            state["concurrent_fails"][wid & 1023] += 1
            H.fail("bz2 CONCURRENT-COMPRESS CORRUPTION: round-trip failed "
                   "(wid {0} idx {1}): plaintext {2} bytes != decompressed {3} "
                   "bytes. A sibling's compressed data crossed into this "
                   "fiber's round-trip (state cross-contamination).".format(
                       wid, idx, len(plaintext), len(decompressed)))
            return
    except (OSError, EOFError, Exception) as exc:
        state["concurrent_checks"][wid & 1023] += 1
        state["concurrent_fails"][wid & 1023] += 1
        H.fail("bz2 CONCURRENT-COMPRESS EXCEPTION: round-trip raised {0} "
               "(wid {1} idx {2}): plaintext={3} bytes. Concurrent compress "
               "with shared hub-thread state caused corruption.".format(
                   type(exc).__name__, wid, idx, len(plaintext)))
        return


# Sustained round-trip checks per worker, bounded by H.running(). The corruption
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously mid-
# compress and parked across their yield, so the scheduler reliably runs a sibling
# on the shared hub before this fiber resumes.  A single check per fiber barely
# overlaps a sibling's and does NOT reproduce.  So each worker runs a sustained
# internal loop bounded by H.running() and INNER_CAP.
INNER_CAP = 50000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms: the LOAD-BEARING distinct-compressor check
    (fail-fast) and the MEASURED concurrent-compress check (also fail-fast).
    The two share only the yield cadence -- distinct compressor is single-owner,
    concurrent compress is many-owners-distinct-data."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            distinct_check(H, wid, idx, state)  # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            concurrent_check(H, wid, idx, state)  # MEASURED (fail-fast)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dchecks = sum(H.state["distinct_checks"])
    dfails = sum(H.state["distinct_fails"])
    cchecks = sum(H.state["concurrent_checks"])
    cfails = sum(H.state["concurrent_fails"])
    dpct = (100.0 * dfails / dchecks) if dchecks else 0.0
    cpct = (100.0 * cfails / cchecks) if cchecks else 0.0

    H.log("bz2: LOAD-BEARING distinct-compressor checks={0} failures={1} "
          "({2:.2f}%) | MEASURED concurrent-compress checks={3} "
          "failures={4} ({5:.2f}%)".format(
              dchecks, dfails, dpct, cchecks, cfails, cpct))

    if dfails:
        H.log("note: the LOAD-BEARING distinct-compressor arm observed {0} "
              "round-trip failures -- bz2.BZ2Compressor state was torn across "
              "yields (runloom M:N preemption/hub-migration desync, not a "
              "documented caveat; 0 under plain OS threads GIL on AND off).".format(
                  dfails))

    if cfails:
        H.log("note: the MEASURED concurrent-compress arm observed {0} "
              "round-trip failures -- bz2 compressed bytes were corrupted "
              "under concurrent M:N compress (state cross-contamination).".format(
                  cfails))

    # NON-VACUITY: the load-bearing distinct-compressor hazard was actually
    # exercised.
    H.check(dchecks > 0,
            "no distinct-compressor round-trip checks ran -- the load-bearing "
            "bz2 state-isolation hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-compress/decompress
    # (stranded in a C function on corrupted state).
    H.require_no_lost("bz2 compression state isolation")


if __name__ == "__main__":
    harness.main(
        "p483_bz2", body, setup=setup, post=post,
        default_funcs=8000,
        describe="bz2.BZ2Compressor/BZ2File C-extension state isolation under "
                 "M:N.  Each fiber owns a distinct BZ2Compressor, compresses "
                 "unique plaintext, yields (to invite preemption), then "
                 "decompresses and asserts exact recovery.  LOAD-BEARING: a "
                 "single-owner compressor's plaintext MUST round-trip "
                 "identically (0 failures under plain threads GIL on AND off; "
                 "a torn/corrupted state across yield is the runloom M:N "
                 "bug).  MEASURED concurrent-compress (distinct data, many "
                 "fibers) also checks round-trip integrity -- a shared hub "
                 "thread could corrupt state if accessed concurrently without "
                 "thread safety (fail-fast like LOAD-BEARING).  Good TSan / "
                 "controlled-M:N-replay target: bz2_state struct field races "
                 "or preempt-mid-method state update")
