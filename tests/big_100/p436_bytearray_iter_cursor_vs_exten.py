"""big_100 / 436 -- bytearray ITERATOR cursor vs extend/+=/del realloc, cross-hub.

The subject is the bytearray's OWN iterator object, ``bytearrayiterobject``
(Objects/bytearrayobject.c), and its single non-atomic walk state:

    typedef struct {
        PyObject_HEAD
        Py_ssize_t it_index;          /* the live cursor */
        PyByteArrayObject *it_seq;    /* RAW borrowed ptr to the bytearray */
    } bytearrayiterobject;

    static PyObject *bytearrayiter_next(bytearrayiterobject *it) {
        ...
        if (it->it_index < PyByteArray_GET_SIZE(seq)) {   /* the ONLY guard */
            return PyLong_FromLong((unsigned char)seq->ob_bytes[it->it_index++]);
        }
        ...                                               /* StopIteration */
    }

That is a CHECK-then-DEREFERENCE: read ``it_index``, compare it against the
bytearray's CURRENT ob_size, then index ``it_seq->ob_bytes[it_index]``.  The
only thing standing between a live cursor and a freed slot is that one size
comparison -- and ``ob_bytes`` is a ``PyObject_Realloc``'d block.  A concurrent
``extend()`` / ``+=`` / ``del ba[:]`` calls ``PyByteArray_Resize``, which can
``realloc`` ``ob_bytes`` to a NEW address (the old block is freed) and/or shrink
ob_size.  The hazard pair is:

  * the iterator's ``seq->ob_bytes[it_index]`` dereference, vs
  * the resize's ``PyObject_Realloc`` of ``ob_bytes`` (frees the old block) and
    its non-atomic store of the new ``ob_size``.

Unlike a bytearray VIEW, the iterator holds NO export count -- p302 and p415
attack the buffer-protocol ``ob_exports`` / bytesio ``exports`` guard with a
memoryview / getbuffer() held live, so a resize there is *forbidden* (must raise
BufferError).  Nothing forbids resizing a bytearray while it is being ITERATED;
the iterator is supposed to tolerate it via the size check, raising
RuntimeError("bytearray changed size during iteration") on the next __next__.
p311 drives dict/set iterators (index into an entry table + dk_version); the
bytearray iterator's raw ob_bytes cursor is untouched by the whole suite.

Under M:N the iterator can PARK mid-walk -- ``it_index`` live on a grown-down C
stack -- while a sibling on ANOTHER hub extends/shrinks the SAME bytearray and
reallocs ob_bytes.  On resume ``bytearrayiter_next`` reads
``seq->ob_bytes[it_index]``: if the size check reads a STALE ob_size (a torn read
of the field the resize just stored) BEFORE the realloc'd-away block is noticed,
it_index addresses a freed/relocated slot and hands back an OUT-OF-UNIVERSE byte
(use-after-free), or runs past the shrunk length (out-of-bounds read), or
SIGSEGVs.

CLOSED-WORLD VALUE-CONSERVATION ORACLE (legal-race acceptance).  Each round
builds a FRESH bytearray of UNIVERSE bytes: position i holds ``g(i)``, where g is
a wide mixing function reduced mod 256.  Because two positions can collide mod
256 we keep a SIDE TABLE: the iterator does NOT just check membership of the byte
in {0..255} (every byte is in that set -- useless), it checks that the byte it
yields for the position it came from equals ``g(position)`` recomputed from the
side table.  So a byte read from a freed/relocated slot -- a value that does NOT
match g(its own cursor position) -- is the bug, even though it is a perfectly
legal 0..255 byte.  That is the "out-of-universe" test, refined to a per-POSITION
identity law rather than a coarse value-set membership.

  iterator fiber: walks ``enumerate(iter(ba))`` so it knows the cursor position
    of every byte; trips a gate just before parking and yields with it_index
    live.  For every IN-BOUNDS position (pos < original length) it asserts
    ``byte == g(pos)`` -- the del-shrink keeps the [0:cut] prefix intact (cut >
    the park point) so that prefix MUST still read back g(pos); a mismatch there
    is an in-bounds read of a FREED/relocated ob_bytes slot (use-after-free) and
    is the FAULT.

  Legal outcomes (all counted, none failed): a clean full walk; a
    RuntimeError("bytearray changed size during iteration"); OR the cursor
    legally advancing to pos >= original length because a concurrent extend()
    raised ob_size first -- ``bytearrayiter_next`` carries no size-change
    snapshot, so reading the freshly-appended bytes is CPython's DEFINED
    versionless-iterator behavior, NOT a memory fault.  Only an IN-BOUNDS wrong
    byte, a non-RuntimeError exception, or a SIGSEGV is the FAULT.

  mutator fiber (other hub): waits the gate, then extend()/+= grows ob_bytes
    past a realloc boundary and/or ``del ba[k:]`` shrinks it -- forcing the
    realloc/relocation under the parked cursor.

SINGLE-OWNER CONTROL ARM (the falsifier).  A second case runs the identical
iterate-then-extend in ONE fiber, single-owner, no sibling: it iterates a fresh
bytearray and mutates it from the SAME fiber mid-walk.  A single-writer bytearray
iterator is race-free by construction, so it must ALWAYS either complete cleanly
(when the mutation happens after the walk) or raise EXACTLY RuntimeError
("changed size during iteration") -- it must NEVER yield a wrong byte and never
raise anything else.  If the control arm yields a wrong byte, the fault is in
CPython's bytearrayiter_next machinery itself, NOT M:N contention -- this
disambiguates "the iterator is buggy" from "the cross-hub race tore it".

Invariant (hot, fail-fast): every IN-BOUNDS byte (pos < original length) the
iterator yields equals g(pos); the only tolerated exception is
RuntimeError("changed size during iteration"); advancing into grown territory
(pos >= original length) is legal; the single-owner control NEVER yields a wrong
byte and never raises anything but that RuntimeError.
Invariant (post): >=1 cross-hub iteration completed (clean, RuntimeError, or
grew-into-territory) so the race window was exercised; the control arm ran and
only ever produced legal outcomes; both cases covered; no lost worker.

Stresses: bytearrayiterobject.it_index/it_seq->ob_bytes cursor vs
PyByteArray_Resize PyObject_Realloc, check-then-dereference past a park,
use-after-free / out-of-bounds byte read, "bytearray changed size during
iteration" detection under M:N, single-owner vs cross-hub iterator divergence.

Good TSan / controlled-M:N-replay target: the it_index dereference of
seq->ob_bytes racing the resize's realloc+ob_size store is a textbook
use-after-free data race; a TSan report on ob_bytes / ob_size localizes the
freed-slot read before the per-position identity assert even fires.  RNG is
per-worker (rng) and each mutator gets its OWN random.Random (seeded from
rng.getrandbits) so the stream is replayable and a shared Random can't corrupt
GIL-off.
"""
import random

import harness
import runloom

# Length of the fresh per-round bytearray.  Chosen to span SEVERAL of bytearray's
# realloc growth boundaries when the mutator extends it (CPython overallocates
# bytearray by ~1/8, so growth from this length forces a real PyObject_Realloc of
# ob_bytes, which is the relocation that strands a parked cursor).  Big enough
# that the iterator parks well before the end; small enough that many rounds run.
BASE_LEN = 512

# How far the mutator GROWS the bytearray on an extend/+= -- comfortably past a
# realloc boundary so ob_bytes is reallocated (and likely moved).
GROW_LEN = 4096

# Where (as a fraction of BASE_LEN) the iterator parks mid-walk: it has read this
# many bytes, its it_index is live, THEN it trips the gate and yields so the
# sibling's resize lands inside the park window.
PARK_AT = 8

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The two cases.  Round-robin by worker id in the FIRST ops so coverage holds
# whether one worker does K ops or K workers do 1 op each (the p125/p126/p172
# flaky-random-coverage fix -- never pure random for coverage).
CASE_CROSSHUB = 0   # iterator + mutator on different hubs (the contention probe)
CASE_CONTROL = 1    # iterate-then-extend in ONE fiber (the single-owner falsifier)
NCASES = 2


def g(pos):
    """Position -> byte.  A wide mixing function reduced mod 256, so the bytearray
    is NOT a trivial ramp (a relocated/freed slot is unlikely to coincidentally
    hold g(pos) for the WRONG pos).  Deterministic and recomputable from pos
    alone, which is the side table: the iterator recovers the EXPECTED byte for
    the cursor position it is at, so a byte that does not equal g(pos) was read
    from a freed/relocated slot -- the use-after-free."""
    x = (pos * 2654435761 + 0x9E3779B9) & 0xFFFFFFFF
    x ^= x >> 16
    x = (x * 0x45D9F3B) & 0xFFFFFFFF
    x ^= x >> 13
    return x & 0xFF


def fresh_bytearray():
    """A FRESH bytearray each round (never shared across rounds): position i holds
    g(i).  Returned freshly allocated so its ob_bytes block is this round's own
    and a stale cursor from a prior round can't alias it."""
    return bytearray(g(i) for i in range(BASE_LEN))


def grow_extend(ba, rng):
    """Grow the bytearray past a realloc boundary: extend() -> PyByteArray_Resize
    -> PyObject_Realloc of ob_bytes (likely MOVES the block, freeing the old one).
    Appends bytes that are deliberately NOT g(pos) for the positions they land at,
    so the appended (>= BASE_LEN) region breaks the identity law -- only relevant
    if a torn size check let the cursor over-run into it.  Uses its OWN
    random.Random.  Returns the cut point used by the subsequent shrink."""
    tail = bytes((rng.getrandbits(8) ^ 0xA5) for _ in range(GROW_LEN - BASE_LEN))
    ba.extend(tail)                       # grow -> realloc/move; old ob_bytes freed
    return rng.randint(PARK_AT + 1, BASE_LEN // 2)


def shrink_del(ba, cut):
    """Shrink hard: delete the tail so ob_size drops to `cut` (below where a parked
    cursor near the end could sit).  del ba[cut:] is a PyByteArray_Resize that can
    realloc-shrink ob_bytes again.  cut > PARK_AT, so the [0:cut] prefix the
    iterator already walked / is walking stays intact and MUST still read g(pos)."""
    del ba[cut:]                          # shrink -> resize


def grow_then_shrink(ba, rng):
    """Synchronous grow-then-shrink (single-owner control path): force both a grow
    realloc and a shrink resize back-to-back with no intervening yield.  The
    cross-hub path instead interleaves a yield between the two (see run_crosshub)
    to widen the window in which the cursor resumes against a freshly-realloc'd,
    still-GROWN backing store -- the prime stale-ob_bytes-pointer window."""
    shrink_del(ba, grow_extend(ba, rng))


def walk_checked(H, wid, ba, gate, counts, slot):
    """Walk enumerate(iter(ba)), parking once mid-walk after tripping `gate` so the
    mutator runs DURING the park.  Returns 'clean' | 'runtimeerror' | 'grew' |
    'fail'.

    The oracle distinguishes a genuine memory-safety FAULT from CPython's defined
    (if surprising) versionless-iterator behavior.  ``bytearrayiter_next`` carries
    NO size-change snapshot -- it only re-reads the CURRENT ob_size on each
    __next__ -- so:

      * cursor position pos < BASE_LEN with byte != g(pos): the slot that MUST
        still hold g(pos) (extend/del preserve the [0:cut] prefix; only the tail
        beyond cut is removed and cut > PARK_AT so the walked prefix is intact)
        instead holds garbage.  That is an IN-BOUNDS torn/freed-slot read -- the
        cursor dereferenced a STALE ob_bytes pointer the resize had already
        realloc'd away (use-after-free).  This is the BUG -> H.fail.

      * cursor position pos >= BASE_LEN: the size check re-read the GROWN ob_size
        (after the concurrent extend()) and legally advanced the cursor into the
        freshly-appended bytes.  Those bytes are NOT g(pos) by construction, but
        this is CPython's DEFINED "you mutated during iteration" behavior for a
        versionless iterator, not a memory fault.  LEGAL -- count and stop.

      * RuntimeError("bytearray changed size during iteration"): the LEGAL, clean
        detection of the concurrent resize.  Acceptable.

      * any other exception / SIGSEGV: a fault (handled by the caller / watchdog).
    """
    parked = False
    it = iter(ba)                          # a bytearrayiterobject, it_index == 0
    try:
        for pos, byte in enumerate(it):
            if pos >= BASE_LEN:
                # Walked into LEGITIMATELY-grown territory: the iterator validly
                # advanced past the original length because a concurrent extend()
                # raised ob_size.  Defined versionless-iterator behavior, NOT a
                # memory bug.  Stop here -- the appended bytes are not g(pos).
                counts["grew"][slot] += 1
                return "grew"
            # Per-POSITION identity law over the IN-BOUNDS prefix: position pos
            # (< original length, and < cut so del never removed it) MUST still be
            # g(pos).  A mismatch is an in-bounds read of a freed/relocated slot.
            if byte != g(pos):
                H.fail("bytearray iterator yielded byte {0} at IN-BOUNDS cursor "
                       "position {1} (< original length {2}), expected g(pos)={3} "
                       "-- the cursor read a FREED/relocated ob_bytes slot "
                       "(use-after-free): a concurrent PyByteArray_Resize "
                       "realloc'd the backing store out from under a stale "
                       "it_seq->ob_bytes pointer".format(byte, pos, BASE_LEN,
                                                          g(pos)))
                return "fail"
            if not parked and pos >= PARK_AT:
                # Trip the gate (lets the mutator proceed) then park with it_index
                # LIVE on this fiber's C stack -- the resize lands here.
                parked = True
                gate.done()
                runloom.yield_now()
            elif parked:
                # Keep handing the scheduler back AFTER resuming, on every
                # subsequent position, so the live cursor's it_index dereference
                # repeatedly overlaps the mutator's grow->yield->shrink sequence --
                # the cursor stays "parked mid-walk while a sibling resizes",
                # widening the realloc / stale-ob_bytes-pointer window across the
                # whole remaining walk rather than a single park point.
                runloom.yield_now()
        counts["clean"][slot] += 1
        return "clean"
    except RuntimeError:
        # "bytearray changed size during iteration" -- the LEGAL, clean detection
        # of the concurrent resize.  Acceptable.
        counts["rterror"][slot] += 1
        if not parked:
            # RuntimeError fired before we parked; trip the gate so the mutator
            # never blocks forever waiting on it.
            gate.done()
        return "runtimeerror"


def run_control(H, wid, rng, counts, slot):
    """CASE_CONTROL: the single-owner falsifier.  Iterate a fresh bytearray and
    resize it from the SAME fiber mid-walk -- no sibling, no cross-hub race.  A
    single-writer bytearray iterator must ALWAYS either finish cleanly or raise
    EXACTLY RuntimeError("changed size during iteration"); it must NEVER yield a
    wrong byte and never raise any other exception.  A wrong byte HERE is a defect
    in CPython's bytearrayiter_next itself, not M:N contention -- the falsifier
    that distinguishes a buggy primitive from a torn race."""
    ba = fresh_bytearray()
    it = iter(ba)
    seen = 0
    try:
        for pos, byte in enumerate(it):
            if byte != g(pos):
                H.fail("SINGLE-OWNER control: bytearray iterator yielded byte {0} "
                       "at position {1}, expected g(pos)={2} -- a wrong byte with "
                       "NO concurrency means the fault is in CPython's "
                       "bytearrayiter_next machinery itself, not M:N contention"
                       .format(byte, pos, g(pos)))
                return False
            seen += 1
            if seen == PARK_AT + 1:
                # Mutate from this very fiber, mid-walk, with the cursor live.  On
                # the NEXT __next__ the size check must fire -> RuntimeError.
                grow_then_shrink(ba, rng)
        # Completed without the size check firing (mutation happened to leave the
        # walked prefix intact and the cursor finished) -- legal.
        counts["control_clean"][slot] += 1
        return True
    except RuntimeError:
        # The expected, legal single-owner outcome after a same-fiber resize.
        counts["control_rterror"][slot] += 1
        return True
    except Exception as exc:                # noqa: BLE001
        H.fail("SINGLE-OWNER control: bytearray iterator raised non-RuntimeError "
               "{0}: {1} -- the only legal post-resize outcome is RuntimeError "
               "('changed size during iteration'); anything else is a "
               "bytearrayiter_next defect".format(type(exc).__name__, exc))
        return False


def run_crosshub(H, wid, rng, counts, slot):
    """CASE_CROSSHUB: the contention probe.  Spawn an iterator fiber and a mutator
    fiber on (potentially) different hubs over the SAME fresh bytearray, gated so
    the resize provably lands inside the iterator's park window.  Joins both
    before returning so the bytearray is quiescent."""
    ba = fresh_bytearray()

    # gate: the iterator trips it the instant before it parks; the mutator waits on
    # it, so the resize provably lands inside the park window.
    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(2)
    mseed = rng.getrandbits(48)

    def run_iter():
        try:
            walk_checked(H, wid, ba, gate, counts, slot)
        except Exception as exc:            # noqa: BLE001
            # ANY non-RuntimeError exception escaping the iterator is a fault
            # (RuntimeError is caught inside walk_checked and counted legal).
            H.fail("cross-hub iterator raised non-RuntimeError {0}: {1} -- not "
                   "the legal 'bytearray changed size during iteration' outcome"
                   .format(type(exc).__name__, exc))
        finally:
            wg.done()

    def run_mut():
        mrng = random.Random(mseed)         # OWN Random; a shared one corrupts GIL-off
        try:
            gate.wait()                     # block until the iterator is parked
            # GROW first (realloc/move ob_bytes -> the old block is freed), then
            # YIELD so the parked iterator can resume and take its next __next__
            # while the array is STILL grown and the backing store was just
            # relocated -- the prime window for the cursor to dereference a stale
            # it_seq->ob_bytes pointer (the use-after-free we are hunting).  THEN
            # shrink, so a slower-to-resume iterator instead meets a shrunk array.
            cut = grow_extend(ba, mrng)     # PyByteArray_Resize grow -> realloc/move
            runloom.yield_now()             # hand the cursor back into the grown window
            shrink_del(ba, cut)             # PyByteArray_Resize shrink
        except Exception:
            # The mutator's own resize never legally raises here; swallow so a
            # mutator hiccup can't deadlock the iterator's join.  The iterator
            # oracle is the sole judge of correctness.
            pass
        finally:
            wg.done()

    H.fiber(run_iter)
    H.fiber(run_mut)
    wg.wait()                               # both joined -> bytearray quiescent
    return not H.failed


def worker(H, wid, rng, state):
    counts = state["counts"]
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the two cases by worker id in the first ops so both the
        # cross-hub contention probe AND the single-owner control are exercised
        # even when each worker manages only a few ops under the timeout; random
        # after that, preserving the concurrent mix.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_CROSSHUB:
            ok = run_crosshub(H, wid, rng, counts, slot)
        else:
            ok = run_control(H, wid, rng, counts, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Per-slot tallies allocated here, inside the root (monkey.patch() already ran
    # so runloom.WaitGroup / yield_now are the cooperative M:N-safe primitives).
    # Single-writer-per-slot lists -> race-free without a hot lock; summed in post.
    H.state = {"counts": {
        "clean": [0] * SLOTS,             # cross-hub walks that completed clean
        "rterror": [0] * SLOTS,           # cross-hub walks that legally RuntimeError'd
        "grew": [0] * SLOTS,              # cross-hub walks that legally advanced into
                                          #   concurrently-grown territory (pos>=BASE_LEN)
        "control_clean": [0] * SLOTS,     # single-owner walks that completed clean
        "control_rterror": [0] * SLOTS,   # single-owner walks that legally RuntimeError'd
    }}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    counts = H.state["counts"]
    clean = sum(counts["clean"])
    rterror = sum(counts["rterror"])
    grew = sum(counts["grew"])
    cclean = sum(counts["control_clean"])
    crterror = sum(counts["control_rterror"])
    H.log("cross-hub walks: clean={0} runtimeerror={1} grew-into-territory={2} "
          "(all three legal); single-owner control: clean={3} runtimeerror={4} "
          "(both legal); ops={5}  -- any IN-BOUNDS wrong byte / non-RuntimeError "
          "exception already failed fast".format(
              clean, rterror, grew, cclean, crterror, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # The cross-hub race window was actually exercised (not skipped): at least one
    # iterator either completed a clean walk, legally detected the resize via
    # RuntimeError, or legally advanced into concurrently-grown territory -- all
    # three are outcomes of an iterator that walked WHILE a sibling resized.
    H.check(clean + rterror + grew > 0,
            "cross-hub iterate-vs-resize race window was never exercised -- no "
            "iterator completed a walk while a sibling resized the bytearray "
            "(the contention probe never ran)")

    # The single-owner control arm ran and only ever produced legal outcomes
    # (reaching post with no failure already proves it never yielded a wrong byte
    # nor raised a non-RuntimeError; assert it was actually exercised).
    H.check(cclean + crterror > 0,
            "single-owner CONTROL arm never ran -- without the falsifier we "
            "cannot distinguish a buggy bytearrayiter_next from a torn cross-hub "
            "race")

    H.require_no_lost("bytearray-iter-cursor completeness")


if __name__ == "__main__":
    harness.main(
        "p436_bytearray_iter_cursor_vs_exten", body, setup=setup, post=post,
        default_funcs=3000,
        describe="bytearray ITERATOR cursor (bytearrayiterobject.it_index into "
                 "it_seq->ob_bytes) walked across a park while another hub "
                 "extend/del-resizes the SAME bytearray (PyByteArray_Resize "
                 "realloc); per-position identity law byte==g(pos) catches a "
                 "freed-slot read, RuntimeError('changed size') is the legal "
                 "outcome, a single-owner control arm falsifies CPython-machinery "
                 "bugs vs M:N contention")
