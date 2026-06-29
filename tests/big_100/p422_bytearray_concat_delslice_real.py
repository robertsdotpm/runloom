"""big_100 / 422 -- shared bytearray realloc/memmove vs a live iterator (no view).

The subject is CPython's ``bytearrayobject`` and its ``bytearray_iterator``
(``bytearrayiterobject``), driven through the PLAIN growth/shrink RMW paths --
``bytearray_iconcat`` (``ba += chunk``), ``bytearray_extend`` (``ba.extend``),
``bytearray_ass_subscript`` del-slice (``del ba[i:j]``), and ``bytearray_pop``
(``ba.pop()``) -- with NO buffer view exported.  Those are exactly the paths the
suite does NOT yet attack: p302/p404 only fault bytearray/array through the
buffer-EXPORT guard (``ob_exports`` forbids resize while a memoryview is live),
so a plain shared bytearray whose ``ob_bytes`` is being realloc'd/moved while a
sibling iterator walks it is uncovered ground.

THE EXACT C-LEVEL STATE UNDER ATTACK.  A bytearray is { ``ob_bytes`` (the live
heap buffer, malloc'd), ``ob_start`` (offset of byte 0 into ``ob_bytes`` -- a
del at the FRONT advances ``ob_start`` instead of memmoving), ``ob_alloc``
(capacity), and ``Py_SIZE`` (logical length) }.  ``PyByteArray_Resize`` may
``PyObject_Realloc(ob_bytes, ...)`` (the buffer can MOVE in memory) and rewrites
``ob_alloc``/``Py_SIZE``; ``bytearray_ass_subscript`` del-slice does a raw
``memmove`` over ``ob_bytes`` then shrinks ``Py_SIZE``; a front-del bumps
``ob_start`` and leaves ``Py_SIZE`` smaller without freeing.  The iterator
``bytearrayiterobject`` holds only ``it_index`` (a plain Py_ssize_t) and a
borrowed ``*it_seq`` reference; ``bytearrayiter_next`` does, every step:
``if (it->it_index < Py_SIZE(seq)) return seq->ob_bytes[it->it_index++]`` -- a
FRESH bounds-check-then-index-read against the LIVE ``ob_bytes`` each call.

THE PRECISE M:N HAZARD (the racing op pair).  The iterator can PARK mid-walk on
its grown-down C stack with ``it_index`` live, while a sibling on ANOTHER hub
calls ``ba += chunk`` / ``del ba[i:j]`` / ``ba.pop()`` and that resize does a
``realloc`` that FREES-and-MOVES ``ob_bytes``.  On resume ``bytearrayiter_next``
reads ``seq->ob_bytes[it_index]`` -- but if its bounds check read a stale
``Py_SIZE`` while a concurrent del already shrank it, OR if ``ob_bytes`` was
realloc'd out from under the index, the read is a USE-AFTER-REALLOC: a byte from
freed/relocated memory (an out-of-universe value), or a torn ``ob_start`` /
``Py_SIZE`` length desync (the bounds check and the data read disagree), or a
SIGSEGV off the end of a shrunk buffer.

CLOSED-WORLD INVARIANT (finite sentinel UNIVERSE).  Every byte ever pushed into
the shared bytearray is drawn from a fixed 64-value UNIVERSE alphabet, so the
bytearray only ever legally holds UNIVERSE bytes.  Per round a worker owns ONE
shared bytearray (seeded with UNIVERSE bytes) and spawns: a GROWER fiber
(``ba += chunk`` / ``ba.extend(chunk)`` of UNIVERSE bytes), a SHRINKER fiber
(``del ba[i:j]`` mid-slice + front-del to move ``ob_start``, and ``ba.pop()``),
and an ITERATOR fiber that walks ``iter(ba)`` and PARKS mid-walk (via a gate the
mutators wait on, so their realloc provably lands during the park).  WRITES are
serialized under ONE cooperative Lock (conservation framing: the bytearray is
documented thread-unsafe, so we serialize writers and make the oracle a memory-
safety + length-consistency law); the ITERATOR holds NO lock -- it is the
use-after-realloc probe racing the writers' realloc/memmove.

  HOT, fail-fast (the iterator, unlocked):
    * every byte the iterator yields is in UNIVERSE_SET.  A byte outside the
      alphabet is a read of freed/relocated ``ob_bytes`` -- a hard fault.
    * the only tolerated exception is nothing: iterating a bytearray does NOT
      raise "changed size during iteration" (unlike dict/list, the bytearray
      iterator has no version guard -- it silently re-reads the live buffer), so
      ANY exception escaping the walk other than a benign IndexError-free finish
      is the bug.  We DO accept a SHORT walk (the buffer shrank under us) as long
      as every byte seen was in UNIVERSE.

  POST-quiescent reconciliation (after grower+shrinker+iterator all join):
    * len(ba) == len(bytes(ba)) == len(list(ba))  -- ``ob_start`` / ``Py_SIZE``
      / ``ob_bytes`` agree (a torn length desync makes these diverge).
    * every byte of bytes(ba) is in UNIVERSE_SET (no freed/relocated byte
      survived into the quiescent buffer).

SINGLE-OWNER CONTROL ARM.  Alongside the shared bytearray, the worker keeps a
PRIVATE bytearray and applies the byte-EXACT SAME grow/shrink sequence to it
single-owner (race-free by construction).  Because the shared writes are
serialized under the lock in the SAME recorded order, the shared bytearray's
final content must be byte-identical to the private control's.  A divergence
that never crashes -- a dropped/doubled/relocated chunk, a memmove that moved
the wrong span -- is localized to CPython's realloc/memmove machinery, not to
contention (the private arm has one writer and cannot lose a byte).

Stresses: bytearray_iconcat / bytearray_extend / bytearray_ass_subscript
del-slice memmove / bytearray_pop realloc of ob_bytes, ob_start/Py_SIZE rewrite,
bytearrayiter_next raw ob_bytes[it_index] read racing a concurrent resize,
use-after-realloc on a live iterator, torn ob_start/Py_SIZE length desync,
shared-vs-private byte-exact divergence.

Good TSan / controlled-M:N-replay target: bytearrayiter_next's
``seq->ob_bytes[it_index]`` read versus PyByteArray_Resize's realloc/memmove of
``ob_bytes`` is a textbook use-after-realloc data race; a TSan report on the
ob_bytes load/store, or a single out-of-universe byte under replay, localizes
the fault before the universe-membership assert even closes.
"""
import harness
import runloom


# Finite sentinel UNIVERSE: a fixed 64-value byte alphabet.  Every byte the
# shared bytearray ever legally holds is one of these; a byte the iterator (or
# the quiescent buffer) yields that is NOT in this set is a read of freed /
# relocated ob_bytes -- a hard memory-safety fault.  64 distinct printable-ish
# values, none zero (a zero often means an uninitialised/freed slot), spaced so a
# torn high/low nibble is unlikely to land back inside the set.
UNIVERSE = tuple(0x21 + (i * 3) % 0x5E for i in range(64))
UNIVERSE_SET = frozenset(UNIVERSE)
UNIVERSE_BYTES = bytes(UNIVERSE)

# Seed length: enough that ob_bytes is a real heap buffer (not the empty-array
# special case) and a del-slice has a span to memmove.  Sized so the grower's
# concats push it across several PyByteArray_Resize / realloc growth boundaries.
SEED_LEN = 256

# How many UNIVERSE bytes the grower appends per concat/extend chunk.  Large
# enough that the cumulative growth forces repeated realloc (ob_bytes MOVES),
# small enough that many rounds complete under the timeout.
CHUNK = 96

# How many grow ops and shrink ops the mutators do per round.  A mix of
# +=, extend, del-slice (interior memmove), front-del (ob_start advance), and
# pop, so every realloc/memmove path is driven against the live iterator.
GROW_OPS = 6
SHRINK_OPS = 5

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The grow/shrink CASES, round-robined by worker id in the first ops so coverage
# is deterministic (pure random reliably MISSES a case at low op-count under
# load -- the p125/p126/p172 flaky-coverage bug the suite already had to fix).
CASE_ICONCAT = 0     # ba += chunk            -> bytearray_iconcat realloc
CASE_EXTEND = 1      # ba.extend(chunk)       -> bytearray_extend realloc
CASE_DELSLICE = 2    # del ba[i:j] (interior) -> bytearray_ass_subscript memmove
CASE_FRONTDEL = 3    # del ba[:k]             -> ob_start advance + Py_SIZE shrink
CASE_POP = 4         # ba.pop()               -> tail shrink + possible realloc
NCASES = 5


def make_chunk(rng, n):
    """A chunk of n UNIVERSE bytes (closed-world: only alphabet bytes enter the
    shared buffer).  Uses the caller's per-fiber rng (a shared random.Random
    corrupts GIL-off)."""
    return bytes(UNIVERSE_BYTES[rng.randrange(64)] for _ in range(n))


def apply_op(ba, op, payload):
    """Apply ONE recorded grow/shrink op to `ba` (shared OR private control).

    op is (case, arg).  The SAME function drives both arms so the shared
    bytearray and the single-owner private control go through a byte-identical
    sequence -- any divergence in their final content is a realloc/memmove fault,
    not contention.  Returns nothing; mutates ba in place.  All inserted bytes are
    from UNIVERSE, all deletes only shrink, so the closed world is preserved."""
    case, arg = op
    if case == CASE_ICONCAT:
        ba += arg                       # bytearray_iconcat: realloc + memcpy tail
    elif case == CASE_EXTEND:
        ba.extend(arg)                  # bytearray_extend: realloc + append
    elif case == CASE_DELSLICE:
        n = len(ba)
        if n >= 4:
            i, j = arg
            i %= n
            j = i + (j % max(1, (n - i)))
            del ba[i:j]                 # interior memmove + Py_SIZE shrink
    elif case == CASE_FRONTDEL:
        n = len(ba)
        if n >= 4:
            k = 1 + (arg % (n // 2))
            del ba[:k]                  # front del: advance ob_start, shrink size
    elif case == CASE_POP:
        if len(ba) > 0:
            ba.pop()                    # tail shrink (may shrink-realloc ob_bytes)


def build_grow_ops(rng):
    """Build the grower's recorded op list: GROW_OPS of += / extend, each with a
    fresh UNIVERSE chunk.  Returned as (case, arg) tuples so the identical
    sequence can be replayed on the private control."""
    ops = []
    for i in range(GROW_OPS):
        case = CASE_ICONCAT if (i & 1) == 0 else CASE_EXTEND
        ops.append((case, make_chunk(rng, CHUNK)))
    return ops


def build_shrink_ops(rng, start_case):
    """Build the shrinker's recorded op list: SHRINK_OPS spanning del-slice,
    front-del, and pop, round-robined from start_case so coverage is
    deterministic.  Args are RNG params resolved against the live length inside
    apply_op (so they remain valid as the buffer shrinks)."""
    ops = []
    shrink_cases = (CASE_DELSLICE, CASE_FRONTDEL, CASE_POP)
    for i in range(SHRINK_OPS):
        case = shrink_cases[(start_case + i) % len(shrink_cases)]
        if case == CASE_DELSLICE:
            arg = (rng.randrange(1 << 20), 1 + rng.randrange(48))
        elif case == CASE_FRONTDEL:
            arg = rng.randrange(1 << 20)
        else:
            arg = 0
        ops.append((case, arg))
    return ops


def walk_iterator(H, ba, gate, counts, slot):
    """Walk iter(ba), parking once mid-walk after tripping `gate` (so the mutators'
    realloc/memmove provably lands DURING the park), checking every yielded byte
    is in UNIVERSE.  Holds NO lock -- this is the use-after-realloc probe.

    A SHORT walk is legal (a concurrent del shrank Py_SIZE, so
    bytearrayiter_next's bounds check stops early); what is NEVER legal is an
    out-of-universe byte (freed/relocated ob_bytes read) or any exception
    escaping the walk (the bytearray iterator has no 'changed size' guard, so it
    must not raise -- a raised exception means a torn internal index)."""
    parked = False
    seen = 0
    try:
        for b in ba:
            if b not in UNIVERSE_SET:
                H.fail("bytearray iterator yielded OUT-OF-UNIVERSE byte {0!r} at "
                       "step {1} -- a use-after-realloc read of freed/relocated "
                       "ob_bytes (a sibling's bytearray resize moved the buffer "
                       "under the live it_index)".format(b, seen))
                gate_trip(gate, parked)
                return "fail"
            seen += 1
            if not parked and seen >= 2:
                # Trip the gate (release the mutators) then PARK with it_index
                # live -- the realloc/memmove lands here, in the park window.
                parked = True
                gate.done()
                runloom.yield_now()
        counts["walked"][slot] += 1
        return "clean"
    except Exception as exc:                # noqa: BLE001
        # bytearrayiter_next has NO version/size guard, so a legal concurrent
        # resize never raises (it just re-reads the live buffer / stops short).
        # ANY exception here is a torn internal index / corrupted iterator state.
        H.fail("bytearray iterator raised {0}: {1} after {2} bytes -- the "
               "bytearray iterator has no 'changed size' guard, so a raised "
               "exception means a torn it_index / corrupted iterator state under "
               "the concurrent realloc".format(type(exc).__name__, exc, seen))
        gate_trip(gate, parked)
        return "fail"


def gate_trip(gate, parked):
    """Ensure the gate is released so the mutators never block forever if the
    iterator bailed before it parked."""
    if not parked:
        try:
            gate.done()
        except Exception:
            pass


def run_round_impl(H, wid, rng, slot, state):
    """One round: build the grower's and shrinker's recorded op sequences, apply
    them to a PRIVATE control bytearray single-owner first (race-free reference),
    then drive the SAME ops into a SHARED bytearray under the write-lock while an
    iterator races the realloc/memmove on another hub.  Quiescent post-round:
    shared content == private control content (byte-exact), lengths agree across
    ob_start/Py_SIZE/ob_bytes, all bytes in UNIVERSE."""
    lock = state["lock"]

    # Fresh seed of UNIVERSE bytes (real heap buffer, has a span to memmove).
    seed = bytes(UNIVERSE_BYTES[rng.randrange(64)] for _ in range(SEED_LEN))

    grow_ops = build_grow_ops(rng)
    shrink_ops = build_shrink_ops(rng, wid % 3)

    # The SERIALIZED write order: the grower op then the shrinker op, interleaved,
    # exactly as they will be applied under the lock in the shared arm.  The
    # private control replays this identical merged order single-owner.
    merged = []
    gi = si = 0
    turn = 0
    while gi < len(grow_ops) or si < len(shrink_ops):
        if turn == 0 and gi < len(grow_ops):
            merged.append(grow_ops[gi]); gi += 1
        elif si < len(shrink_ops):
            merged.append(shrink_ops[si]); si += 1
        elif gi < len(grow_ops):
            merged.append(grow_ops[gi]); gi += 1
        turn ^= 1

    # ---- single-owner control arm: byte-exact reference, race-free ----------
    control = bytearray(seed)
    for op in merged:
        apply_op(control, op, None)
    expected_bytes = bytes(control)

    # ---- shared arm: same ops, serialized writers, racing iterator ----------
    shared = bytearray(seed)

    # The write order is shared mutable state stepped under the lock; a simple
    # index works because every write holds the lock.  We replay `merged` so the
    # serialized shared order is byte-identical to the control's.
    order = {"i": 0}

    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(2)
    # The mutators wait on the gate (tripped by the iterator just before it parks)
    # so their realloc/memmove lands inside the park window.

    def run_mutators():
        # ONE fiber drains the merged op list under the lock (serialized writers,
        # conservation framing).  We split nothing across fibers for the WRITE
        # path -- the contention under test is iterator-vs-resize, and serializing
        # writers is what makes the post-round oracle a clean byte-exact law.
        try:
            gate.wait()                 # iterator has parked mid-walk
            for op in merged:
                with lock:
                    apply_op(shared, op, None)
                    runloom.yield_now()  # iterator resumes its read during our RMW
        except Exception as exc:        # noqa: BLE001
            H.error(wid, exc)
        finally:
            wg.done()

    def run_iter():
        try:
            walk_iterator(H, shared, gate, state["counts"], slot)
        finally:
            wg.done()

    H.fiber(run_iter)
    H.fiber(run_mutators)
    wg.wait()                           # both joined -> shared now quiescent

    if H.failed:
        return

    # ---- closed-world reconciliation (quiescent, single-owner) --------------
    # Length self-consistency: ob_start / Py_SIZE / ob_bytes must agree.  A torn
    # length desync makes len(ba) (Py_SIZE), len(bytes(ba)) (copy via the buffer),
    # and len(list(ba)) (iterate ob_bytes[it_index] to Py_SIZE) diverge.
    snap = bytes(shared)
    as_list = list(shared)
    if not H.check(len(shared) == len(snap) == len(as_list),
                   "length desync: len(ba)={0} len(bytes(ba))={1} "
                   "len(list(ba))={2} -- ob_start/Py_SIZE/ob_bytes disagree after "
                   "the concurrent realloc/memmove (torn length state)".format(
                       len(shared), len(snap), len(as_list))):
        return

    # Every quiescent byte is in UNIVERSE (no freed/relocated byte survived).
    for idx, b in enumerate(snap):
        if b not in UNIVERSE_SET:
            H.fail("quiescent shared bytearray holds OUT-OF-UNIVERSE byte {0!r} "
                   "at offset {1} -- a freed/relocated ob_bytes byte survived the "
                   "concurrent resize (use-after-realloc)".format(b, idx))
            return

    # SINGLE-OWNER CONTROL: the serialized shared writes were applied in the same
    # order as the private control, so the shared content must be BYTE-EXACT equal
    # to the control.  A divergence that never crashed (a relocated chunk, a
    # mis-targeted memmove, a dropped/doubled append) is a realloc/memmove bug in
    # CPython's bytearray machinery, NOT contention -- the control has one writer.
    if not H.check(snap == expected_bytes,
                   "shared-vs-control divergence: shared bytearray content differs "
                   "from the byte-exact private control after the identical "
                   "serialized grow/shrink sequence (len shared={0} control={1}) "
                   "-- a realloc/memmove relocated/dropped/doubled bytes under the "
                   "live iterator".format(len(snap), len(expected_bytes))):
        return

    # Tallies (single-writer-per-slot, race-free; summed in post()).
    state["bytes_in"][slot] += len(snap)
    state["rounds"][slot] += 1


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    for _ in H.round_range():
        if not H.running():
            break
        run_round_impl(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so runloom.sync.Lock is
    # the cooperative M:N-safe primitive.  The lock serializes WRITES to the shared
    # bytearray (bytearray is documented NOT thread-safe); the iterator races
    # without it, which is the use-after-realloc probe.
    H.state = {
        "lock": runloom.sync.Lock(),
        "counts": {"walked": [0] * SLOTS},   # iterator walks that finished clean
        "bytes_in": [0] * SLOTS,             # quiescent bytes reconciled
        "rounds": [0] * SLOTS,               # rounds whose byte-exact law held
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    walked = sum(H.state["counts"]["walked"])
    bytes_in = sum(H.state["bytes_in"])
    rounds = sum(H.state["rounds"])
    H.log("iterator clean-walks={0} rounds-reconciled={1} bytes-reconciled={2} "
          "ops={3} (every out-of-universe byte / length desync / shared-vs-"
          "control divergence already failed fast)".format(
              walked, rounds, bytes_in, H.total_ops()))
    # Reaching post with no failure already proves every per-round byte-exact law
    # and universe-membership check held; assert the run actually did work (else
    # the realloc-vs-iterate window was never exercised).
    H.check(rounds > 0,
            "no rounds reconciled -- the bytearray realloc-vs-iterator race "
            "window was never exercised")
    H.check(H.total_ops() > 0, "no rounds completed")
    H.require_no_lost("bytearray-realloc-vs-iterator completeness")


if __name__ == "__main__":
    harness.main(
        "p422_bytearray_concat_delslice_real", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a shared bytearray is grown (ba+=/extend) and shrunk (del "
                 "ba[i:j] / front-del / pop) -- realloc/memmove of ob_bytes, "
                 "ob_start/Py_SIZE rewrite -- while a live bytearray iterator "
                 "walks it across a park on another hub; closed-world law: every "
                 "byte in a 64-value sentinel UNIVERSE, len(ba)==len(bytes(ba))=="
                 "len(list(ba)), and shared content byte-exact == a single-owner "
                 "private control -- an out-of-universe byte (use-after-realloc), "
                 "a length desync, or a control divergence fails")
