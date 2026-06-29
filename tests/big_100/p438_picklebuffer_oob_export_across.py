"""big_100 / 438 -- PickleBuffer out-of-band export held ACROSS an M:N park.

The subject is ``pickle.PickleBuffer`` driving a protocol-5 OUT-OF-BAND buffer.
No other program in the suite touches the protocol-5 ``buffer_callback`` path or a
buffer EXPORTER held by a STDLIB object (the pickler) rather than by a user-held
``memoryview``.

THE EXACT C-LEVEL STATE UNDER ATTACK.  ``pickle.PickleBuffer(obj)`` opens a live
``Py_buffer`` over ``obj`` WITHOUT copying.  For a ``bytearray`` source that
acquire bumps the object's internal export counter -- ``PyByteArrayObject.ob_exports``
(CPython Objects/bytearrayobject.c) -- and the matching ``PickleBuffer.release()``
(or the PickleBuffer's deallocation) runs ``PyBuffer_Release`` which DECREMENTS it.
While ``ob_exports > 0`` the bytearray is FROZEN: every resize path
(``PyByteArray_Resize`` behind ``.append()`` / ``.extend()`` / ``+=`` / ``del b[:]``)
checks ``ob_exports`` and raises ``BufferError`` rather than ``realloc`` the backing
``ob_bytes`` out from under a live exporter.  ``array.array`` carries the same
``ob_exports`` interlock for the control case.

THE RACING OP PAIR.  The hazard is a TORN export count: the PickleBuffer
creation/release export RMW (``ob_exports += 1`` / ``-= 1`` inside
``PyObject_GetBuffer`` / ``PyBuffer_Release``) racing a sibling hub's
``bytearray`` resize.  If a creation increment is LOST (torn read of a stale
``ob_exports``), the resize check sees 0 and ``realloc`` moves ``ob_bytes`` while
the PickleBuffer's ``buf`` pointer still aims at the freed/old block -- the
out-of-band payload then reconstructed by unpickle reads FREED OR MOVED memory:
silent corruption (or SIGSEGV) of the round-tripped object.  If a release
decrement is LOST instead, the export LEAKS: the source stays frozen forever and
a later legal ``append()`` is wrongly refused.

WHY M:N MAKES IT REACHABLE.  ``pickle.dumps(carrier, protocol=5,
buffer_callback=cb)`` drives the carrier's buffer OUT-OF-BAND: the pickler calls
``cb(PickleBuffer(src))`` and writes only a placeholder, so the collected
PickleBuffer holds ``src``'s export OPEN across the ENTIRE transfer.  That
transfer step is COOPERATIVE -- the fiber that collected the PickleBuffer(s)
PARKS (``runloom.yield_now()``) on its grown-down C stack with the export still
held, exactly as a real out-of-band transport would await an ack.  A sibling
fiber on ANOTHER hub, released into that park window, hammers ``src.append()`` /
``src += ...`` on the SAME bytearray.  Every one of those resizes MUST be refused
with ``BufferError`` for as long as ANY PickleBuffer over ``src`` is live; the
moment one slips through, ``ob_bytes`` is reallocated and the parked PickleBuffer
dangles.

CLOSED-WORLD, FALSIFIABLE ROUND-TRIP IDENTITY.  Each round builds a FRESH source
``bytearray`` of length L with ``src[i] = f(round_key, i)`` -- a deterministic
finite SENTINEL UNIVERSE of bytes.  The worker pickles it protocol-5 with a
``buffer_callback`` collecting the PickleBuffer(s), trips a gate, PARKS, and a
sibling mutator attempts ``src.append()``/``+=``/``del`` repeatedly during the
park.  The invariants, all mutually-exclusive and each falsifiable:

  * REFUSED-WHILE-HELD: every sibling resize attempt while ANY PickleBuffer over
    ``src`` is live MUST raise ``BufferError`` (we count refusals; a single
    SILENT success means an export increment was torn/lost -> the buffer can move
    -> FAIL).
  * IDENTITY CONSERVED: on resume the fiber ``pickle.loads(data, buffers=pbufs)``
    and asserts the reconstructed bytes equal ``f(round_key, i)`` for EVERY i.  A
    resize that slipped during the park would have realloc'd the block and the
    reconstructed payload reads freed/moved memory -> an OUT-OF-UNIVERSE byte or a
    torn value -> FAIL.  (A SIGSEGV mid-reconstruct is the harder crash the
    watchdog/faulthandler catches.)
  * EXPORT RETURNED TO ZERO: after ALL PickleBuffers are ``.release()``d, a fresh
    ``src.append()`` MUST succeed (and ``del src[:]`` MUST succeed) -- the export
    count came back to 0, no leak.  If it still raises ``BufferError``, a release
    decrement was LOST -> permanent leak -> FAIL.

SINGLE-OWNER CONTROL ARM (case CONTROL).  The SAME pickle -> collect -> resize-
refused -> release -> resize-ok sequence run entirely in ONE fiber with NO
cross-hub mutator.  A single owner cannot race itself, so it MUST round-trip
byte-exact AND leave ``src`` resizable.  If the CONTROL ever loses identity or
leaks the export, the fault is in CPython's PickleBuffer/ob_exports machinery
itself, not in M:N contention -- this disambiguates "the primitive is buggy" from
"a cross-hub race corrupted it".  A second control sub-case pickles an
``array.array`` (the other ``ob_exports`` exporter) so both buffer-exporting
builtins are covered.

COVERAGE.  Cases are round-robined by worker id in the first ops (``sel =
(wid + i) % NCASES``) -- never flaky random (the p125/p126/p172 lesson) -- then
random; post() asserts each case ran and that refusals actually fired (so the
export interlock was genuinely exercised, not skipped).

Invariant (hot, fail-fast): a sibling resize on a buffer-held bytearray raises
BufferError (refusals > 0); reconstructed bytes == f(key, i) for every i;
post-release append/del succeeds.  Invariant (post): round-trips == reconstructed
== conserved (no identity lost), refusals > 0, leaks == 0, every case exercised,
no lost worker.

Stresses: PickleBuffer protocol-5 out-of-band buffer_callback export held across a
park, bytearray ob_exports increment/decrement RMW vs resize-realloc, BufferError
resize interlock under cross-hub mutation, round-trip identity conservation,
export-count leak, single-owner vs cross-hub disambiguation.

Good TSan / controlled-M:N-replay target: the ob_exports increment in
PyObject_GetBuffer racing PyByteArray_Resize's ob_exports check is a textbook
read-modify-write data race; a TSan report on ob_exports, or a single silent
resize / out-of-universe reconstructed byte under replay, localizes the torn
export before the identity assert even fires.
"""
import array
import pickle

import harness
import runloom

# Length of each fresh source bytearray.  Big enough that a slipped realloc moves
# a lot of payload (so a dangling reconstruct is very likely to land out-of-
# universe), and that the buffer is a real heap block CPython would actually
# realloc to a new address on grow; small enough that thousands of rounds fit.
SRC_LEN = 512

# Finite sentinel UNIVERSE of bytes.  src[i] = f(round_key, i) is a deterministic
# byte in [0, 256); a reconstructed byte that does not equal f(round_key, i) is a
# TORN/FREED read.  We also keep a per-round recognizable key so a payload from a
# DIFFERENT round (a reused freed block) is caught as out-of-universe-for-this-key.
def f(round_key, i):
    """Deterministic source byte for position i of the round keyed by round_key.

    A bijection-ish mix so a torn/moved read is very unlikely to coincidentally
    reproduce the expected byte.  Returns a value in [0, 256)."""
    return ((round_key * 0x9E3779B1) ^ (i * 0x85EBCA77) ^ (i >> 3)) & 0xFF


# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# How many resize attempts the sibling mutator makes during the park.  Each MUST
# be refused with BufferError while a PickleBuffer is live; >1 widens the window
# in which a torn export increment could let one slip.
MUTATE_ATTEMPTS = 6

# The cases, round-robined by worker id so coverage is deterministic.
CASE_CROSS_APPEND = 0    # cross-hub: sibling hammers src.append() during the park
CASE_CROSS_IADD = 1      # cross-hub: sibling hammers src += b'..' / del src[:]
CASE_CONTROL_BA = 2      # single-owner control: bytearray, no cross-hub mutator
CASE_CONTROL_ARR = 3     # single-owner control: array.array exporter
NCASES = 4


class Carrier(object):
    """A picklable wrapper whose protocol-5 reduction emits a PickleBuffer over its
    backing object WITHOUT copying, so pickle's buffer_callback drives the buffer
    out-of-band and the collected PickleBuffer holds the source's export OPEN.

    For protocol < 5 it falls back to an in-band bytes copy (never used here -- we
    always pickle with protocol=5 -- but keeps the reduction total)."""

    def __init__(self, obj):
        self.obj = obj

    def __reduce_ex__(self, protocol):
        if protocol >= 5:
            # PickleBuffer(self.obj) opens a live Py_buffer over self.obj and bumps
            # its ob_exports.  The pickler hands it to buffer_callback (out-of-band)
            # and the PickleBuffer keeps the export held until release()/dealloc.
            return (Carrier.from_buffer, (pickle.PickleBuffer(self.obj),))
        return (Carrier.from_bytes, (bytes(self.obj),))

    @staticmethod
    def from_buffer(buf):
        # Reconstruct: copy the out-of-band buffer's bytes into a fresh bytearray.
        # If the source was realloc'd out from under `buf` (a slipped resize), THIS
        # read touches freed/moved memory -- the corruption surfaces here.
        with memoryview(buf) as m:
            return Carrier(bytearray(m))

    @staticmethod
    def from_bytes(b):
        return Carrier(bytearray(b))


def fresh_source(round_key):
    """A fresh bytearray src with src[i] = f(round_key, i) over the universe."""
    return bytearray(f(round_key, i) for i in range(SRC_LEN))


def expected_bytes(round_key):
    return bytes(f(round_key, i) for i in range(SRC_LEN))


def check_identity(H, round_key, carrier_out):
    """Assert the reconstructed Carrier's bytes equal f(round_key, i) for every i.
    A mismatch is a torn/freed read (the source moved under a live PickleBuffer).
    Returns True iff identity is conserved."""
    got = carrier_out.obj
    want = expected_bytes(round_key)
    if len(got) != len(want):
        H.fail("round-trip identity LOST: reconstructed length {0} != {1} for "
               "round_key {2} -- the out-of-band buffer was resized/realloc'd "
               "under a live PickleBuffer (dangling buf pointer)".format(
                   len(got), len(want), round_key))
        return False
    if bytes(got) != want:
        # Find the first divergent index for a precise, falsifiable message.
        bad = next((i for i in range(len(want)) if got[i] != want[i]), -1)
        bv = got[bad] if 0 <= bad < len(got) else None
        H.fail("round-trip identity LOST: reconstructed byte[{0}]={1!r} != "
               "f(key={2}, {0})={3!r} -- an OUT-OF-UNIVERSE byte: the source "
               "bytearray was realloc'd during the park while a torn ob_exports "
               "left the PickleBuffer's buf pointer dangling (freed/moved read)"
               .format(bad, bv, round_key, f(round_key, bad)))
        return False
    return True


def resize_refused(H, src, tally, slot, label):
    """Attempt several resizes on `src`.  While ANY PickleBuffer over it is live
    EVERY attempt MUST raise BufferError.  Returns (refusals, slipped): refusals is
    how many correctly raised; slipped is True iff one silently succeeded (the
    bug).  A slip means an export increment was torn/lost so the resize check saw
    ob_exports == 0 and reallocated the backing block."""
    refusals = 0
    slipped = False
    for n in range(MUTATE_ATTEMPTS):
        # Alternate the resize PATH so several distinct PyByteArray_Resize callers
        # are exercised against the ob_exports interlock.
        try:
            if n % 3 == 0:
                src.append(0x7F)
            elif n % 3 == 1:
                src += b"\xff\xff\xff\xff"
            else:
                del src[: 4]
        except BufferError:
            refusals += 1
            tally[slot] += 1
            continue
        # A silent success is the fault: the source was resized while a
        # PickleBuffer over it is live.
        slipped = True
        H.fail("{0}: a resize on a bytearray with a LIVE PickleBuffer export "
               "SUCCEEDED silently (no BufferError) -- ob_exports was torn to 0 "
               "under cross-hub contention, so the backing block can be realloc'd "
               "out from under the parked PickleBuffer (dangling buf -> "
               "use-after-free)".format(label))
        break
    return refusals, slipped


def assert_export_returned(H, src, label):
    """After every PickleBuffer over `src` is released, the export count is back at
    0, so a fresh resize MUST succeed.  If it still raises BufferError a release
    decrement was LOST -> a permanent export LEAK.  Returns True iff resizable."""
    try:
        src.append(0x01)
        del src[-1:]
    except BufferError:
        H.fail("{0}: after releasing ALL PickleBuffers, src.append() STILL raised "
               "BufferError -- a release decrement of ob_exports was LOST, the "
               "export LEAKED and the bytearray is permanently frozen".format(
                   label))
        return False
    return True


def do_cross(H, wid, rng, state, slot, iadd):
    """Cross-hub case: pickle src protocol-5 collecting its PickleBuffer(s), trip a
    gate, PARK while a sibling on another hub hammers resizes (which MUST all be
    refused), then resume, reconstruct (identity MUST be conserved), release, and
    assert the export returned to 0.  iadd selects the sibling's resize flavor."""
    refused = state["refused"]
    rt = state["roundtrip"]
    leaks = state["leaks"]
    round_key = (wid << 20) ^ (rng.getrandbits(20) | 1)
    src = fresh_source(round_key)

    pbufs = []
    data = pickle.dumps(Carrier(src), protocol=5,
                        buffer_callback=pbufs.append)
    # Out-of-band must actually have happened, else the export was never held and
    # the whole hazard is moot.
    if not H.check(len(pbufs) >= 1,
                   "protocol-5 buffer_callback collected NO PickleBuffer -- the "
                   "buffer did not go out-of-band, so no export was held (the "
                   "hazard window never opened)"):
        return False

    # gate: the worker trips it the instant before it parks; the mutator waits on
    # it, so the sibling's resize attempts provably land INSIDE the park window
    # while the PickleBuffer(s) hold src's export open.
    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(1)
    mseed = rng.getrandbits(48)
    refusals_box = [0]
    slipped_box = [False]

    def run_mutator(src=src, gate=gate, mseed=mseed, iadd=iadd):
        # Its OWN random.Random -- a shared one corrupts GIL-off.
        import random
        mrng = random.Random(mseed)
        try:
            gate.wait()                     # released into the park window
            label = "cross-iadd" if iadd else "cross-append"
            refusals, slipped = resize_refused(H, src, refused, slot, label)
            refusals_box[0] = refusals
            slipped_box[0] = slipped
            # Touch mrng so its per-fiber stream is real (keeps replay seeded even
            # though the resize set is deterministic).
            if mrng.getrandbits(1) and False:   # never taken; keeps mrng live
                src.append(0)
        finally:
            wg.done()

    H.fiber(run_mutator)

    # Trip the gate, then PARK with the PickleBuffer(s) live and the export held.
    gate.done()
    runloom.yield_now()                     # the sibling's resizes land here
    runloom.yield_now()
    wg.wait()                               # mutator finished its attempts

    if H.failed:
        return False

    # The sibling made MUTATE_ATTEMPTS attempts; every one MUST have been refused
    # (none slipped) while the export was held.
    if slipped_box[0]:
        return False
    if not H.check(refusals_box[0] == MUTATE_ATTEMPTS,
                   "cross-hub: only {0}/{1} sibling resizes were refused while a "
                   "PickleBuffer held src's export -- a resize that was neither "
                   "refused nor counted means a lost/torn ob_exports event".format(
                       refusals_box[0], MUTATE_ATTEMPTS)):
        return False

    # Resume: reconstruct from the out-of-band buffers.  Identity MUST be conserved
    # -- a resize that slipped during the park would have moved the block and this
    # read would land out-of-universe.
    out = pickle.loads(data, buffers=pbufs)
    if not check_identity(H, round_key, out):
        return False
    rt[slot] += 1

    # Release every PickleBuffer -> the export must return to 0.
    for pb in pbufs:
        pb.release()
    label = "cross-iadd" if iadd else "cross-append"
    if not assert_export_returned(H, src, label):
        leaks[slot] += 1
        return False
    return True


def do_control(H, wid, rng, state, slot, use_array):
    """Single-owner CONTROL: the whole pickle -> collect -> resize-refused ->
    reconstruct -> release -> resize-ok sequence in ONE fiber with NO cross-hub
    mutator.  A single owner cannot race itself, so it MUST round-trip byte-exact
    and leave the source resizable; if it does not, the fault is in CPython's
    PickleBuffer/ob_exports machinery, not in M:N contention."""
    rt = state["roundtrip"]
    refused = state["refused"]
    leaks = state["leaks"]
    round_key = (wid << 20) ^ (rng.getrandbits(20) | 1)

    if use_array:
        # array.array('B') over the same universe -- the OTHER ob_exports exporter.
        src = array.array("B", (f(round_key, i) for i in range(SRC_LEN)))
        want = bytes(f(round_key, i) for i in range(SRC_LEN))
    else:
        src = fresh_source(round_key)
        want = expected_bytes(round_key)

    pbufs = []
    data = pickle.dumps(Carrier(src), protocol=5,
                        buffer_callback=pbufs.append)
    if not H.check(len(pbufs) >= 1,
                   "control: protocol-5 buffer_callback collected NO PickleBuffer "
                   "for {0} -- buffer did not go out-of-band".format(
                       "array" if use_array else "bytearray")):
        return False

    # While the PickleBuffer is live, a resize MUST be refused -- in the SAME
    # single-owning fiber (no contention at all), so a refusal here is the pure
    # ob_exports interlock, and a SLIP here is a primitive bug, not a race.
    label = "control-array" if use_array else "control-bytearray"
    if use_array:
        # array.array resize via append/extend also checks ob_exports.
        try:
            src.append(0x7F)
            slipped = True
        except BufferError:
            refused[slot] += 1
            slipped = False
        if slipped:
            H.fail("{0}: resizing an array.array with a LIVE PickleBuffer export "
                   "SUCCEEDED silently (no BufferError) -- the single-owner "
                   "ob_exports interlock itself is broken (a CPython bug, not "
                   "contention)".format(label))
            return False
    else:
        refusals, slipped = resize_refused(H, src, refused, slot, label)
        if slipped:
            return False
        if not H.check(refusals == MUTATE_ATTEMPTS,
                       "{0}: only {1}/{2} single-owner resizes refused while a "
                       "PickleBuffer held the export -- the ob_exports interlock "
                       "lost an event with NO contention (CPython bug)".format(
                           label, refusals, MUTATE_ATTEMPTS)):
            return False

    # Park anyway (a control fiber still cooperatively yields) -- proves a park with
    # the export held is fine WITHOUT a racing sibling.
    runloom.yield_now()

    out = pickle.loads(data, buffers=pbufs)
    got = bytes(out.obj)
    if got != want:
        bad = next((i for i in range(len(want)) if i >= len(got) or got[i] != want[i]), -1)
        H.fail("{0}: single-owner round-trip identity LOST at byte[{1}] -- a "
               "PickleBuffer round-trip with NO contention corrupted the payload "
               "(CPython PickleBuffer machinery bug, not M:N)".format(label, bad))
        return False
    rt[slot] += 1

    for pb in pbufs:
        pb.release()
    if not assert_export_returned_any(H, src, label, use_array):
        leaks[slot] += 1
        return False
    return True


def assert_export_returned_any(H, src, label, use_array):
    """Export-returned-to-0 check that works for both bytearray and array.array."""
    try:
        if use_array:
            src.append(0x01)
            del src[-1:]
        else:
            src.append(0x01)
            del src[-1:]
    except BufferError:
        H.fail("{0}: after release the source STILL refused a resize -- a release "
               "decrement of ob_exports was LOST with NO contention (CPython "
               "export-leak bug)".format(label))
        return False
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the cases by worker id in the first ops so every case is
        # exercised even when each worker manages only a few ops under the timeout
        # (the p125/p126/p172 flaky-random-coverage fix); random after.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_CROSS_APPEND:
            ok = do_cross(H, wid, rng, state, slot, iadd=False)
        elif sel == CASE_CROSS_IADD:
            ok = do_cross(H, wid, rng, state, slot, iadd=True)
        elif sel == CASE_CONTROL_BA:
            ok = do_control(H, wid, rng, state, slot, use_array=False)
        else:
            ok = do_control(H, wid, rng, state, slot, use_array=True)
        if not ok:
            return
        # Tally which case ran (single-writer-per-slot, race-free).
        state["case"][sel][slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran).  All per-slot tallies are
    # single-writer-per-slot lists summed in post(); no shared += on the hot path.
    H.state = {
        "roundtrip": [0] * SLOTS,           # round-trips with identity conserved
        "refused": [0] * SLOTS,             # resizes correctly refused (BufferError)
        "leaks": [0] * SLOTS,               # post-release export leaks (should be 0)
        "case": [[0] * SLOTS for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rt = sum(H.state["roundtrip"])
    refused = sum(H.state["refused"])
    leaks = sum(H.state["leaks"])
    case_totals = [sum(H.state["case"][c]) for c in range(NCASES)]
    H.log("round-trips(identity-conserved)={0} resizes-refused={1} "
          "export-leaks={2} cases(cross-append/cross-iadd/ctl-ba/ctl-arr)={3} "
          "ops={4}".format(rt, refused, leaks, case_totals, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # Round-trip identity was conserved on every successful round (the per-round
    # check is fail-fast, so reaching post with rt>0 and no failure proves it).
    H.check(rt > 0,
            "no PickleBuffer round-trip ever completed with identity conserved -- "
            "the out-of-band export/transfer/reconstruct path was never exercised")

    # The ob_exports resize interlock was genuinely tested: at least one resize on
    # a buffer-held source was refused with BufferError.  If refused==0 the whole
    # export-held invariant was vacuous.
    H.check(refused > 0,
            "no resize was ever refused while a PickleBuffer held the export -- "
            "the ob_exports interlock was never exercised (invariant untested)")

    # No export leaked: every PickleBuffer.release() returned the count to 0 and a
    # subsequent resize succeeded.  (Per-round checks are fail-fast; this asserts
    # the post-quiescent total.)
    H.check(leaks == 0,
            "{0} export LEAK(s): after releasing all PickleBuffers a source still "
            "refused a resize -- a release decrement of ob_exports was lost".format(
                leaks))

    # Every case was exercised (deterministic round-robin guarantees it once
    # enough ops ran).  Both the cross-hub probes AND both single-owner controls
    # must have run so the bug/no-bug disambiguation is real.
    names = ["cross-append", "cross-iadd", "control-bytearray", "control-array"]
    for c in range(NCASES):
        H.check(case_totals[c] > 0,
                "case {0!r} never exercised -- coverage gap (the {1} arm did not "
                "run)".format(names[c],
                              "cross-hub" if c < 2 else "single-owner control"))

    H.require_no_lost("picklebuffer-oob-export completeness")


if __name__ == "__main__":
    harness.main(
        "p438_picklebuffer_oob_export_across", body, setup=setup, post=post,
        default_funcs=3000,
        describe="PickleBuffer protocol-5 out-of-band export held across an M:N "
                 "park: a sibling hub's bytearray resize MUST raise BufferError "
                 "while the PickleBuffer is live, the reconstructed bytes MUST "
                 "equal f(key,i) (round-trip identity), and after release the "
                 "source MUST be resizable again -- a torn ob_exports lets the "
                 "source realloc under the dangling buffer (silent corruption)")
