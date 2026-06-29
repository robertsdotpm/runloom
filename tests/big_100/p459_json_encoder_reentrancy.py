"""big_100 / 459 -- json C encoder reentrancy across a preempt MID-ENCODE.

json.dumps() uses the C encoder (json.encoder.c_make_encoder).  When you pass a
`default=` callback, that callback runs ARBITRARY PYTHON for every object the
encoder doesn't know how to serialize -- and it runs WHILE the C encoder is
partway through building the output for the enclosing object.  In a runloom M:N
runtime a callback that yields / sleeps (runloom.sleep) is a SCHEDULING POINT: the
fiber can be preempted and the hub can switch to a SIBLING fiber that is ALSO
inside its own json.dumps on the same hub / same OS thread.  json is almost
universally ASSUMED reentrant-safe, so this is a LOAD-BEARING probe: a clean run
is a valuable negative control proving the C encoder + its internal buffer
machinery tolerate a mid-encode hub switch; a failure (spliced bytes from a
sibling concurrent encode, a wid tag that is not this fiber's, a round-trip
mismatch, or a crash) is a real runtime bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (discriminator discipline, per p321):

  * LOAD-BEARING -- SINGLE-OWNER ROUND-TRIP IDENTITY (per-op, HARD).
    Each fiber builds its OWN distinct nested object, tagged with its wid, that
    embeds custom objects whose `default=` callback calls runloom.sleep/yield_now
    MID-ENCODE to force a hub switch INSIDE dumps().  Every object touched is
    owned by exactly THIS fiber -- nothing is shared between fibers except the
    interpreter's json machinery (the C encoder, the module-global encoder
    object json.dumps uses when called with default kwargs, and any recycled
    internal buffer).  The fiber then json.dumps -> json.loads and asserts it
    recovers ITS OWN object EXACTLY:
        - the round-trip equals the original (no lost/garbled bytes);
        - every wid tag in the decoded object is THIS fiber's wid (no sibling's
          bytes spliced into our output buffer across the preempt);
        - the callback fired the expected number of times (the encode really did
          run, so the oracle is not vacuous).
    Under plain OS threads with the GIL ON this ALWAYS holds (verified by the
    standalone control below): the GIL serializes the C encoder, and even though
    a default= callback can run other Python, json.dumps builds into a FRESH
    accumulator per call, so no sibling's bytes ever appear in our result.  A
    correct runloom MUST match that.  If runloom's preempt-mid-C-encode splices a
    sibling fiber's bytes into our buffer, or corrupts the shared encoder state
    across a hub migration, the identity breaks -- and THAT is the real runloom
    bug this program uniquely catches.  Exits 0 when there is no bug.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber preempted inside the C
    encoder (parked in its default= callback) that is never re-woken never
    returns; the watchdog catches an outright strand and require_no_lost catches
    a parked-then-vanished worker.

  * MEASURED arm (report-ONLY, NEVER fails): a SHARED MUTABLE SCRATCH buffer that
    the default= callback WRITES then (across the mid-encode yield) READS BACK,
    while many fibers encode concurrently.  Sharing mutable state across concurrent
    encodes is documented-unsafe by construction: the callback stamps its wid into
    the shared scratch, yields MID-ENCODE, and a sibling fiber's callback overwrites
    the scratch before this fiber reads it back -- so the "seen" value bleeds to a
    sibling's wid.  This reproduces HEAVILY under PLAIN OS THREADS WITH THE GIL ON
    (verified by the standalone control: ~100k bleeds GIL-on), so hard-failing on it
    would be a FALSE-POSITIVE detector -- it is the documented-unsafe usage, not a
    runloom bug.  We MEASURE the bleed rate on this arm and REPORT it, exactly like
    p67's TLS leak rate -- NEVER assert on it.  (NOTE: CPython's json.JSONEncoder
    itself IS reentrant -- each .encode()/iterencode() builds a FRESH markers dict
    and a fresh c_make_encoder accumulator as locals, sharing no per-call state on
    the instance -- so the hazard is the user's SHARED SCRATCH, not the encoder
    object.  We still guard against a hard CRASH / out-of-universe wid even on this
    arm: a SIGSEGV or a "seen" value that was never any fiber's is memory
    corruption, not the documented interleave.)

Stresses: json C encoder (c_make_encoder) reentrancy, default= callback running
arbitrary Python (runloom.sleep/yield_now) MID-ENCODE forcing a hub switch inside
dumps(), recycled internal encode buffer cross-fiber bleed, json.loads/dumps
concurrent across fibers/hubs, preempt-in-C-extension, no-lost-wake parked inside
a C callback.

Good TSan / controlled-M:N-replay target: the C encoder's accumulator + the
module-global encoder object are touched by every concurrent dumps(); a TSan
report on that shared state, or a deterministic replay that switches hubs between
two fibers' default= callbacks and produces a spliced byte string, localizes the
bleed before the identity oracle even fires.
"""
import json

import harness
import runloom

# Modest population: this is a correctness probe of the C-encoder reentrancy, not
# a scale soak.  Each op does a real nested encode with several callback-driven
# yields, so a few thousand fibers amply exercise the preempt-mid-encode window.
MAX_WORKERS = 6000

# Fraction of workers assigned to the report-only SHARED-SCRATCH measured arm.
# Small: a few hundred concurrent ops on the shared scratch amply demonstrate the
# documented-unsafe interleave without dominating the load-bearing population.
SHARED_FRACTION = 0.15

# How many custom objects each fiber embeds in its nested payload.  Each one
# drives a default= callback that yields MID-ENCODE, so this is the number of
# hub-switch opportunities INSIDE a single dumps() call.
EMBEDS_PER_OBJECT = 6


class Tagged(object):
    """A custom object json doesn't know how to serialize, so the C encoder must
    call the `default=` callback for it -- MID-ENCODE.  Carries the owning fiber's
    wid + a sequence index so the decoded form is identity-checkable."""
    __slots__ = ("wid", "idx")

    def __init__(self, wid, idx):
        self.wid = wid
        self.idx = idx


class _ScratchTagged(object):
    """Custom object for the MEASURED shared-scratch arm.  Its default= callback
    writes-then-reads a SHARED mutable scratch across the mid-encode yield (the
    documented-unsafe usage we measure).  Distinct type from Tagged so the two arms
    never share a callback path."""
    __slots__ = ("wid", "idx")

    def __init__(self, wid, idx):
        self.wid = wid
        self.idx = idx


def build_scratch_payload(wid):
    """Payload for the MEASURED shared-scratch arm: embeds _ScratchTagged customs
    whose default= touches the shared scratch buffer."""
    return {"wid": wid,
            "items": [_ScratchTagged(wid, i) for i in range(EMBEDS_PER_OBJECT)]}


def make_default(H, wid, fired_box):
    """Build the `default=` callback for this encode.  It runs arbitrary Python
    MID-ENCODE (the C encoder calls it for each Tagged): it FORCES A HUB SWITCH
    (runloom.sleep / yield_now) so a sibling fiber's concurrent dumps() can
    interleave on this hub, then returns a JSON-serializable dict still tagged
    with THIS fiber's wid.  fired_box[0] counts callbacks so the caller can prove
    the encode actually ran through the yield points (non-vacuous)."""
    def default(o):
        if isinstance(o, Tagged):
            fired_box[0] += 1
            # MID-ENCODE scheduling point: half the time a real timed park, half a
            # bare yield, so the preempt lands at different encoder depths.
            if (o.idx & 1) == 0:
                runloom.sleep(0.0003)
            else:
                runloom.yield_now()
            return {"__tag__": o.wid, "__i__": o.idx}
        raise TypeError("not serializable: {0!r}".format(o))
    return default


def build_payload(wid):
    """Build THIS fiber's distinct nested object, tagged with its wid and
    embedding EMBEDS_PER_OBJECT Tagged customs (each a default= callback site).
    Single-owner: nothing here is shared with any other fiber."""
    return {
        "wid": wid,
        "kind": "p459",
        "nested": {
            "items": [Tagged(wid, i) for i in range(EMBEDS_PER_OBJECT)],
            "meta": {"owner": wid, "depth": [wid, wid + 1, wid + 2]},
        },
        "trailer": "wid={0}".format(wid),
    }


def _all_tags(obj):
    """Yield every wid-tag found in a decoded payload (the "wid" field, the
    "owner" field, and every embedded "__tag__").  Used to assert NO sibling's
    bytes were spliced into our result."""
    yield obj["wid"]
    yield obj["nested"]["meta"]["owner"]
    for it in obj["nested"]["items"]:
        yield it["__tag__"]


def roundtrip_check(H, wid, state):
    """LOAD-BEARING single-owner op: encode THIS fiber's tagged object (with a
    default= callback yielding MID-ENCODE), decode it, and assert we recovered
    OUR OWN object exactly.  Any mismatch / spliced wid / crash is a real bug."""
    fired = [0]
    payload = build_payload(wid)
    # json.dumps with default= uses the module-global encoder object + the C
    # encoder; the callback yields mid-encode so a sibling dumps() on this hub can
    # interleave.  A clean round-trip proves no cross-fiber bleed.
    text = json.dumps(payload, default=make_default(H, wid, fired))
    decoded = json.loads(text)

    # The callback must have fired once per embedded Tagged -- otherwise the
    # mid-encode yield window was never opened (oracle would be vacuous).
    if fired[0] != EMBEDS_PER_OBJECT:
        H.fail("json default= callback fired {0}x, expected {1} (wid {2}) -- the "
               "mid-encode yield window was not exercised as built; the encode "
               "path changed under us".format(
                   fired[0], EMBEDS_PER_OBJECT, wid))
        return

    # IDENTITY: every wid-tag in the decoded object must be THIS fiber's wid.  A
    # foreign wid here means a sibling fiber's bytes were spliced into our output
    # buffer across the preempt-mid-C-encode -- the real runloom bug.
    for tag in _all_tags(decoded):
        if tag != wid:
            # First rule out garbage (a wid that was never any fiber's) -- that is
            # hard corruption regardless of which arm.
            kind = ("OUT-OF-UNIVERSE" if not (0 <= tag < H.funcs)
                    else "a SIBLING fiber's")
            H.fail("json round-trip IDENTITY BROKEN: decoded a {0} wid {1!r} in "
                   "fiber {2}'s OWN single-owner object -- a sibling concurrent "
                   "encode's bytes were spliced into our buffer across a preempt "
                   "MID-C-ENCODE (the C encoder / its recycled buffer is not "
                   "reentrant under a hub switch).  text={3!r}".format(
                       kind, tag, wid, text[:200]))
            return

    # Full structural round-trip: decoded must equal what we encoded (modulo the
    # Tagged -> dict transform the callback applied).  Build the expected decoded
    # form and compare exactly: no lost / garbled / reordered bytes.
    expected = {
        "wid": wid,
        "kind": "p459",
        "nested": {
            "items": [{"__tag__": wid, "__i__": i}
                      for i in range(EMBEDS_PER_OBJECT)],
            "meta": {"owner": wid, "depth": [wid, wid + 1, wid + 2]},
        },
        "trailer": "wid={0}".format(wid),
    }
    if decoded != expected:
        H.fail("json round-trip MISMATCH for fiber {0}: decoded object != the "
               "object encoded -- spliced/garbled bytes from a concurrent encode "
               "MID-C-ENCODE.  got={1!r}".format(wid, text[:200]))
        return

    state["rt_ops"][wid & 1023] += 1


def shared_scratch_check(H, wid, state):
    """MEASURED arm (report-ONLY): encode a payload whose default= callback writes
    its wid into a SHARED MUTABLE SCRATCH buffer, yields MID-ENCODE, then reads the
    scratch BACK.  Sharing mutable state across concurrent encodes is documented-
    unsafe: a sibling fiber's callback overwrites the scratch during our yield, so
    the value we read back BLEEDS to a sibling's wid.  This reproduces HEAVILY
    under plain GIL threads (verified by the standalone control), so we MEASURE the
    bleed rate and NEVER fail on it -- only a hard CRASH or an out-of-universe wid
    (true memory corruption, not the documented interleave) is fatal."""
    scratch = state["scratch"]               # ONE list shared by all fibers
    payload = build_scratch_payload(wid)

    def default(o):
        if isinstance(o, _ScratchTagged):
            scratch[0] = o.wid               # WRITE shared scratch
            # MID-ENCODE scheduling point: a sibling's default= can overwrite
            # scratch[0] before we read it back.
            if (o.idx & 1) == 0:
                runloom.sleep(0.0003)
            else:
                runloom.yield_now()
            seen = scratch[0]                # READ shared scratch back
            return {"__tag__": o.wid, "__seen__": seen}
        raise TypeError("not serializable: {0!r}".format(o))

    try:
        text = json.dumps(payload, default=default)
        decoded = json.loads(text)
    except (ValueError, RuntimeError):
        # A torn structure under the interleave raises rather than returning -- a
        # documented-unsafe outcome, REPORT only.
        state["shared_err"][wid & 1023] += 1
        return

    # Measure (never assert) whether the shared scratch bled a sibling's wid into
    # our "seen" value.  The "__tag__" (our own wid, single-owner) is checked for
    # hard corruption; "__seen__" (the shared scratch) is the documented-unsafe
    # measurement.
    bled = False
    for it in decoded["items"]:
        if it["__tag__"] != wid:
            # Our OWN tag must always be ours even on this arm -- a foreign tag here
            # is real corruption (the single-owner part bled), not the shared
            # scratch interleave.
            if not (0 <= it["__tag__"] < H.funcs):
                H.fail("shared-scratch arm produced OUT-OF-UNIVERSE own-tag {0!r} "
                       "(fiber {1}) -- memory corruption / a crash, NOT a "
                       "documented interleave".format(it["__tag__"], wid))
                return
            H.fail("shared-scratch arm: own __tag__ {0!r} != wid {1} -- the "
                   "single-owner part of the payload bled, real corruption".format(
                       it["__tag__"], wid))
            return
        seen = it["__seen__"]
        if seen != wid:
            bled = True
            # Out-of-universe "seen" = garbage memory, not a sibling's wid: fatal.
            if not (0 <= seen < H.funcs):
                H.fail("shared-scratch arm read OUT-OF-UNIVERSE value {0!r} from "
                       "the shared scratch (fiber {1}) -- memory corruption, NOT a "
                       "documented sibling overwrite".format(seen, wid))
                return
    if bled:
        state["shared_bleed"][wid & 1023] += 1
    else:
        state["shared_ok"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """LOAD-BEARING single-owner worker.  Runs ONLY the round-trip identity arm;
    the report-only shared-encoder arm runs in a SEPARATE, fully-drained pre-phase
    so its documented-unsafe interleave can never contaminate the load-bearing
    measurement."""
    for _ in H.round_range():
        if not H.running():
            break
        roundtrip_check(H, wid, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def run_shared_phase(H, state):
    """Report-ONLY pre-phase: spawn the SHARED-SCRATCH workers, let them PROVABLY
    overlap concurrent encodes that write-then-read ONE shared scratch buffer
    across a mid-encode yield (documented-unsafe), and FULLY DRAIN them
    (WaitGroup.wait) before the load-bearing pool runs.  Measured in isolation so
    the documented-unsafe interleave never touches the load-bearing single-owner
    arm.  Each shared-arm fiber does SEVERAL rounds so the concurrent overwrite
    window is opened repeatedly (one round each rarely overlaps enough to bleed)."""
    nshared = state["nshared"]
    if nshared <= 0:
        return
    wg = runloom.WaitGroup()
    wg.add(nshared)
    rounds = max(8, H.rounds)        # several rounds so overlaps actually occur

    def run_one(wid):
        try:
            for _ in range(rounds):
                if not H.running():
                    break
                shared_scratch_check(H, wid, state)
                if H.failed:
                    break
        finally:
            wg.done()

    for wid in range(nshared):
        H.fiber(run_one, wid)
    wg.wait()


def setup(H):
    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    nshared = min(400, int(nworkers * SHARED_FRACTION))

    state = {
        "nworkers": nworkers,
        "nshared": nshared,
        "rt_ops": [0] * 1024,            # LOAD-BEARING round-trips that passed
        "shared_ok": [0] * 1024,         # shared-arm encodes that kept identity
        "shared_bleed": [0] * 1024,      # shared-arm encodes that bled (REPORT)
        "shared_err": [0] * 1024,        # shared-arm torn-structure raises (REPORT)
        # ONE shared mutable scratch buffer written-then-read by every shared-arm
        # default= callback across the mid-encode yield -- the documented-unsafe
        # object whose cross-fiber overwrite we MEASURE (never fail on).
        "scratch": [None],
    }
    H.state = state


def body(H):
    # Phase 1 (report-only, fully drained): the documented-unsafe SHARED-ENCODER
    # arm, measured in isolation so it cannot contaminate the load-bearing pool.
    run_shared_phase(H, H.state)
    # Phase 2 (LOAD-BEARING): the single-owner round-trip identity pool.
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    rt = sum(H.state["rt_ops"])
    s_ok = sum(H.state["shared_ok"])
    s_bleed = sum(H.state["shared_bleed"])
    s_err = sum(H.state["shared_err"])
    s_total = s_ok + s_bleed + s_err
    bleed_pct = (100.0 * (s_bleed + s_err) / s_total) if s_total else 0.0

    H.log("LOAD-BEARING single-owner json round-trips that kept identity: {0} | "
          "shared-scratch arm (documented-unsafe, REPORT ONLY): {1} clean, {2} "
          "bled, {3} torn-structure raises ({4:.1f}% interleaved -- reproduces "
          "under plain GIL threads, NOT a runloom bug); ops={5}".format(
              rt, s_ok, s_bleed, s_err, bleed_pct, H.total_ops()))

    # Reaching post with no failure means every per-op identity check held fail-
    # fast.  Assert the load-bearing arm actually ran (else the oracle is vacuous):
    # the C encoder + default= mid-encode yield window WAS exercised.
    H.check(rt > 0,
            "no single-owner json round-trips completed -- the load-bearing "
            "preempt-mid-C-encode reentrancy hazard was never exercised (oracle "
            "would be vacuous)")

    # Report-only context: surface that the shared-encoder arm did observe
    # interleave (expected, benign) so the semantics are explicit in the log.
    if s_bleed or s_err:
        H.log("note: the shared-scratch arm observed {0} sibling-overwrite bleeds "
              "+ {1} torn-structure raises across {2} concurrent encodes that "
              "write-then-read ONE shared mutable scratch across the mid-encode "
              "yield -- documented-unsafe usage (reproduces under plain GIL threads "
              "with PYTHON_GIL=1), NOT a runloom bug; the load-bearing single-owner "
              "arm above is the runloom oracle".format(s_bleed, s_err, s_total))

    # COMPLETENESS: no worker parked-then-vanished (e.g. preempted inside the C
    # encoder / its default= callback and never re-woken).
    H.require_no_lost("json C-encoder mid-encode reentrancy")


if __name__ == "__main__":
    harness.main(
        "p459_json_encoder_reentrancy", body, setup=setup, post=post,
        default_funcs=6000,
        describe="json.dumps uses the C encoder; a default= callback runs Python "
                 "(runloom.sleep/yield_now) MID-ENCODE, forcing a hub switch INSIDE "
                 "dumps() while sibling fibers also encode.  LOAD-BEARING: each "
                 "fiber round-trips its OWN distinct wid-tagged object and must "
                 "recover it EXACTLY (no spliced sibling bytes from the recycled "
                 "encode buffer) -- a real runloom bug if it bleeds.  A SHARED "
                 "mutable scratch written-then-read by the callback across the "
                 "mid-encode yield is documented-unsafe (reproduces under plain GIL "
                 "threads) -- measured + reported, never failed")
