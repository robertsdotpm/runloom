"""big_100 / 468 -- reprlib.recursive_repr false-suppression under M:N.

reprlib.recursive_repr keys its per-decorator recursion guard by

    key = (id(self), get_ident())

and stashes that key in a `repr_running` set for the duration of the decorated
__repr__, removing it on the way out.  The guard exists so a genuinely RECURSIVE
structure (a list that contains itself) reprs as the fillvalue ('...') instead of
recursing forever: the SECOND entry for the SAME (object, thread) sees the key
already present and returns the fillvalue.

WHERE M:N BREAKS IT (the gap this program catches).  Under runloom's M:N
scheduler many fibers ("goroutines") share ONE hub OS-thread, so they all report
the SAME get_ident().  While fiber A is mid-repr of a SHARED object X -- its key
(id(X), hub_ident) live in repr_running -- and yields at a scheduling point, a
SIBLING fiber B on the same hub that reprs the SAME X computes the IDENTICAL key,
finds it already present, and is FALSELY suppressed to the fillvalue -- even
though B is NOT recursing.  The recursion guard fired with no recursion.  This is
the shared-hub-identity class: the get_ident()-keyed guard assumes one logical
control-flow per (object, ident), which holds for genuine OS threads but NOT for
M:N fibers multiplexed onto one hub thread (the same root cause as p66's
contextvar leak and p67's threading.local).

This is a runloom M:N-SPECIFIC gap: stdlib reprlib is CORRECT under genuine
OS-thread semantics (distinct get_ident() per thread).  Verified with a
standalone plain-threads control (same shared-object logic, NO runloom): 0 false
suppressions under PYTHON_GIL=1 AND PYTHON_GIL=0 -- each OS thread keys the guard
by its own get_ident(), so a sibling thread's live key never collides.  The fix
therefore lives in runloom (a fiber-local recursion guard / a fiber-aware
recursive_repr that keys by the running FIBER, not get_ident()), not in stdlib
reprlib, which is correct under real OS-thread semantics.

LOAD-BEARING INVARIANT / WHY THE ORACLE IS NON-VACUOUS.  The objects under test
are deliberately NON-RECURSIVE: each Node wraps a flat finite tuple of ints, so a
CORRECT top-level repr is ALWAYS the full bracketed string "R[tag|e0,e1,...]" and
the fillvalue can ONLY ever appear via FALSE suppression.  A non-recursing fiber
whose repr(shared_obj) EQUALS the fillvalue (or contains it) means the recursion
guard fired when there was no recursion -- a wrong value a real program
(logging / debugging / __repr__ of a shared structure) would actually emit.  We
inject a runloom scheduling point INSIDE the decorated body, AFTER reprlib has
added the key to repr_running but BEFORE it discards it (via a per-call handle
stashed on the instance), so siblings reliably interleave in the live-key window.

ARMS:
  * LOAD-BEARING -- SHARED arm (worker, HARD, fail-fast).  A small pool of SHARED
    Node objects is hammered by all fibers: same id(self) + same hub get_ident()
    => key collision.  The oracle: a non-recursing fiber's repr(shared_obj) MUST
    EQUAL the precomputed full string and MUST NEVER be (or contain) the
    fillvalue.  got == fillvalue => H.fail "false recursion suppression" -- the
    runloom bug.  (On a CORRECT runtime -- and plain threads, GIL on AND off --
    this NEVER fires, so the program exits 0 when there is no bug.)
  * PRIVATE CONTROL -- PRIVATE arm (worker, MEASURED, must stay 0%).  Each fiber
    reprs its OWN private Node (distinct id(self)) at the SAME yield, so even with
    a shared hub get_ident() the (id, ident) key never collides.  This proves the
    gap is the shared-(id, ident) key, not scheduling noise: it stays 0% false
    suppression.  We MEASURE + REPORT it; it must be clean (a private-arm hit
    would mean the bug is something other than the shared-key collision).

FAIL ON: a non-recursing repr of a SHARED object collapsing to (or containing)
the fillvalue, or any other wrong/torn repr value.  The private-control arm is
report-only and is expected to stay 0% -- a non-zero private rate is itself a
fail (it would mean the mechanism is not the shared-key collision).

EXPECTED RESULT: this catches a real, currently-UNFIXED runloom bug, so under
runloom M:N the program is EXPECTED to FAIL exit 1 with the false-fillvalue
diagnostic (it is a bug-catcher, like p460's sibling oracles).  The fix is a
fiber-local recursion guard in runloom; until then the SHARED arm fires.

Stresses: reprlib.recursive_repr's get_ident()-keyed recursion guard across hub
fibers, the (id(self), get_ident()) key colliding for siblings on one hub thread,
a scheduling point inside the live-key window of the decorated __repr__.
"""
import reprlib

import harness
import runloom

FILLVALUE = "<FILL>"

# Flat, finite, NON-recursive payload size: a correct repr is ALWAYS the full
# bracketed form, so the fillvalue can only ever appear via FALSE suppression.
N_ELEMS = 6

# A SMALL shared pool so many fibers overlap on the same object -> same id(self),
# and (on one hub) the same get_ident() => the (id, ident) key collides.  Small
# enough that fibers reliably contend on one object; the pool exists only so the
# load is not pinned to a single object.
POOL_SIZE = 8


class Node(object):
    """A NON-recursive node whose decorated __repr__ yields while its
    (id(self), get_ident()) key is live in reprlib's repr_running set.  The yield
    is published per-call via a handle on the instance so __repr__ can reach the
    caller's rng without recursion mutating any shared decorator state."""

    __slots__ = ("tag", "elems", "yield_handle")

    def __init__(self, tag, n):
        self.tag = tag
        self.elems = tuple(range(n))     # flat, finite, NON-recursive
        # Per-call handle the caller sets so __repr__ can yield inside the guard
        # window.  A 1-slot list so the published rng is a plain attribute read
        # (no recursion into any shared decorator state).
        self.yield_handle = [None]

    @reprlib.recursive_repr(fillvalue=FILLVALUE)
    def __repr__(self):
        # We are now INSIDE the guard: reprlib has added (id(self), get_ident())
        # to repr_running.  Yield HERE -- after the key is live, before reprlib
        # discards it -- so a sibling fiber sharing this hub's get_ident() can run
        # and (if it reprs THIS object) observe the live key and be falsely
        # suppressed.  On genuine OS threads the sibling has a DIFFERENT
        # get_ident(), so its key never collides and this is a no-op for it.
        rng = self.yield_handle[0]
        if rng is not None:
            if rng.random() < 0.5:
                runloom.yield_now()
            else:
                runloom.sleep(0.0003)
        body = ",".join(str(e) for e in self.elems)
        return "R[{0}|{1}]".format(self.tag, body)


def expected_repr(node):
    """The one CORRECT top-level repr of a NON-recursive Node: always the full
    bracketed form, NEVER the fillvalue."""
    body = ",".join(str(e) for e in node.elems)
    return "R[{0}|{1}]".format(node.tag, body)


def make_pool():
    return [Node("S%d" % i, N_ELEMS) for i in range(POOL_SIZE)]


def setup(H):
    H.state = {
        "shared_pool": make_pool(),
        # SHARED-arm (LOAD-BEARING) counters.
        "shared_checks": [0] * 1024,    # non-recursing reprs of a SHARED object
        "false_fill": [0] * 1024,       # collapsed to the fillvalue (the bug)
        "torn": [0] * 1024,             # contains the fillvalue but is not bare
        "wrong": [0] * 1024,            # any other wrong value
        # PRIVATE-control-arm (MEASURED) counters -- must stay 0% false_fill.
        "priv_checks": [0] * 1024,
        "priv_false_fill": [0] * 1024,
        # first observed bad sample, for the diagnostic.
        "sample": [None],
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: SHARED object.  Many fibers repr the SAME Node (same id +
# same hub get_ident() => key collision).  A non-recursing repr that equals (or
# contains) the fillvalue is the false-suppression bug -- fail fast.
# --------------------------------------------------------------------------
def shared_check(H, wid, state):
    obj = state["shared_pool"][wid % POOL_SIZE]
    # Publish our rng so __repr__ yields inside the live-key window.  Single
    # writer per call; the value read by __repr__ is whichever rng was last
    # published, which is always a valid rng (never garbage) -- the yield cadence
    # is the only thing that varies, never correctness.
    obj.yield_handle[0] = _rng_for(wid)
    want = expected_repr(obj)
    got = repr(obj)
    state["shared_checks"][wid & 1023] += 1
    if got == want:
        return
    if got == FILLVALUE:
        # The WHOLE repr collapsed to the fillvalue: reprlib's recursion guard
        # fired with NO recursion (the object is a flat tuple).  This is the
        # false-suppression signature -- the runloom M:N bug.
        state["false_fill"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "shared", got)
        H.fail("reprlib.recursive_repr FALSE SUPPRESSION: repr of a NON-recursive "
               "SHARED object collapsed to the fillvalue {0!r} (wid {1}, expected "
               "{2!r}) -- a sibling fiber on this hub was mid-repr of the SAME "
               "object, so the (id(self), get_ident()) key was already live in "
               "repr_running and this NON-recursing fiber was falsely suppressed.  "
               "M:N fibers share one hub get_ident(); the recursion guard fired "
               "with no recursion (the runloom shared-hub-identity bug -- 0 under "
               "plain threads).".format(FILLVALUE, wid, want))
        return
    if FILLVALUE in got:
        # A torn value that embeds the fillvalue -- also corruption.
        state["torn"][wid & 1023] += 1
        if state["sample"][0] is None:
            state["sample"][0] = (wid, "shared", got)
        H.fail("reprlib.recursive_repr TORN repr: NON-recursive SHARED object "
               "repr {0!r} embeds the fillvalue {1!r} (wid {2}, expected {3!r}) -- "
               "a sibling's live recursion-guard key corrupted this repr "
               "(runloom shared-hub-identity bug).".format(
                   got, FILLVALUE, wid, want))
        return
    # Any other mismatch is still a corruption of a closed-world-correct value.
    state["wrong"][wid & 1023] += 1
    if state["sample"][0] is None:
        state["sample"][0] = (wid, "shared", got)
    H.fail("reprlib.recursive_repr WRONG repr: NON-recursive SHARED object repr "
           "{0!r} != expected {1!r} (wid {2}) -- the repr of a flat, finite "
           "object was corrupted under M:N.".format(got, want, wid))


# --------------------------------------------------------------------------
# PRIVATE control arm: each fiber reprs its OWN Node (distinct id(self)).  Even
# with a shared hub get_ident(), the (id, ident) key cannot collide across
# fibers -- so this MUST stay 0% false suppression.  MEASURED + reported; a hit
# here would mean the gap is NOT the shared-(id, ident) key.  A private hit is
# still a fail because it would invalidate the attribution.
# --------------------------------------------------------------------------
def private_check(H, wid, state):
    obj = Node("P%d" % wid, N_ELEMS)            # distinct id(self) per fiber
    obj.yield_handle[0] = _rng_for(wid)
    want = expected_repr(obj)
    got = repr(obj)
    state["priv_checks"][wid & 1023] += 1
    if got == want:
        return
    # A private-arm fillvalue would break the attribution (distinct id => no
    # cross-fiber key collision is possible), so it is a real fail.
    state["priv_false_fill"][wid & 1023] += 1
    if state["sample"][0] is None:
        state["sample"][0] = (wid, "private", got)
    H.fail("reprlib.recursive_repr PRIVATE-control CORRUPTION: a fiber's OWN "
           "private object (distinct id(self)) repr'd as {0!r} != expected {1!r} "
           "(wid {2}) -- the private control MUST stay clean (no cross-fiber key "
           "collision is possible with distinct ids).  A hit here means the "
           "mechanism is NOT the shared-(id, get_ident()) key, invalidating the "
           "attribution.".format(got, want, wid))


# Per-worker deterministic rng cache so each call publishes a valid rng to
# __repr__ without re-deriving one every iteration.  Keyed by wid; single-writer.
_RNG_CACHE = {}


def _rng_for(wid):
    rng = _RNG_CACHE.get(wid)
    if rng is None:
        import random
        rng = random.Random(0x9E3779B1 * (wid + 1))
        _RNG_CACHE[wid] = rng
    return rng


# The false-suppression hazard only manifests under SUSTAINED churn -- many
# fibers simultaneously mid-repr of a shared object and PARKED across the yield,
# so the scheduler reliably runs a sibling inside the live-key window before this
# fiber resumes.  A single repr per fiber barely overlaps a sibling's.  So each
# worker runs a sustained internal loop (one shared-arm check + one private-arm
# check per iteration) bounded by H.running() -- which makes the load-bearing
# oracle fire at the DEFAULT --rounds 1.  INNER_CAP stops one worker from
# monopolizing teardown on a slow box.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING SHARED check
    (fail-fast on a false fillvalue) and the MEASURED PRIVATE control (must stay
    clean).  The two share only the yield cadence -- the SHARED arm contends on
    the small pool (key collision); the PRIVATE arm uses a fresh object each time
    (distinct id, no collision), isolating the shared-key mechanism."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            shared_check(H, wid, state)         # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            private_check(H, wid, state)        # MEASURED control (must stay 0%)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    schecks = sum(H.state["shared_checks"])
    ff = sum(H.state["false_fill"])
    torn = sum(H.state["torn"])
    wrong = sum(H.state["wrong"])
    pchecks = sum(H.state["priv_checks"])
    pff = sum(H.state["priv_false_fill"])
    ffpct = (100.0 * ff / schecks) if schecks else 0.0
    ppct = (100.0 * pff / pchecks) if pchecks else 0.0
    sample = H.state["sample"][0]
    H.log("reprlib[shared LOAD-BEARING]: {0} checks  false_fill={1} ({2:.2f}%)  "
          "torn={3}  wrong={4}  sample={5}".format(
              schecks, ff, ffpct, torn, wrong, sample))
    H.log("reprlib[private CONTROL]: {0} checks  false_fill={1} ({2:.2f}%) -- "
          "MUST stay 0% (distinct id(self) => no cross-fiber key collision; "
          "proves the gap is the shared-(id, get_ident()) key, not scheduling "
          "noise)".format(pchecks, pff, ppct))
    if ff or torn or wrong:
        H.log("note: the SHARED arm observed false recursion suppression -- "
              "reprlib.recursive_repr keys its guard by (id(self), get_ident()), "
              "and runloom M:N fibers share one hub get_ident(), so a sibling "
              "mid-repr of the same object makes this NON-recursing fiber emit the "
              "fillvalue.  This is a runloom M:N gap (0 under plain threads GIL on "
              "AND off); the fix is a fiber-local recursion guard in runloom, not "
              "in stdlib reprlib (correct under real OS-thread semantics).")
    # NON-VACUITY: the load-bearing shared hazard was actually exercised.
    H.check(schecks > 0,
            "no shared-object reprs ran -- the load-bearing false-suppression "
            "hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-repr (stranded inside the
    # decorated body on a never-delivered wake).
    H.require_no_lost("reprlib.recursive_repr false-suppression")


if __name__ == "__main__":
    harness.main(
        "p468_reprlib_recursive_repr", body, setup=setup, post=post,
        default_funcs=8000,
        describe="reprlib.recursive_repr keys its recursion guard by "
                 "(id(self), get_ident()); runloom M:N fibers share one hub "
                 "get_ident(), so while one fiber is mid-repr of a SHARED "
                 "NON-recursive object (its key live in repr_running) a sibling "
                 "repr'ing the SAME object is FALSELY suppressed to the fillvalue "
                 "-- the recursion guard fires with no recursion.  LOAD-BEARING: a "
                 "non-recursing fiber's repr(shared_obj) MUST equal the full "
                 "bracketed string and NEVER the fillvalue (0 under plain threads "
                 "GIL on AND off; the shared-(id, get_ident()) collision is the "
                 "runloom bug).  PRIVATE control (distinct id) stays 0% -- proves "
                 "the mechanism.  Same class as p66/p67; fix is a fiber-local "
                 "recursion guard in runloom")
