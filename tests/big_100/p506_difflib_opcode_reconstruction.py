"""big_100 / 506 -- difflib.SequenceMatcher opcode reconstruction law under M:N.

difflib.SequenceMatcher lazily builds and CACHES several pieces of per-instance
derived state the first time they are requested:

  * b2j  -- a dict mapping each element of the second sequence `b` to the list of
    indices at which it occurs (built in __init_b2j, filtered by autojunk / the
    junk predicate).  Built lazily on set_seq2 / set_seqs.
  * matching_blocks -- the list of (i, j, n) triples produced by the recursive
    find_longest_match walk, cached in self.matching_blocks on first
    get_matching_blocks().
  * opcodes -- the (tag, i1, i2, j1, j2) 5-tuples derived from matching_blocks,
    cached in self.opcodes on first get_opcodes().

Every one of these is filled ON DEMAND and then STORED on the instance, and the
opcode/matching-block builders read b2j and each other's half-built results while
they run.  Under free-threaded 3.14t with hubs>1 and tens of thousands of
goroutines, the hazard this program probes is a HUB MIGRATION landing in the
middle of one of those lazy fills:

  * a fiber forces get_opcodes(), the recursive find_longest_match walk parks
    (cooperative yield inside the runtime) with matching_blocks only partially
    appended, resumes on a DIFFERENT hub, and either publishes a HALF-BUILT
    opcode/matching-block list, or -- if the per-instance cache slots were not
    isolated -- SPLICES a sibling matcher's blocks into this instance;
  * the cached opcodes then no longer describe a valid a->b transform: applying
    them to `a` fails to reconstruct `b`, or references an index outside a/b, or
    the second get_opcodes() returns a DIFFERENT object than the first cached one.

WHICH ORACLE IS LOAD-BEARING, AND WHY (a true closed-world conservation law):

  Each fiber owns its OWN SequenceMatcher over a FIBER-LOCAL pair (a, b) whose
  elements are drawn from a UNIQUE per-wid namespace (every value is
  wid*STRIDE + k, so a sibling fiber's element can NEVER equal one of ours -- a
  spliced-in foreign block is detectable by value alone).  The DEFINING property
  of a correct opcode list is the RECONSTRUCTION LAW:

      applying the opcodes to `a` -- copy a[i1:i2] on 'equal', substitute
      b[j1:j2] on 'replace', insert b[j1:j2] on 'insert', drop a[i1:i2] on
      'delete' -- MUST rebuild `b` EXACTLY.

  This is a conservation law (no element created, lost, or foreign), not a racy
  probe: it holds for ANY valid opcode list difflib ever produces, on GIL-on and
  GIL-off plain threads alike (verified: a standalone 8-thread control where each
  thread reconstructs its own private (a,b) reconstructs b 100% of the time, 0
  mismatches, GIL on and off).  Because the matcher and both sequences are
  single-owner (fiber-local, never shared), a reconstruction MISMATCH, an
  out-of-range opcode index, an 'equal' block whose a[i1:i2] != b[j1:j2], a
  matching-block element-sum that disagrees with the 'equal' opcode span sum, or
  a second get_opcodes() returning a non-identical list, can ONLY be a runloom
  cache-isolation / half-built-publish desync -- NOT documented Python behavior.
  The load-bearing oracle PASSES on a correct runtime (program exits 0).

ORACLES:
  * LOAD-BEARING -- OPCODE RECONSTRUCTION (worker, HARD, fail-fast).  For each
    fiber-local (a, b):
      - force get_opcodes() (op1) and get_matching_blocks() (mb1) -- both fill
        and cache the instance's lazy state;
      - YIELD (yield_now / tiny sleep) so a sibling's own lazy fill interleaves
        on this or another hub;
      - re-request get_opcodes() (op2) and get_matching_blocks() (mb2): assert
        op2 IS op1 and mb2 IS mb1 (the cache slot was not rebuilt or replaced by
        a sibling's list) and that they are equal element-for-element;
      - RECONSTRUCTION LAW: apply op2 to a and assert the result == b exactly;
      - for every 'equal' opcode assert a[i1:i2] == b[j1:j2] (self-consistency of
        the block against both sequences);
      - MATCHING-BLOCK CONSISTENCY: sum of n over matching blocks == sum of
        (i2-i1) over 'equal' opcodes (both count matched positions); final
        matching block is exactly (len(a), len(b), 0).
    Single-owner: the matcher and (a, b) live only in this fiber's frame; a
    failure is a runloom SequenceMatcher-cache desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    half-built find_longest_match / opcode fill never returns; the watchdog +
    require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (diff_checks > 0)
    and conserved a positive number of reconstructed b-elements.

FAIL ON: a reconstructed sequence that differs from b, an out-of-range opcode
index, an 'equal' block that disagrees between a and b, a matching-block/opcode
span-sum mismatch, a wrong final matching block, or a second get_opcodes()/
get_matching_blocks() returning a different (or unequal) cached object.  Every
one of these is a torn/half-built/spliced per-instance cache -- a real runtime
bug -- because the matcher and its sequences are single-owner.

Stresses: SequenceMatcher lazy b2j / matching_blocks / opcodes cache fill and
publication across a yield + hub migration, find_longest_match recursion parked
mid-walk, get_opcodes()/get_matching_blocks() identity caching, per-instance
cache isolation between concurrently-diffing fibers, and the opcode->reconstruction
conservation law over fiber-local unique-namespace sequences.

Good TSan / controlled-M:N-replay target: the lazy self.matching_blocks /
self.opcodes / self.b2j assignments are per-instance Python attribute writes read
back by the same instance's builders; a data-race report on one of those slots, or
a deterministic replay that resumes find_longest_match on a foreign hub with a
half-appended block list, localizes the desync before the reconstruction sum even
closes.
"""
import difflib

import harness
import runloom

# Each fiber's sequence elements are integers drawn ONLY from
# [wid*STRIDE, wid*STRIDE + SPAN).  STRIDE > SPAN guarantees the per-wid
# namespaces are DISJOINT, so any element from a sibling matcher spliced into
# this fiber's cache is out-of-namespace and shows up as a reconstruction
# mismatch (its value can never coincide with one of ours).
STRIDE = 1000000
SPAN = 48

# Fiber-local sequence length band.  Long enough that find_longest_match recurses
# several levels (so a mid-walk park has real half-built state) and the backing
# b2j dict grows through rehash boundaries; short enough that many checks run
# under the timeout.
LEN_LO = 40
LEN_HI = 90

# Sustained checks per worker, bounded by H.running().  The lazy-cache desync
# hazard only manifests under SUSTAINED churn: many fibers simultaneously forcing
# opcode fills while sleep-PARKED across their yield, so the scheduler reliably
# interleaves a sibling's fill before this fiber resumes.  A single check per
# fiber barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_pair(rng, wid):
    """Build a FIBER-LOCAL (a, b) pair over wid's UNIQUE integer namespace.

    `b` is derived from `a` by random edits (keep / delete / replace / insert),
    all substituted/inserted values drawn from the SAME per-wid namespace, so the
    opcode reconstruction of b from a is non-trivial (a real mix of equal /
    replace / insert / delete blocks) yet every element is provably ours."""
    base = wid * STRIDE
    la = rng.randint(LEN_LO, LEN_HI)
    a = [base + rng.randrange(SPAN) for _ in range(la)]

    b = []
    i = 0
    while i < len(a):
        roll = rng.random()
        if roll < 0.15:                    # delete: drop a[i]
            i += 1
        elif roll < 0.30:                  # replace: swap a[i] for another local value
            b.append(base + rng.randrange(SPAN))
            i += 1
        elif roll < 0.45:                  # insert: add a local value, keep a[i]
            b.append(base + rng.randrange(SPAN))
        else:                              # keep a[i]
            b.append(a[i])
            i += 1
    # A few trailing inserts sometimes, so b can be longer than a.
    for _ in range(rng.randrange(4)):
        b.append(base + rng.randrange(SPAN))

    return tuple(a), tuple(b)


def apply_opcodes(a, b, opcodes):
    """Apply difflib opcodes to `a` to reconstruct `b` (the conservation law).

    'equal'   -> copy a[i1:i2]   (must equal b[j1:j2])
    'replace' -> substitute b[j1:j2]
    'insert'  -> insert b[j1:j2]
    'delete'  -> drop a[i1:i2]

    Returns (reconstructed_tuple, bad_equal_block_or_None)."""
    out = []
    bad_equal = None
    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            if a[i1:i2] != b[j1:j2] and bad_equal is None:
                bad_equal = (i1, i2, j1, j2)
            out.extend(a[i1:i2])
        elif tag == "replace":
            out.extend(b[j1:j2])
        elif tag == "insert":
            out.extend(b[j1:j2])
        elif tag == "delete":
            pass
        else:                              # pragma: no cover - difflib never emits others
            if bad_equal is None:
                bad_equal = ("BADTAG", tag, i1, i2)
    return tuple(out), bad_equal


def diff_check(H, wid, idx, state):
    """Single-owner opcode reconstruction check on a fiber-local matcher+pair.

    A cache desync (half-built or sibling-spliced opcodes/matching_blocks) makes
    the reconstruction fail to rebuild b, or breaks the identity/consistency
    invariants."""
    rng = state["rng_seed"][wid]
    a, b = build_pair(rng, wid)

    sm = difflib.SequenceMatcher(None, a, b, autojunk=False)

    # Force the lazy caches to fill (matching_blocks first, then opcodes derived
    # from it).  Record the cached objects so we can assert identity after a yield.
    mb1 = sm.get_matching_blocks()
    op1 = sm.get_opcodes()

    # YIELD: let siblings force their own lazy fills on this / another hub while
    # this instance's cache slots are populated.  If cache slots were not isolated,
    # a resume could observe a rebuilt or spliced list.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # Re-request: on a correct runtime the cache is returned verbatim -- SAME
    # object (identity) -- and never rebuilt.
    mb2 = sm.get_matching_blocks()
    op2 = sm.get_opcodes()

    if op2 is not op1:
        H.fail("get_opcodes() cache IDENTITY CHANGED across a yield (wid {0}): "
               "second call returned a different list object -- the per-instance "
               "opcode cache was rebuilt or replaced (possibly by a sibling "
               "matcher) under M:N".format(wid))
        return
    if mb2 is not mb1:
        H.fail("get_matching_blocks() cache IDENTITY CHANGED across a yield "
               "(wid {0}): the per-instance matching_blocks cache was rebuilt "
               "or replaced under M:N".format(wid))
        return
    if op2 != op1 or mb2 != mb1:
        H.fail("SequenceMatcher cache VALUE CHANGED across a yield (wid {0}): "
               "cached opcodes/matching_blocks are no longer element-equal to "
               "the first fill -- a torn/half-built cache".format(wid))
        return

    # RECONSTRUCTION LAW: applying the cached opcodes to `a` must rebuild `b`.
    recon, bad_equal = apply_opcodes(a, b, op2)

    if bad_equal is not None:
        H.fail("opcode block SELF-INCONSISTENT (wid {0}): {1} -- an 'equal' "
               "block's a-slice != its b-slice (or a bad tag), so the cached "
               "opcodes do not describe a valid a->b transform -- a spliced / "
               "half-built opcode list".format(wid, bad_equal))
        return

    if recon != b:
        H.fail("RECONSTRUCTION LAW BROKEN (wid {0}): applying cached opcodes to "
               "a rebuilt a sequence of len {1} that is NOT b (len {2}) -- the "
               "opcode list does not conserve b (a torn/spliced/half-built "
               "SequenceMatcher cache under M:N)".format(wid, len(recon), len(b)))
        return

    # MATCHING-BLOCK CONSISTENCY: matched-position count via matching_blocks must
    # equal the count via 'equal' opcodes (both describe the SAME matched cells).
    mb_matched = 0
    for bi, bj, bn in mb2:
        # every block must sit inside both sequences.
        if bi < 0 or bj < 0 or bi + bn > len(a) or bj + bn > len(b):
            H.fail("matching block OUT OF RANGE (wid {0}): block (i={1}, j={2}, "
                   "n={3}) exceeds a(len {4})/b(len {5}) -- a corrupted "
                   "matching_blocks entry".format(wid, bi, bj, bn, len(a), len(b)))
            return
        if a[bi:bi + bn] != b[bj:bj + bn]:
            H.fail("matching block MISMATCH (wid {0}): a[{1}:{2}] != b[{3}:{4}] "
                   "-- the matching_blocks entry claims a match that is not one "
                   "(torn/spliced cache)".format(
                       wid, bi, bi + bn, bj, bj + bn))
            return
        mb_matched += bn

    op_matched = 0
    for tag, i1, i2, j1, j2 in op2:
        if tag == "equal":
            op_matched += (i2 - i1)

    if mb_matched != op_matched:
        H.fail("matching-block / opcode SPAN-SUM DISAGREE (wid {0}): "
               "matching_blocks total n={1} != 'equal' opcode span sum {2} -- "
               "the opcodes and matching_blocks caches describe DIFFERENT match "
               "sets (a half-built derivation)".format(wid, mb_matched, op_matched))
        return

    # Final matching block sentinel must be exactly (len(a), len(b), 0).
    last = mb2[-1]
    if last != (len(a), len(b), 0):
        H.fail("final matching-block SENTINEL WRONG (wid {0}): got {1}, expected "
               "({2}, {3}, 0) -- the matching_blocks list was truncated or "
               "corrupted".format(wid, last, len(a), len(b)))
        return

    # Conserve: one successful reconstruction, len(b) elements rebuilt exactly.
    state["diff_checks"][wid] += 1
    state["elems"][wid] += len(b)


def worker(H, wid, rng, state):
    """Sustained single-owner reconstruction churn.  Each iteration builds a fresh
    fiber-local matcher+pair, forces its lazy caches, yields (parking siblings
    mid-fill), then verifies identity + reconstruction + block consistency."""
    # One private RNG per wid, seeded from the harness-derived rng, stored in a
    # per-wid slot (single-owner) so pair generation is deterministic per fiber
    # and never shares RNG state across fibers.
    state["rng_seed"][wid] = H.derive("difflib", wid)
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            diff_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        # LOAD-BEARING conservation counters: ONE slot per worker (wid-indexed,
        # single-writer-per-slot, race-free -- never wid & MASK for a conservation
        # tally).  Allocated here where H.funcs is known.
        "diff_checks": [0] * H.funcs,      # successful reconstructions per wid
        "elems": [0] * H.funcs,            # b-elements reconstructed exactly per wid
        "rng_seed": [None] * H.funcs,      # per-wid private RNG (single-owner)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["diff_checks"])
    elems = sum(H.state["elems"])
    H.log("difflib opcode-reconstruction[single-owner LOAD-BEARING]: {0} "
          "reconstruction-law checks (all passed fail-fast), {1} b-elements "
          "rebuilt EXACTLY from cached opcodes; ops={2}".format(
              checks, elems, H.total_ops()))

    # NON-VACUITY: the load-bearing reconstruction arm actually ran and conserved
    # a positive number of elements (else the law was vacuous).
    H.check(checks > 0,
            "no opcode-reconstruction checks ran -- the SequenceMatcher lazy-"
            "cache desync hazard was never exercised (oracle would be vacuous)")
    H.check(elems > 0,
            "zero b-elements reconstructed -- the reconstruction conservation "
            "law never conserved anything (vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a half-
    # built find_longest_match / opcode fill).
    H.require_no_lost("difflib opcode reconstruction")


if __name__ == "__main__":
    harness.main(
        "p506_difflib_opcode_reconstruction", body, setup=setup, post=post,
        default_funcs=5000,
        describe="each fiber owns its own difflib.SequenceMatcher over a fiber-"
                 "local (a, b) drawn from a UNIQUE per-wid integer namespace; it "
                 "forces the lazy matching_blocks/opcodes caches, YIELDS (parking "
                 "siblings mid-fill), then asserts the conservation law: applying "
                 "the cached opcodes to a rebuilds b EXACTLY, the second "
                 "get_opcodes()/get_matching_blocks() return the IDENTICAL cached "
                 "object, every 'equal' block agrees between a and b, and the "
                 "matching-block/opcode matched-position sums agree.  A "
                 "reconstruction mismatch, out-of-range index, cache-identity "
                 "change, or span-sum disagreement is a torn/half-built/spliced "
                 "per-instance SequenceMatcher cache under M:N -- a real runtime "
                 "bug, since the matcher and sequences are single-owner")
