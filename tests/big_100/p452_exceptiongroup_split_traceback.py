"""big_100 / 452 -- BaseExceptionGroup.split()/subgroup traceback-chain conservation under M:N.

The subject is CPython's exception-group machinery: BaseExceptionGroup.split(),
.subgroup(), and the C reraise* path (_PyExc_PrepReraiseStar / the
exceptiongroup split implementation in Objects/exceptions.c / Lib/traceback-
adjacent C).  split(condition) and subgroup(condition) do NOT copy the leaf
exceptions -- they REBUILD new group objects that SHARE each matched leaf's
identity and its ``__traceback__`` (a PyTracebackObject ``tb_next`` singly-linked
list), and they re-derive the group's runtime type via ``__class_getitem__`` /
``derive`` while walking the ``_exceptions`` tuple.  Concretely the non-atomic
internal state under attack is:

  * each leaf's ``PyTracebackObject->tb_next`` linked-list pointer chain -- shared
    by the original group, the matched split half, the unmatched split half, and
    any subgroup, because split() does NOT clone the traceback; it hands the SAME
    PyTracebackObject* head to every result group;
  * the group's ``_exceptions`` tuple (the ``ob_item`` array of leaf pointers)
    that split walks to partition, and re-derives a NEW tuple for each half;
  * the leaves' ``__context__`` / ``__cause__`` slots that a traceback/context
    walk chases.

The M:N hazard (the precise racing op pair): one fiber runs group.split(cond)
(or subgroup) -- it walks each leaf's ``tb_next`` chain and re-derives the group,
and it may PARK mid-rebuild on a grown-down C stack (the derive() / tuple-build
allocates, the allocator can hand off) -- WHILE a sibling fiber on ANOTHER hub
either (a) walks the SAME leaves' tracebacks (traceback.extract_tb over tb_next /
chases __context__), or (b) does a SECOND split of the same group.  A torn
``tb_next`` write gives a CYCLIC or broken traceback chain -- an infinite walk or
a freed-frame dereference (SIGSEGV); a torn ``_exceptions`` ob_item read yields a
leaf OUTSIDE the closed universe (a pointer from a freed/rehashed slot).

We make that a CLOSED-WORLD CONSERVATION + IDENTITY law, not a racy probe.

  Finite sentinel UNIVERSE of leaf TAGS.  Each round constructs exactly K leaf
  exceptions of two MARKER types (MarkA / MarkB), each carrying a distinct
  universe tag and a KNOWN-DEPTH traceback (raised through a fixed-depth recursion
  ``descend(exc, DEPTH)`` so every leaf's ``tb_next`` chain has the SAME, exactly
  computed length BASE_TB_LEN).  It builds one BaseExceptionGroup over those K
  leaves, then spawns sibling fibers on different hubs:

    * a SPLITTER fiber calls group.split(MarkA) -> (match, rest), parking once
      mid-rebuild (yield_now after tripping a gate) so a sibling's walk/second
      split provably lands DURING the rebuild;
    * a WALKER fiber walks every leaf's tb_next chain (traceback.extract_tb +
      a manual bounded tb_next traversal) and chases __context__/__cause__,
      asserting every leaf tag it sees is in UNIVERSE and every chain length is
      <= a hard cap (a torn/cyclic tb_next would loop past the cap -> caught, not
      hung);
    * a SECOND-SPLITTER fiber re-splits the SAME group by the other marker.

  INVARIANT (hot, fail-fast), checked once the round is quiescent (siblings
  joined):
    * CONSERVATION + IDENTITY: the two split halves PARTITION the K leaves with
      NO leaf lost and NONE duplicated -- by object id, matched + unmatched == the
      original K leaf objects exactly (closed universe of identities);
    * every leaf the splitter/walker ever saw is one of the K constructed leaves
      (its tag in UNIVERSE) -- a torn ob_item read yields an out-of-universe tag;
    * every result leaf's traceback length == BASE_TB_LEN -- a torn tb_next gives
      a wrong (short) or LOOPING (capped) length;
    * the marker partition is correct: every leaf in ``match`` is a MarkA and
      every leaf in ``rest`` is a MarkB (a torn split that mis-files a leaf is a
      partition corruption even when count is conserved);
    * the walker never dereferenced a freed frame (no SIGSEGV -- the faulthandler/
      watchdog catches that) and saw only universe leaves.

  CONTROL ARM (single-owner, race-free by construction): the SAME round ALSO does
  a build+split+subgroup ENTIRELY within ONE fiber (no sibling touches it).  A
  single-owner exception group split must partition exactly K leaves with each
  traceback at exactly BASE_TB_LEN -- so if the CONTROL loses/duplicates a leaf or
  reports a wrong depth, the fault is in CPython's exception-group machinery
  itself, NOT M:N contention.  The shared/contended arm is the contention probe;
  the private control arm is the falsifier that disambiguates "split() is buggy"
  from "M:N contention tore the tb_next/_exceptions".

  COVERAGE (the flaky-random lesson p125/p126/p172 already had to fix): three
  split CASES (split-by-A / split-by-B / subgroup) are round-robined by worker id
  in the FIRST ops (``sel = (wid + i) % NCASES``) then random, so every case is
  exercised whether one worker does K ops or K workers do 1 each.

Stresses: BaseExceptionGroup.split()/subgroup tb_next chain sharing under M:N,
_exceptions ob_item tuple re-derive racing a concurrent traceback walk / second
split, leaf-identity conservation (no lost/duplicated leaf), closed-universe tag
membership, known-depth traceback preservation (torn/cyclic tb_next), marker
partition correctness, __context__/__cause__ chase over shared leaves.

Good TSan / controlled-M:N-replay target: the split rebuild's read of a leaf's
tb_next / the _exceptions ob_item array, racing the walker's tb_next traversal
on another hub, is a textbook shared-pointer data race; a TSan report on the
PyTracebackObject->tb_next or the tuple ob_item write/read localizes the tear
before the conservation/identity assert even closes.
"""
import sys
import traceback

import harness
import runloom


# Finite sentinel UNIVERSE of leaf TAGS.  A leaf whose tag is NOT in this set is a
# torn/corrupted leaf read out of a freed _exceptions ob_item slot -- a hard
# fault.  Sized larger than K so a torn read landing on a stale adjacent tag is
# very likely OUT of universe (recognizable), and recognizable as an int constant.
UNIVERSE_SIZE = 4096
UNIVERSE_BASE = 0x45200000
UNIVERSE = tuple(UNIVERSE_BASE + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Leaves per group per round.  Even so the two markers split exactly K/2 each.
# Big enough that the _exceptions tuple is a real array to partition (and to push
# the split's re-derived tuples through allocation), small enough that many rounds
# complete under the timeout.
K = 16

# Fixed recursion depth each leaf is raised through, so every leaf's tb_next chain
# has the SAME, exactly-computable length.  A leaf whose traceback length differs
# from BASE_TB_LEN has a torn (short) or cyclic (capped-out) tb_next chain.
DEPTH = 6

# Hard cap on a tb_next / __context__ walk.  A correct chain is BASE_TB_LEN long;
# a torn/cyclic tb_next would loop forever -- we bound the walk at this cap and
# treat exceeding it as a CORRUPTION (a looping chain), never a hang.
TB_WALK_CAP = 256
CTX_WALK_CAP = 64

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The split CASES.  post() asserts each was exercised, so the worker round-robins
# them by id in its first ops (NOT random -- pure random reliably MISSES a case at
# low op-count under load, the flaky-coverage bug the suite already had to fix in
# p125/p126/p172).
CASE_SPLIT_A = 0      # group.split(MarkA)  -> (A-leaves, B-leaves)
CASE_SPLIT_B = 1      # group.split(MarkB)  -> (B-leaves, A-leaves)
CASE_SUBGROUP = 2     # group.subgroup(MarkA) then group.subgroup(MarkB)
NCASES = 3


class MarkA(Exception):
    """Marker leaf type A.  split(MarkA) selects exactly these."""


class MarkB(Exception):
    """Marker leaf type B.  split(MarkB) selects exactly these."""


def descend(exc, depth):
    """Raise `exc` through a fixed-depth recursion so the resulting leaf carries a
    tb_next chain whose length is a deterministic constant (BASE_TB_LEN).  A torn
    tb_next under M:N split gives a DIFFERENT length, which the oracle catches."""
    if depth <= 0:
        raise exc
    descend(exc, depth - 1)


def make_leaf(cls, tag):
    """Construct one leaf of marker type `cls` carrying universe `tag`, with a
    KNOWN-DEPTH traceback (raised through descend()).  The tag is stashed as the
    first arg so the walker/oracle can recover it; a leaf whose recovered tag is
    not in UNIVERSE is a torn read."""
    try:
        descend(cls(tag), DEPTH)
    except Exception as e:                  # noqa: BLE001 - we WANT the leaf object
        return e


def tb_len(exc, cap=TB_WALK_CAP):
    """Length of a leaf's tb_next linked list, walked manually with a HARD CAP.

    Returns the chain length, or cap+1 if the walk hit the cap (a torn/cyclic
    tb_next that would otherwise loop forever -- bounded, never a hang).  This is
    the raw PyTracebackObject->tb_next traversal that races the splitter's
    rebuild."""
    n = 0
    tb = exc.__traceback__
    while tb is not None:
        n += 1
        if n > cap:
            return cap + 1                  # looping / torn chain -- bounded
        tb = tb.tb_next
    return n


def leaf_tag(exc):
    """Recover a leaf's universe tag from its args.  Returns None if it doesn't
    look like one of our marker leaves (which itself is an out-of-universe
    signal)."""
    a = getattr(exc, "args", None)
    if not a:
        return None
    return a[0]


# Computed once at import on a clean (single-thread) build: the canonical tb_next
# length of a leaf raised through descend(.., DEPTH).  Every correctly-preserved
# leaf -- in the original group, in either split half, in a subgroup -- must have
# exactly this length.
BASE_TB_LEN = tb_len(make_leaf(MarkA, UNIVERSE[0]))


def build_group(rng):
    """Build K leaves (K/2 MarkA, K/2 MarkB), each with a DISTINCT universe tag and
    a known-depth traceback, and wrap them in one BaseExceptionGroup.

    Returns (group, leaves, id_to_kind) where leaves is the flat list of the K
    constructed leaf objects (the closed universe of IDENTITIES for this round),
    and id_to_kind maps id(leaf) -> MarkA/MarkB so the partition can be checked.
    Tags are drawn WITHOUT replacement from UNIVERSE so every leaf is unique and a
    duplicated-id leaf in a split half is unambiguously a doubling."""
    tags = rng.sample(UNIVERSE, K)
    leaves = []
    id_to_kind = {}
    for i, tag in enumerate(tags):
        cls = MarkA if (i & 1) == 0 else MarkB
        leaf = make_leaf(cls, tag)
        leaves.append(leaf)
        id_to_kind[id(leaf)] = cls
    group = BaseExceptionGroup("round-group", leaves)
    return group, leaves, id_to_kind


def split_half_leaves(half):
    """Flatten a split-result group (which may be None or nested) to its leaf
    exceptions, in order.  split() over a flat group yields a flat group, but we
    recurse defensively so a (legal) nested rebuild is still handled."""
    out = []
    if half is None:
        return out
    for e in half.exceptions:
        if isinstance(e, BaseExceptionGroup):
            out.extend(split_half_leaves(e))
        else:
            out.append(e)
    return out


def check_leaf_universe(H, leaf, orig_ids, label):
    """Validate one leaf seen anywhere (split half / walker / subgroup): its tag is
    in UNIVERSE, it is one of the constructed identities, and its traceback length
    is exactly BASE_TB_LEN.  Returns False on the first violation."""
    tag = leaf_tag(leaf)
    if tag not in UNIVERSE_SET:
        H.fail("{0}: leaf with OUT-OF-UNIVERSE tag {1!r} (type {2}) -- a torn "
               "_exceptions ob_item read handed back a leaf outside the closed "
               "universe of K constructed leaves".format(
                   label, tag, type(leaf).__name__))
        return False
    if id(leaf) not in orig_ids:
        H.fail("{0}: leaf tag {1!r} is in the tag universe but its OBJECT id is "
               "not one of the K constructed leaves -- split rebuilt/aliased a "
               "leaf identity (torn _exceptions slot)".format(label, tag))
        return False
    n = tb_len(leaf)
    if n != BASE_TB_LEN:
        if n > TB_WALK_CAP:
            H.fail("{0}: leaf tag {1!r} traceback walk exceeded cap {2} -- a "
                   "CYCLIC/torn tb_next chain (split re-linked tb_next into a "
                   "loop); would be an infinite walk".format(
                       label, tag, TB_WALK_CAP))
        else:
            H.fail("{0}: leaf tag {1!r} tb_next length {2} != known depth {3} -- "
                   "a torn tb_next pointer (split dropped/relinked a frame)".format(
                       label, tag, n, BASE_TB_LEN))
        return False
    return True


def walk_contexts(H, leaf, orig_ids, label):
    """Chase a leaf's __context__/__cause__ chain (bounded) -- the slots the C
    reraise path links.  Every exception-group node we pass through must stay in
    the closed universe; a cycle/torn link is bounded by CTX_WALK_CAP (never a
    hang).  Returns False on a violation."""
    seen = 0
    cur = leaf
    while cur is not None:
        seen += 1
        if seen > CTX_WALK_CAP:
            H.fail("{0}: __context__/__cause__ chain exceeded cap {1} -- a "
                   "cyclic/torn context link from the split rebuild".format(
                       label, CTX_WALK_CAP))
            return False
        nxt = getattr(cur, "__cause__", None)
        if nxt is None:
            nxt = getattr(cur, "__context__", None)
        cur = nxt
    return True


def walker(H, group, leaves, orig_ids, done_ch):
    """Loop walking every leaf's tb_next chain (traceback.extract_tb + the manual
    bounded traversal) and __context__/__cause__ chain WHILE the splitter rebuilds
    the same group on another hub.  Asserts every leaf stays in the closed universe
    and every chain length is bounded/exact.  Holds NO lock -- this is the
    walk-vs-rebuild race.  Exits once the splitters signal done.

    A SIGSEGV here (freed-frame deref off a torn tb_next) is the crash the
    watchdog/faulthandler catches; an out-of-universe tag or a capped/looping
    chain is a fail-fast invariant break."""
    while True:
        for leaf in leaves:
            if not check_leaf_universe(H, leaf, orig_ids, "walker"):
                return
            # traceback.extract_tb walks the SAME tb_next chain through the C
            # path -- the op that dereferences each frame (a freed frame would
            # SIGSEGV here).  Bounded by `limit` so a torn/cyclic chain can't loop.
            try:
                frames = traceback.extract_tb(leaf.__traceback__, limit=TB_WALK_CAP)
            except Exception as exc:        # noqa: BLE001
                H.fail("walker: traceback.extract_tb over a leaf raised {0}: {1} "
                       "-- a torn tb_next chain under concurrent split rebuild"
                       .format(type(exc).__name__, exc))
                return
            # extract_tb must see exactly the known depth (it walked the same chain).
            if len(frames) != BASE_TB_LEN:
                H.fail("walker: extract_tb yielded {0} frames != known depth {1} "
                       "for leaf tag {2!r} -- torn/relinked tb_next under "
                       "concurrent split".format(
                           len(frames), BASE_TB_LEN, leaf_tag(leaf)))
                return
            if not walk_contexts(H, leaf, orig_ids, "walker"):
                return
            if H.failed:
                return
        # Also walk the group's own _exceptions tuple (the ob_item the split reads).
        for e in split_half_leaves(group):
            if not check_leaf_universe(H, e, orig_ids, "walker-group"):
                return
        if done_ch.try_recv() is not None:
            break
        runloom.yield_now()                 # hand off so a split lands mid-walk


def do_split_case(H, group, leaves, id_to_kind, orig_ids, case, gate):
    """Run ONE split case on the SHARED group, parking once mid-rebuild (after
    tripping `gate`) so the sibling walk/second split lands during the park.
    Validates the partition + identity conservation of the RESULT.  Returns the
    (matched_leaves, unmatched_leaves) by identity, or None on failure."""
    # Trip the gate just before split parks, so the walker/second-splitter run
    # DURING the rebuild window.
    gate.done()
    runloom.yield_now()

    if case == CASE_SPLIT_A:
        match, rest = group.split(MarkA)
        want_match = MarkA
        want_rest = MarkB
    elif case == CASE_SPLIT_B:
        match, rest = group.split(MarkB)
        want_match = MarkB
        want_rest = MarkA
    else:  # CASE_SUBGROUP -- subgroup(A) is the "match" half, subgroup(B) the rest
        match = group.subgroup(MarkA)
        rest = group.subgroup(MarkB)
        want_match = MarkA
        want_rest = MarkB

    mleaves = split_half_leaves(match)
    rleaves = split_half_leaves(rest)

    # Every result leaf is in the closed universe (tag + identity) with an exact
    # traceback depth.
    for leaf in mleaves:
        if not check_leaf_universe(H, leaf, orig_ids, "split-match"):
            return None
    for leaf in rleaves:
        if not check_leaf_universe(H, leaf, orig_ids, "split-rest"):
            return None

    # Partition correctness: every match leaf is the matched marker type, every
    # rest leaf the other (a torn split that mis-files a leaf is corruption even
    # if counts are conserved).
    for leaf in mleaves:
        if id_to_kind.get(id(leaf)) is not want_match:
            H.fail("split mis-filed a leaf: tag {0!r} of kind {1} landed in the "
                   "{2}-match half -- the condition walk read a torn _exceptions "
                   "slot / mismatched leaf".format(
                       leaf_tag(leaf), type(leaf).__name__, want_match.__name__))
            return None
    for leaf in rleaves:
        if id_to_kind.get(id(leaf)) is not want_rest:
            H.fail("split mis-filed a leaf: tag {0!r} of kind {1} landed in the "
                   "{2}-rest half".format(
                       leaf_tag(leaf), type(leaf).__name__, want_rest.__name__))
            return None
    return mleaves, rleaves


def check_conservation(H, leaves, mleaves, rleaves, label):
    """CONSERVATION + IDENTITY: the two halves partition the K original leaves with
    no leaf lost and none duplicated, by OBJECT ID.  Returns False on violation."""
    orig_ids = set(id(x) for x in leaves)
    m_ids = [id(x) for x in mleaves]
    r_ids = [id(x) for x in rleaves]
    all_ids = m_ids + r_ids

    # No duplicate across or within the halves.
    if len(all_ids) != len(set(all_ids)):
        H.fail("{0}: a leaf is DUPLICATED across the split halves "
               "(match={1} + rest={2} leaves, {3} distinct) -- split aliased a "
               "leaf into both halves (torn _exceptions rebuild)".format(
                   label, len(m_ids), len(r_ids), len(set(all_ids))))
        return False
    seen = set(all_ids)
    # No leaf lost: the union of the halves is exactly the original K identities.
    if seen != orig_ids:
        lost = orig_ids - seen
        extra = seen - orig_ids
        H.fail("{0}: leaf-identity conservation broken -- {1} original leaf(s) "
               "LOST {2}, {3} out-of-universe leaf(s) gained {4} (match+rest must "
               "exactly partition the K={5} constructed leaves)".format(
                   label, len(lost), sorted(lost)[:4], len(extra),
                   sorted(extra)[:4], K))
        return False
    # And the count is exactly K.
    if len(all_ids) != K:
        H.fail("{0}: match+rest == {1} leaves, expected exactly K={2} -- a leaf "
               "was lost or doubled by split".format(label, len(all_ids), K))
        return False
    return True


def control_arm(H, rng):
    """SINGLE-OWNER CONTROL: build a group, split it, and subgroup it ENTIRELY
    within this one fiber -- no sibling touches it, so it is race-free by
    construction.  A single-owner exception-group split MUST partition exactly K
    leaves with each traceback at exactly BASE_TB_LEN; if it does NOT, the fault is
    in CPython's exception-group machinery itself, not M:N contention.  Returns
    True on success."""
    group, leaves, id_to_kind = build_group(rng)
    orig_ids = set(id(x) for x in leaves)

    match, rest = group.split(MarkA)
    mleaves = split_half_leaves(match)
    rleaves = split_half_leaves(rest)
    for leaf in mleaves + rleaves:
        if not check_leaf_universe(H, leaf, orig_ids, "control-split"):
            return False
    if not check_conservation(H, leaves, mleaves, rleaves, "control-split"):
        return False
    # Marker partition in the control: A's in match, B's in rest, exactly K/2 each.
    if len(mleaves) != K // 2 or len(rleaves) != K // 2:
        H.fail("control split unbalanced: match={0} rest={1}, expected K/2={2} "
               "each (a single-owner split lost/mis-filed a leaf -- exception-"
               "group machinery corruption, NOT contention)".format(
                   len(mleaves), len(rleaves), K // 2))
        return False
    for leaf in mleaves:
        if not isinstance(leaf, MarkA):
            H.fail("control split mis-filed a leaf into the MarkA half (kind "
                   "{0}) -- machinery corruption".format(type(leaf).__name__))
            return False
    for leaf in rleaves:
        if not isinstance(leaf, MarkB):
            H.fail("control split mis-filed a leaf into the MarkB half (kind "
                   "{0}) -- machinery corruption".format(type(leaf).__name__))
            return False

    # subgroup control: subgroup(A)+subgroup(B) re-partition the same K leaves.
    sa = split_half_leaves(group.subgroup(MarkA))
    sb = split_half_leaves(group.subgroup(MarkB))
    if not check_conservation(H, leaves, sa, sb, "control-subgroup"):
        return False
    return True


def run_round_impl(H, wid, rng, slot, state):
    """One round: build a shared group, run a round-robined split CASE on it under
    a concurrent walker + second-splitter on other hubs, check conservation +
    identity + depth + partition; PLUS a fully single-owner control arm.

    Siblings join on a WaitGroup before the post-round oracle reads the result, so
    the group is provably quiescent (no fiber still mutating it)."""
    case = state["case_for"](wid, slot)

    # ---- CONTROL ARM (single-owner, race-free) --------------------------------
    if not control_arm(H, rng):
        return
    state["control"][slot] += 1
    if H.failed:
        return

    # ---- CONTENDED SHARED ARM -------------------------------------------------
    group, leaves, id_to_kind = build_group(rng)
    orig_ids = set(id(x) for x in leaves)

    # gate: the primary splitter trips it the instant before it parks; the walker
    # and second-splitter wait on it so their work provably lands inside the
    # rebuild park window.
    gate = runloom.WaitGroup()
    gate.add(1)
    done_ch = runloom.Chan(1)
    wg = runloom.WaitGroup()
    wg.add(3)                               # splitter + walker + second-splitter

    result = {"halves": None}

    def run_splitter():
        try:
            r = do_split_case(H, group, leaves, id_to_kind, orig_ids, case, gate)
            result["halves"] = r
        except Exception as exc:            # noqa: BLE001
            H.error(wid, exc)
        finally:
            done_ch.send(True)              # tell the walker to stop
            wg.done()

    def run_walker():
        try:
            gate.wait()                     # land DURING the split rebuild
            walker(H, group, leaves, orig_ids, done_ch)
        except Exception as exc:            # noqa: BLE001
            H.error(wid, exc)
        finally:
            wg.done()

    def run_second_splitter():
        try:
            gate.wait()                     # a SECOND split of the SAME group,
            # racing the primary split's tb_next / _exceptions rebuild.
            other = MarkB if case != CASE_SPLIT_B else MarkA
            m2 = group.subgroup(other)
            for leaf in split_half_leaves(m2):
                if not check_leaf_universe(H, leaf, orig_ids, "second-split"):
                    break
        except Exception as exc:            # noqa: BLE001
            H.error(wid, exc)
        finally:
            wg.done()

    H.fiber(run_splitter)
    H.fiber(run_walker)
    H.fiber(run_second_splitter)
    wg.wait()                               # all siblings joined -> group quiescent

    if H.failed:
        return

    halves = result["halves"]
    if halves is None:
        # The splitter failed (already recorded) or produced nothing; nothing more
        # to assert.
        return
    mleaves, rleaves = halves

    # ---- closed-world conservation + identity (round now quiescent) -----------
    if not check_conservation(H, leaves, mleaves, rleaves,
                              "shared-split case {0}".format(case)):
        return

    state["shared"][slot] += 1
    state["case_seen"][case][slot] += 1


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
    # Built INSIDE the root (monkey.patch() already ran).  Round-robin the split
    # CASE by worker id in the first ops, then random -- so every case is covered
    # whether one worker does many ops or many workers do one each (the
    # p125/p126/p172 flaky-coverage fix).  We track per-(wid,op) selection via a
    # tiny per-slot op counter.
    op_i = [0] * SLOTS

    def case_for(wid, slot):
        i = op_i[slot]
        op_i[slot] = i + 1
        if i < NCASES:
            return (wid + i) % NCASES
        # After the first NCASES ops, derive deterministically from wid+i (still
        # not a shared RNG -- single-writer per slot).
        return (wid * 2654435761 + i) % NCASES

    H.state = {
        "case_for": case_for,
        "control": [0] * SLOTS,             # control-arm rounds completed
        "shared": [0] * SLOTS,              # contended-arm rounds completed
        "case_seen": [[0] * SLOTS for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    control = sum(H.state["control"])
    shared = sum(H.state["shared"])
    seen = [sum(H.state["case_seen"][c]) for c in range(NCASES)]
    H.log("control-arm rounds={0} shared-arm rounds={1} case_seen={2} "
          "BASE_TB_LEN={3} ops={4}".format(
              control, shared, seen, BASE_TB_LEN, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # Reaching post with no failure already proves every per-round conservation +
    # identity + depth + partition check held fail-fast; assert the run actually
    # exercised BOTH arms (else the law was vacuous).
    H.check(control > 0,
            "control arm never ran -- the single-owner split falsifier was not "
            "exercised")
    H.check(shared > 0,
            "contended shared arm never ran -- the split-vs-walk race window was "
            "never exercised")

    # Every split CASE was exercised (deterministic round-robin guarantees it once
    # enough ops ran).
    for c in range(NCASES):
        H.check(seen[c] > 0,
                "split case {0} ({1}) was never exercised -- coverage gap".format(
                    c, ("split-A", "split-B", "subgroup")[c]))

    H.require_no_lost("exceptiongroup-split conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p452_exceptiongroup_split_traceback", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many hubs split/subgroup a shared BaseExceptionGroup (sharing "
                 "the leaves' tb_next chains + _exceptions tuple) while a sibling "
                 "walks the same leaves' tracebacks / second-splits; closed-world "
                 "law: match+rest partition exactly K leaves by identity (none "
                 "lost/duplicated), every leaf tag in a finite universe, every "
                 "traceback at the known depth, correct marker partition -- a torn "
                 "tb_next (wrong/looping depth), torn _exceptions (out-of-universe "
                 "leaf), lost/duplicated leaf, or SIGSEGV fails; a single-owner "
                 "control arm falsifies machinery corruption vs contention")
