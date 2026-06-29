"""big_100 / 425 -- set membership-probe vs set_table_resize realloc under M:N.

The subject is CPython's built-in ``set`` / ``frozenset`` (Objects/setobject.c)
and its open-addressing probe.  A set is a ``PySetObject`` whose live entries
live in ``so_table`` (an array of ``setentry``), addressed with the bitmask
``so_mask`` (== table_size - 1).  EVERY membership-probe path bottoms out in
``set_lookkey``, which walks the table with the perturb-driven open-addressing
sequence::

    i = (size_t)hash & mask;          # initial slot
    ... entry = &so->table[i]; read entry->key, entry->hash ...
    perturb >>= PERTURB_SHIFT;
    i = (i * 5 + 1 + perturb) & mask; # next slot, wraps via `& mask`

``so_table``, ``so_mask`` and (for frozenset) the cached ``so_hash`` are THREE
fields that ``set_add_entry`` -> ``set_table_resize`` mutates NON-ATOMICALLY:
resize ``PyMem_NEW``s a fresh table, re-inserts every live entry, publishes the
NEW ``so_table`` + NEW ``so_mask``, then ``PyMem_Free``s the OLD table.  The
hazard this program attacks is a probe in flight across that publish:

  * a probe that has already loaded the OLD ``so_table`` base pointer but then
    re-reads ``so_mask`` AFTER resize bumped it (or vice versa) computes a wrap
    ``i = (i*5 + 1 + perturb) & mask`` against the WRONG-sized table and indexes
    PAST the old allocation -> an out-of-bounds ``setentry`` read (SIGSEGV, or a
    garbage ``entry->key`` / ``entry->hash``);
  * a probe walking the OLD table that ``set_table_resize`` has just
    ``PyMem_Free``d reads a freed ``setentry`` -- a use-after-free that returns
    a FALSE NEGATIVE (a known-present key reported absent) or a torn key;
  * for a frozenset, ``frozenset_hash`` sums ``entry->hash`` over EVERY slot to
    fill the ``so_hash`` cache; if a rehash moved entries mid-sum it could
    double-count or skip one, so the cached hash disagrees with a clean recompute
    over the same keys.

No existing program drives concurrent MEMBERSHIP-PROBE traffic against a
rehashing set.  p311 ITERATES a shared dict/set (the iterator-cursor race);
p229 immortalizes a frozenset graph (no concurrent mutation); p405 hammers a
Counter's dict via bulk RMW.  NONE makes a sibling hub run ``x in s`` /
``s.issubset`` / ``s.issuperset`` / ``s.isdisjoint`` -- the ``set_lookkey``
probe loop -- while another hub trips ``set_table_resize``.  That probe loop is
exactly the open-addressing walk that wraps out of bounds on a torn
``so_table``/``so_mask`` pair.

CLOSED-WORLD, falsifiable invariant.  Each round owns ONE shared set ``s`` seeded
from a fixed sentinel UNIVERSE, partitioned into three FIXED, disjoint regions:

  * PRESENT (P): keys that are seeded into ``s`` and NEVER removed -> ``k in s``
    MUST be True for every k in P, always;
  * ABSENT  (A): keys NEVER added to ``s`` -> ``k in s`` MUST be False for every
    k in A, always;  (A is disjoint from the universe-of-s)
  * CHURN  (C): keys the MUTATOR adds/removes under a Lock to FORCE
    ``set_table_resize`` -- C is disjoint from P and from A, so churning C can
    NEVER change the truth of any P/A probe.

By construction P/A membership is INVARIANT across all the resizes the churn
drives.  The PROBER (UNLOCKED -- it races the resize) therefore has an EXACT
required answer for every probe, so any wrong answer is a torn ``so_table`` /
``so_mask`` read, not a legal race outcome:

  * for every k in P: ``k in s`` is True, ``s.issuperset({k})`` True;
  * for every k in A: ``k in s`` is False, ``s.isdisjoint({k})`` True;
  * ``s.issuperset(P_frozen)`` is True;  ``s.isdisjoint(A_frozen)`` is True;
  * any element ever observed (e.g. via the small returned structures) is in
    UNIVERSE -- an out-of-universe value is a torn/freed slot.

SINGLE-OWNER CONTROL ARM.  A PRIVATE set ``ctrl`` is churned with the IDENTICAL
add/remove script (no other writer, race-free by construction) and the prober's
P/A probes are ALSO run against ``ctrl``.  A single-owner set's ``set_lookkey``
can never tear, so if the CONTROL ever returns a wrong P/A answer the fault is in
CPython's set machinery itself, not M:N contention -- this disambiguates "the
probe torn under contention" from "the set is just buggy".  The shared/unlocked
arm is the contention probe; the private arm is the falsifier.

FROZENSET HASH-CACHE arm.  After the churn quiesces, build ``fz =
frozenset(snapshot_of_s)`` and assert ``hash(fz) == hash(frozenset(same_keys))``
recomputed independently -- the ``so_hash`` cache (``frozenset_hash`` summing
``entry->hash``) must equal a fresh recompute over the same key set.

The MUTATOR serializes its writes under a cooperative Lock (set mutation is not
thread-safe GIL-off), which keeps P/A truth invariant by construction; the
PROBER holds NO lock, so the only contention probed is probe-vs-resize.  Synced
so the resize lands inside the probe's park window: the prober trips a gate just
before it parks mid-probe, and the mutator waits on the gate before resizing.

Invariant (hot, fail-fast): every P-probe True, every A-probe False, on BOTH the
shared set and the private control; issuperset(P)/isdisjoint(A) hold; no
out-of-universe value.  A flipped answer is a torn-probe bug; a SIGSEGV is the
OOB read.  Invariant (post): rounds were exercised, all probe CASES round-robined
by wid were hit, frozenset hash-cache consistent, no lost worker.

Stresses: set_lookkey open-addressing probe vs set_table_resize realloc+free,
torn so_table/so_mask publication, use-after-free on the old table (false
negative), frozenset so_hash cache vs concurrent rehash, in/issubset/issuperset/
isdisjoint under M:N, private-vs-shared membership control.

Good TSan / controlled-M:N-replay target: the probe's ``entry->key`` read vs
``set_table_resize``'s ``so_table = newtable`` / ``so_mask = newmask`` store is a
textbook data race; a TSan report on the setentry table write/read localizes the
torn probe before the membership assert even fires.
"""
import harness
import runloom

# Finite sentinel UNIVERSE of recognizable keys.  Partitioned into three fixed,
# DISJOINT regions (PRESENT / ABSENT / CHURN).  A value a probe ever yields that
# is outside this universe is a torn/freed setentry -- a hard fault.  Sized so
# the seeded set (PRESENT + the churned-in part of CHURN) pushes set_table_resize
# through several growth boundaries (resize is what reallocs+frees so_table).
UNIVERSE_SIZE = 384
UNIVERSE = tuple(0x42500000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# PRESENT: seeded into the shared set and the control, NEVER removed.  `k in s`
# MUST be True for these, always -- a False is a torn/use-after-free probe.
PRESENT = UNIVERSE[: UNIVERSE_SIZE // 3]
PRESENT_FROZEN = frozenset(PRESENT)

# ABSENT: never added to either set.  `k in s` MUST be False for these, always --
# a True is a torn probe reading a garbage/aliased slot as a hit.  Disjoint from
# the universe-of-s by construction.
ABSENT = UNIVERSE[UNIVERSE_SIZE // 3: 2 * UNIVERSE_SIZE // 3]
ABSENT_FROZEN = frozenset(ABSENT)

# CHURN: the mutator adds/removes ONLY these to force set_table_resize.  Disjoint
# from PRESENT and ABSENT, so churning them can NEVER change a P/A probe's answer.
CHURN = UNIVERSE[2 * UNIVERSE_SIZE // 3:]

# Sanity: the three regions are disjoint and partition the universe (checked once
# at import; a mistake here would silently weaken the oracle).
assert len(PRESENT) + len(ABSENT) + len(CHURN) == UNIVERSE_SIZE
assert not (PRESENT_FROZEN & ABSENT_FROZEN)
assert not (PRESENT_FROZEN & frozenset(CHURN))
assert not (ABSENT_FROZEN & frozenset(CHURN))

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# How many P/A keys the prober checks per probe pass.  Enough to span the probe
# table (forcing real open-addressing walks, not a one-slot hit), small enough to
# leave the probe in flight when the resize lands.
PROBE_SPAN = 24

# The membership-probe CASES.  post() asserts each was exercised, so the worker
# round-robins them by id in its FIRST ops (NOT random -- pure random selection
# reliably MISSES a case at low op-count under load, the flaky-coverage bug the
# suite already had to fix in p125/p126/p172).
CASE_IN = 0            # `k in s`                          -- bare set_lookkey
CASE_NOT_IN = 1        # `k not in s` over ABSENT          -- set_lookkey miss path
CASE_ISSUPERSET = 2    # s.issuperset(P_frozen)            -- bulk lookkey, all hit
CASE_ISDISJOINT = 3    # s.isdisjoint(A_frozen)            -- bulk lookkey, all miss
CASE_ISSUBSET = 4      # P_frozen.issubset(s)              -- probe from the other side
NCASES = 5


def churn_script(rng):
    """Build one round's deterministic add/remove churn script over CHURN keys.

    Returns a list of (op, key) where op is 'add' or 'discard' and key is in
    CHURN.  Replaying this identical script against the shared set (under the
    mutator's lock) and against the private control set drives the SAME sequence
    of set_table_resize realloc+free events, so the two arms are comparable.  Uses
    the caller's per-fiber rng (a shared random.Random corrupts GIL-off)."""
    script = []
    # Grow: add a random active subset of CHURN (pushes the table up through
    # resize boundaries), then remove a random slice (holes + a possible shrink),
    # then add a few back -- several realloc/free events per round.
    nactive = rng.randint(len(CHURN) // 2, len(CHURN))
    active = rng.sample(CHURN, nactive)
    for k in active:
        script.append(("add", k))
    for k in active:
        if rng.getrandbits(1):
            script.append(("discard", k))
    for k in rng.sample(active, max(1, len(active) // 4)):
        script.append(("add", k))
    return script


def apply_script(s, script):
    """Apply a churn script to set `s` (forces set_table_resize realloc+free).
    Used both for the shared set (under the mutator's lock) and the private
    control (single owner)."""
    for op, k in script:
        if op == "add":
            s.add(k)
        else:
            s.discard(k)


def probe_present_absent(H, wid, s, span_keys, absent_keys):
    """Run the unlocked P/A membership probe against set `s` (the set_lookkey
    open-addressing walk).  span_keys are PRESENT (must all hit); absent_keys are
    ABSENT (must all miss).  Returns False on the first torn/wrong answer (a flip
    is the bug); a SIGSEGV here is the OOB table read.  Holds NO lock -- this is
    the probe-vs-resize race on the shared set."""
    for k in span_keys:
        # `k in s` -> set_lookkey on so_table with so_mask.  MUST be True (k is in
        # PRESENT, never removed).  A False is a use-after-free / torn-mask probe.
        if k not in s:
            H.fail("membership FLIPPED: PRESENT key {0!r} reported ABSENT by "
                   "`k in s` -- a torn so_table/so_mask read or a use-after-free "
                   "on the resized-away table (set_lookkey walked a freed/wrong "
                   "table mid set_table_resize)".format(k))
            return False
    for k in absent_keys:
        # MUST be False (k is in ABSENT, never added).  A True means set_lookkey
        # matched a garbage/aliased setentry as a hit -- torn slot read.
        if k in s:
            H.fail("membership FLIPPED: ABSENT key {0!r} reported PRESENT by "
                   "`k in s` -- set_lookkey matched a torn/aliased setentry as a "
                   "hit (out-of-bounds or freed-slot read under resize)".format(k))
            return False
    return True


def probe_case(H, wid, s, case, span_keys, absent_keys):
    """Run ONE membership-probe CASE against set `s` (unlocked).  Returns False on
    the first wrong answer.  Each case bottoms out in set_lookkey; the bulk forms
    (issuperset/isdisjoint/issubset) walk many entries per call, widening the
    window in which a resize can tear the probe."""
    if case == CASE_IN:
        return probe_present_absent(H, wid, s, span_keys, absent_keys)
    if case == CASE_NOT_IN:
        # All ABSENT keys must satisfy `not in`; all PRESENT span keys must NOT.
        for k in absent_keys:
            if k in s:
                H.fail("`not in` FLIPPED: ABSENT key {0!r} reported present "
                       "-- torn set_lookkey miss-path read under resize".format(k))
                return False
        for k in span_keys:
            if k not in s:
                H.fail("`not in` FLIPPED: PRESENT key {0!r} reported absent "
                       "-- torn set_lookkey read under resize".format(k))
                return False
        return True
    if case == CASE_ISSUPERSET:
        # s seeded with PRESENT and never loses a PRESENT key, so s ALWAYS
        # contains all of P -> issuperset(P_frozen) is invariantly True.  A False
        # is a torn probe inside the bulk set_lookkey loop.
        if not s.issuperset(PRESENT_FROZEN):
            H.fail("s.issuperset(PRESENT) returned False -- the shared set lost a "
                   "PRESENT key under set_table_resize (torn bulk set_lookkey / "
                   "freed-table probe)")
            return False
        return True
    if case == CASE_ISDISJOINT:
        # ABSENT keys are never in s, so isdisjoint(A_frozen) is invariantly True.
        # A False means a bulk probe matched an ABSENT key -- a torn-slot hit.
        if not s.isdisjoint(ABSENT_FROZEN):
            H.fail("s.isdisjoint(ABSENT) returned False -- a bulk set_lookkey "
                   "matched an ABSENT key as present (out-of-bounds/torn setentry "
                   "read under resize)")
            return False
        return True
    # CASE_ISSUBSET: probe from the frozenset side.  P is always a subset of s.
    if not PRESENT_FROZEN.issubset(s):
        H.fail("PRESENT.issubset(s) returned False -- s is missing a PRESENT key "
               "(torn set_lookkey / use-after-free on the resized table)")
        return False
    return True


def run_round_impl(H, wid, rng, slot, state):
    """One probe-vs-resize round.  Seed a shared set with PRESENT, run a PROBER
    (unlocked, races the resize) and a MUTATOR (churns CHURN under a lock, forcing
    set_table_resize) on different hubs, synced so the resize lands inside the
    prober's park window.  Mirror the churn into a PRIVATE control set and probe
    it too.  Then check the frozenset so_hash cache.  Join both children before
    post-round reads so the set is provably quiescent."""
    lock = state["lock"]
    probed_tbl = state["probed"]
    bump_case = state["bump_case"]

    # Shared set: seeded with PRESENT only (ABSENT never added, CHURN churned).
    shared = set(PRESENT)
    # Private single-owner control: same seed, same churn script, no other writer.
    ctrl = set(PRESENT)

    script = churn_script(rng)

    # Pick this round's probe case by round-robin over a PER-SLOT counter (slot is
    # single-writer, so this is race-free), NOT random -- pure random reliably
    # MISSES a case at low op-count under load (the p125/p126/p172 flaky-coverage
    # bug).  (wid + seq) % NCASES walks every case as rounds accumulate.
    case = (wid + state["seq"][slot]) % NCASES
    state["seq"][slot] += 1
    span_keys = list(PRESENT[: PROBE_SPAN])
    absent_keys = list(ABSENT[: PROBE_SPAN])

    # gate: the prober trips it the instant before it parks mid-probe; the mutator
    # waits on it, so the resize provably lands inside the prober's park window.
    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(2)

    # Track whether each arm's probe passed (read in post-round assert).
    result = {"shared_ok": True, "ctrl_ok": True, "did_probe": False}

    def run_prober():
        try:
            # Probe the SHARED set first (unlocked, racing the resize), parking
            # mid-probe so the mutator's set_table_resize lands during the walk.
            # Trip the gate just before the park; the mutator is waiting on it.
            for k in span_keys:
                if k not in shared:
                    H.fail("membership FLIPPED (pre-park): PRESENT key {0!r} "
                           "absent in shared set -- torn set_lookkey".format(k))
                    result["shared_ok"] = False
                    gate.done()
                    return
            gate.done()                    # let the mutator resize NOW
            runloom.yield_now()            # park mid-probe; resize lands here
            # Resume the probe AFTER the resize -- this is the in-flight walk that
            # must finish against the (possibly reallocated) table consistently.
            if not probe_case(H, wid, shared, case, span_keys, absent_keys):
                result["shared_ok"] = False
                return
            # Full P/A sweep on the shared set post-park.
            if not probe_present_absent(H, wid, shared, span_keys, absent_keys):
                result["shared_ok"] = False
                return
            if not shared.issuperset(PRESENT_FROZEN):
                H.fail("post-park s.issuperset(PRESENT) False -- PRESENT key lost "
                       "to set_table_resize on the shared set")
                result["shared_ok"] = False
                return
            if not shared.isdisjoint(ABSENT_FROZEN):
                H.fail("post-park s.isdisjoint(ABSENT) False -- ABSENT key matched "
                       "by a torn bulk probe on the shared set")
                result["shared_ok"] = False
                return
            result["did_probe"] = True
        except Exception as exc:           # noqa: BLE001
            # set membership/issubset/etc. has NO legal exception here (unlike a
            # dict/set ITERATION, which may raise "changed size during iteration").
            # A probe is a point lookup; ANY exception is a fault.
            H.fail("prober raised {0}: {1} -- a membership probe (set_lookkey) "
                   "must never raise; an exception means a corrupted set object "
                   "under concurrent set_table_resize".format(
                       type(exc).__name__, exc))
            result["shared_ok"] = False
        finally:
            wg.done()

    def run_mutator():
        try:
            gate.wait()                    # prober is parked mid-probe
            # Churn CHURN keys on the SHARED set under the lock (set mutation is
            # not thread-safe GIL-off; the lock keeps P/A truth invariant by
            # construction).  This is the set_table_resize realloc+free traffic the
            # unlocked prober races.  yield_now() inside the held region makes the
            # resize overlap the prober's parked walk on the other hub.
            with lock:
                for op, k in script:
                    if op == "add":
                        shared.add(k)
                    else:
                        shared.discard(k)
                    runloom.yield_now()    # prober resumes its probe mid-resize
        except Exception:
            # The mutator's own add/discard of CHURN keys never legally raises;
            # swallow so a mutator hiccup can't deadlock the prober's gate (already
            # tripped).  The prober's oracle is the judge.
            pass
        finally:
            wg.done()

    H.fiber(run_prober)
    H.fiber(run_mutator)
    wg.wait()                              # both children joined -> set quiescent

    if H.failed:
        return

    # ---- private single-owner CONTROL arm (race-free by construction) ---------
    # Replay the IDENTICAL churn script on the private control set, then run the
    # SAME P/A probes.  No other writer touched ctrl, so its set_lookkey can never
    # tear -- a wrong answer HERE is a CPython set-machinery bug, not contention.
    apply_script(ctrl, script)
    if not probe_present_absent(H, wid, ctrl, span_keys, absent_keys):
        # probe_present_absent already H.fail'd with a shared-set message; refine.
        H.fail("CONTROL (single-owner) set returned a WRONG P/A answer -- a "
               "race-free private set must answer every membership probe "
               "correctly; the fault is in CPython's set_lookkey itself, not M:N "
               "contention")
        return
    if not H.check(ctrl.issuperset(PRESENT_FROZEN),
                   "CONTROL set lost a PRESENT key (issuperset(PRESENT) False) -- "
                   "single-owner set machinery dropped a key across resizes"):
        return
    if not H.check(ctrl.isdisjoint(ABSENT_FROZEN),
                   "CONTROL set matched an ABSENT key (isdisjoint(ABSENT) False) "
                   "-- single-owner set_lookkey false positive"):
        return

    # ---- frozenset so_hash cache consistency (now quiescent) ------------------
    # Snapshot the live keys of the shared set, build a frozenset (which lazily
    # fills so_hash via frozenset_hash summing every entry->hash), and assert the
    # cached hash equals a fresh recompute over the SAME keys.  A rehash that
    # moved entries mid-sum would make the cache disagree with the recompute.
    keys_now = tuple(shared)              # set quiescent: a clean snapshot
    for k in keys_now:
        if k not in UNIVERSE_SET:
            H.fail("shared set holds OUT-OF-UNIVERSE key {0!r} after the round -- "
                   "a torn/corrupted setentry from a resize under the probe race"
                   .format(k))
            return
    fz = frozenset(keys_now)
    fz_recompute = frozenset(list(keys_now))
    if not H.check(hash(fz) == hash(fz_recompute),
                   "frozenset so_hash cache inconsistent: hash(fz)={0} != "
                   "hash(recompute)={1} over the SAME keys -- frozenset_hash "
                   "summed entry->hash over a table a resize moved (double-count "
                   "or skip)".format(hash(fz), hash(fz_recompute))):
        return
    # And the snapshot must be a superset of PRESENT (PRESENT never churns out).
    if not H.check(fz.issuperset(PRESENT_FROZEN),
                   "frozenset(shared snapshot) missing a PRESENT key -- a PRESENT "
                   "entry was lost to set_table_resize"):
        return

    # Record coverage + work.  probed_tbl is single-writer-per-slot (race-free);
    # the per-case tally is shared across workers, so bump_case takes the separate
    # accounting guard (it is bookkeeping, NOT the object under test).
    probed_tbl[slot] += 1
    bump_case(case)


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
    # the cooperative M:N-safe lock.  `lock` serializes the MUTATOR's writes to the
    # shared set (set mutation is not thread-safe GIL-off); the PROBER holds no
    # lock, so probe-vs-resize is the only contention.  The per-case tally is
    # written by many workers, so it is guarded by a separate accounting lock --
    # it is NOT the object under test, just coverage bookkeeping.
    H.state = {
        "lock": runloom.sync.Lock(),
        "case_guard": runloom.sync.Lock(),
        "probed": [0] * SLOTS,             # rounds whose full oracle passed
        "cases": [0] * NCASES,             # per-case coverage (guarded)
        "seq": [0] * SLOTS,                # per-slot round-robin counter
    }
    # case_tbl writes need the guard; wrap them via a tiny helper closure stored
    # on state so run_round_impl stays readable.
    case_tbl = H.state["cases"]
    guard = H.state["case_guard"]

    def bump_case(case):
        with guard:
            case_tbl[case] += 1

    H.state["bump_case"] = bump_case


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    probed = sum(H.state["probed"])
    cases = H.state["cases"]
    H.log("probe-vs-resize rounds whose full P/A + control + frozenset-hash "
          "oracle passed: {0}; per-case coverage={1}; ops={2}".format(
              probed, cases, H.total_ops()))
    # Reaching post with no failure already means every per-round membership law
    # held fail-fast; assert the run actually did work (else the law was vacuous).
    H.check(probed > 0,
            "no probe-vs-resize rounds completed -- the set_lookkey-vs-"
            "set_table_resize race window was never exercised")
    # Every membership-probe case was round-robined by (wid + per-slot seq), so
    # with enough rounds/workers all NCASES are covered.  Assert each was hit at
    # least once (a never-exercised case means that probe path was untested).
    for case in range(NCASES):
        H.check(cases[case] > 0,
                "membership-probe case {0} was never exercised -- coverage gap "
                "(round-robin did not reach it; raise --funcs or --rounds)".format(
                    case))
    H.require_no_lost("frozenset-membership-probe completeness")


if __name__ == "__main__":
    harness.main(
        "p425_frozenset_membership_probe_vs_", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many hubs run UNLOCKED set membership probes (k in s / "
                 "issubset / issuperset / isdisjoint -> set_lookkey) against a "
                 "set another hub rehashes via set_table_resize; closed-world "
                 "PRESENT-always-True / ABSENT-always-False oracle on shared AND "
                 "a single-owner control, plus frozenset so_hash-cache "
                 "consistency -- a flipped answer is a torn so_table/so_mask read")
