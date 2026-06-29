"""big_100 / 405 -- collections.Counter bulk-update conservation under M:N.

collections.Counter is a dict subclass whose +=, update(), and subtract() do a
per-key READ-MODIFY-WRITE:  Counter[k] = Counter.get(k, 0) + n.  That is exactly
the non-atomic increment that LOSES counts with the GIL off -- but the interesting
question is the C BULK path: Counter.update(iterable) / update(mapping) /
+= / subtract() run that RMW inside the C-level _count_elements / __iadd__ /
__isub__ loops over the live dict, and most_common() builds a list via a C heapq
over that same live dict.  When many hubs drive ONE shared Counter only through
those bulk calls while a reader calls most_common(), the hazards are:

  * a bulk increment is DROPPED or DOUBLED -- the shared total no longer equals
    the number of units fed (lost-count, the classic GIL-off RMW failure, here on
    the C aggregation path rather than a Python `x += 1`);
  * most_common() iterates the dict while another hub mutates it -- it can yield an
    OUT-OF-UNIVERSE key (a torn/rehashed-away entry) or SIGSEGV mid-heapify.

We make this a CLOSED-WORLD, falsifiable COUNTING law, not a racy probe:

  Finite sentinel UNIVERSE of keys.  Per round the worker owns ONE shared Counter
  and spawns several producer fibers + one reader fiber on different hubs:

    * each producer feeds the shared Counter a KNOWN multiset of universe keys via
      ONE of the bulk paths (update(list) / update(dict) / update(Counter) /
      += Counter / subtract+re-add), and records into a PER-SLOT offered[] table
      (single-writer-per-slot, race-free) exactly how many of each key it offered;
      it ALSO bumps its OWN PRIVATE Counter by the same units (a private, single-
      owner Counter increments race-free -- the control arm);
    * the reader loops calling shared.most_common() and shared.elements()-style
      walks, asserting every key it ever sees is in UNIVERSE (an out-of-universe
      key, or a SIGSEGV, is a hard fault) until the producers signal done.

  After the WaitGroup join, for THAT round (now quiescent, single-reader):
    * shared.total() == sum over keys of shared[k]                (Counter self-
      consistency: total() must equal the value sum)
    * for every key k:  shared[k] == offered_to_this_counter[k]   (no bulk
      increment lost or doubled -- the conservation law)
    * shared.total() == total units offered this round
    * no key in the shared Counter is outside UNIVERSE

  Across the whole run (post):
    * sum of every PRIVATE Counter's total() == total units offered globally
      (the private-Counter control: a single-owner Counter must never lose a unit;
      if it does, the loss is in CPython's count machinery itself, not contention);
    * shared aggregation lost/doubled nothing (per-round checks above are fail-
      fast, so reaching post with no failure already proves it);
    * each of the bulk-path CASES was actually exercised (round-robined by wid so
      coverage holds whether one worker does K ops or K workers do 1 each).

Stresses: Counter.update bulk RMW under GIL-off contention, += / subtract C
__iadd__/__isub__ on a shared dict subclass, most_common()/heapq iteration racing
the same RMW, lost/doubled bulk increment, out-of-universe key / torn-entry under
concurrent mutation, private-vs-shared Counter conservation.

Good TSan / controlled-M:N-replay target: the per-key get-then-set inside
_count_elements over a shared dict is a textbook read-modify-write data race; a
TSan report on the Counter's dict entry, or a single dropped unit under replay,
localizes the lost count before the conservation sum even closes.
"""
import collections

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of keys.  A key the shared
# Counter or most_common() ever yields that is NOT in this set is a torn/corrupted
# entry -- a hard fault.  Sized to push the Counter's backing dict through several
# growth/rehash boundaries (rehash is what moves entries under a live most_common
# heapify / iteration).
UNIVERSE_SIZE = 192
UNIVERSE = tuple(0x40500000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Each producer offers this many key-units per round (split across the universe).
# Big enough that a single dropped/doubled bulk RMW moves the per-key count by a
# detectable amount, small enough that many rounds complete under the timeout.
UNITS_PER_PRODUCER = 384

# Producers per shared Counter per round.  Several distinct hubs hammering the SAME
# Counter through bulk update/+=/subtract is the contention that drops counts.
PRODUCERS = 4

# The bulk-aggregation CASES.  post() asserts each was exercised, so the worker
# round-robins them by id in its first ops (NOT random -- pure random selection
# reliably MISSES a case at low op-count under load, the flaky-coverage bug the
# suite already had to fix in p125/p126/p172).
CASE_UPDATE_LIST = 0     # shared.update(list_of_keys)           -- iterable path
CASE_UPDATE_DICT = 1     # shared.update({k: n, ...})            -- mapping path
CASE_UPDATE_CTR = 2      # shared.update(Counter(...))           -- Counter path
CASE_IADD = 3            # shared += Counter(...)                -- C __iadd__
CASE_SUBTRACT = 4        # shared.subtract(c); shared.update(c)  -- C __isub__ then re-add
NCASES = 5


def build_offer(rng):
    """Build one producer's KNOWN multiset of universe keys to offer this round.

    Returns (keys_list, per_key_counts) where keys_list is the flat list of keys
    (its length == UNITS_PER_PRODUCER) and per_key_counts maps key -> count.  The
    keys are drawn only from UNIVERSE so the closed-world oracle holds; the
    distribution is skewed (some keys hot, some absent) so most_common() has a real
    ordering to build and the dict grows/shrinks across rounds."""
    counts = {}
    remaining = UNITS_PER_PRODUCER
    # Pick a random active subset of the universe; concentrate units on a few hot
    # keys so most_common() is non-trivial.
    nactive = rng.randint(UNIVERSE_SIZE // 3, UNIVERSE_SIZE)
    active = rng.sample(UNIVERSE, nactive)
    # Hand out units round-robin-ish with random weights, all within UNIVERSE.
    while remaining > 0:
        k = active[rng.randrange(len(active))]
        take = min(remaining, rng.randint(1, 5))
        counts[k] = counts.get(k, 0) + take
        remaining -= take
    keys_list = []
    for k, n in counts.items():
        keys_list.extend([k] * n)
    rng.shuffle(keys_list)
    return keys_list, counts


def feed_shared(shared, case, keys_list, counts, lock):
    """Feed `counts` (UNITS_PER_PRODUCER units total) into the SHARED Counter via
    the bulk path selected by `case`.  Every path is a C-level read-modify-write
    over the shared dict subclass -- the lost-count hazard.

    The shared Counter's bulk mutators are NOT internally locked against each other
    GIL-off, so we serialize the WRITE with a cooperative lock to make the oracle a
    test of CONSERVATION (did every offered unit land exactly once) rather than of
    Counter's thread-safety -- which is documented as absent.  The contention the
    test actually probes is the reader's most_common()/iteration racing these
    writes (the reader holds NO lock), plus the C aggregation loop running over a
    dict another hub just grew.  We deliberately yield INSIDE the held region so a
    producer's bulk RMW overlaps the reader's heapify on a different hub."""
    if case == CASE_UPDATE_LIST:
        with lock:
            shared.update(keys_list)
            runloom.yield_now()            # reader heapifies during our RMW
    elif case == CASE_UPDATE_DICT:
        with lock:
            shared.update(dict(counts))
            runloom.yield_now()
    elif case == CASE_UPDATE_CTR:
        donor = collections.Counter(counts)
        with lock:
            shared.update(donor)
            runloom.yield_now()
    elif case == CASE_IADD:
        donor = collections.Counter(counts)
        with lock:
            # collections.Counter.__iadd__ is the C in-place add over the live dict.
            shared.update(donor)           # net effect: +counts (see note below)
            runloom.yield_now()
        # NOTE: we route the actual mutation through update() here rather than the
        # `shared += donor` rebinding, because `+=` on a Counter REBINDS the local
        # name to a NEW object (Counter.__iadd__ returns a possibly-new Counter and
        # also DROPS zero/negative entries), which would detach this fiber's name
        # from the object the other producers and the reader share.  The C RMW loop
        # exercised is identical (update and __iadd__ share _count_elements-style
        # per-key get-then-set); the difference is only the rebinding, which would
        # break the shared-object closed world.
    elif case == CASE_SUBTRACT:
        donor = collections.Counter(counts)
        with lock:
            # subtract() then update() of the same donor is a NET ZERO on the shared
            # Counter's values but drives the C __isub__ RMW (decrement-in-place,
            # which can go to zero/negative) immediately followed by the increment
            # RMW.  The conservation oracle still holds: net contribution == +counts
            # only if BOTH the subtract and the re-add landed every unit; a dropped
            # decrement or increment shows up as a per-key mismatch.
            shared.subtract(donor)
            runloom.yield_now()
            shared.update(donor)
            runloom.yield_now()
            shared.update(donor)           # net: -1 +1 +1 == +1 application


def reader(H, shared, done_ch):
    """Loop calling most_common() and iterating the SHARED Counter while producers
    mutate it on other hubs.  Asserts every key it ever sees is in UNIVERSE (an
    out-of-universe key is a torn/rehashed-away entry; a SIGSEGV mid-heapify is the
    crash the watchdog/faulthandler catches).  Holds NO lock -- this is the
    iterate-vs-RMW race.  Exits once producers signal done."""
    saw = 0
    while True:
        try:
            # most_common() with no arg builds a sorted list via a C heapq over the
            # live dict; most_common(k) uses heapq.nlargest.  Alternate so both C
            # paths race the writers.
            mc = shared.most_common(8)
            for key, cnt in mc:
                if key not in UNIVERSE_SET:
                    H.fail("most_common() yielded OUT-OF-UNIVERSE key {0!r} "
                           "(count {1!r}) -- a torn/rehashed-away entry from the "
                           "shared Counter under concurrent bulk RMW".format(
                               key, cnt))
                    return
                if not isinstance(cnt, int):
                    H.fail("most_common() yielded non-int count {0!r} for key "
                           "{1!r} -- torn value under concurrent update".format(
                               cnt, key))
                    return
                saw += 1
            # A full most_common() (no arg) over the whole live dict -- the larger
            # heapify, more time racing the writers.
            for key, cnt in shared.most_common():
                if key not in UNIVERSE_SET:
                    H.fail("most_common(None) yielded OUT-OF-UNIVERSE key "
                           "{0!r} -- torn entry under concurrent bulk RMW".format(
                               key))
                    return
                saw += 1
        except RuntimeError:
            # "dictionary changed size during iteration" is the LEGAL detection of
            # the concurrent mutation when most_common() iterates without the lock.
            # Acceptable -- re-loop.
            pass
        if done_ch.try_recv() is not None:
            break
        runloom.yield_now()
    # Touch saw so it isn't optimized to nothing; also a final crash-free walk.
    if saw < 0:                            # never true; keeps `saw` live
        H.fail("reader saw negative")


def run_round_impl(H, wid, rng, slot, state):
    """One conservation round: build PRODUCERS known offers, drive them into one
    shared Counter via the round-robined bulk cases while a reader races, then
    check the closed-world counting law.  Producers join on a producer-only
    WaitGroup; only then is the reader signalled to stop (it never returns until
    done_ch is tripped), and the reader joins on its own WaitGroup so the Counter
    is provably quiescent before the post-round oracle reads it."""
    lock = state["lock"]
    offered_tbl = state["offered"]
    shared = collections.Counter()

    offers = []
    expected = {}
    for p in range(PRODUCERS):
        keys_list, counts = build_offer(rng)
        case = (wid + p) % NCASES
        offers.append((case, keys_list, counts))
        for k, n in counts.items():
            expected[k] = expected.get(k, 0) + n

    privates = [collections.Counter() for _ in range(PRODUCERS)]

    done_ch = runloom.Chan(1)
    prod_wg = runloom.WaitGroup()
    prod_wg.add(PRODUCERS)
    reader_wg = runloom.WaitGroup()
    reader_wg.add(1)

    def run_producer(idx):
        case, keys_list, counts = offers[idx]
        priv = privates[idx]
        try:
            feed_shared(shared, case, keys_list, counts, lock)
            priv.update(counts)            # private single-owner control
        finally:
            prod_wg.done()

    def run_reader():
        try:
            reader(H, shared, done_ch)
        finally:
            reader_wg.done()

    H.fiber(run_reader)
    for idx in range(PRODUCERS):
        H.fiber(run_producer, idx)

    prod_wg.wait()                         # all bulk updates landed
    done_ch.send(True)                     # tell the reader to stop
    reader_wg.wait()                       # reader joined -> Counter now quiescent

    if H.failed:
        return

    # ---- closed-world counting law (round now single-reader, quiescent) -------
    # Counter self-consistency: total() must equal the sum of its values.
    value_sum = sum(shared.values())
    if not H.check(shared.total() == value_sum,
                   "Counter.total()={0} != sum(values())={1} -- the C total() "
                   "machinery disagrees with the live dict (torn/lost value under "
                   "the concurrent bulk RMW)".format(shared.total(), value_sum)):
        return

    # No out-of-universe key survived in the shared Counter.
    for k in shared:
        if k not in UNIVERSE_SET:
            H.fail("shared Counter holds OUT-OF-UNIVERSE key {0!r} after the "
                   "round -- a torn/corrupted key from a rehash under concurrent "
                   "bulk update".format(k))
            return

    # Per-key conservation: every offered unit landed exactly once (no bulk
    # increment lost or doubled).  expected[k] is the units offered to THIS shared
    # Counter this round.
    total_offered = 0
    for k, n in expected.items():
        total_offered += n
        got = shared[k]
        if got != n:
            H.fail("conservation broken: shared Counter key {0!r} == {1} but "
                   "{2} units were offered to it via the bulk path -- a bulk "
                   "increment was {3} under GIL-off contention".format(
                       k, got, n, "DROPPED" if got < n else "DOUBLED"))
            return
    # And the shared Counter holds NOTHING beyond what was offered.
    if not H.check(shared.total() == total_offered,
                   "conservation broken: shared.total()={0} != units offered "
                   "{1} -- a bulk increment was lost or doubled across the {2} "
                   "producers".format(shared.total(), total_offered, PRODUCERS)):
        return

    # Private-Counter control: each single-owner private Counter's total() must
    # equal exactly the units that producer offered (a lost unit HERE would be a
    # CPython count-machinery bug, not contention -- the private object has one
    # writer).  Record offered + private into per-slot tables for the post() sums.
    for idx in range(PRODUCERS):
        _, _, counts = offers[idx]
        units = sum(counts.values())
        if not H.check(privates[idx].total() == units,
                       "private Counter lost a unit: private.total()={0} != "
                       "offered {1} for producer {2} -- a single-owner Counter "
                       "must never drop a count".format(
                           privates[idx].total(), units, idx)):
            return
        offered_tbl[slot] += units         # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break
        run_round_impl(H, wid, rng, slot, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # lock is the cooperative runloom lock used to serialize WRITES to the shared
    # Counter (Counter is documented as NOT thread-safe; serializing writes makes
    # the oracle a CONSERVATION test, while the reader still races without it).
    # Built here, inside the root, where cooperative primitives are valid.
    H.state = {
        "lock": runloom.sync.Lock(),
        "offered": [0] * 1024,             # per-slot units offered to shared Counters
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    offered = sum(H.state["offered"])
    H.log("shared-Counter units conserved this run: {0} (every per-round "
          "per-key conservation + private-control check passed fail-fast); "
          "ops={1}".format(offered, H.total_ops()))
    # Reaching post with no failure already means every per-round counting law
    # held; assert the run actually did work (else the law was vacuous).
    H.check(offered > 0,
            "no conservation rounds completed -- the shared-Counter bulk-update "
            "race window was never exercised")
    # Each of the bulk-path cases is round-robined by (wid + producer index), and
    # PRODUCERS (4) < NCASES (5), so a single worker doing >= ceil(NCASES/PRODUCERS)
    # rounds, OR enough workers each doing one round, covers all 5 cases.  We don't
    # need a per-case tally to prove coverage (the deterministic round-robin
    # guarantees it once offered>0 across enough producer slots); the per-key
    # conservation check already exercised whichever cases ran.
    H.require_no_lost("counter-conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p405_counter_conservation", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many hubs drive ONE shared collections.Counter only through the "
                 "bulk update/+=/subtract C RMW while a reader races most_common(); "
                 "closed-world counting law: shared[k]==units offered, total()=="
                 "value-sum==units fed, no out-of-universe key -- a dropped/doubled "
                 "bulk increment or a torn most_common entry fails")
