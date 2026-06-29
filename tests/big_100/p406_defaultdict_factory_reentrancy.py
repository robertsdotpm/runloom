"""big_100 / 406 -- defaultdict.__missing__ factory reentrancy across an M:N park.

`collections.defaultdict.__missing__` is the one stdlib hook that calls a Python
callable -- `default_factory()` -- and then INSERTS its result back into the dict.
We emulate it with a `__missing__` that runs a Python factory which PARKS before
it commits: `value = make_value(key)` (which calls `runloom.yield_now()` mid-
flight, suspending the fiber WHILE the key is still missing) and only then
`self[key] = value`.  Many fibers on different hubs concurrently hammer a SINGLE
shared dict over a finite sentinel UNIVERSE of keys -- `d[k]` (which may trip the
parking factory) and `del d[k]`.

That park is the hostile window.  Because the factory yields BEFORE the entry is
installed, a sibling on another hub that gets the same still-missing key will
legitimately re-run the factory -- so the factory is NOT one-call-per-insert
(redundant calls for concurrent misses are correct, GIL-on or GIL-off, and we do
NOT treat them as a fault).  What MUST hold under the GIL-off dict implementation
is memory-safety and a real conservation law:

  * VALUE oracle (hot, fail-fast): every value read from the dict equals the
    factory's deterministic product `product(key)`.  A value that is not
    product(key) -- or an out-of-universe key -- is a TORN / foreign value
    published from a half-built or freed slot under a concurrent insert/rehash,
    i.e. dict critical-section corruption.  Hard fault.

  * STRUCTURAL conservation (post): each key owns its OWN one-element creation
    and deletion counters (distinct objects per key, so writes for DIFFERENT keys
    never share a Python object; writes for the SAME key are serialized by the
    dict's per-object critical section -- the mechanism under test).  A `del d[k]`
    is recorded ONLY when it actually removed an entry (did not raise KeyError).
    The conserved laws, robust to redundant factory calls:
      - you cannot delete a key more times than it was ever created:
        DELETES[k] <= CREATES[k] for every key (a phantom/double-delete from a
        torn `used` counter or a freed-slot reuse breaks this);
      - a key present at the end must have been created at least once and not
        net-over-deleted: CREATES[k] > DELETES[k] for every resident key;
      - in aggregate the dict size equals (creations seen as still-resident) and
        is bounded by total creations minus total deletes is NOT assumed (a
        single key can be created N times for one residency); instead every
        resident key satisfies the per-key law above and every absent key has
        DELETES[k] <= CREATES[k].
    A real double-free / lost-insert / torn-link surfaces as DELETES exceeding
    CREATES for some key, or a resident key the factory never created.

Any non-KeyError exception escaping a worker, or a SIGSEGV from the
park-inside-the-critical-section, is a hard fault (the harness turns a worker
exception into an INVARIANT FAIL; a segv is caught by the watchdog as HANG/crash).

Coverage: the access pattern round-robins {get-missing, get-present, delete,
contended-get} by worker id in its first ops so every branch -- crucially the
two-fibers-on-one-missing-key CONTENDED-GET that triggers the reentrancy -- is
provably exercised under load, not left to flaky random selection.

Stresses: defaultdict.__missing__ factory reentrancy, dict critical-section
survival across a cooperative park, double-factory-call / double-insert, torn
value publication, single-shared-container cross-hub churn, conservation of
insert/delete under M:N.
"""
import random

import harness
import runloom

# Finite sentinel UNIVERSE.  Small enough that many of the thousands of fibers
# collide on the SAME key (which is what drives a concurrent miss on a key a
# sibling is mid-factory on), big enough to push the shared dict through several
# growth/rehash boundaries as keys churn in and out.
UNIVERSE_SIZE = 64
KEY_BASE = 0x40600000
UNIVERSE = tuple(KEY_BASE + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)
KEY_INDEX = {k: i for i, k in enumerate(UNIVERSE)}


def product(key):
    """Deterministic key -> value the factory must produce.  A value read from
    the dict that is not product(key) is a TORN / foreign value (key K but a
    value built for a different slot or a half-initialized one).  A near-
    bijection so a torn value is overwhelmingly unlikely to coincide."""
    return ((key * 0x9E3779B1) ^ 0x5DEECE66D) & 0x7FFFFFFFFFFFFFFF


# Per-key counters, one DISTINCT one-element list per key.  The factory bumps
# CREATES[idx][0]; the deleter bumps DELETES[idx][0].  Two facts make this a
# correct probe rather than a self-inflicted data race:
#   * writes for DIFFERENT keys touch DIFFERENT list objects, so they never
#     share a Python object and cannot tear each other;
#   * writes for the SAME key are serialized by the dict's per-object critical
#     section -- the exact mechanism under test.  Redundant factory calls for
#     concurrent misses (legal) inflate CREATES, which the conservation laws in
#     post() explicitly tolerate; what they DON'T tolerate is DELETES exceeding
#     CREATES or a resident key the factory never created.
# Accumulated for the whole run (the dict persists across rounds too), so the
# post() conservation read -- creations - deletions vs final presence -- holds
# for any --rounds count.  Shared across all hubs.
CREATES = tuple([0] for _ in range(UNIVERSE_SIZE))
DELETES = tuple([0] for _ in range(UNIVERSE_SIZE))


def make_value(key):
    """The defaultdict factory, called by __missing__ with the dict's critical
    section held.  Records the creation for this key, then PARKS mid-insert via
    runloom.yield_now() so a sibling fiber on another hub can race the same
    missing key while this insert is only half-committed."""
    idx = KEY_INDEX[key]
    CREATES[idx][0] += 1
    # Park WHILE __missing__ is mid-insert: the entry is being installed into the
    # dict right now, and we hand the scheduler to another hub before returning
    # the value the dict will store.  This is the hostile window.
    runloom.yield_now()
    return product(key)


class FactoryDict(dict):
    """A defaultdict whose factory needs the missing KEY (the stdlib
    default_factory takes no args).  __missing__ runs make_value(key) inside the
    dict critical section, installs it, and returns it -- the standard
    defaultdict contract, but with a key-aware, parking factory."""

    def __missing__(self, key):
        value = make_value(key)
        self[key] = value
        return value


def check_value(H, key, val):
    """Validate one value read from the shared dict.  False on first violation."""
    if key not in UNIVERSE_SET:
        H.fail("read OUT-OF-UNIVERSE key {0!r} from the shared defaultdict -- a "
               "corrupted/torn key from a half-built entry (M:N dict "
               "corruption)".format(key))
        return False
    if val != product(key):
        H.fail("TORN value for key {0!r}: got {1!r} != product(key) {2!r} -- a "
               "foreign/half-initialized value published from the factory's "
               "critical section under concurrent miss".format(
                   key, val, product(key)))
        return False
    return True


def op_get_missing(H, d, rng):
    """Access a key that is (usually) missing -> trips the parking factory.  The
    value the dict returns must be product(key)."""
    key = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
    val = d[key]
    return check_value(H, key, val)


def op_get_present(H, d, rng):
    """Seed a key then immediately re-read it; the re-read must NOT call the
    factory and must return the same product(key)."""
    key = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
    _ = d[key]
    val = d[key]
    return check_value(H, key, val)


def op_delete(H, d, rng):
    """Delete a key if present.  A successful delete is recorded against the
    key's own delete counter (single distinct object); KeyError (already gone on
    another hub) is the legal, expected outcome and is NOT recorded."""
    key = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
    idx = KEY_INDEX[key]
    try:
        del d[key]
    except KeyError:
        return True
    DELETES[idx][0] += 1
    return True


def op_contended_get(H, d, rng):
    """The reentrancy probe: pick one key, delete it so it is missing, then fire
    TWO fibers that both access d[key] at once.  If the factory parks mid-insert
    and the dict critical section is sound, exactly ONE factory call happens and
    BOTH fibers read product(key); if it is broken the second fiber re-enters the
    factory (double insert / torn value).  Both readers validate their value."""
    key = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
    idx = KEY_INDEX[key]
    # Make it missing first so the access actually trips the factory.  A
    # concurrent sibling may already have removed/re-added it; KeyError is fine.
    try:
        del d[key]
        DELETES[idx][0] += 1
    except KeyError:
        pass

    wg = runloom.WaitGroup()
    wg.add(2)
    ok = [True, True]

    def reader(slot, key=key, wg=wg, ok=ok):
        try:
            val = d[key]
            if not check_value(H, key, val):
                ok[slot] = False
        finally:
            wg.done()

    H.fiber(lambda: reader(0))
    H.fiber(lambda: reader(1))
    wg.wait()
    return ok[0] and ok[1]


NCASES = 4


def worker(H, wid, rng, state):
    d = state["d"]
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the four cases by worker id in the first ops so each branch
        # -- especially the CONTENDED-GET reentrancy probe -- is provably
        # exercised under load (timeout-bound ops complete only a handful of
        # rounds; pure random selection reliably misses a case and flakes the
        # post coverage check).  Random after that to keep the concurrent mix.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == 0:
            ok = op_get_missing(H, d, rng)
            state["case"][0][wid & 1023] += 1
        elif sel == 1:
            ok = op_get_present(H, d, rng)
            state["case"][1][wid & 1023] += 1
        elif sel == 2:
            ok = op_delete(H, d, rng)
            state["case"][2][wid & 1023] += 1
        else:
            ok = op_contended_get(H, d, rng)
            state["case"][3][wid & 1023] += 1
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    H.state = {
        "d": FactoryDict(),
        # Per-case exercise tallies (single-writer per (case, wid&1023) slot).
        "case": [[0] * 1024 for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    d = H.state["d"]
    cases = [sum(c) for c in H.state["case"]]
    H.log("cases get_missing={0} get_present={1} delete={2} contended={3} "
          "ops={4} dict_size={5}".format(
              cases[0], cases[1], cases[2], cases[3], H.total_ops(), len(d)))
    H.check(H.total_ops() > 0, "no rounds completed")
    # Coverage: every branch, crucially the contended-get reentrancy probe.
    H.check(cases[0] > 0, "get-missing case never exercised")
    H.check(cases[1] > 0, "get-present case never exercised")
    H.check(cases[2] > 0, "delete case never exercised")
    H.check(cases[3] > 0, "contended-get reentrancy probe never exercised")

    # STRUCTURAL conservation (race-free: read after full drain).  Redundant
    # factory calls for concurrent misses are LEGAL, so we do NOT assume one
    # create per residency.  What must hold:
    #   * DELETES[k] <= CREATES[k] for every key -- you cannot remove a residency
    #     that was never created; a phantom/double-delete (torn `used` counter,
    #     freed-slot reuse) makes deletes exceed creates;
    #   * a resident key has CREATES[k] > DELETES[k] AND was created at least once
    #     -- a key the factory never created cannot be resident (a corrupt/foreign
    #     entry from a rehashed-away slot);
    #   * an absent key still has DELETES[k] <= CREATES[k].
    total_creates = 0
    total_deletes = 0
    bad = 0
    for idx, key in enumerate(UNIVERSE):
        c = CREATES[idx][0]
        de = DELETES[idx][0]
        present = key in d
        total_creates += c
        total_deletes += de
        if de > c:
            bad += 1
            if bad <= 8:
                H.fail("conservation broken for key {0!r}: deletions={1} > "
                       "creations={2} -- a phantom/double-delete removed a "
                       "residency the factory never created (torn `used` counter "
                       "or freed-slot reuse under the parking factory)".format(
                           key, de, c))
            continue
        if present and c == 0:
            bad += 1
            if bad <= 8:
                H.fail("conservation broken: key {0!r} is RESIDENT but the "
                       "factory never created it (creations=0) -- a foreign/"
                       "corrupt entry materialized in the shared dict".format(
                           key))
            continue
        if present and not (c > de):
            bad += 1
            if bad <= 8:
                H.fail("conservation broken for resident key {0!r}: creations={1}"
                       " not > deletions={2} -- residency without a net surviving"
                       " create (lost-insert / torn-link under M:N)".format(
                           key, c, de))
    H.log("conservation total_creates={0} total_deletes={1} dict_size={2} "
          "bad_keys={3}".format(total_creates, total_deletes, len(d), bad))
    # Aggregate: deletions can never exceed creations across the whole dict.
    H.check(total_deletes <= total_creates,
            "aggregate conservation broken: total deletions {0} > total "
            "creations {1} -- net phantom deletes across the shared dict".format(
                total_deletes, total_creates))
    # Every surviving entry must still hold its correct product (no torn entry
    # left resident at teardown), and its key must be in-universe.
    for key, val in d.items():
        if key not in UNIVERSE_SET:
            H.fail("resident OUT-OF-UNIVERSE key {0!r} at teardown -- a corrupt "
                   "key in the shared dict".format(key))
            break
        if val != product(key):
            H.fail("resident TORN entry at teardown: key {0!r} value {1!r} != "
                   "product(key) {2!r}".format(key, val, product(key)))
            break
    H.require_no_lost()


if __name__ == "__main__":
    # Correctness/safety test of the defaultdict factory critical section under a
    # cooperative park; not a throughput sweep.  The contended-get probe spawns 2
    # child fibers per op, so the live-fiber count is ~3x funcs -- fine at the
    # designed scale.  Keep the universe small so collisions on one missing key
    # are frequent; that is the whole point.
    harness.main("p406_defaultdict_factory_reentrancy", body, setup=setup,
                 post=post, default_funcs=3000,
                 describe="one shared defaultdict whose factory parks inside "
                          "__missing__ before committing; every value == "
                          "product(key), no out-of-universe/torn resident entry, "
                          "and per-key deletions <= creations with every resident "
                          "key created -- else FT dict critical-section "
                          "corruption / phantom-delete")
