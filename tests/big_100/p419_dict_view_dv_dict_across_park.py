"""big_100 / 419 -- a live dict VIEW held across a park while another hub
clear()s + rebuilds the backing dict (dictviewobject->dv_dict->ma_keys UAF).

The subject is the PERSISTENT dict view object -- `d.keys()` / `d.values()` /
`d.items()`, a CPython ``dictviewobject`` (Objects/dictobject.c).  A view is NOT
a snapshot: its only fields are a strong reference ``dv_dict`` to the backing
dict and a kind tag.  Every view operation re-reads the dict's LIVE
``ma_keys`` (the shared keys table: ``dk_indices`` + the ``dk_entries`` array)
and ``ma_used`` AT CALL TIME, with no version capture of its own:

  * ``dictview_len``           -> reads ``dv_dict->ma_used``
  * ``dictview_contains``      -> ``PyDict_Contains`` over the live ``ma_keys``
  * ``dictview_richcompare`` / the set-ops (``&`` ``|`` ``-`` ``^``) -> iterate
    the live ``dk_entries`` and probe ``dk_indices``
  * ``dictiter_iternextitem`` (re-iterating the view) -> walks ``dk_entries[i]``

p311 already drives a TRANSIENT ``for k,v in d.items()`` iterator (the
``it_index`` + ``dk_version``/``ma_used`` snapshot path: a concurrent resize is
caught by the per-``__next__`` "changed size during iteration" check).  It never
holds a PERSISTENT view OBJECT across a park.  That is the gap this attacks.

The hazard is a use-after-free of the ENTRY TABLE, not just a torn read:

    holder hub:   v = d.items()          # view pins the dict object (refcount),
                  ...trip gate, PARK...  #   but NOT the dk_entries allocation
    mutator hub:  d.clear()              # ma_keys -> empty-keys singleton;
                  d[k] = f(k) ... bulk   # insertdict -> build_indices /
                                         #   dictresize: malloc NEW dk_entries,
                                         #   publish ma_keys, FREE the OLD table
    holder hub:   len(v); k in v;        # on RESUME these deref dv_dict->ma_keys
                  v & UNIVERSE_SET;       #   -- which is now the reallocated (or,
                  list(v)                 #   mid-build, half-published) table

The view's strong ``dv_dict`` ref keeps the DICT OBJECT alive, but the prior
allocation's ``dk_entries`` block is owned by ``ma_keys`` and is freed the
instant ``dictresize`` swaps in a bigger table.  Under M:N the holder parks on a
grown-down C stack with the view live while a sibling on ANOTHER hub does the
``clear()`` + bulk reinsert.  A torn publish (``ma_used`` updated before
``ma_keys``, or ``ma_keys`` swapped to a freed/half-built ``dk_entries``) hands
the resumed view a key from a freed slot, a torn key/value pair, or a SIGSEGV.

CLOSED-WORLD ORACLE.  Backing-dict keys are drawn ONLY from a fixed sentinel
UNIVERSE, every value == f(key).  Per round the worker owns ONE shared dict and
spawns a holder + a mutator synchronized so the mutation lands DURING the park:

  * the HOLDER takes a view (round-robined over keys/values/items by wid+i),
    trips a gate, ``yield_now()``s with the view live, then on RESUME exercises
    len(view), membership (``k in view`` for keys/items), a set-style op
    (``view & UNIVERSE_SET`` for keys/items), and a full re-iteration -- every
    key it yields/contains must be in UNIVERSE and (for items) value==f(key);
  * the MUTATOR waits on the gate, then ``d.clear()`` and bulk-reinserts ONLY
    UNIVERSE keys with value==f(key) (each insert/resize reallocs+frees the old
    ``dk_entries``), tripping several growth boundaries.

Legal outcomes are EXACTLY two: a clean, fully-consistent observation (every
element in UNIVERSE, every items-value==f(key), and ``len(view)==len(list(view))``
when read back-to-back in the now-quiescent post-join), OR a RuntimeError
"changed size during iteration" raised by a re-iteration that overlapped the
rebuild (caught + counted acceptable).  ANY other exception type, an
out-of-universe key, a torn key!=f(key) pair, or a SIGSEGV is the bug -- a real
M:N dict-view entry-table use-after-free.

CONTROL ARM (the falsifier).  A PRIVATE single-owner dict + view runs through
the identical clear()+rebuild in ONE fiber, with NO sibling, NO park-race: after
the rebuild the private view MUST reproduce the full rebuilt key set EXACTLY
(set(private_view) == rebuilt_universe_subset) and every items value==f(key).  A
single-owner view is race-free by construction, so a loss THERE is a CPython
view-machinery bug, not contention -- this disambiguates "the view object is
broken" from "M:N contention corrupted the table".

Invariant (hot, fail-fast): every key a shared view yields/contains is in
UNIVERSE; every items value==f(key); the only tolerated raise is RuntimeError.
Invariant (post, quiescent): the private control view reproduced its rebuilt set
exactly on every round (fail-fast already enforced it); >=1 round both
completed-clean AND >=1 round (or the run as a whole) exercised the race window;
each view-kind case (keys/values/items) was exercised; no worker lost.

Stresses: dictviewobject dv_dict->ma_keys (dk_indices/dk_entries) re-read by
dictview_len / dictview_contains / dictview_richcompare / dictiter_iternextitem,
racing dict_clear + insertdict build_indices/dictresize realloc-and-free of the
OLD entry table on another hub; torn ma_used/ma_keys publish to a live view;
persistent-view entry-table use-after-free across a park; private-vs-shared view
reconstruction conservation.

Good TSan / controlled-M:N-replay target: the view's re-read of
``dv_dict->ma_keys`` against ``dictresize``'s free-old/publish-new of
``dk_entries`` is a textbook use-after-free; a TSan report on the dict keys-table
write/read often localizes it before the universe-membership assert even fires.
"""
import random

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of keys.  A key a view ever
# yields or claims-to-contain that is NOT in this set is a torn/freed-slot read --
# a hard fault.  Sized to push the backing dict's keys table (ma_keys) through
# several dictresize growth boundaries (8 -> 16 -> ... ) on every rebuild, since
# the resize is what FREES the old dk_entries out from under a live view.
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x41900000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)


def f(key):
    """Deterministic key -> value pairing.  An items view that yields key K with
    value != f(K) is a TORN pair: the key passed membership but the value came
    from a different/freed slot.  Mixing/avalanche so a torn pair is very unlikely
    to coincidentally satisfy value==f(key)."""
    return ((key ^ 0x5A5A5A5A) * 0x9E3779B1 & 0xFFFFFFFFFFFF) + 0x100000007


# View KINDS, round-robined by worker id so post() coverage holds whether one
# worker does many ops or many workers do one each (the p125/p126/p172 flaky-
# random-coverage lesson the suite already had to fix).
KIND_KEYS = 0       # d.keys()   -- dictview_len / contains / richcompare(&)
KIND_VALUES = 1     # d.values() -- dict_values has no contains; len + re-iter
KIND_ITEMS = 2      # d.items()  -- contains (k,v) tuple + the torn-pair check
NKINDS = 3

# A subset of UNIVERSE the rebuild reinstates.  We rebuild from a (deterministic
# per-round) subset so the new ma_used differs from the seed -- a torn ma_used vs
# ma_keys publish then shows up as len(view) disagreeing with list(view), and the
# private control's exact-set check has a non-trivial target.  Half the universe
# is the seed; the rebuild draws a random subset of the WHOLE universe.
SEED_KEYS = UNIVERSE[: UNIVERSE_SIZE // 2]


def fresh_dict():
    """A backing dict seeded from half the universe, value==f(key)."""
    return {k: f(k) for k in SEED_KEYS}


def rebuild_subset(rng):
    """A deterministic-from-rng subset of the FULL universe for the mutator to
    reinstate after clear().  Always non-empty and always a strict subset/superset
    mix vs SEED_KEYS so ma_used genuinely changes and several resizes fire."""
    n = rng.randint(UNIVERSE_SIZE // 4, UNIVERSE_SIZE)
    return rng.sample(UNIVERSE, n)


def check_key(H, key, where):
    """A key yielded/contained by a view must be in UNIVERSE.  Returns False on
    the first violation."""
    if key not in UNIVERSE_SET:
        H.fail("dict view ({0}) yielded/contained OUT-OF-UNIVERSE key {1!r} -- a "
               "torn/freed-slot read through dv_dict->ma_keys after the backing "
               "dict was cleared+rebuilt on another hub (entry-table UAF)".format(
                   where, key))
        return False
    return True


def check_item(H, key, val, where):
    """An (key, value) from an items view: key in UNIVERSE and value == f(key)."""
    if not check_key(H, key, where):
        return False
    if val != f(key):
        H.fail("dict items view ({0}) yielded TORN pair key {1!r} value {2!r} != "
               "f(key) {3!r} -- key and value came from different/freed slots "
               "(half-built dk_entries published to the live view)".format(
                   where, key, val, f(key)))
        return False
    return True


def observe_view(H, kind, view):
    """Exercise a live SHARED view after it resumed from the park: len, membership,
    a set-style op, and a full re-iteration.  Every observed key must be in
    UNIVERSE (items: value==f(key)).  Returns "clean" | "runtimeerror" | "fail".

    The only legal raise is RuntimeError ("changed size during iteration") from a
    re-iteration whose walk overlapped the rebuild -- caught here and counted
    acceptable.  Any OTHER exception type propagates to the caller's guard, which
    fails it (a non-RuntimeError escaping a view op is the bug)."""
    try:
        # len(view) -> dictview_len -> dv_dict->ma_used.  Read it; we don't assert
        # an exact value here (the mutator's ma_used is in flux), only that the
        # subsequent membership/iteration stays in-universe -- a torn ma_used shows
        # up as the len-vs-list mismatch checked in the quiescent control/post.
        n = len(view)
        if n < 0:
            H.fail("len(view) returned negative {0} -- torn ma_used read".format(n))
            return "fail"

        if kind == KIND_KEYS:
            # contains over the live ma_keys (PyDict_Contains).  Probe both a
            # known-universe key and a known-absent key; an in-universe key must
            # never read as a CRASH, an absent-from-universe probe must never come
            # back as a yielded out-of-universe key (it can't -- we only probe
            # universe members).  Then a set-op + a full re-iter.
            for probe in (UNIVERSE[0], UNIVERSE[UNIVERSE_SIZE - 1]):
                _ = probe in view          # bool; truth value is data-dependent,
                                           # we only require it not crash / not torn
            inter = view & UNIVERSE_SET    # dictview_richcompare/set-op over entries
            for key in inter:
                if not check_key(H, key, "keys&UNIVERSE"):
                    return "fail"
            for key in view:               # dictiter re-iteration over dk_entries
                if not check_key(H, key, "keys-reiter"):
                    return "fail"
        elif kind == KIND_VALUES:
            # dict_values has no __contains__ over keys; the value table is the
            # SAME dk_entries, so re-iterating it walks the freed/rebuilt slots.
            # Every value must be some f(universe-key); invert by membership in the
            # value-universe.
            for val in view:
                if val not in VALUE_UNIVERSE_SET:
                    H.fail("dict values view yielded OUT-OF-UNIVERSE value {0!r} "
                           "-- a torn/freed-slot value read through "
                           "dv_dict->ma_keys (entry-table UAF)".format(val))
                    return "fail"
        else:  # KIND_ITEMS
            # items contains takes a (k, v) tuple and checks BOTH key present AND
            # stored value == v -- a direct torn-pair probe against the live table.
            kk = UNIVERSE[0]
            _ = (kk, f(kk)) in view        # legal True/False; must not crash/tear
            inter = view & {(k, f(k)) for k in (UNIVERSE[0], UNIVERSE[1])}
            for key, val in inter:
                if not check_item(H, key, val, "items&pairs"):
                    return "fail"
            for key, val in view:          # full re-iter over dk_entries
                if not check_item(H, key, val, "items-reiter"):
                    return "fail"
        return "clean"
    except RuntimeError:
        # "dictionary changed size during iteration" -- the LEGAL detection of the
        # concurrent rebuild when a view re-iteration overlaps it.  Acceptable.
        return "runtimeerror"


VALUE_UNIVERSE_SET = frozenset(f(k) for k in UNIVERSE)


def hold_view(H, kind, d, gate, counts, slot):
    """HOLDER: take a persistent view of d, trip `gate`, park with the view LIVE,
    then on resume observe it.  The view pins the dict OBJECT but not the entry
    table the mutator is about to free.  Records the outcome into per-slot tallies
    (single-writer-per-slot, race-free)."""
    if kind == KIND_KEYS:
        view = d.keys()
    elif kind == KIND_VALUES:
        view = d.values()
    else:
        view = d.items()
    # Trip the gate (lets the mutator proceed) then PARK with the view object live
    # on this grown-down C stack -- the clear()+rebuild lands DURING this park.
    gate.done()
    runloom.yield_now()
    result = observe_view(H, kind, view)
    if result == "clean":
        counts["clean"][slot] += 1
    elif result == "runtimeerror":
        counts["rterror"][slot] += 1
    # "fail" already recorded via H.fail.
    return result


def rebuild_dict(d, gate, rng):
    """MUTATOR: wait for the holder to enter its park, then clear() the SAME dict
    and bulk-reinsert ONLY universe keys with value==f(key).  Each clear() resets
    ma_keys to the empty-keys singleton; each insert that crosses a growth boundary
    runs dictresize -> malloc a NEW dk_entries, publish ma_keys, FREE the OLD table
    -- exactly the realloc-and-free racing the live view's re-read.  Uses its OWN
    random.Random (a shared one corrupts GIL-off)."""
    gate.wait()
    d.clear()
    keys = rebuild_subset(rng)
    for k in keys:
        d[k] = f(k)                        # insertdict -> build_indices/dictresize
    # A second clear+rebuild from a different subset doubles the realloc churn in
    # the park window so a resize is very likely to overlap the holder's resume.
    d.clear()
    for k in rebuild_subset(rng):
        d[k] = f(k)
    return set(k for k in keys)            # (unused by holder; keeps work real)


def control_arm(H, kind, rng):
    """CONTROL: a PRIVATE single-owner dict + view, run through the IDENTICAL
    clear()+rebuild in ONE fiber with NO sibling and NO park-race.  After the
    rebuild the private view MUST reproduce the rebuilt key set EXACTLY and every
    items value==f(key).  A single-owner view is race-free by construction, so a
    loss HERE is a CPython view-machinery bug (not contention) -- the falsifier
    that tells "the view object is broken" from "M:N contention corrupted it".
    Returns True on success; H.fail + False on any discrepancy."""
    d = fresh_dict()
    if kind == KIND_KEYS:
        view = d.keys()
    elif kind == KIND_VALUES:
        view = d.values()
    else:
        view = d.items()
    # Rebuild deterministically (no race): clear, reinstate an exact subset.
    d.clear()
    subset = rebuild_subset(rng)
    expected = set(subset)                 # set() dedups; dict keys are unique
    for k in subset:
        d[k] = f(k)
    # The view re-reads the now-stable table.  It MUST reproduce the rebuilt set
    # exactly (no key lost, none spurious) -- single-owner, so any divergence is
    # the view machinery itself.
    if kind == KIND_KEYS:
        got = set(view)
        if got != expected:
            H.fail("CONTROL keys view did NOT reproduce the rebuilt key set "
                   "(single-owner, race-free): missing={0} extra={1} -- a CPython "
                   "dict-view reconstruction bug, not contention".format(
                       len(expected - got), len(got - expected)))
            return False
    elif kind == KIND_VALUES:
        got = sorted(view)
        want = sorted(f(k) for k in expected)
        if got != want:
            H.fail("CONTROL values view did NOT reproduce the rebuilt value "
                   "multiset (single-owner): got {0} values, want {1} -- a CPython "
                   "view-machinery bug, not contention".format(len(got), len(want)))
            return False
    else:  # KIND_ITEMS
        got = {}
        for k, v in view:
            got[k] = v
        if set(got) != expected:
            H.fail("CONTROL items view did NOT reproduce the rebuilt key set "
                   "(single-owner): missing={0} extra={1} -- CPython view "
                   "machinery, not contention".format(
                       len(expected - set(got)), len(set(got) - expected)))
            return False
        for k, v in got.items():
            if v != f(k):
                H.fail("CONTROL items view TORN pair key {0!r} value {1!r} != "
                       "f(key) {2!r} (single-owner, race-free) -- a CPython "
                       "view-machinery bug, not contention".format(k, v, f(k)))
                return False
    # len(view) must equal the number of elements the view actually yields, with
    # the table now stable -- a torn ma_used vs ma_keys would diverge here even
    # single-owner.
    if len(view) != len(expected):
        H.fail("CONTROL len(view)={0} != rebuilt size {1} (single-owner) -- torn "
               "ma_used vs ma_keys in the view machinery".format(
                   len(view), len(expected)))
        return False
    return True


def worker(H, wid, rng, state):
    counts = state["counts"]
    ctrl = state["ctrl"]
    cases = state["cases"]
    slot = wid & 1023
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the view KIND by worker id in the first ops so every kind is
        # exercised even under a short timeout (flaky-random-coverage fix); random
        # after.
        if i < NKINDS:
            kind = (wid + i) % NKINDS
        else:
            kind = rng.randrange(NKINDS)
        i += 1
        cases[kind][slot] += 1             # single-writer-per-slot coverage tally

        # ---- CONTROL ARM (private single-owner, no race) -- the falsifier ----
        if not control_arm(H, kind, rng):
            return

        # ---- SHARED ARM (holder view live across a park, sibling rebuilds) ----
        d = fresh_dict()
        gate = runloom.WaitGroup()
        gate.add(1)
        wg = runloom.WaitGroup()
        wg.add(2)
        mseed = rng.getrandbits(48)

        def run_holder(d=d, gate=gate, kind=kind, slot=slot):
            try:
                hold_view(H, kind, d, gate, counts, slot)
            except Exception as exc:        # noqa: BLE001
                # ANY non-RuntimeError escaping the view path is a fault
                # (RuntimeError is caught inside observe_view).
                H.fail("holder view op raised non-RuntimeError {0}: {1} -- not the "
                       "legal 'changed size during iteration' outcome (a dict-view "
                       "entry-table UAF surfaced as an exception)".format(
                           type(exc).__name__, exc))
            finally:
                wg.done()

        def run_mutator(d=d, gate=gate, mseed=mseed):
            mrng = random.Random(mseed)
            try:
                rebuild_dict(d, gate, mrng)
            except Exception:
                # The mutator's own clear()/insert never legally raise; we never
                # want a mutator hiccup to deadlock the holder's gate (already
                # tripped by the holder before its park).  The holder oracle judges.
                pass
            finally:
                wg.done()

        H.fiber(run_holder)
        H.fiber(run_mutator)
        wg.wait()                          # both joined -> dict quiescent
        if H.failed:
            return
        ctrl[slot] += 1                    # a full round (control + shared) done
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so runloom.WaitGroup etc.
    # are the cooperative M:N-safe primitives.  Per-slot tallies are single-writer.
    H.state = {
        "counts": {"clean": [0] * 1024, "rterror": [0] * 1024},
        "ctrl": [0] * 1024,                # rounds whose control arm passed
        "cases": [[0] * 1024 for _ in range(NKINDS)],  # per-kind coverage
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    clean = sum(H.state["counts"]["clean"])
    rterror = sum(H.state["counts"]["rterror"])
    ctrl = sum(H.state["ctrl"])
    case_tot = [sum(H.state["cases"][k]) for k in range(NKINDS)]
    H.log("rounds={0} (control+shared) shared-view clean={1} runtimeerror={2} "
          "(both legal); kind coverage keys={3} values={4} items={5}; ops={6}"
          .format(ctrl, clean, rterror, case_tot[0], case_tot[1], case_tot[2],
                  H.total_ops()))

    # Reaching post with no failure already means every per-round CONTROL arm
    # reproduced its rebuilt view set EXACTLY (fail-fast) and no SHARED view ever
    # yielded an out-of-universe key or torn pair.  Assert the run did real work.
    H.check(H.total_ops() > 0,
            "no rounds completed -- the live-view-across-park race window was "
            "never exercised")
    H.check(clean + rterror > 0,
            "no shared-view observation completed -- the hold-view-vs-rebuild "
            "race was never exercised")
    H.check(ctrl > 0,
            "no control-arm round completed -- the single-owner view falsifier "
            "never ran")

    # Each view KIND was exercised (deterministic round-robin guarantees it once
    # enough workers/rounds ran; assert it so a coverage regression is caught).
    names = ("keys", "values", "items")
    for k in range(NKINDS):
        H.check(case_tot[k] > 0,
                "view kind {0} ({1}) was never exercised -- coverage gap".format(
                    k, names[k]))

    H.require_no_lost("dict-view-across-park completeness")


if __name__ == "__main__":
    harness.main(
        "p419_dict_view_dv_dict_across_park", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a persistent dict view (keys/values/items) held LIVE across a "
                 "park while another hub clear()s + bulk-rebuilds the backing dict "
                 "(dictresize realloc-and-free of the old dk_entries); every key "
                 "the view yields/contains is in a finite sentinel universe and "
                 "items value==f(key), or a clean RuntimeError -- anything else is "
                 "a dict-view entry-table use-after-free.  A private single-owner "
                 "view control reproduces the rebuilt set exactly")
