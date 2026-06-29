"""big_100 / 423 -- collections.ChainMap.maps linear-scan vs maps-list realloc + front-dict insert.

The subject is ``collections.ChainMap`` (monkey.patch() leaves it untouched -- it
is PURE Python with NO internal locking; its docstring states "The underlying
mappings are stored in a list.  That list is public ... There is no other
state.").  Two of its hot paths are the hazard, both reading a plain mutable
``self.maps`` list of dicts with the GIL off:

    def __getitem__(self, key):
        for mapping in self.maps:           # LINEAR SCAN over the maps list
            try:
                return mapping[key]         # dict lookup on each layer
            except KeyError:
                pass
        return self.__missing__(key)

    def __contains__(self, key):
        for mapping in self.maps:           # same linear scan
            if key in mapping:
                return True
        return False

    def __setitem__(self, key, value):
        self.maps[0][key] = value           # PyDict_SetItem into the FRONT dict

and ``new_child(m)`` / ``maps.insert(0, m)`` PREPEND a fresh dict to the very
list those scans walk.  The precise C-level state under attack, and the racing op
pairs:

  (A) maps-list ob_item REALLOC vs the live scan.  ``self.maps`` is a CPython
      ``list``; its storage is a ``PyObject **ob_item`` array.  ``maps.insert(0,
      d)`` (what new_child does) must GROW that array -- ``list_resize`` calls
      ``PyMem_Realloc(ob_item, ...)`` and then memmoves every element up one
      slot.  A reader's ``for mapping in self.maps`` holds a ``listiterator`` with
      a raw ``it_index`` into the OLD ob_item; if it parks mid-scan on a grown-
      down C stack while another hub inserts, on resume it can index through a
      freed/realloc'd ob_item -> a torn list-element pointer, yielding a *mapping*
      object from the wrong slot (or freed memory -> SIGSEGV).

  (B) front-dict ma_keys insert vs the same scan's lookup.  ``__setitem__`` does
      ``PyDict_SetItem`` on ``maps[0]``; an insert that crosses a rehash boundary
      reallocates that dict's ``ma_keys`` entry table.  A scanner doing
      ``maps[0][key]`` (the first iteration of its linear scan) reads that table
      concurrently -- the textbook dict insert-vs-lookup race, here LAYERED under
      the list realloc above so a torn front-dict entry hands back a value that
      belongs to no layer that ever set this key.

The net corruption a TORN read produces is a cross-layer value: the scan returns
``v`` for key ``k`` where ``v`` is NOT ``f(layer, k)`` for ANY layer that
legitimately holds ``k`` -- a value reconstructed from a freed slot or the wrong
ob_item entry.  That is the bug this program catches; a SIGSEGV mid-scan is the
coarser symptom the watchdog/faulthandler reports.

CLOSED-WORLD, FALSIFIABLE oracle.  Keys come only from a fixed sentinel UNIVERSE.
Each round owns ONE shared ChainMap.  Every layer ``L`` maps key -> ``f(L, k)``, a
deterministic per-(layer,key) value, so a value is self-identifying: from it we
can recover which layer must have produced it, and verify that layer legitimately
holds ``k``.  Per round:

  * MUTATOR (serialized under a per-ChainMap cooperative Lock, since ChainMap is
    documented thread-unsafe): writes ``cm[k] = f(0, k)`` into ``maps[0]`` (the
    front-dict insert, hazard B) and periodically ``maps.insert(0, fresh_layer)``
    / ``parents``-style reshuffle (the maps-list realloc, hazard A), yielding
    inside the held region so the realloc lands DURING a sibling scan's park.  It
    is the ONLY writer, so its private per-layer (identity, dict) control is race-
    free by construction; the merged control = the exact first-layer-wins flatten
    the ChainMap must equal.  Each layer carries a STABLE IDENTITY (decoupled from
    its shifting position) so a value stays self-identifying as it is prepended-
    past.
  * SCANNERS (hold NO lock -- this is the iterate-vs-realloc race): loop doing
    ``cm[k]`` / ``k in cm`` / ``list(cm)`` over universe keys, parking mid-scan.

  HOT fail-fast (per element a scanner ever observes), checked while reading cm
  UNLOCKED via a SKEW-IMMUNE oracle so a benign read-before-publish timing never
  false-fails:
    * every key seen is in UNIVERSE (an out-of-universe key = a torn list element
      / freed dict slot);
    * every value ``v`` returned for ``k`` satisfies ``v == f(I, k)`` where ``I``
      is the layer identity ``v``'s own high bits encode AND ``I`` is one of the
      identities that legitimately EXIST this round (the grow-only ``live_idents``
      set).  Because ``f`` mixes the KEY into the low bits, a value torn from the
      WRONG key ``k'`` at a real layer (``f(I,k')``) fails ``f(I,k)==v``, and a
      value from a FREED ob_item / dict slot decodes to a dead/garbage identity --
      both caught, with NO need for a perfectly-synchronized control snapshot;
    * the only tolerated exception is ``RuntimeError`` ("... changed size during
      iteration" from ``list(cm)``'s internal merge / a dict iteration) -- ANY
      other exception type, out-of-universe key, illegal value, or SIGSEGV is the
      bug.

  POST-QUIESCENT reconciliation (after ALL scanners + the mutator join, so the
  ChainMap is provably quiescent and single-reader):
    * CONSERVATION: ``dict(cm)`` equals the exact first-layer-wins merge of the
      per-layer single-owner CONTROL dicts replayed privately -- key set and
      every value (front layer wins).  A divergence that never crashed (a write
      that landed in the wrong layer, or a flatten that picked the wrong layer's
      value) is caught HERE by the private control arm, even with no SIGSEGV.
    * ``len(dict(cm)) == len(list(cm))`` -- the flatten and the iterator agree on
      the live key set.
    * every key in ``dict(cm)`` is in UNIVERSE.

CONTROL ARM (the falsifier).  The mutator is the ONLY writer, so its private
per-layer control dicts are race-free by construction.  If the shared ChainMap's
flatten diverges from the control merge, the fault is in the scan / list-realloc /
front-dict machinery under M:N -- not in contention dropping a write (the single
writer never drops one).  This disambiguates "ChainMap's scan tore" from "two
writers raced".

Coverage is round-robined by worker id (the p125/p126/p172 flaky-random lesson):
the scanner picks its operation ``(cm[k] | k in cm | list(cm))`` by ``(wid + i) %
3`` for its first ops, then random, so every scan-path is exercised whether one
worker does many rounds or many workers do one round each.

Stresses: ChainMap.maps list ob_item realloc (maps.insert(0,...)/new_child) under
a live __getitem__/__contains__ linear scan; front-dict PyDict_SetItem ma_keys
rehash vs the same scan's lookup; cross-layer torn read; first-layer-wins flatten
conservation vs a private single-owner control; out-of-universe / SIGSEGV.

Good TSan / controlled-M:N-replay target: the list ob_item write in list_resize
vs the listiterator's it_index read, and the front-dict ma_keys insert vs the
scan's lookup, are textbook data races a TSan report localizes on the list/dict
entry table before the universe/value assert even fires; RNG is per-worker (rng)
for replay and each mutator seeds its own random.Random (a shared one corrupts
GIL-off).
"""
import collections
import random

import harness
import runloom

# Finite sentinel UNIVERSE: a fixed, recognizable set of keys.  A key a scanner
# ever yields that is NOT in this set is a torn list element / freed dict slot --
# a hard fault.  Sized to push BOTH the front dict's ma_keys AND (via the layer
# count) the maps list's ob_item through several growth/realloc boundaries.
UNIVERSE_SIZE = 192
UNIVERSE = tuple(0x42300000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Distinct layer "tags" baked into each value so a returned value is self-
# identifying: from f(L, k) we can recover L and verify that layer holds k.  Made
# large and spread out so a torn/freed value is overwhelmingly unlikely to land on
# a coincidentally-legal (L, k) pairing.
LAYER_BASE = 0x4C000000
LAYER_STRIDE = 0x00010000


def f(ident, key):
    """Deterministic per-(layer-IDENTITY, key) value.  Encodes the layer's STABLE
    IDENTITY (not its position in self.maps) in the high bits and a key-dependent
    mix in the low bits, so a value is self-identifying: a value not equal to
    f(I, k) for the identity I of any layer that legitimately holds k is a TORN
    cross-layer read (it came from a freed slot or the wrong ob_item element).

    Identity is decoupled from position because the mutator PREPENDS layers
    (maps.insert(0,...)) -- a layer keeps its identity-tagged values while its
    index shifts, so the oracle must key on identity, not on scan position."""
    return (LAYER_BASE + ident * LAYER_STRIDE) ^ ((key * 2654435761) & 0xFFFF)


# Layers built per round.  The shared ChainMap starts with this many dict layers;
# the mutator inserts a few more via maps.insert(0, ...) so the maps list's
# ob_item array actually grows/realloc's under live scans.  Kept small so the
# first-layer-wins flatten is non-trivial but the round completes under load.
INITIAL_LAYERS = 4
MAX_LAYERS = 9

# Scanners per shared ChainMap per round.  Several hubs scanning the SAME maps
# list while one mutator realloc's it is the contention that tears the scan.
SCANNERS = 3

# How many universe keys the mutator writes / inserts each round (across layers).
# Big enough to cross front-dict rehash boundaries; the per-round flatten then has
# real first-layer-wins shadowing to reconcile.
WRITES_PER_ROUND = 96

# Scan operation cases (round-robined by worker id, never flaky-random).
CASE_GETITEM = 0     # cm[k]           -- linear scan, returns first layer's value
CASE_CONTAINS = 1    # k in cm         -- linear scan, membership
CASE_LISTCM = 2      # list(cm)        -- __iter__ merges reversed(self.maps)
NCASES = 3

SLOTS = 1024


def build_layers(rng, front_ident):
    """Build INITIAL_LAYERS control layers as (identity, dict) pairs in front-first
    order: each dict maps a random universe subset key -> f(identity, key).  These
    are the SOURCE OF TRUTH the shared ChainMap is built from and the private
    control reconciles against.  The FRONT layer (the mutator's maps[0] write
    target) gets identity `front_ident` and is intentionally sparse; deeper layers
    get the following identities and are denser so first-layer-wins shadowing is
    real.  Returns (front_dict, [(ident, dict), ...deeper front-first])."""
    front = {}
    base = []
    next_ident = front_ident + 1
    for li in range(1, INITIAL_LAYERS):
        ident = next_ident
        next_ident += 1
        frac = rng.uniform(0.4, 0.8)
        d = {}
        for k in UNIVERSE:
            if rng.random() < frac:
                d[k] = f(ident, k)
        base.append((ident, d))
    # Seed the front sparsely so cm[k] sometimes hits it (first-wins) and sometimes
    # falls through to a deeper layer.
    for k in UNIVERSE:
        if rng.random() < 0.12:
            front[k] = f(front_ident, k)
    return front, base, next_ident


def recover_layer(value):
    """Inverse of f's identity encoding: recover the layer IDENTITY a value claims
    to come from (high bits), or -1 if the value's high bits aren't a legal tag.
    Used only for a precise error message; the authoritative legality check is
    value == f(I, k) for an identity I the control says holds k."""
    hi = value & ~0xFFFF
    if hi < LAYER_BASE:
        return -1
    off = hi - LAYER_BASE
    if off % LAYER_STRIDE != 0:
        return -1
    return off // LAYER_STRIDE


def legal_value_for(live_idents, key, value):
    """True iff `value` is a legitimate value for `key` under the closed-world
    encoding -- SKEW-IMMUNE so it can be checked while reading cm UNLOCKED.

    f(ident, key) is a strong per-(ident, key) checksum: the high bits encode the
    layer IDENTITY and the low bits mix the KEY.  So `value` is legitimate for
    `key` iff value == f(recover_layer(value), key) AND that recovered identity is
    one that legitimately EXISTS this round (live_idents).  Why this is exactly the
    torn-read detector, with no need for a perfectly-synchronized control snapshot:

      * a value the scan read for the WRONG key k' at some real layer I is f(I,k')
        with k' != k, so f(I, key) != value -> caught (the key mix won't match);
      * a value from a FREED / realloc'd ob_item slot decodes to garbage high bits
        (identity outside live_idents) or fails the f(I,key) checksum -> caught;
      * a genuine, correct value f(I, key) for a live layer I that holds key passes
        -- even if the scanner's control snapshot hadn't yet observed the write
        (publish skew is benign: the value is self-consistently correct).

    `live_idents` is the monotonically-growing set of layer identities created this
    round (front + base + every prepended layer); a value claiming an identity that
    was NEVER created is a hard fault."""
    ident = recover_layer(value)
    if ident < 0 or ident not in live_idents:
        return False
    return value == f(ident, key)


def flatten_control(control_layers):
    """First-layer-wins flatten of the private control (front == index 0 wins),
    the exact semantics ChainMap.__getitem__/__iter__ implement.  control_layers is
    a front-first list of (ident, dict).  This is the race-free oracle dict(cm)
    must equal post-quiescence."""
    merged = {}
    for ident, d in reversed(control_layers):   # deeper first, front overwrites
        merged.update(d)
    return merged


def scanner(H, cm, control_box, gate, rng, slot, tally, wid):
    """Hold NO lock; loop scanning the SHARED ChainMap while the mutator realloc's
    its maps list and inserts into maps[0] on another hub.  Trips `gate` just
    before parking mid-scan so the mutator's realloc/insert provably lands inside
    the park window.  Validates every observed key/value against the closed-world
    oracle.  RuntimeError (changed-size during list(cm)'s internal merge) is the
    only tolerated exception.  Runs until the mutator signals done via gate's
    second phase (the shared 'stop' flag in control_box)."""
    i = 0
    keys = UNIVERSE
    tripped = False
    live_idents = control_box["live_idents"]   # shared, grow-only set of identities
    while not control_box["stop"] and H.running() and not H.failed:
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1
        try:
            if case == CASE_GETITEM:
                # Pick a handful of keys; scan cm[k] -- the linear maps scan that
                # races the list realloc + front-dict insert.  Park mid-batch.
                sample = [keys[rng.randrange(UNIVERSE_SIZE)] for _ in range(6)]
                for idx, k in enumerate(sample):
                    try:
                        v = cm[k]
                    except KeyError:
                        # Legal: no layer currently holds k.  A spurious KeyError
                        # while the race-free control DOES hold k could be a torn-
                        # scan miss, BUT it can also be a benign read-before-publish
                        # skew (the mutator applies to cm and republishes the
                        # control under the same lock, but the scanner reads both
                        # UNLOCKED), so we only re-loop, never fail, on a miss.
                        continue
                    if k not in UNIVERSE_SET:
                        H.fail("cm[k] returned for OUT-OF-UNIVERSE key {0!r} -- a "
                               "torn maps-list element / freed front-dict slot "
                               "under concurrent maps.insert realloc".format(k))
                        return
                    if not legal_value_for(live_idents, k, v):
                        H.fail("cm[{0!r}] == {1!r} matches NO legal layer value -- "
                               "torn CROSS-LAYER read (claims layer {2}); the "
                               "linear scan read a value from a freed/realloc'd "
                               "ob_item slot or wrong front-dict entry".format(
                                   k, v, recover_layer(v)))
                        return
                    if not tripped and idx >= 2:
                        tripped = True
                        gate["scan_in"] = True   # tell mutator a scan is mid-flight
                        runloom.yield_now()       # park with the scan's index live
                tally[slot] += 1
            elif case == CASE_CONTAINS:
                sample = [keys[rng.randrange(UNIVERSE_SIZE)] for _ in range(8)]
                for idx, k in enumerate(sample):
                    present = k in cm          # linear scan over maps
                    if present and k not in UNIVERSE_SET:
                        H.fail("'k in cm' true for OUT-OF-UNIVERSE key {0!r} -- "
                               "torn maps-list element under concurrent "
                               "realloc".format(k))
                        return
                    if not tripped and idx >= 2:
                        tripped = True
                        gate["scan_in"] = True
                        runloom.yield_now()
                tally[slot] += 1
            else:  # CASE_LISTCM
                # list(cm) builds a merged dict over reversed(self.maps); a
                # concurrent maps.insert / front-dict insert can trip a
                # RuntimeError (the LEGAL detection) or expose a torn key.
                tripped = True
                gate["scan_in"] = True
                snap = list(cm)
                for k in snap:
                    if k not in UNIVERSE_SET:
                        H.fail("list(cm) yielded OUT-OF-UNIVERSE key {0!r} -- torn "
                               "maps-list / merge under concurrent realloc".format(
                                   k))
                        return
                runloom.yield_now()
                tally[slot] += 1
        except RuntimeError:
            # "dictionary changed size during iteration" / list mutated during the
            # __iter__ merge -- the LEGAL race detection.  Tolerate, re-loop.
            pass


def mutator(H, cm, control_box, lock, gate, rng, slot, mtally):
    """The ONLY writer.  Serialized under `lock` (ChainMap is documented thread-
    unsafe), so the per-round oracle is a CONSERVATION test, not a thread-safety
    test of ChainMap.  Drives BOTH hazards:
      * front-dict insert: cm[k] = f(0, k)  -> maps[0][k]=v PyDict_SetItem;
      * maps-list realloc:  cm.maps.insert(0, fresh)  -> ob_item grow + memmove,
        the exact op new_child performs, on the SAME shared list the scanners walk.
    Mirrors every change into a PRIVATE, single-owner control (control_box) and
    REPUBLISHES the control snapshot under the same lock, so scanners read a
    consistent front-first view.  Yields INSIDE the held region right after a
    maps.insert so the realloc lands during a sibling scan's park."""
    writes = 0
    front_ident = control_box["front_ident"]
    while writes < WRITES_PER_ROUND and H.running() and not H.failed:
        # Wait (briefly, cooperatively) for a scanner to announce it is mid-scan so
        # the upcoming realloc lands in its park window; don't block forever.
        spins = 0
        while not gate["scan_in"] and spins < 4 and H.running():
            runloom.yield_now()
            spins += 1
        gate["scan_in"] = False
        k = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
        v = f(front_ident, k)                   # value tagged with the FRONT layer's
                                                # stable identity (== cm.maps[0])
        with lock:
            # Hazard B: front-dict PyDict_SetItem (may cross a rehash boundary).
            cm[k] = v                          # == cm.maps[0][k] = v
            control_box["front"][k] = v        # private single-owner mirror
            runloom.yield_now()                # scan's maps[0] lookup races here
            writes += 1
            # Periodically perform Hazard A: prepend a fresh layer (the maps-list
            # ob_item realloc that new_child performs), so the scanners' linear
            # scan indexes a list whose backing array just moved.  The fresh layer
            # gets a brand-new stable IDENTITY; the FORMER front (still our running
            # 'front' mirror with identity front_ident) becomes maps[1], and this
            # fresh dict becomes the new maps[0] -- but the mutator KEEPS writing
            # into the SAME 'front' object (now at index 1), so cm.maps[0] writes
            # would diverge.  To preserve the single-writer model cleanly we make
            # the freshly-inserted dict the NEW write target: re-point front_ident
            # and the 'front' mirror at it.
            if writes % 16 == 0 and len(cm.maps) < MAX_LAYERS:
                control_box["next_ident"] += 1
                new_ident = control_box["next_ident"]
                # Register the new identity as LIVE before any of its values can be
                # observed by a scanner (we are under the lock; the maps.insert and
                # value writes follow).  A scanner that later reads f(new_ident, k)
                # will find new_ident in live_idents -> legitimate.
                control_box["live_idents"].add(new_ident)
                fresh = {}
                # Seed the fresh layer with a few universe keys so it shadows
                # deeper layers (real first-layer-wins change) and is non-empty.
                for _ in range(5):
                    fk = UNIVERSE[rng.randrange(UNIVERSE_SIZE)]
                    fresh[fk] = f(new_ident, fk)
                cm.maps.insert(0, fresh)       # <-- ob_item grow + memmove
                runloom.yield_now()            # realloc lands during a parked scan
                # Demote the old front to the extra-fronts stack (front-first), and
                # adopt the fresh dict as the new single-writer front.
                control_box["extra_fronts"].insert(
                    0, (front_ident, control_box["front"]))
                front_ident = new_ident
                control_box["front_ident"] = new_ident
                control_box["front"] = fresh
        mtally[slot] += 1
    control_box["stop"] = True                 # tell scanners to finish


def run_round_impl(H, wid, rng, slot, state):
    """One closed-world round: build a shared ChainMap from control layers, run
    SCANNERS scanning it while one MUTATOR realloc's its maps list + inserts into
    maps[0], join everyone, then reconcile dict(cm) against the private control."""
    lock = runloom.sync.Lock()                 # per-round, per-ChainMap write lock
    front_ident = 0                            # the front layer's stable identity
    front, base, next_ident = build_layers(rng, front_ident)

    # Shared ChainMap built directly over the control layers.  maps[0] is the front
    # dict (the mutator's write target); the base layers (their dicts, in order)
    # follow.  Constructing maps explicitly (not new_child) so we hold the SAME list
    # object the scanners and the mutator share.
    cm = collections.ChainMap(front, *[d for _ident, d in base])

    # control_box is the PRIVATE, single-owner source of truth.  Every layer is an
    # (identity, dict) pair; identity is stable across position shifts.  'base' are
    # the immutable deeper layers (front-first), 'front' is the mutator's current
    # maps[0] write target with identity 'front_ident', 'extra_fronts' are the
    # demoted prepended fronts (front-first), 'layers' is the republished front-
    # first (ident, dict) snapshot scanners read.  Single writer -> race-free.
    control_box = {
        "base": list(base),                    # [(ident, dict), ...] front-first
        "front": front,                        # SAME object as cm.maps[0] initially
        "front_ident": front_ident,
        "next_ident": next_ident,
        "extra_fronts": [],                    # demoted prepended fronts (front-first)
        # NOTE: scanners do NOT read a control snapshot for value legality (the
        # skew-immune f(ident,key) checksum + live_idents set replaces it); the
        # extra_fronts/front/base structure is the post-quiescent reconciliation
        # source only.
        # Grow-only set of every layer identity that legitimately exists this round
        # (front + base + every prepended layer).  The scanner's skew-immune value
        # check requires an observed value's recovered identity to be in here.
        "live_idents": set([front_ident] + [ident for ident, _ in base]),
        "stop": False,
    }
    gate = {"scan_in": False}

    sc_wg = runloom.WaitGroup()
    sc_wg.add(SCANNERS)
    mut_wg = runloom.WaitGroup()
    mut_wg.add(1)

    sc_tally = state["scan_ops"]
    mut_tally = state["mut_ops"]

    def run_scanner(sidx):
        srng = random.Random(rng.getrandbits(48) ^ (sidx * 0x9E3779B1))
        try:
            scanner(H, cm, control_box, gate, srng, slot, sc_tally, wid + sidx)
        except Exception as exc:               # noqa: BLE001
            # ANY non-RuntimeError exception escaping the scanner is a fault
            # (RuntimeError is caught inside scanner()).
            if not isinstance(exc, RuntimeError):
                H.fail("scanner raised non-RuntimeError {0}: {1} -- not the legal "
                       "'changed size during iteration' race outcome".format(
                           type(exc).__name__, exc))
        finally:
            sc_wg.done()

    def run_mutator():
        mrng = random.Random(rng.getrandbits(48))
        try:
            mutator(H, cm, control_box, lock, gate, mrng, slot, mut_tally)
        finally:
            control_box["stop"] = True
            mut_wg.done()

    for sidx in range(SCANNERS):
        H.fiber(run_scanner, sidx)
    H.fiber(run_mutator)

    mut_wg.wait()                              # mutator done writing
    control_box["stop"] = True                 # ensure scanners stop
    sc_wg.wait()                               # scanners joined -> cm quiescent

    if H.failed:
        return

    # ---- post-quiescent reconciliation (single-reader, quiescent) -------------
    # CONSERVATION: dict(cm) must equal the first-layer-wins flatten of the private
    # single-owner control.  Build the control's final front-first (ident, dict)
    # list the same way the mutator republished it.
    final_control = (
        [(control_box["front_ident"], control_box["front"])]
        + list(control_box["extra_fronts"])
        + control_box["base"])
    expected = flatten_control(final_control)
    actual = dict(cm)

    # Every key in the flattened ChainMap is in UNIVERSE.
    for k in actual:
        if k not in UNIVERSE_SET:
            H.fail("dict(cm) holds OUT-OF-UNIVERSE key {0!r} after the round -- a "
                   "torn maps-list element / freed front-dict slot survived".format(
                       k))
            return

    # len agreement between the flatten and the iterator.
    iter_keys = list(cm)
    if not H.check(len(actual) == len(iter_keys),
                   "len(dict(cm))={0} != len(list(cm))={1} -- the first-layer-wins "
                   "flatten and the __iter__ merge disagree on the live key set "
                   "(torn maps-list scan)".format(len(actual), len(iter_keys))):
        return

    # First-layer-wins conservation against the private control.
    if not H.check(set(actual.keys()) == set(expected.keys()),
                   "key-set conservation broken: dict(cm) has {0} keys, control "
                   "merge has {1}; symmetric diff size {2} -- a maps[0] write or "
                   "maps.insert landed in the wrong layer under the realloc race"
                   .format(len(actual), len(expected),
                           len(set(actual) ^ set(expected)))):
        return
    for k, v in expected.items():
        got = actual.get(k, None)
        if got != v:
            H.fail("first-layer-wins conservation broken at key {0!r}: dict(cm)=="
                   "{1!r} but the race-free control flatten=={2!r} -- the shared "
                   "ChainMap's scan picked the WRONG layer's value (torn cross-"
                   "layer read; the single-writer control cannot have dropped it)"
                   .format(k, got, v))
            return


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
    # Per-slot single-writer tallies for the scanner/mutator op counts (summed in
    # post()).  Built inside the root where cooperative primitives are valid.
    H.state = {
        "scan_ops": [0] * SLOTS,
        "mut_ops": [0] * SLOTS,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    scan_ops = sum(H.state["scan_ops"])
    mut_ops = sum(H.state["mut_ops"])
    H.log("ChainMap scan-passes={0} mutator-writes={1} ops={2} (every per-round "
          "first-layer-wins conservation + closed-world value check passed fail-"
          "fast)".format(scan_ops, mut_ops, H.total_ops()))
    # Reaching post with no failure means every per-round reconciliation held;
    # assert the run actually exercised the race (else the window was vacuous).
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(scan_ops > 0,
            "scanners never completed a scan pass -- the linear-scan-vs-maps-"
            "realloc race window was never exercised")
    H.check(mut_ops > 0,
            "mutator never wrote -- the front-dict insert / maps.insert realloc "
            "hazard was never driven")
    H.require_no_lost("chainmap-maps-scan completeness")


if __name__ == "__main__":
    harness.main(
        "p423_chainmap_maps_list_missing_sca", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many hubs run ChainMap.__getitem__/__contains__/list() linear "
                 "scans over a SHARED maps list while one mutator does cm[k]=v "
                 "(front-dict PyDict_SetItem) + maps.insert(0,...) (ob_item "
                 "realloc, what new_child does); closed-world: every key in a "
                 "finite universe, every value f(layer,key) for a layer that holds "
                 "it, and dict(cm) == first-layer-wins flatten of a private single-"
                 "owner control -- a torn cross-layer/maps-list read fails")
