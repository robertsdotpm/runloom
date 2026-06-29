"""big_100 / 454 -- sys.intern() shared interned-string table under M:N.

The subject is sys.intern() and the per-interpreter INTERNED-STRING TABLE it
mutates.  On a free-threaded build that table -- ``interp->cached_objects
.interned`` (Objects/unicodeobject.c, the ``_Py_unicode_state.interned`` dict in
3.13/3.14t) -- is a single SHARED dict, and ``PyUnicode_InternInPlace`` mutates
it WITHOUT the GIL.  The exact internal state attacked and the racing op pair:

  * the table is a CPython dict; its ``dk_indices`` / ``dk_entries`` block and
    its ``ma_used`` / ``dk_version`` counters are REALLOC'd when a concurrent
    intern() inserts the key that grows the table past a load-factor boundary
    (``dictresize`` builds a NEW keys object and frees the old one);
  * racing op pair A -- ``PyUnicode_InternInPlace`` INSERT+rehash on one hub vs a
    sibling hub's ``PyUnicode_InternInPlace`` LOOKUP that did ``lookdict`` against
    the SAME (now stale) ``dk_indices`` pointer.  A torn rehash can return TWO
    distinct str objects for the same text (an IDENTITY SPLIT) or hand back a
    freed/realloc'd entry (a torn read -> out-of-universe value / SIGSEGV);
  * racing op pair B -- ``_PyUnicode_InternInPlace`` marks the winning string
    IMMORTAL (3.12+: SetInterned/_Py_SetImmortal writes ``ob_refcnt`` to the
    immortal sentinel) -- a refcount-FREEZE STORE -- while a sibling fiber that
    just looked the entry up holds a BORROWED reference across a park; the freeze
    store racing that borrow is a publish-vs-read on ``ob_refcnt``.

WHY THIS STRESSES FT.  Thousands of fibers across hubs interning the SAME finite
alphabet of text values drive concurrent INSERT + LOOKUP into that ONE shared
dict, while a sibling fiber parked mid-intern (on a grown-down C stack) holds a
borrowed reference to a table entry that another hub is rehashing or
immortalizing.  The load-bearing invariant is sys.intern()'s IDENTITY GUARANTEE:
for interned strings, ``a == b`` implies ``a is b``.  If the table tears, two
fibers interning the same text can walk away with two DISTINCT canonical objects
(``a == b`` but ``a is not b``) -- the guarantee silently broken, no crash.

CLOSED-WORLD ORACLE.  A finite sentinel UNIVERSE of N fixed text values.  Each
text value is BUILT FRESH per round from its parts (``"".join(...)`` -- never a
literal, so the constant-folder has NOT pre-interned it and every round forces a
real lookup-then-insert into the live table).  For each text we keep a shared
per-text CANONICAL CELL: the FIRST interned result, recorded ONCE under a
per-text guard that is DISTINCT from the intern table (a runloom Lock array, not
the dict).  Every later sys.intern(same text) MUST return an object that ``is``
that recorded canonical and whose ``str`` value ``==`` the original text.

  hot, fail-fast (the contended arm): for every intern, the result is a str, its
  value is in UNIVERSE, and -- once the canonical cell is set -- ``result is
  canonical``.  An identity split (``result is not canonical`` while ``result ==
  canonical``'s text) FAILS; a value not in UNIVERSE or a non-str (a torn/freed
  entry) FAILS.

CONTROL ARM.  A single-owner fiber interns the WHOLE universe ALONE, before/while
the contended pool runs, and records each text's canonical into a private
race-free dict (single writer -> no contention can corrupt it).  After the run,
the contended arm's canonical for each text MUST be ``is``-equal to the control's
canonical for that text.  A single-owner intern of one text must always return
the one canonical the table already holds, so if the CONTROL and CONTENDED
canonicals differ, the split is in CPython's intern machinery, not in our
accounting -- this is the falsifier that disambiguates "intern table tore" from
"our recording raced".

CONSERVATION.  In post(): the count of DISTINCT canonical object ids observed per
text == 1 (we collect ids into a per-text shared set under the per-text guard; a
torn table that handed out two objects for one text leaves that set at size 2 ->
identity split caught even if it slipped past the hot check).  And every str in
the canonical table is immortal-interned: ``sys.intern(canonical) is canonical``
still holds at quiescence.

COVERAGE (the p125/p126/p172 flaky-random lesson).  Each round a worker interns a
ROUND-ROBINED slice of the universe selected by ``(wid + i) % UNIVERSE_SIZE`` in
its first ops (then random), so under a short timeout every text is still hit by
some worker -- a pure-random pick would reliably MISS a text and leave its cell
unset.  post() asserts every text's canonical cell was set (the whole universe
was exercised) and matches the control.

Stresses: PyUnicode_InternInPlace insert+rehash vs concurrent lookup on a shared
dk_indices/dk_entries, dictresize realloc under a parked borrow, the
SetInterned/immortalize refcount-freeze store vs a concurrent borrow, the
``a==b -> a is b`` identity guarantee under GIL-off contention, torn/freed entry
(out-of-universe value / non-str), single-owner-control vs contended canonical.

Good TSan / controlled-M:N-replay target: the lookdict read and the dictresize
write on the shared interned dict are a textbook data race; a TSan report on the
interned-dict entry table, or a single identity split under replay, localizes the
torn rehash before the conservation set even closes.
"""
import sys

import harness
import runloom

# Finite sentinel UNIVERSE of text values.  Sized to push the interned dict
# through several growth/rehash boundaries (dictresize is what realloc's the
# entry table out from under a concurrent lookup).  Each text is a fixed,
# recognizable string built from a unique index so a torn/freed entry reads as a
# value NOT in UNIVERSE_SET.  We pick a distinctive prefix unlikely to collide
# with any pre-interned identifier already in the table.
UNIVERSE_SIZE = 384
PREFIX = "p454_intern_universe_token"

# Per-text shared cells live in arrays indexed by text-id (0..UNIVERSE_SIZE-1).
# We never use a shared dict keyed by the text itself for accounting, because
# THAT dict would be the very kind of contended structure under test -- the
# accounting must be race-free by construction.

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024


def text_for(idx):
    """Build text value #idx FRESH from parts -- NEVER a literal.

    A string literal would be pre-interned by the compiler's constant-folder, so
    sys.intern() on it would be a pure lookup that never inserts.  Joining parts
    at runtime yields a brand-new str object whose first intern() this round
    drives a real lookup-then-INSERT into the live table (the insert is what
    grows/rehashes it).  The text is deterministic in idx, so every fiber
    interning text-id idx feeds the table the SAME logical key and they must all
    converge on ONE canonical object."""
    # "".join keeps the result un-foldable; the numeric body makes each text
    # distinct and recognizable.  zfill so all texts are the same length class
    # (forces dict collisions/probing rather than trivially distinct hashes).
    return "".join((PREFIX, "_", str(idx).zfill(6), "_end"))


def build_universe_set():
    """The frozenset of all legal text values.  Any intern() result whose value
    is not in here is a torn/freed entry from the shared table -- a hard fault."""
    return frozenset(text_for(i) for i in range(UNIVERSE_SIZE))


def record_canonical(H, state, idx, obj):
    """Record obj as observed for text-id idx, under the per-text guard (a Lock
    DISTINCT from the intern table).  Sets the canonical cell the FIRST time and
    collects the id into the per-text observed-id set for the conservation check.

    Returns the canonical object for idx (the first one ever recorded).  The hot
    identity check compares the caller's obj against this return with `is`."""
    guard = state["guards"][idx]
    canon = state["canon"]
    seen_ids = state["seen_ids"]
    with guard:
        if canon[idx] is None:
            canon[idx] = obj
        seen_ids[idx].add(id(obj))
        return canon[idx]


def intern_and_check(H, wid, state, idx, universe_set):
    """Intern text-id idx and enforce the identity/value invariant.

    Builds the text fresh, interns it, and checks: result is a str; its value is
    in UNIVERSE_SET (else a torn/freed entry); and -- against the recorded
    canonical -- result `is` canonical (else an IDENTITY SPLIT: two distinct
    objects for one text, the broken `a==b -> a is b` guarantee).  Returns True
    on success, False on the first violation (caller stops)."""
    text = text_for(idx)

    # The racing insert/lookup is INSIDE this call: another hub may be growing
    # the table (dictresize) while we lookdict.  Yield right before so a sibling's
    # insert is more likely to land in our park window during the intern.
    runloom.yield_now()
    obj = sys.intern(text)

    if not isinstance(obj, str):
        H.fail("sys.intern() returned a NON-str object {0!r} (type {1}) for "
               "text-id {2} -- a torn/freed entry handed back from the shared "
               "interned dict under a concurrent rehash".format(
                   type(obj).__name__, obj, idx))
        return False

    # The interned object's VALUE must be exactly the text we fed (a torn read
    # over a realloc'd dk_entries could hand back a different text's bytes).
    if obj != text or obj not in universe_set:
        H.fail("sys.intern() returned OUT-OF-UNIVERSE value {0!r} for text-id "
               "{1} (expected {2!r}) -- torn/freed entry from the interned dict "
               "under concurrent insert+rehash".format(obj, idx, text))
        return False

    # Record (and on first sight, fix) the canonical object for this text under
    # the per-text guard, then enforce identity against it.
    canon = record_canonical(H, state, idx, obj)

    # Hold the borrowed reference across a park: a sibling hub may be
    # immortalizing (refcount-freeze store) or rehashing this very entry right
    # now.  On resume, identity must still hold.
    runloom.yield_now()

    if obj is not canon:
        # IDENTITY SPLIT: same text, two distinct canonical objects.  This is the
        # exact `a == b -> a is b` guarantee breaking with no crash.
        if obj == canon:
            H.fail("IDENTITY SPLIT: sys.intern() returned a DISTINCT object for "
                   "text-id {0} (id {1:#x}) than the recorded canonical (id "
                   "{2:#x}) although their values are equal {3!r} -- the interned "
                   "table handed out two objects for one text (torn lookup-vs-"
                   "insert rehash broke a==b => a is b)".format(
                       idx, id(obj), id(canon), obj))
        else:
            H.fail("sys.intern() canonical mismatch for text-id {0}: returned "
                   "{1!r} but canonical is {2!r} -- torn entry".format(
                       idx, obj, canon))
        return False
    return True


def control_intern_universe(H, state, universe_set):
    """CONTROL ARM (single-owner, race-free).  One fiber interns the WHOLE
    universe ALONE and records each text's canonical into a PRIVATE dict that no
    other fiber writes -- so contention cannot corrupt it.  After the run the
    contended arm's canonical for each text must be `is`-equal to this control's
    canonical: a single-owner intern of a text always returns the one object the
    table holds, so a difference localizes the fault to CPython's intern
    machinery, not our accounting.

    Runs concurrently with the contended pool (it is just another fiber on the
    hubs), so it ALSO contends on the table -- but because it alone writes the
    control dict, the control dict is a clean reference snapshot of "what the
    table canonicalized each text to"."""
    control = state["control"]
    for idx in range(UNIVERSE_SIZE):
        text = text_for(idx)
        obj = sys.intern(text)
        if not isinstance(obj, str) or obj != text or obj not in universe_set:
            H.fail("CONTROL: sys.intern() returned bad value {0!r} for text-id "
                   "{1} (expected {2!r}) -- torn/freed entry seen even by the "
                   "single-owner control".format(obj, idx, text))
            return
        # Single writer: race-free record.
        control[idx] = obj
        runloom.yield_now()


def worker(H, wid, rng, state):
    """Per-goroutine body: intern a round-robined slice of the universe and
    enforce identity/value each time.  Round-robin by (wid + i) in the first ops
    so coverage holds under a short timeout (the flaky-random fix); random after,
    seeded per-worker via rng for replay."""
    universe_set = state["universe_set"]
    tally = state["tally"]
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin coverage: text-id walks the whole universe deterministically
        # across (wid, i) before going random, so every text is interned by some
        # worker even if each worker manages only a handful of ops.
        if i < UNIVERSE_SIZE:
            idx = (wid + i) % UNIVERSE_SIZE
        else:
            idx = rng.randrange(UNIVERSE_SIZE)
        i += 1
        if not intern_and_check(H, wid, state, idx, universe_set):
            return
        tally[slot] += 1                 # single-writer-per-slot, race-free
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so runloom.sync.Lock is
    # the cooperative M:N-safe primitive.  These shared cells are accounting that
    # is DISTINCT from the intern table under test:
    #   guards[idx]  -- per-text cooperative Lock (NOT the intern dict) guarding
    #                   the canonical cell + observed-id set for text-id idx.
    #   canon[idx]   -- the FIRST interned object recorded for text-id idx (the
    #                   shared canonical the contended arm must converge on).
    #   seen_ids[idx]-- set of id() of every object the contended arm saw for
    #                   text-id idx; size must stay 1 (conservation: one canonical
    #                   object per text).  A torn table that split identity pushes
    #                   it to 2.
    #   control[idx] -- the single-owner control arm's canonical for text-id idx
    #                   (a private list; only the control fiber writes it).
    #   tally        -- per-slot count of successful interns (summed in post()).
    H.state = {
        "universe_set": build_universe_set(),
        "guards": [runloom.sync.Lock() for _ in range(UNIVERSE_SIZE)],
        "canon": [None] * UNIVERSE_SIZE,
        "seen_ids": [set() for _ in range(UNIVERSE_SIZE)],
        "control": [None] * UNIVERSE_SIZE,
        "control_wg": runloom.WaitGroup(),
        "tally": [0] * SLOTS,
    }


def body(H):
    # Spawn the single-owner CONTROL fiber FIRST (inside the root), so it interns
    # the whole universe alongside the contended pool.  It joins on its own
    # WaitGroup, waited on in post(), so the control snapshot is complete and
    # quiescent before we compare.
    universe_set = H.state["universe_set"]
    wg = H.state["control_wg"]
    wg.add(1)

    def run_control():
        try:
            control_intern_universe(H, H.state, universe_set)
        finally:
            wg.done()

    H.fiber(run_control)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    state = H.state
    # The control fiber was spawned in body(); join it so its snapshot is
    # complete and the table is quiescent before we read the cells.
    state["control_wg"].wait()

    total = sum(state["tally"])
    canon = state["canon"]
    control = state["control"]
    seen_ids = state["seen_ids"]
    universe_set = state["universe_set"]

    set_cells = sum(1 for c in canon if c is not None)
    ctrl_cells = sum(1 for c in control if c is not None)
    H.log("interns={0} ops={1} contended-canonicals-set={2}/{3} "
          "control-canonicals-set={4}/{5}".format(
              total, H.total_ops(), set_cells, UNIVERSE_SIZE,
              ctrl_cells, UNIVERSE_SIZE))

    if not H.check(H.total_ops() > 0,
                   "no interns completed -- the shared interned-table race window "
                   "was never exercised"):
        return

    # The control arm interns the WHOLE universe alone, so every control cell must
    # be set -- if not, the single-owner control itself lost a text (a CPython
    # machinery fault, not contention).
    if not H.check(ctrl_cells == UNIVERSE_SIZE,
                   "CONTROL did not record all {0} canonicals (got {1}) -- the "
                   "single-owner control arm failed to intern part of the "
                   "universe".format(UNIVERSE_SIZE, ctrl_cells)):
        return

    # COVERAGE: the deterministic round-robin walks text-id by (wid + i), so once
    # the contended arm has done at least UNIVERSE_SIZE interns total it has hit
    # every text and every canonical cell is set.  A tiny smoke run (funcs *
    # rounds < UNIVERSE_SIZE) legitimately can't cover the whole universe -- the
    # control arm still interns it ALL unconditionally, so identity/conservation
    # stays fully checked for every text the contended arm DID touch; we only
    # require FULL contended coverage once enough interns ran to make it
    # deterministic (this is the p125 lesson: don't assert coverage the op budget
    # can't fund).
    if total >= UNIVERSE_SIZE:
        H.check(set_cells == UNIVERSE_SIZE,
                "contended arm left {0}/{1} canonical cells unset despite {2} "
                "interns (>= universe size) -- the round-robin should have hit "
                "every text (coverage gap)".format(
                    UNIVERSE_SIZE - set_cells, UNIVERSE_SIZE, total))
    else:
        H.check(set_cells > 0,
                "contended arm set NO canonical cells -- no text was interned")

    # CONSERVATION + CONTROL reconciliation, per text.
    for idx in range(UNIVERSE_SIZE):
        c = canon[idx]
        if c is None:
            continue                     # unset cell already flagged above
        ctrl = control[idx]
        text = text_for(idx)

        # Canonical value sanity (post-quiescent re-read).
        if not H.check(c == text and c in universe_set,
                       "post: contended canonical for text-id {0} has value {1!r} "
                       "!= text {2!r} -- a torn/corrupted interned entry survived "
                       "the run".format(idx, c, text)):
            return

        # CONSERVATION: exactly ONE distinct canonical object id was ever observed
        # for this text.  Size 2+ == an identity split that may have slipped past
        # the hot check (e.g. the racing fiber recorded both before the guard
        # serialized them).
        nids = len(seen_ids[idx])
        if not H.check(nids == 1,
                       "CONSERVATION broken for text-id {0}: {1} DISTINCT "
                       "canonical object ids observed (expected 1) -- the interned "
                       "table handed out multiple objects for one text (identity "
                       "split under concurrent insert+rehash)".format(idx, nids)):
            return

        # CONTROL reconciliation: the contended arm's canonical MUST be the SAME
        # object the single-owner control interned.  `is`-inequality with equal
        # values is an identity split localized to the intern machinery (the
        # control arm has one writer, so its cell can't have been raced).
        if ctrl is not None:
            if not H.check(c is ctrl,
                           "CONTROL MISMATCH for text-id {0}: contended canonical "
                           "(id {1:#x}) is NOT the single-owner control canonical "
                           "(id {2:#x}), values {3!r} / {4!r} -- sys.intern() "
                           "produced two distinct objects for one text across the "
                           "contended and control arms (broken identity "
                           "guarantee)".format(idx, id(c), id(ctrl), c, ctrl)):
                return

        # IDENTITY guarantee at quiescence: re-interning the canonical returns the
        # SAME object (it is interned + immortal; a fresh-built equal text must
        # canonicalize back to it).
        again = sys.intern(text_for(idx))
        if not H.check(again is c,
                       "post: re-interning text-id {0} returned a DIFFERENT object "
                       "(id {1:#x}) than the recorded canonical (id {2:#x}) -- the "
                       "interned table no longer canonicalizes this text to one "
                       "object (identity guarantee lost)".format(
                           idx, id(again), id(c))):
            return

    H.require_no_lost("sys.intern identity/conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p454_sys_intern_table", body, setup=setup, post=post,
        default_funcs=3000,
        describe="thousands of fibers across hubs sys.intern() the SAME finite "
                 "universe of fresh-built texts, driving concurrent insert+rehash "
                 "vs lookup on the ONE shared interned dict; closed-world oracle: "
                 "every intern returns a str whose value is in-universe and `is` "
                 "the recorded canonical, exactly one canonical object per text "
                 "(conservation), and the contended canonical `is` the single-"
                 "owner control's -- an identity split (a==b but a is not b) or a "
                 "torn/freed entry fails")
