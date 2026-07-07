"""big_100 / 598 -- shelve.Shelf single-owner persistence round-trip under M:N.

shelve.open(path) returns a Shelf: a dict-like PERSISTENT store backed by a dbm
database (here dbm.sqlite3, the 3.14 default), whose values are arbitrary picklable
Python objects.  __setitem__ pickles the value and writes the bytes into the dbm
file; __getitem__ reads the bytes back and unpickles them.  Each Shelf also owns an
open dbm handle (an sqlite3 connection to its file) and an internal name cache.

Every one of those operations -- open(), the pickle dump, the dbm write, sync(),
the dbm read, the pickle load, close(), reopen() -- is a BLOCKING file/sqlite call
that the monkey layer OFFLOADS to the scheduler's blocking-worker pool.  A fiber
therefore PARKS and can MIGRATE hubs at every store and fetch.  The M:N gap this
program probes: if a fiber's Shelf handle, the pickle byte-buffer in flight, or the
key/value being written is corrupted or CROSSED with a sibling's across one of those
park/hub-migration boundaries, the persistent round-trip breaks -- a value comes
back not-equal to what was stored, a key vanishes or a foreign key appears, or the
value returned belongs to a DIFFERENT fiber's Shelf (a cross-fiber handle leak).

WHY THIS IS A LEGITIMATE SINGLE-OWNER / CLOSED-WORLD ORACLE (HARD RULE 2):
  Each fiber owns its OWN Shelf, at its OWN unique path (base/shelf_w<wid>), touched
  by NO other fiber.  A Shelf is a mutable container, so a SHARED Shelf under M:N
  would race exactly like a shared dict across OS threads -- documented, not a bug --
  which is precisely why we never share one.  Because the store is single-owner AND
  the value set is a KNOWN closed world encoding wid, we get two exact, falsifiable
  laws that a correct runtime must satisfy:

    * ROUND-TRIP (identity): for every key k, Shelf[k] == expected[k] AND
      type(Shelf[k]) is type(expected[k]).  pickle dump->store->fetch->load is a
      bijection for these plain values, so any inequality is torn bytes, a lost
      write, or a cross-fiber handle leak -- never Python semantics.
    * CONSERVATION (closed world): set(Shelf.keys()) == expected key set AND
      len(Shelf) == len(expected).  No key offered was dropped; no key outside this
      fiber's universe (i.e. a sibling's key) ever appears.

  Every expected value ENCODES wid (base = wid * VALUE_SCALE), so if a fiber's fetch
  ever returned a sibling's Shelf contents, the values would decode a DIFFERENT wid
  and the round-trip law would fire.  We verify the laws (a) on the same open handle
  after sync() + a yield, and (b) after close()+reopen() (flag='r'), forcing a full
  persist->reopen->reload cycle across additional park/migration boundaries.

  Verified against plain threads: 8 OS threads each driving its own Shelf at its own
  path (GIL on and off) round-trips 100% -- 0 mismatches, 0 lost keys.  So under a
  correct runloom the single-owner oracle PASSES (exit 0) when there is no bug.

ORACLES:
  * LOAD-BEARING -- ROUND-TRIP + CONSERVATION (worker, HARD, fail-fast).  Each
    fiber, per round: opens a fresh Shelf at its own path (flag='n' truncates), stores
    its KNOWN wid-encoded key->value universe (yielding between stores so a sibling
    reliably interleaves its own file I/O on this hub), sync()s, yields, and verifies
    both laws on the same handle; then close()s, yields, reopen()s read-only, and
    verifies both laws again on the persisted copy.  Single-owner: no Shelf, path, or
    value is ever shared.  A failure is a runloom desync (torn/lost/crossed value or
    key across a park/hub-migration), not shelve semantics.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-offload inside
    a dbm read/write/open (parked on a blocking-worker completion that never comes)
    never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (round_trips > 0).

FAIL ON: a persisted value not equal (or wrong type) to what this fiber stored, a
missing/extra/foreign key in this fiber's own Shelf, or a value decoding a different
wid (a cross-fiber Shelf-handle leak).  There is NO shared-Shelf arm -- a shared
mutable Shelf races by documented semantics, so it would only be a false-positive
generator; the whole design stays single-owner.

Resource discipline (HARD RULE 5): file/sqlite-heavy -- each fiber holds an open dbm
handle and churns a file per round, so max_funcs caps the forever loop's
--funcs 1000000 at 1000, the base tmpdir is shutil.rmtree'd at shutdown, and each
round reuses the SAME per-wid path (flag='n' truncates) so disk use stays bounded
across a --rounds 0 soak.

Stresses: shelve.Shelf __setitem__/__getitem__/sync/close/reopen over dbm.sqlite3,
the pickle dump/load round-trip, blocking file+sqlite offload with park/hub-migration
between every store and fetch, per-fiber Shelf-handle isolation, closed-world key
conservation and value identity across persist->reopen->reload.
"""
import os
import shelve

import harness
import runloom

# Each fiber's values are keyed off base = wid * VALUE_SCALE so a cross-fiber Shelf
# leak (fetching a sibling's file/handle) decodes a visibly different wid and trips
# the round-trip law.  Large enough that no two fibers' value bands overlap.
VALUE_SCALE = 1000000

# Number of keys in each fiber's closed-world universe.  Enough that the dbm file
# holds several rows (so keys()/len() have real work) and pickle sees varied shapes,
# small enough that hundreds of fibers each churn a file per round under the window.
NKEYS = 24

# File/sqlite-heavy: each fiber holds an open dbm handle + churns a file per round.
# Cap the forever loop's --funcs 1000000 well under the fd/sqlite ceiling.
MAX_FUNCS = 1000


def build_expected(wid):
    """This fiber's KNOWN closed-world key -> value universe.

    Deterministic in wid (no RNG needed): every value ENCODES base = wid*VALUE_SCALE
    through a variety of picklable shapes (int, float, str, bytes, tuple, list, dict,
    nested), so a fetch that returned a sibling's Shelf would decode a different wid
    and the round-trip law fires.  Returns an ordered dict of string-key -> value."""
    base = wid * VALUE_SCALE
    expected = {}
    for i in range(NKEYS):
        v = base + i
        shape = i % 8
        if shape == 0:
            val = v
        elif shape == 1:
            val = float(v) + 0.5
        elif shape == 2:
            val = "w{0}_i{1}_v{2}".format(wid, i, v)
        elif shape == 3:
            val = ("t", v, wid, i)
        elif shape == 4:
            val = [v, v + 1, v + 2, wid]
        elif shape == 5:
            val = {"wid": wid, "i": i, "v": v, "nested": [v, {"k": v}]}
        elif shape == 6:
            val = ("bytes-tag", str(v).encode("ascii"), wid)
        else:
            val = {"pair": (v, wid), "list": [i, v, wid], "s": "v{0}".format(v)}
        expected["k{0:03d}".format(i)] = val
    return expected


def verify(H, wid, sh, expected, phase):
    """Assert both closed-world laws on this fiber's OWN Shelf handle `sh`.

    Returns True on success; on any violation calls H.fail and returns False.  All
    checks are exact: the key set must equal this fiber's universe (conservation) and
    every value must be == and same-type as stored (round-trip identity)."""
    # CONSERVATION: the persisted key set is exactly this fiber's universe.
    try:
        got_keys = set(sh.keys())
    except Exception as exc:               # a torn/failed dbm read is a hard fault
        H.fail("shelve keys() raised {0}: {1} (wid {2}, phase {3}) -- the dbm "
               "handle or its file was corrupted across a park/hub-migration".format(
                   type(exc).__name__, exc, wid, phase))
        return False
    exp_keys = set(expected.keys())
    if got_keys != exp_keys:
        missing = sorted(exp_keys - got_keys)
        extra = sorted(got_keys - exp_keys)
        H.fail("shelve CONSERVATION broken: wid {0} phase {1} key set differs -- "
               "missing {2!r} extra {3!r} (a store was lost, or a sibling's key "
               "leaked into this fiber's own single-owner Shelf across a park/"
               "hub-migration)".format(wid, phase, missing[:6], extra[:6]))
        return False
    if len(sh) != len(expected):
        H.fail("shelve CONSERVATION broken: wid {0} phase {1} len(Shelf)={2} != "
               "{3} expected -- a key was dropped or duplicated in this fiber's "
               "own Shelf".format(wid, phase, len(sh), len(expected)))
        return False

    # ROUND-TRIP: every value comes back exactly equal + same type as stored.
    for k in exp_keys:
        expv = expected[k]
        try:
            got = sh[k]
        except Exception as exc:
            H.fail("shelve fetch Shelf[{0!r}] raised {1}: {2} (wid {3} phase {4}) "
                   "-- torn pickle bytes or a corrupted dbm handle across a park".format(
                       k, type(exc).__name__, exc, wid, phase))
            return False
        if type(got) is not type(expv):
            H.fail("shelve ROUND-TRIP broken: wid {0} phase {1} key {2!r} returned "
                   "type {3} expected {4} -- torn value or cross-fiber Shelf leak".format(
                       wid, phase, k, type(got).__name__, type(expv).__name__))
            return False
        if got != expv:
            H.fail("shelve ROUND-TRIP broken: wid {0} phase {1} key {2!r} == {3!r} "
                   "but {4!r} was stored -- a lost/torn write, or this fetch returned "
                   "a DIFFERENT fiber's Shelf contents (cross-fiber handle leak)".format(
                       wid, phase, k, got, expv))
            return False
    return True


def run_shelf_round(H, wid, base_dir, state):
    """One single-owner round: build this fiber's known universe, store it into its
    OWN Shelf with yields between stores, verify both laws on the live handle, then
    close+reopen and verify the persisted copy.  Reuses the same per-wid path each
    round (flag='n' truncates) so a --rounds 0 soak stays disk-bounded."""
    path = os.path.join(base_dir, "shelf_w{0}".format(wid))
    expected = build_expected(wid)
    keys = list(expected.keys())

    # ---- store phase: write the whole universe, yielding mid-write so a sibling
    # reliably interleaves its own offloaded dbm I/O on this hub before we finish.
    sh = shelve.open(path, flag="n")       # 'n' = always a fresh empty db
    try:
        for i, k in enumerate(keys):
            sh[k] = expected[k]            # pickle dump + dbm write (offloaded)
            if (i & 3) == 3:
                runloom.yield_now()        # sibling stores/fetches during our write
        sh.sync()                          # flush to the dbm file (offloaded)
        runloom.yield_now()                # park across the store/verify boundary
        # ---- verify on the LIVE handle (same-handle round-trip) ------------------
        if not verify(H, wid, sh, expected, "live"):
            return
    finally:
        sh.close()

    # ---- reopen phase: force a full persist -> reopen -> reload cycle across more
    # park/hub-migration boundaries, then verify the persisted copy read-only.
    runloom.yield_now()
    sh2 = shelve.open(path, flag="r")      # read-only reopen of the persisted file
    try:
        if not verify(H, wid, sh2, expected, "reopened"):
            return
    finally:
        sh2.close()

    state["round_trips"][wid] += 1         # single-writer-per-slot (wid) -> race-free


def worker(H, wid, rng, state):
    base_dir = state["base"]
    for _ in H.round_range():
        if not H.running():
            break
        run_shelf_round(H, wid, base_dir, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    base = H.make_tmpdir("big100_shelve_")   # shutil.rmtree'd at shutdown

    # WARM UP the lazy imports + lazy global init BEFORE the fibers run.  shelve.open
    # does `import dbm` on first use, and dbm.open() then LAZILY imports the backend
    # and caches it in the module global dbm._defaultmod.  Under M:N those first-use
    # imports would run concurrently across hundreds of fibers, and a concurrent
    # import exposing a PARTIALLY-INITIALIZED module (dbm.open not yet bound) is
    # DOCUMENTED CPython import semantics -- NOT a runloom bug.  Doing one full
    # open/store/close here, single-threaded in the root, forces `import dbm`, the
    # backend detection (dbm.sqlite3), dbm._defaultmod caching, and the pickle
    # machinery to complete up front, so the fibers exercise only the (thread-safe,
    # single-owner) data path and the oracle can't false-positive on an import race.
    warm = os.path.join(base, "warmup")
    sh = shelve.open(warm, flag="n")
    sh["warm"] = ("warm", 0, [1, 2], {"k": 3})
    sh.sync()
    _ = sh["warm"]
    sh.close()
    sh = shelve.open(warm, flag="r")
    _ = sh["warm"]
    sh.close()

    H.state = {
        "base": base,
        # ONE slot per worker (wid-indexed, single-writer) -> race-free conservation
        # counter feeding the non-vacuity check.  Allocated here where H.funcs is known.
        "round_trips": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    rts = sum(H.state["round_trips"])
    H.log("shelve single-owner round-trips completed: {0} (every same-handle + "
          "reopened ROUND-TRIP/CONSERVATION check passed fail-fast); ops={1}".format(
              rts, H.total_ops()))
    # NON-VACUITY: the load-bearing single-owner persistence hazard actually ran.
    H.check(rts > 0,
            "no shelve round-trips completed -- the single-owner persist/reopen "
            "round-trip oracle was never exercised (would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid dbm offload.
    H.require_no_lost("shelve round-trip")


if __name__ == "__main__":
    harness.main(
        "p598_shelve_roundtrip", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=MAX_FUNCS,
        describe="each fiber owns its OWN shelve.Shelf (dbm.sqlite3) at its OWN "
                 "path and stores a KNOWN wid-encoded key universe with yields "
                 "between offloaded stores, then verifies two exact laws -- "
                 "ROUND-TRIP (Shelf[k]==stored, same type) and CONSERVATION "
                 "(key set == universe, len matches) -- on the live handle AND "
                 "after close()+reopen().  Single-owner + closed-world: a value "
                 "not equal to what was stored, a missing/foreign key, or a value "
                 "decoding a different wid (cross-fiber Shelf-handle leak) across a "
                 "park/hub-migration is the runloom bug; no shared Shelf exists.")
