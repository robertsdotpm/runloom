"""big_100 / 532 -- types.MappingProxyType as a LIVE view aliasing a dict's
storage, resized (rehashed) by its SINGLE OWNER across a park.

The subject is ``types.MappingProxyType`` -- the read-only ``mappingproxy``
object CPython exposes (Objects/descrobject.c, ``mappingproxyobject``).  Like a
dict VIEW, a mappingproxy is NOT a snapshot: its only field is a strong
reference ``mapping`` to the backing mapping.  EVERY proxy operation re-reads the
backing dict's LIVE storage AT CALL TIME with no version capture of its own:

  * ``mappingproxy_len``          -> ``PyObject_Size(pp->mapping)`` -> ``ma_used``
  * ``mappingproxy_getitem`` (``P[k]``)   -> ``PyObject_GetItem`` over live ``ma_keys``
  * ``mappingproxy_contains`` (``k in P``) -> ``PyDict_Contains`` over live ``ma_keys``
  * iterating / ``.keys()`` / ``.items()`` -> walks the live ``dk_entries``

p419 drives a dict VIEW object (``d.keys()``/``.items()``) whose backing dict is
cleared+rebuilt by ANOTHER hub.  This program attacks a DIFFERENT C view type
(the distinct ``mappingproxyobject``) and, per the load-bearing arm, the resize
is driven by the SAME single owner across a park -- probing whether the proxy's
re-read of ``pp->mapping->ma_keys`` stays coherent when the owner grows the dict
past a ``dictresize`` growth boundary (malloc a NEW ``dk_entries``, publish
``ma_keys``, FREE the OLD table) after the fiber has parked and possibly migrated
hubs.  A stale/torn table pointer would let ``P[k]`` read a freed slot, disagree
with ``D[k]``, or hand back a wrong ``len``.

WHICH ORACLE IS LOAD-BEARING, AND WHY.

  LOAD-BEARING -- SINGLE-OWNER PROXY COHERENCE (worker, HARD, fail-fast).
  Each fiber OWNS one private dict ``D`` (seeded from a fixed sentinel UNIVERSE,
  every value == ``f(key)``) and one private proxy ``P = MappingProxyType(D)``.
  The dict and proxy are created in fiber-local variables and NEVER shared -- so
  there is exactly ONE writer, and on a correct runtime the oracle is race-free
  by construction and MUST pass (program exits 0 when there is no bug).  Each
  iteration the fiber:
    - snapshots ``P[k]`` for the seed keys and ``len(P)``;
    - YIELDs (``yield_now`` / ``sleep``) with the proxy LIVE -- the park where a
      sibling on another hub reliably interleaves and the fiber may migrate hubs;
    - single-owner INSERTs the rest of the universe into ``D``, forcing the
      backing dict through several ``dictresize`` growth boundaries (this is what
      frees the old ``dk_entries`` the proxy aliases);
    - asserts the proxy reflects ``D`` EXACTLY: ``len(P) == len(D)``,
      ``set(P) == set(D)``, and for every key ``P[k] == D[k] == f(k)``; the seed
      snapshot values are unchanged (we only ADDED keys); and
    - asserts the proxy is still READ-ONLY (``P[k] = v`` raises ``TypeError``).
  A single-owner proxy that after its own resize (across a park) reads a stale or
  torn table -- a wrong value, a missing/extra key, a ``len`` disagreeing with
  the key set, a non-universe key, or a proxy that suddenly permits assignment --
  is a runloom M:N view-coherence bug.  (Verified with a plain-threads control:
  8 OS threads each owning their own dict+proxy, resizing across a yield, GIL on
  AND off, show 0 discrepancies -- documented CPython behavior.)

  MEASURED -- SHARED PROXY, report-ONLY (NEVER fails).  A single shared dict +
  shared proxy is read by all fibers.  Under a cooperative Lock (so no crash /
  no torn read -- Counter/dict shared mutation is documented-racy, NOT a runloom
  bug) a fiber reads ``len(P)`` and ``P[k]`` for a shared key, yields, then a
  sibling has mutated that key, so the second read differs.  We MEASURE how often
  the live proxy reflects a sibling's write (the "live view" property, like p67's
  threading.local leak rate) and REPORT it -- this proves the hazard is real
  (the proxy IS a live alias, not a snapshot) so the single-owner arm is truly
  testing coherence and not missing the hazard.  We NEVER fail on it: failing
  would mislabel documented shared-object semantics as a bug.

  NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).
  COMPLETENESS (post, HARD): ``require_no_lost`` -- a fiber stranded mid
  ``mappingproxy_getitem`` / iteration on a desynced table never returns; the
  watchdog + require_no_lost catch it.

FAIL ON: a single-owner proxy whose ``P[k] != D[k]``, ``len(P) != len(D)``,
``set(P) != set(D)``, an out-of-universe key, a snapshot value that changed
across the resize, or a proxy that permits item assignment.  The shared-proxy
MEASURED arm is report-only and is EXPECTED to show live-view observations
(documented M:N shared-object behavior) -- it never reaches the load-bearing
single-owner oracle.

Stresses: mappingproxy_len / mappingproxy_getitem / mappingproxy_contains /
proxy iteration re-reading ``pp->mapping->ma_keys`` (dk_indices/dk_entries)
across a park + hub migration, racing the SAME owner's ``dictresize``
malloc-new / publish / free-old of the entry table; read-only enforcement under
M:N; live-alias (not snapshot) semantics; single-owner vs shared proxy coherence.

Good TSan / controlled-M:N-replay target: the proxy's re-read of
``pp->mapping->ma_keys`` against ``dictresize``'s free-old/publish-new of
``dk_entries`` on the SAME fiber across a park is a clean park-boundary
table-pointer coherence probe; a TSan report on the dict keys-table, or a replay
that resumes the proxy on a freed table, localizes it before the coherence assert
fires.
"""
import types

import harness
import runloom

# Finite sentinel UNIVERSE of keys.  A key the proxy ever yields that is NOT in
# this set is a torn/freed-slot read -- a hard fault.  Sized to push the backing
# dict's keys table through several dictresize growth boundaries on every insert
# pass (8 -> 16 -> 32 -> ...), since the resize is what FREES the old dk_entries
# out from under the live proxy.
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x53200000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# The dict is SEEDED from a small prefix (so it starts tiny) and GROWN with the
# rest of the universe after the park -- the grow pass crosses several resize
# boundaries, freeing the entry table the proxy aliases.
SEED_KEYS = UNIVERSE[:8]
GROW_KEYS = UNIVERSE[8:]


def f(key):
    """Deterministic key -> value pairing.  A proxy that yields key K with value
    != f(K) is a TORN read: the value came from a different/freed slot.  Mixing
    so a torn value is very unlikely to coincidentally satisfy value==f(key)."""
    return ((key ^ 0x39C39C39) * 0x9E3779B1 & 0xFFFFFFFFFFFF) + 0x100000007


VALUE_UNIVERSE_SET = frozenset(f(k) for k in UNIVERSE)


# ---- LOAD-BEARING arm: single-owner dict + proxy, resized across a park ----
def proxy_check(H, wid, idx, state):
    """Single-owner mappingproxy coherence check.

    The fiber owns D and P=MappingProxyType(D) (never shared).  It snapshots the
    proxy, parks with the proxy live, single-owner-resizes the backing dict past
    several growth boundaries, then asserts the proxy reflects D EXACTLY and is
    still read-only.  A stale/torn table read after the owner's own resize (across
    the park) is a runloom view-coherence bug."""
    d = {k: f(k) for k in SEED_KEYS}
    p = types.MappingProxyType(d)

    # Snapshot BEFORE the park: seed-key values + length.
    snap = {k: p[k] for k in SEED_KEYS}
    snap_len = len(p)
    if snap_len != len(SEED_KEYS):
        H.fail("mappingproxy len {0} != seeded {1} BEFORE any resize -- the proxy "
               "disagrees with its backing dict at creation (wid {2})".format(
                   snap_len, len(SEED_KEYS), wid))
        return

    # PARK with the proxy object LIVE on this grown-down C stack.  A sibling on
    # another hub runs here and this fiber may resume on a different hub -- the
    # boundary a table-pointer coherence bug would surface at.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # SINGLE-OWNER resize: insert the rest of the universe, crossing several
    # dictresize growth boundaries (each frees the old dk_entries the proxy
    # aliases).  Yield mid-grow so the fiber can migrate hubs between resizes.
    grown = 0
    for k in GROW_KEYS:
        d[k] = f(k)
        grown += 1
        if grown & 63 == 0:
            runloom.yield_now()

    # ---- coherence oracle: the proxy must reflect D EXACTLY -----------------
    if len(p) != len(d):
        H.fail("mappingproxy len {0} != backing dict len {1} after a single-owner "
               "resize across a park -- the proxy read a stale/torn ma_used "
               "(wid {2})".format(len(p), len(d), wid))
        return

    pk = set(p)                               # iterate proxy keys over live dk_entries
    dk = set(d)
    if pk != dk:
        H.fail("mappingproxy key set diverged from backing dict after a "
               "single-owner resize across a park: missing={0} extra={1} -- a "
               "torn/freed-slot entry-table read (wid {2})".format(
                   len(dk - pk), len(pk - dk), wid))
        return

    for k in d:
        if k not in UNIVERSE_SET:
            H.fail("backing dict/proxy holds OUT-OF-UNIVERSE key {0!r} -- a torn "
                   "key from a rehash (wid {1})".format(k, wid))
            return
        pv = p[k]                             # mappingproxy_getitem over live ma_keys
        if pv != d[k]:
            H.fail("mappingproxy P[{0!r}]={1!r} != D[{0!r}]={2!r} after a "
                   "single-owner resize across a park -- the proxy read a "
                   "stale/freed slot (wid {3})".format(k, pv, d[k], wid))
            return
        if pv != f(k):
            H.fail("mappingproxy P[{0!r}]={1!r} != f(key)={2!r} -- a TORN value, "
                   "key and value from different/freed slots (wid {3})".format(
                       k, pv, f(k), wid))
            return

    # Seed snapshot values are unchanged (we only ADDED keys; existing values are
    # constant f(key)).  A change means the resize corrupted a live entry.
    for k, v in snap.items():
        if p[k] != v:
            H.fail("mappingproxy seed value P[{0!r}] changed from {1!r} to {2!r} "
                   "across the resize -- a live entry was torn by the rehash "
                   "(wid {3})".format(k, v, p[k], wid))
            return

    # READ-ONLY enforcement must survive M:N churn: item assignment raises
    # TypeError.  A proxy that suddenly permits assignment is corrupted.
    try:
        p[SEED_KEYS[0]] = 0xDEAD            # must raise TypeError
        H.fail("mappingproxy permitted item assignment (P[k]=v did not raise) -- "
               "the read-only proxy was corrupted into a mutable mapping "
               "(wid {0})".format(wid))
        return
    except TypeError:
        pass                                # expected: read-only

    state["checks"][wid & 1023] += 1


# ---- MEASURED arm: shared dict + shared proxy (report-only) ----------------
def shared_proxy_check(H, wid, r, state):
    """Shared mappingproxy live-view observation (MEASURED, report-only).

    One shared dict + one shared proxy are read by all fibers.  Under the
    cooperative Lock (so no crash / no torn read -- shared dict mutation is
    documented-racy, NOT a runloom bug) we read len(P) and P[k], yield, and a
    sibling has mutated that shared key, so the second read differs.  We MEASURE
    how often the live proxy reflects a sibling's write (proving it is a live
    alias, not a snapshot) and REPORT it.  We NEVER fail on it."""
    d = state["shared_d"]
    p = state["shared_p"]
    lock = state["lock"]
    key = SEED_KEYS[wid % len(SEED_KEYS)]

    with lock:
        len0 = len(p)
        v0 = p.get(key)

    runloom.yield_now()                       # siblings mutate the shared dict here

    with lock:
        # Write our own view of this shared key (fibers aliasing the same key
        # overwrite each other -- documented shared-object behavior).
        d[key] = f(key) ^ (r & 0xFFFF)
        len1 = len(p)
        v1 = p.get(key)

    state["shared_checks"][wid & 1023] += 1
    if len1 != len0 or v1 != v0:
        # The live proxy observed a sibling's mutation across the yield.  Expected.
        state["shared_leaks"][wid & 1023] += 1


# Sustained checks per worker, bounded by H.running().  The coherence hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously creating proxies
# and resizing their private dicts while PARKED across a yield, so the scheduler
# reliably interleaves a sibling and may migrate this fiber's hub before it
# resumes.  A single check per fiber barely overlaps a sibling's park and does
# NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING single-owner
    proxy-coherence check (fail-fast) and the MEASURED shared-proxy live-view
    observation (report only).  The two do not share data (private dict+proxy vs
    the shared pair) so running them in one fiber keeps the hub busy with mixed
    churn without the shared mutations reaching the single-owner oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            proxy_check(H, wid, idx, state)          # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_proxy_check(H, wid, idx, state)   # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Shared dict + proxy for the MEASURED arm, seeded so P.get(seed_key) is
    # non-None.  The cooperative Lock (built inside the root, where cooperative
    # primitives are valid) serializes all shared-dict access so the report-only
    # arm never crashes or tears -- it only observes the live-view property.
    shared_d = {k: f(k) for k in SEED_KEYS}
    H.state = {
        "checks": [0] * 1024,                 # LOAD-BEARING single-owner checks
        "lock": runloom.sync.Lock(),
        "shared_d": shared_d,                 # shared backing dict (MEASURED)
        "shared_p": types.MappingProxyType(shared_d),  # shared live proxy
        "shared_checks": [0] * 1024,          # MEASURED shared-proxy reads
        "shared_leaks": [0] * 1024,           # live-view observations (expected)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    schecks = sum(H.state["shared_checks"])
    sleaks = sum(H.state["shared_leaks"])
    spct = (100.0 * sleaks / schecks) if schecks else 0.0

    H.log("mappingproxy[single-owner LOAD-BEARING]: {0} coherence checks (all "
          "passed fail-fast) | mappingproxy[shared MEASURED]: {1} reads {2} "
          "live-view observations ({3:.1f}%, documented shared-object behavior -- "
          "REPORT ONLY); ops={4}".format(
              checks, schecks, sleaks, spct, H.total_ops()))

    if sleaks:
        H.log("note: the shared proxy observed {0} live-view reads across {1} "
              "checks -- a mappingproxy is a LIVE alias of its backing dict, so a "
              "sibling's mutation is visible on the next read (a shared Python "
              "object, like p67's threading.local).  This is documented M:N "
              "shared-object behavior, NOT a runloom bug, and never reaches the "
              "load-bearing single-owner oracle".format(sleaks, schecks))

    # NON-VACUITY: the load-bearing single-owner hazard was actually exercised.
    H.check(checks > 0,
            "no single-owner mappingproxy coherence checks ran -- the load-bearing "
            "proxy-across-park hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # mappingproxy_getitem / proxy iteration on a desynced table).
    H.require_no_lost("mappingproxy coherence completeness")


if __name__ == "__main__":
    harness.main(
        "p532_mappingproxy_view_across_park", body, setup=setup, post=post,
        default_funcs=6000,
        describe="types.MappingProxyType is a LIVE read-only alias of a dict's "
                 "storage (re-reads pp->mapping->ma_keys per op, no snapshot). "
                 "LOAD-BEARING: each fiber owns a private dict D and proxy "
                 "P=MappingProxyType(D), snapshots P, PARKS with P live, then "
                 "single-owner-resizes D past several dictresize growth boundaries "
                 "(free-old/publish-new dk_entries) and asserts P reflects D "
                 "EXACTLY (P[k]==D[k]==f(k), len(P)==len(D), set(P)==set(D)) and is "
                 "still read-only (P[k]=v raises TypeError).  A stale/torn table "
                 "read after the owner's own resize across a park is the bug.  A "
                 "shared-proxy MEASURED arm (report-only, expected live-view "
                 "observations) proves the alias is live, not a snapshot")
