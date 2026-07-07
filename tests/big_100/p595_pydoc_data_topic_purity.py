"""big_100 / 595 -- pydoc_data read-only doc-dict purity under M:N.

pydoc_data ships two process-GLOBAL, read-only dictionaries built once at import:

  * pydoc_data.topics.topics        -- 80 entries, help-topic-name -> help TEXT
    (the multi-KB reST-ish strings `help()` prints for "assert", "async", ...);
  * pydoc_data.module_docs.module_docs -- 315 entries, module-name -> doc-ref
    string (e.g. "__future__#module-__future__").

Both dicts are SHARED by every fiber on every hub, are never mutated after import,
and hold immutable `str` values.  That makes them a clean CLOSED-WORLD purity
target for the runtime: a read-only shared dict looked up concurrently from many
hubs MUST always return the SAME immutable string object with byte-identical
content.  There is no legitimate way for a lookup to return a different object, a
torn string, a wrong-key value, or a KeyError -- so ANY such observation is a real
M:N runtime fault (torn dict read, corrupted shared str object across a hub
migration, a lost/duplicated hash-table probe under free-threading), never
documented Python semantics.

WHERE M:N COULD BREAK IT (the gap this program probes).  The GIL-off dict is
supposed to make concurrent READ-ONLY lookups lock-free-safe (gh-116738-era
free-threading audit).  If a lookup on the shared dict races a hub migration and
returns a half-published bucket, or if the shared immutable `str` object's cached
fields (hash, length, char buffer) are observed torn across a yield while a sibling
touches the same object on another hub, a fiber would see a value that differs from
the golden content.  runloom parks the fiber across `yield_now`/`sleep` and can
resume it on a DIFFERENT hub, so the two halves of each check (before/after the
yield) are the read-before-migration and read-after-migration of the same shared
object.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  The value of `topics[key]` is fixed at
import and never changes.  We snapshot, in setup() on the root, the GOLDEN identity
(id) + GOLDEN crc32 over the encoded bytes + GOLDEN length of every entry in both
dicts.  A fiber then, for its fiber-local key:

  - reads s = D[key] and recomputes crc32 over its ACTUAL bytes (a real read of the
    whole char buffer, so a torn char is caught, not short-circuited by identity);
  - YIELDs (yield_now / tiny sleep) -- runloom may migrate it to another hub here;
  - re-reads s2 = D[key] and asserts: same object identity as the golden snapshot
    (a read-only dict must hand back the one interned value object), crc32 stable
    across the yield AND equal to the golden crc, byte length equal to golden.

This is a PURITY law: the recomputed checksum must be bit-identical across the
yield and match the closed-form golden captured before any fiber ran.  Verified
against plain threads (8 OS threads hammering the same two dicts, GIL on AND off):
100% of lookups return the identical object with identical crc -- 0 deviations.
Under a correct runloom it must also hold, so the oracle PASSES (exit 0) when there
is no bug.

The dicts are shared and read-only, so there is NO shared-mutable hazard to
mislabel: every arm is single-value-single-truth.  There is intentionally no
"MEASURED report-only" arm because a read-only shared dict has no documented racy
observation to report -- any deviation IS the bug.

ORACLES:
  * LOAD-BEARING -- READ-ONLY DICT PURITY (worker, HARD, fail-fast).  Per fiber-
    local key: id + crc32 + len of the looked-up string must equal the golden
    snapshot and be stable across a yield.  Also `key in D`, `D.get(key) is D[key]`,
    and membership consistency are asserted (a torn probe would drop the key).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a dict
    probe / crc loop on a torn shared object never returns; the watchdog catches it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

FAIL ON: a shared doc-string whose identity, byte content (crc32), or length
differs from the golden import-time snapshot, or changes across a yield; a
looked-up key that vanishes (KeyError / `key not in D`); `D.get(key)` returning a
different object than `D[key]`.  Every such event is a torn read of a read-only
shared object -- a real runtime bug.

Stresses: concurrent read-only lookups on two process-global shared dicts across
hub migration, crc32 over shared immutable str buffers before/after a yield, dict
membership + get/[]-consistency under free-threaded M:N, id/hash/len stability of
interned doc strings shared by tens of thousands of goroutines.

CPU/stdlib-only; no fds, no subprocess -- runs at full --funcs.
"""
import zlib

import harness
import runloom
import pydoc_data.topics
import pydoc_data.module_docs


def encode_bytes(s):
    """Encode a doc string to bytes for crc32.  surrogatepass keeps it total for
    any str the stdlib could ship (the pydoc_data strings are plain ASCII/UTF-8,
    but we stay defensive so the golden and the fiber recompute identically)."""
    return s.encode("utf-8", "surrogatepass")


def build_pool(H):
    """Flatten both read-only dicts into ONE ordered pool of probes and capture the
    GOLDEN import-time snapshot (id, crc32, len) for each.  Built on the root before
    any fiber runs, so the snapshot is the single source of truth every fiber checks
    against.  Returns (pool, golden) where pool is a list of (tag, key) and golden
    maps (tag, key) -> (id, crc, length)."""
    topics = pydoc_data.topics.topics
    module_docs = pydoc_data.module_docs.module_docs
    pool = []
    golden = {}
    for tag, D in (("T", topics), ("M", module_docs)):
        for key in D:
            s = D[key]
            ident = (tag, key)
            pool.append(ident)
            golden[ident] = (id(s), zlib.crc32(encode_bytes(s)), len(s))
    return pool, golden


def lookup_dict(state, tag):
    return state["topics"] if tag == "T" else state["module_docs"]


# Sustained checks per worker iteration.  The torn-read hazard only manifests under
# SUSTAINED concurrent lookups while fibers are parked across the yield and resumed
# on another hub, so a single probe barely overlaps a sibling's.
INNER_CAP = 100000


def purity_check(H, wid, idx, state):
    """Single-truth read-only purity check on one fiber-local pool entry.

    Selection is deterministic + fiber-local (rotates the pool by wid+idx) so every
    fiber walks its own slice and coverage is broad without any shared selection
    state.  The looked-up value is a SHARED read-only immutable str; the golden was
    captured at import.  A deviation is a torn read of a read-only shared object."""
    pool = state["pool"]
    golden = state["golden"]
    tag, key = pool[(wid + idx) % len(pool)]
    D = lookup_dict(state, tag)
    g_id, g_crc, g_len = golden[(tag, key)]

    # Membership must hold (a torn probe that drops the key is a hard fault).
    if key not in D:
        H.fail("read-only dict LOST KEY {0!r} (tag {1}) -- `key in D` is False for "
               "a key present at import; a torn hash-table probe dropped a bucket "
               "under M:N (wid {2})".format(key, tag, wid))
        return

    # `[]` and `.get()` must hand back the SAME shared object (no torn variant).
    s = D[key]
    if D.get(key) is not s:
        H.fail("read-only dict get()/[] DISAGREE for {0!r} (tag {1}) -- D.get(key) "
               "returned a different object than D[key] for a read-only shared "
               "dict (wid {2})".format(key, tag, wid))
        return

    # Recompute crc over the ACTUAL bytes BEFORE the yield (full char-buffer read).
    before_crc = zlib.crc32(encode_bytes(s))
    before_len = len(s)
    before_id = id(s)

    # YIELD: runloom may park + resume this fiber on a DIFFERENT hub here while
    # siblings hammer the same shared dict/strings on other hubs.
    runloom.yield_now()
    if idx & 3 == 0:
        runloom.sleep(0.0002)

    # Re-read and re-check against the golden import snapshot + the pre-yield read.
    s2 = D[key]

    # Identity: a read-only dict must return the one shared value object every time.
    if id(s2) != g_id or before_id != g_id:
        H.fail("read-only doc string IDENTITY CHANGED for {0!r} (tag {1}): golden "
               "id {2}, before-yield id {3}, after-yield id {4} -- a read-only "
               "shared dict returned a different object (torn bucket / republished "
               "value under M:N, wid {5})".format(
                   key, tag, g_id, before_id, id(s2), wid))
        return

    after_crc = zlib.crc32(encode_bytes(s2))
    after_len = len(s2)

    # Length stable + equal to golden (a torn ob_size would move it).
    if after_len != g_len or before_len != g_len:
        H.fail("read-only doc string LENGTH CHANGED for {0!r} (tag {1}): golden "
               "{2}, before {3}, after {4} -- torn length of a shared immutable "
               "str across a hub migration (wid {5})".format(
                   key, tag, g_len, before_len, after_len, wid))
        return

    # Content crc stable across the yield AND equal to the golden captured before
    # any fiber ran -- the closed-form purity law.
    if before_crc != g_crc or after_crc != g_crc:
        H.fail("read-only doc string CONTENT CHANGED for {0!r} (tag {1}): golden "
               "crc {2:#x}, before-yield crc {3:#x}, after-yield crc {4:#x} -- the "
               "shared immutable str's char buffer was observed torn across a yield "
               "(a real M:N runtime fault, wid {5})".format(
                   key, tag, g_crc, before_crc, after_crc, wid))
        return

    state["checks"][wid] += 1              # one writer per wid slot -- race-free


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            purity_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    pool, golden = build_pool(H)
    H.state = {
        "topics": pydoc_data.topics.topics,
        "module_docs": pydoc_data.module_docs.module_docs,
        "pool": pool,
        "golden": golden,
        "checks": [0] * H.funcs,           # ONE slot per worker (wid-indexed, race-free)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("pydoc_data read-only purity: {0} single-truth lookups over {1} pool "
          "entries (both topics + module_docs), every id/crc32/len matched the "
          "import-time golden fail-fast; ops={2}".format(
              checks, len(H.state["pool"]), H.total_ops()))
    # NON-VACUITY: the load-bearing purity arm actually ran.
    H.check(checks > 0,
            "no read-only purity checks ran -- the shared-dict lookup hazard was "
            "never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished inside a probe / crc loop.
    H.require_no_lost("pydoc_data read-only purity")


if __name__ == "__main__":
    harness.main(
        "p595_pydoc_data_topic_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="pydoc_data ships two process-global read-only dicts "
                 "(topics.topics: 80 help-text strings; module_docs.module_docs: "
                 "315 doc-refs), shared by every fiber and never mutated after "
                 "import.  LOAD-BEARING closed-world purity: a fiber snapshots the "
                 "golden id+crc32+len of a fiber-local key at import, looks it up, "
                 "recomputes crc over the real bytes, yields (possible hub "
                 "migration), and asserts the shared immutable str's identity, "
                 "content, and length are byte-identical to the golden and stable "
                 "across the yield.  A read-only shared dict MUST return the same "
                 "object with the same bytes every time -- any torn read / lost key "
                 "/ get-[]-disagreement is a real M:N runtime fault")
