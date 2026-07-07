"""big_100 / 510 -- unicodedata UCD read-cache isolation under M:N.

unicodedata answers every query -- normalize(), name(), lookup(), category(),
numeric(), decomposition(), combining(), bidirectional(), ... -- out of a single
process-wide, READ-ONLY Unicode Character Database compiled into the C extension.
Some of those queries (notably normalize() and the quick-check / canonical-
decomposition path) walk internal lookup structures and, in some CPython builds,
touch small mutable scratch/quick-check state while composing a result.  The C
code was written assuming a single logical thread of control per call; under the
GIL that held for free.  With the GIL OFF and runloom's M:N scheduler, a normalize
call may PARK at a cooperative yield mid-computation and RESUME on a DIFFERENT hub
(OS thread) while a sibling fiber is driving its OWN normalize/name/lookup over
the same shared C tables.  If any of that shared read path is not race-safe -- a
mutable quick-check cache, a shared decomposition scratch buffer, a static result
pointer -- this fiber could resume holding a SIBLING's codepoint data and produce
a result that does not correspond to its own input string.

WHERE M:N BREAKS IT (the gap this program probes).  The database itself is
immutable and shared; there is nothing wrong with sharing it.  The hazard is
purely whether the C reader keeps ANY per-call mutable state in a location that is
not on the C stack / not fiber-private.  A fiber owns its OWN input strings (built
fresh from a fixed codepoint pool, never shared).  It computes a batch of results,
yields (so the scheduler reliably interleaves a sibling that is hammering the same
tables), then recomputes the SAME queries on the SAME single-owner input and
asserts byte-identical results.  Because the input is single-owner and the DB is
read-only, the result is a pure function of the input: it MUST be identical across
the yield.  Any difference is a shared-read-path race in the C UCD reader --
this fiber's result was corrupted by a sibling's concurrent lookup.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Unicode normalization obeys mathematical laws that hold for EVERY string on a
  correct implementation, independent of concurrency:

    * IDEMPOTENCE:   NFC(NFC(x))  == NFC(x),   NFD(NFD(x))  == NFD(x),
                     NFKC(NFKC(x))== NFKC(x),  NFKD(NFKD(x))== NFKD(x)
    * COMPOSITION:   NFC(NFD(x))  == NFC(x),   NFD(NFC(x))  == NFD(x)
                     NFKC(NFKD(x))== NFKC(x)
    * IS_NORMALIZED: is_normalized('NFC', NFC(x)) is True (and NFD/NFKC/NFKD)
    * ROUND-TRIP:    lookup(name(ch)) == ch   for every ch in the pool (all pool
                     members are pre-filtered at import to have a round-trippable
                     name, so this is an always-true law, not a probe)
    * STABILITY:     name/category/combining/bidirectional/east_asian_width/
                     mirrored/decimal/digit/numeric/decomposition of a fixed ch
                     are constants of the DB -- identical on every call.

  All of these are pure functions of read-only data.  Under a CORRECT runtime they
  hold on every call regardless of hub migration, so the load-bearing oracle
  PASSES (program exits 0 when there is no bug).  A fiber recomputes the whole
  batch across a yield and asserts byte-identical output; a mismatch -- or a law
  that suddenly fails on a string for which it held one line earlier -- means the C
  reader returned another fiber's codepoint data, i.e. a UCD read-cache race in the
  runloom hub migration.  Single-owner: every input string is built fiber-local
  from an immutable codepoint pool and never handed to another fiber, so there is
  no shared-mutable container for the "documented M:N shared-object" escape hatch
  to apply -- a mismatch here can ONLY be a runtime corruption bug.

ORACLES:
  * LOAD-BEARING -- UCD READ ISOLATION (worker, HARD, fail-fast).  Per iteration a
    fiber builds a fresh single-owner string, snapshots (a) the four normal forms,
    (b) the normalization laws above, and (c) a per-character DB snapshot for every
    char in the pool sample.  It yields (yield_now + occasional tiny sleep so a
    sibling parks/migrates in the same window), then recomputes everything and
    asserts: the normal forms are byte-identical across the yield, the laws still
    hold, and every per-char DB tuple is unchanged.  A failure is a runloom UCD
    read-path desync.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a C
    normalize/name call that never returned (deadlocked on a corrupted internal
    pointer) is caught by the watchdog + require_no_lost.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0), and
    the pool is non-empty and did include composable/decomposable characters (so
    normalize() had real work to do, not an all-ASCII no-op).

FAIL ON: a normal form that changes across a yield on a single-owner string, a
normalization law that breaks, a lookup(name(ch)) round-trip that fails, or a
per-char DB attribute that changes between two calls with no mutation in between.
Every one of these is a pure-function violation over read-only data -- a genuine
C-reader race, never documented Python semantics.

Stresses: unicodedata.normalize NFC/NFD/NFKC/NFKD quick-check + canonical/
compatibility decomposition + recomposition over the shared UCD tables, name()/
lookup() name-database round trip, category/combining/bidirectional/numeric/
decomposition scalar lookups, all across hub migration + cooperative yield under
tens of thousands of goroutines with the GIL off.

Good TSan / controlled-M:N-replay target: any per-call mutable scratch in the C
UCD reader is a shared-read data race under this pattern; a TSan report on a
quick-check/decomposition buffer, or a deterministic-replay that reads a codepoint
attribute mid-update by a sibling's call, localizes the corruption before the
byte-compare oracle fires.
"""
import unicodedata

import harness
import runloom


# ---- Immutable codepoint pool -------------------------------------------------
# A fixed, recognizable set of codepoints spanning the interesting corners of the
# UCD: decomposable accented Latin, Greek, Cyrillic, precomposed Latin-Extended-
# Additional (rich canonical decompositions), CJK ideographs (numeric + name),
# Hiragana, and Number Forms (roman numerals / vulgar fractions -> numeric()).
# Every member is pre-filtered at import time to (a) HAVE a name and (b) satisfy
# lookup(name(ch)) == ch, so the round-trip law is always-true and the per-char
# snapshot never raises.  The pool is READ-ONLY and shared by all fibers -- that
# is fine (it is immutable); the single-owner objects are the per-fiber STRINGS
# built from it, never the pool itself.
CANDIDATE_RANGES = (
    (0x00C0, 0x0180),   # Latin-1 Supplement + Latin Extended-A (decomposable)
    (0x0391, 0x03D0),   # Greek and Coptic
    (0x0400, 0x0460),   # Cyrillic
    (0x1E00, 0x1F00),   # Latin Extended Additional (many canonical decompositions)
    (0x2150, 0x2190),   # Number Forms (roman numerals, vulgar fractions)
    (0x3041, 0x30A0),   # Hiragana / start of Katakana
    (0x4E00, 0x4F00),   # CJK Unified Ideographs (subset)
)


def build_pool():
    pool = []
    has_decomp = False
    has_numeric = False
    for lo, hi in CANDIDATE_RANGES:
        for cp in range(lo, hi):
            ch = chr(cp)
            try:
                nm = unicodedata.name(ch)
            except ValueError:
                continue
            try:
                if unicodedata.lookup(nm) != ch:
                    continue
            except KeyError:
                continue
            pool.append(ch)
            if unicodedata.decomposition(ch):
                has_decomp = True
            if unicodedata.numeric(ch, None) is not None:
                has_numeric = True
    return pool, has_decomp, has_numeric


POOL, POOL_HAS_DECOMP, POOL_HAS_NUMERIC = build_pool()
POOL_LEN = len(POOL)

# String length band per fiber-local input.  Long enough that normalize() has real
# multi-codepoint composition/decomposition work (reordering combining marks,
# recomposing), short enough that many iterations complete under the timeout.
STR_MIN = 6
STR_MAX = 28

# Sustained checks per round: the read-cache hazard only manifests under sustained
# churn -- many fibers simultaneously driving normalize/name/lookup while parked
# across their yield, so a sibling reliably interleaves before this fiber resumes.
INNER_CAP = 100000


def char_snapshot(ch):
    """A tuple of every scalar UCD attribute of ch.  Constants of the read-only
    DB -- identical on every call.  Uses default-arg forms so no ValueError."""
    return (
        unicodedata.name(ch),
        unicodedata.category(ch),
        unicodedata.combining(ch),
        unicodedata.bidirectional(ch),
        unicodedata.east_asian_width(ch),
        unicodedata.mirrored(ch),
        unicodedata.decimal(ch, None),
        unicodedata.digit(ch, None),
        unicodedata.numeric(ch, None),
        unicodedata.decomposition(ch),
    )


def norm_forms(s):
    """The four normal forms of s (each a fresh str; pure function of s)."""
    return (
        unicodedata.normalize("NFC", s),
        unicodedata.normalize("NFD", s),
        unicodedata.normalize("NFKC", s),
        unicodedata.normalize("NFKD", s),
    )


def check_laws(H, wid, s, forms):
    """Assert the concurrency-independent normalization laws on (s, forms).  These
    hold for EVERY string on a correct implementation; a break means the C reader
    returned corrupted data.  Returns False (and has called H.fail) on violation."""
    nfc, nfd, nfkc, nfkd = forms

    # Idempotence.
    if unicodedata.normalize("NFC", nfc) != nfc:
        H.fail("NFC idempotence broken (wid {0}): NFC(NFC(x)) != NFC(x) -- the "
               "UCD recomposition path returned data inconsistent with a prior "
               "call on the same single-owner string".format(wid))
        return False
    if unicodedata.normalize("NFD", nfd) != nfd:
        H.fail("NFD idempotence broken (wid {0}): NFD(NFD(x)) != NFD(x)".format(wid))
        return False
    if unicodedata.normalize("NFKC", nfkc) != nfkc:
        H.fail("NFKC idempotence broken (wid {0}): NFKC(NFKC(x)) != NFKC(x)".format(wid))
        return False
    if unicodedata.normalize("NFKD", nfkd) != nfkd:
        H.fail("NFKD idempotence broken (wid {0}): NFKD(NFKD(x)) != NFKD(x)".format(wid))
        return False

    # Composition consistency: recomposing the decomposition == direct NFC, and
    # decomposing the composition == direct NFD.
    if unicodedata.normalize("NFC", nfd) != nfc:
        H.fail("composition law broken (wid {0}): NFC(NFD(x)) != NFC(x) -- the "
               "decomposition->recomposition round trip diverged from the direct "
               "NFC over the shared UCD tables".format(wid))
        return False
    if unicodedata.normalize("NFD", nfc) != nfd:
        H.fail("composition law broken (wid {0}): NFD(NFC(x)) != NFD(x)".format(wid))
        return False
    if unicodedata.normalize("NFKC", nfkd) != nfkc:
        H.fail("composition law broken (wid {0}): NFKC(NFKD(x)) != NFKC(x)".format(wid))
        return False

    # is_normalized must agree with the corresponding normal form.
    if not unicodedata.is_normalized("NFC", nfc):
        H.fail("is_normalized disagrees (wid {0}): is_normalized('NFC', NFC(x)) "
               "is False -- quick-check state inconsistent with the recomposed "
               "form".format(wid))
        return False
    if not unicodedata.is_normalized("NFD", nfd):
        H.fail("is_normalized disagrees (wid {0}): is_normalized('NFD', NFD(x)) "
               "is False".format(wid))
        return False
    if not unicodedata.is_normalized("NFKC", nfkc):
        H.fail("is_normalized disagrees (wid {0}): is_normalized('NFKC', NFKC(x)) "
               "is False".format(wid))
        return False
    if not unicodedata.is_normalized("NFKD", nfkd):
        H.fail("is_normalized disagrees (wid {0}): is_normalized('NFKD', NFKD(x)) "
               "is False".format(wid))
        return False
    return True


def do_check(H, wid, rng, state):
    """One load-bearing UCD read-isolation check on a fresh single-owner string.

    Snapshot everything BEFORE a yield, yield (sibling parks/migrates over the same
    C tables), recompute AFTER, and assert byte-identical.  Because the input is
    single-owner and the DB is read-only, any difference is a C-reader race."""
    n = rng.randint(STR_MIN, STR_MAX)
    # Fresh, fiber-local string built from the immutable pool -- never shared.
    chars = [POOL[rng.randrange(POOL_LEN)] for _ in range(n)]
    s = "".join(chars)

    # --- BEFORE the yield: full snapshot ---
    forms0 = norm_forms(s)
    if not check_laws(H, wid, s, forms0):
        return
    # Per-char DB snapshot + name-round-trip for the distinct chars in this string.
    distinct = list(dict.fromkeys(chars))       # order-preserving unique
    snaps0 = {}
    for ch in distinct:
        snap = char_snapshot(ch)
        snaps0[ch] = snap
        # Round-trip law: lookup(name(ch)) == ch (pool guarantees name exists).
        if unicodedata.lookup(snap[0]) != ch:
            H.fail("name/lookup round-trip broken (wid {0}): lookup(name(U+{1:04X}"
                   ")) != U+{1:04X} -- the shared name database returned a name "
                   "resolving to a different codepoint".format(wid, ord(ch)))
            return

    # --- YIELD: allow a sibling to park/migrate and drive the same C tables ---
    runloom.yield_now()
    if n & 1:
        runloom.sleep(0.0003)

    # --- AFTER the yield: recompute and demand byte-identical results ---
    forms1 = norm_forms(s)
    labels = ("NFC", "NFD", "NFKC", "NFKD")
    for i in range(4):
        if forms1[i] != forms0[i]:
            H.fail("normal form {0} CHANGED across a yield (wid {1}) on a single-"
                   "owner string of {2} codepoints -- the UCD reader returned a "
                   "different result for the SAME input, i.e. a sibling's "
                   "codepoint data leaked through a shared read-path cache".format(
                       labels[i], wid, n))
            return
    # Laws must still hold on the recomputed forms.
    if not check_laws(H, wid, s, forms1):
        return
    # Per-char DB tuples must be unchanged (constants of the DB).
    for ch in distinct:
        snap1 = char_snapshot(ch)
        if snap1 != snaps0[ch]:
            H.fail("UCD attribute of U+{0:04X} CHANGED across a yield (wid {1}): "
                   "{2!r} -> {3!r} -- a per-character DB read returned different "
                   "data for the same codepoint, a shared-read-path race".format(
                       ord(ch), wid, snaps0[ch], snap1))
            return

    state["checks"][wid] += 1                    # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            do_check(H, wid, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One race-free slot per worker (single writer per slot; wid < H.funcs).
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("unicodedata UCD read-isolation: {0} single-owner byte-identical "
          "snapshot checks passed fail-fast (pool={1} codepoints, decomposable={2}"
          ", numeric={3}); ops={4}".format(
              checks, POOL_LEN, POOL_HAS_DECOMP, POOL_HAS_NUMERIC, H.total_ops()))

    # NON-VACUITY: the pool must be real work, and the load-bearing arm must have run.
    H.check(POOL_LEN > 0,
            "codepoint pool is empty -- the UCD read-isolation hazard has no "
            "input to exercise (oracle would be vacuous)")
    H.check(POOL_HAS_DECOMP,
            "codepoint pool has no decomposable characters -- normalize() would "
            "be an all-no-op, missing the composition/decomposition read path")
    H.check(checks > 0,
            "no single-owner UCD read-isolation checks ran -- the load-bearing "
            "normalize/name/lookup hazard was never exercised (oracle vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a C normalize/name call.
    H.require_no_lost("unicodedata read isolation")


if __name__ == "__main__":
    harness.main(
        "p510_unicodedata_normalize_idempotent", body, setup=setup, post=post,
        default_funcs=8000,
        describe="unicodedata answers every query out of a single process-wide "
                 "read-only UCD compiled into the C extension.  Under M:N a "
                 "normalize/name/lookup call may park mid-computation and resume "
                 "on another hub while a sibling drives the same shared tables; "
                 "if any per-call scratch/quick-check state is not fiber-private "
                 "the result could be corrupted with a sibling's codepoint data. "
                 "LOAD-BEARING: each fiber builds a fresh single-owner string, "
                 "snapshots the four normal forms + normalization laws (NFC/NFD "
                 "idempotence, NFC(NFD(x))==NFC(x), is_normalized) + per-char DB "
                 "attributes + lookup(name(ch))==ch, YIELDS, then recomputes and "
                 "demands byte-identical results.  Input is single-owner and the "
                 "DB read-only, so any cross-yield mismatch is a UCD read-cache "
                 "race, never documented Python semantics")
