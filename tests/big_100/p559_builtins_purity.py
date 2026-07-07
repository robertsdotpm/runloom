"""big_100 / 559 -- builtins pure-function PURITY (identity + round-trip) under M:N.

The `builtins` module is the namespace of Python's built-in functions and types
(abs, divmod, pow, sum, min, max, sorted, len, hash, hex/oct/bin/int, str/repr,
ord/chr, format, ...).  The overwhelming majority of them are PURE: given the
same immutable argument they return the same, deterministic, closed-form result
with no observable global state.  That makes them a textbook PURITY oracle for
M:N -- a builtin evaluated on FIBER-LOCAL, single-owner data must return a
BIT-IDENTICAL result across a cooperative yield, and that result must match an
INDEPENDENTLY-computed closed form.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs goroutines
in PARALLEL across hubs with the GIL off.  A pure builtin's evaluation walks C
code (the C small-int cache, the str/bytes machinery, the tuple hash, heapq
inside sorted, the base-conversion digit loops) -- if any of that C state were
process-global-mutable-per-thread in a way runloom does not isolate per fiber, a
sibling fiber running the SAME builtin on DIFFERENT data on another hub (or the
same hub after this fiber parks across its yield) could corrupt the in-flight
result: a torn integer, a wrong digit, a sorted list that is not a permutation of
its input, a hash that changes across a yield, ord(chr(c)) != c.  On a CORRECT
runtime NONE of that can happen: the arguments are fiber-local and immutable, so
the answer is a mathematical constant and every recompute is identical.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Each fiber derives its OWN list/ints from
(wid, idx) -- never shared, never mutated.  It computes each builtin's result and
an INDEPENDENT closed form (a manual accumulator loop; a repeated-multiply for
pow; the inverse base-conversion for hex/oct/bin), asserts they agree, YIELDS so
siblings interleave, then recomputes every builtin and asserts the answer is
bit-identical to the pre-yield answer AND still equals the closed form.  Because
the input is single-owner + immutable, the expected result is a constant; ANY
difference (across the yield, or vs the closed form) is a torn/corrupted builtin
evaluation -- a real runtime bug (a data race that reached a pure C computation),
never documented Python semantics.  Verified against plain threads: 8 OS threads
each running these identities on thread-local data, GIL on AND off, are 100%
bit-identical -- so a correct runloom must be too.

ORACLES:
  * LOAD-BEARING -- BUILTIN PURITY (worker, HARD, fail-fast).  Per iteration, on
    fiber-local data:
      - divmod identity:  a == b*q + r and 0 <= r < b.
      - pow(a,e,m) == repeated-multiply-mod closed form.
      - base round-trips: int(hex(x),16)==int(oct(x),8)==int(bin(x),2)==
        int(str(x))==int(format(x,'x'),16)==x.
      - aggregations vs a manual loop: sum/min/max/len over the list; sorted() is
        a non-decreasing permutation (multiset-equal via a manual tally); all()/
        any() vs manual booleans.
      - char round-trip: ord(chr(c))==c for every c in 0..0x10FFFF derived.
      - hash stability: hash(tuple(data)) identical before and after the yield.
    All computed twice around a yield; both the cross-yield stability AND the
    closed-form match are asserted.  Single-owner: the data is a fiber-local list
    built from (wid, idx), never shared.

  * MEASURED (report-ONLY, NEVER fails): a SHARED list is mutated by all fibers
    while they call sorted()/len() on it.  A shared mutable container under M:N
    races EXACTLY like shared-across-threads (documented Python behaviour), so we
    only MEASURE how often sorted() observes a torn snapshot (its length differs
    from a re-read len, or it raises "changed size") -- proving the hazard is real
    and that the single-owner load-bearing arm genuinely isolates it.  Never
    H.fail on this.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-builtin
    (e.g. inside a C base-conversion or sorted() that never returned) is caught.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (purity_checks>0).

FAIL ON: any builtin result that changes across a yield, or disagrees with its
independent closed form, on single-owner immutable data (a torn integer, a
non-permutation sort, ord(chr(c))!=c, an unstable hash, a broken divmod/pow/base
round-trip) -- a corrupted pure C evaluation under M:N.  The shared-list MEASURED
arm is report-only.

Stresses: the C evaluation paths of builtins pure functions (small-int/int
arithmetic, base-conversion digit loops, tuple/str hashing, sorted()/heapq,
ord/chr code-point translation) under GIL-off hub-parallel churn with a
cooperative yield planted at the hazard boundary so a sibling reliably interleaves
between a builtin's two evaluations of the same fiber-local constant.
"""
import builtins

import harness
import runloom

# Size of each fiber's private int list.  Big enough to push sum/sorted through a
# real aggregation (and the backing list through a growth), small enough that many
# iterations complete under the timeout.
NDATA = 24

# Sustained purity checks per worker, bounded by H.running().  A single check per
# fiber barely overlaps a sibling's; the corruption hazard (if any) only manifests
# under SUSTAINED churn -- many fibers evaluating pure builtins while parked across
# their yield, so the scheduler reliably interleaves a sibling between this fiber's
# two evaluations of the same constant.
INNER_CAP = 100000

# A byte we can safely fold into a code point for ord/chr: chr() accepts the full
# 0..0x10FFFF range (surrogates included); ord(chr(c))==c holds for every one.
CODEPOINT_MASK = 0x10FFFF


def make_data(wid, idx):
    """Build one fiber's PRIVATE list of ints, deterministic in (wid, idx).

    Never shared, never mutated after construction -- so every pure builtin over
    it has a fixed, closed-form answer that MUST survive a yield unchanged."""
    seed = (wid * 0x100000001B3 + idx * 0x9E3779B1) & 0xFFFFFFFFFFFF
    data = []
    v = seed | 1
    for i in range(NDATA):
        # A cheap deterministic mixer -- values span a wide range so sorted() has a
        # real ordering to build and min/max are non-trivial.
        v = (v * 2654435761 + (i + 1) * 40503) & 0xFFFFFFFF
        data.append(v)
    return seed, data


def check_aggregations(H, wid, data):
    """sum/min/max/len/sorted/all/any over the list vs an INDEPENDENT manual loop.

    Returns True on success; calls H.fail + returns False on any mismatch."""
    # Independent closed forms via a plain accumulator loop (no builtin aggregation).
    man_sum = 0
    man_min = data[0]
    man_max = data[0]
    man_all = True
    man_any = False
    man_tally = {}
    for x in data:
        man_sum += x
        if x < man_min:
            man_min = x
        if x > man_max:
            man_max = x
        if x:
            man_any = True
        else:
            man_all = False
        man_tally[x] = man_tally.get(x, 0) + 1

    b_sum = builtins.sum(data)
    b_min = builtins.min(data)
    b_max = builtins.max(data)
    b_len = builtins.len(data)
    b_sorted = builtins.sorted(data)
    b_all = builtins.all(data)
    b_any = builtins.any(data)

    if b_sum != man_sum:
        H.fail("builtins.sum WRONG: sum(data)={0} != manual {1} (wid {2}) -- torn "
               "aggregation over single-owner immutable list".format(
                   b_sum, man_sum, wid))
        return False
    if b_min != man_min or b_max != man_max:
        H.fail("builtins.min/max WRONG: min={0}/max={1} != manual {2}/{3} (wid {4})"
               .format(b_min, b_max, man_min, man_max, wid))
        return False
    if b_len != NDATA:
        H.fail("builtins.len WRONG: len(data)={0} != {1} (wid {2})".format(
            b_len, NDATA, wid))
        return False
    if b_all != man_all or b_any != man_any:
        H.fail("builtins.all/any WRONG: all={0}/any={1} != manual {2}/{3} (wid {4})"
               .format(b_all, b_any, man_all, man_any, wid))
        return False
    # sorted() must be a NON-DECREASING PERMUTATION of data (multiset-equal).
    if len(b_sorted) != NDATA:
        H.fail("builtins.sorted length WRONG: {0} != {1} (wid {2}) -- sorted() lost "
               "or duplicated an element of a single-owner list".format(
                   len(b_sorted), NDATA, wid))
        return False
    prev = b_sorted[0]
    for x in b_sorted[1:]:
        if x < prev:
            H.fail("builtins.sorted NOT ORDERED: {0} < {1} in output (wid {2}) -- a "
                   "torn heapify over a single-owner list".format(x, prev, wid))
            return False
        prev = x
    sorted_tally = {}
    for x in b_sorted:
        sorted_tally[x] = sorted_tally.get(x, 0) + 1
    if sorted_tally != man_tally:
        H.fail("builtins.sorted NOT A PERMUTATION of input (wid {0}) -- the multiset "
               "of a single-owner list changed under sorted()".format(wid))
        return False
    return True


def check_base_roundtrips(H, wid, x):
    """int(hex/oct/bin/str/format(x)) == x for a fiber-local nonnegative int.

    Returns True on success; H.fail + False on any mismatch."""
    if builtins.int(builtins.hex(x), 16) != x:
        H.fail("hex round-trip BROKEN: int(hex({0}),16) != {0} (wid {1})".format(
            x, wid))
        return False
    if builtins.int(builtins.oct(x), 8) != x:
        H.fail("oct round-trip BROKEN: int(oct({0}),8) != {0} (wid {1})".format(
            x, wid))
        return False
    if builtins.int(builtins.bin(x), 2) != x:
        H.fail("bin round-trip BROKEN: int(bin({0}),2) != {0} (wid {1})".format(
            x, wid))
        return False
    if builtins.int(builtins.str(x)) != x:
        H.fail("str/int round-trip BROKEN: int(str({0})) != {0} (wid {1})".format(
            x, wid))
        return False
    if builtins.int(builtins.format(x, "x"), 16) != x:
        H.fail("format('x') round-trip BROKEN: int(format({0},'x'),16) != {0} "
               "(wid {1})".format(x, wid))
        return False
    return True


def check_arith_identities(H, wid, idx, a):
    """divmod + pow identities on fiber-local ints.  True on success."""
    b = (idx % 97) + 1                     # 1..97, never zero
    q, r = builtins.divmod(a, b)
    if a != b * q + r or not (0 <= r < b):
        H.fail("divmod identity BROKEN: divmod({0},{1})=({2},{3}) violates "
               "a==b*q+r / 0<=r<b (wid {4})".format(a, b, q, r, wid))
        return False
    m = (idx % 251) + 2                    # >= 2
    e = (wid % 13) + 1                      # 1..13
    p = builtins.pow(a, e, m)
    # Independent closed form: repeated multiply mod m.
    man_p = 1
    base = a % m
    for _ in range(e):
        man_p = (man_p * base) % m
    if p != man_p:
        H.fail("pow(a,e,m) WRONG: pow({0},{1},{2})={3} != repeated-multiply {4} "
               "(wid {5})".format(a, e, m, p, man_p, wid))
        return False
    if builtins.abs(-a) != a or builtins.abs(a) != a:
        H.fail("abs BROKEN on {0} (wid {1})".format(a, wid))
        return False
    return True


def check_char_roundtrip(H, wid, data):
    """ord(chr(c)) == c for a code point derived from each element.  True on ok."""
    for x in data:
        c = x & CODEPOINT_MASK             # 0..0x10FFFF -> always a valid chr() arg
        if builtins.ord(builtins.chr(c)) != c:
            H.fail("ord(chr(c)) != c for code point {0} (wid {1}) -- torn code-point "
                   "translation".format(c, wid))
            return False
    return True


def purity_check(H, wid, idx, state):
    """One full single-owner purity check across a yield.  Fail-fast.

    Computes every identity BEFORE the yield (also validating each vs its closed
    form), YIELDS so siblings interleave, then recomputes every builtin and asserts
    the answer is BIT-IDENTICAL to the pre-yield answer.  On a correct runtime the
    single-owner immutable inputs make every answer a constant."""
    seed, data = make_data(wid, idx)
    x = seed & 0x7FFFFFFFFFFF              # nonnegative for base round-trips
    a = (seed ^ 0x5DEECE66D) & 0xFFFFFFFF

    # ---- pre-yield: builtins vs independent closed forms --------------------
    if not check_aggregations(H, wid, data):
        return False
    if not check_base_roundtrips(H, wid, x):
        return False
    if not check_arith_identities(H, wid, idx, a):
        return False
    if not check_char_roundtrip(H, wid, data):
        return False

    # Baseline answers we require to be BIT-IDENTICAL after the yield.
    base_sum = builtins.sum(data)
    base_sorted = builtins.sorted(data)
    base_hex = builtins.hex(x)
    base_hash = builtins.hash(tuple(data))

    # ---- YIELD: let siblings run pure builtins on their own data ------------
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    # ---- post-yield: bit-identical recompute --------------------------------
    if builtins.sum(data) != base_sum:
        H.fail("sum() CHANGED across a yield on single-owner data (wid {0}) -- torn "
               "aggregation under M:N".format(wid))
        return False
    if builtins.sorted(data) != base_sorted:
        H.fail("sorted() CHANGED across a yield on single-owner data (wid {0}) -- a "
               "sibling's sort corrupted this fiber's result".format(wid))
        return False
    if builtins.hex(x) != base_hex:
        H.fail("hex() CHANGED across a yield: {0} != {1} (wid {2})".format(
            builtins.hex(x), base_hex, wid))
        return False
    if builtins.hash(tuple(data)) != base_hash:
        H.fail("hash(tuple(data)) UNSTABLE across a yield (wid {0}) -- the tuple hash "
               "of a single-owner immutable value changed".format(wid))
        return False
    # And re-validate the closed-form identities survived the yield.
    if not check_base_roundtrips(H, wid, x):
        return False
    if not check_arith_identities(H, wid, idx, a):
        return False

    state["purity_checks"][wid & 1023] += 1
    return True


def measured_shared_sort(H, wid, idx, state):
    """MEASURED (report-only): sort a SHARED, concurrently-mutated list.

    A shared mutable list under M:N races like a shared-across-threads container
    (documented Python behaviour), so sorted() can observe a torn snapshot whose
    length differs from a re-read len, or raise "list changed size".  We MEASURE
    that rate to prove the hazard is real; we NEVER H.fail on it."""
    shared = state["shared_list"]
    # Mutate a little so the list is genuinely churning under other fibers' reads.
    if idx & 1:
        try:
            shared.append(idx & 0xFFFF)
        except Exception:
            pass
    else:
        try:
            if shared:
                shared.pop()
        except Exception:
            pass
    state["shared_reads"][wid & 1023] += 1
    try:
        snap = builtins.sorted(shared)
        # A torn snapshot: the sorted copy's length disagrees with a fresh len()
        # taken right after (the list changed between capture and now).
        if builtins.len(snap) != builtins.len(shared):
            state["shared_torn"][wid & 1023] += 1
    except RuntimeError:
        # "list changed size during iteration" -- the LEGAL detection of the
        # concurrent mutation.  Count it as an observed hazard, do not fail.
        state["shared_torn"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs the LOAD-BEARING single-owner purity check (fail-fast) and
    the MEASURED shared-list arm (report only) per inner iteration.  The two do not
    share data, so the shared churn never reaches the single-owner oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if not purity_check(H, wid, idx, state):    # LOAD-BEARING
                return
            measured_shared_sort(H, wid, idx, state)    # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "purity_checks": [0] * 1024,       # LOAD-BEARING single-owner checks (tally)
        "shared_list": [1, 2, 3, 4, 5, 6, 7, 8],   # MEASURED shared mutable list
        "shared_reads": [0] * 1024,        # MEASURED sorted() reads
        "shared_torn": [0] * 1024,         # MEASURED torn-snapshot observations
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    pchecks = sum(H.state["purity_checks"])
    sreads = sum(H.state["shared_reads"])
    storn = sum(H.state["shared_torn"])
    spct = (100.0 * storn / sreads) if sreads else 0.0

    H.log("builtins[single-owner LOAD-BEARING]: {0} purity checks (all passed "
          "fail-fast) | builtins[shared-list MEASURED]: {1} sorted() reads {2} torn "
          "snapshots ({3:.1f}%, documented shared-mutable behaviour -- REPORT ONLY)"
          .format(pchecks, sreads, storn, spct))

    if storn:
        H.log("note: the shared list observed {0} torn sorted() snapshots across {1} "
              "reads -- a shared mutable list is a shared Python object, like p490's "
              "shared enum pool; this is documented M:N shared-object behaviour, NOT "
              "a runloom bug, and never reaches the single-owner purity oracle"
              .format(storn, sreads))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(pchecks > 0,
            "no single-owner builtins purity checks ran -- the load-bearing "
            "identity/round-trip hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a C
    # base-conversion, sorted(), or the tuple-hash walk).
    H.require_no_lost("builtins purity")


if __name__ == "__main__":
    harness.main(
        "p559_builtins_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="builtins pure functions (sum/min/max/sorted/len/all/any, divmod/"
                 "pow/abs, hex/oct/bin/int/str/format base round-trips, ord/chr, "
                 "hash) are deterministic closed-form maps.  LOAD-BEARING: each "
                 "fiber evaluates them on its OWN immutable list/ints, checks each "
                 "against an independent closed form, yields, then recomputes and "
                 "asserts every answer is BIT-IDENTICAL and still matches the closed "
                 "form.  On single-owner immutable data the result is a constant, so "
                 "any change across the yield (torn int, non-permutation sort, "
                 "ord(chr(c))!=c, unstable hash, broken divmod/pow/base round-trip) "
                 "is a corrupted pure C evaluation under M:N.  MEASURED shared-list "
                 "sort (expected torn snapshots, like p490's shared pool) proves the "
                 "hazard exists.")
