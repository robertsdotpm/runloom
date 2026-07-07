"""big_100 / 547 -- json DECODER / C scanner round-trip identity across a preempt
MID-PARSE.

json.loads() / JSONDecoder.decode() drive the C scanner built by
_json.make_scanner (json.scanner.c_make_scanner).  That C scanner object carries
mutable per-decode state: the current parse INDEX into the input string, the
recursion of nested container parses, AND a `memo` dict that INTERNS object keys
so repeated keys across a document share one str object.  On top of that, when a
JSONDecoder is constructed with an `object_hook=`, that hook runs ARBITRARY
PYTHON for EVERY JSON object the scanner closes -- and it runs WHILE the C scanner
is partway through the enclosing document, its parse index and key-memo live on
the scanner struct.

In a runloom M:N runtime an object_hook that yields (runloom.yield_now /
runloom.sleep) is a SCHEDULING POINT: the fiber can be preempted MID-PARSE and the
hub can switch to a SIBLING fiber that is ALSO inside its own JSONDecoder.decode()
on the same hub / same OS thread.  json decoding is almost universally ASSUMED
reentrant-safe, so this is a LOAD-BEARING probe of the DECODER side (p459 covers
the C ENCODER; the hazard is disjoint -- encoder buffer/accumulator there, scanner
parse-index + key-memo here).  If runloom's preempt-mid-C-scan bleeds one fiber's
scanner index or interned-key memo into a sibling's decode, the recovered object
will differ from what this fiber serialized -- a real runtime bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (single-owner round-trip identity):

  * LOAD-BEARING -- SINGLE-OWNER ROUND-TRIP IDENTITY (per-op, HARD, fail-fast).
    Each fiber:
      - builds its OWN distinct nested structure, tagged with its wid in BOTH the
        keys (a "k<wid>" key) AND the values (a "w": wid marker in EVERY object,
        arrays holding wid, ints derived from wid, non-ASCII strings that force
        \\uXXXX escapes through the scanner's unescape path);
      - json.dumps()es it to text (its OWN local text);
      - decodes that text with its OWN JSONDecoder whose object_hook calls
        runloom.yield_now() on EVERY object -- forcing a sibling scanner onto the
        hub mid-parse -- and, from inside the hook, asserts the object it just
        closed carries THIS fiber's wid ("w" == wid), never a sibling's;
      - asserts loads(dumps(x)) == x EXACTLY (no lost/spliced bytes, no torn
        number, no dropped key from a bled memo).
    Single-owner: the structure, the dumped text, and the JSONDecoder (hence its C
    scanner + memo) are all owned by exactly THIS fiber; nothing is shared between
    fibers except the interpreter's json machinery itself.  Under plain OS threads
    with the GIL on this ALWAYS holds (each decode uses its own scanner + fresh
    memo, and even though object_hook can run other Python, the scanner state lives
    on the per-fiber decoder), so a correct runloom MUST match it.  A cross-fiber
    wid in a recovered object, or a round-trip inequality, is the real runloom
    decoder-scanner isolation bug this program uniquely catches.  Exits 0 when
    there is no bug.

  * NON-VACUITY (post, HARD): decode_checks > 0 (round-trips actually ran) AND
    hook_fires > 0 (the object_hook -- the mid-parse yield -- actually executed;
    else the hazard was never armed).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber preempted inside the C
    scanner (parked in its object_hook mid-parse) that is never re-woken never
    returns; the watchdog + require_no_lost catch a parked-then-vanished worker.

FAIL ON: a recovered object carrying a wid that is not this fiber's (a sibling's
interned key or spliced value bled across the mid-parse preempt), a round-trip
inequality (loads(dumps(x)) != x), a torn number / dropped key, or a SIGSEGV
mid-scan.  There is NO shared-mutable arm here -- the whole oracle is single-owner,
so any FAIL is a genuine cross-fiber scanner-state leak, not documented shared-
object Python semantics.

Stresses: _json C scanner parse-index + key-memo across hub migration, object_hook
Python callback firing mid-C-scan, \\uXXXX unescape path, nested container recursion
in the scanner, JSONDecoder.decode() reentrancy under M:N with the GIL off.

Good TSan / controlled-M:N-replay target: the scanner's `memo` dict and its parse
index are mutated in C while the object_hook yields the hub to a sibling scanner;
a data-race report on the scanner struct, or a deterministic-replay that reads a
sibling's interned key mid-scan, localizes the leak before the round-trip equality
even closes.
"""
import json

import harness
import runloom

# Non-ASCII payload embedded in string values.  With json.dumps default
# ensure_ascii=True these become \\uXXXX (and surrogate-pair) escapes in the text,
# so the round-trip drives the scanner's unescape path; loads() must reconstruct
# the identical str.  A byte spliced from a sibling's escape run would break the
# round-trip equality.
UNI = "café-中文-\U0001f600-ÿ"

# The object_hook yields on EVERY object, so a document with several nested
# objects hands the hub to a sibling many times per decode.  Keep the structure
# small enough that many round-trips complete under the timeout but deep/wide
# enough that the scanner recurses and its memo interns repeated keys.
MIN_DEPTH = 2
MAX_DEPTH = 3

# Sustained round-trips per worker, bounded by H.running().  The mid-parse preempt
# hazard only manifests under SUSTAINED churn: many fibers simultaneously decoding
# while PARKED (yield) inside their object_hook, so the scheduler reliably lands a
# sibling scanner on the hub before this fiber resumes.  A single decode per fiber
# barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def build_struct(wid, idx, rng):
    """Build ONE distinct nested structure, tagged with `wid` throughout.

    Every object dict carries a "w": wid marker (the load-bearing tag the
    object_hook checks) plus a wid-specific "k<wid>" key (so the scanner's key-memo
    interns strings that are UNIQUE to this fiber -- a sibling's memo entry showing
    up would be a distinct key).  Values embed wid in ints, arrays, and non-ASCII
    strings.  Uses ints only (no floats) so the round-trip is bit-exact.  Single-
    owner: the returned object is never shared with another fiber."""
    def node(depth):
        if depth <= 0:
            return {
                "w": wid,
                "leaf": wid * 7 + idx,
                "s": "leaf-{0}-{1}-{2}".format(wid, idx, UNI),
            }
        children = [node(depth - 1) for _ in range(rng.randint(2, 4))]
        return {
            "w": wid,
            # wid-specific key -> unique memo intern per fiber.
            "k{0}".format(wid): wid * 1000 + idx,
            "arr": [wid, idx, wid + idx, "u{0}-{1}".format(wid, UNI)],
            "nested": children,
            "tag": "wid={0};idx={1};{2}".format(wid, idx, UNI),
            "n": -(wid * 13 + idx),
        }
    return node(rng.randint(MIN_DEPTH, MAX_DEPTH))


def make_hook(H, wid, state):
    """Build this fiber's object_hook.  It fires for EVERY JSON object the C
    scanner closes.  It (a) YIELDS the hub mid-parse so a sibling scanner runs
    before this decode finishes, (b) records that it fired (non-vacuity), and
    (c) asserts the just-closed object carries THIS fiber's wid -- a different wid
    means a sibling's interned key/value bled across the preempt.  Single writer
    per slot (wid), so the counters are race-free."""
    fires = state["hook_fires"]
    def object_hook(obj):
        # Mid-parse scheduling point: hand the hub to a sibling scanner.
        runloom.yield_now()
        fires[wid] += 1                    # single-writer-per-slot (wid), race-free
        w = obj.get("w")
        if w != wid:
            H.fail("json decoder scanner BLEED: object_hook closed an object with "
                   "w={0!r}, expected THIS fiber's wid {1} (obj keys={2!r}) -- a "
                   "sibling fiber's interned key or spliced value bled into this "
                   "decode across the mid-parse preempt".format(
                       w, wid, sorted(obj.keys())))
        return obj
    return object_hook


def worker(H, wid, rng, state):
    """LOAD-BEARING single-owner round-trip: build a wid-tagged structure, dumps
    it, decode it with this fiber's OWN JSONDecoder whose object_hook yields mid-
    parse, and assert the round-trip recovers THIS fiber's object exactly.  The
    decoder (and its C scanner + memo) is created once per fiber and reused across
    inner iterations so the scanner's memo accumulates -- still single-owner."""
    decoder = json.JSONDecoder(object_hook=make_hook(H, wid, state))
    checks = state["decode_checks"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            original = build_struct(wid, idx, rng)
            text = json.dumps(original)
            decoded = decoder.decode(text)
            if H.failed:                   # hook fired a cross-fiber wid bleed
                return
            if decoded != original:
                H.fail("json round-trip MISMATCH (wid {0}, idx {1}): "
                       "loads(dumps(x)) != x -- the decoder's C scanner lost, "
                       "spliced, or torn bytes across the mid-parse preempt "
                       "(sibling scanner state bled in).  decoded={2!r}".format(
                           wid, idx, decoded))
                return
            checks[wid] += 1               # single-writer-per-slot (wid), race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-slot (wid-indexed) counters: ONE writer per slot -> race-free even GIL-off.
    H.state = {
        "decode_checks": [0] * H.funcs,    # LOAD-BEARING round-trips that passed
        "hook_fires": [0] * H.funcs,       # object_hook invocations (non-vacuity)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["decode_checks"])
    fires = sum(H.state["hook_fires"])
    H.log("json decoder[single-owner LOAD-BEARING]: {0} round-trip identity checks "
          "(all passed fail-fast) | {1} object_hook fires (mid-parse yields); "
          "ops={2}".format(checks, fires, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip actually ran AND the object_hook
    # (the mid-parse yield that arms the hazard) actually fired.
    H.check(checks > 0,
            "no decoder round-trips ran -- the load-bearing scanner-reentrancy "
            "hazard was never exercised (oracle would be vacuous)")
    H.check(fires > 0,
            "object_hook never fired -- the mid-parse yield that forces a sibling "
            "scanner onto the hub never armed (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (stranded inside its object_hook
    # mid-C-scan and never re-woken).
    H.require_no_lost("json decoder scanner round-trip")


if __name__ == "__main__":
    harness.main(
        "p547_json_decoder_scanner_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="json.JSONDecoder drives the C scanner (make_scanner) holding a "
                 "parse index + a key-memo dict; with object_hook= the hook runs "
                 "Python for every object MID-PARSE.  Under M:N an object_hook that "
                 "yields is a scheduling point that lands a SIBLING scanner on the "
                 "hub mid-parse.  LOAD-BEARING: each fiber dumps its OWN wid-tagged "
                 "nested structure (wid in keys AND values, arrays, ints, \\uXXXX "
                 "escapes) and decodes it with its OWN JSONDecoder whose object_hook "
                 "yields on every object; loads(dumps(x))==x AND every recovered "
                 "object carries THIS fiber's wid.  A sibling's wid in a recovered "
                 "object, or a round-trip inequality, is the runloom decoder-"
                 "scanner isolation bug (disjoint from p459's C encoder)")
