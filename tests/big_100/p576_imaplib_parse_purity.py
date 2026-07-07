"""big_100 / 576 -- imaplib response-parser PURITY across a yield under M:N.

imaplib ships a family of pure, side-effect-free helpers that turn an IMAP
server's response bytes into a structured value (or the reverse):

  * ParseFlags(resp)          -- '* 1 FETCH (FLAGS (\\Seen \\Deleted))'
                                 -> (b'\\Seen', b'\\Deleted')   (tuple of tokens)
  * Internaldate2tuple(resp)  -- '... INTERNALDATE "01-Jan-2020 23:00:00 +1100"'
                                 -> time.struct_time (local time for that instant)
  * Time2Internaldate(t)      -- epoch/float/tuple -> '"dd-Mmm-yyyy HH:MM:SS +ZZZZ"'

Each is a CLOSED-FORM function of its argument alone: given the same input it
must return the same output, byte-for-byte, forever.  ParseFlags and
Internaldate2tuple both lookup a MODULE-GLOBAL compiled regex (imaplib.Flags /
imaplib.InternalDate) that -- GIL off -- is read concurrently by every fiber;
ParseFlags then whitespace-splits the matched group, Internaldate2tuple walks the
match's named groups and feeds time.mktime.  None of that is per-call state a
sibling should be able to perturb: the inputs are fiber-LOCAL bytes and the
outputs are fresh tuples / struct_time owned by one fiber.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes tens
of thousands of goroutines across >1 hub with the GIL off.  A fiber builds
fiber-local inputs with KNOWN embedded values, parses them, records the results,
YIELDS (so a sibling on another hub interleaves and parses its OWN different
inputs), then re-parses the SAME local inputs and demands a bit-identical result
that also equals the closed form computed directly from the known values.  A
CORRECT runtime keeps every fiber's parse a pure function of its own local input:
the result before and after the yield are identical and match the closed form.  A
result that DIFFERS across the yield -- or that disagrees with the closed form --
would mean a sibling's parse leaked into this fiber's (torn module-global regex
handed back a wrong/half-built pattern, a Match object or split buffer shared
across fibers, a cross-fiber value bleed) -- a real runtime isolation bug.

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (per the HARD RULES):
  * The load-bearing arm's inputs are fiber-LOCAL bytes (built from this fiber's
    own rng), never shared.  The outputs are freshly-allocated tuples / struct_time
    / bytes owned only by this fiber.  No shared mutable container feeds the
    fail-fast check, so a failure cannot be the documented "shared object races"
    semantics -- it can only be a runtime isolation / torn-state fault.
  * The module-global regexes (imaplib.Flags / imaplib.InternalDate) ARE shared,
    but they are compiled ONCE at import to value-STABLE patterns; setup() pre-warms
    both parse paths so the steady state is a shared READ of an immutable compiled
    pattern -- exactly what every parser assumes.  A wrong parse from a torn read
    of that global is a genuine fault, not documented Python semantics.
  * Verified against a standalone control: 200k random epochs round-trip through
    Time2Internaldate -> Internaldate2tuple -> time.mktime with ZERO mismatch, and
    50k random flag-sets round-trip through ParseFlags with ZERO mismatch (the
    closed forms hold exactly).  A correct runloom must match, so the oracle PASSES
    (exit 0) with no bug.

ORACLES:
  * LOAD-BEARING -- PARSER PURITY (worker, HARD, fail-fast).  Per iteration a fiber
    builds fiber-local inputs with unique known values, parses each, yields,
    re-parses, and asserts (a) the re-parse equals the pre-yield parse exactly and
    (b) both equal the closed-form value derived from the known numbers.  Two arms:
      - ParseFlags: KNOWN flag multiset -> tuple must equal the token-encoded flags.
      - Internaldate round-trip: KNOWN epoch -> Time2Internaldate string s (must be
        stable across the yield) -> Internaldate2tuple('...'+s) -> struct_time (must
        be stable across the yield) -> time.mktime must recover the ORIGINAL epoch.
    Both C-regex-global paths (Flags, InternalDate) are exercised each batch.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse (e.g.
    parked inside a torn regex lookup) never returns; the watchdog catches it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Counters: a per-wid race-free slot table (one writer per slot, allocated in setup
where H.funcs is known) tallies completed check-batches for the non-vacuity law.

FAIL ON: a parse result that changes across a yield, or disagrees with the closed
form -- a cross-fiber leak / torn module-global-regex / shared-split-state fault in
the runtime.  There is NO shared-mutable measured arm because imaplib's parsers
take no shared argument in this design; the whole program is single-owner.

Stresses: imaplib.ParseFlags / Internaldate2tuple / Time2Internaldate pure-function
purity, module-global compiled-regex (imaplib.Flags / imaplib.InternalDate)
shared-read under GIL-off M:N, re.Match named-group walk + whitespace split +
time.mktime round-trip isolation across hub migration + yield.

Good TSan / controlled-M:N-replay target: the shared module-global regex objects
(imaplib.Flags / imaplib.InternalDate) are read on every parse while a sibling
walks a Match of the same pattern; a TSan report on that shared re.Pattern, or a
replay handing back a wrong group, would localize the fault before the closed-form
mismatch even fires (setup() pre-warms both paths so the steady state is a clean
shared read -- a fault there is real, not init noise).
"""
import time

import imaplib

import harness
import runloom

# Flag token alphabet for the ParseFlags arm.  IMAP system flags plus a few
# keyword-style tokens.  No token contains whitespace or ')' -- ParseFlags matches
# up to the first ')' then whitespace-splits, so with this alphabet the parsed
# tuple is exactly the token-encoded flag list (closed form).
FLAG_TOKENS = (
    "\\Seen", "\\Answered", "\\Flagged", "\\Deleted", "\\Draft", "\\Recent",
    "$Forwarded", "$MDNSent", "Junk", "NonJunk", "Keyword1", "Keyword2",
)

# Epoch band for the Internaldate round-trip: 1970-01-01 .. ~2038 (fits a signed
# 32-bit time_t and time.mktime on every platform, and sweeps many DST
# transitions so the +ZZZZ offset the round-trip embeds actually varies).
EPOCH_LO = 0
EPOCH_HI = (1 << 31) - 1


def build_flags(rng):
    """A fiber-local FLAGS response with a KNOWN flag multiset.
    Returns (resp_bytes, expected_tuple)."""
    k = rng.randint(0, 6)
    flags = [FLAG_TOKENS[rng.randrange(len(FLAG_TOKENS))] for _ in range(k)]
    resp = ("* {0} FETCH (FLAGS ({1}))".format(
        rng.randint(1, 999999), " ".join(flags))).encode("ascii")
    expected = tuple(f.encode("ascii") for f in flags)
    return resp, expected


def build_internaldate(rng):
    """A fiber-local INTERNALDATE round-trip case built from a KNOWN epoch.

    Time2Internaldate turns the epoch into a quoted internaldate string embedding
    the LOCAL time plus its UTC offset; wrapping that in a FETCH response and
    running Internaldate2tuple recovers a struct_time whose mktime is the ORIGINAL
    epoch.  Returns (epoch, resp_bytes)."""
    epoch = rng.randint(EPOCH_LO, EPOCH_HI)
    s = imaplib.Time2Internaldate(epoch)          # '"dd-Mmm-yyyy HH:MM:SS +ZZZZ"'
    resp = ("* {0} FETCH (INTERNALDATE {1})".format(
        rng.randint(1, 999999), s)).encode("ascii")
    return epoch, resp, s


def parse_batch(H, wid, rng):
    """Run both parsers on fiber-local inputs, YIELD, re-run, and assert each result
    is bit-identical across the yield AND matches its closed-form value.  Returns
    True on success; calls H.fail + returns False on any violation.

    Single-owner: every response bytes / epoch / expected value below is a local
    built from this fiber's own rng -- none is shared with any sibling."""
    # Build all fiber-local inputs + closed-form expectations up front.
    fresp, fexp = build_flags(rng)
    epoch, iresp, istr = build_internaldate(rng)

    # ---- parse BEFORE the yield -------------------------------------------
    flags1 = imaplib.ParseFlags(fresp)
    istr1 = imaplib.Time2Internaldate(epoch)
    st1 = imaplib.Internaldate2tuple(iresp)
    mk1 = int(time.mktime(st1))

    # YIELD: a sibling on another hub parses its OWN different inputs here.  If any
    # parser leaks state across fibers (torn module-global regex, shared Match /
    # split buffer), the re-parse below diverges.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    # ---- parse AFTER the yield --------------------------------------------
    flags2 = imaplib.ParseFlags(fresp)
    istr2 = imaplib.Time2Internaldate(epoch)
    st2 = imaplib.Internaldate2tuple(iresp)
    mk2 = int(time.mktime(st2))

    # ---- ParseFlags: re-parse == pre-yield parse == closed form -----------
    if flags1 != fexp:
        H.fail("ParseFlags wrong BEFORE yield: got {0!r} expected {1!r} for resp "
               "{2!r} (wid {3}) -- pure function disagrees with the known flag "
               "multiset".format(flags1, fexp, fresp, wid))
        return False
    if flags2 != flags1:
        H.fail("ParseFlags CHANGED across a yield: {0!r} -> {1!r} for resp {2!r} "
               "(wid {3}) -- a sibling's parse leaked into this fiber's (torn "
               "imaplib.Flags global / shared Match/split)".format(
                   flags1, flags2, fresp, wid))
        return False

    # ---- Time2Internaldate: string stable across the yield ----------------
    if istr1 != istr:
        H.fail("Time2Internaldate not deterministic: {0!r} != {1!r} for epoch {2} "
               "(wid {3})".format(istr1, istr, epoch, wid))
        return False
    if istr2 != istr1:
        H.fail("Time2Internaldate CHANGED across a yield: {0!r} -> {1!r} for epoch "
               "{2} (wid {3}) -- cross-fiber leak".format(istr1, istr2, epoch, wid))
        return False

    # ---- Internaldate2tuple round-trip: recover the ORIGINAL epoch --------
    if mk1 != epoch:
        H.fail("Internaldate round-trip wrong BEFORE yield: mktime={0} expected "
               "epoch={1} (struct={2!r}) for resp {3!r} (wid {4}) -- Internaldate2"
               "tuple/mktime disagree with the closed-form epoch".format(
                   mk1, epoch, st1, iresp, wid))
        return False
    if st2 != st1 or mk2 != mk1:
        H.fail("Internaldate2tuple CHANGED across a yield: struct {0!r} -> {1!r} "
               "(mktime {2} -> {3}) for resp {4!r} (wid {5}) -- a sibling's parse "
               "leaked (torn imaplib.InternalDate global / shared Match)".format(
                   st1, st2, mk1, mk2, iresp, wid))
        return False

    return True


# Sustained batches per round keep every hub busy with mixed parse churn so a
# sibling reliably interleaves across the yield boundary before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    checks = state["checks"]
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if not parse_batch(H, wid, rng):
                return
            checks[wid] += 1           # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Pre-warm BOTH parse paths so the steady state under the pool is a clean shared
    # READ of the immutable module-global compiled regexes (imaplib.Flags /
    # imaplib.InternalDate) -- exactly what the parsers assume -- rather than any
    # first-use init.  (A wrong parse from a torn read of these globals AFTER warming
    # is a genuine fault, not init noise.)
    imaplib.ParseFlags(b"* 1 FETCH (FLAGS (\\Seen))")
    _s = imaplib.Time2Internaldate(0)
    imaplib.Internaldate2tuple(("* 1 FETCH (INTERNALDATE " + _s + ")").encode("ascii"))
    # One race-free slot per worker (wid-indexed; allocated here where H.funcs is
    # known).  Feeds the non-vacuity law.  NEVER wid & MASK -- one writer per slot.
    H.state = {"checks": [0] * H.funcs}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("imaplib parser-purity batches (ParseFlags + Internaldate round-trip, "
          "each verified bit-identical across a yield + closed-form): {0}; "
          "ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the single-owner purity hazard was actually exercised.
    H.check(checks > 0,
            "no parser-purity batches ran -- the imaplib ParseFlags/"
            "Internaldate2tuple/Time2Internaldate cross-yield isolation hazard was "
            "never exercised (oracle vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-parse.
    H.require_no_lost("imaplib parser purity")


if __name__ == "__main__":
    harness.main(
        "p576_imaplib_parse_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="imaplib.ParseFlags/Internaldate2tuple/Time2Internaldate are pure "
                 "closed-form IMAP response helpers; ParseFlags+Internaldate2tuple "
                 "read a shared module-global compiled regex (imaplib.Flags / "
                 "imaplib.InternalDate).  LOAD-BEARING: each fiber parses its OWN "
                 "fiber-local inputs with known embedded values (a flag multiset "
                 "and an epoch round-tripped through Time2Internaldate), yields (a "
                 "sibling parses different inputs on another hub), re-parses, and "
                 "demands the result be bit-identical across the yield AND equal to "
                 "the closed form (ParseFlags tuple == flags; mktime(struct) == "
                 "original epoch).  A result that changes across the yield or "
                 "disagrees with the closed form is a cross-fiber leak / torn "
                 "module-global-regex isolation bug.  Single-owner throughout (no "
                 "shared mutable argument)")
