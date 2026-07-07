"""big_100 / 599 -- smtplib DATA-framing (dot-stuffing + CRLF) PURITY + ROUND-TRIP
conservation under M:N.

smtplib's outbound message body passes through the RFC 5321 "transparency"
transform before it hits the wire: smtplib.quotedata(data) first normalizes every
line ending to CRLF (smtplib._fix_eols, a re.sub of `(?:\r\n|\n|\r(?!\n))` -> CRLF)
and then dot-stuffs -- any line that BEGINS with '.' gets a second leading '.' (a
re.sub of `(?m)^dot` -> '..').  The bytes twin smtplib._quote_periods does the
dot-stuffing half on a bytes object.  These are PURE functions of their argument:
same input -> same output, always, with no instance/global state of their own.
The only state they touch is the module-level compiled `re` pattern cache, which is
READ-ONLY during matching (documented thread-safe) -- exactly like colorsys touching
the math tables.  So each is a legitimate single-owner PURITY oracle when the input
is fiber-local.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under runloom, thousands of
fibers call quotedata()/quote_periods()/quoteaddr() across hubs with the GIL off,
parking on a cooperative yield mid-workload.  A pure transform MUST return a value
that (a) exactly equals the closed-form expected computed independently, and (b) is
BIT-IDENTICAL across a yield for the same fiber-local input.  If a fiber's result
changes across a yield, or diverges from the closed form, that points at torn shared
state (the re engine's SRE state, the pattern cache, a scratch buffer) leaking across
fibers, a lost/duplicated character in the C sub loop, or a cross-fiber value swap --
a real runtime bug, not documented Python semantics.  We ALSO verify the SMTP
framing CONSERVATION law: dot-stuffing is reversible at the receiver, so
un-stuffing quotedata()'s output must recover exactly the CRLF-normalized body (no
byte gained or lost in transit).

WHICH ORACLE IS LOAD-BEARING, AND WHY.  quotedata/_quote_periods/quoteaddr are the
documented public interface and are referentially transparent.  We verified against
a plain-threads control (8 OS threads, GIL on AND off, each transforming its own
fiber-local body): 100% of results equal the closed form and are stable -- 0
divergences.  Under a CORRECT runloom it must also hold, so the single-owner oracle
PASSES on a correct runtime (exits 0 when there is no bug).  Every input is built in
a fiber-local variable and never shared, so a divergence cannot be "shared mutable
container raced" -- it can only be a runtime corruption.

ORACLES:
  * LOAD-BEARING -- SMTP FRAMING PURITY + ROUND-TRIP (worker, HARD, fail-fast).
    Each fiber builds its OWN random message body (mixed \n / \r / \r\n endings,
    lines starting with 0/1/2/3 dots, blank lines) and its OWN address strings.
    It independently computes -- WITHOUT calling re/smtplib -- the closed-form
    CRLF-normalized body and the closed-form dot-stuffed wire body.  Then:
      - quotedata(body) MUST equal the closed-form wire body (correctness);
      - _quote_periods(normalized.encode) MUST equal the wire body encoded
        (bytes twin agrees);
      - YIELD (yield_now / sleep) so siblings interleave on this + other hubs;
      - quotedata(body) recomputed MUST be BIT-IDENTICAL to the pre-yield result
        AND still equal the closed form (stability across a hub migration);
      - un-stuffing the wire body MUST recover the CRLF-normalized body exactly
        (the DATA-transparency conservation law: no byte lost/gained);
      - quoteaddr(addr) MUST be deterministic across the yield (a1 == a2 == a
        fresh recompute) for each fiber-local address.
    Single-owner: the body/address are fiber-local; a failure is a runloom purity
    or framing-conservation desync.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside the C
    re.sub loop (parked mid-transform, never re-woken) never returns; the
    watchdog + require_no_lost catch it.

FAIL ON: quotedata/_quote_periods/quoteaddr returning a value that differs from the
independently computed closed form, a result that changes across a yield for the
same fiber-local input, or an un-stuff that does not recover the normalized body
(a lost/duplicated byte on the SMTP DATA framing path).

Stresses: smtplib.quotedata / _fix_eols / _quote_periods / quoteaddr referential
transparency under M:N, the shared `re` compiled-pattern cache + SRE match state
under concurrent re.sub from many hubs, dot-stuffing/CRLF framing conservation,
value/identity stability of a pure transform across a cooperative yield + hub
migration.

Good TSan / controlled-M:N-replay target: the re.sub inside quotedata runs the C
SRE engine over the shared compiled pattern; a data-race report on the pattern's
match state, or a replay that returns a body one byte short, localizes the
corruption before the closed-form comparison even fires.
"""
import smtplib

import harness
import runloom

# CRLF, per RFC 5321 -- the canonical line ending quotedata normalizes to.
CRLF = "\r\n"

# The characters that make up a random line's body (no CR/LF here; endings are
# inserted separately so we control the EOL mix).  Kept to printable latin-1 so
# the bytes twin (_quote_periods on an encoded body) round-trips through
# latin-1 losslessly.
BODY_CHARS = ("abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ"
              "0123456789 .,;:!?@#$%&*()-_=+[]{}")

# The EOL variants a real message might carry; _fix_eols must collapse each to
# CRLF.  A bare '\r' NOT followed by '\n' (Mac), a bare '\n' (Unix), and a real
# '\r\n' (already canonical) all exercise a different alternative of the
# `(?:\r\n|\n|\r(?!\n))` pattern.
EOLS = ["\n", "\r", "\r\n"]

# Sustained checks per fiber round: the purity/stability hazard only manifests
# under SUSTAINED churn -- many fibers running the C re.sub while parked across
# their yield, so a sibling reliably runs a transform on another hub before this
# fiber resumes.  A single transform barely overlaps and does not reproduce.
INNER_CAP = 100000


# --------------------------------------------------------------------------
# CLOSED-FORM reference transforms.  These reimplement smtplib's framing WITHOUT
# calling re or smtplib, so the comparison is a genuine independent oracle, not
# "smtplib agrees with itself".
# --------------------------------------------------------------------------
def norm_eols_ref(s):
    """Independent reimplementation of smtplib._fix_eols: collapse '\\r\\n',
    lone '\\n', and lone '\\r' (Mac, not followed by '\\n') all to CRLF.  Matches
    the re alternation `(?:\\r\\n|\\n|\\r(?!\\n))` -> CRLF exactly."""
    out = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == "\r":
            if i + 1 < n and s[i + 1] == "\n":
                out.append(CRLF)
                i += 2
            else:                       # lone \r (Mac)
                out.append(CRLF)
                i += 1
        elif c == "\n":                 # lone \n (Unix)
            out.append(CRLF)
            i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def dot_stuff_ref(s):
    """Independent reimplementation of the `(?m)^\\.` -> '..' dot-stuffing on a
    CRLF-normalized body.  MULTILINE ^ matches at position 0 and immediately
    after each '\\n'; a '.' there is doubled."""
    out = []
    at_line_start = True                # position 0 is a line start
    for c in s:
        if at_line_start and c == ".":
            out.append("..")            # double the leading dot
        else:
            out.append(c)
        at_line_start = (c == "\n")     # next char begins a line iff we just saw \n
    return "".join(out)


def dot_unstuff_ref(s):
    """Inverse of dot-stuffing (what an RFC 5321 receiver does): remove exactly
    ONE leading '.' from any line that begins with a '.'.  dot_unstuff_ref(
    dot_stuff_ref(x)) == x for any CRLF-normalized x -- the DATA transparency
    conservation law."""
    out = []
    at_line_start = True
    for c in s:
        if at_line_start and c == ".":
            # Drop exactly one leading dot; the char after is not a line start.
            at_line_start = False
            continue
        out.append(c)
        at_line_start = (c == "\n")
    return "".join(out)


def build_body(rng):
    """Build one fiber-local message body with a mix of EOLs and leading-dot
    lines, so both _fix_eols and dot-stuffing have real work to do."""
    nlines = rng.randint(1, 12)
    parts = []
    for _ in range(nlines):
        ndots = rng.randint(0, 3)               # 0..3 leading dots
        ln_len = rng.randint(0, 10)
        chars = [rng.choice(BODY_CHARS) for _ in range(ln_len)]
        line = ("." * ndots) + "".join(chars)
        parts.append(line)
        parts.append(rng.choice(EOLS))          # a (possibly non-CRLF) ending
    # Occasionally drop the trailing EOL so the last line has none.
    if parts and rng.random() < 0.3:
        parts.pop()
    return "".join(parts)


def build_addr(rng, wid, idx):
    """A fiber-local address string for the quoteaddr determinism check.  Mixes
    the bare-address and display-name forms email.utils.parseaddr handles."""
    local = "u{0}x{1}".format(wid, idx)
    host = "h{0}.example".format(rng.randint(0, 999))
    form = rng.randint(0, 3)
    if form == 0:
        return "{0}@{1}".format(local, host)
    if form == 1:
        return "<{0}@{1}>".format(local, host)
    if form == 2:
        return "Display Name <{0}@{1}>".format(local, host)
    return "  {0}@{1}  ".format(local, host)    # surrounding whitespace


def one_check(H, wid, idx, rng, state):
    """One single-owner SMTP-framing purity + round-trip check (fail-fast)."""
    body = build_body(rng)

    # Independent closed-form expected values (no re / smtplib used).
    normalized = norm_eols_ref(body)
    wire = dot_stuff_ref(normalized)

    # 1) quotedata correctness vs the closed form.
    got1 = smtplib.quotedata(body)
    if got1 != wire:
        H.fail("quotedata() DIVERGED from closed form (wid {0} idx {1}): "
               "body={2!r} got={3!r} expected={4!r} -- the SMTP DATA framing "
               "transform produced the wrong bytes (torn re.sub / cross-fiber "
               "state)".format(wid, idx, body, got1, wire))
        return

    # 2) bytes twin _quote_periods agrees (dot-stuffing on the normalized body).
    braw = normalized.encode("latin-1")
    got_b = smtplib._quote_periods(braw)
    exp_b = wire.encode("latin-1")
    if got_b != exp_b:
        H.fail("_quote_periods() DIVERGED from closed form (wid {0} idx {1}): "
               "got={2!r} expected={3!r} -- the bytes dot-stuffing path lost or "
               "duplicated a byte".format(wid, idx, got_b, exp_b))
        return

    # 3) quoteaddr determinism baseline (recompute across the yield below).
    addr = build_addr(rng, wid, idx)
    qa1 = smtplib.quoteaddr(addr)

    # YIELD: park so siblings run their own transforms on this + other hubs.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0002)

    # 4) STABILITY across the yield: same fiber-local input -> bit-identical out.
    got2 = smtplib.quotedata(body)
    if got2 != got1:
        H.fail("quotedata() CHANGED across a yield (wid {0} idx {1}): before="
               "{2!r} after={3!r} for the SAME fiber-local body -- a pure "
               "transform must be stable across a hub migration".format(
                   wid, idx, got1, got2))
        return
    if got2 != wire:
        H.fail("quotedata() post-yield DIVERGED from closed form (wid {0} idx "
               "{1}): got={2!r} expected={3!r}".format(wid, idx, got2, wire))
        return

    # 5) FRAMING CONSERVATION: un-stuffing the wire body recovers the normalized
    #    body exactly -- no byte lost or gained on the DATA path.
    recovered = dot_unstuff_ref(got2)
    if recovered != normalized:
        H.fail("SMTP DATA transparency BROKEN (wid {0} idx {1}): un-stuff of "
               "wire body did not recover the normalized body: recovered={2!r} "
               "normalized={3!r} -- a byte was lost or duplicated in the framing "
               "round-trip".format(wid, idx, recovered, normalized))
        return

    # 6) quoteaddr determinism across the yield: a1 == a2 == fresh recompute.
    qa2 = smtplib.quoteaddr(addr)
    qa3 = smtplib.quoteaddr(addr)
    if not (qa1 == qa2 == qa3):
        H.fail("quoteaddr() NON-DETERMINISTIC across a yield (wid {0} idx {1}): "
               "addr={2!r} -> {3!r} / {4!r} / {5!r} -- a pure address quoter must "
               "return the same value every time".format(
                   wid, idx, addr, qa1, qa2, qa3))
        return

    state["checks"][wid] += 1           # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            one_check(H, wid, idx, rng, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # One race-free non-vacuity slot per worker (wid-indexed; H.funcs known here).
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("smtplib framing purity+round-trip checks: {0} (all passed fail-fast); "
          "ops={1}".format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing single-owner arm actually ran.
    H.check(checks > 0,
            "no smtplib framing checks ran -- the quotedata/quoteaddr purity + "
            "DATA-transparency oracle was never exercised (would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished inside the C re.sub transform.
    H.require_no_lost("smtplib framing purity")


if __name__ == "__main__":
    harness.main(
        "p599_smtplib_quotedata", body, setup=setup, post=post,
        default_funcs=8000,
        describe="smtplib.quotedata/_quote_periods/quoteaddr are PURE SMTP DATA "
                 "framing transforms (CRLF-normalize + dot-stuff, RFC 5321 "
                 "transparency).  LOAD-BEARING: each fiber transforms its OWN "
                 "random body and asserts the result equals an independently "
                 "computed closed form, is BIT-IDENTICAL across a yield/hub "
                 "migration, and that un-stuffing recovers the normalized body "
                 "exactly (framing conservation); quoteaddr must be deterministic "
                 "across the yield.  A divergence from the closed form, a value "
                 "that changes across the yield, or a lost/duplicated byte on the "
                 "round-trip is the runloom bug the shared re-engine could cause")
