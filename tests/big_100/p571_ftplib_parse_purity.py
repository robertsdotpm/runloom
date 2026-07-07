"""big_100 / 571 -- ftplib response-parser PURITY across a yield under M:N.

ftplib ships a family of pure, side-effect-free response parsers that turn a
server's control-channel reply line into a structured value:

  * parse227(resp)        -- '227 ... (h1,h2,h3,h4,p1,p2)' PASV reply
                             -> ('h1.h2.h3.h4', (p1<<8)+p2)
  * parse229(resp, peer)  -- '229 ... (|||port|)' EPSV reply
                             -> (peer[0], port)
  * parse257(resp)        -- '257 "dirname" ...' MKD/PWD reply -> 'dirname'
  * parse150(resp)        -- '150 ... (N bytes)' RETR reply     -> N (or None)

Each is a CLOSED-FORM function of its argument string alone: given the same input
it must return the same output, byte-for-byte, forever.  parse227 and parse150
lazily compile a MODULE-GLOBAL compiled-regex (ftplib._227_re / ftplib._150_re) on
first use -- a `global X; if X is None: X = re.compile(...)` idiom that, GIL off,
is touched concurrently by every fiber.  parse227 also walks a regex Match object;
parse257 runs a hand-written character scanner over the string.  None of that is
per-call state a sibling should be able to perturb: the inputs are fiber-LOCAL
strings and the outputs are fresh tuples/ints.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom multiplexes tens
of thousands of goroutines across >1 hub with the GIL off.  A fiber builds a
fiber-local reply string with KNOWN embedded values, parses it, records the result,
YIELDS (so a sibling on another hub interleaves and parses its OWN different reply),
then re-parses the SAME local string and demands a bit-identical result that also
equals the closed-form value computed directly from the known embedded numbers.  A
CORRECT runtime keeps every fiber's parse a pure function of its own local input:
the result before and after the yield are identical and match the closed form.  A
result that DIFFERS across the yield -- or that disagrees with the closed form --
would mean a sibling's parse leaked into this fiber's (torn module-global regex
handed back a wrong/half-built pattern, a Match object or scanner buffer shared
across fibers, a cross-fiber value bleed) -- a real runtime isolation bug.

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (per the HARD RULES):
  * The load-bearing arm's inputs are fiber-LOCAL strings (built from this fiber's
    wid + a private counter), never shared.  The outputs are freshly-allocated
    tuples/ints/str owned only by this fiber.  No shared mutable container feeds the
    fail-fast check, so a failure cannot be the documented "shared object races"
    semantics -- it can only be a runtime isolation/torn-state fault.
  * The module-global regexes (_227_re / _150_re) ARE shared, but they are compiled
    ONCE to a value-STABLE pattern (idempotent lazy init: whichever fiber wins, the
    pattern for a fixed regex string is identical), and setup() pre-warms them so
    the steady state is a shared READ of an immutable compiled pattern -- exactly
    what every parser assumes.  A wrong parse from a torn read of that global is a
    genuine fault, not documented Python semantics.
  * Verified against plain threads: 8 OS threads each parsing their own reply lines
    (GIL on and off) return the closed-form value 100% of the time, 0 cross-thread
    bleed.  A correct runloom must match, so the oracle PASSES (exit 0) with no bug.

ORACLES:
  * LOAD-BEARING -- PARSER PURITY (worker, HARD, fail-fast).  Per iteration a fiber
    builds four fiber-local reply strings with unique known values, parses each,
    yields, re-parses, and asserts (a) the re-parse equals the pre-yield parse
    exactly and (b) both equal the closed-form value derived from the known numbers.
    Covers all four parsers so each C/regex/scanner path is exercised.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse (e.g.
    parked inside a torn regex lookup) never returns; the watchdog catches it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

Counters: a per-wid race-free slot table (one writer per slot, allocated in setup
where H.funcs is known) tallies completed check-batches for the non-vacuity law.

FAIL ON: a parse result that changes across a yield, or disagrees with the closed
form -- a cross-fiber leak / torn module-global-regex / shared-scanner-state fault
in the runtime.  There is NO shared-mutable measured arm because ftplib's parsers
take no shared argument in this design; the whole program is single-owner.

Stresses: ftplib.parse227 / parse229 / parse257 / parse150 pure-function purity,
lazy module-global compiled-regex (_227_re/_150_re) shared-read under GIL-off M:N,
re.Match walk + hand-written char scanner isolation across hub migration + yield.

Good TSan / controlled-M:N-replay target: the `global _227_re; if _227_re is None:
_227_re = re.compile(...)` lazy init is a textbook publish-once race; a TSan report
on that global, or a replay handing back a half-built pattern, would localize the
fault before the closed-form mismatch even fires (setup() pre-warms it so the
steady state is a clean shared read -- a fault there is real, not init churn).
"""
import ftplib

import harness
import runloom

# Number of distinct parser CASES exercised per batch (all four parsers).  Each is
# fed a fiber-local reply string with known embedded values so the expected output
# is closed-form.
NCASES = 4


def build_227(rng):
    """A fiber-local '227' PASV reply with known octets + port halves.
    Returns (resp, expected_host, expected_port)."""
    h0 = rng.randint(0, 255)
    h1 = rng.randint(0, 255)
    h2 = rng.randint(0, 255)
    h3 = rng.randint(0, 255)
    p1 = rng.randint(0, 255)
    p2 = rng.randint(0, 255)
    resp = "227 Entering Passive Mode ({0},{1},{2},{3},{4},{5}).".format(
        h0, h1, h2, h3, p1, p2)
    exp_host = "{0}.{1}.{2}.{3}".format(h0, h1, h2, h3)
    exp_port = (p1 << 8) + p2
    return resp, exp_host, exp_port


def build_229(rng):
    """A fiber-local '229' EPSV reply with a known port + peer host.
    Returns (resp, peer, expected_host, expected_port)."""
    port = rng.randint(1, 65535)
    # A distinct, recognizable peer host per call (never shared).
    peer_host = "203.0.{0}.{1}".format(rng.randint(0, 255), rng.randint(0, 255))
    peer = (peer_host, 21)
    resp = "229 Entering Extended Passive Mode (|||{0}|)".format(port)
    return resp, peer, peer_host, port


# Directory-name alphabet: no embedded double-quotes, so parse257's quote-doubling
# rule never rewrites the name -- expected output equals the literal name.
DIRCHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/_-.~"


def build_257(rng):
    """A fiber-local '257' MKD/PWD reply with a known directory name.
    Returns (resp, expected_dirname)."""
    n = rng.randint(1, 24)
    name = "/" + "".join(DIRCHARS[rng.randrange(len(DIRCHARS))] for _ in range(n))
    resp = '257 "{0}" created.'.format(name)
    return resp, name


def build_150(rng):
    """A fiber-local '150' RETR reply with a known byte count.
    Returns (resp, expected_size)."""
    size = rng.randint(0, 1 << 34)
    resp = ("150 Opening BINARY mode data connection for datafile "
            "({0} bytes)".format(size))
    return resp, size


def parse_batch(H, wid, rng):
    """Run all four parsers on fiber-local inputs, YIELD, re-run, and assert each
    result is bit-identical across the yield AND matches its closed-form value.
    Returns True on success; calls H.fail + returns False on any violation.

    Single-owner: every reply string, peer tuple, and expected value below is a
    local built from this fiber's own rng -- none is shared with any sibling."""
    # Build all fiber-local inputs + closed-form expectations up front.
    r227, exp_host, exp_port = build_227(rng)
    r229, peer, exp_h229, exp_p229 = build_229(rng)
    r257, exp_dir = build_257(rng)
    r150, exp_size = build_150(rng)

    # ---- parse BEFORE the yield -------------------------------------------
    host1, port1 = ftplib.parse227(r227)
    h229_1, p229_1 = ftplib.parse229(r229, peer)
    dir1 = ftplib.parse257(r257)
    size1 = ftplib.parse150(r150)

    # YIELD: a sibling on another hub parses its OWN different replies here.  If any
    # parser leaks state across fibers (torn module-global regex, shared Match /
    # scanner buffer), the re-parse below diverges.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    # ---- parse AFTER the yield --------------------------------------------
    host2, port2 = ftplib.parse227(r227)
    h229_2, p229_2 = ftplib.parse229(r229, peer)
    dir2 = ftplib.parse257(r257)
    size2 = ftplib.parse150(r150)

    # ---- purity: re-parse == pre-yield parse == closed form ---------------
    # parse227 (PASV): host + port.
    if host1 != exp_host or port1 != exp_port:
        H.fail("parse227 wrong BEFORE yield: got ({0!r},{1!r}) expected "
               "({2!r},{3!r}) for resp {4!r} (wid {5}) -- pure function "
               "disagrees with closed form".format(
                   host1, port1, exp_host, exp_port, r227, wid))
        return False
    if host2 != host1 or port2 != port1:
        H.fail("parse227 CHANGED across a yield: ({0!r},{1!r}) -> ({2!r},{3!r}) "
               "for resp {4!r} (wid {5}) -- a sibling's parse leaked into this "
               "fiber's (torn _227_re global / shared Match)".format(
                   host1, port1, host2, port2, r227, wid))
        return False

    # parse229 (EPSV): host echoed from peer + port.
    if h229_1 != exp_h229 or p229_1 != exp_p229:
        H.fail("parse229 wrong BEFORE yield: got ({0!r},{1!r}) expected "
               "({2!r},{3!r}) for resp {4!r} peer {5!r} (wid {6})".format(
                   h229_1, p229_1, exp_h229, exp_p229, r229, peer, wid))
        return False
    if h229_2 != h229_1 or p229_2 != p229_1:
        H.fail("parse229 CHANGED across a yield: ({0!r},{1!r}) -> ({2!r},{3!r}) "
               "for resp {4!r} peer {5!r} (wid {6}) -- cross-fiber leak".format(
                   h229_1, p229_1, h229_2, p229_2, r229, peer, wid))
        return False

    # parse257 (MKD/PWD): directory name.
    if dir1 != exp_dir:
        H.fail("parse257 wrong BEFORE yield: got {0!r} expected {1!r} for resp "
               "{2!r} (wid {3}) -- scanner disagrees with the embedded name".format(
                   dir1, exp_dir, r257, wid))
        return False
    if dir2 != dir1:
        H.fail("parse257 CHANGED across a yield: {0!r} -> {1!r} for resp {2!r} "
               "(wid {3}) -- a sibling's parse257 scanner state leaked in".format(
                   dir1, dir2, r257, wid))
        return False

    # parse150 (RETR size): int byte count.
    if size1 != exp_size:
        H.fail("parse150 wrong BEFORE yield: got {0!r} expected {1!r} for resp "
               "{2!r} (wid {3}) -- torn _150_re global handed back a wrong "
               "size".format(size1, exp_size, r150, wid))
        return False
    if size2 != size1:
        H.fail("parse150 CHANGED across a yield: {0!r} -> {1!r} for resp {2!r} "
               "(wid {3}) -- cross-fiber leak on the module-global regex".format(
                   size1, size2, r150, wid))
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
    # Pre-warm the lazy module-global compiled regexes (ftplib._227_re /
    # ftplib._150_re) so the steady state under the pool is a clean shared READ of
    # an immutable compiled pattern -- exactly what the parsers assume -- rather
    # than compile-once init churn.  (A wrong parse from a torn read of these
    # globals AFTER warming is a genuine fault, not init noise.)
    ftplib.parse227("227 Entering Passive Mode (127,0,0,1,7,138).")
    ftplib.parse150("150 Opening BINARY mode data connection for f (1 bytes)")
    # One race-free slot per worker (wid-indexed; allocated here where H.funcs is
    # known).  Feeds the non-vacuity law.  NEVER wid & MASK -- one writer per slot.
    H.state = {"checks": [0] * H.funcs}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("ftplib parser-purity batches (all four parsers, each verified bit-"
          "identical across a yield + closed-form): {0}; ops={1}".format(
              checks, H.total_ops()))
    # NON-VACUITY: the single-owner purity hazard was actually exercised.
    H.check(checks > 0,
            "no parser-purity batches ran -- the ftplib parse227/229/257/150 "
            "cross-yield isolation hazard was never exercised (oracle vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-parse.
    H.require_no_lost("ftplib parser purity")


if __name__ == "__main__":
    harness.main(
        "p571_ftplib_parse_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="ftplib.parse227/parse229/parse257/parse150 are pure closed-form "
                 "reply parsers (PASV/EPSV/MKD-PWD/RETR); parse227+parse150 lazily "
                 "compile a shared module-global regex.  LOAD-BEARING: each fiber "
                 "parses its OWN fiber-local reply strings with known embedded "
                 "values, yields (a sibling parses different replies on another "
                 "hub), re-parses, and demands the result be bit-identical across "
                 "the yield AND equal to the closed form.  A result that changes "
                 "across the yield or disagrees with the closed form is a cross-"
                 "fiber leak / torn module-global-regex isolation bug.  Single-"
                 "owner throughout (no shared mutable argument)")
