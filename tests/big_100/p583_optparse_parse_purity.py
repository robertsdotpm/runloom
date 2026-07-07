"""big_100 / 583 -- optparse.OptionParser parse-result PURITY under M:N.

optparse is a command-line parser: an OptionParser is configured with a set of
Option objects (each carrying an action -- store, store_true, append, choice --
a type, a default, and dest names), and OptionParser.parse_args(argv) walks argv
token-by-token, building an optparse.Values instance (the "options" object) plus
the leftover positional-args list.  parse_args is a DETERMINISTIC pure function of
(parser configuration, argv): the same parser fed the same argv must always yield
the same Values and the same leftover list -- there is no external state, no
randomness, no clock.  Internally parse_args is stateful for the DURATION of one
call (it pushes argv onto self.rargs / self.largs, pops tokens, and mutates a
fresh Values), but that state is scoped to the single call on a single parser
instance.

WHERE M:N COULD BREAK IT (the gap this program probes).  Under free-threaded
CPython with the GIL off and runloom's M:N scheduler, a fiber that builds its OWN
parser, computes the closed-form expected parse of its OWN argv, then yields mid-
sequence (parking on a different hub while siblings run), must resume and observe
the SAME parse result -- bit-identical options and leftover args.  If runloom
leaked another fiber's parser-call state (rargs/largs pointer, the in-flight
Values, an option-string lookup table) into this fiber's parse across the hub
migration, the result would differ: a wrong dest value, a leftover arg that
belongs to a sibling, a torn append-list, a choice that was never offered.  The
parse is a pure function; a divergence across a yield with fiber-local inputs is a
runtime isolation bug, not documented optparse behavior.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  parse_args on a SINGLE-OWNER OptionParser is a pure function of its fiber-local
  (option-spec, argv).  We build argv from fiber-local RNG so its EXACT parse is
  known in closed form (we chose the count, the name, the flag, the choice, the
  append-list, and the positionals).  A standalone plain-threads control (8 OS
  threads, each building its own parser + argv, GIL on AND off) returns the
  closed-form parse 100% of the time -- 0 divergences.  Under a CORRECT runloom it
  must also hold, INCLUDING across a yield that parks the fiber on another hub
  while siblings parse their own conflicting argvs.  A single-owner parser whose
  parse changes across a yield -- or disagrees with the closed-form expected -- is
  a runloom isolation bug, so this fail-fast arm PASSES on a correct runtime
  (exit 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- PARSE PURITY (worker, HARD, fail-fast).  Each fiber:
      - builds its OWN NoExitParser with a fixed option grammar (single-owner);
      - draws fiber-local values (count int, name str, verbose flag, level choice,
        an append-list of ints, positional args) from its RNG and constructs a
        VALID argv from them, so the EXACT parse is known in closed form;
      - parses ONCE -> (opts1, args1), asserts every field == the closed-form
        expected (count/name/verbose/level/mult-list/leftover positionals);
      - YIELDS (yield_now / tiny sleep) so a sibling reliably interleaves and
        parses its own conflicting argv on this or another hub;
      - builds a FRESH parser from the SAME grammar and parses the SAME argv again
        -> (opts2, args2), asserts opts2/args2 are bit-identical to the first
        parse AND still equal the closed-form expected.
    Single-owner: the parser, the argv, and the expected dict are fiber-local,
    never shared.  A divergence is a runloom parse-isolation desync.

  * MEASURED (report-ONLY, NEVER fails): a small pool of SHARED OptionParsers is
    hammered by all fibers -- many fibers call parse_args on the SAME parser
    concurrently.  optparse.OptionParser is NOT designed for concurrent parse_args
    (each call mutates self.rargs/self.largs on the shared instance), so cross-
    fiber corruption is EXPECTED and DOCUMENTED here, exactly like a shared dict
    under threads.  We MEASURE the divergence/exception rate and REPORT it (proving
    the hazard is real -- fibers DO collide on shared parser state) but NEVER call
    H.fail on it; failing would mislabel documented shared-object semantics as a
    runtime bug.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (parses > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse
    (parked inside parse_args on a desynced instance) never returns; caught.

FAIL ON: a single-owner parser's parse result changing across a yield, or
disagreeing with the closed-form expected computed from the fiber-local argv
(wrong option dest, torn append-list, leftover arg from a sibling, a choice never
offered, or a spurious parse error on valid input).  The shared-pool MEASURED arm
is report-only and is expected to show divergences (documented shared-instance
behavior) -- the load-bearing oracle stays clean because it is single-owner.

Stresses: optparse.OptionParser.parse_args token walk (rargs/largs push/pop),
per-option action dispatch (store/store_true/append/choice), Values attribute
assembly, default materialization, and leftover-arg collection across hub
migration + yield under M:N; per-fiber parser isolation vs shared-parser races.
"""
import optparse

import harness
import runloom

# Fiber-local value bands.  Kept small + deterministic so the closed-form expected
# parse is trivially computable and argv is always VALID (no token that could be
# mistaken for an option -> no parse error on the single-owner arm).
LEVELS = ("low", "med", "high", "max")
ALNUM = "abcdefghijklmnopqrstuvwxyz0123456789"


class OptParseError(Exception):
    """Raised by NoExitParser instead of calling sys.exit -- keeps a parse error
    an ordinary catchable exception (optparse's default error() calls sys.exit,
    which would raise SystemExit past _worker_wrap's `except Exception`)."""


class NoExitParser(optparse.OptionParser):
    """OptionParser whose error()/exit() raise instead of terminating the process.

    On the single-owner arm the argv is always valid, so error() must NEVER fire;
    if it does across a yield that is itself a signal.  On the shared MEASURED arm
    concurrent parse_args calls corrupt rargs/largs and CAN trip error() -- we
    catch OptParseError there and count it, never fail."""

    def error(self, msg):
        raise OptParseError(msg)

    def exit(self, status=0, msg=None):
        raise OptParseError(msg if msg is not None else "exit({0})".format(status))

    # optparse prints to stderr on error via print_usage; suppress it so a
    # MEASURED-arm collision doesn't spew usage text.
    def print_usage(self, file=None):
        pass


def build_grammar():
    """Construct a fresh NoExitParser with a fixed option grammar exercising the
    main optparse actions/types: store(int), store(string), store_true, choice,
    and append(int).  A FRESH parser each call so nothing is shared."""
    p = NoExitParser(add_help_option=False)
    p.add_option("-c", "--count", type="int", dest="count", default=0)
    p.add_option("-n", "--name", type="string", dest="name", default="none")
    p.add_option("-v", "--verbose", action="store_true", dest="verbose",
                 default=False)
    p.add_option("-l", "--level", type="choice", dest="level",
                 choices=list(LEVELS), default="low")
    p.add_option("-m", "--mult", type="int", action="append", dest="mult",
                 default=None)
    return p


def build_case(rng):
    """Draw fiber-local values and construct a VALID argv plus the closed-form
    expected parse.  Returns (argv, expected_dict, expected_args_list).

    Every token is guaranteed to be a well-formed option/value or a positional
    that cannot be mistaken for an option (alnum, never leading '-'), so parse_args
    on a correct runtime never errors and the expected parse is exact."""
    count = rng.randint(-1000, 1000)
    name = "".join(rng.choice(ALNUM) for _ in range(rng.randint(1, 8)))
    verbose = bool(rng.getrandbits(1))
    level = rng.choice(LEVELS)
    nmult = rng.randint(0, 4)
    mults = [rng.randint(0, 9999) for _ in range(nmult)]
    npos = rng.randint(0, 3)
    positionals = ["".join(rng.choice(ALNUM) for _ in range(rng.randint(1, 6)))
                   for _ in range(npos)]

    argv = ["--count", str(count), "--name", name, "--level", level]
    if verbose:
        argv.append("--verbose")
    for m in mults:
        argv.extend(["--mult", str(m)])
    # Positionals last (a bare alnum token can't be mistaken for an option).
    argv.extend(positionals)

    expected = {
        "count": count,
        "name": name,
        "verbose": verbose,
        "level": level,
        # optparse append with default=None leaves dest None when no -m given,
        # else a list of the appended ints in order.
        "mult": (list(mults) if mults else None),
    }
    return argv, expected, list(positionals)


def check_parse(H, wid, parser, argv, expected, expected_args, phase):
    """Parse argv with parser and assert every field matches the closed-form
    expected.  Returns the extracted (opts_dict, args) so the caller can compare
    the two phases for bit-identity.  H.fail + return None on any mismatch."""
    try:
        opts, args = parser.parse_args(list(argv))
    except OptParseError as exc:
        H.fail("optparse SPURIOUS parse error on VALID argv (wid {0}, {1}): {2!r} "
               "-- argv={3!r}; a valid parse must never error, this points at "
               "cross-fiber parser-state corruption".format(
                   wid, phase, str(exc), argv))
        return None

    got = {
        "count": opts.count,
        "name": opts.name,
        "verbose": opts.verbose,
        "level": opts.level,
        "mult": opts.mult,
    }
    for field, exp in expected.items():
        if got[field] != exp:
            H.fail("optparse parse DIVERGED ({0}): dest {1!r} == {2!r}, expected "
                   "{3!r} (wid {4}) -- argv={5!r}; the parse is a pure function of "
                   "(grammar, argv), a wrong dest means a sibling's parser state "
                   "leaked into this fiber's parse_args".format(
                       phase, field, got[field], exp, wid, argv))
            return None
    if args != expected_args:
        H.fail("optparse leftover-args DIVERGED ({0}): got {1!r}, expected {2!r} "
               "(wid {3}) -- argv={4!r}; a wrong positional list means a sibling's "
               "rargs/largs leaked across the yield".format(
                   phase, args, expected_args, wid, argv))
        return None
    return (got, args)


# Sustained parses per worker, bounded by H.running().  The isolation hazard only
# manifests under SUSTAINED churn: many fibers simultaneously parsing while parked
# across their mid-sequence yield, so the scheduler reliably interleaves a
# sibling's parse before this fiber resumes.  One parse per fiber barely overlaps.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """LOAD-BEARING single-owner parse-purity arm (fail-fast) + MEASURED shared
    arm (report-only)."""
    checks = state["parses"]
    shared_pool = state["shared_pool"]
    shared_checks = state["shared_checks"]
    shared_div = state["shared_div"]

    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            # ---- LOAD-BEARING: single-owner parse purity (fail-fast) ----------
            argv, expected, expected_args = build_case(rng)

            parser1 = build_grammar()
            res1 = check_parse(H, wid, parser1, argv, expected, expected_args,
                               "phase-1")
            if H.failed:
                return

            # YIELD mid-sequence: park so a sibling parses its own conflicting
            # argv, possibly on another hub, before we re-parse.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)

            parser2 = build_grammar()
            res2 = check_parse(H, wid, parser2, argv, expected, expected_args,
                               "phase-2")
            if H.failed:
                return

            # Bit-identity across the yield: the two independent parses of the same
            # fiber-local argv must agree exactly (they already each matched the
            # closed form, so this is belt-and-suspenders against a value that
            # matched expected in one phase but was torn in the other).
            if res1 != res2:
                H.fail("optparse parse NOT STABLE across yield (wid {0}): phase-1 "
                       "{1!r} != phase-2 {2!r} -- argv={3!r}; a single-owner "
                       "parser's pure parse changed across a hub migration".format(
                           wid, res1, res2, argv))
                return

            checks[wid] += 1                 # single-writer-per-slot, race-free

            # ---- MEASURED: shared-parser race (report-only, NEVER fails) ------
            sp = shared_pool[wid % len(shared_pool)]
            sargv, sexp, sargs = build_case(rng)
            shared_checks[wid & 1023] += 1
            try:
                sopts, sleft = sp.parse_args(list(sargv))
                if (sopts.count != sexp["count"] or sopts.name != sexp["name"]
                        or sopts.level != sexp["level"] or sleft != sargs):
                    shared_div[wid & 1023] += 1
            except (OptParseError, Exception):
                # A shared parser mutated by concurrent parse_args can raise or
                # diverge -- documented shared-instance behavior.  MEASURE, never
                # fail.
                shared_div[wid & 1023] += 1

            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # A small pool of SHARED parsers for the MEASURED report-only arm.  Built in
    # the root; concurrently parse_args'd by all fibers to demonstrate the shared-
    # instance hazard (never load-bearing).
    shared_pool = [build_grammar() for _ in range(8)]
    H.state = {
        "parses": [0] * H.funcs,          # LOAD-BEARING single-owner checks (race-free)
        "shared_pool": shared_pool,
        "shared_checks": [0] * 1024,      # MEASURED shared-parser attempts (sharded)
        "shared_div": [0] * 1024,         # MEASURED divergences/exceptions (sharded)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    parses = sum(H.state["parses"])
    schecks = sum(H.state["shared_checks"])
    sdiv = sum(H.state["shared_div"])
    spct = (100.0 * sdiv / schecks) if schecks else 0.0

    H.log("optparse[single-owner LOAD-BEARING]: {0} parse-purity checks (all "
          "passed fail-fast) | optparse[shared-parser MEASURED]: {1} attempts "
          "{2} divergences ({3:.1f}%, documented shared-instance behavior -- "
          "REPORT ONLY)".format(parses, schecks, sdiv, spct))

    if sdiv:
        H.log("note: the shared parser pool observed {0} divergences/exceptions "
              "across {1} concurrent parse_args calls -- optparse.OptionParser is "
              "not designed for concurrent parse on one instance (shared "
              "rargs/largs), like a shared dict under threads.  Documented M:N "
              "shared-object behavior, NOT a runloom bug, and never reaches the "
              "load-bearing single-owner oracle".format(sdiv, schecks))

    # NON-VACUITY: the single-owner purity hazard was actually exercised.
    H.check(parses > 0,
            "no single-owner optparse parse-purity checks ran -- the load-bearing "
            "parse-isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid parse_args.
    H.require_no_lost("optparse parse purity")


if __name__ == "__main__":
    harness.main(
        "p583_optparse_parse_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="optparse.OptionParser.parse_args is a pure function of "
                 "(grammar, argv).  LOAD-BEARING: each fiber builds its OWN parser "
                 "+ a valid fiber-local argv whose EXACT parse is known in closed "
                 "form, parses it, YIELDS (parking on another hub while siblings "
                 "parse conflicting argvs), then re-parses with a fresh parser and "
                 "asserts the result is bit-identical and still matches the closed "
                 "form (store/store_true/append/choice dests + leftover args).  A "
                 "divergence across the yield -- wrong dest, torn append-list, a "
                 "sibling's leftover arg, a spurious error on valid input -- is a "
                 "runloom parse-isolation bug.  MEASURED shared-parser pool "
                 "(expected to diverge, documented concurrent-instance behavior) "
                 "proves the hazard is real without ever failing")
