"""big_100 / 557 -- argparse parse round-trip PURITY under M:N.

argparse.ArgumentParser is a stateful object: add_argument() builds a private
_actions list, a _option_string_actions map, per-action type/default/choices
metadata, and mutual-exclusion / subparser bookkeeping -- all set at
CONSTRUCTION time.  parse_args(argv) is then meant to be a PURE FUNCTION of
(parser, argv): it walks argv, runs each token through the matching action's
type converter + choices check, and returns a FRESH Namespace of the resolved
values.  It does NOT mutate the parser (defaults live on the parser; the
resolved values live on the returned namespace), so the SAME parser parsing the
SAME argv must return the SAME namespace values every time -- a closed-form,
bit-identical round trip.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs fibers in
PARALLEL across hubs with the GIL off.  argparse's parse path threads a mutable
`namespace`, an intermediate `seen_actions` set, an `arg_strings` list, and the
per-action `_get_values` type-conversion loop through many small helper calls
(_parse_known_args -> consume_optional/consume_positionals -> take_action ->
_get_values -> action.__call__).  If ANY of that intermediate machinery leaked
across fibers -- a shared scratch buffer, a torn list append, a type-converter
result written to the wrong fiber's namespace, an identity/value change of a
resolved attribute across a yield -- a fiber would read back a value that is NOT
the closed-form expected result for its OWN fiber-local argv.  That is the
runtime bug this catches.

SINGLE-OWNER, why it is load-bearing (verified against plain threads):

  Each fiber OWNS its ArgumentParser (built inside the fiber, never shared) and
  OWNS its argv list (built from wid, never shared).  It computes, in closed
  form, EXACTLY what every namespace attribute must be (count==wid, ratio==the
  exact float it encoded via repr(), the choices pick, the appended tag list,
  the nargs='+' int list, the positional).  It then:
    - parse_args(argv) -> ns1, and asserts every attribute equals the closed-
      form expected value (a wrong value here is a torn parse, not a shared-
      object race: the parser and argv are single-owner);
    - YIELDs (yield_now / tiny sleep) so siblings interleave their own parses
      on other hubs, potentially through the same argparse helper code;
    - parse_args(argv) AGAIN on the SAME parser -> ns2, and asserts ns2 equals
      ns1 field-by-field AND still equals the closed-form expected (the parse
      is stable across the yield -- no cross-fiber leak mutated the parser or
      returned another fiber's resolved value).
  A plain-threads control (8 OS threads, each its own parser+argv, GIL on AND
  off) returns the closed-form namespace 100% of the time, 0 cross-parse leaks.
  Under a CORRECT runloom it must also hold; this oracle PASSES (exit 0) when
  there is no bug.

  argv is built with repr() for the float and str() for the ints, and every
  option value is drawn only from a value the converter round-trips EXACTLY
  (float(repr(x)) == x for all finite x; int(str(n)) == n), so the expected
  namespace is a closed form -- there is no float-formatting slop to excuse a
  mismatch.  Every argv this program feeds is VALID for its parser, so a correct
  parse NEVER calls parser.error()/SystemExit; a SystemExit or a value mismatch
  is therefore a real fault, not documented argparse behavior.

ORACLES:
  * LOAD-BEARING -- PARSE ROUND-TRIP (worker, HARD, fail-fast).  Single-owner
    parser + single-owner argv; closed-form namespace asserted before and after
    a yield; ns2==ns1 and both==expected.  A SystemExit from a valid argv, a
    wrong attribute value, or a value that changes across the yield is a FAIL.
  * NON-VACUITY (post, HARD): the round-trip arm actually ran (checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside
    _parse_known_args / a type converter never returns; the watchdog +
    require_no_lost catch it.

FAIL ON: a single-owner parser returning a namespace attribute that is not the
closed-form expected value, an attribute that changes across a yield, or a
SystemExit raised by parsing a VALID fiber-local argv (all real runtime faults).
There is no shared-parser arm: an ArgumentParser shared across fibers and mutated
concurrently would race exactly like any shared object (documented, not a runloom
bug), so this program keeps the parser strictly single-owner.

Stresses: argparse ArgumentParser construction (_actions / _option_string_actions
build), parse_args intermediate machinery (_parse_known_args, _get_values type
conversion, append action list build, nargs='+' consumption, choices check)
across hub migration + a yield, per-fiber parser + argv isolation under M:N.
"""
import argparse

import harness
import runloom

# The choices offered to the --color option.  The fiber picks one by wid % 3,
# so every parse exercises the choices-validation path with a valid pick.
COLORS = ("red", "green", "blue")

# How many --tag options the fiber appends (exercises the 'append' action's
# per-call list build).  A small fixed count keeps argv bounded.
NTAGS = 3

# How many ints the fiber feeds to the nargs='+' --nums option (exercises the
# variable-arity consume_positionals/consume_optional arg-gathering loop).
NNUMS = 4


def build_parser(prog):
    """Build a fresh, single-owner ArgumentParser.  Every option's metadata
    (type converter, choices, action, default) is fixed here at construction;
    parse_args() over it is then a pure function of (parser, argv).

    add_help=False so there is no -h/--help action (we never feed -h, and this
    keeps the parser off the help-formatting / gettext path entirely).  prog is
    passed explicitly so parser construction never READS the process-global
    sys.argv[0] (which would be a shared read, not a single-owner input)."""
    p = argparse.ArgumentParser(prog=prog, add_help=False,
                                allow_abbrev=False)
    p.add_argument("--count", type=int, required=True)
    p.add_argument("--ratio", type=float, required=True)
    p.add_argument("--name", type=str, required=True)
    p.add_argument("--color", choices=COLORS, required=True)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--tag", action="append", default=[])
    p.add_argument("--nums", type=int, nargs="+", required=True)
    p.add_argument("target")                 # single positional
    return p


def build_case(wid, idx):
    """Compute this fiber's fiber-local argv + the CLOSED-FORM expected namespace
    values, both derived only from (wid, idx) -- no shared state.

    Every encoded value round-trips its converter EXACTLY:
      * int(str(n)) == n for all n,
      * float(repr(f)) == f for all finite f (repr is the shortest round-tripping
        form in CPython), so ratio has no formatting slop.
    Returns (argv_list, expected_dict)."""
    count = wid                              # int, exact
    # A float that is NOT a round number, encoded via repr so float(repr(x))==x.
    ratio = (wid * 7 + idx) / 4.0 + 0.125
    name = "w{0}i{1}".format(wid, idx)
    color = COLORS[wid % len(COLORS)]
    verbose = bool((wid + idx) & 1)
    tags = ["t{0}_{1}".format(wid, j) for j in range(NTAGS)]
    nums = [wid * 100 + idx * 10 + j for j in range(NNUMS)]
    target = "tgt_{0}_{1}".format(wid, idx)

    # NOTE: the positional `target` goes FIRST.  argparse's nargs='+' --nums
    # greedily consumes every following argument-string, so a trailing positional
    # after --nums would be swallowed into nums (a documented argparse quirk, not
    # a runtime bug) -- placing the positional up front keeps every argv VALID.
    argv = [target,
            "--count", str(count),
            "--ratio", repr(ratio),
            "--name", name,
            "--color", color]
    if verbose:
        argv.append("--verbose")
    for t in tags:
        argv += ["--tag", t]
    argv.append("--nums")
    argv += [str(n) for n in nums]

    expected = {
        "count": count,
        "ratio": ratio,                      # exact: float(repr(ratio)) == ratio
        "name": name,
        "color": color,
        "verbose": verbose,
        "tag": list(tags),
        "nums": list(nums),
        "target": target,
    }
    return argv, expected


def compare_namespace(H, ns, expected, wid, when):
    """Assert every attribute of `ns` equals the closed-form `expected`.
    Returns True on match; calls H.fail + returns False on the first mismatch."""
    for attr, want in expected.items():
        got = getattr(ns, attr, "<<MISSING>>")
        if got != want:
            H.fail("argparse round-trip WRONG ({0}): attr {1!r} == {2!r}, "
                   "expected {3!r} (wid {4}) -- parse_args returned a value that "
                   "is not the closed-form result for this fiber's own argv; a "
                   "torn parse or cross-fiber leak of resolved namespace state"
                   .format(when, attr, got, want, wid))
            return False
    return True


def roundtrip_check(H, wid, idx):
    """Single-owner parse round-trip.  Fiber-local parser + argv; closed-form
    namespace asserted before and after a yield; second parse must equal the
    first field-by-field and still equal the closed form."""
    prog = "fiber_w{0}".format(wid)
    parser = build_parser(prog)              # single-owner
    argv, expected = build_case(wid, idx)    # single-owner, closed-form

    try:
        ns1 = parser.parse_args(argv)
    except SystemExit as e:
        H.fail("argparse SystemExit on a VALID single-owner argv (wid {0}, "
               "code {1!r}): {2!r} -- a correct parse of valid fiber-local input "
               "must never call parser.error()/exit; this is a torn parse under "
               "M:N".format(wid, getattr(e, "code", None), argv))
        return
    if not compare_namespace(H, ns1, expected, wid, "before yield"):
        return

    # Snapshot the resolved values so we can prove stability across the yield.
    snap = {k: getattr(ns1, k) for k in expected}

    # YIELD at the hazard boundary: siblings on other hubs run their own parses
    # through the same argparse helper code before this fiber re-parses.
    runloom.yield_now()
    if idx & 1:
        runloom.sleep(0.0003)

    try:
        ns2 = parser.parse_args(argv)        # SAME parser, SAME argv
    except SystemExit as e:
        H.fail("argparse SystemExit on RE-PARSE of a valid single-owner argv "
               "(wid {0}, code {1!r}) -- the parser was corrupted across a yield"
               .format(wid, getattr(e, "code", None)))
        return

    # Second parse must match the closed form AND the first parse exactly.
    if not compare_namespace(H, ns2, expected, wid, "after yield"):
        return
    for k in expected:
        if getattr(ns2, k) != snap[k]:
            H.fail("argparse round-trip UNSTABLE across yield: attr {0!r} was "
                   "{1!r} before the yield and {2!r} after (wid {3}) -- the same "
                   "parser+argv produced a different value; a sibling parse "
                   "leaked into this fiber's re-parse".format(
                       k, snap[k], getattr(ns2, k), wid))
            return

    return True


# Sustained round-trips per worker, bounded by H.running().  The leak hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously building
# parsers + parsing while sleep-PARKED across their yield, so a sibling reliably
# interleaves its own parse before this fiber re-parses.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            ok = roundtrip_check(H, wid, idx)
            if H.failed:
                return
            if ok:
                state["checks"][wid] += 1    # single-writer-per-slot, race-free
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # ONE slot per worker (wid-indexed, single writer) -> race-free conservation
    # tally for the non-vacuity check.  Allocated here where H.funcs is known.
    H.state = {
        "checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("argparse[single-owner LOAD-BEARING]: {0} parse round-trips (all "
          "closed-form namespace assertions passed fail-fast); ops={1}".format(
              checks, H.total_ops()))

    # NON-VACUITY: the load-bearing round-trip hazard was actually exercised.
    H.check(checks > 0,
            "no argparse parse round-trips ran -- the load-bearing purity "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside
    # _parse_known_args or a type converter).
    H.require_no_lost("argparse parse round-trip")


if __name__ == "__main__":
    harness.main(
        "p557_argparse_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="argparse.ArgumentParser.parse_args(argv) is a PURE function of "
                 "(parser, argv): it returns a fresh Namespace and never mutates "
                 "the parser.  LOAD-BEARING: each fiber owns its parser AND its "
                 "argv (both built from wid, never shared) and asserts the parsed "
                 "namespace equals the CLOSED-FORM expected values (int/str/float "
                 "all round-trip exactly) before and after a yield; a second "
                 "parse on the same parser+argv must equal the first field-by-"
                 "field.  A wrong attribute value, a value that changes across "
                 "the yield, or a SystemExit from a VALID argv is the runloom "
                 "parse-isolation bug")
