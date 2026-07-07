"""big_100 / 573 -- getopt.getopt / gnu_getopt PURITY across a yield under M:N.

getopt is a PURE command-line parser: getopt.getopt(args, shortopts, longopts)
and getopt.gnu_getopt(...) hold NO module-global mutable state -- every call is a
function of its arguments alone, returning a freshly built (opts, args) pair.  For
a fixed (argv, shortopts, longopts), the result is a CLOSED-FORM constant: the
exact list of (option, value) tuples in encounter order plus the trailing
positional args.  A GetoptError (unknown option / missing required arg) is
likewise a deterministic function of the same inputs.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs tens of
thousands of goroutines across hubs>1 with the GIL off, migrating a fiber's frame
between hubs across a cooperative yield.  If any part of the getopt call path
(argument-list slicing, the `do_shorts` / `do_longs` recursion, the
long-option prefix-match table, the returned list objects) were to leak state
across fibers, alias another fiber's argv/opts buffer, or be resumed on a torn
frame after a hub migration, a fiber could observe a parse result that (a) does
NOT match the closed-form expected for its OWN fiber-local inputs, or (b) CHANGES
between two identical calls straddling a yield.  Either is a corruption a correct
runtime must never produce.

SINGLE-OWNER, CLOSED-FORM oracle (load-bearing, fail-fast):
  Each fiber builds a FIBER-LOCAL argv from its own RNG together with the EXACT
  expected (opts, args) it must decode to -- the argv is CONSTRUCTED from the
  answer, so the expected value is known by construction, not by re-implementing
  getopt.  The fiber then:
    * parses argv once  -> (opts1, args1); asserts opts1 == expected_opts and
      args1 == expected_args (closed-form correctness);
    * YIELDS (yield_now / tiny sleep) so a sibling reliably interleaves and the
      frame may migrate hubs;
    * parses the SAME argv again -> (opts2, args2); asserts opts2 == opts1 and
      args2 == args1 (PURITY: bit-identical across the yield) and still == the
      closed-form expected.
  A dedicated ERROR sub-case builds an argv with an unknown short option and
  asserts getopt raises GetoptError with the SAME message before and after a
  yield (the error path is pure too).
  Everything -- argv, shortopts view, expected tuples -- is fiber-local and never
  shared, so a mismatch cannot be "documented shared-object races": it can only be
  a runloom frame/scheduling corruption.  On a correct runtime the oracle PASSES
  (program exits 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-parse
    (parked inside a getopt recursion after a hub migration and never re-woken)
    never returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (parses > 0).

FAIL ON: a getopt/gnu_getopt result that does not equal the closed-form expected
for a fiber's own inputs, a result that changes across a yield, a GetoptError that
appears/vanishes/changes message across a yield, or a SIGSEGV in the parser under
hub migration.  There is NO shared-mutable arm: getopt exposes no instance/global
state, so every observation here is single-owner and load-bearing.

Stresses: getopt.getopt / getopt.gnu_getopt short+long option decoding, the
long-option prefix-match table, the do_shorts/do_longs recursion and argv slicing,
GetoptError construction, all across cooperative yields + hub migration under M:N.
"""
import getopt

import harness
import runloom

# Short-option spec: 'a' and 'b' take an argument, 'c' is a flag.
SHORTOPTS = "a:b:c"
# Long-option spec: 'foo' and 'baz' take an argument, 'bar' is a flag.
LONGOPTS = ["foo=", "bar", "baz="]

# Value alphabet -- alphanumeric only, so a generated value can never be mistaken
# for an option (no leading '-') or a separator ('--'); this keeps the closed-form
# expected exact.
VALUE_ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

# Sustained parses per worker, bounded by H.running().  The frame-migration hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously parsing while
# yield-PARKED, so the scheduler reliably interleaves a sibling before this fiber
# resumes.  A single parse per fiber barely overlaps a sibling's and does NOT
# reproduce a migration corruption.
INNER_CAP = 100000


def randval(rng):
    """A fiber-local option VALUE: 1-8 alphanumeric chars (never option-like)."""
    n = rng.randint(1, 8)
    return "".join(rng.choice(VALUE_ALPHABET) for _ in range(n))


def build_case(rng):
    """Build a FIBER-LOCAL argv together with the EXACT (opts, args) it must decode
    to.  The argv is constructed FROM the answer, so the expected value is known by
    construction (we never re-implement getopt to derive it).

    Options are emitted first, then -- if there are positionals -- a '--' separator
    followed by the positional args.  With options-first + '--', getopt and
    gnu_getopt produce the IDENTICAL result (gnu has nothing to permute), so the
    same closed-form expected validates BOTH entry points."""
    argv = []
    expected_opts = []
    nopts = rng.randint(0, 12)
    for _ in range(nopts):
        choice = rng.randrange(5)
        if choice == 0:                      # -a val   (short w/ arg, separate)
            val = randval(rng)
            argv.append("-a")
            argv.append(val)
            expected_opts.append(("-a", val))
        elif choice == 1:                    # -bval    (short w/ arg, attached)
            val = randval(rng)
            argv.append("-b" + val)
            expected_opts.append(("-b", val))
        elif choice == 2:                    # -c       (short flag)
            argv.append("-c")
            expected_opts.append(("-c", ""))
        elif choice == 3:                    # --foo=v  (long w/ arg)
            val = randval(rng)
            argv.append("--foo=" + val)
            expected_opts.append(("--foo", val))
        else:                                # --bar    (long flag)
            argv.append("--bar")
            expected_opts.append(("--bar", ""))
    npos = rng.randint(0, 4)
    expected_args = [randval(rng) for _ in range(npos)]
    if expected_args:
        argv.append("--")                    # clean, unambiguous option terminator
        argv.extend(expected_args)
    return argv, expected_opts, expected_args


def check_valid(H, wid, rng, use_gnu, state):
    """LOAD-BEARING single-owner purity check on a VALID argv.  Parse, yield, parse
    again; the result must equal the closed-form expected both times and be
    bit-identical across the yield."""
    argv, expected_opts, expected_args = build_case(rng)
    parse = getopt.gnu_getopt if use_gnu else getopt.getopt
    fname = "gnu_getopt" if use_gnu else "getopt"

    opts1, args1 = parse(argv, SHORTOPTS, LONGOPTS)

    # Closed-form correctness BEFORE the yield.
    if opts1 != expected_opts:
        H.fail("{0}({1!r}) opts == {2!r}, expected {3!r} (wid {4}) -- the parser "
               "returned the wrong option list for this fiber's own inputs".format(
                   fname, argv, opts1, expected_opts, wid))
        return
    if args1 != expected_args:
        H.fail("{0}({1!r}) args == {2!r}, expected {3!r} (wid {4}) -- wrong "
               "positional-args list for this fiber's own inputs".format(
                   fname, argv, args1, expected_args, wid))
        return

    # YIELD: allow siblings to run and the frame to migrate hubs.
    runloom.yield_now()
    if wid & 1:
        runloom.sleep(0.0002)

    # PURITY: an identical call must return an identical result after the yield.
    opts2, args2 = parse(argv, SHORTOPTS, LONGOPTS)
    if opts2 != opts1:
        H.fail("{0}({1!r}) opts CHANGED across a yield: {2!r} -> {3!r} (wid {4}) "
               "-- getopt is pure; a differing result means a torn frame / cross-"
               "fiber state leak under hub migration".format(
                   fname, argv, opts1, opts2, wid))
        return
    if args2 != args1:
        H.fail("{0}({1!r}) args CHANGED across a yield: {2!r} -> {3!r} (wid {4}) "
               "-- pure parser returned a different positional list after a "
               "yield".format(fname, argv, args1, args2, wid))
        return
    # And still the closed-form expected (defends against BOTH calls being wrong).
    if opts2 != expected_opts or args2 != expected_args:
        H.fail("{0}({1!r}) post-yield result {2!r}/{3!r} != expected {4!r}/{5!r} "
               "(wid {6})".format(fname, argv, opts2, args2, expected_opts,
                                  expected_args, wid))
        return

    state["parses"][wid] += 1


def check_error(H, wid, rng, state):
    """LOAD-BEARING single-owner purity check on the ERROR path.  An unknown short
    option must raise GetoptError with the SAME message before and after a yield."""
    bad = "-" + rng.choice("zqZQ")           # z/q/Z/Q are not in SHORTOPTS/LONGOPTS
    argv = [bad, randval(rng)]

    try:
        getopt.getopt(argv, SHORTOPTS, LONGOPTS)
        H.fail("getopt({0!r}) did NOT raise GetoptError for an unknown option "
               "(wid {1}) -- the error path silently accepted an invalid "
               "argv".format(argv, wid))
        return
    except getopt.GetoptError as e:
        msg1 = str(e)
        opt1 = e.opt

    runloom.yield_now()

    try:
        getopt.getopt(argv, SHORTOPTS, LONGOPTS)
        H.fail("getopt({0!r}) raised the first time but NOT after a yield (wid "
               "{1}) -- the error path is not pure across hub migration".format(
                   argv, wid))
        return
    except getopt.GetoptError as e:
        if str(e) != msg1 or e.opt != opt1:
            H.fail("GetoptError CHANGED across a yield for {0!r}: {1!r}/{2!r} -> "
                   "{3!r}/{4!r} (wid {5}) -- a differing error means torn parser "
                   "state under migration".format(
                       argv, msg1, opt1, str(e), e.opt, wid))
            return

    state["errchecks"][wid] += 1


def worker(H, wid, rng, state):
    """Each fiber runs the single-owner load-bearing purity checks on fiber-local
    inputs: mostly VALID argv parses (alternating getopt / gnu_getopt), with an
    occasional ERROR-path check.  No shared state -- every observation is
    load-bearing."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if idx % 8 == 7:
                check_error(H, wid, rng, state)          # error path (pure)
            else:
                use_gnu = bool(idx & 1)
                check_valid(H, wid, rng, use_gnu, state)  # valid path (pure)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-wid slots (one writer per slot -> race-free) for the non-vacuity tallies.
    # Allocated here where H.funcs is known.
    H.state = {
        "parses": [0] * H.funcs,        # valid-argv purity checks
        "errchecks": [0] * H.funcs,     # error-path purity checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    parses = sum(H.state["parses"])
    errchecks = sum(H.state["errchecks"])
    H.log("getopt[single-owner LOAD-BEARING]: {0} valid-argv purity checks + {1} "
          "error-path purity checks (all passed fail-fast); ops={2}".format(
              parses, errchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(parses > 0,
            "no getopt purity checks ran -- the closed-form parse oracle was "
            "never exercised (would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a getopt
    # recursion after a hub migration).
    H.require_no_lost("getopt purity")


if __name__ == "__main__":
    harness.main(
        "p573_getopt_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="getopt.getopt / gnu_getopt are PURE parsers with no module-global "
                 "mutable state: for fixed (argv, shortopts, longopts) the (opts, "
                 "args) result is a closed-form constant.  LOAD-BEARING: each fiber "
                 "builds a fiber-local argv FROM the exact expected (opts, args) it "
                 "must decode to, parses it, yields (frame may migrate hubs), and "
                 "re-parses -- the result MUST equal the closed-form expected both "
                 "times and be bit-identical across the yield (an ERROR sub-case "
                 "asserts GetoptError is likewise stable).  A result that mismatches "
                 "the expected, changes across the yield, or an error that "
                 "appears/vanishes is a runloom frame/scheduling corruption")
