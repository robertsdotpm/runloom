"""big_100 / 561 -- cmd.Cmd line dispatch + parseline purity under M:N.

cmd.Cmd is a line-oriented interpreter framework.  Its two load-bearing pieces
for an M:N correctness probe are BOTH deterministic on a SINGLE-OWNER instance:

  * parseline(line) -- a PURE function of `line` + the class's identchars (and
    whether the instance has a do_shell attribute).  It strips the line, rewrites
    a leading '?' to 'help ...' and a leading '!' to 'shell ...', then peels the
    longest identchars prefix off as the command name, returning
    (cmd, arg, rewritten_line).  For a fixed instance and a fixed input it MUST
    return a bit-identical tuple every time.

  * onecmd(line) -- parses the line, sets self.lastcmd, and dispatches to the
    bound method do_<cmd>(arg) (or default(line) when no such method exists).
    For a fiber-local instance driven by a KNOWN script of command lines, the
    number of times each do_<cmd> fires, and the arguments it receives, are
    fully determined by the script -- a CLOSED-WORLD counting law.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each fiber owns its own
Cmd subclass instance (created in a fiber-local variable, never shared) with its
own fiber-local StringIO stdout and its own per-instance tally dict.  The fiber
drives a KNOWN multiset of command lines through onecmd across a yield.  Because
the instance is single-owner, a CORRECT runtime must conserve every dispatch
exactly: do_<cmd> fires exactly as many times as the script names it, receives
exactly the arguments the script encoded, and self.lastcmd equals the last line
fed.  If a dispatch is DROPPED or DOUBLED, if a do_<cmd> receives an argument
belonging to a SIBLING fiber's instance (a cross-fiber leak of single-owner
interpreter state), if getattr(self,'do_'+cmd) resolves to the wrong bound
method across a hub migration, or if parseline returns a value that changes
across a yield, that is a runloom bug -- not documented cmd/Python behavior.

WHICH ORACLE IS LOAD-BEARING, AND WHY.  Everything the oracle touches is owned by
exactly ONE fiber: the interpreter instance, its stdout, its tally dict, its
args-seen list, and the script.  A single-owner object driven by one fiber must
behave EXACTLY as it does under plain single-threaded use -- verified trivially
by construction (no other fiber has a reference).  So any deviation (a lost or
doubled dispatch, a torn tally, a mismatched lastcmd, a parseline result that
mutates across a yield) is a genuine runtime desync, and the program PASSES on a
correct runtime (exits 0 when there is no bug).  No shared mutable container is
ever placed under the fail-fast oracle, so this can never mislabel documented
shared-object semantics as a bug.

ORACLES:
  * LOAD-BEARING -- DISPATCH CONSERVATION (worker, HARD, fail-fast).  Build a
    fiber-local KNOWN script: a shuffled list of command lines, each either a
    recognized "do_" command with a unique per-fiber argument token, or an
    unrecognized command routed to default().  Precompute the exact expected
    per-command dispatch counts and the exact expected default() count.  Feed the
    script through instance.onecmd() with a yield inserted mid-script (so a
    sibling reliably interleaves while this instance is half-driven).  After the
    script, assert:
      - every do_<cmd> fired exactly its expected count (no drop/double);
      - every argument a do_<cmd> received belongs to THIS fiber (carries this
        fiber's unique token -- not a sibling's -> no cross-fiber leak);
      - default() fired exactly its expected count;
      - self.lastcmd == the last script line (stripped);
      - the total dispatches == len(script).

  * LOAD-BEARING -- PARSELINE PURITY (worker, HARD, fail-fast).  For a set of
    fiber-local input lines (including the '?'-> 'help' and '!'-> 'shell'
    rewrites and identchars-prefix peeling), compute parseline() BEFORE a yield,
    yield, then recompute AFTER the yield and assert the (cmd, arg, line) tuple is
    bit-identical -- a pure function's output must not change across a hub
    migration.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-onecmd
    (inside getattr dispatch or a do_ method) never returns; caught by the
    watchdog + require_no_lost.

  * NON-VACUITY (post, HARD): the load-bearing arms actually ran (dispatch
    conservation checks > 0).

Stresses: cmd.Cmd.onecmd dispatch via getattr(self,'do_'+cmd) under hub
migration, parseline string peeling + '?'/'!'/EOF rewriting purity across a
yield, per-instance lastcmd / tally isolation, bound-method resolution on a
single-owner instance under M:N churn.

Good TSan / controlled-M:N-replay target: onecmd resolves the dispatch target by
name via getattr on a per-instance bound method; a replay that migrates the fiber
mid-dispatch and returns a sibling instance's method, or a torn per-instance tally
increment, localizes the desync before the conservation sum closes.
"""
import io
import cmd

import harness
import runloom

# The recognized commands this fiber-local interpreter knows.  Each maps to a
# do_<name> method that records (into the single-owner instance) which command
# fired and with what argument.  A fixed, small, closed set so the conservation
# law is exact.
COMMANDS = ("alpha", "beta", "gamma", "delta", "epsilon")

# Unrecognized command names -- these route to default() (no do_ method).  Kept
# distinct from COMMANDS and from cmd's own builtins (help/?/!) so the default
# tally is unambiguous.
UNKNOWN_COMMANDS = ("zeta", "eta", "theta")

# Lines per script.  Big enough that a dropped/doubled dispatch moves a per-
# command count detectably; small enough that many rounds finish under the
# timeout.  The mid-script yield splits the drive so a sibling interleaves.
SCRIPT_LINES = 96

# parseline purity inputs per check -- a fixed menu exercising every parseline
# branch: identchars-prefix peeling, '?'-> help rewrite, '!'-> shell rewrite
# (the instance HAS do_shell), and trailing-arg stripping.
PARSE_INPUTS = (
    "alpha one two",
    "  beta   trailing   ",
    "?help topic",
    "!shellcmd arg",
    "gamma",
    "delta_with_underscores rest",
    "EOF",
    "123numeric prefix",
)


class FiberInterp(cmd.Cmd):
    """A single-owner cmd.Cmd subclass.  Every recognized command records the
    (name, arg) it was dispatched with into per-instance single-owner state so
    the driving fiber can assert the closed-world dispatch-conservation law.

    Each instance is created in a fiber-local variable and NEVER shared, so all
    of self.tally / self.args_seen / self.default_count / self.lastcmd have
    exactly one writer -- the owning fiber.  use_rawinput/completekey are left at
    defaults but never exercised (we call onecmd/parseline directly, never
    cmdloop), so no stdin/readline is ever touched."""

    def __init__(self, token, stdout):
        # token: this fiber's UNIQUE argument marker; any do_ that sees an arg
        # NOT carrying this token has been handed a sibling's line (cross-fiber
        # leak of single-owner interpreter state).
        super().__init__(stdin=None, stdout=stdout)
        self.token = token
        self.tally = {name: 0 for name in COMMANDS}
        self.args_seen = {name: [] for name in COMMANDS}
        self.default_count = 0
        self.leaked_args = 0            # do_ args not carrying this fiber's token

    def record(self, name, arg):
        self.tally[name] += 1
        self.args_seen[name].append(arg)
        # The script always hands recognized commands an argument carrying this
        # fiber's token; a foreign token means single-owner state leaked across
        # fibers.
        if self.token not in arg:
            self.leaked_args += 1

    def do_alpha(self, arg):
        self.record("alpha", arg)

    def do_beta(self, arg):
        self.record("beta", arg)

    def do_gamma(self, arg):
        self.record("gamma", arg)

    def do_delta(self, arg):
        self.record("delta", arg)

    def do_epsilon(self, arg):
        self.record("epsilon", arg)

    def do_shell(self, arg):
        # Present so parseline's '!' branch rewrites to 'shell ...'; the purity
        # arm exercises that rewrite.  Not counted in the conservation script.
        self.record_shell = arg

    def default(self, line):
        # Unrecognized command -> counted, but do NOT write to the real stdout
        # loop (we passed a fiber-local StringIO, so even the base write is
        # isolated).  Count it for the closed-world law.
        self.default_count += 1


def build_script(rng, token):
    """Build one fiber's KNOWN command script + its exact expected tallies.

    Returns (lines, expected_cmd_counts, expected_default_count, last_line).
    Every recognized line carries `token` in its argument so a cross-fiber leak
    is detectable.  Lines are shuffled so the recognized/unknown mix interleaves,
    but the expected counts are computed from the exact composition, not the
    order."""
    lines = []
    expected = {name: 0 for name in COMMANDS}
    expected_default = 0
    for i in range(SCRIPT_LINES):
        if rng.random() < 0.75:
            name = COMMANDS[rng.randrange(len(COMMANDS))]
            # Argument carries this fiber's token + a per-line marker.
            arg = "{0}_ln{1}".format(token, i)
            lines.append("{0} {1}".format(name, arg))
            expected[name] += 1
        else:
            name = UNKNOWN_COMMANDS[rng.randrange(len(UNKNOWN_COMMANDS))]
            arg = "{0}_ln{1}".format(token, i)
            lines.append("{0} {1}".format(name, arg))
            expected_default += 1
    # onecmd sets lastcmd to the (stripped) line for every recognized-syntax
    # line -- including unknown commands, since parseline still yields a non-None
    # cmd.  The last script line (already unstripped-but-simple) is the expected
    # lastcmd.
    last_line = lines[-1]
    return lines, expected, expected_default, last_line


def reference_parseline(line, has_do_shell):
    """A stand-alone reimplementation of cmd.Cmd.parseline for the identchars
    default set, used to cross-check the real parseline's output (a closed-form
    expectation, not just self-consistency).  Mirrors cmd.py exactly."""
    identchars = cmd.IDENTCHARS
    line = line.strip()
    if not line:
        return None, None, line
    elif line[0] == '?':
        line = 'help ' + line[1:]
    elif line[0] == '!':
        if has_do_shell:
            line = 'shell ' + line[1:]
        else:
            return None, None, line
    i, n = 0, len(line)
    while i < n and line[i] in identchars:
        i = i + 1
        _ = 0  # keep body multi-statement-free of surprises
    c, arg = line[:i], line[i:].strip()
    return c, arg, line


def dispatch_conservation(H, wid, idx, state, interp):
    """LOAD-BEARING: drive a KNOWN script through the single-owner interpreter and
    assert the closed-world dispatch-conservation law holds across a mid-script
    yield.  A dropped/doubled dispatch, a cross-fiber argument leak, or a wrong
    lastcmd is a runloom desync."""
    rng = state["rng_pool"][wid]
    token = interp.token
    lines, expected, expected_default, last_line = build_script(rng, token)

    # Reset per-round single-owner state (the instance is reused across rounds by
    # this ONE fiber, so resetting here keeps each round's law self-contained).
    for name in COMMANDS:
        interp.tally[name] = 0
        interp.args_seen[name] = []
    interp.default_count = 0
    interp.leaked_args = 0
    interp.lastcmd = ''

    split = len(lines) // 2
    for pos, line in enumerate(lines):
        interp.onecmd(line)
        if pos == split:
            # YIELD mid-script: a sibling fiber runs while this instance is only
            # half-driven.  If interpreter dispatch state is not fiber-isolated,
            # the second half could see a corrupted tally / wrong bound method.
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)

    if H.failed:
        return

    # --- closed-world dispatch conservation (single-owner, race-free) ---------
    # A do_ argument that did not carry this fiber's token is a cross-fiber leak.
    if interp.leaked_args:
        H.fail("cmd dispatch CROSS-FIBER LEAK: {0} do_ argument(s) on wid {1}'s "
               "single-owner interpreter did not carry this fiber's token {2!r} "
               "-- a sibling fiber's command line reached this instance's "
               "dispatch".format(interp.leaked_args, wid, token))
        return

    total = 0
    for name in COMMANDS:
        got = interp.tally[name]
        exp = expected[name]
        total += got
        if got != exp:
            H.fail("cmd dispatch conservation broken: do_{0} fired {1} time(s) "
                   "but the script named it {2} time(s) (wid {3}) -- a dispatch "
                   "was {4} on a single-owner cmd.Cmd instance".format(
                       name, got, exp, wid,
                       "DROPPED" if got < exp else "DOUBLED"))
            return

    if interp.default_count != expected_default:
        H.fail("cmd default() conservation broken: default() fired {0} time(s) "
               "but {1} unknown-command line(s) were fed (wid {2}) -- a "
               "dispatch was dropped or doubled".format(
                   interp.default_count, expected_default, wid))
        return

    if total + interp.default_count != len(lines):
        H.fail("cmd total dispatch mismatch: {0} do_ dispatches + {1} default() "
               "!= {2} script lines (wid {3}) -- a line was lost or doubled in "
               "onecmd".format(total, interp.default_count, len(lines), wid))
        return

    # onecmd sets lastcmd to the stripped rewritten line; our script lines have
    # no leading/trailing whitespace and no '?'/'!' rewrite, so the expected
    # lastcmd is the raw last line.
    if interp.lastcmd != last_line:
        H.fail("cmd lastcmd desync: interp.lastcmd == {0!r}, expected {1!r} "
               "(wid {2}) -- the single-owner interpreter's lastcmd was "
               "corrupted across the mid-script yield".format(
                   interp.lastcmd, last_line, wid))
        return

    state["dispatch_checks"][wid] += 1


def parseline_purity(H, wid, idx, state, interp):
    """LOAD-BEARING: parseline is a pure function of the line + identchars +
    do_shell presence.  Compute it before a yield, yield, recompute, and assert
    the (cmd, arg, line) tuple is bit-identical AND matches a stand-alone
    reference reimplementation (closed-form)."""
    has_shell = hasattr(interp, "do_shell")
    baseline = []
    for line in PARSE_INPUTS:
        baseline.append(interp.parseline(line))

    runloom.yield_now()

    for k, line in enumerate(PARSE_INPUTS):
        after = interp.parseline(line)
        before = baseline[k]
        if after != before:
            H.fail("cmd parseline NOT PURE: parseline({0!r}) returned {1!r} "
                   "before a yield and {2!r} after (wid {3}) -- a pure function's "
                   "output changed across a hub migration".format(
                       line, before, after, wid))
            return
        ref = reference_parseline(line, has_shell)
        if after != ref:
            H.fail("cmd parseline WRONG: parseline({0!r}) == {1!r}, reference "
                   "reimplementation says {2!r} (wid {3}) -- parseline produced "
                   "an incorrect parse".format(line, after, ref, wid))
            return

    state["parse_checks"][wid] += 1


INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber owns ONE FiberInterp instance (fiber-local StringIO stdout,
    fiber-local tally) and drives both load-bearing arms per iteration: the
    dispatch-conservation script and the parseline purity round-trip.  Nothing is
    shared, so both arms are safely fail-fast."""
    token = "W{0}".format(wid)
    stdout = io.StringIO()
    interp = FiberInterp(token, stdout)
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            dispatch_conservation(H, wid, idx, state, interp)
            if H.failed:
                return
            parseline_purity(H, wid, idx, state, interp)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-worker RNG (single-owner, derived deterministically) and per-worker
    # race-free tally slots (one writer per wid), allocated where H.funcs is
    # known.
    H.state = {
        "rng_pool": [H.derive("cmd", wid) for wid in range(H.funcs)],
        "dispatch_checks": [0] * H.funcs,   # race-free: one slot per worker
        "parse_checks": [0] * H.funcs,      # race-free: one slot per worker
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dchecks = sum(H.state["dispatch_checks"])
    pchecks = sum(H.state["parse_checks"])
    H.log("cmd[single-owner LOAD-BEARING]: {0} dispatch-conservation checks + "
          "{1} parseline-purity checks (all passed fail-fast); ops={2}".format(
              dchecks, pchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing arms actually ran.
    H.check(dchecks > 0,
            "no cmd dispatch-conservation checks ran -- the load-bearing "
            "single-owner interpreter hazard was never exercised (oracle would "
            "be vacuous)")
    H.check(pchecks > 0,
            "no cmd parseline-purity checks ran -- the parseline purity oracle "
            "was never exercised")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded mid-onecmd
    # inside getattr dispatch or a do_ method).
    H.require_no_lost("cmd dispatch conservation")


if __name__ == "__main__":
    harness.main(
        "p561_cmd_dispatch", body, setup=setup, post=post,
        default_funcs=8000,
        describe="cmd.Cmd line dispatch + parseline purity under M:N.  Each fiber "
                 "owns a single-owner cmd.Cmd subclass instance (fiber-local "
                 "StringIO stdout + per-instance tally) and drives a KNOWN script "
                 "of command lines through onecmd across a mid-script yield.  "
                 "LOAD-BEARING closed-world law: each do_<cmd> fires exactly as "
                 "the script names it, every dispatched argument carries this "
                 "fiber's token (no cross-fiber leak), default() fires its exact "
                 "count, and lastcmd == the last line.  A second arm asserts "
                 "parseline is bit-identical across a yield and matches a "
                 "reference reimplementation.  A dropped/doubled dispatch, a "
                 "cross-fiber interpreter-state leak, a wrong bound-method "
                 "resolution, or a parseline that mutates across a yield is the "
                 "runloom bug")
