"""big_100 / 574 -- getpass._raw_input password round-trip PURITY under M:N.

getpass's core reader is getpass._raw_input(prompt, stream, input, echo_char):
it WRITES the prompt to `stream`, then READS one password line from `input`
(either input.readline() when echo_char is None, or a char-at-a-time loop in
_readline_with_echo_char that echoes echo_char per typed character), and RETURNS
the line with its trailing newline stripped.  This is a pure transformation of
its two file-object arguments -- it touches NO module-global state when both
`stream` and `input` are supplied (the sys.stdin/sys.stderr defaults are only
consulted when an argument is missing, which we never do).  getuser() likewise
reads only the process-global os.environ / pwd database, which no fiber mutates,
so it is a deterministic pure lookup here.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom runs each fiber
on its own Python frame stack across >1 hubs with the GIL off, inserting a
cooperative yield at the hazard boundary so a sibling reliably interleaves.  If
the runtime torn-copied a fiber's local StringIO handle, leaked another fiber's
`stream`/`input` object into this call, or lost/duplicated bytes while the
char-at-a-time echo loop was PARKED across a yield mid-read, this fiber's
returned password would differ from the fiber-local plaintext it fed in, or the
prompt/echo bytes written to its private out-stream would not match the exact
closed-form expected.  On a CORRECT runtime the transform is bit-identical every
time and equals the closed-form law, so the oracle PASSES (exit 0, no bug).

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (verified against plain threads):

  Every file object handed to _raw_input is a FRESH io.StringIO created inside
  the fiber and never shared: the input carries a fiber-local random password
  line, the out-stream starts empty.  _raw_input reads the input to EOF and
  writes only to the out-stream -- no cross-fiber container is touched.  We
  verified the closed-form laws with a standalone control (8 OS threads, GIL on
  AND off, each with its own StringIO pair):
    * echo_char=None:  return == pw           and out.getvalue() == prompt
    * echo_char='*':   return == pw            and out.getvalue() == prompt +
                                                   echo_char * len(pw)
  100% bit-identical across threads -- 0 cross-thread bleed.  Under a correct
  runloom it must also hold.  A returned password that is not the fiber-local
  plaintext, an out-stream that is not the exact prompt(+echo) string, or a
  change in either across the yield, is a runloom isolation / lost-byte bug.

ORACLES:
  * LOAD-BEARING -- RAW_INPUT ROUND-TRIP PURITY (worker, HARD, fail-fast).  Each
    fiber derives a fiber-local random password (printable ASCII, no newline /
    control chars so both reader paths are exercised cleanly) and a fiber-local
    prompt, then:
      - Runs _raw_input with FRESH fiber-local StringIO streams and asserts the
        returned line == pw and the out-stream == the closed-form expected
        (prompt, or prompt + echo_char*len(pw)).  This is r1.
      - Yields (yield_now / occasional sleep) so a sibling on another hub runs
        its own _raw_input concurrently.
      - Re-runs the SAME transform on a second set of fresh fiber-local streams
        and asserts the result is bit-identical to r1 AND still equals the
        closed-form law.  A drift means a torn handle or a leaked sibling stream.
    Single-owner: the StringIO pair is fiber-local, created per call, never
    shared; the password/prompt are fiber-local.  A failure is a runloom bug.

  * LOAD-BEARING -- getuser() STABILITY (worker, HARD, fail-fast).  getuser()
    reads process-global os.environ / pwd (no fiber mutates it), so it must
    return a string byte-identical to the baseline captured in setup, across a
    yield.  A change is a cross-fiber leak of the returned string object.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside
    input.readline() / the echo loop across a yield never returns; the watchdog
    + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

FAIL ON: a returned password != the fiber-local plaintext fed in, an out-stream
that != the exact closed-form prompt/echo string, either result changing across
a yield, or getuser() returning a different string than the baseline.  There is
NO shared-mutable arm here -- the transform is purely a function of fiber-local
file objects, so every observation is load-bearing.

Stresses: getpass._raw_input readline path + _readline_with_echo_char
char-at-a-time echo loop over fiber-local StringIO handles across hub migration
and a park/resume yield, getpass.getuser() deterministic env/pwd lookup, per-
fiber file-object isolation under M:N with the GIL off.

Good TSan / controlled-M:N-replay target: the echo loop does repeated
input.read(1) + stream.write(echo_char) on two distinct StringIO buffers; a
data-race report on either buffer, or a single dropped/duplicated byte under
deterministic replay while the loop is parked across the yield, localizes a
lost-byte bug before the round-trip equality even fires.
"""
import io

import getpass

import harness
import runloom

# Password character alphabet: printable ASCII, EXCLUDING newline and every byte
# the echo-char reader treats specially ('\n' '\r' '\x03' ETX, '\x7f'/'\b'
# backspace, '\x04' EOF, '\x00' NUL) so both reader paths return the plaintext
# verbatim and the closed-form out-stream law is exact.  All chars here are >= ' '
# and printable, so none collide with those control bytes.
ALPHABET = "".join(chr(c) for c in range(0x20, 0x7F))    # ' ' .. '~'
MAX_PW_LEN = 40                         # 0..MAX_PW_LEN inclusive per fiber
PROMPTS = ("Password: ", "", "pw> ", "Enter secret: ", u"päss: ")

# Sustained checks per worker, bounded by H.running().  The park/resume hazard
# only manifests under SUSTAINED churn -- many fibers simultaneously running the
# reader while sleep-PARKED across their yield, so the scheduler reliably
# interleaves a sibling before this fiber resumes.  A single check barely
# overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def make_password(rng):
    """A fiber-local random password: printable ASCII, no newline/control chars."""
    n = rng.randint(0, MAX_PW_LEN)
    return "".join(ALPHABET[rng.randrange(len(ALPHABET))] for _ in range(n))


def raw_input_law(prompt, pw, echo_char):
    """Run getpass._raw_input on FRESH fiber-local StringIO streams and return
    (result, out_text).  Every file object here is created in this call and never
    escapes -- single-owner.  Closed-form expected:
        result   == pw
        out_text == prompt                       (echo_char is None)
        out_text == prompt + echo_char*len(pw)   (echo_char given)
    """
    in_stream = io.StringIO(pw + "\n")
    out_stream = io.StringIO()
    result = getpass._raw_input(prompt, out_stream, input=in_stream,
                                echo_char=echo_char)
    return result, out_stream.getvalue()


def raw_input_check(H, wid, idx, state):
    """Single-owner round-trip purity check for one fiber-local password."""
    rng = state["rngs"][wid]
    pw = make_password(rng)
    prompt = PROMPTS[idx % len(PROMPTS)]
    # Alternate the two reader paths so both readline() and the echo loop race.
    echo_char = "*" if (idx & 1) else None
    expected_out = prompt + (echo_char * len(pw) if echo_char else "")

    # Compute the law BEFORE the yield.
    r1, out1 = raw_input_law(prompt, pw, echo_char)

    if r1 != pw:
        H.fail("getpass._raw_input returned WRONG password: got {0!r} but fed "
               "fiber-local plaintext {1!r} (wid {2}, echo={3!r}) -- a lost/"
               "duplicated byte or a leaked sibling input stream".format(
                   r1, pw, wid, echo_char))
        return
    if out1 != expected_out:
        H.fail("getpass._raw_input wrote WRONG prompt/echo: out={0!r} expected "
               "{1!r} (wid {2}, echo={3!r}) -- a leaked sibling out-stream or a "
               "torn write".format(out1, expected_out, wid, echo_char))
        return

    # YIELD: let a sibling on another hub run its own _raw_input concurrently.
    runloom.yield_now()
    if idx & 2:
        runloom.sleep(0.0003)

    # Re-run the SAME transform on fresh fiber-local streams; must be bit-
    # identical to r1 AND still equal the closed-form law.
    r2, out2 = raw_input_law(prompt, pw, echo_char)

    if r2 != r1:
        H.fail("getpass._raw_input NOT STABLE across a yield: got {0!r} before, "
               "{1!r} after (fed {2!r}, wid {3}, echo={4!r}) -- a cross-fiber "
               "leak corrupted this fiber's read".format(r1, r2, pw, wid,
                                                         echo_char))
        return
    if out2 != out1:
        H.fail("getpass._raw_input out-stream NOT STABLE across a yield: {0!r} "
               "before, {1!r} after (wid {2}, echo={3!r}) -- a leaked sibling "
               "stream".format(out1, out2, wid, echo_char))
        return
    if r2 != pw or out2 != expected_out:
        H.fail("getpass._raw_input drifted off the closed-form law after the "
               "yield: got ({0!r},{1!r}) expected ({2!r},{3!r}) (wid {4}) -- a "
               "runloom lost-byte/isolation bug".format(r2, out2, pw,
                                                        expected_out, wid))
        return

    # getuser() reads only process-global env/pwd which no fiber mutates -> it
    # must equal the baseline captured in setup, byte-identical, across a yield.
    user = getpass.getuser()
    if user != state["user"]:
        H.fail("getpass.getuser() returned {0!r} but the baseline is {1!r} "
               "(wid {2}) -- a cross-fiber leak of the returned username string "
               "or a mutated os.environ under M:N".format(user, state["user"],
                                                          wid))
        return

    state["checks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Each fiber runs the single-owner round-trip check on fiber-local passwords
    in a sustained inner loop so many fibers are simultaneously parked mid-read
    across their yield, maximising the interleave the hazard needs."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            raw_input_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Per-fiber RNG (single-writer, one per wid) so each fiber's password stream
    # is deterministic + replayable and never shared.  getuser() baseline is
    # captured once in the root before any fiber runs.
    H.state = {
        "rngs": [H.derive("getpass", wid) for wid in range(H.funcs)],
        "user": getpass.getuser(),
        "checks": [0] * 1024,           # NON-VACUITY tally (sharded; report only)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("getpass._raw_input single-owner round-trip checks (all passed fail-"
          "fast): {0}; getuser baseline {1!r}; ops={2}".format(
              checks, H.state["user"], H.total_ops()))
    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(checks > 0,
            "no getpass._raw_input round-trip checks ran -- the load-bearing "
            "purity hazard was never exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished inside a read / echo loop.
    H.require_no_lost("getpass raw_input round-trip")


if __name__ == "__main__":
    harness.main(
        "p574_getpass_rawinput_roundtrip", body, setup=setup, post=post,
        default_funcs=8000,
        describe="getpass._raw_input(prompt, stream, input, echo_char) is a pure "
                 "transform of two fiber-local StringIO handles: it writes the "
                 "prompt (+ echo_char per typed char) to the out-stream and "
                 "returns the input line minus its trailing newline.  LOAD-"
                 "BEARING: each fiber feeds a fiber-local random password through "
                 "FRESH per-call StringIO streams and asserts return==pw and "
                 "out==prompt(+echo*len) both before and (bit-identically) after "
                 "a yield, plus getpass.getuser() stable vs a baseline.  A "
                 "returned password != the plaintext, a wrong prompt/echo write, "
                 "or drift across the yield is a runloom lost-byte/isolation bug")
