"""big_100 / 604 -- stat mode-decoding PURITY under M:N.

The `stat` module is a bag of PURE functions over an integer file-mode word:
S_IFMT(mode) / S_IMODE(mode) mask off the type- and permission-bit fields;
S_ISDIR/S_ISREG/S_ISLNK/... test the type field against a constant; filemode(mode)
renders the classic 10-char "-rwxr-xr-x" string by walking a bit table.  On Linux
these come from the C accelerator `_stat` (stat.py ends with `from _stat import *`),
so under free-threaded CPython 3.14t they run as C code with the GIL OFF -- exactly
the surface an M:N runtime can tear if a pure computation's intermediate state (a
returned int, the per-position char list inside filemode) leaks across a fiber
boundary or is observed mid-flight by a sibling.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each stat function is a
mathematical map: the SAME mode word must ALWAYS yield the SAME S_IFMT / S_IMODE /
predicate tuple / filemode string, and that output is fixed by a closed form
(mode & 0o170000, mode & 0o7777, a fixed bit table).  runloom parks a fiber at a
cooperative yield and may resume it on a DIFFERENT hub while siblings hammer the
same C functions with THEIR OWN mode words.  If the C stat path kept any hidden
mutable/global scratch (a shared buffer for the filemode char list, a cached last-
mode result, a module-level temporary) that were not fiber/thread isolated, a fiber
computing filemode(mode_A) that yields mid-way, or recomputes after a yield, could
observe a value derived from a sibling's mode_B -- a torn/cross-fiber result on a
function that is supposed to be pure.

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (not a shared-object false positive):
the ONLY input is a fiber-local immutable int `mode`, built in this fiber and never
shared.  stat's functions take that int and return fresh values; there is no shared
mutable container in the oracle.  So a mismatch cannot be "documented shared-object
racing" (there is no shared object) -- it can only be the runtime tearing a pure
computation.  We verified the closed-form laws below against the C `_stat` on plain
threads (GIL on and off): every mode word decodes identically every time, 0
mismatches.  Under a CORRECT runloom the oracle therefore PASSES (exit 0).

ORACLES:
  * LOAD-BEARING -- STAT PURITY (worker, HARD, fail-fast).  Each fiber draws a
    fiber-local `mode` (a random file-type nibble OR'd with random 12-bit
    permission/setuid/setgid/sticky bits, plus occasional junk type nibbles to hit
    the "unknown type" branch).  It:
      - computes the full decode bundle B = (S_IFMT, S_IMODE, predicate-tuple,
        filemode string) via stat;
      - asserts B matches an INDEPENDENT closed-form reference computed here
        (ref_fmt/ref_imode/ref_preds/ref_filemode -- a hand-written reimplementation
        of the documented bit rules, so agreement is a real cross-check, not a
        tautology);
      - asserts the self-consistency laws: S_IFMT|S_IMODE == mode & 0o177777, at
        most one of the seven real type predicates is true, S_ISDOOR/PORT/WHT are
        always False, len(filemode)==10, filemode[0] agrees with the type predicate;
      - YIELDS (yield_now / tiny sleep) so siblings interleave on this and other
        hubs;
      - recomputes the bundle A and asserts A == B bit-identical (purity across the
        yield) AND A still matches the closed form.
    Single-owner: `mode` is a fiber-local int, never shared; a mismatch is a runloom
    purity/tearing bug, not documented Python semantics.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a C stat
    call (parked-then-vanished) never returns; the watchdog + require_no_lost catch
    it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0), via a
    race-free per-wid slot table (one writer per slot).

FAIL ON: a stat decode that disagrees with the closed form, a decode that changes
across a yield, a broken self-consistency law, or a SIGSEGV inside a C stat call.
There is NO shared-mutable arm here -- the module is pure, so every observation is
load-bearing.

Stresses: _stat C accelerator functions (S_IFMT/S_IMODE/S_IS*/filemode) under GIL-
off M:N churn, purity of a pure computation across hub migration + yield, the
filemode bit-table walk (per-position char selection) racing siblings.

Good TSan / controlled-M:N-replay target: filemode() builds a small per-call char
list and joins it; if the C path had any shared scratch, a TSan report on it -- or a
replay that resumes filemode() mid-walk with a sibling's mode -- would localize the
tear before the string-equality oracle fires.
"""
import stat

import harness
import runloom

# The seven file-type format words that the real predicates recognize (Linux;
# S_IFDOOR/PORT/WHT are 0 fallbacks and their predicates are hardwired False).
REAL_TYPES = (
    stat.S_IFDIR, stat.S_IFCHR, stat.S_IFBLK, stat.S_IFREG,
    stat.S_IFIFO, stat.S_IFLNK, stat.S_IFSOCK,
)

# Type nibble -> filemode()'s leading char.  Mirrors _filemode_table[0].
TYPE_CHAR = {
    stat.S_IFLNK: "l",
    stat.S_IFSOCK: "s",
    stat.S_IFREG: "-",
    stat.S_IFBLK: "b",
    stat.S_IFDIR: "d",
    stat.S_IFCHR: "c",
    stat.S_IFIFO: "p",
}

# The predicate functions, in a fixed order.  The first seven are the real type
# tests; the last three (DOOR/PORT/WHT) are documented to always return False on
# platforms without those types (Linux), which the reference encodes.
PREDS = (
    stat.S_ISDIR, stat.S_ISCHR, stat.S_ISBLK, stat.S_ISREG,
    stat.S_ISFIFO, stat.S_ISLNK, stat.S_ISSOCK,
    stat.S_ISDOOR, stat.S_ISPORT, stat.S_ISWHT,
)
PRED_TYPE = (
    stat.S_IFDIR, stat.S_IFCHR, stat.S_IFBLK, stat.S_IFREG,
    stat.S_IFIFO, stat.S_IFLNK, stat.S_IFSOCK,
)

# Junk type nibbles (0o030000, 0o070000, 0o160000, 0o170000) that are NOT any of
# the seven real types -> exercise filemode()'s "unknown type" '?' branch and the
# all-predicates-False case.  Kept inside the 0o170000 mask so S_IFMT still maps
# them to themselves.
JUNK_TYPES = (0o030000, 0o050000, 0o070000, 0o110000, 0o160000, 0o170000)

INNER_CAP = 100000


def ref_filemode(mode):
    """Independent reimplementation of stat.filemode() from the documented bit
    rules (NOT a call into stat), so agreeing with stat.filemode is a genuine
    cross-check.  Returns the 10-char '-rwxrwxrwx'-style string."""
    fmt = mode & 0o170000
    chars = [TYPE_CHAR.get(fmt, "?")]
    # owner r / w
    chars.append("r" if (mode & 0o400) else "-")
    chars.append("w" if (mode & 0o200) else "-")
    # owner execute + setuid
    xu = mode & 0o100
    su = mode & 0o4000
    if xu and su:
        chars.append("s")
    elif su:
        chars.append("S")
    elif xu:
        chars.append("x")
    else:
        chars.append("-")
    # group r / w
    chars.append("r" if (mode & 0o040) else "-")
    chars.append("w" if (mode & 0o020) else "-")
    # group execute + setgid
    xg = mode & 0o010
    sg = mode & 0o2000
    if xg and sg:
        chars.append("s")
    elif sg:
        chars.append("S")
    elif xg:
        chars.append("x")
    else:
        chars.append("-")
    # other r / w
    chars.append("r" if (mode & 0o004) else "-")
    chars.append("w" if (mode & 0o002) else "-")
    # other execute + sticky
    xo = mode & 0o001
    vt = mode & 0o1000
    if xo and vt:
        chars.append("t")
    elif vt:
        chars.append("T")
    elif xo:
        chars.append("x")
    else:
        chars.append("-")
    return "".join(chars)


def ref_preds(mode):
    """Independent closed-form predicate tuple: real type tests plus the three
    always-False door/port/whiteout predicates."""
    fmt = mode & 0o170000
    out = []
    for t in PRED_TYPE:
        out.append(fmt == t)
    out.append(False)   # S_ISDOOR
    out.append(False)   # S_ISPORT
    out.append(False)   # S_ISWHT
    return tuple(out)


def decode(mode):
    """The full stat decode bundle for a mode word, via stat (the C _stat path on
    Linux).  Returns (fmt, imode, preds_tuple, filemode_string)."""
    fmt = stat.S_IFMT(mode)
    imode = stat.S_IMODE(mode)
    preds = tuple(p(mode) for p in PREDS)
    fm = stat.filemode(mode)
    return (fmt, imode, preds, fm)


def build_mode(rng):
    """Build one fiber-local mode word: a type nibble (usually a real type, some-
    times junk) OR'd with random 12-bit permission/special bits."""
    if rng.random() < 0.15:
        typ = rng.choice(JUNK_TYPES)
    else:
        typ = rng.choice(REAL_TYPES)
    perm = rng.randrange(0o10000)     # full 0o7777 span incl. setuid/setgid/sticky
    return typ | perm


def check_once(H, wid, mode, state):
    """Single-owner purity check on one fiber-local mode word.  Fail-fast."""
    exp_fmt = mode & 0o170000
    exp_imode = mode & 0o7777
    exp_preds = ref_preds(mode)
    exp_fm = ref_filemode(mode)

    # Baseline decode via stat (C path).
    b_fmt, b_imode, b_preds, b_fm = decode(mode)

    # ---- closed-form agreement (pre-yield) --------------------------------
    if b_fmt != exp_fmt:
        H.fail("S_IFMT({0:o}) == {1:o} but closed form is {2:o} (wid {3}) -- stat "
               "type-mask disagrees with mode & 0o170000".format(
                   mode, b_fmt, exp_fmt, wid))
        return
    if b_imode != exp_imode:
        H.fail("S_IMODE({0:o}) == {1:o} but closed form is {2:o} (wid {3}) -- stat "
               "perm-mask disagrees with mode & 0o7777".format(
                   mode, b_imode, exp_imode, wid))
        return
    if b_preds != exp_preds:
        H.fail("predicate tuple for mode {0:o} is {1} but closed form is {2} "
               "(wid {3}) -- a stat S_IS* predicate disagrees with the type "
               "field".format(mode, b_preds, exp_preds, wid))
        return
    if b_fm != exp_fm:
        H.fail("filemode({0:o}) == {1!r} but closed form is {2!r} (wid {3}) -- "
               "stat.filemode disagrees with the documented bit table".format(
                   mode, b_fm, exp_fm, wid))
        return

    # ---- self-consistency laws --------------------------------------------
    if (b_fmt | b_imode) != (mode & 0o177777):
        H.fail("law broken: S_IFMT|S_IMODE == {0:o} != mode&0o177777 == {1:o} "
               "for mode {2:o} (wid {3})".format(
                   b_fmt | b_imode, mode & 0o177777, mode, wid))
        return
    ntrue = sum(1 for x in b_preds[:7] if x)
    if ntrue > 1:
        H.fail("law broken: {0} real type predicates true for mode {1:o} (at most "
               "one may be) preds={2} (wid {3})".format(
                   ntrue, mode, b_preds, wid))
        return
    if b_preds[7] or b_preds[8] or b_preds[9]:
        H.fail("law broken: S_ISDOOR/PORT/WHT must be False on this platform, got "
               "{0} for mode {1:o} (wid {2})".format(b_preds[7:], mode, wid))
        return
    if len(b_fm) != 10:
        H.fail("law broken: filemode({0:o}) length {1} != 10 ({2!r}) (wid {3})"
               .format(mode, len(b_fm), b_fm, wid))
        return
    # filemode[0] must agree with the type: real type -> its char; junk -> '?'.
    fmt = mode & 0o170000
    exp_lead = TYPE_CHAR.get(fmt, "?")
    if b_fm[0] != exp_lead:
        H.fail("law broken: filemode leading char {0!r} != expected {1!r} for "
               "type {2:o} (mode {3:o}, wid {4})".format(
                   b_fm[0], exp_lead, fmt, mode, wid))
        return

    # ---- YIELD: let siblings hammer the same C functions on other hubs ------
    runloom.yield_now()
    if mode & 1:
        runloom.sleep(0.0002)

    # ---- purity across the yield: recompute must be bit-identical ----------
    a_fmt, a_imode, a_preds, a_fm = decode(mode)
    if a_fmt != b_fmt or a_imode != b_imode or a_preds != b_preds or a_fm != b_fm:
        H.fail("PURITY BROKEN: stat decode of mode {0:o} changed across a yield -- "
               "before=({1:o},{2:o},{3},{4!r}) after=({5:o},{6:o},{7},{8!r}) "
               "(wid {9}) -- a pure stat computation was torn or observed a "
               "sibling's mode".format(
                   mode, b_fmt, b_imode, b_preds, b_fm,
                   a_fmt, a_imode, a_preds, a_fm, wid))
        return
    # And it must STILL match the closed form after the yield.
    if a_fmt != exp_fmt or a_imode != exp_imode or a_preds != exp_preds \
            or a_fm != exp_fm:
        H.fail("POST-YIELD closed-form mismatch: stat decode of mode {0:o} no "
               "longer matches the closed form after a yield (wid {1})".format(
                   mode, wid))
        return

    state["checks"][wid] += 1        # single-writer-per-slot, race-free


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            mode = build_mode(rng)
            check_once(H, wid, mode, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Race-free non-vacuity tally: ONE slot per worker (wid-indexed, single writer).
    H.state = {"checks": [0] * H.funcs}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("stat purity checks (single-owner, all passed fail-fast): {0}; ops={1}"
          .format(checks, H.total_ops()))
    # NON-VACUITY: the load-bearing purity hazard was actually exercised.
    H.check(checks > 0,
            "no stat purity checks ran -- the pure-decode hazard was never "
            "exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished inside a C stat call.
    H.require_no_lost("stat mode purity")


if __name__ == "__main__":
    harness.main(
        "p604_stat_mode_purity", body, setup=setup, post=post,
        default_funcs=8000,
        describe="stat's mode-decoding functions (S_IFMT/S_IMODE/S_IS*/filemode, "
                 "backed by the C _stat accelerator) are PURE maps over an integer "
                 "mode word.  LOAD-BEARING: each fiber decodes a fiber-local mode, "
                 "cross-checks it against an independent closed-form reference and "
                 "the self-consistency laws, yields so siblings interleave on other "
                 "hubs, then recomputes and asserts the decode is bit-identical and "
                 "still matches the closed form.  A decode that disagrees with the "
                 "closed form, changes across a yield, or breaks a law is a runloom "
                 "purity/tearing bug (no shared mutable state exists here)")
