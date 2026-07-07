"""big_100 / 558 -- bdb.Bdb per-instance canonic() purity + breakpoint-table
conservation under M:N.

bdb.Bdb is the debugger core.  Two pieces of its machinery are per-instance data
structures that make good single-owner oracles WITHOUT touching the debugger's
process-global tracing hooks (sys.monitoring / sys.settrace, which are NOT single-
owner and are deliberately never armed here -- we never call run()/set_trace()):

  * canonic(filename) -- a PURE per-instance function: for a real path it returns
    os.path.normcase(os.path.abspath(filename)); for an "<angle>"-bracketed pseudo
    name it returns the name unchanged.  Results are memoized in the per-instance
    dict self.fncache.  Nothing global is read or written.  A fiber's fncache is
    owned by exactly one fiber, so its value/identity must be bit-stable across a
    yield and must always equal the closed-form expected canonical path.

  * the breakpoint table -- set_break/get_break/get_file_breaks/get_all_breaks/
    clear_break maintain self.breaks (a per-instance dict filename -> [linenos])
    AND a PROCESS-GLOBAL registry (Breakpoint.bplist / Breakpoint.bpbynumber).  The
    global registry is shared mutable state: concurrent set_break()/Bdb() (whose
    __init__ runs _load_breaks(), iterating Breakpoint.bplist) race on it EXACTLY
    like any shared dict across OS threads -- DOCUMENTED Python behavior, not a
    runloom bug.  So every operation that reads or writes the global registry --
    Bdb() construction, set_break, clear_break -- is serialized under ONE
    cooperative Lock, turning the breakpoint arm into a CONSERVATION test (did every
    break I set read back exactly, and clear back to empty) rather than a test of
    bdb's (absent) thread-safety.  Because each arm-2 pass CLEARS all the breaks it
    set before releasing the lock, the global registry is EMPTY at every lock
    acquisition, so a freshly constructed Bdb's _load_breaks() inherits nothing and
    the per-instance self.breaks is a closed world equal to exactly what this fiber
    set.

WHERE M:N COULD BREAK IT.  runloom runs these fibers in parallel across hubs with
the GIL off.  If a fiber's per-instance fncache (arm 1) or self.breaks (arm 2) were
to leak into or from a sibling's instance -- a cross-fiber leak of single-owner
state, a torn dict entry, an identity/value change across a yield -- the closed-form
purity check or the set==read==clear conservation law would catch it.  Both arms are
single-owner (arm 1) / closed-world-serialized (arm 2); a FAIL means a real runtime
bug (cross-fiber leak, torn object, lost/doubled table entry), never documented
Python semantics.

ORACLES:
  * LOAD-BEARING A -- canonic() PURITY (worker, HARD, fail-fast, CPU-only, no
    global state).  Each fiber owns one Bdb.  For a fiber-local list of filenames
    (unique absolute paths embedding wid, plus a couple of "<angle>" names), it
    computes canonic() (populating the per-instance fncache), YIELDS so siblings
    interleave, then recomputes and asserts every result is BIT-IDENTICAL and equals
    the closed-form expected (normcase(abspath(name)) for a real path, name itself
    for an "<angle>" name).  A value/identity change across the yield, or a value
    that does not match the closed form, is a per-fiber fncache isolation bug.

  * LOAD-BEARING B -- breakpoint-table CONSERVATION (worker, HARD, fail-fast, under
    the cooperative Lock).  Each fiber owns a unique real source file (so its
    (file, line) registry keys never collide with a sibling's).  Under the lock it
    constructs a fresh Bdb, sets a KNOWN set of breakpoints on its file, and asserts
    get_file_breaks / get_break / get_all_breaks report EXACTLY that set (closed
    world: registry empty at lock time, so self.breaks == only my breaks), then
    clears every break and asserts the table is empty again and the global registry
    holds none of my keys.  A dropped/doubled/leaked table entry fails.

  * NON-VACUITY (post, HARD): both arms actually ran (canonic_checks > 0 and
    break_units > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside canonic /
    set_break / _load_breaks (parked-then-vanished) never returns; the watchdog +
    require_no_lost catch it.

Stresses: bdb.Bdb.canonic per-instance fncache memoization across hub migration +
yield, os.path.abspath/normcase purity, set_break/clear_break/get_*_break table
bookkeeping over self.breaks and the process-global Breakpoint registry under
serialized concurrent construction, per-fiber breakpoint-table isolation.

File-heavy (one small source file per fiber for linecache-backed set_break), so
max_funcs is capped -- the forever loop's --funcs 1000000 is bounded.
"""
import os

import bdb

import harness
import runloom

# Per-fiber canonic() filename set for arm 1.  A mix of unique absolute real-path
# strings (canonic -> normcase(abspath)) and "<angle>" pseudo names (canonic ->
# unchanged), so both branches of canonic() are exercised.  Absolute paths make the
# closed form independent of os.getcwd() (constant but avoided for clarity).
CANONIC_NAMES_PER_FIBER = 12

# Arm-2 source file line count and how many breakpoints to set per pass.  The file
# has BREAK_FILE_LINES real lines so linecache.getline (called inside set_break)
# returns a non-empty line for every chosen lineno (else set_break refuses the
# break with an error string).
BREAK_FILE_LINES = 40
BREAKS_PER_PASS = 10

# Inner iterations per round: sustained churn is what makes the M:N interleave
# reliably overlap a sibling mid-yield (a single pass barely races).  Bounded by
# H.running() so every fiber still returns.
INNER_CAP = 100000


def canonic_names(wid):
    """Fiber-local filename list for the canonic() purity arm.

    Unique per wid so no two fibers canonicalize the same string (keeps each
    fiber's fncache genuinely single-owner in what it caches).  Returns a list of
    (name, expected_canonic) pairs computed by the closed form."""
    pairs = []
    for i in range(CANONIC_NAMES_PER_FIBER):
        if i % 6 == 5:
            # "<angle>" pseudo name: canonic() returns it unchanged.
            name = "<bdb_pseudo_w{0}_{1}>".format(wid, i)
            expected = name
        else:
            # Absolute real-looking path: canonic() -> normcase(abspath(name)).
            name = "/big100/bdb/w{0}/mod{1}.py".format(wid, i)
            expected = os.path.normcase(os.path.abspath(name))
        pairs.append((name, expected))
    return pairs


def canonic_check(H, wid, dbg, pairs):
    """LOAD-BEARING A: per-instance canonic() purity across a yield.

    dbg is this fiber's OWN Bdb (single-owner).  Populate the per-instance fncache,
    yield so siblings run their own canonic()/set_break in parallel, then recompute
    and assert every result is bit-identical + equals the closed form."""
    # First pass: populate fncache, verify against the closed form.
    first = []
    for name, expected in pairs:
        got = dbg.canonic(name)
        if got != expected:
            H.fail("canonic() WRONG (pre-yield): dbg.canonic({0!r})=={1!r}, "
                   "expected {2!r} (wid {3}) -- canonic is a pure per-instance "
                   "function; a mismatch is a torn result or cross-fiber "
                   "fncache leak".format(name, got, expected, wid))
            return False
        first.append(got)

    # YIELD: let siblings interleave on their own instances.  A leak of another
    # fiber's fncache into this one would corrupt the recompute.
    runloom.yield_now()

    # Second pass: must be BIT-IDENTICAL to the first and still equal the closed
    # form (the fncache entry must survive the yield unchanged).
    for idx, (name, expected) in enumerate(pairs):
        got = dbg.canonic(name)
        if got != first[idx]:
            H.fail("canonic() CHANGED across a yield: dbg.canonic({0!r}) was "
                   "{1!r}, now {2!r} (wid {3}) -- the per-instance fncache entry "
                   "was replaced or a sibling's fncache leaked in".format(
                       name, first[idx], got, wid))
            return False
        if got != expected:
            H.fail("canonic() DRIFTED from closed form (post-yield): "
                   "dbg.canonic({0!r})=={1!r}, expected {2!r} (wid {3})".format(
                       name, got, expected, wid))
            return False
    return True


def break_check(H, wid, glock, srcfile, linenos):
    """LOAD-BEARING B: breakpoint-table conservation, fully under `glock`.

    All global-Breakpoint-registry access (Bdb() __init__ -> _load_breaks, set_break,
    clear_break) is serialized here.  Because every pass CLEARS what it set before
    releasing the lock, the global registry is EMPTY at lock time, so a fresh Bdb's
    self.breaks is a closed world equal to exactly this fiber's breaks.  Returns the
    number of breakpoint units conserved (0 on failure)."""
    with glock:
        dbg = bdb.Bdb()
        canon = dbg.canonic(srcfile)

        # Registry empty at lock time -> a fresh Bdb inherited nothing.
        if dbg.get_all_breaks():
            H.fail("breakpoint registry NOT empty at lock acquisition: a fresh "
                   "Bdb inherited {0!r} via _load_breaks (wid {1}) -- a prior "
                   "pass leaked breaks past its clear, or the global registry is "
                   "torn under concurrent construction".format(
                       dict(dbg.get_all_breaks()), wid))
            return 0

        # Set a KNOWN set of breaks on this fiber's unique file.
        for ln in linenos:
            err = dbg.set_break(srcfile, ln)
            if err is not None:
                H.fail("set_break({0!r}, {1}) refused: {2!r} (wid {3}) -- the "
                       "line exists in the fiber-local source file; a refusal is "
                       "a torn linecache/self.breaks under M:N".format(
                           srcfile, ln, err, wid))
                return 0

        want = set(linenos)

        # get_file_breaks / get_break / get_all_breaks must report EXACTLY `want`.
        got_file = set(dbg.get_file_breaks(srcfile))
        if got_file != want:
            H.fail("get_file_breaks conservation broken: got {0} want {1} "
                   "(wid {2}) -- a break was DROPPED or DOUBLED in the per-"
                   "instance table".format(sorted(got_file), sorted(want), wid))
            return 0
        for ln in linenos:
            if not dbg.get_break(srcfile, ln):
                H.fail("get_break({0!r}, {1}) is False after set_break "
                       "(wid {2}) -- a set break vanished from self.breaks".format(
                           srcfile, ln, wid))
                return 0
        # A line we did NOT set must not report a break.
        unset = BREAK_FILE_LINES  # last valid line, deliberately never set
        if unset not in want and dbg.get_break(srcfile, unset):
            H.fail("get_break({0!r}, {1}) is True but was never set (wid {2}) -- "
                   "a phantom/leaked break entry".format(srcfile, unset, wid))
            return 0
        allb = dbg.get_all_breaks()
        if set(allb.keys()) != {canon} or set(allb[canon]) != want:
            H.fail("get_all_breaks closed-world broken: {0!r} (wid {1}), expected "
                   "just {{{2!r}: {3}}} -- the table holds a foreign key or a "
                   "wrong line set (cross-fiber leak or torn registry)".format(
                       dict(allb), wid, canon, sorted(want)))
            return 0

        # Clear every break; the table must return to empty and the global
        # registry must hold none of my keys.
        for ln in linenos:
            err = dbg.clear_break(srcfile, ln)
            if err is not None:
                H.fail("clear_break({0!r}, {1}) failed: {2!r} (wid {3}) -- a set "
                       "break could not be cleared (torn table)".format(
                           srcfile, ln, err, wid))
                return 0
        if dbg.get_file_breaks(srcfile):
            H.fail("get_file_breaks non-empty after clearing all breaks: {0} "
                   "(wid {1}) -- a break survived its clear".format(
                       dbg.get_file_breaks(srcfile), wid))
            return 0
        if dbg.get_all_breaks():
            H.fail("get_all_breaks non-empty after clearing all breaks: {0!r} "
                   "(wid {1})".format(dict(dbg.get_all_breaks()), wid))
            return 0
        for ln in linenos:
            if (canon, ln) in bdb.Breakpoint.bplist:
                H.fail("global Breakpoint.bplist still holds ({0!r}, {1}) after "
                       "clear (wid {2}) -- the shared registry leaked a cleared "
                       "break".format(canon, ln, wid))
                return 0
    return len(linenos)


def worker(H, wid, rng, state):
    glock = state["glock"]
    srcdir = state["srcdir"]

    # One unique real source file per fiber (created once, reused across rounds).
    # Unique path -> unique (file, line) registry keys, so no sibling collision.
    srcfile = os.path.join(srcdir, "src_w{0}.py".format(wid))
    lines = ["x{0} = {0}\n".format(i) for i in range(1, BREAK_FILE_LINES + 1)]
    with open(srcfile, "w") as f:
        f.writelines(lines)

    pairs = canonic_names(wid)
    # This fiber's OWN Bdb for the canonic() arm.  Construction reads the global
    # registry (_load_breaks), so build it under the lock; the registry is empty at
    # lock time, so self.breaks starts empty and canonic() alone is used after.
    with glock:
        my_dbg = bdb.Bdb()

    # Deterministic per-fiber breakpoint lineno set (subset of 1..BREAK_FILE_LINES,
    # never the reserved last line used as the negative probe).
    pool = list(range(1, BREAK_FILE_LINES))
    rng.shuffle(pool)
    linenos = sorted(pool[:BREAKS_PER_PASS])

    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            if not canonic_check(H, wid, my_dbg, pairs):   # LOAD-BEARING A
                return
            units = break_check(H, wid, glock, srcfile, linenos)  # LOAD-BEARING B
            if H.failed:
                return
            state["canonic_checks"][wid] += 1       # single-writer-per-wid
            state["break_units"][wid] += units      # single-writer-per-wid
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    srcdir = H.make_tmpdir(prefix="p558_bdb_")
    H.state = {
        # Serializes every access to the PROCESS-GLOBAL Breakpoint registry
        # (Bdb() construction, set_break, clear_break) so arm 2 is a CONSERVATION
        # test, not a test of bdb's (absent) thread-safety.  Built in the root.
        "glock": runloom.sync.Lock(),
        "srcdir": srcdir,
        # Race-free per-wid counters (one writer per slot; see HARD RULE 1).
        "canonic_checks": [0] * H.funcs,
        "break_units": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    cchecks = sum(H.state["canonic_checks"])
    bunits = sum(H.state["break_units"])
    H.log("bdb[canonic purity LOAD-BEARING]: {0} per-instance fncache checks "
          "(all bit-identical + closed-form) | bdb[breakpoint conservation "
          "LOAD-BEARING]: {1} breaks set==read==cleared (serialized closed "
          "world); ops={2}".format(cchecks, bunits, H.total_ops()))

    # NON-VACUITY: both load-bearing arms actually exercised their hazard.
    H.check(cchecks > 0,
            "no canonic() purity checks ran -- the per-instance fncache hazard "
            "was never exercised (oracle would be vacuous)")
    H.check(bunits > 0,
            "no breakpoint-table conservation ran -- the set/get/clear registry "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside canonic / set_break /
    # _load_breaks / clear_break.
    H.require_no_lost("bdb canonic + breakpoint conservation")


if __name__ == "__main__":
    harness.main(
        "p558_bdb_canonic_breaks", body, setup=setup, post=post,
        default_funcs=1024,
        max_funcs=1024,
        describe="bdb.Bdb per-instance canonic() purity (single-owner fncache, "
                 "closed-form + bit-identical across a yield) and breakpoint-table "
                 "conservation (set==read==cleared on a fiber-local file, all "
                 "process-global Breakpoint-registry access serialized under a "
                 "cooperative lock so the arm tests CONSERVATION not bdb's absent "
                 "thread-safety).  A cross-fiber fncache/self.breaks leak, a torn "
                 "result, or a dropped/doubled/phantom break entry fails; the "
                 "global tracing hooks (sys.monitoring/settrace) are never armed")
