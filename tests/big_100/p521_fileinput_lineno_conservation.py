"""big_100 / 521 -- fileinput.FileInput per-instance line-cursor conservation under M:N.

fileinput.FileInput is a stateful, stream-like object.  A single instance carries
a live cursor across the sequence of files it was handed:

  * lineno()      -- the CUMULATIVE line number across all files opened so far
                     (1, 2, 3, ... monotonically, +1 per readline);
  * filelineno()  -- the line number WITHIN the current file (resets to 1 at every
                     file boundary as fileinput transparently advances to the next
                     file);
  * filename()    -- the path of the file the cursor is currently reading;
  * isfirstline() -- True exactly when filelineno()==1 (a fresh file boundary).

All of that state (the open file handle, the two counters, the current-file index)
lives on the INSTANCE.  In 3.14t with the GIL off and M:N scheduling, each fiber's
readline() blocks on a real filesystem read that is offloaded through the monkey
patch; the fiber PARKS there while siblings run on the same and other hubs.  If the
per-instance cursor were not truly fiber-private -- if a FileInput's _lineno /
_filelineno / current-file handle were parked in a location a sibling's FileInput
could clobber, or if the offloaded read resumed against another instance's file
handle -- then when THIS fiber's readline() resumes it could adopt a SIBLING's line
position: a jump in lineno() (a gap or a duplicate), a filelineno() that fails to
reset at the file boundary, or -- most damning -- a line whose CONTENT belongs to
another fiber's file entirely.

WE DELIBERATELY USE fileinput.FileInput INSTANCES, NOT the module-level
fileinput.input().  fileinput.input() drives a single process-global _state object
(the classic shared-mutable container); many fibers sharing it would race EXACTLY
like sharing it across OS threads -- documented Python behavior, NOT a runloom bug,
and off-limits for a fail-fast oracle (HARD RULE 2).  Each fiber here constructs its
OWN FileInput over its OWN temp files -> single-owner, closed-world.

CLOSED-WORLD CONSERVATION LAW (single-owner, fail-fast -- THE LOAD-BEARING ORACLE):

  Each fiber owns NFILES tiny temp files with FIXED, KNOWN contents.  File j has a
  fixed line count; every line encodes its own coordinates:

        "W{wid} F{j} L{k}\n"     (k = 1-based line index within file j)

  so the total line count N = sum(LINES_PER_FILE) is known exactly, and every line
  is SELF-IDENTIFYING.  The fiber builds a fileinput.FileInput over its own files
  and iterates it, PARKING (yield / tiny sleep) between reads so siblings reliably
  interleave.  At iteration step g (g = 1..N) it asserts:

    1. fi.lineno()     == g           -- cumulative counter is exactly monotone,
                                         no gap (a lost line) and no dup (a
                                         re-read / adopted sibling position);
    2. fi.filelineno() == k           -- the within-file counter reset to 1 at the
                                         file boundary and advances 1 per line
                                         (k is the expected within-file index);
    3. fi.isfirstline() iff k == 1    -- the file-boundary flag agrees with k;
    4. fi.filename()   == paths[j]    -- the cursor is on the RIGHT file;
    5. the line CONTENT decodes to (wid, j, k) EXACTLY -- the strongest check: a
       cross-fiber cursor adoption would surface a line carrying a DIFFERENT wid or
       a file/line index that does not match this fiber's expected position.

  After the loop it asserts the fiber read EXACTLY N lines (no early stop = a lost
  line under the park; no over-read = a doubled/adopted line).  All five are
  properties of a SINGLE-OWNER object touched by ONE fiber, so on a correct runtime
  the oracle PASSES (program exits 0); a failure is a real runloom cursor-isolation
  or lost/torn-read bug.

WHY THIS IS NOT A FALSE-POSITIVE GENERATOR (verified against plain threads):

  A standalone control -- 8 OS threads, each iterating its OWN FileInput over its
  own files, GIL on AND off -- reads every line in order with lineno() 1..N,
  filelineno() resetting per file, and content matching, 100% of the time with 0
  cross-thread adoptions.  The instance cursor IS thread-private; under a correct
  runloom it must also be fiber-private.  There is no shared-mutable container in the
  fail-fast arm (each FileInput + its files belong to one fiber), so nothing here can
  mislabel documented Python semantics as a bug.

ORACLES:
  * LOAD-BEARING -- PER-INSTANCE CURSOR CONSERVATION (worker, HARD, fail-fast):
    the five per-step checks + the exact lines-read == N check above.  Single-owner.
  * NON-VACUITY (post, HARD): lines were actually read (sum of the per-wid
    lines_read table > 0), so the cursor hazard was genuinely exercised.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished while parked
    inside an offloaded readline() (stranded mid-iteration) never returns; the
    watchdog + require_no_lost catch it.

FAIL ON: a lineno()/filelineno() gap, dup, or missing reset; a filename() or line
CONTENT that belongs to another fiber's file; or lines-read != N.  Every counter
feeding the non-vacuity law is a [0]*H.funcs slot indexed by wid (one writer per
slot, race-free -- HARD RULE 1).

Resource-bounded (HARD RULE 5): NFILES tiny files per fiber, so max_funcs caps the
forever loop's --funcs 1000000 at 2000 fibers (<= 6000 tiny files) under one
make_tmpdir that is rmtree'd at shutdown.

Stresses: fileinput.FileInput instance state (lineno/filelineno/current-file cursor)
across a parked, monkey-offloaded readline() under M:N; per-file boundary reset;
self-identifying-content conservation across hub migration + yield; single-owner
stream isolation vs the process-global fileinput.input() shared state we avoid.
"""
import os

import fileinput

import harness
import runloom

# Each fiber owns this many tiny files.  Fixed, small (HARD RULE 5): 2-3 files.
# Distinct, non-uniform line counts so the file-boundary reset of filelineno() is
# non-trivial and lineno() crosses several boundaries.
LINES_PER_FILE = (7, 5, 9)
NFILES = len(LINES_PER_FILE)
TOTAL_LINES = sum(LINES_PER_FILE)          # N -- the closed-world line count

# Precompute, for global step g (1..N), the (file-index j, within-file line k)
# the cursor SHOULD be at.  positions[g-1] == (j, k).  This is the single source of
# truth the load-bearing oracle checks fi.lineno()/filelineno()/content against.
POSITIONS = []
for _j, _cnt in enumerate(LINES_PER_FILE):
    for _k in range(1, _cnt + 1):
        POSITIONS.append((_j, _k))


def line_text(wid, j, k):
    """The FIXED, self-identifying content of file j's k-th line for this fiber."""
    return "W{0} F{1} L{2}\n".format(wid, j, k)


def write_fiber_files(base, wid):
    """Create this fiber's OWN NFILES tiny files with fixed, self-identifying
    contents.  Returns the ordered list of paths (the order the FileInput reads
    them).  Files live under a per-fiber subdir of the shared tmpdir (which is
    rmtree'd at shutdown), so no per-file cleanup registration is needed."""
    d = os.path.join(base, "w{0}".format(wid))
    os.makedirs(d, exist_ok=True)
    paths = []
    for j in range(NFILES):
        p = os.path.join(d, "f{0}.txt".format(j))
        with open(p, "w") as f:
            for k in range(1, LINES_PER_FILE[j] + 1):
                f.write(line_text(wid, j, k))
        paths.append(p)
    return paths


def iterate_and_check(H, wid, paths, state):
    """LOAD-BEARING single-owner cursor-conservation pass.

    Build a fresh fileinput.FileInput over THIS fiber's files, iterate it while
    parking between reads so siblings interleave, and assert the five per-step
    cursor invariants + the exact lines-read == N conservation law.  Every object
    touched (the FileInput and its files) is owned by this one fiber."""
    fi = fileinput.FileInput(files=paths)
    step = 0
    try:
        for line in fi:
            step += 1
            if step > TOTAL_LINES:
                H.fail("fileinput OVER-READ: read line #{0} but this fiber's files "
                       "hold only {1} lines (wid {2}) -- the cursor adopted a "
                       "sibling's position or re-read a line across the park".format(
                           step, TOTAL_LINES, wid))
                return
            j, k = POSITIONS[step - 1]

            # 1. cumulative lineno() must be exactly monotone (no gap, no dup).
            ln = fi.lineno()
            if ln != step:
                H.fail("fileinput lineno() GAP/DUP: at read #{0} lineno()=={1}, "
                       "expected {0} (wid {2}) -- the cumulative cursor jumped "
                       "across a parked readline(), adopting another position".format(
                           step, ln, wid))
                return

            # 2. within-file filelineno() must equal the expected within-file index.
            fln = fi.filelineno()
            if fln != k:
                H.fail("fileinput filelineno() WRONG: at cumulative line {0} "
                       "(file {1}, expected within-file line {2}) filelineno()=={3} "
                       "(wid {4}) -- the per-file counter failed to reset at the "
                       "boundary or adopted a sibling's file position".format(
                           step, j, k, fln, wid))
                return

            # 3. the file-boundary flag must agree with k==1.
            first = fi.isfirstline()
            if bool(first) != (k == 1):
                H.fail("fileinput isfirstline()=={0} but expected {1} at file {2} "
                       "within-line {3} (wid {4}) -- file-boundary state desynced "
                       "from the cursor".format(bool(first), k == 1, j, k, wid))
                return

            # 4. the cursor must be on the RIGHT file.
            fn = fi.filename()
            if fn != paths[j]:
                H.fail("fileinput filename()=={0!r} but the cursor should be on "
                       "file {1} == {2!r} at cumulative line {3} (wid {4}) -- the "
                       "current-file handle was swapped for a sibling's".format(
                           fn, j, paths[j], step, wid))
                return

            # 5. STRONGEST: the line content must decode to THIS fiber's (wid, j, k).
            if line != line_text(wid, j, k):
                H.fail("fileinput CONTENT MISMATCH: at cumulative line {0} read "
                       "{1!r}, expected {2!r} (wid {3}) -- a cross-fiber cursor "
                       "adoption returned a line from another fiber's file".format(
                           step, line, line_text(wid, j, k), wid))
                return

            # PARK at the hazard boundary so a sibling reliably interleaves between
            # our readline()s (single readline barely overlaps and does not repro).
            runloom.yield_now()
            if step & 3 == 0:
                runloom.sleep(0.0002)
    finally:
        fi.close()

    if H.failed:
        return

    # Conservation: EXACTLY N lines were read (no early stop = a lost line under the
    # park; over-read already failed fail-fast above).
    if step != TOTAL_LINES:
        H.fail("fileinput UNDER-READ: read {0} lines but this fiber's files hold "
               "{1} (wid {2}) -- a line was lost while parked in an offloaded "
               "readline()".format(step, TOTAL_LINES, wid))
        return

    state["lines_read"][wid] += step          # one writer per slot (race-free)


def worker(H, wid, rng, state):
    paths = write_fiber_files(state["base"], wid)    # this fiber's OWN files
    for _ in H.round_range():
        if not H.running():
            break
        iterate_and_check(H, wid, paths, state)      # LOAD-BEARING (fail-fast)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    base = H.make_tmpdir("big100_fileinput_")
    H.state = {
        "base": base,
        # ONE slot per worker (wid-indexed, single-writer, race-free -- HARD RULE 1).
        "lines_read": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total = sum(H.state["lines_read"])
    H.log("fileinput[single-owner LOAD-BEARING]: {0} lines read across per-fiber "
          "FileInput instances (every lineno()/filelineno()/isfirstline()/"
          "filename()/content check + lines-read=={1} conservation passed fail-"
          "fast); ops={2}".format(total, TOTAL_LINES, H.total_ops()))

    # NON-VACUITY: the cursor hazard was actually exercised (lines really got read).
    H.check(total > 0,
            "no fileinput lines were read -- the per-instance cursor-conservation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-iteration (stranded inside an
    # offloaded readline()).
    H.require_no_lost("fileinput lineno conservation")


if __name__ == "__main__":
    harness.main(
        "p521_fileinput_lineno_conservation", body, setup=setup, post=post,
        default_funcs=2000,
        max_funcs=2000,
        describe="each fiber iterates its OWN fileinput.FileInput over per-fiber "
                 "temp files with a KNOWN total line count N, parking between reads "
                 "so siblings interleave.  Closed-world conservation: lineno() runs "
                 "1..N monotonically, filelineno() resets to 1 at each file "
                 "boundary, filename()/isfirstline()/self-identifying content match "
                 "the expected position, and lines-read==N.  A gap/dup in lineno(), "
                 "a missing filelineno() reset, or a line from another fiber's file "
                 "is a per-instance cursor-isolation bug.  We use FileInput "
                 "instances (single-owner), NOT the process-global fileinput.input()")
