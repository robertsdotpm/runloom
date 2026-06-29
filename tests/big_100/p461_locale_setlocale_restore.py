"""big_100 / 461 -- locale.setlocale() PROCESS-GLOBAL save/restore integrity
under M:N.

locale.setlocale(category, value) mutates PROCESS-GLOBAL C-library state: the
active LC_NUMERIC drives how the C library formats numbers (the decimal point
and the thousands separator that locale.format_string / locale.localeconv read).
There is no per-thread, let alone per-goroutine, locale on a normal CPython
build -- the active locale is one global owned by the C runtime.  Like p321
(warnings.filters) this is a bare save/restore hazard over a PROCESS-GLOBAL with
NO per-goroutine identity: a fiber that does
    saved = setlocale(cat)        # read the baseline
    setlocale(cat, "en_US.UTF-8") # install a locale
    ...format a number...
    setlocale(cat, saved)         # restore the baseline
assumes strict-LIFO nesting; if two such blocks ever OVERLAP the global does not
return to baseline.  It is NOT contextvar-backed (the BUG#7 contextvar isolation
fix does NOT cover it), and NOT threading.local-backed -- it is one C-library
global.  This is adjacent-but-distinct from p66/p67 (those guard
goroutine/hub-local containers) and a sibling of p321 (a different process-global,
the same save/restore-LIFO shape).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  setlocale's save/restore assumes STRICT-LIFO nesting and is documented in
  CPython as NOT thread-safe (the C setlocale itself is not).  If two blocks
  OVERLAP -- one installs a locale between another's save and restore -- the
  global LC_NUMERIC does NOT return to the baseline (a 'C' baseline gets stranded
  at 'en_US.UTF-8', so a later format groups with commas when it should not).  We
  verified that overlap corruption reproduces under PLAIN OS THREADS *with the
  GIL fully ON* (PYTHON_GIL=1) on this very interpreter -- i.e. OVERLAPPING /
  unserialized setlocale is documented-unsafe usage for ANY concurrency model and
  ANY GIL setting, NOT a runloom-specific bug.  An oracle that hard-failed on that
  would be a FALSE-POSITIVE detector (it fires identically without runloom).  So
  the overlap drift is MEASURED and REPORTED, never failed -- like p67's TLS leak
  rate and p321's overlap drift.

  What IS a genuine runloom M:N invariant -- and the LOAD-BEARING oracle here --
  is the SERIALIZED STRICT-LIFO arm: every setlocale block runs behind ONE shared
  cooperative Lock, so the blocks are globally strict-LIFO (never two open at once
  -- the documented-SAFE usage).  Workers still PARK / yield / migrate hubs
  OUTSIDE the lock between blocks, so a goroutine routinely opens its block on one
  hub and could be preempted / migrated during the set or the restore.  Under
  run(1)/GIL this serialized usage ALWAYS restores the global to baseline
  (verified); under M:N it MUST too.  If runloom's save/restore desyncs across a
  hub migration or a preempt-mid-restore -- a runloom regression, NOT a documented
  caveat -- the baseline does not restore.  THAT is the bug this program uniquely
  catches, and the serialized arm PASSES on a correct runtime (so the program
  exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- GLOBAL LC_NUMERIC baseline-restore integrity across the
    SERIALIZED strict-LIFO M:N arm (post, HARD):
        baseline = setlocale(LC_NUMERIC) captured at setup; every serialized
        worker holds the shared Lock around its whole save/set/format/restore
        block (strict global LIFO) but parks/migrates between blocks; post()
        H.check(setlocale(LC_NUMERIC) == baseline).
    A LC_NUMERIC stuck off the baseline after the serialized arm quiesces is a
    runloom save/restore desync (migration / preempt-mid-restore) -- it does NOT
    reproduce under stock serialized LIFO use, so it is a true runloom signal.
  * LOAD-BEARING -- per-block format agreement (in-block, HARD): inside the held
    lock, after setting the install locale, the number we format MUST match what
    that exact locale produces (grouping-or-not).  If a sibling on the same hub
    raced the global setlocale into our critical section -- mid-block -- our
    format would silently come out under the WRONG locale even though we hold the
    lock.  Under correct serialized M:N that can never happen; a mismatch is a
    runloom desync.  (Self-checked against locale.format_string for the install
    locale captured single-threaded at setup, so the expectation is itself
    race-free.)
  * COMPLETENESS (post, HARD): require_no_lost -- a worker stranded holding the
    shared Lock when it vanished, or parked-then-vanished, never returns; the
    watchdog catches an outright strand and require_no_lost catches a
    parked-then-vanished worker.

  * MEASURED (report-ONLY, NEVER fails): the OVERLAP arm's global drift.  A
    SEPARATE, fully-drained pre-phase runs paired workers that PROVABLY overlap
    two open setlocale blocks (a rendezvous park inside the block, NO shared
    lock) -- the documented-unsafe non-LIFO case.  We measure whether the overlap
    left the global LC_NUMERIC off its pre-block snapshot (it does -- reproduces
    under plain GIL threads), reported like p67's leak rate, never asserted.  Each
    overlap block EXPLICITLY restores its own pre-block snapshot, and after the
    drain the global is hard-reset to the baseline, so the documented-unsafe drift
    never reaches the load-bearing serialized-arm check.

Keep contenders MODEST: this is a correctness probe of the serialized global
save/restore, not a scale soak.

Stresses: locale.setlocale() PROCESS-GLOBAL LC_NUMERIC save/restore across hub
migration + preempt-mid-restore under serialized strict-LIFO usage, C-library
global (non-contextvar / non-threadlocal) isolation, locale.format_string
correctness under the set locale, no-lost-wake while holding a shared cooperative
lock.

Guards for locale availability: needs at least one locale BEYOND 'C' whose
LC_NUMERIC differs observably; tries the common 'en_US.UTF-8' / 'C.UTF-8' set and
SKIPs cleanly (exit 0) if none beyond 'C' is settable.
"""
import locale
import socket
import sys

import harness
import runloom

# Modest population.  Most workers run the LOAD-BEARING serialized strict-LIFO
# arm; a SEPARATE small paired population runs the report-only OVERLAP pre-phase.
MAX_WORKERS = 4000
# Fraction of nworkers used to size the (separate, drained) overlap pre-phase.
OVERLAP_FRACTION = 0.2

# The category we drive: LC_NUMERIC governs number formatting (decimal point +
# thousands grouping), so a baseline-vs-install difference is directly observable
# via locale.format_string -- a self-checking in-block oracle.
CAT = locale.LC_NUMERIC

# A number whose formatting DIFFERS between a grouping locale (en_US: "1,234,567")
# and a non-grouping one (C: "1234567"), so the in-block format-agreement oracle
# has a real signal and the overlap-drift measure can see a stranded locale.
SAMPLE = 1234567.5


def _try_set(value):
    """Try to make `value` the active LC_NUMERIC; restore whatever was active and
    return the canonical name the C library reports for it (or None if unsettable).
    Run single-threaded at setup BEFORE any worker, so it is race-free."""
    prev = locale.setlocale(CAT)
    try:
        name = locale.setlocale(CAT, value)
    except (locale.Error, ValueError):
        return None
    finally:
        locale.setlocale(CAT, prev)
    return name


def _fmt_under(value):
    """Format SAMPLE under locale `value` (single-threaded), restore, and return
    the resulting string -- the race-free expectation for the in-block oracle."""
    prev = locale.setlocale(CAT)
    try:
        locale.setlocale(CAT, value)
        return locale.format_string("%.1f", SAMPLE, grouping=True)
    finally:
        locale.setlocale(CAT, prev)


# --------------------------------------------------------------------------
# LOAD-BEARING arm: SERIALIZED strict-LIFO.  Every setlocale block runs under ONE
# shared cooperative Lock, so the blocks are globally strict-LIFO (never two open
# at once = the documented-SAFE usage).  Workers PARK / yield / migrate hubs
# OUTSIDE the lock between blocks.  The global LC_NUMERIC MUST restore to baseline
# -- the run(1)/GIL behaviour a runloom save/restore desync across a hub migration
# / preempt-mid-restore would break.
# --------------------------------------------------------------------------
def serialized_block(H, wid, r, rng, state):
    lock = state["lock"]
    install = state["install"]
    expect = state["install_fmt"]
    # Park / migrate hub OUTSIDE the critical section so the goroutine can be on a
    # different hub each time it takes the lock (exercises migration around the
    # save/restore), without ever overlapping another block.
    runloom.sleep(0.0003)
    runloom.yield_now()
    with lock:
        # Save the live global (should be the baseline), install our locale,
        # format, then restore EXACTLY what we saved -- strict-LIFO, one block at a
        # time.  yield/migrate INSIDE the held region too, so a correct runtime
        # must keep the global ours across the scheduling point (a sibling cannot
        # be in its block -- the lock guarantees it).
        saved = locale.setlocale(CAT)
        locale.setlocale(CAT, install)
        runloom.yield_now()                 # preempt/migrate mid-block (still safe)
        got = locale.format_string("%.1f", SAMPLE, grouping=True)
        # LOAD-BEARING in-block oracle: under the lock our format MUST be the
        # install locale's formatting.  A mismatch means the global setlocale was
        # raced into our critical section across the scheduling point (a runloom
        # desync) -- impossible under correct serialized M:N.
        if got != expect:
            H.fail("IN-BLOCK LOCALE DESYNC: under the shared lock with "
                   "LC_NUMERIC set to {0!r}, format_string produced {1!r} but the "
                   "install locale formats {2!r} as {3!r} -- a sibling raced the "
                   "PROCESS-GLOBAL setlocale into this serialized (lock-held) "
                   "critical section across a scheduling point (a runloom "
                   "save/restore desync; locale is a C-library global, NOT "
                   "contextvar/threadlocal-isolated)".format(
                       install, got, SAMPLE, expect))
            return
        locale.setlocale(CAT, saved)        # restore baseline (strict LIFO)
    state["ser_blocks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# REPORT-ONLY OVERLAP arm: paired workers PROVABLY overlap two open setlocale
# blocks (the documented-unsafe non-LIFO case, NO shared lock).  Measured, never
# failed.  Each block explicitly restores its own pre-block global snapshot, and
# the pre-phase is fully drained before the load-bearing pool runs, so it cannot
# poison the load-bearing serialized arm.
# --------------------------------------------------------------------------
def overlap_block(H, wid, r, rng, state):
    pair = wid // 2
    slots = state["pairs"]
    if pair >= len(slots):
        return
    a_sock, b_sock = slots[pair]
    me = a_sock if (wid & 1) == 0 else b_sock
    install = state["install"]
    # Snapshot the global ourselves so we can EXPLICITLY restore it after the block
    # (the overlap drift must never reach the load-bearing check).
    saved = locale.setlocale(CAT)
    locale.setlocale(CAT, install)
    # Rendezvous so the SIBLING's setlocale block is provably open at the same time
    # as ours (non-LIFO overlap).  Real socketpair recv = netpoll park, so the
    # overlap holds across hubs.
    try:
        me.send(b"x")
        me.settimeout(state["rdv_timeout"])
        try:
            me.recv(1)                      # park until the peer's block is open too
        except (socket.timeout, OSError):
            pass                            # peer absent/closed -> proceed (no strand)
    except OSError:
        pass
    # MEASURED: did the overlap leave the global off our pre-block snapshot?
    # (documented-unsafe for any concurrency model / GIL setting -- report only.)
    if locale.setlocale(CAT) != saved:
        state["overlap_drift"][wid & 1023] += 1
    # Explicitly restore OUR pre-block snapshot so the documented-unsafe overlap
    # drift cannot reach the load-bearing serialized-arm check.
    try:
        locale.setlocale(CAT, saved)
    except (locale.Error, ValueError):
        pass
    state["overlap_blocks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """The LOAD-BEARING serialized-arm worker.  Owns its own random.Random (a
    shared one corrupts the Mersenne state GIL-off).  Runs ONLY serialized blocks
    -- the report-only OVERLAP arm runs in a SEPARATE, fully-drained pre-phase so
    its documented-unsafe drift can never contaminate the shared global while the
    load-bearing pool measures it."""
    for r in H.round_range():
        if not H.running():
            break
        serialized_block(H, wid, r, rng, state)
        if H.failed:
            return
        H.op(wid)
    H.task_done(wid)


def run_overlap_phase(H, state):
    """Report-ONLY pre-phase: spawn the paired OVERLAP workers, let them PROVABLY
    overlap two open setlocale blocks (documented-unsafe non-LIFO), and FULLY DRAIN
    them (WaitGroup.wait) before returning.  This runs BEFORE the load-bearing
    serialized pool, so the two arms never touch the PROCESS-GLOBAL locale
    concurrently -- the overlap drift is measured in isolation and cannot poison
    the serialized-arm restore-integrity oracle.  Each overlap block also
    self-restores its own pre-block snapshot; after the drain we hard-reset the
    global to the captured baseline so the serialized pool starts pristine."""
    noverlap = state["noverlap"]
    if noverlap <= 0:
        return
    wg = runloom.WaitGroup()
    wg.add(noverlap)

    def run_one(wid):
        rng = H.derive("overlap", wid)
        try:
            for r in range(max(1, H.rounds)):
                if not H.running():
                    break
                overlap_block(H, wid, r, rng, state)
        finally:
            wg.done()

    for wid in range(noverlap):
        H.fiber(run_one, wid)
    wg.wait()
    # Hard-reset the global to the pristine baseline before the load-bearing pool
    # runs: the documented-unsafe overlap drift is now fully isolated to this
    # drained pre-phase + its measured counters, and CANNOT reach the serialized
    # oracle.
    try:
        locale.setlocale(CAT, state["baseline"])
    except (locale.Error, ValueError):
        pass


def setup(H):
    # Capture the baseline LC_NUMERIC BEFORE any block runs.  The serialized
    # strict-LIFO arm must leave this exactly restored.
    baseline = locale.setlocale(CAT)

    # Guard for locale availability: we need at least one settable locale BEYOND
    # 'C' whose LC_NUMERIC formatting DIFFERS observably from the baseline (so the
    # in-block format-agreement oracle has a real signal and the hazard is real).
    install = None
    install_fmt = None
    base_fmt = locale.format_string("%.1f", SAMPLE, grouping=True)
    for cand in ("en_US.UTF-8", "en_US.utf8", "C.UTF-8", "C.utf8", "POSIX"):
        name = _try_set(cand)
        if name is None:
            continue
        fmt = _fmt_under(cand)
        if fmt != base_fmt:                 # observably different formatting
            install = cand
            install_fmt = fmt
            break
    if install is None:
        # No locale beyond the baseline gives observably-different number
        # formatting on this box -> the hazard cannot be exercised meaningfully.
        # SKIP cleanly (PASS) rather than run a vacuous oracle.
        print("[p461_locale_setlocale_restore] SKIP: no settable locale beyond "
              "baseline {0!r} produces observably-different LC_NUMERIC formatting "
              "on this box (tried en_US.UTF-8/C.UTF-8/POSIX); the setlocale "
              "save/restore hazard cannot be exercised -- skipping cleanly.".format(
                  baseline))
        sys.stdout.flush()
        sys.exit(0)

    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    noverlap = min(400, int(nworkers * OVERLAP_FRACTION))
    if noverlap % 2:
        noverlap -= 1                       # keep the overlap arm fully paired
    npairs = noverlap // 2

    pairs = []
    for _ in range(npairs):
        a, b = socket.socketpair()
        H.register_close(a)
        H.register_close(b)
        pairs.append((a, b))

    H.state = {
        "baseline": baseline,               # the global LC_NUMERIC baseline
        "install": install,                 # the locale each block installs
        "install_fmt": install_fmt,         # race-free expected format under it
        "base_fmt": base_fmt,               # format under the baseline
        "lock": runloom.sync.Lock(),        # serializes the load-bearing arm
        "nworkers": nworkers,
        "noverlap": noverlap,               # report-only overlap pre-phase pop
        "pairs": pairs,
        "rdv_timeout": 2.0,
        "ser_blocks": [0] * 1024,           # serialized strict-LIFO blocks completed
        "overlap_blocks": [0] * 1024,       # overlap blocks completed (report only)
        "overlap_drift": [0] * 1024,        # overlap blocks that drifted the global
    }
    H.log("baseline LC_NUMERIC={0!r} fmt({1})={2!r} | install={3!r} fmt={4!r} "
          "(observably different -> oracle non-vacuous)".format(
              baseline, SAMPLE, base_fmt, install, install_fmt))


def body(H):
    # Phase 1 (report-only, fully drained): the documented-unsafe OVERLAP arm,
    # measured in isolation so it cannot contaminate the shared global while the
    # load-bearing pool measures it.
    run_overlap_phase(H, H.state)
    # Phase 2 (LOAD-BEARING): the serialized strict-LIFO pool.  The global starts
    # at the pristine baseline (run_overlap_phase reset it), so any baseline drift
    # left at post() is a serialized-arm save/restore desync under M:N.
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    baseline = H.state["baseline"]
    ser = sum(H.state["ser_blocks"])
    ovl = sum(H.state["overlap_blocks"])
    drift = sum(H.state["overlap_drift"])
    drift_pct = (100.0 * drift / ovl) if ovl else 0.0

    # The global is read DIRECTLY -- NOT reset first -- so this is a genuine,
    # non-vacuous measurement.  The overlap arm self-restored per block AND was
    # hard-reset after its drained pre-phase, so any drift left in the global after
    # the whole run is attributable to the LOAD-BEARING serialized strict-LIFO arm:
    # a serialized worker whose restore desynced across a hub migration / preempt
    # and failed to restore the global LC_NUMERIC.  That is the runloom bug.
    now = locale.setlocale(CAT)
    H.log("serialized-LIFO blocks={0} (LOAD-BEARING) | overlap blocks={1} "
          "drifted={2} ({3:.1f}%, documented-unsafe non-LIFO -- REPORT ONLY) | "
          "baseline={4!r} final={5!r}".format(
              ser, ovl, drift, drift_pct, baseline, now))

    # LOAD-BEARING: the GLOBAL LC_NUMERIC MUST be the exact baseline after the run.
    # The overlap arm self-restored + was hard-reset, so a residual off-baseline
    # locale is a SERIALIZED-arm save/restore desync under M:N (hub migration /
    # preempt-mid-restore) -- a runloom bug, NOT a documented caveat (serialized
    # strict-LIFO use always restores under run(1)/GIL -- verified).
    H.check(now == baseline,
            "GLOBAL LOCALE CORRUPTED: LC_NUMERIC={0!r} != baseline {1!r} after the "
            "SERIALIZED strict-LIFO arm quiesced -- a lock-serialized (never-"
            "overlapping) setlocale block's restore desynced across a hub "
            "migration/preempt and stranded the PROCESS-GLOBAL locale off baseline "
            "(locale is a C-library global, NOT contextvar/threadlocal-isolated)"
            .format(now, baseline))

    # Sanity: the load-bearing serialized arm actually ran (the hazard was
    # exercised, not skipped) -- otherwise the oracle is vacuous.
    H.check(ser > 0,
            "no serialized setlocale block ran -- the load-bearing global "
            "save/restore hazard was never exercised (oracle would be vacuous)")

    # Report-only context: surface that the documented-unsafe overlap arm observed
    # drift (expected, benign) so the semantics are explicit in the log.
    if drift:
        H.log("note: the overlap arm observed {0} per-block global-locale drifts "
              "across {1} overlapping blocks -- documented-unsafe non-LIFO "
              "setlocale usage (reproduces under plain GIL threads with "
              "PYTHON_GIL=1), NOT a runloom bug; each overlap block self-restored "
              "and the pre-phase was hard-reset so this never reaches the "
              "load-bearing check".format(drift, ovl))

    # COMPLETENESS: no worker parked-then-vanished (e.g. stranded holding the
    # shared Lock when it vanished, or parked in the rendezvous).
    H.require_no_lost("locale.setlocale global save/restore")


if __name__ == "__main__":
    harness.main("p461_locale_setlocale_restore", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="locale.setlocale() saves/restores the PROCESS-GLOBAL "
                          "C-library LC_NUMERIC (a non-contextvar/non-threadlocal "
                          "global driving number formatting) assuming LIFO "
                          "nesting; the SERIALIZED strict-LIFO arm (one shared "
                          "lock, park+migrate between/inside blocks) MUST restore "
                          "the global to its exact baseline under M:N -- a "
                          "save/restore desync across hub migration is the real "
                          "runloom bug.  The non-LIFO OVERLAP drift is documented-"
                          "unsafe (reproduces under plain GIL threads) -- "
                          "report-only.  SKIPs cleanly if no locale beyond 'C' is "
                          "available")
