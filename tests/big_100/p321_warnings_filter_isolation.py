"""big_100 / 321 -- warnings.catch_warnings() global save/restore stack
isolation under M:N.

warnings.catch_warnings() is a context manager that, on __enter__, SAVES the
process-GLOBAL state -- `warnings.filters` (a plain module-level list),
`warnings.showwarning`, and `warnings._filters_mutated` bookkeeping -- and on
__exit__ RESTORES exactly what it saved.  That save/restore is a bare
save-on-enter / overwrite-on-exit pair.  It is NOT contextvar-backed:
`warnings.filters` is a plain global list, so the PyContext_CopyCurrent / BUG#7
contextvar isolation fix does NOT cover it.  This is adjacent-but-distinct from
p66 (contextvars) and p67 (threading.local): those guard goroutine/hub-local
containers; this guards a single PROCESS-GLOBAL list with no per-goroutine
identity at all.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  catch_warnings()'s save/restore assumes STRICT LIFO nesting of blocks, and is
  DOCUMENTED in CPython as "not thread-safe".  If two blocks ever OVERLAP -- one
  __enter__ runs between another's __enter__ and __exit__ -- the global
  `warnings.filters` does NOT return to baseline (a filter leaks: the 6-vs-5 /
  5-vs-10 drift).  We verified this corruption reproduces under PLAIN OS THREADS
  *with the GIL fully ON* (PYTHON_GIL=1) on this very interpreter, and even for an
  atomic block that does no I/O inside it -- i.e. OVERLAPPING / unserialized
  catch_warnings is documented-unsafe usage for ANY concurrency model and ANY
  GIL setting, NOT a runloom-specific bug.  An oracle that hard-failed on that
  would be a FALSE-POSITIVE detector (it fires identically without runloom).  So
  the overlap drift is measured and REPORTED, never failed -- like p67's TLS leak
  rate.

  What IS a genuine runloom M:N invariant -- and the LOAD-BEARING oracle here --
  is the SERIALIZED STRICT-LIFO arm: every catch_warnings block runs behind ONE
  shared cooperative Lock, so the blocks are globally strict-LIFO (never two open
  at once -- the documented-SAFE usage).  Workers still PARK / yield / migrate
  hubs OUTSIDE the lock between blocks, so a goroutine routinely opens its block
  on one hub and could be preempted / migrated during __enter__ / __exit__.
  Under run(1)/GIL this serialized usage ALWAYS restores the global to baseline
  (verified); under M:N it MUST too.  If runloom's save/restore desyncs across a
  hub migration or a preempt-mid-__exit__ -- a runloom regression, NOT a
  documented caveat -- the baseline does not restore.  THAT is the bug this
  program uniquely catches, and the serialized arm PASSES on a correct runtime
  (so the program exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- GLOBAL FILTER-STACK RESTORE INTEGRITY across the SERIALIZED
    strict-LIFO M:N arm (post, HARD):
        snapshot = tuple(warnings.filters) at setup; every serialized worker
        holds the shared Lock around its whole catch_warnings block (strict
        global LIFO) but parks/migrates between blocks; post()
        H.check(tuple(warnings.filters) == snapshot).
    A leaked / dropped / re-ordered filter after the serialized arm quiesces is a
    runloom save/restore desync (migration / preempt-mid-__exit__) -- it does NOT
    reproduce under stock serialized LIFO use, so it is a true runloom signal.
  * COMPLETENESS (post, HARD): require_no_lost -- a worker stranded inside
    catch_warnings.__exit__ on a corrupted stack, or holding the shared Lock when
    it vanished, never returns; the watchdog catches an outright strand and
    require_no_lost catches a parked-then-vanished worker.

  * SECONDARY-A (report-ONLY, NEVER fails): the OVERLAP arm's global-stack drift.
    A minority of workers are paired and PROVABLY overlap two open
    catch_warnings blocks (a rendezvous park inside the block, NO shared lock) --
    the documented-unsafe non-LIFO case.  We measure whether the overlap leaked
    filters from the global list (it does -- reproduces under plain GIL threads),
    reported like p67's leak rate, never asserted.  Each overlap block EXPLICITLY
    restores its own pre-block snapshot of the global list, so the documented-
    unsafe drift never reaches the load-bearing serialized-arm check.
  * SECONDARY-B (report-ONLY, NEVER fails): per-block cross-capture under
    record=True (a uniquely-tagged warn captured by a SIBLING's `w`).
    catch_warnings(record=True) swaps the global showwarning and is documented
    thread-unsafe even in stock CPython, so an interleave cross-capture is
    documented-unsafe-usage -- measured as a rate, never failed.

Keep contenders MODEST: this is a correctness probe of the serialized global-
stack restore, not a scale soak.

Stresses: warnings.catch_warnings() global save/restore (warnings.filters list +
showwarning) across hub migration + preempt-mid-__exit__ under serialized strict-
LIFO usage, __warningregistry__ churn, plain-global (non-contextvar) isolation,
no-lost-wake in __exit__ / while holding a shared cooperative lock.

Good TSan / controlled-M:N-replay target: `warnings.filters` is a plain Python
list mutated (insert(0,...) on simplefilter, slice-restore on __exit__); under
the serialized arm the mutation is single-block-at-a-time, so a data-race report
on the list object -- or a deterministic-replay that migrates a hub between a
serialized block's __enter__ and __exit__ -- is the cleanest signal before the
post() length oracle fires.
"""
import socket
import warnings

import harness
import runloom

# Modest population.  Most workers run the LOAD-BEARING serialized strict-LIFO
# arm; a minority run the report-only OVERLAP arm.
MAX_WORKERS = 4000
# Fraction of workers assigned to the report-only OVERLAP arm (paired, rendezvous
# inside the block, NO shared lock).  Small: a few hundred paired overlaps amply
# demonstrate the documented-unsafe drift without dominating the population.
OVERLAP_FRACTION = 0.2


# --------------------------------------------------------------------------
# LOAD-BEARING arm: SERIALIZED strict-LIFO.  Every catch_warnings block runs
# under ONE shared cooperative Lock, so the blocks are globally strict-LIFO
# (never two open at once = the documented-SAFE usage).  Workers PARK / yield /
# migrate hubs OUTSIDE the lock between blocks.  The global filters MUST restore
# to baseline -- the run(1)/GIL behaviour a runloom save/restore desync across a
# hub migration / preempt-mid-__exit__ would break.
# --------------------------------------------------------------------------
def serialized_block(H, wid, r, rng, state):
    tag = "ser-{0}-{1}".format(wid, r)
    lock = state["lock"]
    # Park / migrate hub OUTSIDE the critical section so the goroutine can be on a
    # different hub each time it takes the lock (exercises migration around the
    # save/restore), without ever overlapping another block.
    runloom.sleep(0.0003)
    runloom.yield_now()
    with lock:
        with warnings.catch_warnings(record=True) as w:
            # Mutates the GLOBAL warnings.filters (prepends a filter) -- the thing
            # catch_warnings must restore on __exit__.  'always' dodges
            # __warningregistry__ dedup so the warn fires.
            warnings.simplefilter("always")
            warnings.warn(tag, UserWarning)
            # record=True cross-capture is documented-unsafe -> SECONDARY-B.
            # (Inside the lock, capture should be clean; we still count any
            # foreign tag rather than assert it.)
            own = sum(1 for rec in w if str(rec.message) == tag)
            foreign = len(w) - own
    if foreign:
        state["cross"][wid & 1023] += 1
    state["ser_blocks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# REPORT-ONLY OVERLAP arm: paired workers PROVABLY overlap two open
# catch_warnings blocks (the documented-unsafe non-LIFO case, NO shared lock).
# Measured, never failed.  Each block explicitly restores its own pre-block
# global snapshot so it cannot poison the load-bearing serialized arm.
# --------------------------------------------------------------------------
def overlap_block(H, wid, r, rng, state):
    pair = wid // 2
    slots = state["pairs"]
    if pair >= len(slots):
        # No pair slot (population edge) -> run a serialized block instead so the
        # worker still does useful, non-poisoning work.
        serialized_block(H, wid, r, rng, state)
        return
    a_sock, b_sock = slots[pair]
    me = a_sock if (wid & 1) == 0 else b_sock
    tag = "ovl-{0}-{1}".format(wid, r)
    # Snapshot the global list ourselves so we can EXPLICITLY restore it after the
    # block (the overlap drift must never reach the load-bearing check).
    saved = warnings.filters[:]
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        # Rendezvous so the SIBLING's catch_warnings block is provably open at the
        # same time as ours (non-LIFO overlap).  Real socketpair recv = netpoll
        # park, so the overlap holds across hubs.
        try:
            me.send(b"x")
            me.settimeout(state["rdv_timeout"])
            try:
                me.recv(1)            # park until the peer's block is also open
            except (socket.timeout, OSError):
                pass                  # peer absent/closed -> proceed (no strand)
        except OSError:
            pass
        warnings.warn(tag, UserWarning)
    # SECONDARY-A: did the overlap leak/drop a filter from the GLOBAL list?
    # (documented-unsafe for any concurrency model / GIL setting -- report only.)
    if warnings.filters[:] != saved:
        state["overlap_drift"][wid & 1023] += 1
    # Explicitly restore OUR pre-block global snapshot so the documented-unsafe
    # overlap drift cannot reach the load-bearing serialized-arm check.
    warnings.filters[:] = saved
    try:
        warnings._filters_mutated()
    except Exception:
        pass
    state["overlap_blocks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """The LOAD-BEARING serialized-arm worker.  Owns its own random.Random (a
    shared one corrupts the Mersenne state GIL-off).  Runs ONLY serialized blocks
    -- the report-only OVERLAP arm runs in a SEPARATE, fully-drained pre-phase
    (run_overlap_phase) so its documented-unsafe drift can never contaminate the
    shared global `warnings.filters` while the load-bearing pool measures it."""
    for r in H.round_range():
        if not H.running():
            break
        serialized_block(H, wid, r, rng, state)
        H.op(wid)
    H.task_done(wid)


def run_overlap_phase(H, state):
    """Report-ONLY pre-phase: spawn the paired OVERLAP workers, let them PROVABLY
    overlap two open catch_warnings blocks (documented-unsafe non-LIFO), and FULLY
    DRAIN them (WaitGroup.wait) before returning.  This runs BEFORE the load-
    bearing serialized pool, so the two arms never touch the shared global
    `warnings.filters` concurrently -- the overlap drift is measured in isolation
    and cannot poison the serialized-arm restore-integrity oracle.  Each overlap
    block also self-restores its own pre-block snapshot; after the drain we hard-
    reset the global to the captured baseline so the serialized pool starts from a
    pristine global regardless of any residual overlap drift."""
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
    # drained pre-phase and its measured counters, and CANNOT reach the
    # serialized-arm oracle.
    warnings.filters[:] = list(state["snapshot"])
    try:
        warnings._filters_mutated()
    except Exception:
        pass


def setup(H):
    # LOAD-BEARING baseline: snapshot the GLOBAL warnings.filters BEFORE any block
    # runs.  The serialized strict-LIFO arm must leave this exactly restored.
    snapshot = tuple(warnings.filters)

    # nworkers = the LOAD-BEARING serialized pool size.
    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    # noverlap = a SEPARATE, modest paired population for the report-only overlap
    # pre-phase (independent of nworkers; capped so the pre-phase stays quick).
    noverlap = min(400, int(nworkers * OVERLAP_FRACTION))
    if noverlap % 2:
        noverlap -= 1                 # keep the overlap arm fully paired
    npairs = noverlap // 2

    pairs = []
    for _ in range(npairs):
        a, b = socket.socketpair()
        H.register_close(a)
        H.register_close(b)
        pairs.append((a, b))

    H.state = {
        "snapshot": snapshot,             # the baseline global filter stack
        "lock": runloom.sync.Lock(),      # serializes the load-bearing arm
        "nworkers": nworkers,
        "noverlap": noverlap,             # report-only overlap pre-phase pop
        "pairs": pairs,
        "rdv_timeout": 2.0,
        "ser_blocks": [0] * 1024,         # serialized strict-LIFO blocks completed
        "overlap_blocks": [0] * 1024,     # overlap blocks completed (report only)
        "overlap_drift": [0] * 1024,      # overlap blocks that drifted the global
        "cross": [0] * 1024,              # record=True cross-captures (report only)
    }


def body(H):
    # Phase 1 (report-only, fully drained): the documented-unsafe OVERLAP arm,
    # measured in isolation so it cannot contaminate the shared global while the
    # load-bearing pool measures it.
    run_overlap_phase(H, H.state)
    # Phase 2 (LOAD-BEARING): the serialized strict-LIFO pool.  The global starts
    # pristine (run_overlap_phase reset it), so any drift left at post() is a
    # serialized-arm save/restore desync under M:N.
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    snap = H.state["snapshot"]
    ser = sum(H.state["ser_blocks"])
    ovl = sum(H.state["overlap_blocks"])
    drift = sum(H.state["overlap_drift"])
    cross = sum(H.state["cross"])
    drift_pct = (100.0 * drift / ovl) if ovl else 0.0

    # The global is read DIRECTLY -- NOT reset first -- so this is a genuine,
    # non-vacuous measurement.  The overlap arm self-restored per block, so any
    # NET drift left in the global after the whole run is attributable to the
    # LOAD-BEARING serialized strict-LIFO arm: a serialized worker whose
    # catch_warnings.__exit__ desynced across a hub migration / preempt and failed
    # to restore the global list.  That is the runloom bug.
    now = tuple(warnings.filters)
    H.log("serialized-LIFO blocks={0} (LOAD-BEARING) | overlap blocks={1} "
          "drifted={2} ({3:.1f}%, documented-unsafe non-LIFO -- REPORT ONLY) | "
          "record=True cross-capture={4} (documented-unsafe -- REPORT ONLY) | "
          "baseline_filters={5} final_filters={6}".format(
              ser, ovl, drift, drift_pct, cross, len(snap), len(now)))

    # LOAD-BEARING: the GLOBAL warnings.filters MUST be the exact baseline after
    # the run.  The overlap arm self-restored, so a residual leaked/dropped/
    # re-ordered filter is a SERIALIZED-arm save/restore desync under M:N (hub
    # migration / preempt-mid-__exit__) -- a runloom bug, NOT a documented caveat
    # (serialized strict-LIFO use always restores under run(1)/GIL -- verified).
    if len(now) != len(snap):
        H.fail("GLOBAL FILTER STACK CORRUPTED: len(warnings.filters)={0} != "
               "baseline {1} after the SERIALIZED strict-LIFO arm quiesced -- a "
               "filter was leaked/dropped by a lock-serialized (never-overlapping) "
               "catch_warnings whose __exit__ desynced across a hub "
               "migration/preempt (warnings.filters is a plain global list, NOT "
               "contextvar-isolated)".format(len(now), len(snap)))
    else:
        H.check(now == snap,
                "GLOBAL FILTER STACK CORRUPTED: warnings.filters restored to the "
                "right LENGTH but the WRONG contents/order vs baseline after the "
                "SERIALIZED strict-LIFO arm quiesced (a save/restore desync "
                "swapped a filter under M:N)")

    # Sanity: the load-bearing serialized arm actually ran (the hazard was
    # exercised, not skipped) -- otherwise the oracle is vacuous.
    H.check(ser > 0,
            "no serialized catch_warnings block ran -- the load-bearing global "
            "save/restore hazard was never exercised (oracle would be vacuous)")

    # Report-only context: surface that the documented-unsafe overlap arm did
    # observe drift (expected, benign) so the semantics are explicit in the log.
    if drift:
        H.log("note: the overlap arm observed {0} per-block global-filter drifts "
              "across {1} overlapping blocks -- documented-unsafe non-LIFO "
              "catch_warnings usage (reproduces under plain GIL threads with "
              "PYTHON_GIL=1), NOT a runloom bug; each overlap block self-restored "
              "so this never reaches the load-bearing check".format(drift, ovl))

    # COMPLETENESS: no worker parked-then-vanished (e.g. stranded in
    # catch_warnings.__exit__ on a corrupted stack, holding the shared Lock when
    # it vanished, or parked in the rendezvous).
    H.require_no_lost("warnings.catch_warnings global save/restore")


if __name__ == "__main__":
    harness.main("p321_warnings_filter_isolation", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="warnings.catch_warnings() saves/restores the PROCESS-"
                          "GLOBAL warnings.filters list (a plain non-contextvar "
                          "global) assuming LIFO nesting; the SERIALIZED strict-"
                          "LIFO arm (one shared lock, park+migrate between blocks) "
                          "MUST restore the global to its exact baseline under M:N "
                          "-- a save/restore desync across hub migration is the "
                          "real runloom bug.  The non-LIFO OVERLAP drift + "
                          "record=True cross-capture are documented-unsafe "
                          "(reproduce under plain GIL threads) -- report-only")
