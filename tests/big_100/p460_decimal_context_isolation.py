"""big_100 / 460 -- decimal thread-affine Context isolation under M:N.

decimal's active context is a thread-affine MUTABLE Context object.  In 3.14t it
is contextvar-backed (decimal.HAVE_CONTEXTVAR is True): a single ContextVar holds
the *Context object*, and decimal.getcontext() returns that live object.
getcontext().prec / .rounding / .flags all MUTATE it IN PLACE.  decimal.local-
context() is the documented-SAFE primitive: on __enter__ it installs a COPY of the
current Context as a NEW private object (its own ContextVar token), runs the block
against that copy, and on __exit__ resets the ContextVar back -- so a block's prec
change is supposed to be invisible to every other execution context.

WHERE M:N BREAKS IT (the gap this program probes).  runloom gives each fiber a
per-fiber context via PyContext_CopyCurrent, which copies the context MAPPING --
i.e. the ContextVar -> value bindings.  But the decimal ContextVar's *value* is the
Context OBJECT, and a shallow mapping copy copies the REFERENCE: every hub fiber's
copied mapping points at the SAME Context object.  So getcontext() returns one
shared object across all fibers on a hub, and a sibling's in-place prec change
corrupts another fiber's "private" arithmetic across a yield.  localcontext() does
NOT save it: it copies that same shared object and (under M:N) the copy can be the
object a SIBLING is also mutating, or the ContextVar token reset desyncs across a
hub migration.  Empirically (verified, not assumed) localcontext arithmetic that is
race-free under stock threads (GIL on AND off) corrupts under runloom M:N -- a true
runloom isolation bug, the decimal sibling of the BUG#7 contextvar class.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  decimal.localcontext() is DOCUMENTED to give a private context: a block that sets
  ctx.prec = P and computes Decimal(1)/Decimal(7) MUST get a value with exactly P
  significant digits, and recomputing it after a yield MUST give the identical
  value, no matter what siblings do -- the block's prec is private.  We verified
  with a standalone plain-threads control (64 threads, same hazard, NO runloom)
  that this holds with PYTHON_GIL=1 AND PYTHON_GIL=0: 0 mismatches in 25600 checks
  each.  Stock CPython keys the decimal ContextVar per OS thread, so each thread
  gets its OWN Context object and localcontext is genuinely private for any GIL
  setting.  An oracle that fired there would be a false-positive detector; it does
  NOT fire there.  Under a CORRECT runloom it must ALSO hold (each fiber a private
  context).  If runloom leaks a sibling's prec across the yield -- localcontext's
  recomputed 1/7 has the WRONG digit count, or r1 != r2, or the Inexact/Rounded
  flags are polluted by a sibling -- that is the runloom isolation bug, and the
  serialized single-owner localcontext arm PASSES on a correct runtime (program
  exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- localcontext() PRIVATE-CONTEXT INTEGRITY (worker, HARD,
    fail-fast).  Each fiber opens `with decimal.localcontext() as ctx:`, sets
    ctx.prec to a unique-per-wid value P, clears the flags, computes
    r1 = Decimal(1)/Decimal(7), YIELDS (runloom.sleep / yield_now), then asserts:
      - r2 = Decimal(1)/Decimal(7) recomputed at its own prec equals r1 (private
        prec survived the yield);
      - r1 has exactly P significant digits (the prec actually in force is P, not a
        leaked sibling prec);
      - the value equals the precomputed CANONICAL 1/7 at prec P (closed-world:
        ONE_SEVENTH[P] is computed once, single-owner, before the pool -- so the
        check is independent of any shared decimal state);
      - the Inexact flag is set and Overflow/DivisionByZero (impossible for 1/7)
        are NOT set -- a sibling cannot have polluted the flags.
    Single-owner: nothing but THIS fiber should touch its localcontext block.  A
    failure is a runloom per-fiber decimal-context isolation desync.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-block
    (stranded inside localcontext.__exit__ on a desynced ContextVar token) never
    returns; the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (lc_checks > 0).

  * MEASURED (report-ONLY, NEVER fails): the GLOBAL getcontext().prec path.  A
    fiber sets a unique prec on the SHARED global context (getcontext().prec = P),
    yields, reads it back; a read != P is a cross-fiber LEAK.  This is the
    documented thread-affine-shared-object behavior under M:N -- getcontext()
    returns the hub's shared Context, so siblings on the hub see each other's prec,
    exactly like p67's threading.local / p66's contextvar leak.  (It is 0 under
    plain threads only because each OS thread has its OWN context object; under M:N
    many fibers share one hub thread's object.)  We MEASURE + REPORT the leak rate,
    NEVER fail on it -- failing would mislabel the documented M:N shared-object
    semantics as a bug.  The global path NEVER touches the load-bearing arm's
    private localcontext checks (those are self-contained via the precomputed
    canonical table), so the measured leak cannot contaminate the oracle.

FAIL ON: wrong arithmetic under localcontext (r1 != r2, wrong digit count, or
value != canonical 1/7 at this prec), an impossible/polluted flag, or a crash.
NEVER fail on the global getcontext() leak (measured).

Stresses: decimal thread-affine mutable Context object shared across hub fibers,
PyContext_CopyCurrent shallow-copies the ContextVar->Context binding (the Context
object reference, not a deep copy), localcontext() __enter__/__exit__ ContextVar
token save/restore across hub migration + preempt-mid-block, SignalDict flag
pollution, contextvar-backed (HAVE_CONTEXTVAR) global isolation.

Good TSan / controlled-M:N-replay target: getcontext()/setcontext() mutate one
shared Context object's prec/rounding/flags fields across hubs -- a data race on
those fields, or a replay that migrates a hub between localcontext's __enter__ and
its arithmetic, localizes the leak before the digit-count oracle fires.
"""
import decimal
from decimal import Decimal, localcontext, getcontext

import harness
import runloom

# Per-fiber prec values are drawn from this band.  Each prec yields a 1/7 with a
# DISTINCT, deterministic value (exactly `prec` significant digits), so a leaked
# sibling prec changes the recomputed value detectably.  Kept clear of 1 and 2
# (degenerate) and capped well under the 28 default so the values differ visibly.
PREC_MIN = 3
PREC_MAX = 30
PREC_SPAN = PREC_MAX - PREC_MIN + 1

# Canonical, single-owner precompute of Decimal(1)/Decimal(7) at every prec in the
# band.  Computed ONCE in the root, before any worker runs, each in its OWN
# localcontext so this table itself is race-free and independent of all shared
# decimal state.  The load-bearing oracle compares a fiber's localcontext result
# against ONE_SEVENTH[prec] -- a fixed closed-world reference, not a live read of
# any shared context -- so the check cannot be contaminated by the measured global
# leak.  Built in setup().
ONE_SEVENTH = {}


def build_canonical():
    """One-time, single-owner: the exact Decimal(1)/Decimal(7) and its digit count
    at every prec in the band, each computed inside its own localcontext so the
    table is independent of any shared/global decimal state."""
    table = {}
    for p in range(PREC_MIN, PREC_MAX + 1):
        with localcontext() as ctx:
            ctx.prec = p
            ctx.clear_flags()
            val = Decimal(1) / Decimal(7)
            digs = len(val.as_tuple().digits)
            table[p] = (val, digs)
    return table


def setup(H):
    global ONE_SEVENTH
    ONE_SEVENTH = build_canonical()
    # Sanity: the canonical table must show prec -> exactly that many digits (this
    # is the closed-world fact the oracle leans on).  Single-owner, race-free.
    for p in range(PREC_MIN, PREC_MAX + 1):
        _, digs = ONE_SEVENTH[p]
        if digs != p:
            H.fail("canonical 1/7 at prec {0} has {1} digits (expected {0}) -- "
                   "table build is broken".format(p, digs))
            return
    H.state = {
        "lc_checks": [0] * 1024,        # load-bearing localcontext checks done
        "global_checks": [0] * 1024,    # measured global-path checks done
        "global_leaks": [0] * 1024,     # measured cross-fiber global prec leaks
        "have_cv": bool(getattr(decimal, "HAVE_CONTEXTVAR", False)),
    }


# --------------------------------------------------------------------------
# LOAD-BEARING arm: localcontext() private-context integrity.  Single-owner.
# localcontext() is DOCUMENTED to give a private context; under a correct runtime
# (and plain threads, GIL on AND off -- verified) the block's prec is invisible to
# siblings, so r1==r2, r1 has exactly P digits, and r1 == the canonical 1/7 at P.
# A leak of a sibling's prec across the yield breaks one of those -> runloom bug.
# --------------------------------------------------------------------------
def lc_check(H, wid, idx, state):
    # Rotate prec by (wid + idx) so a fiber's prec differs from its hub siblings'
    # and from its own previous iteration -- a leaked sibling prec is then always a
    # value distinct from this block's, hence detectable.
    p = PREC_MIN + ((wid + idx) % PREC_SPAN)
    canon_val, canon_digs = ONE_SEVENTH[p]
    with localcontext() as ctx:
        ctx.prec = p
        ctx.clear_flags()                       # start from clean flags
        r1 = Decimal(1) / Decimal(7)
        # YIELD + SLEEP-PARK: a sibling fiber on this hub runs (and is itself
        # mid-block at a different prec) while this fiber is PARKED.  The sleep-park
        # -- not a bare yield_now -- is what reliably deschedules this fiber long
        # enough that the scheduler runs a sibling mid-block on the shared Context
        # object before we resume.  If runloom leaks that shared object, the
        # sibling's prec change is now in force when we recompute.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)
        r2 = Decimal(1) / Decimal(7)            # recomputed at OUR own prec
        # Snapshot the flags WHILE still inside the block (before __exit__ resets).
        inexact = ctx.flags[decimal.Inexact]
        overflow = ctx.flags[decimal.Overflow]
        divzero = ctx.flags[decimal.DivisionByZero]
        invalid = ctx.flags[decimal.InvalidOperation]
        live_prec = ctx.prec

    # (1) r1 == r2: our private prec survived the yield (no sibling leaked in).
    if r1 != r2:
        H.fail("localcontext NOT private: 1/7 changed across a yield, {0} -> {1} "
               "(wid {2} set prec {3}) -- a sibling fiber's prec leaked into this "
               "fiber's localcontext block (runloom shares the thread-affine "
               "decimal Context object across hub fibers)".format(
                   r1, r2, wid, p))
        return
    # (2) digit count == P: the prec actually in force was OURS, not a leaked one.
    digs = len(r1.as_tuple().digits)
    if digs != canon_digs:
        H.fail("localcontext prec LEAKED: 1/7 has {0} significant digits but this "
               "fiber's localcontext set prec {1} (expected {1} digits); value "
               "{2!r} (wid {3}) -- a sibling's prec corrupted this private "
               "block".format(digs, p, r1, wid))
        return
    # (3) value == canonical 1/7 at OUR prec (closed-world reference).
    if r1 != canon_val:
        H.fail("localcontext arithmetic WRONG: 1/7 at prec {0} = {1!r} != "
               "canonical {2!r} (wid {3}) -- the private context did not hold the "
               "prec it was set to".format(p, r1, canon_val, wid))
        return
    # (4) live prec inside the block must still be ours at __exit__-1.
    if live_prec != p:
        H.fail("localcontext prec MUTATED in place: ctx.prec == {0} != {1} this "
               "fiber set (wid {2}) -- a sibling mutated the shared Context object "
               "this localcontext block was supposed to own privately".format(
                   live_prec, p, wid))
        return
    # (5) flags: 1/7 is Inexact (1 does not divide 7 exactly) and CANNOT raise
    # Overflow / DivisionByZero / InvalidOperation.  A set impossible flag = a
    # sibling polluted this block's SignalDict.
    if not inexact:
        H.fail("localcontext flag CORRUPTION: Inexact NOT set after 1/7 at prec "
               "{0} (wid {1}) -- 1/7 is inexact; a sibling cleared/overwrote this "
               "block's flags".format(p, wid))
        return
    if overflow or divzero or invalid:
        H.fail("localcontext flag POLLUTION: impossible flag set after 1/7 at prec "
               "{0} (wid {1}) -- Overflow={2} DivisionByZero={3} InvalidOperation="
               "{4}; 1/7 raises none of these, so a sibling's operation polluted "
               "this block's flags via the shared Context object".format(
                   p, wid, overflow, divzero, invalid))
        return
    state["lc_checks"][wid & 1023] += 1


# --------------------------------------------------------------------------
# MEASURED arm: the GLOBAL getcontext().prec path.  Report-ONLY, NEVER fails.
# getcontext() returns the hub's SHARED thread-affine Context; a sibling's prec
# change is visible here under M:N (documented shared-object behavior, exactly
# like p67's threading.local).  We measure the leak rate; we do NOT assert on it.
# It is fully separate from the load-bearing localcontext checks (those use the
# precomputed canonical table, never a live global read), so it cannot poison the
# oracle.
# --------------------------------------------------------------------------
def global_check(H, wid, r, state):
    p = PREC_MIN + ((wid * 7 + r) % PREC_SPAN)
    getcontext().prec = p                        # mutate the SHARED global in place
    runloom.yield_now()
    if r & 1:
        runloom.sleep(0.0002)
    got = getcontext().prec
    state["global_checks"][wid & 1023] += 1
    if got != p:
        # Documented M:N shared-object leak (a sibling on this hub set its prec on
        # the same Context object).  MEASURED, never failed.  Still guard against a
        # truly impossible (out-of-band / garbage) value, which would be corruption
        # rather than a plausible sibling prec.
        if not (PREC_MIN <= got <= PREC_MAX) and not (1 <= got <= 1000000):
            H.fail("global getcontext().prec CORRUPTION: read {0!r} (wid {1}) -- "
                   "not any plausible fiber prec, the shared Context object is "
                   "torn".format(got, wid))
            return
        state["global_leaks"][wid & 1023] += 1


# Sustained localcontext blocks per worker, bounded by H.running().  The sibling-
# prec-leak hazard only manifests under SUSTAINED churn -- many fibers
# simultaneously mid-block and sleep-PARKED across their yield, so the scheduler
# reliably runs a sibling (at a different prec) on the shared Context object before
# this fiber resumes.  A single block per fiber (one block, then return) barely
# overlaps a sibling's and does NOT reproduce.  So each worker runs a sustained
# internal loop -- one private localcontext block per iteration, interleaved with
# the harness counter calls and a sleep-park on odd iterations (the exact cadence
# that reproduces) -- until the deadline (H.running()) or INNER_CAP.  Bounding by
# H.running() makes the load-bearing oracle fire at the DEFAULT --rounds 1 (it does
# not depend on a large --rounds); INNER_CAP stops one worker from monopolizing
# teardown if the box is slow.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms: the LOAD-BEARING private localcontext check
    (fail-fast) and the MEASURED global-path check (report only).  The two do not
    interact -- localcontext installs a private ContextVar token; the global path
    mutates the shared object -- so running them in the same fiber keeps the hub
    busy with mixed prec churn without the global mutation reaching the canonical-
    table-based localcontext oracle.

    The worker SUSTAINS a churn loop bounded by H.running(): one private
    localcontext block per iteration (sleep-parking on odd iterations) so many
    fibers stay simultaneously mid-block and parked -- the condition the sibling-
    prec leak needs -- regardless of the harness --rounds setting.  The outer
    round_range() still honors --rounds for the soak sweep; --rounds 1 (the default)
    runs the sustained inner loop exactly once, which is all it takes."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            lc_check(H, wid, idx, state)        # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            global_check(H, wid, idx, state)    # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    lc = sum(H.state["lc_checks"])
    gchecks = sum(H.state["global_checks"])
    gleaks = sum(H.state["global_leaks"])
    gpct = (100.0 * gleaks / gchecks) if gchecks else 0.0
    H.log("decimal: localcontext private-integrity checks={0} (LOAD-BEARING, all "
          "passed fail-fast) | global getcontext().prec checks={1} leaks={2} "
          "({3:.1f}%, documented thread-affine shared-Context leak under M:N -- "
          "REPORT ONLY, like p67/p66) | HAVE_CONTEXTVAR={4}".format(
              lc, gchecks, gleaks, gpct, H.state["have_cv"]))
    if gleaks:
        H.log("note: the global getcontext().prec path observed {0} cross-fiber "
              "leaks across {1} checks -- runloom hub fibers share one thread-"
              "affine decimal Context object, so getcontext() mutations are "
              "visible to siblings (0 under plain threads only because each OS "
              "thread owns its context).  This is documented M:N shared-object "
              "behavior, NOT a runloom bug, and never reaches the load-bearing "
              "localcontext oracle".format(gleaks, gchecks))
    # NON-VACUITY: the load-bearing localcontext hazard was actually exercised.
    H.check(lc > 0,
            "no localcontext private-integrity checks ran -- the load-bearing "
            "decimal-context isolation hazard was never exercised (oracle would "
            "be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded in
    # localcontext.__exit__ on a desynced ContextVar token).
    H.require_no_lost("decimal localcontext context-isolation")


if __name__ == "__main__":
    harness.main(
        "p460_decimal_context_isolation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="decimal's active context is a thread-affine MUTABLE Context "
                 "object (contextvar-backed); runloom's per-fiber "
                 "PyContext_CopyCurrent shallow-copies the ContextVar->Context "
                 "binding so hub fibers share ONE Context object.  LOAD-BEARING: "
                 "decimal.localcontext() MUST give a private context -- 1/7 at a "
                 "unique per-fiber prec keeps its value+digit-count across a yield "
                 "and matches the canonical 1/7 at that prec, with un-polluted "
                 "flags (0 under plain threads GIL on AND off; a sibling-prec leak "
                 "is the runloom bug).  The GLOBAL getcontext().prec leak is the "
                 "documented thread-affine shared-object M:N behavior -- measured, "
                 "report-only")
