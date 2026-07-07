"""big_100 / 555 -- contextvars.Context.run() single-owner snapshot isolation under M:N.

contextvars.Context is a *mapping* object (ContextVar -> value) with its own private
hamt (`ctx_vars`).  `Context.run(fn)` enters the context -- it saves the thread
state's current context (`ts->context`), points `ts->context` at THIS Context, runs
`fn` (so every `ContextVar.set()` inside `fn` mutates THIS Context's hamt), then on
exit restores the saved context and asserts `ts->context` is still this Context.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom's M:N fibers share
ONE PyThreadState per hub.  When a fiber suspends at a cooperative yield, the runtime
must SAVE that fiber's `ts->context` and, on resume (possibly on a DIFFERENT hub /
PyThreadState), RESTORE it (src/runloom_c/runloom_sched_pystate.c.inc lines 111-127
save; 384-396 restore, with the context_ver bump).  If that per-fiber context
save/restore is wrong -- restores a sibling's context, skips the restore on a
fast-path, or races on `context_ver` -- then a fiber that is INSIDE its own
`ctx.run()` and yields could resume with `ts->context` pointing at a SIBLING's
Context.  A `ContextVar.set()` executing then would mutate the WRONG (sibling's)
Context; conversely a sibling's set could land in OUR Context.  Either way THIS
fiber's single-owner Context would end up holding a value it never set -- a
cross-fiber Context corruption.  `Context.run()` itself would also raise
"cannot exit context: thread state references a different context object" if the
restore left `ts->context` desynced from the entered Context.

DISTINCT FROM p66 (contextvars measure).  p66 uses the GLOBAL (hub-thread) context:
each fiber does `CV.set(wid)` with NO Context.run() and reads back via `CV.get()`,
and MEASURES the cross-fiber leak rate report-only (a shared thread-current context
is documented M:N behavior, never failed).  Here the load-bearing object is a
SINGLE-OWNER `contextvars.Context()` created per fiber; its snapshot integrity --
what the Context object itself maps each ContextVar to -- is a HARD oracle.  Reading
the Context OBJECT's mapping (`ctx[cv]`, `iter(ctx)`, `len(ctx)`) reads that object's
own private hamt, independent of whatever `ts->context` currently is, so on a correct
runtime it is ALWAYS exactly this fiber's values.  The thread-current view
(`CV.get()` / `copy_context()` inside run) is the p66-style report-only arm.

WHICH ORACLE IS LOAD-BEARING, AND WHY (holds on plain threads too):

  A `contextvars.Context()` is a single-owner mapping.  A fiber that creates its own
  empty Context, enters it via `ctx.run(setter)`, and inside the setter binds a fixed
  set of (globally-shared-identity) ContextVars to values UNIQUE to this fiber, then
  yields INSIDE the run (so siblings running their OWN ctx.run() interleave), MUST
  find -- when it later iterates the Context OBJECT it owns -- every ContextVar mapped
  to exactly the value THIS fiber set, and NO extra keys.  Verified with a standalone
  plain-threads control (8 OS threads, each with its own Context and the same shared
  ContextVars set to distinct per-thread values, GIL on AND off): iterating each
  thread's own Context object returns 100% that thread's values, 0 cross-thread
  bleed.  Under a correct runloom it must also hold: the Context is single-owner and
  its hamt is only mutated while it is the current context of THIS fiber.  If iterating
  the owned Context returns a value this fiber never set (a sibling's value, a torn
  value, a missing/extra key), OR `ctx.run()` raises the "different context object"
  RuntimeError, that is a runloom per-fiber-context save/restore bug, and the
  single-owner oracle PASSES on a correct runtime (exit 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- CONTEXT SNAPSHOT ISOLATION (worker, HARD, fail-fast).  Each fiber:
      - creates a FRESH single-owner `contextvars.Context()` (empty, fully isolated);
      - `ctx.run(setter)` where setter binds each shared ContextVar to a UNIQUE
        per-fiber value (`base + i`, base derived from wid so no two fibers overlap),
        then YIELDS inside the run (yield_now + occasional tiny sleep) so siblings
        reliably interleave while this Context is entered;
      - AFTER run returns, iterates the Context OBJECT it owns and asserts:
          (1) every ContextVar is present (`cv in ctx`);
          (2) `ctx[cv] == base + i` -- this fiber's value, never a sibling's;
          (3) `len(ctx) == NUM_VARS` -- no leaked/extra bindings;
          (4) iterating `ctx` yields exactly our ContextVars (closed key set).
    Single-owner: the Context is a fiber-local variable, never shared.  A mismatch is
    a runloom context-isolation desync; a raised RuntimeError from run() is caught and
    turned into a fail (the exit-time "different context object" assertion tripping).

  * MEASURED (report-ONLY, NEVER fails): thread-current context view.  Inside the run,
    after the yield, the setter reads back via `CV.get()` and takes a `copy_context()`
    snapshot -- both read `ts->context`, the SHARED-per-hub thread-current context (the
    p66 hazard).  We MEASURE how often that view disagrees with this fiber's values and
    report the rate; we NEVER fail on it (a shared thread-current context is documented
    M:N behavior, not a runloom bug).  On the current runtime -- which DOES save/restore
    ts->context per fiber -- this rate is expected to be ~0, which independently
    corroborates the isolation; but we keep it report-only so a future change that
    reintroduces thread-current sharing cannot mislabel documented semantics as a bug.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside ctx.run()
    (e.g. parked on the yield and never resumed, or wedged in the exit assertion) never
    returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (context_checks > 0).

FAIL ON: a single-owner Context mapping a ContextVar to a value this fiber never set,
a missing/extra key in the owned Context, or `ctx.run()` raising the "different context
object" RuntimeError across a yield.  The thread-current (CV.get/copy_context) arm is
report-only and may (on a hypothetical thread-current-sharing runtime) show leaks --
the load-bearing single-owner Context oracle must stay clean.

Stresses: contextvars.Context.run() enter/exit across cooperative yields + hub
migration, per-fiber ts->context save/restore (pystate snap), ContextVar.set landing
in the correct entered Context, Context mapping-protocol iteration under M:N, the
run() exit "different context object" invariant.

Good TSan / controlled-M:N-replay target: `ts->context` and `context_ver` are read on
the resume fast-path and written on the swap; a data-race report there, or a replay
that resumes a fiber with a stale context pointer, localizes the desync before the
single-owner value oracle even fires.
"""
import contextvars

import harness
import runloom

# Number of globally-shared-IDENTITY ContextVars.  Each fiber's OWN Context stores
# its own binding for each; the shared identity is what makes a cross-fiber leak
# expressible (a sibling setting "the same var" to a different value).  Sized so the
# Context's hamt has several entries (more than the single-node fast case).
NUM_VARS = 8
CVARS = tuple(
    contextvars.ContextVar("p555_cv_{0}".format(i)) for i in range(NUM_VARS)
)

# Sustained checks per worker, bounded by H.running().  The context save/restore
# hazard only manifests under SUSTAINED churn: many fibers simultaneously entered in
# their own ctx.run() and PARKED across the in-run yield, so the scheduler reliably
# interleaves a sibling (with its own entered Context) before this fiber resumes.  A
# single check per fiber barely overlaps a sibling's and does NOT reproduce.
INNER_CAP = 100000


def fiber_base(wid, idx):
    """Unique value base for (wid, idx).

    Different wids differ by whole multiples of (1<<32), and the NUM_VARS per-var
    offsets stay far below that, so NO two fibers' value ranges ever overlap: a value
    read back from an owned Context that is not `base + i` for THIS call's base is,
    with certainty, not a value this fiber produced -- a cross-fiber leak or torn
    entry, never an ambiguous collision."""
    return (wid + 1) * (1 << 32) + (idx & 0x00FFFFFF) * NUM_VARS


def context_isolation_check(H, wid, idx, state):
    """Single-owner Context snapshot isolation check (LOAD-BEARING, fail-fast).

    Create a fresh isolated Context, bind the shared ContextVars to this fiber's
    unique values inside ctx.run(), yield inside the run so siblings interleave, then
    verify the OWNED Context object still maps every var to exactly this fiber's value.
    """
    base = fiber_base(wid, idx)
    ctx = contextvars.Context()          # fresh, empty, single-owner
    box = {"leaked": False}

    def setter():
        # Bind each shared ContextVar to THIS fiber's unique value.  Because run()
        # made `ctx` the current context before calling us (with no yield in between),
        # every set lands in `ctx`'s own hamt.
        for i in range(NUM_VARS):
            CVARS[i].set(base + i)
        # YIELD INSIDE the run: siblings entered in their OWN ctx.run() interleave
        # here while this Context is entered.  A broken per-fiber context save/restore
        # would resume us (or a sibling) with the wrong ts->context.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)
        # MEASURED (report-only): the thread-current view via CV.get()/copy_context.
        # This reads ts->context (shared-per-hub, the p66 hazard).  Count disagreement
        # but NEVER fail on it -- a shared thread-current context is documented M:N
        # behavior, not a runloom bug.
        leaked = False
        for i in range(NUM_VARS):
            if CVARS[i].get(None) != base + i:
                leaked = True
        snap = contextvars.copy_context()
        for i in range(NUM_VARS):
            if CVARS[i] in snap and snap[CVARS[i]] != base + i:
                leaked = True
        box["leaked"] = leaked

    try:
        ctx.run(setter)
    except RuntimeError as exc:
        # "cannot exit context: thread state references a different context object"
        # is the run() exit assertion firing because the per-fiber restore left
        # ts->context desynced from the entered Context -- a real runtime desync.
        H.fail("ctx.run() raised RuntimeError across an in-run yield (wid {0}): "
               "{1} -- Context.run()'s exit assertion tripped, meaning ts->context "
               "was left pointing at a different Context (per-fiber context "
               "save/restore desync)".format(wid, exc))
        return

    # ---- HARD oracle: iterate the SINGLE-OWNER Context object ----------------
    # Reading the Context object's own mapping is independent of ts->context; on a
    # correct runtime it is exactly this fiber's values.
    seen = 0
    for i in range(NUM_VARS):
        cv = CVARS[i]
        if cv not in ctx:
            H.fail("single-owner Context LOST a binding: {0!r} absent after "
                   "run() (wid {1}) -- this fiber set it inside ctx.run() but the "
                   "owned Context no longer holds it (context corruption)".format(
                       cv, wid))
            return
        val = ctx[cv]
        expected = base + i
        if val != expected:
            H.fail("Context snapshot ISOLATION broken: owned ctx[{0!r}]={1}, "
                   "expected {2} (wid {3}) -- this fiber's single-owner Context "
                   "holds a value it never set; a sibling's ContextVar.set() landed "
                   "in our Context (or ours landed in theirs) across the in-run "
                   "yield -- a per-fiber ts->context save/restore desync".format(
                       cv, val, expected, wid))
            return
        seen += 1

    # Closed key set: no leaked/extra bindings, and iteration matches exactly.
    if len(ctx) != NUM_VARS:
        H.fail("single-owner Context has {0} entries, expected {1} (wid {2}) -- "
               "an extra/leaked ContextVar binding appeared in this fiber's own "
               "Context (cross-fiber Context pollution)".format(
                   len(ctx), NUM_VARS, wid))
        return
    for cv in ctx:
        if cv not in CVARS:
            H.fail("single-owner Context iterated a FOREIGN ContextVar {0!r} "
                   "(wid {1}) -- a key from another fiber's Context leaked into "
                   "this fiber's owned Context".format(cv, wid))
            return

    state["checks"][wid & 1023] += 1
    if box["leaked"]:
        state["leaks"][wid & 1023] += 1


def worker(H, wid, rng, state):
    """Sustained per-fiber isolation checks.  Each fiber owns a fresh Context per
    iteration and enters it via ctx.run() with an in-run yield, so many fibers are
    simultaneously entered-and-parked in distinct Contexts -- the interleaving that
    exercises the per-fiber context save/restore path."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            context_isolation_check(H, wid, idx, state)
            if H.failed:
                return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "checks": [0] * 1024,    # LOAD-BEARING single-owner snapshot checks (tally)
        "leaks": [0] * 1024,     # MEASURED thread-current-view disagreements (report)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    leaks = sum(H.state["leaks"])
    pct = (100.0 * leaks / checks) if checks else 0.0

    H.log("context[single-owner LOAD-BEARING]: {0} snapshot-isolation checks (all "
          "passed fail-fast -- every owned Context mapped its ContextVars to exactly "
          "this fiber's values) | context[thread-current MEASURED]: {1} CV.get()/"
          "copy_context disagreements ({2:.1f}%, p66-style shared thread-current "
          "context -- REPORT ONLY, never failed)".format(checks, leaks, pct))

    if leaks:
        H.log("note: the thread-current (CV.get/copy_context) view disagreed with "
              "the fiber's values {0} times across {1} checks -- reads of the "
              "shared-per-hub ts->context, documented M:N behavior (like p66), NOT a "
              "runloom bug; this NEVER reaches the load-bearing single-owner Context "
              "oracle".format(leaks, checks))

    # NON-VACUITY: the load-bearing single-owner Context hazard was actually run.
    H.check(checks > 0,
            "no single-owner Context snapshot-isolation checks ran -- the load-"
            "bearing Context.run() isolation hazard was never exercised (oracle "
            "would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside ctx.run()
    # on the in-run yield, or wedged in the exit assertion).
    H.require_no_lost("contextvars Context snapshot isolation")


if __name__ == "__main__":
    harness.main(
        "p555_contextvars_context_run_snapshot", body, setup=setup, post=post,
        default_funcs=6000,
        describe="each fiber runs a setter inside its OWN contextvars.Context() via "
                 "ctx.run(), binding shared-identity ContextVars to unique per-fiber "
                 "values, yields INSIDE the run so siblings interleave, then iterates "
                 "the single-owner Context OBJECT: every ContextVar must map to THIS "
                 "fiber's value (never a sibling's), no missing/extra keys, and run() "
                 "must not raise the 'different context object' RuntimeError.  "
                 "LOAD-BEARING single-owner snapshot isolation across the per-fiber "
                 "ts->context save/restore; the thread-current CV.get()/copy_context "
                 "view is p66-style report-only.  Distinct from p66 (which measures a "
                 "shared thread-current context and never fails)")
