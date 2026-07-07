"""big_100 / 611 -- timeit inner-loop call conservation under M:N.

timeit is a PROCESS-GLOBAL-flavoured module: its module-level default_timer,
the gc.disable()/gc.enable() toggle inside Timer.timeit(), and the shared
_globals() namespace are NOT single-owner, so an oracle built on them would race
exactly like any shared global (documented behavior, NOT a runloom bug).  But
timeit ALSO produces a genuinely SINGLE-OWNER, exactly-countable artifact: a
Timer whose stmt is a fiber-local CALLABLE.  timeit.Timer(stmt=fn).timeit(N)
compiles + execs a private `inner(_it, _timer)` closure (each Timer builds its
OWN code object + namespace via compile/exec) whose body is:

    def inner(_it, _timer, _stmt=_stmt, _setup=_setup):
        _setup()
        _t0 = _timer()
        for _i in _it:           # _it = itertools.repeat(None, number)
            _stmt()
        _t1 = _timer()
        return _t1 - _t0

So a call to .timeit(number=N) invokes `_stmt` EXACTLY N times and `_setup`
EXACTLY once, and .repeat(repeat=R, number=N) invokes `_stmt` EXACTLY R*N times
and `_setup` EXACTLY R times.  That is a closed-form CONSERVATION law with a
single writer: the callable closes over a FIBER-LOCAL counter list that ONLY this
fiber's Timer ever calls.  Nobody else touches this fiber's counter, so its
increments are race-free by construction even with the GIL off -- the count is
the oracle, not the wall-clock time.

WHERE M:N BREAKS IT (the gap this program probes).  We deliberately drive a
runloom.yield_now() from INSIDE the timed callable, so this fiber PARKS mid-way
through timeit's `for _i in _it:` inner loop and a sibling on another hub runs
(building/execing its own Timer, toggling process-global gc, calling its own
inner loop) before this fiber is resumed to finish the loop.  If the runtime
loses/doubles a wakeup, resumes the fiber into the wrong frame, or lets a
sibling's timeit corrupt this fiber's parked inner-loop state, the callable's
invocation count will NOT land on the closed-form N (or R*N) -- a dropped or
doubled loop iteration, or a torn resume.  A lost-wakeup strands the fiber inside
the loop forever (it never returns): caught by the watchdog + require_no_lost.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against the timeit template):

  Each fiber owns a private counter list `cc = [0]` and a private setup counter
  `sc = [0]`; its stmt callable does `cc[0] += 1` (+ a periodic yield), its setup
  callable does `sc[0] += 1`.  It builds its OWN timeit.Timer over those two
  callables (single-owner: the Timer, its compiled `inner`, and both callables
  are fiber-local, never shared).  Then:
    * .timeit(number=N)  MUST leave cc[0] == N and sc[0] == 1, and return a
      float >= 0 (perf_counter is monotonic, so t1-t0 is non-negative).
    * a fresh .repeat(repeat=R, number=N) MUST leave cc[0] == R*N and sc[0] == R,
      and return a list of exactly R floats, each >= 0.
  These are closed-form identities of timeit's inner loop; on a CORRECT runtime
  they hold bit-exactly regardless of hub migration or the sibling gc toggling,
  so the load-bearing single-owner oracle PASSES (exit 0) when there is no bug.
  A miscount is a runloom loop-resume / lost-or-doubled-wakeup desync.

ORACLES:
  * LOAD-BEARING -- CALL CONSERVATION (worker, HARD, fail-fast).  Per round, a
    fiber builds its own Timer over fiber-local callables and asserts the exact
    N / R*N invocation counts + the float/list return shape across the mid-loop
    yields.  Single-owner: no shared mutable state reaches this oracle.

  * CLOSED-WORLD SUM (post, HARD).  Every fiber adds the callable invocations it
    OBSERVED into stmt_calls[wid] and the closed-form it EXPECTED into
    stmt_expected[wid] (single-writer-per-slot, wid-indexed -> race-free).  After
    the join, sum(stmt_calls) == sum(stmt_expected): globally, timeit invoked the
    callables exactly as many times as its own inner-loop arithmetic demands, not
    one call more or fewer.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber parked mid-inner-loop
    (inside `for _i in _it:` at a yield) that never resumes never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): checks > 0 -- the conservation arm actually ran.

FAIL ON: a fiber's own Timer invoking its fiber-local callable a number of times
!= the closed-form N / R*N, a setup called != 1 / != R times, a non-float or
negative elapsed, a repeat() list of the wrong length, or a fiber stranded inside
the timed loop.  There is NO shared-mutable arm: the process-global gc toggle and
default_timer are exercised (siblings race them) but never asserted on -- only the
single-owner callable-count is load-bearing.

Stresses: timeit.Timer compile/exec of a per-Timer `inner` closure, the
itertools.repeat(None, number) inner loop, callable stmt/setup invocation
counting across a mid-loop hub-migrating yield, .timeit() vs .repeat() call
arithmetic, gc.disable()/gc.enable() process-global toggling raced across hubs
(report-agnostic), all under M:N with the GIL off.

Good TSan / controlled-M:N-replay target: the timed `for _i in _it:` loop parks
this fiber (yield_now) between iterations while a sibling execs its own `inner`;
a replay that resumes the parked loop into a stale frame, or double-counts an
iteration, shows up as cc[0] != N before any wall-clock value is even inspected.
"""
import timeit

import harness
import runloom

# Inner-loop iteration count per timeit() call.  Small so a round is fast under
# tens of thousands of fibers, but > 1 so a dropped/doubled loop iteration is a
# detectable +-1 on the exact count, and large enough that the mid-loop yield
# fires several times per timing (reliable sibling interleave).
N_LO = 6
N_HI = 40

# repeat() count per round.  Small so setup/loop arithmetic (R and R*N) stays
# cheap; > 1 so the R and R*N identities are non-trivial.
R_LO = 2
R_HI = 5

# Yield from inside the timed callable every YIELD_EVERY invocations, so this
# fiber PARKS mid inner-loop and a sibling reliably runs before it resumes to
# finish counting up to N.  A single yield per timing barely overlaps a sibling;
# yielding a few times per timing keeps the hub busy with mixed timeit churn.
YIELD_EVERY = 3


def make_callables():
    """Build a fiber-local (single-owner) stmt/setup callable pair over private
    counters.  ONLY this fiber's Timer ever calls them, so the counters have a
    single writer and increment race-free even GIL-off.  The stmt periodically
    parks this fiber mid inner-loop so a sibling interleaves before it resumes.

    Returns (stmt, setup, cc, sc) where cc[0] counts stmt invocations and sc[0]
    counts setup invocations."""
    cc = [0]
    sc = [0]

    def stmt():
        cc[0] += 1
        # Park mid inner-loop at the hazard boundary: a sibling runs (execing its
        # own Timer / toggling gc / running its own loop) before we resume and
        # keep counting.  The count must still land EXACTLY on N.
        if cc[0] % YIELD_EVERY == 0:
            runloom.yield_now()

    def setup():
        sc[0] += 1

    return stmt, setup, cc, sc


def one_round(H, wid, rng, state):
    """One conservation round: build a single-owner Timer and assert timeit()'s
    and repeat()'s closed-form invocation counts across mid-loop yields."""
    stmt, setup, cc, sc = make_callables()
    timer = timeit.Timer(stmt=stmt, setup=setup)

    # ---- .timeit(number=N): stmt called EXACTLY N, setup EXACTLY 1 -----------
    n = rng.randint(N_LO, N_HI)
    elapsed = timer.timeit(number=n)

    if cc[0] != n:
        H.fail("timeit call conservation broken: .timeit(number={0}) invoked the "
               "single-owner stmt callable {1} times, expected exactly {0} (wid "
               "{2}) -- a timed inner-loop iteration was {3} across a mid-loop "
               "yield".format(n, cc[0], wid,
                              "DROPPED" if cc[0] < n else "DOUBLED"))
        return
    if sc[0] != 1:
        H.fail("timeit setup conservation broken: .timeit() ran the single-owner "
               "setup callable {0} times, expected exactly 1 (wid {1}) -- setup is "
               "invoked once per timeit()".format(sc[0], wid))
        return
    if not isinstance(elapsed, float):
        H.fail("timeit returned non-float {0!r} (type {1}) for a single-owner "
               "Timer (wid {2}) -- the inner() return was torn".format(
                   elapsed, type(elapsed).__name__, wid))
        return
    if elapsed < 0.0:
        H.fail("timeit returned NEGATIVE elapsed {0!r} (wid {1}) -- perf_counter "
               "is monotonic so t1-t0 must be >= 0; a torn timer read".format(
                   elapsed, wid))
        return

    # ---- .repeat(repeat=R, number=N): stmt EXACTLY R*N, setup EXACTLY R -------
    # Fresh counters via a fresh callable pair + Timer so the counts are the
    # closed form of THIS call alone.
    stmt2, setup2, cc2, sc2 = make_callables()
    timer2 = timeit.Timer(stmt=stmt2, setup=setup2)
    r = rng.randint(R_LO, R_HI)
    n2 = rng.randint(N_LO, N_HI)
    results = timer2.repeat(repeat=r, number=n2)

    if not isinstance(results, list) or len(results) != r:
        H.fail("timeit.repeat(repeat={0}) returned {1!r} -- expected a list of "
               "exactly {0} floats (wid {2})".format(
                   r, results, wid))
        return
    for t in results:
        if not isinstance(t, float) or t < 0.0:
            H.fail("timeit.repeat yielded a bad timing {0!r} (wid {1}) -- each "
                   "result must be a float >= 0".format(t, wid))
            return
    if cc2[0] != r * n2:
        H.fail("repeat call conservation broken: .repeat(repeat={0}, number={1}) "
               "invoked the single-owner stmt callable {2} times, expected exactly "
               "{3} (wid {4}) -- a timed iteration was {5} across the mid-loop "
               "yields".format(r, n2, cc2[0], r * n2, wid,
                               "DROPPED" if cc2[0] < r * n2 else "DOUBLED"))
        return
    if sc2[0] != r:
        H.fail("repeat setup conservation broken: .repeat(repeat={0}) ran the "
               "single-owner setup callable {1} times, expected exactly {0} (wid "
               "{2}) -- setup runs once per timeit() inside repeat()".format(
                   r, sc2[0], wid))
        return

    # ---- CLOSED-WORLD SUM contribution (single-writer-per-slot, wid-indexed) --
    observed = cc[0] + cc2[0]
    expected = n + r * n2
    state["stmt_calls"][wid] += observed        # race-free: one writer per slot
    state["stmt_expected"][wid] += expected
    state["checks"][wid] += 1


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        one_round(H, wid, rng, state)
        if H.failed:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # All three tables are wid-indexed with ONE writer per slot (allocated here
    # where H.funcs is known) -> the closed-world sum is race-free GIL-off.
    H.state = {
        "stmt_calls": [0] * H.funcs,       # callable invocations OBSERVED
        "stmt_expected": [0] * H.funcs,    # closed-form invocations EXPECTED
        "checks": [0] * H.funcs,           # conservation rounds that passed
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    calls = sum(H.state["stmt_calls"])
    expected = sum(H.state["stmt_expected"])
    checks = sum(H.state["checks"])
    H.log("timeit call conservation: {0} single-owner rounds passed fail-fast; "
          "stmt-callable invocations observed={1} expected={2}; ops={3}".format(
              checks, calls, expected, H.total_ops()))

    # CLOSED-WORLD SUM: globally, timeit invoked the fiber-local callables exactly
    # as many times as its inner-loop arithmetic (N + R*N) demands.  (Per-round
    # fail-fast already proved each fiber's counts; this is the global closure.)
    H.check(calls == expected,
            "timeit global call conservation broken: {0} stmt-callable "
            "invocations observed across the run but the closed-form inner-loop "
            "arithmetic demands exactly {1} -- a timed iteration was lost or "
            "doubled under M:N".format(calls, expected))

    # NON-VACUITY: the load-bearing conservation arm actually ran.
    H.check(checks > 0,
            "no timeit conservation rounds ran -- the single-owner call-count "
            "oracle was never exercised (vacuous)")

    # COMPLETENESS: no fiber stranded mid inner-loop (parked at a yield inside
    # timeit's `for _i in _it:` and never resumed).
    H.require_no_lost("timeit call conservation")


if __name__ == "__main__":
    harness.main(
        "p611_timeit_call_conservation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="timeit.Timer(stmt=fiber-local callable).timeit(N) invokes the "
                 "callable EXACTLY N times and setup EXACTLY once (repeat(R,N): "
                 "R*N and R) -- a closed-form conservation law with a single "
                 "writer (the fiber-local counter only this fiber's Timer calls). "
                 "LOAD-BEARING: each fiber builds its own Timer and yields from "
                 "INSIDE the timed inner loop so it parks mid-loop while a sibling "
                 "runs; the callable-invocation count MUST still land exactly on "
                 "N / R*N.  A dropped/doubled loop iteration, a miscounted setup, "
                 "a non-float/negative elapsed, or a fiber stranded inside the "
                 "timed loop is the runloom bug.  The process-global gc toggle + "
                 "default_timer are raced but never asserted on (single-owner "
                 "callable-count is the only oracle)")
