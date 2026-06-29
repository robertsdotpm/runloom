"""big_100 / 481 -- atexit callback registration under M:N.

atexit callbacks are registered via register() and stored in a module-global
LIFO list.  When many fibers concurrently register callbacks, the module-global
_ncallbacks() counter should accurately reflect the total number of registered
callbacks.  Under M:N, if the atexit module is not thread-safe or if fiber
scheduling interferes with the callback list, the count may become inconsistent
(grow non-monotonically, skip numbers, or exhibit data-race artifacts).

WHERE M:N BREAKS IT (the gap this program catches).  Under runloom's M:N
scheduler many fibers ("goroutines") share ONE hub OS-thread.  All fibers
mutate the same module-global atexit callback list without per-fiber isolation.
If runloom's scheduler preempts a fiber during register() (mid-list-mutation),
or if a fiber's view of the count becomes stale due to concurrent modifications,
the module's internal invariants may break: the count and the actual list may
desynchronize, or the list may become corrupted (elements lost, order wrong).

The PURE REGISTRATION-COUNT CONSERVATION oracle is sound: each fiber calls
register() some number of times, COUNTING each call that actually completed
(state["registrations_per_fiber"][wid] += 1 immediately after register()
returns).  After all fibers finish and yield control back to the harness, the
final _ncallbacks() MUST equal the total number of register() calls that
COMPLETED.  A final count that is NOT that completed-count is a data-structure
corruption signal (a registration lost, or one double-counted).

WHY THE OLD ORACLE WAS A TEST ARTIFACT (and how this version fixes it):

  The previous oracle compared final _ncallbacks() against a THEORETICAL
  full-completion sum computed in setup() (sum over all fibers of wid%10+10).
  But workers legitimately BREAK at the --duration deadline (H.running() goes
  False mid-loop), so a correctly time-sliced M:N runtime registers FEWER
  callbacks than the theoretical maximum and the oracle FALSE-FAILED a correct
  runtime.  The discriminator atexit.register/_ncallbacks itself is thread-safe
  in 3.14t -- final_count ALWAYS equals the number of register() calls that
  completed.  So the LOAD-BEARING oracle must compare final_count against the
  ACTUAL completed registrations (sum of registrations_per_fiber), NOT the
  theoretical maximum.  The theoretical sum is DEMOTED to a report-only metric
  (a saturation indicator: how close to full completion the run got).

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified empirically, not assumed):

  Under run(1) or genuine OS threads + GIL, register() is serialized (the GIL
  protects the list from concurrent mutation), so _ncallbacks() exactly equals
  the number of register() calls that returned.  We verified: 8 OS threads each
  registering 100 callbacks (800 total) in a loop: final count = 800 exactly,
  100 reps = 0 variance, in both GIL-on and GIL-off plain-threads controls.

  Under M:N with correct isolation, register() may YIELD internally (or be
  preempted between the list mutation and the count increment), but the module's
  internal bookkeeping must remain consistent: every completed registration must
  be counted exactly once.  A final count != completed-count is a data-structure
  desync (the runloom M:N bug).

ARMS:
  * LOAD-BEARING -- REGISTRATION-COUNT CONSERVATION (post, HARD).  Each fiber
    increments state["registrations_per_fiber"][wid] immediately after each
    register() call RETURNS, so the sum is the EXACT number of completed
    registrations (it tracks the deadline break naturally).  After all fibers
    finish and yield (drain phase), the final _ncallbacks() MUST equal that sum.
    A mismatch => data-structure corruption: the module's count desyncked from
    the registrations that actually happened (one lost, or one double-counted).

  * SECONDARY (report-ONLY, NEVER fails): the theoretical full-completion total
    (sum of wid%10+10 over all fibers).  Reported as a saturation indicator --
    a correctly time-sliced runtime registers FEWER than this when workers break
    at the deadline; that is EXPECTED, never a failure.

  * SECONDARY (report-ONLY, NEVER fails): intermediate counts during the run.
    The count may grow in any order (fibers schedule arbitrarily) and may have
    bursts when many fibers register concurrently.  We collect min/max/final for
    reporting.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-
    register() never returns; the watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing conservation hazard was actually
    exercised (completed registrations > 0, final != 0).

This is a pure correctness probe of the module-global atexit list consistency
under concurrent registration.  Each fiber runs a simple INNER loop:
`for _ in range(registrations_per_fiber): atexit.register(cb); yield()`.

Stresses: atexit module-global LIFO callback list under concurrent register()
calls from M:N fibers, _ncallbacks() count consistency, potential data-structure
corruption under preemption mid-mutation.

Good TSan / controlled-M:N-replay target: register() mutates the C-level
callback list; a data-race report on the internal list structure, or a
deterministic-replay that has one fiber preempted mid-register while another
registers concurrently, localizes the corruption before the count oracle fires.
"""
import atexit

import harness
import runloom


def make_callback(wid, seq):
    """Create a uniquely-tagged callback for this fiber/sequence pair."""
    def cb():
        pass
    cb._p481_tag = "cb-{0}-{1}".format(wid, seq)
    return cb


def setup(H):
    # Clear any stale callbacks from a previous run.
    atexit._clear()

    # Compute expected total registrations: each fiber wid registers
    # (wid % 10 + 10) callbacks.  Total = sum across all fibers.
    expected_total = 0
    for wid in range(H.funcs):
        count_for_wid = (wid % 10) + 10
        expected_total += count_for_wid

    # registrations_per_fiber is sized to H.funcs so EACH fiber owns a distinct
    # slot indexed by its raw wid (single-writer per slot -> race-free GIL-off).
    # This is the LOAD-BEARING ground truth now, so it must not lose increments
    # to aliased-slot races (the old `wid & 1023` mask aliased wids past 1024 and
    # would undercount under M:N -- harmless when the oracle compared against the
    # setup() sum, fatal now that it is the comparand).
    H.state = {
        "expected_total": expected_total,
        "registrations_per_fiber": [0] * max(1, H.funcs),
        "min_count": [None],  # minimum _ncallbacks() observed during run
        "max_count": [None],  # maximum _ncallbacks() observed during run
    }


def worker(H, wid, rng, state):
    """Each fiber registers a fixed, deterministic number of callbacks,
    yielding between registrations.  No unregistering, no checks inside the loop
    -- just pure registration."""

    # Each fiber wid registers (wid % 10 + 10) unique callbacks.
    count_for_wid = (wid % 10) + 10

    for seq in range(count_for_wid):
        if not H.running():
            break

        cb = make_callback(wid, seq)
        atexit.register(cb)
        # Count this completed registration in THIS fiber's OWN slot (single-
        # writer, race-free even GIL-off).  This sum is the load-bearing
        # comparand against final _ncallbacks(), so it must be exact.
        state["registrations_per_fiber"][wid] += 1

        # Sample the count to track min/max (optional secondary metric).
        c = atexit._ncallbacks()
        if state["min_count"][0] is None or c < state["min_count"][0]:
            state["min_count"][0] = c
        if state["max_count"][0] is None or c > state["max_count"][0]:
            state["max_count"][0] = c

        # Yield to allow siblings to run concurrently.
        if seq & 1:
            runloom.yield_now()
        else:
            runloom.sleep(0.0001)

        H.op(wid)

    H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # Clear the registry at the end so atexit doesn't try to fire any
    # remaining callbacks.
    final_count = atexit._ncallbacks()
    atexit._clear()

    # SECONDARY (report-only): the THEORETICAL full-completion total.  A
    # correctly time-sliced runtime registers FEWER than this when workers break
    # at the --duration deadline -- that is EXPECTED, never a failure.  It is a
    # saturation indicator only.
    theoretical_max = H.state["expected_total"]
    # LOAD-BEARING ground truth: the EXACT number of register() calls that
    # COMPLETED (incremented right after each register() returned, so it tracks
    # the deadline break naturally).
    actual_regs = sum(H.state["registrations_per_fiber"])
    min_c = H.state["min_count"][0] or 0
    max_c = H.state["max_count"][0] or 0

    saturation = (100.0 * actual_regs / theoretical_max) if theoretical_max else 0.0
    H.log("atexit registrations: completed={0} final_count={1} "
          "(theoretical_max={2}, saturation={3:.1f}% -- REPORT ONLY: workers "
          "break at the deadline, so < 100%% is EXPECTED) | min_observed={4} "
          "max_observed={5}".format(
              actual_regs, final_count, theoretical_max, saturation,
              min_c, max_c))

    # LOAD-BEARING oracle: final _ncallbacks() must equal the number of
    # register() calls that ACTUALLY COMPLETED (not the theoretical maximum --
    # workers legitimately break at the deadline).  A mismatch is a data-
    # structure corruption: a registration lost, or one double-counted.
    if final_count != actual_regs:
        H.fail("atexit CORRUPTION: final _ncallbacks()={0} != completed "
               "registrations {1} ({2:+d} delta) -- the module's callback count "
               "desyncked from the register() calls that actually returned, "
               "during concurrent registrations under M:N.  Each fiber counted "
               "every register() call that completed; the count should equal "
               "that sum exactly.  This indicates data-structure corruption in "
               "the atexit module under M:N (the runloom bug).".format(
                   final_count, actual_regs, final_count - actual_regs))

    # NON-VACUITY: the oracle was actually exercised.
    H.check(actual_regs > 0,
            "no register() calls completed -- the load-bearing conservation "
            "hazard was never exercised (oracle would be vacuous)")
    H.check(theoretical_max > 0,
            "no callbacks were scheduled -- the load-bearing conservation "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber stranded mid-register().
    H.require_no_lost("atexit registration")


if __name__ == "__main__":
    harness.main(
        "p481_atexit", body, setup=setup, post=post,
        default_funcs=8000,
        describe="atexit callbacks are stored in a module-global LIFO list "
                 "accessible via _ncallbacks(); concurrent register() calls "
                 "from M:N fibers must leave the count consistent with the "
                 "registrations that ACTUALLY COMPLETED.  LOAD-BEARING: each "
                 "fiber counts every register() call that returned (workers "
                 "legitimately break at the deadline, so this is < the "
                 "theoretical max -- that is EXPECTED, report-only).  After the "
                 "run, final _ncallbacks() MUST equal the completed-registration "
                 "count.  A mismatch is data-structure corruption under "
                 "concurrent registration (the M:N bug; fix is thread-safe "
                 "register() or per-fiber isolation in runloom, not in stdlib "
                 "atexit)")
