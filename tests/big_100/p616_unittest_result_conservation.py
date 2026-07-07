"""big_100 / 616 -- unittest TestResult outcome-counting conservation under M:N.

unittest runs a TestSuite of TestCases against a TestResult and ACCUMULATES the
outcome of each test into that result object:  TestResult.startTest bumps an
integer testsRun counter (a read-modify-write, `self.testsRun += 1`), and
addFailure / addError / addSkip append a (testcase, formatted-traceback) tuple to
the failures / errors / skipped lists.  A TestSuite.run(result) walks its cases in
order and drives those mutators once per case.  The result object is therefore a
SINGLE-OWNER accumulator: for a fiber-local suite run against a fiber-local
TestResult, the final counts are a CLOSED-FORM function of the multiset of test
outcomes the fiber built -- nothing else may touch that result.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom drives thousands
of goroutines across >1 hub with the GIL off.  Each fiber builds its OWN suite of
synthetic FunctionTestCases with KNOWN outcomes (pass / fail / error / skip) and
runs it against its OWN TestResult.  Mid-run, inside the test bodies, the fiber
yields so a sibling -- also mid-run, also mutating ITS own result -- interleaves on
another hub.  If runloom leaked one fiber's result accumulation into another's (a
cross-fiber write to the single-owner result, a lost/doubled testsRun RMW that
belongs to THIS fiber alone, a torn list append, an outcome recorded against the
wrong result), the fiber's final counts would NOT match the closed-form expected
multiset it built.  On a correct runtime the single-owner result is touched by
exactly one fiber, so the counting law holds bit-exactly and the program exits 0.

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (verified against plain threads).  A
TestResult accumulated by ONE writer is a closed world: testsRun == number of
cases run, len(failures)==#fail, len(errors)==#error, len(skipped)==#skip,
wasSuccessful()==(#fail==0 and #error==0), and the pass count ==
testsRun-#fail-#error-#skip.  We confirmed with a plain-threads control (8 OS
threads, each building + running its own suite/result, GIL on AND off) that these
counts equal the closed-form expected 100% of the time -- 0 cross-thread bleed.
Under a correct runloom the same must hold.  A mismatch is a runloom single-owner-
isolation / lost-RMW bug, not documented unittest behavior.

ORACLES:
  * LOAD-BEARING -- RESULT CONSERVATION (worker, HARD, fail-fast).  Each fiber:
      - deterministically builds a KNOWN multiset of FunctionTestCase outcomes
        (pass/fail/error/skip) tied to (wid, iteration) -- fiber-local, never
        shared;
      - runs the suite against a FIBER-LOCAL TestResult; several test bodies call
        runloom.yield_now() so siblings interleave WHILE this result is mid-
        accumulation;
      - asserts the closed-form counting law on the final result (testsRun,
        failures, errors, skipped, wasSuccessful, derived pass-count) matches the
        multiset it built exactly;
      - SNAPSHOTS the four counts, YIELDS again, and re-reads them: a single-owner
        result is quiescent after run(), so the counts MUST be identical across the
        yield (a change is a cross-fiber write into this fiber's result).
    Single-owner: the suite, its cases, and the result are all fiber-local.

  * CONSERVATION tally (post, HARD): a per-wid race-free slot ([0]*H.funcs) sums
    every testsRun this fiber's results reported; post asserts it is > 0 (non-
    vacuity) -- the load-bearing arm actually ran.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside
    suite.run()/addError traceback formatting never returns; the watchdog +
    require_no_lost catch it.

  * SECONDARY (report-ONLY, NEVER fails): MEASURED lost-RMW on a SHARED
    TestResult.  All fibers run a tiny all-PASS suite against ONE shared
    TestResult; the shared testsRun is a contended `+= 1` (documented non-atomic
    RMW, exactly like a shared `x += 1` across threads GIL-off).  We know the
    global number of shared tests offered; post compares it to shared.testsRun and
    REPORTS any shortfall (lost increments).  All-PASS keeps the shared result's
    lists empty (list.append is made thread-safe in 3.14t; the pure int RMW is the
    hazard), so this arm demonstrates the race EXISTS without risking a crash and
    NEVER reaches the single-owner oracle.  We never H.fail on it -- doing so would
    mislabel documented shared-object semantics as a runloom bug.

FAIL ON: a fiber-local TestResult whose final counts differ from the closed-form
expected multiset, or whose counts change across a post-run yield (a cross-fiber
write into a single-owner result), or a SIGSEGV mid suite.run().  The shared
MEASURED arm is report-only and is EXPECTED to show lost increments (documented
shared-RMW behavior) -- the load-bearing single-owner oracle must stay clean.

Stresses: unittest.TestSuite.run over FunctionTestCase, TestResult.startTest
testsRun RMW + addFailure/addError/addSkip list appends + _exc_info_to_string
traceback formatting, SkipTest / AssertionError / generic-exception routing,
single-owner result isolation vs shared-result lost-increment across hub migration
and a mid-run yield under M:N concurrency.
"""
import unittest

import harness
import runloom

# Outcome kinds a synthetic test body can produce.  FunctionTestCase routes each:
# a clean return -> pass (no list, just testsRun); AssertionError -> failures;
# any other exception -> errors; unittest.SkipTest -> skipped.
KIND_PASS = 0
KIND_FAIL = 1
KIND_ERROR = 2
KIND_SKIP = 3
NKINDS = 4

# Cases per fiber-local suite.  Big enough that the four outcome buckets are all
# populated and the result accumulates through several list-append + testsRun RMW
# steps with yields interleaved; small enough that thousands of fibers each build
# and run a suite under the timeout.
CASES_MIN = 8
CASES_MAX = 20

# Sustained runs per worker -- the isolation hazard only manifests under SUSTAINED
# churn (many fibers simultaneously mid-run, PARKED across an in-test yield so a
# sibling reliably interleaves before this fiber resumes accumulating its result).
INNER_CAP = 100000

# Shared-arm: each fiber offers this many all-PASS tests to the ONE shared
# TestResult (MEASURED lost-RMW demonstration; report-only).
SHARED_TESTS_PER_FIBER = 4


def make_test_func(kind, do_yield):
    """Build a synthetic zero-arg test body with a KNOWN outcome.

    If do_yield, the body yields at its start so that WHILE this fiber's suite is
    mid-accumulation (its TestResult partially built), a sibling fiber -- also
    mid-run against ITS own result -- interleaves on another hub.  A single-owner
    result must survive that interleave untouched."""
    def tf():
        if do_yield:
            runloom.yield_now()
        if kind == KIND_PASS:
            return
        if kind == KIND_FAIL:
            raise AssertionError("synthetic expected failure")
        if kind == KIND_ERROR:
            raise ValueError("synthetic expected error")
        # KIND_SKIP
        raise unittest.SkipTest("synthetic expected skip")
    return tf


def build_local_suite(rng):
    """Build one fiber-local suite of FunctionTestCases with a KNOWN outcome
    multiset.  Returns (suite, expected) where expected is a dict of the exact
    closed-form counts the run MUST produce."""
    ncases = rng.randint(CASES_MIN, CASES_MAX)
    suite = unittest.TestSuite()
    npass = nfail = nerror = nskip = 0
    for i in range(ncases):
        kind = rng.randrange(NKINDS)
        # Yield in a fraction of the bodies so the interleave is dense but the run
        # still finishes quickly.  Every kind is equally eligible to yield.
        do_yield = (i % 3 == 0)
        suite.addTest(unittest.FunctionTestCase(make_test_func(kind, do_yield)))
        if kind == KIND_PASS:
            npass += 1
        elif kind == KIND_FAIL:
            nfail += 1
        elif kind == KIND_ERROR:
            nerror += 1
        else:
            nskip += 1
    expected = {
        "run": ncases,
        "pass": npass,
        "fail": nfail,
        "error": nerror,
        "skip": nskip,
    }
    return suite, expected


# ---- LOAD-BEARING arm: single-owner fiber-local suite + result -----------
def result_check(H, wid, rng, state):
    """Single-owner unittest result-conservation check.

    Build a fiber-local suite with a KNOWN outcome multiset, run it against a
    fiber-local TestResult across in-run yields, and assert the closed-form
    counting law holds -- and stays stable across a post-run yield."""
    suite, exp = build_local_suite(rng)
    result = unittest.TestResult()

    # Drive the whole suite against this fiber's OWN result.  Test bodies yield
    # mid-run so siblings interleave while this result is partially accumulated.
    suite.run(result)

    got_run = result.testsRun
    got_fail = len(result.failures)
    got_error = len(result.errors)
    got_skip = len(result.skipped)

    # ---- closed-form counting law (single owner, fully quiescent now) --------
    if got_run != exp["run"]:
        H.fail("testsRun WRONG: result.testsRun={0}, expected {1} cases (wid {2}) "
               "-- a testsRun RMW was lost/doubled or a sibling's run leaked into "
               "this fiber's single-owner TestResult".format(
                   got_run, exp["run"], wid))
        return
    if got_fail != exp["fail"]:
        H.fail("failures WRONG: len(result.failures)={0}, expected {1} (wid {2}) "
               "-- an AssertionError was mis-routed or a cross-fiber failure leaked "
               "into this fiber's result".format(got_fail, exp["fail"], wid))
        return
    if got_error != exp["error"]:
        H.fail("errors WRONG: len(result.errors)={0}, expected {1} (wid {2}) -- a "
               "generic-exception outcome was mis-routed or a cross-fiber error "
               "leaked into this fiber's result".format(got_error, exp["error"], wid))
        return
    if got_skip != exp["skip"]:
        H.fail("skipped WRONG: len(result.skipped)={0}, expected {1} (wid {2}) -- a "
               "SkipTest outcome was mis-routed or a cross-fiber skip leaked into "
               "this fiber's result".format(got_skip, exp["skip"], wid))
        return

    # Derived pass-count law: passes leave no list entry, so the only witness is
    # testsRun minus the three recorded buckets.  Must equal the built pass count.
    derived_pass = got_run - got_fail - got_error - got_skip
    if derived_pass != exp["pass"]:
        H.fail("pass-count WRONG: testsRun-fail-error-skip={0}, expected {1} passes "
               "(wid {2}) -- the outcome buckets do not conserve to the total "
               "run".format(derived_pass, exp["pass"], wid))
        return

    # wasSuccessful() must agree with the recorded buckets.
    want_ok = (exp["fail"] == 0 and exp["error"] == 0)
    if result.wasSuccessful() != want_ok:
        H.fail("wasSuccessful()={0} disagrees with buckets (fail={1} error={2}, "
               "expected ok={3}) (wid {4}) -- the result's success predicate is "
               "inconsistent with its own counts".format(
                   result.wasSuccessful(), got_fail, got_error, want_ok, wid))
        return

    # SNAPSHOT + YIELD + re-read: a single-owner result is quiescent after run(),
    # so the counts MUST be identical across a yield.  Any change is a cross-fiber
    # write into this fiber's result.
    runloom.yield_now()
    if (result.testsRun != got_run or len(result.failures) != got_fail
            or len(result.errors) != got_error
            or len(result.skipped) != got_skip):
        H.fail("result MUTATED across a post-run yield (wid {0}): run {1}->{2} "
               "fail {3}->{4} error {5}->{6} skip {7}->{8} -- a sibling wrote into "
               "this fiber's single-owner quiescent TestResult".format(
                   wid, got_run, result.testsRun, got_fail, len(result.failures),
                   got_error, len(result.errors), got_skip, len(result.skipped)))
        return

    # CONSERVATION tally (race-free per-wid slot: one writer).
    state["tests_run"][wid] += got_run


# ---- MEASURED arm: shared TestResult lost-RMW (report-only) --------------
def shared_result_check(H, wid, state):
    """Run a tiny all-PASS suite against the ONE shared TestResult (MEASURED,
    report-only).  The shared testsRun is a contended `+= 1` -- the documented
    non-atomic RMW that loses increments GIL-off, exactly like a shared `x += 1`
    across threads.  All-PASS keeps the shared result's lists empty (list.append
    is thread-safe in 3.14t; only the int RMW is the hazard), so this arm shows
    the race EXISTS without risk of a crash and NEVER reaches the single-owner
    oracle.  We count how many we offered (race-free per-wid slot) and NEVER
    fail."""
    shared = state["shared_result"]
    suite = unittest.TestSuite()
    for _ in range(SHARED_TESTS_PER_FIBER):
        suite.addTest(unittest.FunctionTestCase(make_test_func(KIND_PASS, True)))
    suite.run(shared)
    state["shared_offered"][wid] += SHARED_TESTS_PER_FIBER


def worker(H, wid, rng, state):
    """Each fiber runs BOTH arms per iteration: the LOAD-BEARING single-owner
    suite/result conservation check (fail-fast) and the MEASURED shared-result
    lost-RMW check (report only).  They share no state (fiber-local result vs the
    shared result) so the shared contention never reaches the single-owner
    oracle."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            result_check(H, wid, rng, state)          # LOAD-BEARING (fail-fast)
            if H.failed:
                return
            shared_result_check(H, wid, state)        # MEASURED (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "tests_run": [0] * H.funcs,        # LOAD-BEARING single-owner testsRun sum
        "shared_result": unittest.TestResult(),   # MEASURED shared accumulator
        "shared_offered": [0] * H.funcs,   # units offered to the shared result
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    truns = sum(H.state["tests_run"])
    offered = sum(H.state["shared_offered"])
    got = H.state["shared_result"].testsRun
    lost = offered - got
    lpct = (100.0 * lost / offered) if offered else 0.0

    H.log("unittest[single-owner LOAD-BEARING]: {0} tests conserved across fiber-"
          "local TestResults (every closed-form counting law + post-run-yield "
          "stability check passed fail-fast) | unittest[shared result MEASURED]: "
          "offered {1} lost {2} ({3:.1f}%, documented shared-RMW behavior -- "
          "REPORT ONLY)".format(truns, offered, lost, lpct))

    if lost:
        H.log("note: the SHARED TestResult lost {0} of {1} testsRun increments -- "
              "the shared `self.testsRun += 1` is a non-atomic RMW on a shared "
              "object (documented, like a shared x+=1 across threads GIL-off).  "
              "This is NOT a runloom bug and never reaches the load-bearing single-"
              "owner oracle.".format(lost, offered))

    # NON-VACUITY: the load-bearing single-owner arm actually ran.
    H.check(truns > 0,
            "no single-owner unittest result-conservation checks ran -- the load-"
            "bearing counting-law hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside suite.run()
    # or addError traceback formatting).
    H.require_no_lost("unittest result conservation")


if __name__ == "__main__":
    harness.main(
        "p616_unittest_result_conservation", body, setup=setup, post=post,
        default_funcs=6000,
        describe="each fiber builds a fiber-local unittest.TestSuite of "
                 "FunctionTestCases with a KNOWN outcome multiset (pass/fail/error/"
                 "skip) and runs it against a fiber-local TestResult across in-run "
                 "yields.  LOAD-BEARING closed-world counting law: testsRun, "
                 "failures, errors, skipped, wasSuccessful, and the derived pass-"
                 "count MUST match the built multiset exactly, and stay stable "
                 "across a post-run yield (a single-owner quiescent result may not "
                 "change).  MEASURED shared-result arm (expected to lose testsRun "
                 "RMW increments, like a shared x+=1) proves the hazard exists.  A "
                 "count that mismatches the closed form or mutates across the yield "
                 "is the runloom single-owner-isolation / lost-RMW bug")
