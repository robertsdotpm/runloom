"""big_100 / 519 -- resource.getrusage/getrlimit monotone + identity under M:N.

The `resource` module wraps the POSIX getrusage(2)/getrlimit(2) syscalls.
resource.getrusage(RUSAGE_SELF) returns a fresh `struct_rusage` object built
FIELD BY FIELD in C: the kernel fills a `struct rusage`, then the C wrapper
(_PyStructSequence / the module's struct_rusage_new) copies ru_utime, ru_stime,
ru_maxrss, ... one at a time into a brand-new sequence object.  getrlimit()
likewise assembles a fresh (soft, hard) 2-tuple from a `struct rlimit`.

WHERE M:N COULD BREAK IT (the gap this program probes).  Each call returns a
BRAND-NEW object owned by exactly the one fiber that called getrusage/getrlimit
-- there is no shared container, so this is a pure SINGLE-OWNER oracle.  The
danger under runloom is not shared-state contention but a TORN ASSEMBLY across
a hub migration: if a fiber is preempted / migrated to another hub while the C
wrapper is still copying ru_utime then ru_stime into the fresh struct (or while
the tuple for getrlimit is half-built), a resumed-on-a-different-hub read could
observe a struct whose ru_utime came from one syscall snapshot and ru_stime from
another -- or a non-numeric / garbage field where a live pointer was mid-store.
The visible symptom of such a tear is a VALUE that violates a physical law:
process CPU time going BACKWARDS, or the (constant) RLIMIT_NOFILE tuple changing.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Two physical laws hold for a single process, independent of the GIL, verified
  against plain threads (getrusage from N OS threads never reports CPU going
  backwards, getrlimit is constant):

    (1) MONOTONE CPU.  resource.getrusage(RUSAGE_SELF).ru_utime + .ru_stime is
        the CUMULATIVE user+system CPU consumed by the whole process (summed
        across all threads/hubs).  It is a monotonically NON-DECREASING quantity
        -- consumed CPU can only rise.  A fiber that reads the sum, YIELDS (lets
        siblings burn CPU on other hubs), then reads it again MUST observe a sum
        that is >= the first.  A DECREASE can only come from a torn/garbage read
        of ru_utime or ru_stime -- there is no legitimate way for process CPU to
        go backwards.  This is an ORDERING law across the fiber's own yield.

    (2) CONSTANT RLIMIT.  resource.getrlimit(RLIMIT_NOFILE) returns the process's
        (soft, hard) fd limit.  Nothing in this test calls setrlimit, so the
        tuple is a CONSTANT for the lifetime of the run.  A fiber captures it in
        setup (the baseline) and, across every yield, re-reads it and asserts it
        is byte-identical to the baseline.  A CHANGE is a torn tuple assembly
        (half-built (soft, hard)) -- an IDENTITY-VALUE law.

  Both are SINGLE-OWNER: every getrusage/getrlimit call returns a private fresh
  object touched only by the calling fiber.  Nothing is shared, so a failure
  cannot be documented shared-object M:N semantics -- it can ONLY be a torn
  struct assembled across a hub migration (or a real syscall-wrapper bug).  On a
  CORRECT runtime this program exits 0 (both laws always hold).

ORACLES:
  * LOAD-BEARING -- MONOTONE + IDENTITY (worker, HARD, fail-fast).  Per inner
    iteration each fiber:
      - reads ru = getrusage(RUSAGE_SELF); captures cpu_before = ru_utime+ru_stime
        and asserts both fields are real numbers (float);
      - reads rl_before = getrlimit(RLIMIT_NOFILE);
      - YIELDS (yield_now, plus a tiny sleep on odd iters) so siblings on other
        hubs burn CPU and the scheduler may migrate this fiber before it resumes;
      - re-reads ru2/rl_after; asserts cpu_after >= cpu_before (MONOTONE), each
        field numeric, and rl_after == rl_before == baseline (CONSTANT).
    A failure is a torn struct_rusage / rlimit tuple across the migration.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-syscall-
    wrapper (parked and never resumed) never returns; the watchdog catches it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

FAIL ON: process CPU time (ru_utime+ru_stime) DECREASING across a fiber's own
yield, a non-numeric ru_utime/ru_stime field, or RLIMIT_NOFILE changing from the
constant baseline.  There is NO shared/report-only arm -- every read is single-
owner, so there is no documented-shared-semantics hazard to measure; any
violation is a torn syscall-wrapper read across a hub migration.

Stresses: resource.getrusage(RUSAGE_SELF) fresh-struct_rusage assembly across a
yield/hub-migration, resource.getrlimit(RLIMIT_NOFILE) fresh-tuple assembly,
monotone process-CPU ordering + constant-rlimit identity under M:N, syscall
wrapper _PyStructSequence field-by-field copy racing a preemption boundary.

Good TSan / controlled-M:N-replay target: the C wrapper's field-by-field store
into the fresh struct_rusage is a natural preemption-boundary; a replay that
migrates the fiber mid-assembly and then reads a decreased CPU sum localizes the
tear before the ordering law even closes.  resource is otherwise UNCOVERED in
the suite (p98 is a refcount fuzzer, not a value/ordering oracle).
"""
import resource

import harness
import runloom

# Sustained reads per worker, bounded by H.running().  The tear hazard only
# manifests under SUSTAINED churn -- many fibers simultaneously calling getrusage
# while sleep-PARKED across their yield, so the scheduler reliably migrates a
# sibling's half-built struct read before this fiber resumes.  A single read per
# fiber barely straddles a preemption boundary and does NOT reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """Single-owner MONOTONE + IDENTITY oracle over resource.getrusage/getrlimit.

    Each getrusage/getrlimit call returns a PRIVATE fresh object; the fiber reads
    it, yields (siblings burn CPU / may migrate this fiber), re-reads, and asserts
    process CPU is non-decreasing and RLIMIT_NOFILE is unchanged.  A violation is
    a struct torn across a hub migration."""
    checks = state["checks"]
    baseline_rl = state["baseline_rl"]
    RUSAGE_SELF = resource.RUSAGE_SELF
    RLIMIT_NOFILE = resource.RLIMIT_NOFILE

    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            # ---- read BEFORE the yield (private fresh objects) --------------
            ru_before = resource.getrusage(RUSAGE_SELF)
            ut_b = ru_before.ru_utime
            st_b = ru_before.ru_stime

            # Every field must be a real number.  A non-numeric field is a torn
            # store where a live pointer was caught mid-assembly.
            if not isinstance(ut_b, float) or not isinstance(st_b, float):
                H.fail("getrusage(RUSAGE_SELF) returned NON-NUMERIC CPU field "
                       "before yield: ru_utime={0!r} ru_stime={1!r} (wid {2}) -- "
                       "a torn struct_rusage assembly across a hub migration".format(
                           ut_b, st_b, wid))
                return
            cpu_before = ut_b + st_b

            rl_before = resource.getrlimit(RLIMIT_NOFILE)

            # ---- YIELD: siblings burn CPU on other hubs; may migrate us -----
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0002)

            # ---- read AFTER the yield --------------------------------------
            ru_after = resource.getrusage(RUSAGE_SELF)
            ut_a = ru_after.ru_utime
            st_a = ru_after.ru_stime
            if not isinstance(ut_a, float) or not isinstance(st_a, float):
                H.fail("getrusage(RUSAGE_SELF) returned NON-NUMERIC CPU field "
                       "after yield: ru_utime={0!r} ru_stime={1!r} (wid {2}) -- "
                       "a torn struct_rusage assembly across a hub migration".format(
                           ut_a, st_a, wid))
                return
            cpu_after = ut_a + st_a

            # LAW (1) MONOTONE: process CPU can only rise; a decrease across this
            # fiber's own yield is a torn/garbage read of ru_utime or ru_stime.
            if cpu_after < cpu_before:
                H.fail("process CPU went BACKWARDS across a yield: "
                       "ru_utime+ru_stime {0!r} -> {1!r} (delta {2!r}) for wid {3} "
                       "(before: ut={4!r} st={5!r}; after: ut={6!r} st={7!r}) -- "
                       "cumulative process CPU is monotone non-decreasing; a "
                       "decrease is a torn struct_rusage read across a hub "
                       "migration".format(
                           cpu_before, cpu_after, cpu_after - cpu_before, wid,
                           ut_b, st_b, ut_a, st_a))
                return

            # LAW (2) CONSTANT RLIMIT: nothing calls setrlimit, so the (soft,hard)
            # NOFILE tuple is invariant.  A change is a torn tuple assembly.
            rl_after = resource.getrlimit(RLIMIT_NOFILE)
            if rl_after != rl_before or rl_after != baseline_rl:
                H.fail("getrlimit(RLIMIT_NOFILE) CHANGED across a yield: "
                       "before={0!r} after={1!r} baseline={2!r} (wid {3}) -- no "
                       "setrlimit is called, so the fd limit is constant; a change "
                       "is a torn (soft, hard) tuple assembled across a hub "
                       "migration".format(rl_before, rl_after, baseline_rl, wid))
                return

            checks[wid & 1023] += 1        # sharded NON-VACUITY tally (report only)
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    # Capture the CONSTANT RLIMIT_NOFILE baseline once, in the root, before any
    # fiber runs.  Every fiber asserts getrlimit() stays byte-identical to this.
    H.state = {
        "checks": [0] * 1024,                       # NON-VACUITY tally (sharded)
        "baseline_rl": resource.getrlimit(resource.RLIMIT_NOFILE),
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    H.log("resource[single-owner LOAD-BEARING]: {0} getrusage-monotone + "
          "getrlimit-identity checks (all passed fail-fast); baseline "
          "RLIMIT_NOFILE={1!r}; ops={2}".format(
              checks, H.state["baseline_rl"], H.total_ops()))

    # NON-VACUITY: the load-bearing arm actually exercised the tear hazard.
    H.check(checks > 0,
            "no getrusage/getrlimit checks ran -- the torn-struct-across-migration "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the
    # getrusage/getrlimit C wrapper).
    H.require_no_lost("resource getrusage monotone")


if __name__ == "__main__":
    harness.main(
        "p519_resource_getrusage_monotone", body, setup=setup, post=post,
        default_funcs=6000,
        describe="resource.getrusage(RUSAGE_SELF) returns a fresh struct_rusage "
                 "assembled field-by-field in C; getrlimit(RLIMIT_NOFILE) a fresh "
                 "(soft,hard) tuple.  Each call is SINGLE-OWNER (private fresh "
                 "object).  LOAD-BEARING: a fiber reads ru_utime+ru_stime and the "
                 "NOFILE limit, yields (siblings burn CPU / may migrate it), then "
                 "re-reads: process CPU MUST be non-decreasing (monotone ordering "
                 "law) and RLIMIT_NOFILE MUST be unchanged (constant identity law). "
                 "A CPU decrease or a changed rlimit is a torn syscall-wrapper "
                 "struct assembled across a hub migration")
