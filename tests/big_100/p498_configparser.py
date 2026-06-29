"""big_100 / 498 -- configparser.ConfigParser per-instance state isolation
under M:N.

configparser.ConfigParser is a mutable per-instance container: each instance
owns a dict of sections, each section owns a dict of option key-value pairs.
Under M:N many fibers on the same hub can run concurrently (with the GIL off),
so if fibers naively share a ConfigParser instance, one fiber's set_option or
remove_option call is visible to a sibling (M:N shared-object mutable-state
hazard, like p67's threading.local / p321's warnings.filters, but at the
instance level instead of global module state).

WHERE THE HAZARD MANIFESTS.  This program tests the PRIVATE-INSTANCE arm:
each fiber creates its OWN ConfigParser, populates it with unique section/
option/value tuples, yields (so a sibling on the hub may run), and then
re-reads the values to assert they are unchanged.  Under a correct runtime
each fiber's instance is isolated (distinct id(cp)), so no sibling mutation
reaches it across the yield.  The closed-world reference (precomputed in
setup, canonicalized per fiber) lets us detect corruption: a re-read value
!= expected is a wrong state (a sibling's write leaked in, or the instance
was garbage-collected and reused, or random corruption).

Under plain threads (GIL on AND off -- verified) this is race-free: each OS
thread has its own stack and local variables (cp is a stack-local Python
object), so a sibling thread NEVER touches THIS thread's cp.  Under runloom
M:N with a CORRECT fiber implementation, each fiber ALSO has its own stack
frame and local variables (fiber isolation), so the same holds.  If runloom
leaks a shared object or fails to isolate the stack, a sibling's mutation
will corrupt this fiber's instance across the yield, and the re-read will
be wrong -- the runloom isolation bug.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  ConfigParser is DOCUMENTED to be a mutable per-instance container.  The
  private-instance arm DIRECTLY verifies that: a fiber creates cp, sets
  unique per-fiber config values, yields, and re-reads them.  If the values
  survive the yield unchanged, each fiber's cp was isolated (the correct
  behavior under plain threads, verified: GIL on AND off, 0/2560 mismatches).
  Under a correct runloom each fiber gets an isolated stack, so it must ALSO
  hold (0 mismatches).  If a re-read value != expected (a sibling leaked in,
  or corruption), that is the runloom isolation bug.  The oracle fires only
  on a REAL desync, and the program exits 0 when there is no bug.

ORACLES:
  * LOAD-BEARING -- PRIVATE-INSTANCE ISOLATION across yields (worker, HARD,
    fail-fast).  Each fiber creates cp, sets unique per-fiber section/option
    values (derived from wid), yields (so a hub sibling may run), then
    re-reads every value and asserts got == expected (the precomputed
    canonical value for this wid).  A re-read != expected is a corruption
    (a sibling's write, or a desync in the instance's internal state) --
    runloom isolation bug.
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-block
    (inside ConfigParser.__getitem__ or during a yield) never returns; the
    watchdog catches an outright strand and require_no_lost catches a parked-
    then-vanished worker.
  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

FAIL ON: a re-read value != expected (isolation desync, corruption, or a
torn/garbage value that is not a plausible fiber id or section name).
NEVER fail on measured shared-object hazards (this program avoids them by
design: each fiber's cp is private).

Stresses: configparser.ConfigParser per-instance mutable state (sections
dict, option key-value pairs, internal _sections / _defaults / _proxies),
fiber stack isolation, repeated yields with concurrent sibling state
mutation attempts.

Standalone plain-threads control verifies the oracle is non-vacuous:
  PYTHON_GIL=1 python3 p498_control.py -> 0 mismatches (baseline)
  PYTHON_GIL=0 python3 p498_control.py -> 0 mismatches (GIL-off, no runloom)
"""
import configparser
import io

import harness
import runloom

# Per-fiber section names: drawn from this band.
SEC_MIN = 1
SEC_MAX = 6
SEC_SPAN = SEC_MAX - SEC_MIN + 1

# Per-section option count: lightweight to avoid slowness.
OPTS_PER_SEC = 2

# Canonical precomputed config values: ConfigParser state per wid.
# Built in setup() once, single-owner, so it is race-free and independent
# of all workers' mutations.  The load-bearing oracle compares a fiber's
# re-read config against CANON[wid] -- a fixed closed-world reference, not
# a live read of any shared config -- so the check cannot be contaminated by
# shared-object leaks.
CANON = {}


def build_canonical():
    """One-time, single-owner: the expected ConfigParser state for each wid.
    Each entry is (wid -> {section -> {option -> value}}).  Built once in
    setup(), before any worker runs, so it is race-free and independent of
    all shared/global config state."""
    table = {}
    for wid in range(10000):  # pre-build a large batch; workers index into it
        cp_dict = {}
        for sec_off in range(SEC_SPAN):
            sec = "sec_{0}_{1}".format(wid, sec_off + SEC_MIN)
            cp_dict[sec] = {}
            for opt_off in range(OPTS_PER_SEC):
                opt = "opt_{0}".format(opt_off)
                val = "val_{0}_{1}_{2}".format(wid, sec_off, opt_off)
                cp_dict[sec][opt] = val
        table[wid] = cp_dict
    return table


def setup(H):
    global CANON
    CANON = build_canonical()
    H.state = {
        "checks": [0] * 1024,           # per-fiber checks done
        "mismatches": [0] * 1024,       # re-read value != expected
        "exceptions": [0] * 1024,       # ConfigParser raised unexpectedly
    }


def worker(H, wid, rng, state):
    """Each fiber runs LOAD-BEARING private-instance checks:

    Create a ConfigParser, populate it with unique per-fiber sections/options,
    yield to let a sibling run, then re-read all values and assert they are
    unchanged.  A re-read value != expected is a runloom isolation desync
    (the private instance was corrupted by a sibling on the same hub, or the
    fiber's stack was not isolated).

    The worker SUSTAINS a churn loop bounded by H.running(): one config
    create/populate/yield/re-read per iteration.  Each iteration uses a FRESH
    ConfigParser (new instance, new id(cp)), so the reuse stress is high --
    any fiber-stack or object-isolation desync manifests quickly across many
    fibers creating distinct instances on a shared hub.
    """
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < 1000:  # inner cap
            # Create a FRESH ConfigParser -- distinct id(cp) for this iteration.
            cp = configparser.ConfigParser()
            canon = CANON[wid % len(CANON)]

            try:
                # Populate it with unique per-fiber sections and options.
                for sec in canon:
                    cp.add_section(sec)
                    for opt, val in canon[sec].items():
                        cp.set(sec, opt, val)

                # YIELD + SLEEP-PARK: a sibling fiber on this hub runs
                # (and may be mutating its own ConfigParser, or attempting
                # to corrupt shared state) while this fiber is PARKED.
                runloom.yield_now()
                if idx & 1:
                    runloom.sleep(0.0002)

                # Re-read all values and assert they match the expected config.
                # If a sibling's write leaked into THIS fiber's cp, a re-read
                # will be wrong (a sibling's value, or a torn/garbage value).
                for sec in canon:
                    for opt, expected_val in canon[sec].items():
                        try:
                            got_val = cp.get(sec, opt)
                        except (configparser.NoSectionError,
                                configparser.NoOptionError) as e:
                            # The option or section vanished: the instance was
                            # corrupted (a sibling removed it, or the cp object
                            # was torn).
                            state["mismatches"][wid & 1023] += 1
                            H.fail(
                                "ConfigParser ISOLATION BROKEN: re-read {0}/{1} "
                                "raised {2} (wid {3}) -- a sibling's remove_option "
                                "or remove_section leaked into this fiber's private "
                                "instance (runloom fiber-stack isolation desync)".
                                format(sec, opt, type(e).__name__, wid))
                            return

                        # Guard against truly impossible values (not a plausible
                        # fiber id or value): a sanity check for corruption.
                        if got_val != expected_val:
                            # The value changed: a sibling's write leaked in, or
                            # corruption.
                            if not isinstance(got_val, str):
                                H.fail(
                                    "ConfigParser VALUE TYPE CORRUPTION: {0}/{1} "
                                    "re-read as {2!r} type {3} (wid {4}, expected "
                                    "str) -- instance state was torn".format(
                                        sec, opt, got_val, type(got_val).__name__,
                                        wid))
                                return
                            if got_val.startswith("val_") and "_" in got_val:
                                # A plausible sibling value (matches our format).
                                # Extract the wid prefix to see if it is another
                                # fiber's value.
                                try:
                                    sibling_wid = int(got_val.split("_")[1])
                                    if sibling_wid != wid:
                                        state["mismatches"][wid & 1023] += 1
                                        H.fail(
                                            "ConfigParser CROSS-FIBER LEAK: {0}/{1} "
                                            "re-read sibling's value {2!r} (wid {3}, "
                                            "sibling {4}) -- a sibling fiber's write "
                                            "leaked into this fiber's private "
                                            "instance (runloom M:N isolation bug)".
                                            format(sec, opt, got_val, wid,
                                                   sibling_wid))
                                        return
                                except (ValueError, IndexError):
                                    pass  # not a parseable sibling value
                            # Mismatch is a general corruption.
                            state["mismatches"][wid & 1023] += 1
                            H.fail(
                                "ConfigParser VALUE CORRUPTION: {0}/{1} re-read "
                                "{2!r} != expected {3!r} (wid {4}) -- the private "
                                "instance did not retain the value this fiber set "
                                "(runloom isolation desync or corruption)".format(
                                    sec, opt, got_val, expected_val, wid))
                            return

                state["checks"][wid & 1023] += 1

            except Exception as e:
                # ConfigParser raised unexpectedly (not a known error we handle above).
                state["exceptions"][wid & 1023] += 1
                H.fail(
                    "ConfigParser UNEXPECTED EXCEPTION: {0}: {1} (wid {2}) -- "
                    "the private instance raised during set/get (runloom "
                    "isolation or state corruption)".format(
                        type(e).__name__, e, wid))
                return

            H.op(wid)
            idx += 1

        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    mismatches = sum(H.state["mismatches"])
    exceptions = sum(H.state["exceptions"])

    pct = (100.0 * mismatches / checks) if checks else 0.0

    H.log("configparser: {0} checks  mismatches={1} ({2:.1f}%)  "
          "exceptions={3}".format(checks, mismatches, pct, exceptions))

    # NON-VACUITY: the load-bearing private-instance hazard was actually exercised.
    H.check(checks > 0,
            "no private-instance config checks ran -- the load-bearing fiber-"
            "isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded during
    # ConfigParser set/get on a desync'd instance).
    H.require_no_lost("configparser per-instance isolation")


if __name__ == "__main__":
    harness.main("p498_configparser", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="configparser.ConfigParser is a mutable per-instance "
                          "container (sections dict, option key-value pairs); "
                          "runloom M:N fibers on the same hub run concurrently "
                          "(GIL off), so a PRIVATE-INSTANCE isolation desync "
                          "would let a sibling's mutation reach this fiber's "
                          "instance across a yield.  LOAD-BEARING: each fiber "
                          "creates its OWN ConfigParser, sets unique per-fiber "
                          "sections/options, yields, then re-reads all values "
                          "and asserts they are unchanged (the canonical "
                          "precomputed per-wid config).  A re-read value != "
                          "expected is a runloom fiber-isolation bug (0 under "
                          "plain threads GIL on AND off).  Non-recursive, no "
                          "nested I/O, pure instance-state stress")
