"""big_100 / 501 -- numbers ABC tower isinstance() answer integrity under M:N.

The `numbers` module defines the numeric tower as a chain of abc.ABCMeta virtual
base classes:

    Number > Complex > Real > Rational > Integral

Membership is decided by isinstance(x, numbers.Integral) etc.  That goes through
ABCMeta.__instancecheck__ -> the C `_abc` module's _abc_subclasscheck, which
consults per-ABC SHARED C state:

    * _abc_registry        -- a WeakSet of the types register()ed as virtual
                              subclasses of that ABC;
    * _abc_cache           -- a WeakSet caching POSITIVE subclass answers;
    * _abc_negative_cache  -- a WeakSet caching NEGATIVE answers, invalidated
                              wholesale whenever ANY ABC anywhere is register()ed
                              (a single global ABCMeta invalidation counter is
                              bumped, versioning every negative cache at once).

Every one of those structures is shared across ALL isinstance() callers of that
ABC.  With the GIL off and hubs>1, thousands of fibers on different hubs call
register() (mutating _abc_registry + bumping the global invalidation counter)
and isinstance() (reading/writing _abc_cache / _abc_negative_cache) against the
SAME five tower ABCs concurrently.  If a cache entry tears -- a positive answer
for one type leaking as the answer for a different type's query, a WeakSet
internal corruption, a stale negative-cache version surviving an invalidation,
or a torn registry walk -- isinstance() would return the WRONG virtual-subclass
answer.  That is genuine shared-C-state corruption, a real bug at any scale.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom gives each fiber
its own Python frame stack, but the `numbers` tower ABCs are process-global
objects and their _abc_* caches are shared C state.  A cross-hub isinstance()
that races a sibling's register()/isinstance() on the same ABC could observe a
half-updated cache and answer wrongly -- e.g. report a Real-only type as an
Integral, or fail to report a registered Integral as a Real.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against a closed-form truth).

  Each fiber builds its OWN FRESH class (a unique, never-shared `type` object)
  and registers it as a virtual subclass of the tower ABC at a chosen LEVEL L
  (0=Number .. 4=Integral).  Because the tower is a strict chain, the isinstance
  answer for that fresh class against tower level Q is a CLOSED-FORM CONSTANT:

        isinstance(inst, TOWER[Q])  ==  (Q <= L)

  i.e. a type registered at Integral (L=4) is an instance of every tower ABC; a
  type registered at Real (L=2) is a Number/Complex/Real but NOT a Rational or
  Integral.  We verified this truth vector directly (see the standalone check in
  the commit) on a correct runtime.  The class is UNIQUE per fiber, so its cache
  key (the type object identity) never aliases a sibling's -- a correct _abc
  cache can NEVER confuse two distinct types.  Therefore on ANY correct runtime
  (single-thread, plain threads GIL on AND off, runloom M:N) the answer equals
  the fixed truth vector.  If a fiber ever observes isinstance() disagree with
  (Q <= L) -- a False where the tower says True, or a True where it says False --
  that is real _abc-cache corruption under contention, LOAD-BEARING, exit 1.

  The instance also carries a UNIQUE per-fiber value and implements __int__ /
  __complex__.  int(inst) / complex(inst) must round-trip to that value across a
  yield (a single-owner object-value integrity check riding alongside the
  isinstance oracle): a torn instance would change value across the scheduling
  point.

ORACLES:
  * LOAD-BEARING -- TOWER MEMBERSHIP (worker, HARD, fail-fast).  Each fiber:
      - builds a fresh unique class, register()s it at level L on the shared
        tower ABC (mutating the shared _abc_registry + invalidation counter);
      - instantiates it with a unique per-fiber value;
      - asserts the FULL isinstance vector across all five tower ABCs equals the
        closed-form (Q <= L) truth, and that int()/complex() round-trip;
      - YIELDS (yield_now / sleep) so siblings register/isinstance on the same
        ABCs and churn the shared caches;
      - re-asserts the SAME isinstance vector + value round-trip + instance
        identity/type stability.  A mismatch is _abc-cache corruption.
    Single-owner: the class + instance are fiber-local, never shared.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (tower_checks>0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside a
    _abc_subclasscheck registry walk / WeakSet operation never returns; the
    watchdog + require_no_lost catch it.

FAIL ON: an isinstance() answer that disagrees with the fixed (Q <= L) tower
truth for a fiber's own uniquely-registered class, a value round-trip mismatch,
or an instance identity/type change across a yield.  There is NO shared-mutable
oracle here: the registered classes are all distinct type objects, so a correct
_abc cache is deterministic and a wrong answer is a genuine runtime fault, never
documented Python semantics.

Stresses: numbers ABC tower isinstance()/issubclass() under M:N, ABCMeta.register
mutating a shared _abc_registry WeakSet, the global ABC invalidation counter
bumped concurrently, _abc_cache / _abc_negative_cache positive+negative caching
racing register(), __int__/__complex__ value round-trip across hub migration.

Good TSan / controlled-M:N-replay target: the _abc module's WeakSet caches and
the shared invalidation counter are the exact shared-C-state a data-race report
would localize; a controlled replay that reads _abc_cache mid-register() by a
sibling, returning a wrong isinstance answer, is the cleanest signal before the
closed-form truth vector even fires.
"""
import numbers

import harness
import runloom

# The numeric tower, broad -> narrow.  Level index L: 0=Number .. 4=Integral.
# A type registered at level L satisfies isinstance(inst, TOWER[Q]) iff Q <= L
# (the tower is a strict single chain, so a virtual subclass at L propagates its
# membership UP to every broader ABC and to NONE of the narrower ones).
TOWER = [numbers.Number, numbers.Complex, numbers.Real,
         numbers.Rational, numbers.Integral]
TOWER_NAMES = ["Number", "Complex", "Real", "Rational", "Integral"]
NLEVELS = len(TOWER)

# Per-fiber value band: each wid gets a distinct base so the round-trip value
# differs visibly across fibers (a leaked sibling value would be off-band).
VALUE_SCALE = 100000


def make_number_class(wid, idx, level):
    """Build a FRESH, unique class registered as a virtual subclass of the tower
    ABC at `level`, plus its instance carrying a unique per-fiber value.

    The class is a plain object subclass (never shared) that implements __int__ /
    __complex__ so the value round-trip can ride alongside the isinstance oracle.
    Registration mutates the SHARED _abc_registry of TOWER[level] and bumps the
    global ABC invalidation counter -- the contended shared C state this program
    probes.  Returns (instance, value, expected_truth_vector)."""
    name = "FiberNum_W{0}_I{1}_L{2}".format(wid, idx, level)
    val = wid * VALUE_SCALE + (idx % VALUE_SCALE)

    def to_int(self):
        return self.val

    def to_complex(self):
        return complex(self.val)

    cls = type(name, (object,), {
        "__int__": to_int,
        "__complex__": to_complex,
    })
    # Register as a virtual subclass of the chosen tower ABC (shared mutation).
    TOWER[level].register(cls)

    inst = cls()
    inst.val = val

    # Closed-form truth: True iff the query ABC is at or above the register level.
    expected = tuple(Q <= level for Q in range(NLEVELS))
    return inst, val, expected


def check_vector(H, inst, expected, wid, level, phase):
    """Assert the FULL isinstance vector across the tower equals `expected`.
    Returns True on match; on any mismatch calls H.fail (LOAD-BEARING) and
    returns False."""
    for Q in range(NLEVELS):
        got = isinstance(inst, TOWER[Q])
        want = expected[Q]
        if got != want:
            H.fail("numbers tower isinstance WRONG ({0}): isinstance(inst, "
                   "numbers.{1}) == {2}, closed-form tower truth == {3} for a "
                   "class registered at level {4} ({5}) (wid {6}) -- a shared "
                   "_abc cache/registry corruption returned the wrong virtual-"
                   "subclass answer".format(
                       phase, TOWER_NAMES[Q], got, want, level,
                       TOWER_NAMES[level], wid))
            return False
    return True


# Sustained checks per worker, bounded by H.running().  The cache-corruption
# hazard only manifests under SUSTAINED churn -- many fibers simultaneously
# register()ing distinct classes and hammering isinstance() on the same five
# shared ABCs while sleep-PARKED across their yield, so a sibling's register()
# (which bumps the global invalidation counter and rewrites _abc caches)
# reliably interleaves before this fiber resumes.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    """LOAD-BEARING single-owner tower-membership check (fail-fast).

    Each iteration builds a fresh uniquely-registered class + instance, snapshots
    the closed-form isinstance vector and value round-trip, yields so siblings
    churn the shared _abc caches, then re-checks.  A wrong isinstance answer, a
    torn value, or an identity/type change is a real runtime bug."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            level = (wid + idx) % NLEVELS       # rotate register level for coverage
            inst, val, expected = make_number_class(wid, idx, level)
            inst_id = id(inst)
            inst_type = type(inst)

            # ---- snapshot BEFORE the yield -------------------------------
            if not check_vector(H, inst, expected, wid, level, "pre-yield"):
                return
            if int(inst) != val or complex(inst) != complex(val):
                H.fail("value round-trip WRONG (pre-yield): int(inst)={0} "
                       "complex(inst)={1}, expected {2}/{3} (wid {4}) -- torn "
                       "single-owner instance".format(
                           int(inst), complex(inst), val, complex(val), wid))
                return

            # ---- YIELD: let siblings register/isinstance on the same ABCs
            runloom.yield_now()
            if idx & 1:
                runloom.sleep(0.0003)

            # ---- re-check AFTER the yield --------------------------------
            if id(inst) != inst_id:
                H.fail("instance IDENTITY CHANGED across a yield: id {0} -> {1} "
                       "(wid {2}) -- the single-owner instance object was "
                       "replaced".format(inst_id, id(inst), wid))
                return
            if type(inst) is not inst_type:
                H.fail("instance TYPE CHANGED across a yield (wid {0}) -- the "
                       "single-owner instance's class was replaced".format(wid))
                return
            if not check_vector(H, inst, expected, wid, level, "post-yield"):
                return
            if int(inst) != val or complex(inst) != complex(val):
                H.fail("value round-trip WRONG (post-yield): int(inst)={0} "
                       "complex(inst)={1}, expected {2}/{3} (wid {4}) -- a "
                       "sibling corrupted this fiber's single-owner "
                       "instance".format(
                           int(inst), complex(inst), val, complex(val), wid))
                return

            state["tower_checks"][wid & 1023] += 1
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "tower_checks": [0] * 1024,      # LOAD-BEARING single-owner checks (sharded tally)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    tchecks = sum(H.state["tower_checks"])
    H.log("numbers tower[single-owner LOAD-BEARING]: {0} isinstance-vector "
          "checks (all passed fail-fast against the closed-form (Q<=L) truth); "
          "ops={1}".format(tchecks, H.total_ops()))

    # NON-VACUITY: the load-bearing tower-membership hazard was actually run.
    H.check(tchecks > 0,
            "no single-owner tower isinstance checks ran -- the load-bearing "
            "numbers-ABC-cache hazard was never exercised (oracle would be "
            "vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside a _abc_subclasscheck
    # registry walk / WeakSet operation.
    H.require_no_lost("numbers tower isinstance")


if __name__ == "__main__":
    harness.main(
        "p501_numbers_tower_isinstance", body, setup=setup, post=post,
        default_funcs=8000,
        describe="numbers defines the numeric tower (Number>Complex>Real>"
                 "Rational>Integral) as abc.ABCMeta virtual bases whose "
                 "_abc_registry / _abc_cache / _abc_negative_cache and the "
                 "global ABC invalidation counter are SHARED C state.  Under "
                 "M:N thousands of fibers register() distinct classes and call "
                 "isinstance() on the SAME five ABCs concurrently.  LOAD-"
                 "BEARING: each fiber registers its OWN fresh unique class at a "
                 "chosen tower level L and asserts the full isinstance vector "
                 "equals the closed-form (Q<=L) truth (plus an int()/complex() "
                 "value round-trip) across a yield.  Distinct type objects mean "
                 "a correct _abc cache is deterministic; a wrong isinstance "
                 "answer is genuine shared-cache corruption, not documented "
                 "semantics")
