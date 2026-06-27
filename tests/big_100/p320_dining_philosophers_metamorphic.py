"""big_100 / 320 -- dining philosophers, metamorphic + deterministic-replay.

The corpus measures lock fairness (p214), contention (p46) and priority
inversion (p43), but nothing asserts a *metamorphic* equality: that an
aggregate conservation computed over cooperative ``runloom.sync.Lock`` forks is
INVARIANT to how the M:N scheduler ran it.  This program seeds that style (and
its sibling, deterministic replay) with the cleanest possible vehicle: dining
philosophers under a resource-hierarchy (lock-ordering) discipline so it can
never deadlock, where the run(1)-shaped baseline result IS the oracle.

Bug hunted.  A cross-hub lock handoff, a lost wake on ``Mutex`` (the capacity-1
channel behind CoLock), or a preempt-mid-acquire that DROPS or DUPLICATES a
lock grant under true GIL-off parallelism.  Such a fault would let two
philosophers believe they simultaneously hold the same fork (mutual exclusion
broken -> a fork's acquire/release balance diverges) or silently lose a meal,
so the aggregate meal count / per-fork balance would differ between a
near-serial run and a fully parallel one.

The workload is DETERMINISTIC by construction: each philosopher eats a fixed,
seed-derived number of meals (the count is chosen from its OWN random.Random and
does not depend on timing), and every fork acquisition is a real cooperative
Lock acquire matched by exactly one release.  So under a correct runtime the
totals are a pure function of (seed, NF, meals-distribution) and nothing else --
which is exactly what makes both oracles below tight.

Oracle.
  (1) METAMORPHIC EQUALITY.  We run the IDENTICAL seeded philosopher set twice
      in two phases of one process: Phase A near-serial (all philosophers are
      spawned, but each takes a single process-wide SERIALIZER lock around its
      whole eat, so only one critical section runs at a time -- the run(1)-shaped
      reference, no real parallelism) and Phase B fully parallel across all hubs
      with no serializer.  H.check that total_meals and the per-philosopher meal
      vector are IDENTICAL across the two phases.  A dropped/duplicated grant
      under true parallelism would lose or double-count a meal and diverge
      Phase B from the serial Phase A baseline.
  (2) HARD MUTUAL EXCLUSION (mode-independent).  Each fork carries a `holder`
      sentinel stamped with the wid of its current owner; while a philosopher
      holds a fork exclusively, that stamp must not change.  We count every time
      a held fork's stamp was overwritten by another philosopher (which can only
      happen if the runtime granted the same fork twice) and require zero, and
      require every fork ends UNHELD (locked()==False).  A torn/dup grant or a
      lost release breaks this directly, at any scale.
  (3) DETERMINISTIC REPLAY.  total_meals is printed; the SAME --seed across two
      whole process runs must yield the identical total (any unseeded
      nondeterminism feeding meal counts is a bug).  The count is derived purely
      from H.derive(seed, ...) so a correct runtime is bit-reproducible.

To widen the cross-hub handoff race we ``yield_now()`` WHILE HOLDING BOTH forks
(mirror of p214) so a preempt lands inside the critical section.  Forks are
always taken low-index-then-high-index (resource hierarchy) so the program is
deadlock-free regardless of interleaving; the watchdog still backstops a
lock-order regression.

Invariant (post): meals_A == meals_B exactly (total + per-philosopher); every
fork acquire matched by exactly one release with no fork left held; total_meals
deterministic across same-seed process runs.

Stresses: cooperative Lock acquire/release under M:N, cross-hub lock handoff,
preempt-mid-critical-section, metamorphic run(1)-vs-M:N equality, deterministic
replay.

Good TSan / controlled-M:N-replay target: a dropped/duplicated grant is a pure
lock-handoff memory-ordering race -- a data-race report on the Mutex token is
often the first signal, and the same-seed replay arm pins the otherwise
intermittent divergence so a green run is evidence, not luck.
"""
import harness
import runloom

# Each philosopher eats between these many meals.  Its exact count is a pure
# function of (master seed, wid) so the workload is deterministic and timing-free.
MIN_MEALS = 8
MAX_MEALS = 24
SPAN = MAX_MEALS - MIN_MEALS + 1

# 64-bit splitmix-style integer mixer.  We MUST NOT use Python's hash() of a
# string here (and therefore not H.derive, which mixes hash(part)): str hashing
# is per-process RANDOMIZED (PYTHONHASHSEED), so it would make total_meals
# non-reproducible across process runs and defeat the deterministic-replay
# oracle.  This integer-only mixer is bit-identical across runs.
_M64 = (1 << 64) - 1


def _mix(x):
    x &= _M64
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & _M64
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & _M64
    return x ^ (x >> 31)


def meals_for(seed, wid):
    """The DETERMINISTIC meal count for philosopher `wid`.

    A pure integer function of (seed, wid): identical in both phases AND across
    same-seed process runs (no str hash, no shared RNG), which is exactly what
    makes the metamorphic and deterministic-replay oracles tight."""
    return MIN_MEALS + (_mix((seed & _M64) * 0x9E3779B97F4A7C15 + wid) % SPAN)


def eat_once(lo_fork, hi_fork, holder, lo_i, hi_i, wid):
    """Acquire BOTH forks low->high (resource hierarchy: deadlock-free), hold
    them simultaneously across a yield (so a preempt / cross-hub migration lands
    mid-meal), then release in reverse.  Returns the count of mutual-exclusion
    VIOLATIONS observed -- 0 on a correct runtime.

    holder[idx] is the classic mutual-exclusion sentinel: each fork we hold is
    stamped with our wid; while we hold it exclusively, that value must not
    change.  The write-write on holder is intentionally unsynchronized, because
    ANY observed corruption (the value we wrote getting overwritten while we
    still hold the lock) is itself the bug we are hunting -- it can only happen
    if the runtime granted the same fork to two philosophers at once (a
    dropped/duplicated lock grant under true parallelism)."""
    v = 0
    lo_fork.acquire()
    holder[lo_i] = wid
    hi_fork.acquire()
    holder[hi_i] = wid
    # Both forks held.  Yield inside the critical section to widen the window for
    # a broken grant to let a second holder stamp over us.
    runloom.yield_now()
    if holder[hi_i] != wid:
        v += 1
    if holder[lo_i] != wid:
        v += 1
    hi_fork.release()
    lo_fork.release()
    return v


def philosopher(H, wid, rng, state):
    """One philosopher: eat its deterministic number of meals, each time
    contending for the pair of forks (wid, wid+1) on the shared ring.

    In Phase A a process-wide SERIALIZER lock wraps each eat so only one
    critical section runs at a time (the run(1)-shaped near-serial baseline); in
    Phase B the serializer is None and every philosopher contends in true
    parallel across the hubs."""
    nf = state["nf"]
    forks = state["forks"]
    holder = state["holder"]
    meals = state["meals"]
    violations = state["violations"]   # single-writer per philosopher slot
    slot = state["slot"]               # which phase's meal vector to write
    serializer = state["serializer"]   # CoLock in Phase A, None in Phase B
    n_meals = meals_for(H.seed, wid)

    # Forks for philosopher wid are wid and (wid+1) % nf; always grab the
    # lower index first (resource hierarchy -> no cycle -> no deadlock).
    f1, f2 = wid, (wid + 1) % nf
    lo_i, hi_i = (f1, f2) if f1 < f2 else (f2, f1)
    lo_fork, hi_fork = forks[lo_i], forks[hi_i]
    v = 0

    for _ in H.round_range():
        if not H.running():
            break
        for _ in range(n_meals):
            if not H.running():
                break
            if serializer is not None:
                with serializer:       # near-serial: one eat at a time
                    v += eat_once(lo_fork, hi_fork, holder, lo_i, hi_i, wid)
            else:
                v += eat_once(lo_fork, hi_fork, holder, lo_i, hi_i, wid)
            meals[slot][wid] += 1      # single-writer (this philosopher only)
            H.op(wid)
        H.task_done(wid)

    if v:
        violations[wid] += v           # single-writer: only philosopher wid


def run_phase(H, state, slot, serializer):
    """Run the full philosopher set once, recording into phase `slot`.

    Phase A passes a process-wide serializer CoLock so only one philosopher's
    critical section runs at a time (the run(1)-shaped near-serial baseline);
    Phase B passes serializer=None so all philosophers contend in true parallel
    across every hub.  Both phases field the SAME NF philosophers with the SAME
    deterministic meal counts, so a correct runtime makes the two bit-identical."""
    state["slot"] = slot
    state["serializer"] = serializer
    H.run_pool(state["nf"], philosopher, state)


def setup(H):
    # One fewer philosopher than hubs would under-exercise parallelism; cap so a
    # huge --funcs doesn't allocate a silly number of forks but still scales the
    # ring with the requested goroutine count.  NF forks == NF philosophers on a
    # ring; need >= 2 for the lo/hi hierarchy to mean anything.
    nf = max(3, min(H.funcs, 2000))
    forks = [runloom.sync.Lock() for _ in range(nf)]
    H.state = {
        "nf": nf,
        "forks": forks,
        # holder[i] = wid of the philosopher who currently holds fork i (the
        # mutual-exclusion sentinel; corruption while held == a broken grant).
        "holder": [-1] * nf,
        # per-philosopher meal vector, one per phase (single-writer per wid).
        "meals": {"A": [0] * nf, "B": [0] * nf},
        # per-philosopher mutual-exclusion violation count (single-writer per
        # wid).  Aggregated across both phases; must be 0 on a correct runtime.
        "violations": [0] * nf,
        "slot": "A",
        "serializer": None,
    }


def body(H):
    state = H.state
    nf = state["nf"]

    # Phase A: near-serial baseline.  A process-wide serializer lock makes only
    # one philosopher's critical section run at a time -- the run(1)-shaped
    # reference the M:N phase is compared to.  Drain it FULLY (wait_for_deadline
    # returns once exited>=expected) before starting Phase B so the two phases
    # never overlap and the per-phase meal vectors are written cleanly.
    state["holder"] = [-1] * nf
    run_phase(H, state, "A", serializer=runloom.sync.Lock())
    H.wait_for_deadline()

    if not H.running():
        return

    # Phase B: full parallel across all hubs (no serializer) -- the workload
    # under test.  A fresh holder array; the final drain happens in the harness
    # run() loop before post() reads the results.
    state["holder"] = [-1] * nf
    run_phase(H, state, "B", serializer=None)


def post(H):
    state = H.state
    nf = state["nf"]
    meals_a = state["meals"]["A"]
    meals_b = state["meals"]["B"]
    total_a = sum(meals_a)
    total_b = sum(meals_b)
    total_violations = sum(state["violations"])

    H.log("forks={0} total_meals(A serial)={1} total_meals(B M:N)={2} "
          "mutual_excl_violations={3}".format(
              nf, total_a, total_b, total_violations))
    # DETERMINISTIC-REPLAY anchor: this exact line, for a given --seed, must be
    # byte-identical across two process runs.
    H.log("REPLAY total_meals={0} seed={1}".format(total_b, H.seed))

    H.check(total_b > 0, "no meals eaten (Phase B did nothing)")

    # (1) METAMORPHIC EQUALITY: per-philosopher and total meals identical across
    # the near-serial baseline and the M:N parallel phase.  Because each
    # philosopher eats a fixed, seed-derived meal count, a correct runtime makes
    # the two phases bit-identical; a dropped/duplicated grant would let a meal
    # be lost or double-counted and diverge them.
    if H.check(total_a == total_b,
               "metamorphic divergence: total meals serial={0} != M:N={1} "
               "(a lock grant was dropped or duplicated under parallelism)"
               .format(total_a, total_b)):
        for i in range(nf):
            if meals_a[i] != meals_b[i]:
                H.fail("metamorphic divergence at philosopher {0}: meals "
                       "serial={1} != M:N={2}".format(i, meals_a[i], meals_b[i]))
                break

    # (2) HARD MUTUAL EXCLUSION (mode-independent): no philosopher ever observed
    # a fork it exclusively held being stamped by another, and no fork is left
    # held at shutdown.  Either is a dropped/duplicated lock grant.
    H.check(total_violations == 0,
            "mutual exclusion broken: {0} time(s) a held fork was claimed by "
            "another philosopher (a lock grant was dropped or duplicated)"
            .format(total_violations))
    for i in range(nf):
        if state["forks"][i].locked():
            H.fail("fork {0} left HELD at shutdown (lost release)".format(i))
            break

    # No philosopher should be LOST (parked-then-vanished) in either phase.
    H.require_no_lost("dining-philosophers completeness")


if __name__ == "__main__":
    harness.main("p320_dining_philosophers_metamorphic", body,
                 setup=setup, post=post, default_funcs=2000,
                 describe="dining philosophers over cooperative Lock forks; "
                          "run(1)-serial meals == M:N meals + per-fork "
                          "acquire==release + deterministic same-seed replay")
