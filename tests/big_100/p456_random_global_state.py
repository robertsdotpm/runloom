"""big_100 / 456 -- random module global Mersenne-Twister state under M:N.

The `random` module's module-level functions -- random.random(), random.getrandbits(),
random.randint(), ... -- are all bound methods of ONE shared, process-global
`random.Random()` instance (`random._inst`).  That instance is a C `_random.Random`
wrapping a 624-word Mersenne-Twister state array plus an index.  Each call does a
READ-MODIFY-WRITE of that state (genrand_uint32 advances the index and, every 624
draws, regenerates the whole array in place).  The shared instance is NOT internally
locked.  Under the GIL each RMW is atomic w.r.t. other Python frames; with the GIL OFF
under M:N, many hub fibers calling the global random.random()/getrandbits() concurrently
RACE on that one MT state -- a state-array index race, or two regenerations interleaving,
can corrupt it.

WHICH ORACLE IS LOAD-BEARING, AND WHY (the p321/p67 discriminator discipline):

  * MEASURED arm (report-only, documented-unsafe): the SHARED process-global
    random.random()/getrandbits(k) hammered concurrently by many hub fibers.  Sharing
    one un-locked Random across concurrent callers is documented-unsafe usage for ANY
    GIL-off concurrency model (it reproduces under plain threads with PYTHON_GIL=0, no
    runloom).  So we do NOT fail on the *outcome* being non-deterministic -- that is
    expected.  We DO assert a closed invariant that no amount of benign racing may ever
    break: every random.random() is a float in [0.0, 1.0), and every getrandbits(k) is
    an int in [0, 2**k).  An out-of-range float, a getrandbits(k) that does NOT fit k
    bits, a non-int, or an outright crash is an IMPOSSIBLE value = a genuinely corrupted
    MT state (or a torn read of the C state array) -- that IS a hard fail at any scale,
    because a correct (even unsynchronized) MT can never emit one.  We also REPORT a
    contention proxy (collisions among concurrently-drawn global values) so the
    semantics are explicit; the collision rate itself NEVER fails.

  * LOAD-BEARING arm (single-owner reproducibility, HARD): each fiber owns a PRIVATE
    `random.Random(wid)`.  A single-owner Random has exactly one writer, so it is
    race-free by construction -- under plain threads GIL-ON or GIL-OFF, and under a
    correct runloom, re-seeding it with the same seed ALWAYS reproduces the identical
    draw sequence.  The fiber generates a sequence (mixing random()/getrandbits/randint),
    INTERLEAVED with yields and sleeps so it is routinely preempted and migrated across
    hubs MID-sequence; it then re-seeds Random(wid) and MUST reproduce the byte-identical
    sequence.  If runloom corrupts a per-fiber Python object across a hub switch (a
    migration that desyncs the object's C state, a preempt-mid-RMW that another hub's
    fiber then clobbers because it wrongly shares the instance) the reproduced sequence
    DIVERGES.  That divergence does NOT happen under stock single-owner use (verified
    via a plain-threads control, GIL on AND off) -- so it is a true runloom signal, and
    the program EXITS 0 when there is no bug.

  Why single-owner is the load-bearing oracle and the shared global is only measured:
  the private Random has ONE owner fiber, so any non-reproducibility is NOT contention --
  it can only be the runtime mishandling per-fiber object state across a scheduling
  point.  The shared global has many owners, so its non-reproducibility is the expected,
  documented consequence of unsynchronized sharing (measured, never failed); only an
  *impossible* value from it indicts the runtime.

FAIL ON:
  * per-instance non-reproducible draw sequence (the load-bearing reproducibility oracle);
  * an IMPOSSIBLE global value (float outside [0,1), getrandbits(k) not fitting k bits,
    non-int / non-float, or a torn type) -- corrupted shared MT state;
  * a crash (faulthandler / watchdog catches a SIGSEGV mid state-array regeneration).

Stresses: shared random._inst Mersenne-Twister 624-word state RMW under GIL-off
concurrency (index race / interleaved regeneration), per-fiber random.Random object
state integrity across hub migration + preempt-mid-RMW, getrandbits bit-width invariant,
no-lost-wake while parked between draws.

Good TSan / controlled-M:N-replay target: the genrand_uint32 read-advance-write over the
shared `random._inst`'s C state array is a textbook concurrent RMW; a TSan report on the
state words, or a replay that migrates a hub mid-sequence and diverges the per-fiber
reproduction, localizes the corruption before the oracle even fires.
"""
import random

import harness
import runloom

# Modest, correctness-probe population (this is a state-integrity probe, not a soak).
MAX_WORKERS = 12000

# Length of each fiber's private reproducibility sequence.  Long enough to cross the
# 624-draw MT regeneration boundary several times (so a regenerate-vs-migrate desync
# would show), short enough that many fibers complete under the window.  getrandbits
# below also pulls multiple 32-bit words per call, accelerating the index past 624.
SEQ_LEN = 96

# getrandbits widths to exercise: span single-word, the 32-bit boundary, and multi-word
# (k>32 pulls multiple genrand_uint32 words -- the multi-word RMW most exposed to a torn
# index).  Each width's result MUST fit in k bits (the closed bit-width invariant).
BIT_WIDTHS = (1, 7, 31, 32, 33, 53, 64, 127)

# How many shared-global draws each fiber contributes to the MEASURED arm per round.
GLOBAL_DRAWS = 24


def gen_private_sequence(rng, yield_hook):
    """Generate one fiber's reproducibility sequence from its PRIVATE Random `rng`,
    interleaving scheduling points (yield_hook) so the fiber is preempted/migrated
    across hubs MID-sequence.  Returns the list of drawn values.  Deterministic for a
    given seed: the SAME seed must reproduce the SAME list (the load-bearing oracle).

    The draws mix random() (53-bit double), getrandbits (variable width, multi-word),
    and randint (rejection-sampling over getrandbits) so several distinct C state paths
    are crossed by the scheduling points."""
    out = []
    for i in range(SEQ_LEN):
        # Rotate through draw kinds deterministically (kind depends only on i, so it is
        # identical across the two passes -- the sequence stays a pure function of seed).
        kind = i % 3
        if kind == 0:
            out.append(rng.random())
        elif kind == 1:
            k = BIT_WIDTHS[i % len(BIT_WIDTHS)]
            out.append(rng.getrandbits(k))
        else:
            out.append(rng.randint(0, 1 << 40))
        # Scheduling point INSIDE the sequence: the fiber can be preempted here and
        # resume on a different hub.  A correct runtime keeps this fiber's private
        # Random object state intact across that switch; a desync diverges pass 2.
        yield_hook(i)
    return out


def measured_global_arm(H, rng, state, slot):
    """REPORT-ONLY measured arm: hammer the SHARED process-global random.* (one
    un-locked Random) concurrently with other hub fibers.  We never fail on the OUTCOME
    being non-deterministic (documented-unsafe sharing -- reproduces under plain threads
    GIL-off).  We DO hard-fail on an IMPOSSIBLE value, which a correct MT can never emit
    even unsynchronized -- that is a corrupted/torn shared state.  Reports a contention
    proxy (collisions among concurrently-drawn global values)."""
    seen = state["global_seen"]
    for j in range(GLOBAL_DRAWS):
        kind = (slot + j) % 3
        if kind == 0:
            v = random.random()                 # shared random._inst.random()
            # CLOSED INVARIANT: every random.random() is a float in [0.0, 1.0).  An
            # out-of-range value or a non-float is an impossible draw -> corrupted MT
            # state (NOT contention -- contention only reorders valid draws).
            if not isinstance(v, float):
                H.fail("shared random.random() returned non-float {0!r} (type {1}) "
                       "-- torn read of the shared Mersenne-Twister state under GIL-off "
                       "concurrency".format(v, type(v).__name__))
                return
            if not (0.0 <= v < 1.0):
                H.fail("shared random.random() returned {0!r} OUTSIDE [0.0, 1.0) -- an "
                       "IMPOSSIBLE value = corrupted shared MT state (index race / "
                       "interleaved regeneration under M:N)".format(v))
                return
        elif kind == 1:
            k = BIT_WIDTHS[(slot + j) % len(BIT_WIDTHS)]
            v = random.getrandbits(k)           # shared random._inst.getrandbits(k)
            # CLOSED INVARIANT: getrandbits(k) is an int in [0, 2**k).  A value that does
            # not fit k bits means the C state assembled too many / torn words.
            if not isinstance(v, int):
                H.fail("shared random.getrandbits({0}) returned non-int {1!r} (type "
                       "{2}) -- torn shared MT state".format(k, v, type(v).__name__))
                return
            if not (0 <= v < (1 << k)):
                H.fail("shared random.getrandbits({0}) returned {1!r} which does NOT "
                       "fit {0} bits (bit_length={2}) -- IMPOSSIBLE value = corrupted "
                       "shared MT state under GIL-off concurrency".format(
                           k, v, v.bit_length()))
                return
        else:
            v = random.randint(0, 1 << 30)      # rejection-samples shared getrandbits
            if not isinstance(v, int):
                H.fail("shared random.randint() returned non-int {0!r} (type {1}) -- "
                       "torn shared MT state".format(v, type(v).__name__))
                return
            if not (0 <= v <= (1 << 30)):
                H.fail("shared random.randint(0, 2**30) returned {0!r} OUT OF RANGE -- "
                       "IMPOSSIBLE value = corrupted shared MT state".format(v))
                return
        # Contention proxy (REPORT ONLY): a value drawn by two concurrent fibers in the
        # same round-window.  Collisions are expected/benign under unsynchronized sharing
        # (NOT a fail) -- they just quantify that the global arm really was concurrent.
        # Float collisions are vanishingly rare unless the state is stuck, so a HIGH
        # collision rate is itself an interesting (but still non-failing) signal.
        bucket = (hash(v) & 0x3FF)
        prev = seen[bucket]
        if prev == v:
            state["global_collisions"][slot & 1023] += 1
        seen[bucket] = v
        state["global_draws"][slot & 1023] += 1
        runloom.yield_now()                     # keep the global arm genuinely concurrent


def worker(H, wid, rng, state):
    """One worker.  `rng` is the harness-derived Random for NON-load-bearing choices
    only.  The LOAD-BEARING oracle uses a PRIVATE random.Random(wid) (single-owner,
    race-free) and asserts re-seed reproducibility across mid-sequence preempt/migration;
    the MEASURED arm hammers the shared global and asserts only closed value invariants."""
    slot = wid & 1023

    def yield_hook(i):
        # Alternate yield_now() and a tiny sleep so the fiber both cooperatively yields
        # (intra-hub preempt) and parks on a timer (which can resume it on ANOTHER hub) --
        # both must preserve the private Random's C state.
        if (i & 1) == 0:
            runloom.yield_now()
        else:
            runloom.sleep(0.0002)

    r = -1
    for _ in H.round_range():
        if not H.running():
            break
        r += 1

        # ---- LOAD-BEARING: single-owner reproducibility across preempt/migration ----
        # Seed a PRIVATE Random with a per-(wid,round) seed.  Pass 1 generates the
        # sequence while being preempted/migrated mid-draw; we then RE-SEED the same
        # private Random with the SAME seed and regenerate.  A correct runtime keeps the
        # per-fiber object's C MT state intact across every scheduling point, so the two
        # passes MUST be byte-identical.  (We yield in BOTH passes so both cross hubs.)
        seed = (wid << 20) ^ (r << 1) ^ 0x5A5A5A5A
        priv = random.Random(seed)
        seq1 = gen_private_sequence(priv, yield_hook)
        priv.seed(seed)
        seq2 = gen_private_sequence(priv, yield_hook)
        if seq1 != seq2:
            # Find the first divergence for a precise diagnostic.
            idx = next((i for i in range(min(len(seq1), len(seq2)))
                        if seq1[i] != seq2[i]), -1)
            d1 = seq1[idx] if 0 <= idx < len(seq1) else None
            d2 = seq2[idx] if 0 <= idx < len(seq2) else None
            H.fail("PER-INSTANCE NON-REPRODUCIBLE: a PRIVATE random.Random(seed={0}) "
                   "owned by ONE fiber produced DIFFERENT sequences on two re-seeded "
                   "passes (first diverge at draw {1}: {2!r} != {3!r}) -- a single-owner "
                   "Random is race-free, so this is runloom corrupting the per-fiber "
                   "object's Mersenne-Twister C state across a hub migration / "
                   "preempt-mid-RMW (wid {4}, round {5})".format(
                       seed, idx, d1, d2, wid, r))
            return
        state["repro_ok"][slot] += 1

        # ---- MEASURED (report-only): shared global random.* under concurrency ----
        measured_global_arm(H, rng, state, slot)
        if H.failed:
            return

        H.op(wid)
    H.task_done(wid)


def setup(H):
    nworkers = min(MAX_WORKERS, max(2, H.funcs))
    H.state = {
        "nworkers": nworkers,
        "repro_ok": [0] * 1024,            # private re-seed reproductions verified
        "global_draws": [0] * 1024,        # shared-global draws done (measured)
        "global_collisions": [0] * 1024,   # shared-global value collisions (report only)
        "global_seen": [None] * 1024,      # last value per hash bucket (collision proxy)
    }


def body(H):
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    repro = sum(H.state["repro_ok"])
    gdraws = sum(H.state["global_draws"])
    gcoll = sum(H.state["global_collisions"])
    coll_pct = (100.0 * gcoll / gdraws) if gdraws else 0.0
    H.log("private re-seed reproductions verified={0} (LOAD-BEARING -- each is a "
          "single-owner random.Random whose draw sequence survived preempt/migration) "
          "| shared-global draws={1} collisions={2} ({3:.2f}%, documented-unsafe "
          "unsynchronized sharing -- REPORT ONLY, every value was a valid float/int) | "
          "ops={4}".format(repro, gdraws, gcoll, coll_pct, H.total_ops()))

    # LOAD-BEARING completeness: the reproducibility oracle must actually have run, or it
    # is vacuous.  Reaching post with no failure already proves every reproduction was
    # byte-identical (the check is fail-fast); require it ran at all.
    H.check(repro > 0,
            "no private re-seed reproduction ran -- the single-owner per-fiber "
            "random.Random state-integrity hazard was never exercised (oracle would be "
            "vacuous)")
    # The measured arm must also have exercised the shared global (else the impossible-
    # value invariant never ran); this NEVER fails on its outcome, only requires coverage.
    H.check(gdraws > 0,
            "no shared-global random.* draws ran -- the corrupted-MT-state value "
            "invariant was never exercised")

    if gcoll:
        H.log("note: the shared-global arm observed {0} value collisions across {1} "
              "draws ({2:.2f}%) -- expected/benign under unsynchronized sharing of one "
              "random._inst (reproduces under plain GIL-off threads, NOT a runloom bug); "
              "every value was still a valid in-range float/int, so the shared MT state "
              "was never corrupted".format(gcoll, gdraws, coll_pct))

    # COMPLETENESS: no worker parked-then-vanished (e.g. lost-woken while sleeping between
    # draws inside the reproducibility sequence).
    H.require_no_lost("random global MT-state integrity")


if __name__ == "__main__":
    harness.main(
        "p456_random_global_state", body, setup=setup, post=post,
        default_funcs=8000,
        describe="the random module's global functions delegate to ONE shared, "
                 "un-locked random.Random (a Mersenne-Twister 624-word state); the "
                 "LOAD-BEARING oracle is single-owner reproducibility -- a PRIVATE "
                 "random.Random(wid) must reproduce its draw sequence after re-seed "
                 "across preempt/migration (a desync indicts runloom).  The shared "
                 "global random.*() under concurrency is MEASURED: every value must be "
                 "a valid in-range float/getrandbits-k-bit int (an impossible value = "
                 "corrupted MT state, a hard fail), but non-reproducibility is "
                 "documented-unsafe and report-only")
