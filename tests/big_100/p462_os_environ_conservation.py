"""big_100 / 462 -- os.environ single-owner conservation + identity under M:N.

os.environ is a PROCESS-GLOBAL mapping (os._Environ) backed at the C level by
putenv()/setenv(), which CPython documents as NOT thread-safe: the libc
environ array is a bare char** that putenv() may realloc, and the Python side is
a plain dict (os.environ._data / the _Environ wrapper) mutated without a lock.
Under runloom M:N, many fibers share a hub OS-thread (and a shared hub
PyThreadState) AND run in parallel across hubs with the GIL off, so every
`os.environ[k] = v` is a concurrent read-modify-write of one shared global
mapping -- exactly the shape that loses updates / tears entries if the runtime
does not keep the per-key mutation coherent.

This is adjacent-but-distinct from p67 (threading.local, hub-local) and p66
(contextvars, hub-context): os.environ has NO per-fiber identity at all -- it is
ONE global dict for the whole process.  We make it a CLOSED-WORLD, falsifiable
COUNTING + IDENTITY law rather than a racy probe by giving each fiber a
SINGLE-OWNER key.

WHICH ORACLE IS LOAD-BEARING, AND WHY (calibrated, not assumed):

  Every fiber owns a UNIQUE key  BIG100_W<wid>  and is the ONLY writer of that
  key (single-owner control, like p405's private Counter).  Across yields /
  sleeps / hub migrations it re-sets its key and reads its OWN key back; it also
  reads a few SIBLING keys (the measured arm).  Because each key has exactly one
  writer, the value at a fiber's own key is NOT subject to a write-write race --
  under plain OS threads WITH THE GIL ON this invariant ALWAYS holds (every
  os.environ[k]=v is serialized by the GIL, and each key has one writer, so no
  update is lost and no key is cross-written).  A correct runloom MUST preserve
  that: a fiber that sets BIG100_W<wid>="<wid>" and later reads BIG100_W<wid>
  MUST read back exactly "<wid>", and at quiesce the global mapping MUST contain
  EXACTLY the expected BIG100_W* keys (count == funcs), each holding its own
  value.  A lost update (key absent), a cross-write (key holds another fiber's
  value), an extra/missing BIG100_W* key, or a torn value is a runloom
  coherence bug in the shared global-mapping mutation -- NOT a documented caveat,
  because single-owner-per-key os.environ writes are race-free under the GIL.
  The serialized arm PASSES on a correct runtime, so the program EXITS 0 when
  there is no bug.

  We VERIFIED (standalone plain-threads control, PYTHON_GIL=1 and PYTHON_GIL=0,
  no runloom) that this single-owner identity/conservation oracle does NOT
  false-fire: under the GIL it is always clean; even GIL-off plain threads keep
  single-owner keys coherent at the Python-dict level here.  So a failure under
  runloom M:N is a genuine runtime signal, not documented-unsafe usage.

ORACLES:
  * LOAD-BEARING -- SINGLE-OWNER IDENTITY (per-op, fail-fast): after a fiber sets
    its own key and yields, reading BIG100_W<wid> back MUST equal str(wid).  A
    different worker's value, or a missing key, is a lost-update / cross-write in
    the shared os.environ mapping under M:N (impossible under single-owner GIL
    serialization).
  * LOAD-BEARING -- CONSERVATION + IDENTITY (post, HARD, at quiesce): the global
    os.environ contains EXACTLY the expected set of BIG100_W* keys
    (count == number of workers that ran -- no key lost, none duplicated/extra),
    and every such key holds EXACTLY its own owner's value (no cross-write /
    torn value survived).
  * COMPLETENESS (post, HARD): require_no_lost -- a worker stranded mid-mutation
    of the shared mapping never returns.

  * MEASURED (report-ONLY, NEVER fails): SIBLING-READ STALENESS / putenv
    contention.  Each fiber also reads a few SIBLING keys; a sibling read may see
    a value mid-flight (the sibling is concurrently re-setting its own key) or a
    not-yet-set key.  That is normal concurrent-read behaviour on a shared
    mapping (reproduces under plain GIL-off threads), so we MEASURE the
    sibling-staleness / not-yet-present rate and REPORT it, never assert on it.
    A sibling value that is OUT-OF-UNIVERSE (not a plausible worker id) WOULD be
    corruption and is the one sibling condition that fails.

Keep the population MODEST-to-LARGE but bounded: this is a correctness probe of
the shared-global-mapping coherence, driven at funcs>=8000 to expose the race.

Stresses: os.environ (os._Environ) concurrent __setitem__/__getitem__ across
hubs, libc putenv()/setenv() environ-array realloc under GIL-off RMW, shared
plain-dict mutation with no per-fiber identity, single-owner conservation +
identity, no-lost-wake mid-mutation.

Good TSan / controlled-M:N-replay target: os.environ.__setitem__ does
encode->putenv()->self._data[key]=value -- a C-string realloc plus a plain-dict
store on one shared global; a TSan report on the environ array or the _data
dict entry, or a single dropped single-owner store under replay, localizes the
lost update before the post() conservation count even closes.
"""
import os

import harness
import runloom

# Single-owner key namespace.  Each worker owns exactly ONE key
# BIG100_W<wid>; it is the only writer of that key (the single-owner control).
KEY_PREFIX = "BIG100_W"

# How many SIBLING keys each fiber peeks at per op (the report-only measured
# arm).  Small: enough to exercise concurrent reads of the shared mapping while
# other fibers re-set their own keys, without dominating the work.
SIBLING_PEEKS = 3


def key_for(wid):
    return KEY_PREFIX + str(wid)


def setup(H):
    # Per-slot, single-writer-per-slot tables (race-free) for the measured arm
    # and for op accounting.  nworkers is the single-owner population; cap the
    # key namespace to it so conservation is an exact equality.
    nworkers = max(2, H.funcs)
    H.state = {
        "nworkers": nworkers,
        # measured arm (report-only)
        "sibling_reads": [0] * nworkers,   # ONE slot per worker (race-free; wid-indexed)
        "sibling_stale": [0] * nworkers,   # sibling absent / mid-flight (benign)
        # CALIBRATION: which workers actually SET their key at least once (i.e.
        # ran a round before the window closed).  One bool PER WORKER, written
        # only by that worker (race-free, single-writer-per-slot at index wid).
        # The conservation oracle is asserted over EXACTLY this "ran" set -- a
        # worker that never got CPU before the deadline (benign scale starvation,
        # which reproduces under plain GIL-ON OS threads at scale in sustained
        # --rounds 0 mode) simply never owns a key, so it is NOT counted as a
        # lost update.  Without this, sustained mode would false-fire on
        # deadline-starved workers (verified against the plain-threads control).
        "ran": [False] * nworkers,
    }


def worker(H, wid, rng, state):
    """LOAD-BEARING single-owner worker.

    This fiber is the SOLE writer of key BIG100_W<wid>.  It sets its key, parks /
    migrates hubs, then reads its OWN key back -- which MUST equal str(wid) on a
    correct runtime (single-owner keys are race-free under the GIL, so a
    cross-write / lost update is a runloom mapping-coherence bug).  It also peeks
    a few sibling keys (the report-only measured arm)."""
    nworkers = state["nworkers"]
    ran = state["ran"]
    my_key = key_for(wid)
    my_val = str(wid)
    sib_reads = 0
    sib_stale = 0
    for _ in H.round_range():
        if not H.running():
            break
        # ---- single-owner WRITE of our own key (the only writer of this key) ----
        os.environ[my_key] = my_val
        # Mark that THIS worker ran (set its key at least once).  Single writer
        # per index -> race-free.  The conservation oracle counts only "ran"
        # workers so a deadline-starved worker (benign scale) is not a lost key.
        ran[wid] = True
        # Park / yield / migrate hubs between the write and the read-back so the
        # mutation and the read straddle a scheduling point (and likely different
        # hubs) -- the window a coherence bug would corrupt.
        runloom.yield_now()
        if rng.random() < 0.5:
            runloom.sleep(0.0003)

        # ---- LOAD-BEARING: read our OWN key back; it MUST be exactly ours ----
        try:
            got = os.environ[my_key]
        except KeyError:
            got = None
        if got != my_val:
            # A missing key (lost update) or a sibling's value (cross-write) at
            # OUR single-owner key is impossible under GIL single-owner
            # serialization -> a runloom shared-mapping coherence bug.
            H.fail(
                "os.environ SINGLE-OWNER VIOLATION: our key {0!r} read back "
                "{1!r} but we (the SOLE writer) set it to {2!r} -- a lost "
                "update / cross-write / torn value in the shared os.environ "
                "mapping under M:N (single-owner keys are race-free under the "
                "GIL)".format(my_key, got, my_val))
            return

        # ---- MEASURED arm (report-only): peek a few SIBLING keys ----
        # A sibling may be mid-write of its own key (stale / absent) -- normal
        # concurrent-read behaviour, REPORTED not failed.  But a sibling value
        # that is OUT-OF-UNIVERSE (not a plausible worker id) is corruption.
        for _ in range(SIBLING_PEEKS):
            sw = rng.randrange(nworkers)
            sval = os.environ.get(key_for(sw))
            sib_reads += 1
            if sval is None:
                sib_stale += 1            # sibling not yet set / mid-flight (benign)
                continue
            # The only sibling condition that FAILS: a torn / out-of-universe
            # value -- a value that is not the decimal id of some worker.  (A
            # CORRECT-but-stale sibling value is just some OTHER valid wid; benign.)
            if not (sval.isdigit() and 0 <= int(sval) < nworkers):
                H.fail(
                    "os.environ CORRUPTION: sibling key {0!r} holds {1!r}, "
                    "which is not any worker's value (out-of-universe / torn "
                    "entry in the shared mapping)".format(key_for(sw), sval))
                return
            if int(sval) != sw:
                # sibling key holds a DIFFERENT valid wid's value: that is a
                # cross-write between two single-owner keys -> load-bearing FAIL.
                H.fail(
                    "os.environ CROSS-WRITE: sibling key {0!r} holds {1!r} "
                    "(value belongs to worker {1}, not {2}) -- two single-owner "
                    "keys were cross-written in the shared mapping under "
                    "M:N".format(key_for(sw), sval, sw))
                return

        H.op(wid)
        H.task_done(wid)

    state["sibling_reads"][wid] += sib_reads   # UNIQUE per-worker slot (race-free; see p313)
    state["sibling_stale"][wid] += sib_stale


def body(H):
    n = H.state["nworkers"]
    H.run_pool(n, worker, H.state, max_concurrent=n)


def post(H):
    state = H.state
    nworkers = state["nworkers"]
    reads = sum(state["sibling_reads"])
    stale = sum(state["sibling_stale"])
    stale_pct = (100.0 * stale / reads) if reads else 0.0

    # ---- LOAD-BEARING CONSERVATION + IDENTITY (at quiesce) ------------------
    # The run is fully drained (every worker returned), so the global mapping is
    # quiescent.  Collect the BIG100_W* keys actually present and check:
    #   (1) IDENTITY: every present BIG100_W<wid> holds EXACTLY str(wid) -- no
    #       cross-write / torn value survived to quiesce.
    #   (2) CONSERVATION: the present BIG100_W* key SET is EXACTLY the set of
    #       workers that RAN (set their key at least once) -- no single-owner key
    #       was LOST (a worker ran but its key vanished) and none was
    #       duplicated/extra/corrupted.
    # The expected set is the "ran" workers, NOT all funcs: in deadline-bounded
    # sustained (--rounds 0) mode some workers may never get CPU before the
    # window closes (benign scale starvation -- it reproduces under plain GIL-ON
    # OS threads at scale, verified by the control), so they legitimately own no
    # key.  In the default --rounds 1 mode every worker runs exactly one round
    # (run() joins all of them), so "ran" == all funcs and this is the exact
    # all-keys-present conservation law.
    expected_wids = set(w for w in range(nworkers) if state["ran"][w])
    present = {}
    for k, v in list(os.environ.items()):
        if k.startswith(KEY_PREFIX):
            present[k] = v

    # (1) identity: every present BIG100_W* key holds its own owner's value.
    bad_identity = None
    present_wids = set()
    for k, v in present.items():
        suffix = k[len(KEY_PREFIX):]
        if not suffix.isdigit():
            bad_identity = (k, v, "non-numeric key suffix (corrupted key)")
            break
        owner = int(suffix)
        present_wids.add(owner)
        if v != str(owner):
            bad_identity = (k, v, "expected {0!r}".format(str(owner)))
            break

    # (2) conservation: the present BIG100_W* set MUST equal the set of workers
    # that RAN (set their key >=1 time).  A worker in "ran" whose key is now
    # MISSING is a genuine lost update (it set its single-owner key, the runtime
    # dropped it from the shared mapping).  A present key whose owner is NOT in
    # "ran" (EXTRA) means a key materialized for a worker that never wrote one --
    # a phantom/cross-keyed entry.  Both are real mapping-coherence faults; the
    # benign deadline-starved workers are simply not in "ran", so they never
    # contribute a false "missing".
    missing = expected_wids - present_wids
    extra = present_wids - expected_wids

    H.log("os.environ single-owner conservation: present BIG100_W* keys={0} "
          "expected(ran)={1} of funcs={2} (LOAD-BEARING) | sibling peeks={3} "
          "stale/absent={4} ({5:.1f}%, concurrent-read staleness -- REPORT "
          "ONLY)".format(
              len(present), len(expected_wids), nworkers, reads, stale,
              stale_pct))

    if bad_identity is not None:
        k, v, why = bad_identity
        H.fail("os.environ IDENTITY BROKEN at quiesce: key {0!r} holds {1!r} "
               "({2}) -- a cross-write / torn value survived in the shared "
               "global mapping under M:N".format(k, v, why))
    elif missing:
        sample = sorted(missing)[:8]
        H.fail("os.environ CONSERVATION BROKEN: {0} single-owner key(s) "
               "MISSING at quiesce (lost update on the shared mapping); e.g. "
               "{1}".format(len(missing),
                            [key_for(w) for w in sample]))
    elif extra:
        sample = sorted(extra)[:8]
        H.fail("os.environ CONSERVATION BROKEN: {0} UNEXPECTED BIG100_W* "
               "key(s) at quiesce (an extra/duplicated key in the shared "
               "mapping); e.g. {1}".format(len(extra),
                                           [key_for(w) for w in sample]))

    # Sanity: the hazard was actually exercised (keys were set), else the oracle
    # is vacuous.
    H.check(len(present) > 0,
            "no BIG100_W* keys present at quiesce -- the os.environ single-owner "
            "mutation hazard was never exercised (oracle would be vacuous)")

    # Report-only context: surface the measured sibling-read staleness so the
    # semantics are explicit (it is benign concurrent-read behaviour, never a
    # failure).
    if stale:
        H.log("note: {0} sibling reads ({1:.1f}%) saw an absent / mid-flight "
              "sibling key -- benign concurrent-read staleness on the shared "
              "os.environ mapping (reproduces under plain GIL-off threads), NOT "
              "a runloom bug; the load-bearing oracle is the SINGLE-OWNER "
              "identity/conservation of each fiber's OWN key".format(
                  stale, stale_pct))

    # COMPLETENESS: no worker parked-then-vanished mid-mutation of the shared
    # mapping.
    H.require_no_lost("os.environ single-owner conservation")

    # Clean up the BIG100_W* keys we created (subprocess isolation is the
    # backstop, but leave the environment tidy regardless).
    for k in list(present.keys()):
        try:
            del os.environ[k]
        except KeyError:
            pass


if __name__ == "__main__":
    harness.main(
        "p462_os_environ_conservation", body, setup=setup, post=post,
        default_funcs=8000,
        describe="many hubs concurrently set/get the PROCESS-GLOBAL os.environ "
                 "mapping (putenv()-backed, documented thread-unsafe); each fiber "
                 "is the SOLE writer of BIG100_W<wid> and reads it back across "
                 "yields/migrations -- single-owner identity + conservation "
                 "(every key holds its own value, key set count == funcs at "
                 "quiesce) is the LOAD-BEARING oracle; sibling-read staleness / "
                 "putenv contention is documented-unsafe concurrent reads -- "
                 "report-only")
