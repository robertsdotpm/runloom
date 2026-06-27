"""big_100 / 318 -- singleflight (sync.Group) dedup conservation across hubs.

`runloom.sync.Group` is Go's singleflight: `do(key, fn)` runs `fn` ONCE for an
in-flight key and every concurrent caller of the SAME key shares that one
result, returning `(value, shared: bool)` -- the caller that actually ran `fn`
gets `shared==False` (the LEADER of that in-flight call), every caller that
joined an already-in-flight call gets `shared==True` (a FOLLOWER).  This
primitive is exercised by ZERO other programs, and its dedup law is a precise
conservation oracle with no analog.

The race lives in the in-flight map plus the follower wake: under M:N many
callers hit `do(same key)` across hubs simultaneously.  Two failure modes break
suppression:

  * SPLIT / WRONG-RESULT FOLLOWER -- a follower parked on a leader's Future is
    woken with a value that NO leader of this wave produced (a torn / cross-call
    Future result, or a follower spliced onto the wrong call).
  * LOST FOLLOWER WAKE -- a follower parked on the leader's Future is never
    woken (its wake was lost), so it hangs forever -> the wave's WaitGroup never
    completes -> watchdog hang.
  * MISCOUNTED EXECUTION -- `fn` ran but the leader bool was lost/duplicated, so
    the count of `shared==False` callers diverges from the count of actual
    `fn` executions.

Topology (closed-world per key, WaitGroup-fenced waves so counting is exact).
Each worker owns ONE private key and acts as that key's wave coordinator.  For
each of W = --rounds waves it:
  1. spawns `callers_per_key` caller fibers, all calling `g.do(key, slow_fn)`
     for the SAME key.  A short cooperative sleep inside `slow_fn` widens the
     in-flight window so followers really do pile onto an in-flight leader;
  2. WaitGroup-fences the wave: every caller records (shared, nonce) into its
     own single-writer slot and done()s; the coordinator wait()s for ALL of them
     before it audits results or starts wave W+1.

WHY NOT "exactly one leader per wave": the leader DELETES the key from the
in-flight map BEFORE resolving its Future (sync.py Group.do), so a do(key)
arriving AFTER a leader finished LEGITIMATELY starts a fresh in-flight call.
At scale a wave can therefore contain SEVERAL sequential in-flight calls (each a
correct, deduped leader+followers group) -- that is correct singleflight, not a
bug, and no externally-observable barrier can force a single window (a caller is
only provably "registered as a follower" from INSIDE do(), which we can't see).
So we count PER IN-FLIGHT CALL, identified by the unique nonce each `fn` run
returns, and assert the conservation that DOES hold at any scale.

Each `fn` run returns a per-key-UNIQUE nonce (the leader is the single writer of
the per-key nonce counter; waves are fenced and the library serializes a key's
in-flight calls, so the counter is race-free).  Within a wave:

Invariants (per-wave audit + post, fail-fast):
  * LEADER UNIQUENESS: #(shared==False) callers == #(distinct nonces seen) ==
    fn executions this wave.  Two callers reporting leader for the SAME nonce,
    or a leader bool with no matching execution, breaks dedup accounting.
  * FOLLOWER SOUNDNESS: every follower's nonce is one a leader actually produced
    this wave (follower_nonce in the wave's leader-nonce set) -- a value no
    leader produced == a torn/cross-call Future result.
  * EXEC CONSERVATION (post, per key): the per-key leader-execution counter ==
    the total number of leaders this coordinator observed across all its waves.
  * require_no_lost + watchdog: a follower whose Future wake was lost never
    returns; the wave WaitGroup hangs -> watchdog.  No worker may be LOST.

Stresses: sync.Group singleflight dedup, in-flight-map insert/delete race across
hubs, Future result share + follower wake, WaitGroup wave fence, per-in-flight-
call conservation.

Good TSan / controlled-M:N-replay target: the in-flight-map insert/delete + the
Future publish/wake is a pure ordering race; a data-race report on the map slot
or the Future state is often the first signal, before the conservation oracle
fires.
"""
import harness
import runloom
import runloom.sync as rsync

# Cooperative sleep inside the deduped fn: widens the in-flight window so that
# concurrent same-key callers reliably pile onto an in-flight leader as
# followers (rather than each finding the key already deleted and starting a
# fresh call).  Small enough to keep waves fast.
FN_SLEEP_S = 0.001


def slow_fn(exec_counts, slot, nonce_box):
    """The deduped function -- runs ONLY in a leader.  Sleeps to hold the
    in-flight window open (so followers join), bumps the per-key single-writer
    execution counter, and returns a per-key-UNIQUE nonce.

    Single-writer safety: only a leader runs this; the library serializes a
    key's in-flight calls (one Future in _calls at a time) and waves are
    WaitGroup-fenced, so no two leaders for this key ever run slow_fn
    concurrently -> exec_counts[slot] and nonce_box are each touched by one
    writer at a time."""
    runloom.sleep(FN_SLEEP_S)
    exec_counts[slot] += 1
    nonce = (slot << 24) | (nonce_box[0] & 0xFFFFFF)
    nonce_box[0] = nonce_box[0] + 1
    return nonce


def caller(g, key, results, idx, exec_counts, slot, nonce_box, wg):
    """One caller in a wave: do(key, fn) -- a leader runs slow_fn, a follower
    blocks on the in-flight leader's Future -- then record (shared, nonce) into
    its OWN slot (single-writer, race-free) and signal the wave fence.  ALWAYS
    done()s (even on an unexpected error) so the coordinator's wait() can only
    ever hang on a genuinely lost follower wake, never on a caller exception."""
    try:
        value, shared = g.do(
            key, lambda: slow_fn(exec_counts, slot, nonce_box))
        results[idx] = (shared, value)
    finally:
        wg.done()


def coordinator(H, wid, rng, state):
    """Owns key `wid`; drives W = --rounds waves over it.  Per wave: spawn all
    callers, WaitGroup-fence them, then audit the wave's dedup conservation.
    The coordinator (via its leaders) is the SINGLE writer of exec_counts[slot]
    and nonce_box for this key, and waves are fenced, so both are race-free."""
    g = state["group"]
    exec_counts = state["exec_counts"]
    callers_per_key = state["callers_per_key"]
    # One exec_counts slot PER COORDINATOR (per key), indexed by wid directly --
    # NOT a wid&1023 shard: there can be up to --funcs keys, so a masked shard
    # would alias two coordinators onto one slot and break the per-key
    # single-writer + conservation guarantees.
    slot = wid
    key = ("k", wid)               # this coordinator's private key
    nonce_box = [0]                # leader-only per-key nonce source
    leaders_total = 0              # leaders this coordinator has observed

    for _ in H.round_range():
        if not H.running():
            break
        # Fresh per-wave state.  results[i] is single-writer (caller i only).
        results = [None] * callers_per_key
        wg = runloom.WaitGroup()
        wg.add(callers_per_key)
        for i in range(callers_per_key):
            H.fiber(caller, g, key, results, i,
                    exec_counts, slot, nonce_box, wg)
        wg.wait()                  # fence: ALL callers returned (or watchdog)

        # ---- audit this wave (closed; no caller still in flight) ----
        leader_nonces = set()
        leader_count = 0
        follower_nonces = []
        for r in results:
            if r is None:
                H.fail("wave key={0}: a caller never recorded a result "
                       "(lost follower wake / caller error)".format(wid))
                return
            shared, value = r
            if shared:
                follower_nonces.append(value)
            else:
                leader_count += 1
                leader_nonces.add(value)

        # LEADER UNIQUENESS: each leader reported a distinct nonce -- two
        # leaders sharing a nonce would mean fn was double-counted for one
        # in-flight call (dedup accounting broken).
        if leader_count != len(leader_nonces):
            H.fail("singleflight dedup: wave key={0} had {1} leaders "
                   "(shared==False) but only {2} DISTINCT leader nonces -- two "
                   "callers reported leader for the same in-flight call".format(
                       wid, leader_count, len(leader_nonces)))
            return
        # Every wave must have at least one leader (someone ran fn) and no more
        # leaders than callers.
        if not (1 <= leader_count <= callers_per_key):
            H.fail("singleflight dedup: wave key={0} had {1} leaders, expected "
                   "1..{2} (0 = no caller ran fn / a leader bool was lost)"
                   .format(wid, leader_count, callers_per_key))
            return
        # FOLLOWER SOUNDNESS: every follower got a value a leader of THIS wave
        # actually produced -- a nonce no leader produced is a torn / cross-call
        # Future result delivered to a follower.
        for value in follower_nonces:
            if value not in leader_nonces:
                H.fail("singleflight stale result: wave key={0} follower got "
                       "nonce {1!r} that no leader produced this wave (leaders="
                       "{2!r}) -- a torn / cross-call Future result".format(
                           wid, value, sorted(leader_nonces)))
                return
        leaders_total += leader_count
        H.op(wid, callers_per_key)
        H.task_done(wid)

    # EXEC CONSERVATION (per key): the leader-execution counter slow_fn bumped
    # must equal the number of leaders this coordinator observed across all its
    # waves.  A lost / duplicated leader bool, or a miscounted fn run, breaks it.
    ex = exec_counts[slot]
    if ex != leaders_total:
        H.fail("singleflight exec conservation: key={0} fn executed {1} times "
               "but {2} leaders (shared==False) were observed -- execution count "
               "and leader count diverged (dedup accounting broken)".format(
                   wid, ex, leaders_total))


def setup(H):
    # callers_per_key: enough concurrent same-key callers to make the in-flight
    # insert/share race real, bounded so the total live fiber count
    # (keys * callers_per_key per wave) stays in the design tier.  Each
    # coordinator owns exactly one key.
    callers_per_key = 16
    # One exec slot per coordinator/key (H.funcs already reflects the max_funcs
    # cap here, since main() applies it before run()->setup()).  Indexed by wid
    # directly (not a masked shard) so every key has its own single-writer slot.
    H.state = {
        "group": rsync.Group(),
        "exec_counts": [0] * max(1, H.funcs),   # per-key single-writer exec count
        "callers_per_key": callers_per_key,
    }


def body(H):
    # H.funcs coordinators == H.funcs keys; each spawns callers_per_key callers
    # per wave from inside the root.
    H.run_pool(H.funcs, coordinator, H.state)


def post(H):
    ec = sum(H.state["exec_counts"])
    H.log("keys(coordinators)={0} callers/key={1} leader-executions(total)={2} "
          "audited-caller-ops={3}".format(
              H.expected, H.state["callers_per_key"], ec, H.total_ops()))
    H.check(H.total_ops() > 0, "no waves audited (no singleflight calls ran)")
    H.check(ec > 0, "no leader ever executed fn")
    H.require_no_lost("singleflight follower completeness")


if __name__ == "__main__":
    harness.main("p318_singleflight_dedup", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=20000,
                 describe="sync.Group singleflight: many callers do(key,fn) per "
                          "WaitGroup-fenced wave; #leaders==#distinct nonces=="
                          "#fn-execs, every follower shares a leader's nonce")
