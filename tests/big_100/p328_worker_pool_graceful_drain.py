"""big_100 / 328 -- Go-style worker-pool graceful drain (DUAL close-count).

The textbook Go fan-out/fan-in worker pool, shut down by CLOSING the shared
work channel:

    work    = Chan(cap)            # ONE shared work channel, W rangers on it
    results = Chan(cap)            # fan-in channel a collector drains

    dispatcher:  for tok in J unique tokens: work.send(tok)
                 work.close()                  # graceful drain signal

    worker (one of W, all ranging the SAME work chan):
        for (val, ok) in <recv work>:
            if not ok:                          # work closed AND drained
                results.send(WORK_CLOSE)        # report "my range ended"
                return                          # this ranger exits
            results.send(process(val))          # fan-in the result

    when all W workers have returned -> results.close()   # collectors see it

    collector (one of C, draining results):
        for (val, ok) in <recv results>:
            if not ok: saw_results_close; return
            tally(val)

Graceful drain means: closing the shared work channel must (a) let every
buffered job be delivered to SOME ranger FIRST, and (b) then wake ALL W parked
rangers exactly once with ok=False so each exits cleanly.

WHY THIS IS NOT A p41/p51 RE-SKIN.  The bare in==out conservation (every job
consumed exactly once) is already covered by p41 (one producer / many consumers)
and p51 (close mid-flight, sent==consumed).  The GENUINELY NEW shape here is the
fan-OUT over a SHARED work chan with W *rangers* plus an explicit DUAL
close-count:

  * WORK-CLOSE COUNT.  p41 has ONE producer and its consumers each just `break`
    on ok=False -- nobody counts the close events.  Here W rangers SHARE one work
    chan and a single work.close() must wake EXACTLY W of them, each ONCE, with
    ok=False.  We count those wake-ok=False events as WORK_CLOSE sentinels routed
    through the results chan.  A LOST close-wake -> a ranger stays parked forever
    (work-close count < W, and that ranger is a LOST worker); a SPURIOUS extra
    close-wake or a re-delivered ok=False -> work-close count > W.  Neither is
    observable in p41/p51; both are caught here as `work-closes != W`.

  * RESULTS-CLOSE COUNT.  The fan-IN side is then ALSO closed (after the last
    ranger exits) and each of the C collectors must SEE that close exactly once
    (recv -> ok=False) before exiting.  We count collectors that observed the
    results-close and assert it equals C.  A lost results-close-wake strands a
    collector (a LOST worker); none of p41/p51 closes a SECOND, fan-in channel
    nor counts who saw it.

So the conservation here is a DUAL close-count layered on top of in==out:
   distinct jobs out == J            (no loss / no dup -- the in==out floor)
   work-close sentinels == W         (close woke ALL W rangers exactly once)
   collectors that saw results-close == C   (the fan-in close also delivered)

ORACLE -- exact conservation + dual close-count (a drain bug breaks >=1):

  (1) JOB CONSERVATION.  The dispatcher enqueues exactly J globally-unique
      tokens (wid<<40 | seq).  Collectors collect every processed token into
      per-collector SETS (single writer each -> race-free).  post asserts:
      len(union of all collector sets) == J          (no job lost or invented)
      sum(len(set_i)) == len(union)                  (no job delivered twice).
      A job dropped on close -> union < J; a job double-ranged (two rangers got
      one buffered job) -> sum(len) > len(union).

  (2) WORK-CLOSE COUNT == W  (THE non-redundant delta).  Each ranger, on the
      first recv that returns ok=False, sends exactly one WORK_CLOSE sentinel to
      results and exits.  Collectors count those sentinels.  post asserts the
      total == W * groups: close woke ALL W rangers, each exactly once.
        work-closes < W  -> a ranger's close-wake was LOST (it is still parked);
        work-closes > W  -> a spurious / duplicated close-wake.

  (3) RESULTS-CLOSE SEEN == C.  After all W rangers return, the group closes the
      results chan; each collector's drain loop must end on ok=False.  We count
      collectors that observed it; post asserts == C * groups -- the fan-in close
      also delivered to everyone.

  (4) require_no_lost + watchdog.  A stranded ranger (lost work-close-wake) or a
      stranded collector (lost results-close-wake) never returns; the group's
      WaitGroup never completes -> the worker is LOST -> watchdog / require_no_lost
      fires.  No worker may be lost.

Closed-world per worker so conservation is EXACT: each pool goroutine owns one
work chan + one results chan + its W rangers + C collectors + 1 dispatcher, runs
a fixed J-token budget, and every fiber RETURNS (mn_run joins on pending count).
Sharded token accounting (indexed wid&1023) scales to the design tier.

Stresses: close() waking ALL W parked rangers on a SHARED channel exactly once
(fan-out close-broadcast), buffered-job delivery-before-close, fan-in second
close delivered to all collectors, no lost/dup job across the fan-out, no lost
close-wake on either channel.

Good TSan / controlled-M:N-replay target: the work.close() store vs W rangers'
parked recv re-read across hubs (the close-broadcast wake) is a pure ordering
surface; a data-race on the chan closed-flag / waiter list, or a missed wake, is
often the first signal before the conservation oracle even fires.
"""
import harness
import runloom

W = 4                     # rangers (workers) per group -- share ONE work chan
C = 3                     # collectors per group draining the results chan
JOBS_PER_GROUP = 128      # unique tokens the dispatcher enqueues, then close()
WORK_CAP = 8              # work chan capacity (small -> rangers really park)
RESULTS_CAP = 8           # results chan capacity

# A sentinel value that is NEVER a real token.  Real tokens are non-negative
# (wid<<40 | seq); -1 cannot collide with any token, so a collector can
# distinguish "a ranger saw work-close" from "a processed job".
WORK_CLOSE = -1


def process(tok):
    """Transform a job token into its result token.  Identity-preserving here so
    the collector can tally the exact token set (conservation is on the token
    identity, not the value); a real pool would do work, but the conservation
    law is what we test."""
    return tok


def dispatcher(work, base, n):
    """Enqueue exactly n globally-unique tokens onto the SHARED work chan, then
    close() it.  Close AFTER the last send so every buffered job is delivered to
    some ranger first (graceful drain), and the single close() must then wake all
    W parked rangers."""
    for i in range(n):
        work.send(base | (i & 0xFFFFFFFFFF))
    work.close()


def ranger(work, results, my_set):
    """One of W workers ranging the SHARED work chan.  Range == recv until the
    chan is closed AND drained (ok=False).  For each real job: process it and
    fan it IN through the results chan, recording the token in THIS ranger's own
    set (single writer -> race-free).  On close-wake (ok=False): send exactly one
    WORK_CLOSE sentinel to results (so a collector counts that this ranger's
    range ended) and RETURN.

    The non-redundant hazard: many rangers park on the SAME work chan; the single
    work.close() must wake each of us exactly once.  A lost wake strands this
    ranger here forever (a LOST worker)."""
    while True:
        val, ok = work.recv()          # parks; ok=False once work closed+drained
        if not ok:
            results.send(WORK_CLOSE)    # report: my range ended (one per ranger)
            return
        results.send(process(val))      # fan-in the processed job
        my_set.add(val)                 # single-writer set -> race-free


def collector(results, my_set, saw_close_slot):
    """One of C collectors draining the fan-in results chan.  Tally every real
    processed token into THIS collector's own set (single writer -> race-free)
    and count WORK_CLOSE sentinels into the same set's tally via the shared
    close-counter (see worker()).  Exit when results is closed+drained (ok=False),
    recording that this collector SAW the results-close exactly once -- the
    fan-in second close must reach every collector."""
    while True:
        val, ok = results.recv()       # parks; ok=False once results closed+drained
        if not ok:
            saw_close_slot[0] = 1       # this collector observed results-close
            return
        # A WORK_CLOSE sentinel is counted by the caller via a shared counter;
        # here we just route it -- real tokens go into the per-collector set.
        if val == WORK_CLOSE:
            saw_close_slot[1] += 1      # this collector's tally of work-closes seen
        else:
            my_set.add(val)


def worker(H, wid, rng, state):
    """One closed-world worker-pool group: SHARED work chan with W rangers, a
    results chan with C collectors, 1 dispatcher.  Fixed J-token budget; every
    fiber returns.  Folds the group's conservation + dual close-count into the
    shared sharded accounting."""
    slot = wid & 1023
    for _ in H.round_range():
        if not H.running():
            break

        work = runloom.Chan(WORK_CAP)
        results = runloom.Chan(RESULTS_CAP)
        # Each ranger and each collector owns its OWN set (single writer) -> the
        # union is computed at the end with no shared-set mutation race.
        ranger_sets = [set() for _ in range(W)]
        collector_sets = [set() for _ in range(C)]
        # Per-collector [saw_results_close, work_closes_seen] (single writer
        # each); summed after join.
        collector_close = [[0, 0] for _ in range(C)]

        base = wid << 40                       # globally-unique token prefix

        # Fence the W rangers so we can close the results chan ONLY after the
        # LAST ranger has returned (otherwise a collector could see results-close
        # while a ranger still wants to send -> send-on-closed raise).
        rangers_wg = runloom.WaitGroup()
        rangers_wg.add(W)
        # Fence everyone (rangers + collectors + dispatcher + the closer) so the
        # group can't advance to its conservation audit while a fiber is live.
        all_wg = runloom.WaitGroup()
        all_wg.add(W + C + 1 + 1)              # W rangers, C collectors, disp, closer

        def run_ranger(ri):
            try:
                ranger(work, results, ranger_sets[ri])
            finally:
                rangers_wg.done()
                all_wg.done()

        for ri in range(W):
            H.fiber(run_ranger, ri)

        def run_collector(ci):
            try:
                collector(results, collector_sets[ci], collector_close[ci])
            finally:
                all_wg.done()

        for ci in range(C):
            H.fiber(run_collector, ci)

        def run_dispatcher():
            try:
                dispatcher(work, base, JOBS_PER_GROUP)
            finally:
                all_wg.done()

        H.fiber(run_dispatcher)

        def run_closer():
            # Close the fan-IN results chan ONLY after EVERY ranger has exited
            # (each ranger's last act is results.send(WORK_CLOSE); waiting for the
            # rangers_wg guarantees no ranger will send again, so the close is
            # safe and every collector then drains the buffer and sees ok=False).
            try:
                rangers_wg.wait()
                results.close()
            finally:
                all_wg.done()

        H.fiber(run_closer)

        all_wg.wait()                          # join EVERYONE (or watchdog)

        # ---- per-group conservation + dual close-count (fold into shared) ----
        # JOB CONSERVATION: union of collector sets vs J.
        union = set()
        total_recv = 0
        for cs in collector_sets:
            union |= cs
            total_recv += len(cs)
        distinct = len(union)
        # WORK-CLOSE count: sum the work-closes every collector observed; this
        # must equal W (close woke all W rangers exactly once).
        work_closes = sum(cc[1] for cc in collector_close)
        # RESULTS-CLOSE seen: how many collectors observed the fan-in close.
        results_close_seen = sum(cc[0] for cc in collector_close)

        st = state
        st["produced"][slot] += JOBS_PER_GROUP
        st["distinct"][slot] += distinct
        st["total_recv"][slot] += total_recv
        st["work_closes"][slot] += work_closes
        st["results_close_seen"][slot] += results_close_seen
        st["expected_work_closes"][slot] += W
        st["expected_results_close"][slot] += C

        # Per-group fail-fast (so a breach names the group, not just the global
        # sum).  A real M:N drain bug trips one of these.
        if distinct != JOBS_PER_GROUP:
            H.fail("JOB LOSS/DUP: group wid={0} delivered {1} distinct jobs != "
                   "J={2} (a buffered job dropped on close, or a job ranged by "
                   "two rangers)".format(wid, distinct, JOBS_PER_GROUP))
            return
        if total_recv != distinct:
            H.fail("JOB DUP: group wid={0} sum(per-collector sizes)={1} != "
                   "distinct={2} (a job delivered twice)".format(
                       wid, total_recv, distinct))
            return
        if work_closes != W:
            H.fail("WORK-CLOSE COUNT: group wid={0} saw {1} work-close sentinels "
                   "!= W={2} (close did NOT wake all W rangers exactly once -- a "
                   "lost or spurious close-wake on the shared work chan)".format(
                       wid, work_closes, W))
            return
        if results_close_seen != C:
            H.fail("RESULTS-CLOSE SEEN: group wid={0} had {1} collectors observe "
                   "the results-close != C={2} (a collector's results-close wake "
                   "was lost)".format(wid, results_close_seen, C))
            return

        H.op(slot, JOBS_PER_GROUP)
        H.task_done(slot)


def setup(H):
    H.state = {
        "produced": [0] * 1024,
        "distinct": [0] * 1024,
        "total_recv": [0] * 1024,
        "work_closes": [0] * 1024,
        "results_close_seen": [0] * 1024,
        "expected_work_closes": [0] * 1024,
        "expected_results_close": [0] * 1024,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    st = H.state
    produced = sum(st["produced"])
    distinct = sum(st["distinct"])
    total_recv = sum(st["total_recv"])
    work_closes = sum(st["work_closes"])
    expected_work_closes = sum(st["expected_work_closes"])
    results_close_seen = sum(st["results_close_seen"])
    expected_results_close = sum(st["expected_results_close"])

    H.log("groups-jobs produced={0} distinct={1} total_recv={2} | "
          "work_closes={3}/{4} (W per group) | results_close_seen={5}/{6} "
          "(C per group)".format(
              produced, distinct, total_recv,
              work_closes, expected_work_closes,
              results_close_seen, expected_results_close))

    H.check(produced > 0, "no jobs dispatched (test did no work)")
    # (1) JOB CONSERVATION -- the in==out floor.
    H.check(distinct == produced,
            "JOB LOSS: distinct jobs delivered {0} != dispatched {1} (a buffered "
            "job dropped on close / a stranded ranger)".format(distinct, produced))
    H.check(total_recv == distinct,
            "JOB DUP: sum(per-collector sizes) {0} != distinct {1} (a job "
            "delivered to two rangers)".format(total_recv, distinct))
    # (2) WORK-CLOSE COUNT == W per group -- THE non-redundant delta vs p41/p51.
    H.check(work_closes == expected_work_closes,
            "WORK-CLOSE COUNT: {0} work-close sentinels != expected {1} (W per "
            "group) -- close did NOT wake all W rangers exactly once on the "
            "shared work chan (lost or spurious close-wake)".format(
                work_closes, expected_work_closes))
    # (3) RESULTS-CLOSE SEEN == C per group -- the fan-in second close delivered.
    H.check(results_close_seen == expected_results_close,
            "RESULTS-CLOSE SEEN: {0} collectors observed the results-close != "
            "expected {1} (C per group) -- a collector's fan-in close-wake was "
            "lost".format(results_close_seen, expected_results_close))
    # (4) A stranded ranger or collector = a lost close-wake on either channel.
    H.require_no_lost("worker-pool graceful-drain completeness")


if __name__ == "__main__":
    harness.main("p328_worker_pool_graceful_drain", body, setup=setup, post=post,
                 default_funcs=2000, max_funcs=20000,
                 describe="Go worker-pool graceful drain: shared work chan with W "
                          "rangers + fan-in results chan with C collectors; exact "
                          "job conservation (no loss/dup) + DUAL close-count "
                          "(work-close==W woke all rangers once, results-close "
                          "seen==C)")
