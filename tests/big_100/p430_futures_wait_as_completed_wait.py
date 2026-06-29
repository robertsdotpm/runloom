"""big_100 / 430 -- concurrent.futures.wait()/as_completed() shared _Waiter set
conservation under M:N inline completion.

The subject is STOCK concurrent.futures.wait(return_when=...) and as_completed()
(executors.py:34 "Future.result/wait/as_completed themselves need no patch --
they already cooperate whenever the Future is completed from within a fiber").
Their internals install ONE shared _Waiter object into EVERY future's _waiters
list, and the COMPLETION of each future mutates that SHARED _Waiter inline on the
COMPLETING fiber.  Cite the exact CPython _base.py paths (3.14t):

  * _create_and_install_waiters (_base.py:149) builds one _AllCompletedWaiter /
    _AsCompletedWaiter / _FirstCompletedWaiter and does
        for f in fs: f._waiters.append(waiter)
    under _AcquireFutures (_base.py:135), which acquires ALL the futures'
    _condition locks in id-SORTED order (the deadlock-avoidance ordering).
  * Future.set_result (_base.py:546) / set_exception (:561), under self._condition:
        self._state = FINISHED
        for waiter in self._waiters:
            waiter.add_result(self)       # <-- INLINE, on the completing fiber
        self._condition.notify_all()
  * _AllCompletedWaiter.add_result (_base.py:120):
        super().add_result(future)        # _Waiter.add_result: finished_futures.append(future)  -- NO LOCK
        self._decrement_pending_calls()   # num_pending_calls -= 1 under self.lock; ==0 -> event.set()
    The bare list .append at _base.py:60 runs OUTSIDE _AllCompletedWaiter.lock --
    only the pending-counter decrement is locked.  So the SHARED finished_futures
    list is grown by an UNLOCKED ob_item append on whichever fiber completed the
    future, while a sibling completer on ANOTHER hub appends to the same list.
  * wait() (_base.py:300) parks on waiter.event.wait(timeout) on the CALLER's hub,
    then (_base.py:305) done.update(waiter.finished_futures).
  * as_completed() (_base.py:239) swaps finished = waiter.finished_futures;
    waiter.finished_futures = [] under waiter.lock, then _yield_finished_futures
    (_base.py:171) pops them, each time doing  with f._condition: f._waiters.remove(waiter).

THE M:N HAZARD (the exact C-level state + racing op pair):
  N completing fibers, scattered across hubs by the fiber-backed executor, EACH
  run  list.append(self) on the shared waiter.finished_futures (a list ob_item
  store + Py_SIZE bump on the SHARED list object, _AllCompletedWaiter's append
  UNLOCKED) and decrement the pending counter; VERSUS the wait()/as_completed()
  caller parked on waiter.event reading that same finished_futures list and
  the num_pending_calls counter.  A torn list.append (two appenders racing the
  ob_item realloc / Py_SET_SIZE on one list with the GIL off) LOSES a future from
  the result set or DOUBLES it; a raced num_pending_calls -= 1 fires event.set()
  early (returns with done<N) or never (hangs).  This is the same lost-update
  the GIL used to hide, here on the result-set accounting of wait/as_completed --
  a class NEITHER p410 (add_done_callback firing) NOR p411 (cancel-vs-run state)
  touches: they never check WHICH futures the shared _Waiter is told completed
  under FIRST_COMPLETED / ALL_COMPLETED return_when.

TARGET INVARIANT -- CLOSED-WORLD set CONSERVATION (falsifiable, not a racy probe).
Per round submit N futures to a PRIVATE CoThreadPoolExecutor over a finite
sentinel UNIVERSE of payloads; the worker runs the cases below.  Two
mutually-exclusive failure modes, each made falsifiable:
  * ALL_COMPLETED: returned.done MUST be EXACTLY the N submitted futures, every
    future.done() True, returned.not_done empty -- |done|==N and
    set(done)==set(submitted).  A future MISSING from done (torn append / early
    event) or a FOREIGN future in done both fail; result values checked against
    g(payload) so a torn/mis-routed completion is caught.
  * as_completed: MUST yield EXACTLY the N futures, each EXACTLY once -- a private
    per-future single-writer 'yielded' cell == 1 for ALL N (a 0 = lost from the
    waiter set, a 2 = double-registered / double-yielded).
  * FIRST_COMPLETED: the single returned done future MUST genuinely be .done().

SINGLE-OWNER CONTROL ARM (case SERIAL).  Round-robin a case where the futures
complete SERIALLY -- a private executor with max_workers==1 and a chain where
each task busy-waits (cooperatively) on the prior future's done(), so the shared
_Waiter's finished_futures is built ONE race-free append at a time.  It must
STILL yield all N exactly once and ALL_COMPLETED must still return |done|==N.
Divergence HERE -- where the appends to the _Waiter list are serialized by
construction -- isolates the _Waiter.finished_futures list mutation itself as the
fault (not M:N contention), exactly as p405's private Counter and p412's private
BoundedSemaphore disambiguate "primitive is buggy" from "contention dropped it".

require_no_lost() in post catches an as_completed()/wait() that WEDGES on a
swallowed completion event (a never-fired num_pending_calls==0 / event.set()).

Coverage (the p125/p126/p172 flaky-random lesson): post() asserts each case ran;
the worker round-robins the cases by (wid + i) % NCASES in its first ops, then
random, so coverage holds whether one worker does NCASES ops or NCASES workers do
one each.

Invariant (hot, fail-fast): per round ALL_COMPLETED done==submitted set,
not_done empty, each future done(); as_completed yields each submitted future
exactly once (per-future cell==1); FIRST_COMPLETED's returned future is done();
every non-cancelled result==g(payload).
Invariant (post): per-slot sum of futures-conserved > 0 (window not vacuous),
every case exercised, no lost worker.

Stresses: concurrent.futures.wait/as_completed shared _Waiter.finished_futures
unlocked list.append vs cross-hub read, _AllCompletedWaiter.num_pending_calls
decrement vs event.set(), _AcquireFutures id-sorted _condition lock order,
fiber-backed ThreadPoolExecutor inline set_result completion across hubs.

Good TSan / controlled-M:N-replay target: N completing fibers' unlocked
list.append onto one shared waiter.finished_futures is a textbook ob_item
read-modify-write data race; a TSan report on that list's ob_item/ob_size store
localizes the lost/doubled future before the set-conservation assert closes.
"""
import harness
import runloom

# Futures per round.  Big enough that the shared _Waiter.finished_futures list is
# grown past several ob_item realloc boundaries by concurrent appenders (where a
# torn realloc loses/doubles an element), and that a dropped future moves the
# conserved count by a detectable unit; small enough that a timeout-bound worker
# completes whole rounds.
BATCH = 24

# Concurrency cap for the CONTENDED arm.  < BATCH so the executor's work queue
# genuinely backs up and completions land on DIFFERENT fibers across hubs while
# the wait()/as_completed() caller is parked on the shared waiter.event -- the
# cross-hub append-vs-read window.  >1 so several completers race the SAME list.
MAX_WORKERS = 6

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Finite sentinel UNIVERSE of payloads.  A future result that is not g(payload)
# for SOME universe payload is a torn/mis-routed completion.  Sized so distinct
# payloads recur across rounds but every observed result stays inside g(UNIVERSE).
UNIVERSE_SIZE = 512
UNIVERSE = tuple(0x43000000 + i for i in range(UNIVERSE_SIZE))

# The cases, round-robined by worker id (NOT random) so post() coverage holds.
CASE_ALL_COMPLETED = 0     # wait(return_when=ALL_COMPLETED): done==submitted set
CASE_AS_COMPLETED = 1      # as_completed(): each future yielded exactly once
CASE_FIRST_COMPLETED = 2   # wait(return_when=FIRST_COMPLETED): returned fut done()
CASE_SERIAL = 3            # single-owner control: max_workers==1, chained completion
NCASES = 4


def g(payload):
    """Deterministic task result.  A non-cancelled future whose result is not
    g(its payload) is a torn / mis-routed completion (the waiter handed back a
    future paired with the wrong slot's result)."""
    return (payload ^ 0x6B6B6B6B) + 0x9E3779B9


def submit_batch(ex, wid, rng):
    """Submit BATCH futures over the sentinel UNIVERSE to `ex`.  Returns
    (futs, payload_of, submitted_ids)."""
    futs = []
    payload_of = {}
    submitted_ids = set()
    for i in range(BATCH):
        payload = UNIVERSE[(wid * 131 + i * 17 + rng.getrandbits(20)) % UNIVERSE_SIZE]
        fut = ex.submit(g, payload)
        fid = id(fut)
        payload_of[fid] = payload
        submitted_ids.add(fid)
        futs.append(fut)
    return futs, payload_of, submitted_ids


def check_results(H, futs, payload_of, case):
    """Every non-cancelled future's result MUST equal g(its payload) -- a torn /
    mis-routed completion fails.  Returns False on the first violation."""
    for fut in futs:
        if fut.cancelled():
            continue
        val = fut.result()
        want = g(payload_of[id(fut)])
        if val != want:
            H.fail("future result {0!r} != g(payload) {1!r} -- torn / mis-routed "
                   "completion handed back by the shared _Waiter (case {2})"
                   .format(val, want, case))
            return False
    return True


def run_all_completed(H, ex, wid, rng):
    """CASE_ALL_COMPLETED: wait(return_when=ALL_COMPLETED).  The shared
    _AllCompletedWaiter's finished_futures list is appended to inline by every
    completing fiber (unlocked _Waiter.add_result) and its num_pending_calls is
    decremented under the waiter lock; only at zero does event.set() fire.  When
    wait() returns, returned.done MUST be EXACTLY the N submitted futures, every
    one .done(), returned.not_done empty -- a future LOST from finished_futures
    (torn append) shows as |done|<N or a missing id; an early event.set() (raced
    num_pending_calls) shows as not_done non-empty with un-done futures."""
    from concurrent.futures import wait, ALL_COMPLETED
    futs, payload_of, submitted_ids = submit_batch(ex, wid, rng)

    res = wait(futs, return_when=ALL_COMPLETED)
    done_ids = {id(f) for f in res.done}

    if len(res.done) != BATCH:
        H.fail("ALL_COMPLETED returned |done|={0}, expected {1} -- the shared "
               "_Waiter dropped or doubled a future in finished_futures (torn "
               "unlocked list.append across completing fibers), or event.set() "
               "fired early on a raced num_pending_calls decrement"
               .format(len(res.done), BATCH))
        return False
    if res.not_done:
        H.fail("ALL_COMPLETED returned {0} NOT-done future(s) -- event fired "
               "before num_pending_calls hit 0 (raced decrement) so wait() "
               "returned with futures still pending".format(len(res.not_done)))
        return False
    if done_ids != submitted_ids:
        H.fail("ALL_COMPLETED done-set != submitted-set (missing={0} "
               "foreign={1}) -- _Waiter.finished_futures corruption: a torn "
               "append on the shared list lost a submitted future or admitted a "
               "foreign one".format(len(submitted_ids - done_ids),
                                    len(done_ids - submitted_ids)))
        return False
    for f in res.done:
        if not f.done():
            H.fail("ALL_COMPLETED returned a future in done that reports "
                   ".done()==False -- the result set and the future state "
                   "disagree (torn waiter accounting)")
            return False
    return check_results(H, futs, payload_of, CASE_ALL_COMPLETED)


def run_as_completed(H, ex, wid, rng):
    """CASE_AS_COMPLETED: as_completed() MUST yield each submitted future EXACTLY
    once.  Internally _AsCompletedWaiter.add_result appends to finished_futures
    under waiter.lock + event.set(); as_completed swaps the list out under the
    same lock and pops each, removing the waiter from f._waiters under
    f._condition.  A per-future SINGLE-WRITER cell counts yields: a 0 = a future
    lost from the waiter set, a 2 = double-registered / double-yielded.  We also
    cross-check the id set and the count, and the result value."""
    from concurrent.futures import as_completed
    futs, payload_of, submitted_ids = submit_batch(ex, wid, rng)
    cell_of = {id(f): [0] for f in futs}   # single-writer-per-future yield cells

    seen_ids = set()
    seen = 0
    for fut in as_completed(futs):
        seen += 1
        fid = id(fut)
        cell_of[fid][0] += 1               # this fiber is the sole writer here
        if fid not in submitted_ids:
            H.fail("as_completed yielded an UNSUBMITTED future {0:#x} -- the "
                   "shared _AsCompletedWaiter.finished_futures admitted a foreign "
                   "entry (torn append across hubs)".format(fid))
            return False
        if fid in seen_ids:
            H.fail("as_completed yielded future {0:#x} TWICE -- the waiter "
                   "re-registered a completion (doubled finished_futures append "
                   "/ stale waiter not removed from f._waiters)".format(fid))
            return False
        seen_ids.add(fid)
        if not fut.done():
            H.fail("as_completed yielded a future that reports .done()==False -- "
                   "it was added to finished_futures before its state settled")
            return False

    if seen != BATCH:
        H.fail("as_completed yielded {0} futures, expected {1} -- a completion "
               "was lost from or doubled in the shared _Waiter set".format(
                   seen, BATCH))
        return False
    if seen_ids != submitted_ids:
        H.fail("as_completed id-set != submitted-set (missing={0} foreign={1}) "
               "-- _Waiter.finished_futures corruption".format(
                   len(submitted_ids - seen_ids), len(seen_ids - submitted_ids)))
        return False
    # Per-future single-writer cells: every future yielded EXACTLY once.
    for fid in submitted_ids:
        c = cell_of[fid][0]
        if c != 1:
            H.fail("future {0:#x} yielded by as_completed {1} times, expected 1 "
                   "-- {2} from the shared waiter set (M:N completion path)"
                   .format(fid, c, "LOST" if c == 0 else "DOUBLE-REGISTERED"))
            return False
    return check_results(H, futs, payload_of, CASE_AS_COMPLETED)


def run_first_completed(H, ex, wid, rng):
    """CASE_FIRST_COMPLETED: wait(return_when=FIRST_COMPLETED).  The shared
    _FirstCompletedWaiter fires event.set() on the FIRST completing fiber's
    add_result (which appended self to finished_futures first).  The single
    returned done future MUST genuinely be .done() -- a returned future that is
    NOT done means event.set() fired on a future whose append was torn (it never
    actually landed in finished_futures, or landed for the wrong object).  We
    then drain the rest via ALL_COMPLETED so the round leaves no future pending
    (and verify the full set conserves on the way out)."""
    from concurrent.futures import wait, FIRST_COMPLETED, ALL_COMPLETED
    futs, payload_of, submitted_ids = submit_batch(ex, wid, rng)

    res = wait(futs, return_when=FIRST_COMPLETED)
    if len(res.done) < 1:
        H.fail("FIRST_COMPLETED returned with EMPTY done set -- event.set() "
               "fired (or wait returned) without any future in "
               "finished_futures (swallowed/torn completion)")
        return False
    for f in res.done:
        if not f.done():
            H.fail("FIRST_COMPLETED returned a future in done that reports "
                   ".done()==False -- a future was added to the shared waiter's "
                   "finished_futures before its state settled (torn publish)")
            return False
        if id(f) not in submitted_ids:
            H.fail("FIRST_COMPLETED returned a FOREIGN future {0:#x} not in the "
                   "submitted set -- _Waiter set corruption".format(id(f)))
            return False
    # Drain the remainder; the full set must still conserve to N.
    res2 = wait(futs, return_when=ALL_COMPLETED)
    if len(res2.done) != BATCH or res2.not_done:
        H.fail("after FIRST_COMPLETED, the draining ALL_COMPLETED returned "
               "|done|={0} not_done={1}, expected {2}/0 -- the shared waiter "
               "lost a future across the two wait() calls".format(
                   len(res2.done), len(res2.not_done), BATCH))
        return False
    if {id(f) for f in res2.done} != submitted_ids:
        H.fail("FIRST_COMPLETED drain done-set != submitted-set -- _Waiter "
               "finished_futures corruption")
        return False
    return check_results(H, futs, payload_of, CASE_FIRST_COMPLETED)


def run_serial_control(H, wid, rng):
    """CASE_SERIAL -- the SINGLE-OWNER CONTROL ARM.  A PRIVATE executor with
    max_workers==1: the CoThreadPoolExecutor's CoSemaphore(1) admits exactly ONE
    task fiber at a time, so each task runs to completion (set_result, which
    appends self to the shared _Waiter.finished_futures inline) BEFORE the next
    task acquires the permit.  Completions therefore land STRICTLY ONE AT A TIME
    and the shared _Waiter.finished_futures list is grown by ONE race-free append
    at a time -- there is no concurrent appender to tear the ob_item store.  (We
    do a single runloom.yield_now() inside each task so the wait()/as_completed()
    caller genuinely parks on the waiter event across the serialized completions,
    rather than the whole batch finishing before the caller looks.)

    NOTE on why there is NO inter-task .done() chain: under M:N the task fibers
    are spawned across hubs and acquire the max_workers==1 permit in SCHEDULER
    order, not submission order (M:N is not asyncio-deterministic).  A chain where
    task i holds the permit while busy-waiting on task i-1.done() therefore
    DEADLOCKS by priority inversion if task i grabs the permit before task i-1 --
    a flaw in the control, not the runtime.  max_workers==1 alone already
    serializes the COMPLETIONS (the only thing the _Waiter accounting cares
    about), which is exactly what this control needs.

    Despite the serialized appends, as_completed() MUST STILL yield all N exactly
    once and ALL_COMPLETED MUST STILL return |done|==N.  If THIS arm diverges --
    where the appends are serialized by construction -- the fault is in the
    _Waiter.finished_futures list mutation / removal machinery itself, NOT M:N
    contention (p405 private-Counter / p412 private-semaphore disambiguation)."""
    from concurrent.futures import (ThreadPoolExecutor, as_completed, wait,
                                    ALL_COMPLETED)

    ex = ThreadPoolExecutor(max_workers=1)
    try:
        def serial_task(payload):
            # One cooperative hand-off so the wait()/as_completed() caller actually
            # parks on the waiter event between serialized completions.
            runloom.yield_now()
            return g(payload)

        futs = []
        payload_of = {}
        submitted_ids = set()
        for i in range(BATCH):
            payload = UNIVERSE[(wid * 71 + i * 13 + rng.getrandbits(18)) % UNIVERSE_SIZE]
            fut = ex.submit(serial_task, payload)
            fid = id(fut)
            payload_of[fid] = payload
            submitted_ids.add(fid)
            futs.append(fut)

        # as_completed over the serially-completing set: still exactly N, once each.
        cell_of = {id(f): [0] for f in futs}
        seen_ids = set()
        seen = 0
        for fut in as_completed(futs):
            seen += 1
            fid = id(fut)
            cell_of[fid][0] += 1
            if fid not in submitted_ids:
                H.fail("SERIAL control: as_completed yielded a foreign future "
                       "{0:#x} -- _Waiter list corruption with appends SERIALIZED "
                       "(fault is in the waiter machinery, not contention)"
                       .format(fid))
                return False
            if fid in seen_ids:
                H.fail("SERIAL control: as_completed double-yielded {0:#x} with "
                       "appends serialized -- _Waiter machinery bug".format(fid))
                return False
            seen_ids.add(fid)

        if seen != BATCH or seen_ids != submitted_ids:
            H.fail("SERIAL control: as_completed yielded {0}/{1}, set-match={2} "
                   "-- a future was lost from the shared _Waiter even though "
                   "completions were SERIALIZED (waiter machinery fault, NOT a "
                   "race)".format(seen, BATCH, seen_ids == submitted_ids))
            return False
        for fid in submitted_ids:
            if cell_of[fid][0] != 1:
                H.fail("SERIAL control: future {0:#x} yielded {1} times (expected "
                       "1) with serialized completion -- _Waiter machinery fault"
                       .format(fid, cell_of[fid][0]))
                return False

        # And ALL_COMPLETED over the same (now finished) set conserves to N.
        res = wait(futs, return_when=ALL_COMPLETED)
        if len(res.done) != BATCH or res.not_done:
            H.fail("SERIAL control: ALL_COMPLETED |done|={0} not_done={1}, "
                   "expected {2}/0 -- serialized completion still lost a future "
                   "(waiter machinery fault)".format(
                       len(res.done), len(res.not_done), BATCH))
            return False
        return check_results(H, futs, payload_of, CASE_SERIAL)
    finally:
        ex.shutdown(wait=True)


def run_round(H, wid, rng, case, state):
    """Drive ONE case.  Cases 0-2 share a CONTENDED private executor
    (max_workers=MAX_WORKERS) so completions race across hubs; case SERIAL builds
    its own max_workers==1 chained executor (the single-owner control).  Returns
    True on a conserved round, False on any invariant break (H.fail recorded)."""
    if case == CASE_SERIAL:
        return run_serial_control(H, wid, rng)

    from concurrent.futures import ThreadPoolExecutor
    ex = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    try:
        if case == CASE_ALL_COMPLETED:
            return run_all_completed(H, ex, wid, rng)
        elif case == CASE_AS_COMPLETED:
            return run_as_completed(H, ex, wid, rng)
        else:
            return run_first_completed(H, ex, wid, rng)
    finally:
        # shutdown(wait=True) joins every task fiber so no completion is still
        # in flight when the next round starts and the executor is dropped.
        ex.shutdown(wait=True)


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    conserved = state["conserved"]
    case_hits = state["case_hits"]
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the cases by worker id over the first ops so every case is
        # exercised even when each worker manages only a few ops under the timeout
        # (the p125/p126/p172 flaky-random-coverage fix); random after.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1

        ok = run_round(H, wid, rng, case, state)
        if not ok:
            return                         # H.fail already recorded
        conserved[slot] += BATCH           # single-writer-per-slot, race-free
        case_hits[case][slot] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so concurrent.futures.*
    # is the cooperative fiber-backed executor + the stock wait/as_completed that
    # cooperate over it.  Per-slot single-writer tally lists.
    H.state = {
        "conserved": [0] * SLOTS,                       # futures conserved (BATCH per round)
        "case_hits": [[0] * SLOTS for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    conserved = sum(H.state["conserved"])
    hits = [sum(H.state["case_hits"][c]) for c in range(NCASES)]
    rounds = sum(hits)
    H.log("rounds: all_completed={0} as_completed={1} first_completed={2} "
          "serial={3} (total {4}); futures conserved={5} ops={6}".format(
              hits[CASE_ALL_COMPLETED], hits[CASE_AS_COMPLETED],
              hits[CASE_FIRST_COMPLETED], hits[CASE_SERIAL], rounds,
              conserved, H.total_ops()))

    # Reaching post with no failure already means every per-round set-conservation
    # check held fail-fast; assert the run actually did work (else it was vacuous).
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(conserved > 0,
            "no wait()/as_completed conservation rounds completed -- the shared "
            "_Waiter set race window was never exercised")
    H.check(conserved == BATCH * rounds,
            "futures-conserved {0} != BATCH*rounds {1} -- a round's set "
            "accounting drifted".format(conserved, BATCH * rounds))

    # Every case (incl. the single-owner SERIAL control) was exercised.
    H.check(hits[CASE_ALL_COMPLETED] > 0, "ALL_COMPLETED case never exercised")
    H.check(hits[CASE_AS_COMPLETED] > 0, "as_completed case never exercised")
    H.check(hits[CASE_FIRST_COMPLETED] > 0, "FIRST_COMPLETED case never exercised")
    H.check(hits[CASE_SERIAL] > 0,
            "SERIAL single-owner control never exercised -- the divergence "
            "isolator did not run")

    # require_no_lost catches an as_completed()/wait() that WEDGED on a swallowed
    # completion event (a never-fired num_pending_calls==0 / event.set()).
    H.require_no_lost("futures-wait-set conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p430_futures_wait_as_completed_wait", body, setup=setup, post=post,
        default_funcs=3000,
        describe="concurrent.futures.wait/as_completed shared _Waiter set "
                 "conservation under M:N inline completion: ALL_COMPLETED "
                 "done==submitted set, as_completed yields each future once, "
                 "FIRST_COMPLETED's future is done(), plus a serial single-owner "
                 "control -- a torn finished_futures append or raced "
                 "num_pending_calls fails")
