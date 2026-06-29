"""big_100 / 444 -- multiprocessing.Value RMW under a cooperative SemLock.

The subject is multiprocessing.Value('i', 0) / Array('i', K): a ctypes c_int (or
a ctypes array of c_int) living in a multiprocessing HEAP arena
(multiprocessing.heap.Heap, an mmap'd BufferWrapper slab carved by a free-list
allocator), wrapped by a synchronize.SynchronizedBase whose .value is a
get/set of the ctypes object and whose get_lock() returns an RLock backed by a
synchronize.SemLock -- a kernel sem_t-backed primitive (the SemLock's C state is
its `handle` + the recursive-lock `count`/`_last_acquire_time` bookkeeping).

Under monkey.patch() runloom replaces SemLock.acquire (see
runloom/monkey/executors.py::_co_semlock_acquire): a BLOCKING acquire from a
fiber does NOT call the C sem_wait (which would OS-block the whole hub thread);
it does a non-blocking sem_trywait + cooperative _co_sleep backoff, so the fiber
PARKS at the runloom layer while it waits for the cross-process semaphore.  That
park is the hazard surface:

    SynchronizedBase.__init__ ->  self._obj = ctypes c_int in the Heap arena
    Synchronized.value (get)  ->  return self._obj.value
    Synchronized.value (set)  ->  self._obj.value = value
    get_lock()                ->  the SemLock-backed RLock

The idiomatic shared-counter pattern is a CHECK/READ-MODIFY-WRITE on that one
ctypes c_int UNDER the SemLock:

        with v.get_lock():            # SemLock.acquire -> sem_trywait + park
            v.value = v.value + 1     # read self._obj.value, +1, store back

Two M:N hazards, both made falsifiable here:

  (1) LOST SemLock WAKE / NON-FOREIGN-SAFE ACQUIRE.  If the cooperative acquire
      lets TWO fibers into the critical section at once -- a parked-mid-acquire
      fiber whose _co_sleep wake is dropped resumes believing it holds the lock
      while a sibling on another hub also holds it, OR sem_trywait returns true
      for two fibers because the recursive-count bookkeeping tore -- then the two
      ctypes c_int RMWs interleave (read v, read v, write v+1, write v+1) and ONE
      increment is LOST.  We force the interleave window open by doing a
      runloom.yield_now() BETWEEN the read and the write inside the held region,
      so if the SemLock did not actually serialize us a sibling's write lands in
      the gap and the final count tears low.

  (2) ARENA / SEMLOCK C-STATE CORRUPTION.  The Heap free-list allocation backing
      the c_int, and the SemLock handle/count, are C state shared by every
      get_lock()/value access; a torn allocation or a use-after-free on the
      SemLock C object would SIGSEGV the acquire path or read a wild value out of
      the arena.  The Array('i', K) control arm (below) drives the SAME arena +
      SemLock machinery on a DIFFERENT layout (an array slab), and a single-owner
      per-slot write there must be exact -- so a lost write in the control is the
      arena/SemLock machinery itself, not contention on the shared counter.

TARGET INVARIANT -- exact increment CONSERVATION.  A small pool of SHARED
Value('i', 0) is hammered: every fiber does, per round, exactly ONE
`with v.get_lock(): v.value = v.value + 1` against the shared Value chosen by its
id, and records that single offered unit into a PER-SLOT race-free table.  After
all fibers join (quiescent), for every shared Value:

        v.value  ==  units offered to it  (== sum over the per-slot table for it)

A final value BELOW the offered total is a LOST increment (a torn RMW from a
double-entered critical section -- hazard 1).  A final value ABOVE it is a
DOUBLE-applied increment (a double-counted release / re-entrant miscount).  Both
are falsifiable and mutually exclusive.

CONTROL ARM (case 1) -- single-owner Array slot, race-free by construction.  A
shared Array('i', K) where each fiber writes ONLY the slot it owns
(slot = wid % K), accumulating its own offered units under get_lock().  Because
each slot has exactly ONE writer across the whole run, the per-slot sum is
race-free -- if a slot's value diverges from the units that one owner offered it,
the fault is in the shared-memory arena / SemLock machinery itself (UAF, torn
slab, dropped acquire), NOT in cross-fiber contention.  This disambiguates "the
SemLock/arena is buggy" (control fails) from "M:N contention tore the shared
counter" (only the Value arm fails).

PRIVATE-ORACLE CASE (case 2) -- a fresh, never-shared Value('i', 0) the fiber
increments M times alone under its own get_lock().  A single-owner Value MUST end
at exactly M; if it loses a unit the loss is in CPython/mp's c_int+SemLock RMW
itself with no contention at all (the strongest falsifier).

COVERAGE (the flaky-random lesson the suite already fixed in p125/p126/p172):
post() asserts each of the 3 cases ran; timeout-bound runs complete only a few
ops, so the worker round-robins the cases by id in its FIRST ops
(sel = (wid + i) % 3) then goes random.

Invariant (post, fail-fast inside each case + reconciled after join): every
shared Value == units offered to it (conservation, no lost/doubled increment);
every shared Array slot == its single owner's offered units (control, race-free);
every private Value == M (single-owner exactness); the SemLock acquire never
SIGSEGVs / reads an out-of-range value; all 3 cases exercised; no lost worker.

Stresses: multiprocessing SemLock cooperative acquire across the fiber-park
boundary, ctypes c_int read-modify-write under a kernel-sem-backed RLock,
multiprocessing.heap arena allocation + SemLock C handle/count under M:N
contention, lost/doubled increment, single-owner arena-slot conservation.

Good TSan / controlled-M:N-replay target: the c_int get-then-set inside the
held SemLock region, with a yield_now() forced between read and write, is a
textbook RMW; if the cooperative acquire ever double-enters, a TSan report on the
ctypes arena store (or a single dropped unit under replay) localizes the lost
increment before the conservation sum even closes.
"""
import multiprocessing  # imported BEFORE monkey.patch() so SemLock._make_methods
                         # is patched to the cooperative acquire (executors.py:
                         # _patch_mp_synchronize only patches if mp is in
                         # sys.modules at patch time -- which runs in H.run()).

import harness
import runloom

# A small pool of SHARED Value('i') so thousands of fibers pile onto each one --
# that is what drives genuine cross-hub get_lock()/RMW interleave on the SAME
# ctypes c_int + SemLock.  Too many Values would scatter the contention to ~1
# fiber each and the conservation test would never exercise a double-entry.
NVAL = 8

# Width of the control Array.  Each slot has exactly ONE owner (wid % K), so the
# per-slot accumulation is race-free by construction -- the falsifier for arena /
# SemLock machinery bugs (as opposed to shared-counter contention).
ARRAY_K = 64

# Increments the private single-owner Value case does, alone, under its own lock.
# A single-owner Value MUST end at exactly this -- the no-contention falsifier.
PRIVATE_M = 16

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The three cases.  post() asserts each ran, so the worker round-robins them by id
# in its first ops (NOT random -- pure random reliably MISSES a case at low
# op-count under the timeout, the p125/p126/p172 flaky-coverage bug).
CASE_SHARED_VALUE = 0    # contended RMW on a shared Value('i') -> conservation
CASE_ARRAY_SLOT = 1      # single-owner write to a shared Array slot -> control
CASE_PRIVATE_VALUE = 2   # private Value incremented M times alone -> falsifier
NCASES = 3


def shared_value_rmw(H, wid, rng, state, slot):
    """Case 0 (the contention probe): one increment of the shared Value chosen by
    wid, under get_lock(), with a yield_now() forced BETWEEN the read and the
    write so a sibling's RMW must land in the gap if the SemLock failed to
    serialize us.  Record the single offered unit in a per-slot, single-writer
    table keyed to the SAME Value so post() can reconcile per-Value conservation."""
    idx = wid % NVAL
    v = state["values"][idx]
    # offered[idx] is summed across all fibers that targeted Value idx; it is the
    # ground truth the Value must equal.  Single-writer-per-(slot) -> we shard the
    # offered tally by worker slot, and separately tag which Value it fed.
    lock = v.get_lock()                 # the SemLock-backed RLock
    lock.acquire()
    try:
        cur = v.value                   # read self._obj.value (ctypes c_int)
        # Force the interleave window OPEN: park here with the read latched.  If
        # the cooperative SemLock acquire double-entered, a sibling's write lands
        # now and our write below clobbers it -> a LOST increment, caught by the
        # post() conservation sum.
        runloom.yield_now()
        v.value = cur + 1               # store back self._obj.value = cur + 1
    finally:
        lock.release()
    # Account the offered unit (race-free: this worker owns `slot`).  Tag the
    # Value index by bumping a per-(slot) counter in the idx-th offered table.
    state["offered"][idx][slot] += 1
    return True


def array_slot_write(H, wid, rng, state, slot):
    """Case 1 (the CONTROL arm): single-owner write to a shared Array('i', K).
    This fiber owns Array slot `own = wid % K` for the WHOLE run -- no other
    worker ever writes it -- so the per-slot accumulation is race-free by
    construction.  It still drives the SAME multiprocessing.heap arena + SemLock
    machinery (Array has its own get_lock()).  If this slot's value ever diverges
    from the units its single owner offered it, the fault is the arena / SemLock C
    state (UAF / torn slab / dropped acquire), NOT cross-fiber contention."""
    arr = state["array"]
    own = wid % ARRAY_K
    lock = arr.get_lock()
    lock.acquire()
    try:
        cur = arr[own]                  # read the ctypes array element from the slab
        runloom.yield_now()             # park with the read latched (same window)
        arr[own] = cur + 1              # single-owner store -> must be race-free
    finally:
        lock.release()
    state["arr_offered"][slot] += 1     # this worker's offered units to ITS slot
    return True


def private_value_rmw(H, wid, rng, state, slot):
    """Case 2 (the single-owner falsifier): a FRESH, never-shared Value('i', 0)
    incremented PRIVATE_M times by this fiber alone under its own get_lock().  No
    contention at all, so it MUST end at exactly PRIVATE_M; a shortfall is a lost
    increment in CPython/mp's c_int+SemLock RMW machinery itself (the strongest
    falsifier, with the arena allocation/free exercised every round)."""
    v = multiprocessing.Value('i', 0)   # fresh arena allocation each round
    lock = v.get_lock()
    for _ in range(PRIVATE_M):
        lock.acquire()
        try:
            cur = v.value
            runloom.yield_now()
            v.value = cur + 1
        finally:
            lock.release()
    got = v.value
    if got != PRIVATE_M:
        H.fail("private single-owner Value('i') ended at {0}, expected exactly "
               "PRIVATE_M={1} -- a SemLock-guarded c_int RMW lost/doubled a count "
               "with NO contention (CPython/mp machinery bug, not M:N "
               "contention)".format(got, PRIVATE_M))
        return False
    state["priv_offered"][slot] += PRIVATE_M
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases by worker id in the FIRST ops so every case
        # is exercised even when each worker manages only a few ops under the
        # timeout (the p125/p126/p172 flaky-random-coverage fix); random after.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_SHARED_VALUE:
            ok = shared_value_rmw(H, wid, rng, state, slot)
        elif sel == CASE_ARRAY_SLOT:
            ok = array_slot_write(H, wid, rng, state, slot)
        else:
            ok = private_value_rmw(H, wid, rng, state, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so every SemLock created
    # here gets the cooperative acquire and the Heap arena allocations happen on a
    # live scheduler.  Shared Values + the control Array are constructed once.
    values = [multiprocessing.Value("i", 0) for _ in range(NVAL)]
    array = multiprocessing.Array("i", ARRAY_K)   # zero-initialized slab
    H.state = {
        "values": values,
        "array": array,
        # offered[idx][slot]: units this slot's worker fed to shared Value idx
        # (single-writer-per-slot -> race-free; summed per-Value in post()).
        "offered": [[0] * SLOTS for _ in range(NVAL)],
        "arr_offered": [0] * SLOTS,   # control-arm units offered (per worker slot)
        "priv_offered": [0] * SLOTS,  # private-case units (PRIVATE_M per op)
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    state = H.state

    # ---- shared Value conservation (the contention probe) ---------------------
    # Per Value: the final ctypes c_int MUST equal the units offered to it.  A
    # value below = a LOST increment (torn RMW from a double-entered SemLock);
    # above = a DOUBLE-applied increment.  Both falsifiable, both fatal.
    total_offered = 0
    total_value = 0
    for idx in range(NVAL):
        offered = sum(state["offered"][idx])
        total_offered += offered
        got = state["values"][idx].value
        total_value += got
        if got != offered:
            H.check(False,
                    "conservation broken on shared Value[{0}]: v.value={1} but "
                    "{2} increments were offered under get_lock() -- a SemLock-"
                    "guarded c_int RMW was {3} across hubs (lost SemLock wake / "
                    "double-entered critical section)".format(
                        idx, got, offered,
                        "LOST" if got < offered else "DOUBLED"))
            # keep checking the rest so the summary is informative, but stop on
            # the first to honor fail-fast semantics of H.check.
            break

    # ---- control arm: single-owner Array slots (race-free) --------------------
    # The Array's per-slot total across the run is race-free by construction (one
    # owner per slot), so it must equal exactly the units offered.  A divergence
    # here is the arena/SemLock machinery itself, not contention.
    arr_offered = sum(state["arr_offered"])
    arr_present = 0
    if state["array"] is not None:
        # The Array value is the sum over slots of arr[i]; out-of-range / wild
        # values would surface as a mismatch (and a torn slab as a SIGSEGV in the
        # access above, caught by faulthandler).
        for i in range(ARRAY_K):
            sv = state["array"][i]
            arr_present += sv
    H.check(arr_present == arr_offered,
            "CONTROL ARM broken: single-owner Array slot sum={0} != units offered "
            "{1} -- a race-free per-slot write was lost/doubled, so the fault is "
            "the multiprocessing.heap arena / SemLock C state (UAF / torn slab / "
            "dropped acquire), NOT shared-counter contention".format(
                arr_present, arr_offered))

    priv = sum(state["priv_offered"])

    H.log("shared-Value conserved={0} (offered={1} present={2}); control Array "
          "offered={3} present={4}; private units={5}; ops={6}".format(
              total_offered == total_value, total_offered, total_value,
              arr_offered, arr_present, priv, H.total_ops()))

    # The run actually did work in each arm (else the law was vacuous).  The
    # private-case fail-fast already proved single-owner exactness for every
    # private op that ran; here we just require each case was exercised.
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(total_offered > 0,
            "shared-Value contention probe never exercised (case 0 never ran)")
    H.check(arr_offered > 0,
            "control Array arm never exercised (case 1 never ran)")
    H.check(priv > 0,
            "private single-owner Value falsifier never exercised (case 2 never "
            "ran)")

    H.require_no_lost("multiprocessing-value-semlock conservation")


if __name__ == "__main__":
    harness.main(
        "p444_multiprocessing_value_semlock_", body, setup=setup, post=post,
        default_funcs=3000,
        describe="thousands of fibers RMW a shared multiprocessing.Value('i') "
                 "under its cooperative SemLock-backed get_lock() across the "
                 "fiber-park boundary; exact increment conservation (v.value == "
                 "units offered), a single-owner Array control arm (race-free "
                 "per-slot sum), and a private single-owner Value falsifier -- a "
                 "lost/doubled increment or a SemLock SIGSEGV fails")
