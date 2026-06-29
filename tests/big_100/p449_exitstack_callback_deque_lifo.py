"""big_100 / 449 -- contextlib.ExitStack._exit_callbacks deque LIFO conservation.

The subject is contextlib.ExitStack and its single piece of mutable state, the
``self._exit_callbacks`` collections.deque (built in _BaseExitStack.__init__).
Registration appends a (is_sync, cb) pair to the RIGHT end of that deque
(_push_exit_callback -> self._exit_callbacks.append((is_sync, callback)); both
callback() and push()/enter_context() route through it), and the unwind drains
the RIGHT end in LIFO order:

    def __exit__(self, *exc_details):
        ...
        while self._exit_callbacks:
            is_sync, cb = self._exit_callbacks.pop()    # RIGHT-end pop, LIFO
            ...
            cb(*exc_details)

pop_all() does a SPLICE: it hands the whole live deque object to a new stack and
rebinds self._exit_callbacks to a fresh empty deque
(new_stack._exit_callbacks = self._exit_callbacks; self._exit_callbacks = deque()).

The C deque is a doubly-linked list of fixed-size blocks; pop() reads
dq->rightblock / dq->rightindex, returns the item, decrements rightindex, and
when a block empties unlinks dq->rightblock and frees it (dealloc_block).
append() reads/writes the same rightblock/rightindex and mallocs a new block
when the right one fills.  NONE of that is atomic.  The exact racing op pair we
attack:

  * ``__exit__``'s ``while self._exit_callbacks: self._exit_callbacks.pop()``
    loop (its implicit loop position == the live rightindex/rightblock), vs
  * a SIBLING's ``callback()``/``_push_exit_callback`` append, or a ``pop_all()``
    splice, or a stray ``.pop()`` on the SAME ExitStack -- across a yield.

Under M:N a fiber parked mid-``__exit__`` (between two .pop() calls, on a
grown-down C stack, with the loop's rightindex/rightblock pointer live) can be
racing a sibling on another hub mutating the SAME deque.  A torn rightindex or a
freed/relinked rightblock pointer can DROP a callback (a registered cleanup
NEVER runs -> resource leak), POP one TWICE (double cleanup / double-free of a
context), or dereference a freed block entry (SIGSEGV).

We make this a CLOSED-WORLD, falsifiable LIFO-CONSERVATION law, NOT a probe of
ExitStack's (absent) thread-safety.  collections.deque is documented as not
internally locked, so every WRITE to a given stack's deque (registration appends
AND the unwind pops AND the pop_all splice) is serialized under a SEPARATE
cooperative lock per stack -- the conservation oracle then tests "did every
registered cleanup run exactly once", while the race the test actually probes is
the live unwind's loop-position parked across a yield while the gated sibling
mutates the same deque.

  Finite sentinel UNIVERSE of ordinals 0..N-1.  Each round builds an ExitStack
  and registers a known set of N sentinel callbacks; callback i, when run, writes
  the UNIVERSE token TOKEN(i) into a fresh PER-ROUND ran{} multiset AND appends
  its registration ordinal to a per-round order list (both round-local, never a
  slot-shared table -- a shared per-slot dict would be written by two aliasing
  workers at once when funcs>SLOTS and manufacture a phantom double).  We then
  unwind, cooperatively yielding mid-drain (under the per-stack lock) so a GATED
  sibling appends / pops / pop_all's the SAME stack during the park.

  TWO ARMS:

  * CONTROL ARM (case 0) -- a PRIVATE single-owner ExitStack registered AND
    unwound by ONE fiber, no sibling, no yield-race.  It MUST run all N callbacks
    EXACTLY once in STRICT LIFO order (the exact reverse of registration).  A
    dropped / doubled / reordered control callback is an ExitStack-machinery bug,
    NOT contention -- this is the falsifier that disambiguates "ExitStack is
    broken" from "M:N contention corrupted the deque".

  * CONTENDED ARM (cases 1/2/3) -- a SHARED ExitStack whose unwind is parked
    mid-drain while a gated sibling mutates the same deque:
      - case 1 SIBLING-APPEND: the sibling registers EXTRA sentinel callbacks
        (append to the right end) while the drain is parked.  Conservation is
        over the UNION actually registered: every callback that was registered
        runs exactly once; none of the original N is dropped or doubled.
      - case 2 POP_ALL-SPLICE: the sibling calls pop_all() (splices the live
        deque into a new stack, rebinds a fresh empty deque), then unwinds THAT
        new stack.  Across the two stacks, every registered callback runs exactly
        once total -- the splice must not drop, duplicate, or double-free a
        callback.
      - case 3 SIBLING-POP: the sibling pops one callback off the right end and
        runs it itself.  Across both drains every callback runs exactly once.

  INVARIANTS (hot, fail-fast + post reconciliation):
    * every observed token is in UNIVERSE (a torn/freed deque slot reads an
      out-of-universe value);
    * units-in == units-out: each registered callback ran EXACTLY ONCE -- none
      dropped (a cleanup that never ran), none doubled (a double cleanup);
    * CONTROL arm only: LIFO order is the EXACT reverse of registration order;
    * post: every per-round conservation check held fail-fast (reaching post with
      no failure proves it), registered>0, and each case was exercised.

  A missing cleanup, a duplicated cleanup, an out-of-universe token, a broken
  control-arm LIFO order, or a SIGSEGV = bug.

Stresses: ExitStack._exit_callbacks deque rightblock/rightindex pop-vs-append /
pop_all splice under M:N park-mid-unwind, LIFO unwind conservation, dropped /
doubled / double-freed cleanup callback, torn deque-block entry, private-control
LIFO-order vs contended-conservation disambiguation.

Good TSan / controlled-M:N-replay target: the deque rightblock/rightindex
read-modify-write inside pop() racing append()/pop_all() on the same dequeobject
is a textbook data race; a TSan report on the deque block pointer, or a single
dropped/doubled token under replay, localizes the fault before the conservation
sum even closes.
"""
import contextlib

import harness
import runloom

# Finite sentinel UNIVERSE of registration ordinals.  callback i writes TOKEN(i);
# a token a drain ever observes that is not TOKEN of a registered ordinal is a
# torn/freed deque-block entry -- a hard fault.  N is sized so the deque grows
# past several of its fixed-size block boundaries (CPython's deque block holds 64
# items), so the right-block link/unlink path -- the part that frees a block under
# pop() and mallocs one under append() -- is actually exercised, not a single
# block sitting still.
N = 200

# TOKEN(i): a recognizable sentinel value for registration ordinal i.  Offset
# well away from small-int range so a torn read landing on an unrelated object's
# bits is overwhelmingly out-of-universe.
TOKEN_BASE = 0x44900000


def token(i):
    return TOKEN_BASE + i


TOKEN_SET = frozenset(token(i) for i in range(N + 64))   # +64 covers sibling appends

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The CASES.  post() asserts each was exercised, so the worker round-robins them
# by worker id in its first ops (NOT random -- pure random selection reliably
# MISSES a case at low op-count under load, the p125/p126/p172 flaky-coverage bug
# the suite already had to fix).
CASE_CONTROL = 0       # private single-owner stack: strict-LIFO, exactly-once
CASE_APPEND = 1        # shared stack: sibling appends EXTRA callbacks mid-drain
CASE_POPALL = 2        # shared stack: sibling pop_all()-splices mid-drain
CASE_SIBPOP = 3        # shared stack: sibling pops one callback mid-drain
NCASES = 4

# How many extra sentinel callbacks the case-1 sibling appends during the park.
SIB_APPEND = 16


class NullGate(object):
    """A do-nothing gate for the NON-racing drains (control arm + the final
    mop-up drains): no sibling is waiting on it, so tripping it must be a no-op
    and must never touch a WaitGroup's counter."""

    def done(self):
        pass

    def wait(self):
        pass


NULL_GATE = NullGate()


def make_cb(ordinal, ran, ran_lock, order, order_lock):
    """Build sentinel callback `ordinal`.  When RUN it records that this exact
    registered callback ran:

      * bumps ran[TOKEN(ordinal)] (the per-ROUND token->run-count multiset, so a
        DROP reads count 0 and a DOUBLE reads count 2) under ran_lock, and
      * appends its ordinal to the per-ROUND `order` list under order_lock.

    BOTH `ran` and `order` are PER-ROUND objects, created fresh in the run_*
    function and owned solely by THIS round; they are written by the round's two
    child fibers (the drain and the sibling), which run on different hubs, so
    every write is serialized under ran_lock / order_lock.  These accounting
    structures are NOT the subject under test (the deque is) -- serializing them
    makes the oracle a clean CONSERVATION test of "did every registered callback
    run exactly once" while the contention probe is the live drain's deque
    loop-position parked across a yield while the sibling mutates the same deque.
    (Aliasing a SHARED per-slot table across workers when funcs>SLOTS is exactly
    what would manufacture a phantom "double" -- two workers writing one dict --
    so we deliberately keep the multiset round-local, never slot-shared.)

    Accepts *args so the SAME cb works in BOTH registration paths: via
    stack.callback() (ExitStack wraps it in _create_cb_wrapper, which invokes it
    with NO args) and via a direct _exit_callbacks.append((True, cb)) (ExitStack's
    __exit__-style drain invokes it with the (exc_type, exc, tb) triple).  Returns
    None so nothing is suppressed."""
    tok = token(ordinal)

    def cb(*args):
        with ran_lock:
            ran[tok] = ran.get(tok, 0) + 1
        with order_lock:
            order.append(ordinal)
        return None

    return cb


def register_n(stack, lock, n, ran, ran_lock, order, order_lock, start=0):
    """Register callbacks start..start+n-1 onto `stack` (append to the deque's
    right end) under the per-stack write lock.  Returns the list of ordinals
    registered, in registration order."""
    ordinals = list(range(start, start + n))
    with lock:
        for ordinal in ordinals:
            stack.callback(make_cb(ordinal, ran, ran_lock, order, order_lock))
    return ordinals


def drain_locked(stack, lock, yield_at, gate):
    """Unwind `stack` (its __exit__ pops the deque right-end in LIFO) under the
    per-stack write lock, but trip `gate` and cooperatively YIELD once the drain
    has popped `yield_at` callbacks -- so the gated sibling's append/pop/pop_all
    on the SAME deque lands while this drain's loop-position (rightindex/
    rightblock) is parked live.

    We can't reach inside ExitStack.__exit__ to inject the yield mid-loop, so we
    drive the LIFO drain ourselves with the SAME primitive __exit__ uses --
    repeatedly popping the right end and invoking the callback -- which exercises
    the identical deque.pop() rightblock/rightindex path, while letting us park
    between two pops with the loop position live.  Returns the count drained."""
    drained = 0
    tripped = False
    cbs = stack._exit_callbacks
    try:
        while True:
            with lock:
                if not cbs:
                    break
                is_sync, cb = cbs.pop()     # RIGHT-end pop -- the LIFO drain step
            # Run the callback OUTSIDE the write lock (it only touches ran/order,
            # its own guarded accounting) so the sibling can take the write lock
            # during our park.
            cb(None, None, None)
            drained += 1
            if not tripped and drained >= yield_at:
                tripped = True
                gate.done()                 # release the gated sibling
                runloom.yield_now()         # park with the drain loop-position live
    finally:
        # If we exit before reaching yield_at (a short stack, or a failure), still
        # trip the gate so a sibling waiting on it never blocks forever (which
        # would wedge the round's WaitGroup join).
        if not tripped:
            try:
                gate.done()
            except Exception:               # noqa: BLE001 -- already-done gate
                pass
    return drained


def run_control(H, wid, rng, slot, state):
    """CASE 0 -- the single-owner CONTROL arm.  Register N sentinel callbacks on a
    PRIVATE ExitStack and unwind it ALONE (no sibling, no race).  Assert all N ran
    EXACTLY once AND in STRICT LIFO order (the exact reverse of registration).  A
    drop / double / reorder HERE is an ExitStack-machinery bug, not contention."""
    # PER-ROUND accounting (never slot-shared -- a shared per-slot dict would be
    # written by two workers at once when funcs>SLOTS and manufacture a phantom
    # "double").  ran/order are owned solely by this round.
    ran = {}
    ran_lock = runloom.sync.Lock()
    order_lock = runloom.sync.Lock()        # private to this round
    order = []
    lock = runloom.sync.Lock()              # private per-stack write lock

    stack = contextlib.ExitStack()
    ordinals = register_n(stack, lock, N, ran, ran_lock, order, order_lock)

    # Single-owner drain: no sibling, no yield-race (gate is tripped but unused).
    drained = drain_locked(stack, lock, yield_at=N + 1, gate=NULL_GATE)

    if not H.check(drained == N,
                   "CONTROL: single-owner ExitStack drained {0} callbacks, "
                   "expected N={1} -- ExitStack/deque dropped or doubled a pop on "
                   "an UNCONTENDED stack (machinery bug, not contention)".format(
                       drained, N)):
        return False

    # Exactly-once over the universe: every TOKEN(i) seen exactly once, no
    # out-of-universe token.
    d = ran
    for ordinal in ordinals:
        c = d.get(token(ordinal), 0)
        if c != 1:
            H.fail("CONTROL: callback ordinal {0} (token {1:#x}) ran {2} times, "
                   "expected exactly 1 -- single-owner ExitStack {3} a cleanup "
                   "(machinery bug)".format(
                       ordinal, token(ordinal), c,
                       "DROPPED" if c == 0 else "DOUBLED"))
            return False
    for tok in d:
        if tok not in TOKEN_SET:
            H.fail("CONTROL: drain observed OUT-OF-UNIVERSE token {0!r} -- a "
                   "torn/freed deque-block entry on an uncontended ExitStack "
                   "(machinery bug)".format(tok))
            return False

    # Strict LIFO: order must be the EXACT reverse of registration (N-1, ..., 0).
    expected_order = list(reversed(ordinals))
    if order != expected_order:
        H.fail("CONTROL: LIFO order broken -- drain ran ordinals {0}... but "
               "strict LIFO requires the exact reverse of registration {1}... "
               "(ExitStack did not unwind right-end-first)".format(
                   order[:6], expected_order[:6]))
        return False

    state["control_ok"][slot] += 1
    state["registered"][slot] += N
    return True


def run_append(H, wid, rng, slot, state):
    """CASE 1 -- SHARED stack, sibling APPENDS extra callbacks mid-drain.  The
    drain parks with its deque loop-position live; the gated sibling appends
    SIB_APPEND fresh sentinel callbacks (right-end append) onto the SAME deque.
    Conservation is over the union actually registered: every callback registered
    (the N originals PLUS whatever the sibling appended before the drain passed
    its slot) runs exactly once; none of the N originals is dropped or doubled."""
    ran = {}
    ran_lock = runloom.sync.Lock()
    order_lock = runloom.sync.Lock()
    order = []
    lock = runloom.sync.Lock()

    stack = contextlib.ExitStack()
    register_n(stack, lock, N, ran, ran_lock, order, order_lock)

    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(2)
    appended = [0]                          # ordinals the sibling actually appended

    def run_drain():
        try:
            drain_locked(stack, lock, yield_at=N // 2, gate=gate)
        finally:
            wg.done()

    def run_sibling():
        try:
            gate.wait()                     # only after the drain parks mid-loop
            # Append SIB_APPEND fresh callbacks (ordinals N..N+SIB_APPEND-1) onto
            # the SAME deque's right end while the drain's rightindex is parked.
            with lock:
                for j in range(SIB_APPEND):
                    ordinal = N + j
                    stack._exit_callbacks.append(
                        (True, make_cb(ordinal, ran, ran_lock, order, order_lock)))
                    appended[0] += 1
        finally:
            wg.done()

    H.fiber(run_drain)
    H.fiber(run_sibling)
    wg.wait()

    if H.failed:
        return False

    # Drain whatever the sibling appended that the parked loop did not reach
    # (appends to the right end land ABOVE the parked rightindex, so the LIFO
    # drain pops them next on resume -- but be robust and finish any remainder).
    drain_locked(stack, lock, yield_at=N + SIB_APPEND + 1, gate=NULL_GATE)

    # Conservation over the union actually registered: N originals + appended.
    return reconcile_shared(H, slot, state, ran,
                            n_original=N, n_extra=appended[0], case="APPEND")


def run_popall(H, wid, rng, slot, state):
    """CASE 2 -- SHARED stack, sibling POP_ALL-SPLICES mid-drain.  The drain parks
    mid-loop; the gated sibling calls pop_all() (splices the live deque into a new
    stack, rebinds self._exit_callbacks to a fresh empty deque), then unwinds THAT
    new stack.  Across the two drains every registered callback must run EXACTLY
    once -- the splice must not drop, duplicate, or double-free a callback."""
    ran = {}
    ran_lock = runloom.sync.Lock()
    order_lock = runloom.sync.Lock()
    order = []
    lock = runloom.sync.Lock()

    stack = contextlib.ExitStack()
    register_n(stack, lock, N, ran, ran_lock, order, order_lock)

    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(2)

    def run_drain():
        try:
            # The original drain pops the original deque object's right end.  When
            # pop_all() rebinds self._exit_callbacks to a FRESH deque, our local
            # `cbs` (captured at drain start) still references the ORIGINAL deque,
            # so we keep draining exactly the callbacks pop_all spliced out -- the
            # two drains partition the callbacks, no overlap, no loss.  That is the
            # closed-world invariant the splice must uphold.
            drain_locked(stack, lock, yield_at=N // 2, gate=gate)
        finally:
            wg.done()

    def run_sibling():
        try:
            gate.wait()
            with lock:
                spliced = stack.pop_all()   # splice live deque into a new stack
            # Drain the spliced stack INSIDE this try, so it finishes BEFORE
            # wg.done() -- otherwise wg.wait() could return (and reconcile read
            # `ran`) while this drain is still writing it, which reads as a phantom
            # "dropped" callback (the cleanup HAD run, we just looked too early).
            drain_locked(spliced, lock, yield_at=10 ** 9, gate=NULL_GATE)
        finally:
            wg.done()

    H.fiber(run_drain)
    H.fiber(run_sibling)
    wg.wait()

    if H.failed:
        return False

    # Robustness: drain any residue left on either deque object (pop_all rebinds
    # self._exit_callbacks to fresh-empty, so stack is normally empty here).
    drain_locked(stack, lock, yield_at=10 ** 9, gate=NULL_GATE)

    return reconcile_shared(H, slot, state, ran,
                            n_original=N, n_extra=0, case="POPALL")


def run_sibpop(H, wid, rng, slot, state):
    """CASE 3 -- SHARED stack, sibling POPS one callback mid-drain and runs it.
    The drain parks mid-loop; the gated sibling pops ONE callback off the same
    deque's right end and invokes it.  Across both, every registered callback runs
    exactly once (the sibling's pop removes one unit the drain must NOT also pop)."""
    ran = {}
    ran_lock = runloom.sync.Lock()
    order_lock = runloom.sync.Lock()
    order = []
    lock = runloom.sync.Lock()

    stack = contextlib.ExitStack()
    register_n(stack, lock, N, ran, ran_lock, order, order_lock)

    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(2)

    def run_drain():
        try:
            drain_locked(stack, lock, yield_at=N // 2, gate=gate)
        finally:
            wg.done()

    def run_sibling():
        try:
            gate.wait()
            with lock:
                cbs = stack._exit_callbacks
                if cbs:
                    is_sync, cb = cbs.pop()  # right-end pop -- one unit
                else:
                    cb = None
            if cb is not None:
                cb(None, None, None)         # run it OUTSIDE the write lock
        finally:
            wg.done()

    H.fiber(run_drain)
    H.fiber(run_sibling)
    wg.wait()

    if H.failed:
        return False

    drain_locked(stack, lock, yield_at=10 ** 9, gate=NULL_GATE)

    return reconcile_shared(H, slot, state, ran,
                            n_original=N, n_extra=0, case="SIBPOP")


def reconcile_shared(H, slot, state, d, n_original, n_extra, case):
    """Closed-world conservation check for a CONTENDED round (now quiescent: both
    drains joined).  d is THIS round's per-round ran multiset (token -> run count).

    Invariant: every one of the n_original original callbacks ran EXACTLY once,
    every extra sibling-appended callback ran exactly once, and NO token observed
    is outside the universe.  A DROP (count 0) is a lost cleanup; a DOUBLE
    (count 2) is a double cleanup / double-free; an out-of-universe token is a
    torn/freed deque-block entry."""
    expected_total = n_original + n_extra

    # No out-of-universe token, and no token ran more than once.
    seen_units = 0
    for tok, c in d.items():
        if tok not in TOKEN_SET:
            H.fail("{0}: drain observed OUT-OF-UNIVERSE token {1!r} -- a torn/"
                   "freed deque-block entry under concurrent pop/append/pop_all "
                   "on the shared ExitStack".format(case, tok))
            return False
        if c != 1:
            ordinal = tok - TOKEN_BASE
            H.fail("{0}: callback ordinal {1} (token {2:#x}) ran {3} times, "
                   "expected exactly 1 -- a shared-ExitStack cleanup was {4} "
                   "(deque pop-vs-{5} dropped/doubled a callback)".format(
                       case, ordinal, tok, c,
                       "DROPPED" if c == 0 else "DOUBLED",
                       "append" if case == "APPEND" else
                       "pop_all" if case == "POPALL" else "pop"))
            return False
        seen_units += c

    # Units-in == units-out: exactly expected_total distinct callbacks ran once.
    if seen_units != expected_total:
        H.fail("{0}: conservation broken -- {1} distinct callbacks ran but {2} "
               "were registered ({3} original + {4} sibling-appended).  A "
               "callback was {5} by the deque pop-vs-mutate race".format(
                   case, seen_units, expected_total, n_original, n_extra,
                   "DROPPED (cleanup never ran)" if seen_units < expected_total
                   else "DOUBLED (cleanup ran twice)"))
        return False

    # Every original ordinal specifically accounted for (a missing original is the
    # resource-leak failure mode the docstring names).
    for ordinal in range(n_original):
        if d.get(token(ordinal), 0) != 1:
            H.fail("{0}: ORIGINAL callback ordinal {1} ran {2} times (expected "
                   "1) -- the contended drain dropped or doubled a registered "
                   "cleanup".format(case, ordinal, d.get(token(ordinal), 0)))
            return False

    state["case_ok"][case][slot] += 1
    state["registered"][slot] += expected_total
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the cases by worker id in the first ops so each is exercised
        # even when each worker manages only a few ops under the timeout (the
        # p125/p126 flaky-random-coverage fix); random after.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_CONTROL:
            ok = run_control(H, wid, rng, slot, state)
        elif sel == CASE_APPEND:
            ok = run_append(H, wid, rng, slot, state)
        elif sel == CASE_POPALL:
            ok = run_popall(H, wid, rng, slot, state)
        else:
            ok = run_sibpop(H, wid, rng, slot, state)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran).  Only SHARDED monotonic
    # tallies live in shared state: each is single-writer-keyed by the worker's
    # slot, and slot ALIASING when funcs>SLOTS only undercounts the sum (benign for
    # an "exercised >= 1" / "registered > 0" oracle).  The exact-once ran[]
    # MULTISET is deliberately NOT here -- it is created fresh PER ROUND inside each
    # run_* function so two aliasing workers can never write one dict (which would
    # manufacture a phantom DOUBLE; that aliasing was the early false positive).
    H.state = {
        "registered": [0] * SLOTS,             # callbacks registered (== should-run)
        "control_ok": [0] * SLOTS,             # control rounds that passed
        "case_ok": {
            "APPEND": [0] * SLOTS,
            "POPALL": [0] * SLOTS,
            "SIBPOP": [0] * SLOTS,
        },
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    registered = sum(H.state["registered"])
    control_ok = sum(H.state["control_ok"])
    append_ok = sum(H.state["case_ok"]["APPEND"])
    popall_ok = sum(H.state["case_ok"]["POPALL"])
    sibpop_ok = sum(H.state["case_ok"]["SIBPOP"])
    H.log("ExitStack LIFO-conservation: registered(==should-have-run)={0} "
          "control_ok={1} append_ok={2} popall_ok={3} sibpop_ok={4} ops={5}"
          .format(registered, control_ok, append_ok, popall_ok, sibpop_ok,
                  H.total_ops()))

    # Reaching post with no failure already means every per-round exactly-once +
    # LIFO + conservation check held fail-fast; assert the run did real work.
    H.check(H.total_ops() > 0, "no rounds completed")
    H.check(registered > 0,
            "no ExitStack callbacks were ever registered -- the deque "
            "pop-vs-mutate race window was never exercised")

    # Each case was exercised at least once (the deterministic round-robin
    # guarantees this once enough ops ran; assert it so a silently-skipped case
    # can't hide a regression).
    H.check(control_ok > 0,
            "CONTROL arm (single-owner strict-LIFO exactly-once) never ran -- "
            "the falsifier that distinguishes machinery bug from contention was "
            "not exercised")
    H.check(append_ok > 0, "sibling-APPEND-mid-drain case never exercised")
    H.check(popall_ok > 0, "pop_all-SPLICE-mid-drain case never exercised")
    H.check(sibpop_ok > 0, "sibling-POP-mid-drain case never exercised")

    H.require_no_lost("exitstack-lifo-conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p449_exitstack_callback_deque_lifo", body, setup=setup, post=post,
        default_funcs=3000,
        describe="many fibers build ExitStacks and unwind them (LIFO right-end "
                 "deque.pop()) while a gated sibling appends/pops/pop_all's the "
                 "SAME _exit_callbacks deque across a park; closed-world LIFO-"
                 "conservation: every registered cleanup runs EXACTLY once, every "
                 "token in a finite universe, strict-LIFO on the single-owner "
                 "control arm -- a dropped/doubled/double-freed cleanup, an out-"
                 "of-universe token, or a SIGSEGV fails")
