"""big_100 / 432 -- Executor.map() order-preserving result generator over a
list of futures completing out-of-order across M:N hubs.

The subject is the STOCK ``concurrent.futures.Executor.map`` (the base-class
method; only ThreadPoolExecutor is fiber-backed by runloom.monkey, so map()
itself is the unmodified CPython 3.14 code).  Its mechanism, verbatim:

    fs = [self.submit(fn, *args) for args in zipped_iterables]   # SUBMIT ORDER
    ...
    def result_iterator():
        try:
            fs.reverse()                       # so pop() takes the FRONT
            while fs:
                ...
                yield _result_or_cancel(fs.pop())   # fs.pop() -> oldest submit
        finally:
            for future in fs:
                future.cancel()

That is the exact internal state we attack: a single Python ``list`` object
``fs`` whose ``ob_item`` backing array is mutated by ``list.pop()`` (it
decrements ob_size and reads the last slot) ONCE PER YIELD, interleaved with
``future.result()`` -- which under runloom.monkey parks the generator fiber on
that future's (cooperative) Condition.  Meanwhile every OTHER future in ``fs``
is being completed by its OWN task fiber on a DIFFERENT hub: each completing
fiber does ``fut.set_result(value)`` (store ``_result`` slot + flip ``_state``
to FINISHED + notify the Condition), wholly independently and out of submit
order.  So the precise racing op PAIR is:

  (generator fiber):  fs.pop()  [list ob_item / ob_size mutation in submit order]
                      + fut.result()  [park on the popped future's Condition]
  vs
  (N completing fibers, cross-hub):  fut.set_result(value)  [_result store +
                      _state flip + Condition.notify], firing OUT of submit order.

map() MUST surrender results in INPUT order even though the underlying futures
finish out of order.  p410/p411 drive SINGLE futures (callback / cancel);
nothing in the suite drives map()'s ORDER-PRESERVING generator over a SHARED
list of futures completing concurrently.  The bug this would catch:

  * a TORN ``fs.pop()`` (ob_size / ob_item read while the list object is
    observed mid-mutation) hands result index i the WRONG future, REORDERING or
    DROPPING a mapped value -- list(map(f, xs))[i] != f(xs[i]);
  * a result delivered to the wrong future's ``_result`` slot, or a
    ``set_result`` whose store / state-flip / notify is observed torn by the
    parked generator on resume, yields a STALE / OUT-OF-UNIVERSE value for
    index i;
  * a dropped result shortens the list or wedges (HANG) the generator on a
    future that never wakes.

== TARGET INVARIANT: ORDER + IDENTITY CONSERVATION (closed world) ==
``encode`` is a pure bijection over a finite sentinel UNIVERSE of inputs:
encode(x) = (x ^ MASK) + OFFSET, with a matching ``decode``.  The inputs of a
round are a contiguous slice of the universe (ROUND-ROBINED by worker id so the
WHOLE universe is covered across workers, never flaky-random -- the
p125/p126/p172 coverage lesson).  Then:

  * list(ex.map(encode, inputs)) == [encode(x) for x in inputs] EXACTLY.
    Element i MUST equal encode(inputs[i]): a REORDER puts encode(inputs[j]) at
    i (caught: decode(out[i]) != inputs[i]); a TORN result is out-of-universe
    (caught: decode(out[i]) not in the input set); a DROP shortens the list
    (caught: len(out) != len(inputs)).
  * length conserved == len(inputs).
  * every out[i] decodes back into the input multiset, in order.

== SINGLE-OWNER CONTROL ARM ==
The SAME inputs are also run through a PRIVATE executor with max_workers=1
(SERIAL completion -- the futures finish strictly in submit order, so the
order-preserving generator is never actually exercised against out-of-order
completion).  Its output MUST equal the concurrent output element-for-element.
Any divergence appears ONLY at high max_workers (concurrent cross-hub
completion), which LOCALIZES the fault in map()'s order-preserving
generator/result race rather than in ``encode`` itself (encode is a pure
function; the control proves the inputs+function are fine).

== LEGAL-EXCEPTION CASE ==
map() yields lazily and re-raises the FIRST failing task's exception at the
position it is reached.  Case 2 puts exactly ONE poison input in the slice;
encode() raises ValueError for it.  list(map(...)) MUST raise that ValueError
(and the control arm raises it too) -- and the values yielded BEFORE the poison
position must still be correct & in order.  Any OTHER exception type, an
out-of-universe value, a wrong position, or a missing raise is the bug.

Synchronization into the park window: encode() does a runloom.yield_now()
(case-gated), so while the generator parks on future[i].result() the later
futures i+1..N are actively completing on other hubs -- the cross-hub
out-of-order set_result lands DURING the generator's park, which is the window
that a torn pop / mis-routed result would corrupt.

Invariant (hot, fail-fast per round): concurrent map output == reference ==
control output, same length, every value in-universe and decode(out[i]) ==
inputs[i]; the poison case raises exactly ValueError at the right index.
Invariant (post): every case exercised, the whole universe covered, no lost
worker.

Stresses: Executor.map order-preserving result_iterator, list.pop(ob_item)
across a result()-park, Future.set_result store/state/notify out of submit order
cross-hub, torn/mis-routed/dropped mapped result, lazy-raise position fidelity.

Good TSan / controlled-M:N-replay target: the per-yield list.pop() ob_item
read racing the cross-hub set_result stores is a textbook publish/consume race;
a TSan report on the list ob_item or the Future._result store localizes a
reordered/torn result before the order-conservation assert even fires.
"""
import harness
import runloom

# Finite sentinel UNIVERSE of INPUTS.  encode() is a bijection over it, so every
# legitimate map output decodes back to exactly one universe input; an output
# whose decode is NOT a universe input is a torn / out-of-universe result.  Sized
# so the per-round input slice pushes the executor's future list + the work queue
# through real backlog (slice >> max_workers) and the whole universe is covered by
# the round-robin across workers.
UNIVERSE_SIZE = 256
UNIVERSE_BASE = 0x43200000
UNIVERSE = tuple(UNIVERSE_BASE + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Bijection constants.  encode/decode are exact inverses over the integers, so a
# single wrong/torn value is detectable: decode(encode(x)) == x, and a swapped
# pair shows up as decode(out[i]) != inputs[i].
ENC_MASK = 0x5A5A5A5A
ENC_OFFSET = 0x100000007

# Inputs per round.  >> MAX_WORKERS_CONC so the submit queue genuinely backs up:
# the order-preserving generator pops & parks on an early future while many
# LATER-submitted futures are still completing out of order on other hubs -- the
# exact cross-hub window.  Small enough that whole rounds finish under the timeout.
SLICE = 48

# Concurrent arm: many cross-hub completers, so futures finish OUT of submit order
# while the generator surrenders results IN order.  < SLICE so the queue backs up.
MAX_WORKERS_CONC = 8

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# A sentinel INPUT value that encode() refuses to encode (raises ValueError).  It
# is OUTSIDE the universe so the bijection over the universe is untouched; it only
# appears in the poison case.  map() must lazily re-raise that ValueError.
POISON_INPUT = 0x7FFFFFF1

NCASES = 3
CASE_PLAIN = 0       # straight ordered slice, concurrent vs control vs reference
CASE_YIELD = 1       # encode() yields mid-task -> maximises out-of-order overlap
CASE_POISON = 2      # one poison input -> map() must lazily raise ValueError at i


def encode(x, do_yield=False):
    """Pure bijection over the input universe (do_yield is cooperative timing
    only, it does not change the value).  Raises ValueError for POISON_INPUT --
    the one legal exception map() must surface lazily and in position.

    The yield, when set, forces a scheduler hand-off WHILE this task fiber is
    mid-completion, so the generator parking on an EARLIER future and the LATER
    futures' set_result land interleaved across hubs -- the race window."""
    if x == POISON_INPUT:
        raise ValueError("poison input -- the one legal map() task failure")
    if do_yield:
        runloom.yield_now()
    return (x ^ ENC_MASK) + ENC_OFFSET


def decode(y):
    """Exact inverse of encode over the universe: decode(encode(x)) == x.  An
    out[i] whose decode is not the expected input is a torn / mis-routed /
    reordered result."""
    return (y - ENC_OFFSET) ^ ENC_MASK


def slice_for(wid, round_idx):
    """The contiguous universe slice for this (worker, round).  ROUND-ROBINED by
    worker id so successive workers/rounds sweep the WHOLE universe (never flaky-
    random; coverage holds whether one worker does many rounds or many workers do
    one each).  Wraps around the universe so every slice is full-length."""
    start = ((wid + round_idx) * SLICE) % UNIVERSE_SIZE
    idxs = [(start + j) % UNIVERSE_SIZE for j in range(SLICE)]
    return [UNIVERSE[j] for j in idxs]


def run_map_concurrent(inputs, do_yield):
    """Run inputs through a PRIVATE concurrent executor (MAX_WORKERS_CONC cross-hub
    completers) via the order-preserving map() generator and materialise the
    ordered result list.  Each task is a fiber on a different hub, so the futures
    complete OUT of submit order while map() surrenders them IN order."""
    import concurrent.futures as cf
    ex = cf.ThreadPoolExecutor(max_workers=MAX_WORKERS_CONC)
    try:
        # list(map(...)) forces the order-preserving generator to drive every
        # fs.pop()+result()-park to completion in submit order.
        return list(ex.map(encode, inputs, [do_yield] * len(inputs)))
    finally:
        ex.shutdown(wait=True)


def run_map_serial(inputs, do_yield):
    """CONTROL arm: the SAME inputs through a PRIVATE max_workers=1 executor.
    With one worker the futures finish strictly in submit order, so the order-
    preserving generator is never tested against out-of-order completion -- a
    race-free baseline by construction.  If this diverges from the concurrent
    arm, the fault is in map()'s order machinery under concurrency, not encode."""
    import concurrent.futures as cf
    ex = cf.ThreadPoolExecutor(max_workers=1)
    try:
        return list(ex.map(encode, inputs, [do_yield] * len(inputs)))
    finally:
        ex.shutdown(wait=True)


def check_order_identity(H, wid, inputs, out, case):
    """The closed-world ORDER + IDENTITY law: out must equal encode(inputs[i])
    element-for-element, same length, every value in-universe-image, and
    decode(out[i]) == inputs[i].  Returns False on the first violation."""
    if len(out) != len(inputs):
        H.fail("map() returned {0} results for {1} inputs -- a mapped result was "
               "DROPPED or DUPLICATED by the order-preserving generator (torn "
               "fs.pop() under cross-hub out-of-order completion, case {2}, "
               "wid {3})".format(len(out), len(inputs), case, wid))
        return False
    for i in range(len(inputs)):
        want = encode(inputs[i])               # pure recompute, no yield
        got = out[i]
        if got != want:
            dec = decode(got)
            if dec not in UNIVERSE_SET:
                H.fail("map() result[{0}] == {1!r} decodes to {2!r} which is "
                       "OUT-OF-UNIVERSE -- a torn / mis-routed Future._result "
                       "store delivered a stale value to index {0} (case {3}, "
                       "wid {4})".format(i, got, dec, case, wid))
            else:
                H.fail("map() REORDERED results: result[{0}] == encode({1!r}) but "
                       "input[{0}] == {2!r} -- the order-preserving generator "
                       "surrendered encode(inputs[{3}]) at position {0} (a torn "
                       "fs.pop() / wrong-future result under cross-hub completion, "
                       "case {4}, wid {5})".format(i, dec, inputs[i],
                                                   inputs.index(dec) if dec in inputs
                                                   else -1, case, wid))
            return False
    return True


def run_plain_round(H, wid, rng, inputs, do_yield, case):
    """Concurrent map == reference == serial-control, with full order+identity
    conservation.  do_yield gates the in-task cooperative hand-off (case YIELD)."""
    reference = [encode(x) for x in inputs]     # ground truth, computed serially

    conc = run_map_concurrent(inputs, do_yield)
    if not check_order_identity(H, wid, inputs, conc, case):
        return False
    # Concurrent arm must match the pure reference exactly (this is the primary
    # invariant; check_order_identity already proved element==encode(input)).
    if conc != reference:
        H.fail("concurrent map() output != reference [encode(x) for x in inputs] "
               "-- order-preserving generator diverged under cross-hub completion "
               "(case {0}, wid {1})".format(case, wid))
        return False

    # SINGLE-OWNER CONTROL: serial (max_workers=1) output must match the
    # concurrent output element-for-element.  A divergence here -- present only at
    # high max_workers -- localizes the race to map()'s generator, not encode.
    ctrl = run_map_serial(inputs, do_yield)
    if ctrl != conc:
        H.fail("control divergence: max_workers=1 map() output != concurrent "
               "max_workers={0} output -- identical inputs+function, so the "
               "difference is the ORDER-PRESERVING generator/result race under "
               "concurrent cross-hub completion, NOT encode (case {1}, wid {2})"
               .format(MAX_WORKERS_CONC, case, wid))
        return False
    return True


def run_poison_round(H, wid, rng, inputs, case):
    """Lazy-raise fidelity: exactly one POISON_INPUT sits at a known position;
    encode() raises ValueError for it, so list(map(...)) MUST raise ValueError --
    and the values yielded BEFORE that position must still be correct & in order.
    Both the concurrent and the control (max_workers=1) arms must raise ValueError
    (never a different type, never silently)."""
    import concurrent.futures as cf
    # Place the poison at a deterministic interior position so results before it
    # are surrendered first (the generator yields them in order, THEN hits poison).
    pos = (wid % (SLICE - 2)) + 1
    poisoned = list(inputs)
    poisoned[pos] = POISON_INPUT

    def drive(max_workers):
        ex = cf.ThreadPoolExecutor(max_workers=max_workers)
        try:
            it = ex.map(encode, poisoned, [False] * len(poisoned))
            collected = []
            raised = False
            try:
                for v in it:
                    collected.append(v)
            except ValueError:
                raised = True
            except Exception as exc:            # noqa: BLE001
                H.fail("poison map() raised {0}: {1} -- expected the LEGAL "
                       "ValueError from encode(POISON); any other type is a "
                       "torn/mis-routed completion on the map() generator "
                       "(max_workers={2}, case {3}, wid {4})".format(
                           type(exc).__name__, exc, max_workers, case, wid))
                return None
            return (raised, collected)
        finally:
            ex.shutdown(wait=True)

    conc = drive(MAX_WORKERS_CONC)
    if conc is None:
        return False
    ctrl = drive(1)
    if ctrl is None:
        return False

    conc_raised, conc_vals = conc
    ctrl_raised, ctrl_vals = ctrl

    if not conc_raised:
        H.fail("poison map() (concurrent) did NOT raise ValueError -- the failing "
               "task's exception was LOST by the order-preserving generator "
               "(swallowed set_exception / wrong future popped) (wid {0})"
               .format(wid))
        return False
    if not ctrl_raised:
        H.fail("poison map() (control max_workers=1) did NOT raise ValueError -- "
               "a lost task exception even in the race-free serial baseline "
               "(wid {0})".format(wid))
        return False
    # Values surrendered BEFORE the poison position must be exactly the ordered
    # encode() of the inputs before pos (lazy generator yields them in order first).
    expect_before = [encode(inputs[i]) for i in range(pos)]
    if conc_vals != expect_before:
        H.fail("poison map() yielded {0} pre-poison values but the first {1} "
               "ordered encode()s were expected -- results before the lazy raise "
               "were reordered/torn (concurrent, wid {2})".format(
                   len(conc_vals), pos, wid))
        return False
    # The control arm yields the SAME pre-poison prefix.
    if ctrl_vals != conc_vals:
        H.fail("poison pre-raise prefix diverged: control(max_workers=1)={0} vals "
               "!= concurrent={1} vals -- the order generator surrendered a "
               "different pre-poison prefix under concurrency (wid {2})".format(
                   len(ctrl_vals), len(conc_vals), wid))
        return False
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    covered = state["covered"]            # per-slot count of universe-slices run
    case_hits = state["case_hits"]        # per-case per-slot tally
    i = 0
    round_idx = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases over the first ops keyed off worker id, so
        # coverage holds whether one worker does NCASES rounds or NCASES workers
        # do one each (the suite's flaky-random-coverage fix; see p125).  Random
        # after that to keep the concurrent mix.
        if i < NCASES:
            case = (wid + i) % NCASES
        else:
            case = rng.randrange(NCASES)
        i += 1

        inputs = slice_for(wid, round_idx)
        if case == CASE_PLAIN:
            ok = run_plain_round(H, wid, rng, inputs, False, case)
        elif case == CASE_YIELD:
            ok = run_plain_round(H, wid, rng, inputs, True, case)
        else:
            ok = run_poison_round(H, wid, rng, inputs, case)
        if not ok:
            return

        covered[slot] += len(inputs)
        case_hits[case][slot] += 1
        round_idx += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran) so concurrent.futures'
    # ThreadPoolExecutor resolves to the fiber-backed CoThreadPoolExecutor and the
    # tallies are plain per-slot lists.
    H.state = {
        "covered": [0] * SLOTS,                       # input-units mapped (coverage)
        "case_hits": [[0] * SLOTS for _ in range(NCASES)],
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    covered = sum(H.state["covered"])
    plain = sum(H.state["case_hits"][CASE_PLAIN])
    yld = sum(H.state["case_hits"][CASE_YIELD])
    poison = sum(H.state["case_hits"][CASE_POISON])
    H.log("rounds: plain={0} yield={1} poison={2}; input-units mapped={3} "
          "ops={4} (reaching post with no failure proves every per-round "
          "order+identity+control law held fail-fast)".format(
              plain, yld, poison, covered, H.total_ops()))
    H.check(H.total_ops() > 0,
            "no map() rounds completed -- the order-preserving generator race "
            "window was never exercised")
    H.check(covered > 0, "no input slices were mapped")
    # Each of the three cases was round-robined by worker id over the first ops,
    # so all three are exercised whether one worker does NCASES rounds or NCASES
    # workers do one round each.  Assert explicitly (the p125 coverage lesson).
    H.check(plain > 0, "PLAIN ordered-map case never exercised")
    H.check(yld > 0, "YIELD (in-task hand-off) case never exercised")
    H.check(poison > 0, "POISON lazy-raise case never exercised")
    H.require_no_lost("executor-map order-conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p432_executor_map_result_order_gene", body, setup=setup, post=post,
        default_funcs=3000,
        describe="Executor.map() order-preserving result generator (fs.pop() in "
                 "submit order + future.result()-park) vs N futures completing "
                 "OUT of order via cross-hub set_result; ORDER+IDENTITY law: "
                 "list(map(encode, xs)) == [encode(x) for x in xs] exactly == a "
                 "max_workers=1 control, decode(out[i])==xs[i], lazy-raise in "
                 "position -- a reorder/torn/dropped result fails")
