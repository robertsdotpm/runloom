"""big_100 / 304 -- itertools C-iterator cursor + tee shared across hubs.

itertools iterators are C objects carrying internal pointers/counters that the
GIL historically serialized for free: islice's next/stop indices, accumulate's
running total, and -- the dangerous one -- ``tee``'s SHARED linked deque plus a
per-branch cursor.  ``__next__`` on these does not hold the GIL under free-
threaded CPython, and when a wrapped Python iterator (or a Python key/predicate)
is driven it can yield the interpreter mid-advance.  Under M:N that means one
tee object whose branches are drained by goroutines on DIFFERENT hubs, advancing
the SAME shared deque across hub migrations, can torn-cursor: an element gets
dropped (a branch skips it), duplicated (a branch sees it twice), reordered, or
a torn producer-cursor segfaults.

The oracle is the strongest there is for a pure data structure: EXACT SEQUENCE
EQUALITY against a closed-form source.  The source is a finite deterministic
range, so each branch's expected output is known in advance with zero baseline
recording.

CORRECTNESS NOTE -- why the shared tee is accessed UNDER A COOPERATIVE LOCK:
  CPython's ``itertools.tee`` is documented as NOT thread-safe; advancing two
  sibling branches CONCURRENTLY (two ``next()`` calls genuinely in flight at the
  same instant) corrupts the shared deque even with plain OS threads and the GIL
  off -- it is a CPython substrate limitation, not a runloom invariant, and it
  reproduces on stock free-threaded ``threading.Thread`` with no runloom at all.
  We therefore guard every ``next()`` on the shared tee with a per-tee
  ``runloom.sync.Lock`` so exactly one branch advances at a time (the supported
  way to share a tee).  That EXCLUDES the CPython non-thread-safety and ISOLATES
  the runloom-specific hazard: the shared C deque is advanced by DIFFERENT
  goroutines that MIGRATE across hubs between locked pulls (and ``yield_now()``
  inside the lock-free gap forces the migration).  A torn-cursor-across-migration
  or a lost/duplicated deque node attributable to the scheduler still fails the
  exact-sequence + conservation oracle; concurrent-deque corruption that CPython
  itself does not support is correctly NOT exercised.

PRIMARY mode -- tee fan-out across hubs (lock-serialized advance):
  src = range(N); branches = itertools.tee(iter(src), B).  We spawn B goroutines
  (one per branch) that land on (likely) different hubs.  Each branch goroutine
  drains its branch one lock-guarded ``next()`` at a time, with a ``yield_now()``
  between pulls (outside the lock) so the scheduler migrates it mid-drain and
  hands the shared deque to a goroutine on another hub.  Each branch MUST
  reproduce ``list(range(N))`` exactly:
    * a dropped element  -> that branch's list is short / has a gap
    * a duplicated one   -> that branch's list is long / repeats
    * a reorder          -> branch != range(N) elementwise
    * a torn shared cursor across migration -> any of the above, or an exception
      we surface as a failure.
  Conservation across branches: the multiset union of all B branches equals the
  source taken once per branch (each value appears EXACTLY B times across the
  union).  A shared-deque drop/dup breaks this even if a single branch happens to
  look self-consistent.

BASELINE mode -- single-consumer C pipeline (metamorphic sanity):
  One goroutine drains chain->islice->accumulate over a deterministic source,
  one ``next()`` + ``yield_now()`` at a time, and checks the produced sequence
  equals the pure-Python closed form.  A single C-iterator advanced by one
  goroutine that merely migrates hubs IS safe in CPython FT (per-object locking
  covers the lone __next__ caller), so this is a sanity baseline, not the bug
  finder -- but if it ever diverges, the C cursor torn across migration is real.
  A custom groupby key fn that calls yield_now() forces __next__ to re-enter the
  scheduler mid-advance so the migration genuinely spans an advance.

Invariant (post, fail-fast):
  * every tee branch == list(range(N)) exactly (no drop/dup/reorder);
  * across each tee fan-out, every source value appears exactly B times in the
    union of branches (shared-deque conservation);
  * every baseline pipeline == its closed-form expected sequence;
  * no torn-cursor exception, no lost branch goroutine (require_no_lost).

Stresses: itertools tee shared deque drained by goroutines across hubs (advance
serialized by a cooperative lock, migrations between pulls), C-iterator internal
cursor/counter across hub migration, __next__ re-entering the scheduler mid-
advance, exact sequence-equality + multiset conservation.

Good TSan / controlled-M:N-replay target: the tee deque's producer-cursor /
per-branch-cursor reads/writes are plain C stores handed between hub OS threads
across migration; a data-race report on the tee node link/refcount handed across
the migration boundary is often the first signal, before the sequence-equality
oracle even fires.
"""
import itertools
import random

import harness
import runloom

# Source length per tee fan-out.  Long enough that the B branches genuinely
# interleave many advances of the shared deque (and the deque grows to hold the
# lag between the fastest and slowest branch), short enough that B*N elements
# per round stay cheap at tens of thousands of workers.
SRC_LEN = 256
# Branches per tee object.  >2 widens the shared-deque contention (the slowest
# branch forces the deque to retain SRC_LEN nodes) without exploding cost.
BRANCHES = 3

# Baseline pipeline parameters (deterministic, closed-form).
BASE_A = 40          # chain part A length
BASE_B = 40          # chain part B length
BASE_SKIP = 5        # islice start
BASE_TAKE = 60       # islice count


def drain_branch(branch, lock, out_box, slot):
    """Drain ONE tee branch to a list.  Every ``next()`` on the shared tee is
    taken under ``lock`` so exactly one branch advances the shared deque at a
    time (CPython's tee is not safe for genuinely concurrent advance -- see the
    module docstring CORRECTNESS NOTE).  The ``yield_now()`` sits OUTSIDE the
    lock, between pulls, so the scheduler migrates this goroutine mid-drain and
    hands the shared deque to a sibling on another hub -- exercising the runloom-
    specific torn-cursor-across-migration hazard, not CPython's concurrent-deque
    limitation.  Stores the produced list in out_box[slot] for the post-fan-out
    equality + conservation check."""
    got = []
    while True:
        with lock:
            try:
                v = next(branch)
            except StopIteration:
                break
        got.append(v)
        runloom.yield_now()      # migrate between pulls; deque handed cross-hub
    out_box[slot] = got


class YieldKey(object):
    """A Python groupby key callable that re-enters the scheduler on every
    invocation, so a C-iterator's __next__ genuinely yields the interpreter
    mid-advance (the migration spans an advance, not just the gap between two).
    Keys every element to itself -> groupby yields singleton groups, preserving
    the source order for the closed-form check."""

    def __call__(self, x):
        runloom.yield_now()
        return x


def baseline_pipeline(rng):
    """Build a deterministic chain->islice->accumulate pipeline and its pure-
    Python closed-form expected output.  Returns (c_iter, expected_list).

    Source = chain(range(base, base+BASE_A), range(off, off+BASE_B)); take a
    BASE_TAKE window starting at BASE_SKIP; running-sum accumulate.  All closed
    form -- no recorded baseline."""
    base = rng.randrange(0, 1 << 20)
    off = rng.randrange(0, 1 << 20)
    full = list(range(base, base + BASE_A)) + list(range(off, off + BASE_B))
    windowed = full[BASE_SKIP:BASE_SKIP + BASE_TAKE]
    expected = list(itertools.accumulate(windowed))

    c_iter = itertools.accumulate(
        itertools.islice(
            itertools.chain(range(base, base + BASE_A),
                            range(off, off + BASE_B)),
            BASE_SKIP, BASE_SKIP + BASE_TAKE))
    return c_iter, expected


def drain_stepwise(c_iter):
    """Drain a single C-iterator pipeline one element at a time, yielding
    between every pull so the lone consumer migrates hubs mid-advance."""
    out = []
    while True:
        try:
            v = next(c_iter)
        except StopIteration:
            break
        out.append(v)
        runloom.yield_now()
    return out


def worker(H, wid, rng, state):
    """One worker per round runs a tee fan-out AND a baseline pipeline.

    tee: build itertools.tee over a fresh iter(range(N)); spawn BRANCHES
    goroutines (likely landing on different hubs), each draining one branch;
    WaitGroup-join; then check every branch == range(N) and the cross-branch
    multiset conservation.  baseline: drain a chain/islice/accumulate pipeline
    stepwise and check == closed form."""
    expected_src = list(range(SRC_LEN))
    slot = wid & 1023
    rno = 0
    for _ in H.round_range():
        if not H.running():
            break
        rno += 1

        # ---- PRIMARY: tee shared across hubs (lock-serialized advance) ------
        branches = itertools.tee(iter(range(SRC_LEN)), BRANCHES)
        tee_lock = runloom.sync.Lock()   # one advance-at-a-time on the deque
        out_box = [None] * BRANCHES
        wg = runloom.WaitGroup()
        wg.add(BRANCHES)

        def run_branch(branch, bslot):
            try:
                drain_branch(branch, tee_lock, out_box, bslot)
            finally:
                wg.done()

        for b in range(BRANCHES):
            # Bind branch + slot per-iteration (avoid the late-binding closure
            # capturing the loop variable).
            H.fiber(run_branch, branches[b], b)
        wg.wait()

        # Each branch must replay the FULL source exactly (no drop/dup/reorder).
        for b in range(BRANCHES):
            got = out_box[b]
            if got is None:
                H.fail("tee branch {0} produced nothing (lost branch "
                       "goroutine, wid={1} round={2})".format(b, wid, rno))
                return
            if got != expected_src:
                H.fail("tee branch {0} != source range({1}): a torn shared "
                       "deque drop/dup/reorder (len got={2} expected={3}, "
                       "wid={4} round={5})".format(
                           b, SRC_LEN, len(got), SRC_LEN, wid, rno))
                return

        # Cross-branch conservation: in the multiset union of all branches,
        # every source value must appear EXACTLY BRANCHES times (once per
        # branch).  Catches a shared-deque drop/dup even if a single branch
        # happened to look self-consistent.
        union_counts = {}
        for b in range(BRANCHES):
            for v in out_box[b]:
                union_counts[v] = union_counts.get(v, 0) + 1
        if len(union_counts) != SRC_LEN:
            H.fail("tee union has {0} distinct values, expected {1} (shared-"
                   "deque drop/extra, wid={2} round={3})".format(
                       len(union_counts), SRC_LEN, wid, rno))
            return
        for v in expected_src:
            c = union_counts.get(v, 0)
            if c != BRANCHES:
                H.fail("tee conservation broken: value {0} appears {1}x across "
                       "branches, expected {2}x (once per branch -- shared deque "
                       "dropped/duplicated it, wid={3} round={4})".format(
                           v, c, BRANCHES, wid, rno))
                return
        H.op(wid, SRC_LEN * BRANCHES)

        # ---- BASELINE: single-consumer C pipeline (metamorphic sanity) ------
        c_iter, expected = baseline_pipeline(rng)
        got = drain_stepwise(c_iter)
        if got != expected:
            H.fail("baseline chain->islice->accumulate diverged from closed "
                   "form (C cursor torn across migration; len got={0} "
                   "expected={1}, wid={2} round={3})".format(
                       len(got), len(expected), wid, rno))
            return

        # ---- BASELINE 2: groupby with a yielding Python key -----------------
        # The key fn re-enters the scheduler on every element, so groupby's
        # __next__ yields mid-advance.  Keying each element to itself yields
        # singleton groups in source order -> closed form is the source itself.
        gb_src = list(range(rno & 0xFF, (rno & 0xFF) + 32))
        keys = [k for k, _g in itertools.groupby(iter(gb_src), YieldKey())]
        if keys != gb_src:
            H.fail("groupby(yielding-key) keys diverged from source order "
                   "(__next__ torn mid-advance; wid={0} round={1})".format(
                       wid, rno))
            return

        H.op(wid, len(expected) + len(keys))
        H.task_done(wid)


def setup(H):
    H.state = {}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("tee+pipeline ops={0} tasks={1}".format(
        H.total_ops(), H.total_tasks()))
    H.check(H.total_ops() > 0, "no tee/pipeline work happened")
    H.require_no_lost()


if __name__ == "__main__":
    harness.main("p304_c_iter_tee_crosshub", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="itertools.tee shared across hubs; each branch on a "
                          "different goroutine must replay range(N) exactly and "
                          "every value appears once-per-branch in the union -- "
                          "no shared-deque drop/dup/reorder/torn cursor")
