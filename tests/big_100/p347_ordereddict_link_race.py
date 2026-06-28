"""big_100 / 347 -- collections.OrderedDict C doubly-linked list under M:N splice churn.

SKIPPED FOR NOW -- the SIGSEGV this program catches (concurrent setitem/move_to_end/
popitem corrupting the C order list) is a KNOWN UPSTREAM CPython free-threading bug,
already FIXED in 3.14, NOT a runloom defect:
    https://github.com/python/cpython/issues/125996
    "nogil segmentation fault on ordered dict operations"  (closed; fixed by
    GH-133734 "fix thread safety of ordered dict", in 3.14)
Reproduced with PLAIN threading.Thread (no runloom): 3.13.13t crashes ~25/27
(cross-arch x86 + arm64 -- a use-after-free), while a from-source 3.14.6t build
(which has the fix) is 0/55 clean.  So it crashes on 3.13t only because the fix was
NOT backported to 3.13's experimental free-threading.  setup() therefore AUTO-SKIPS
only on free-threaded Python < 3.14 (where the fix is absent) and runs the full
stress on 3.14+ -- so the test stays live where it should pass, and a crash on 3.14+
would be a genuine NEW regression worth catching.

No existing program stresses `collections.OrderedDict`, and its C internals are a
prime MUTATING shared container for the free-threaded M:N model.  The C
`_collections.OrderedDict` keeps TWO coupled structures behind one per-object
lock: the ordinary dict hash table (key -> value) AND a separate CIRCULAR
DOUBLY-LINKED LIST of nodes that records insertion order.  Every order-touching
operation splices that C list:

  * `od[k] = v`            on a NEW key APPENDS a node at the tail (link 2 ptrs);
  * `move_to_end(k)`       UNLINKS the node and RELINKS it at the head/tail
                           (the classic 4-pointer splice);
  * `popitem(last=...)`    UNLINKS the head/tail node and frees it.

On free-threaded 3.13t with the GIL off and tens of thousands of goroutines
hammering the SAME OrderedDict across >= 4 hubs, those splices run concurrently.
The runloom-specific twist is preempt-mid-splice: sysmon can resume a goroutine
on ANOTHER hub WHILE it is partway through the C linkprev/linknext relink (a
`yield_now()` mid-loop maximizes the chance the NEXT op's splice lands on a
half-relinked node), and concurrent iterators WALK the very list being spliced.

The two failure modes that would matter:
  * a TORN / DROPPED / DUPLICATED link -- the linked list of nodes diverges from
    the dict hash table (a node leaked out of the order list, or got linked
    twice), so the order-list length != the dict length; and
  * a DANGLING pointer in the order list -- an iterator following a half-unlinked
    `node->next` reads freed memory and SEGFAULTs / aborts.

ORACLE (STRUCTURAL container invariant + crash -- NOT a racy counter):
  An OrderedDict's order-list and hash table are two views of ONE multiset of
  keys, so the C linked list must thread EXACTLY the dict's keys, once each.
  Read from the QUIESCENT post() point (the scheduler has fully drained; nothing
  mutates the OD), the invariant is a pure structural identity:

      len(list(iter(od)))  ==  len(list(reversed(od)))  ==  len(od)

  i.e. the FORWARD link walk (follows node->next from the head), the BACKWARD
  link walk (follows node->prev from the tail), and the DICT hash-table count all
  agree.  A dropped/duplicated/dangling link makes the order-list walk diverge
  from the dict length -- a STRUCTURAL over/under-count of nodes -- and we also
  assert the forward and backward walks yield the SAME key set (no node threaded
  into one direction but not the other, no cross-spliced order).

  WHY THIS IS RACE-FREE (the anti-racy-counter rationale): the oracle reads the
  ACTUAL surviving C structure after the run, exactly as p303 reads the real
  lru_cache dict length via gc -- it is NOT a `counter += 1` accumulated across
  fibers (that LOSES increments under FT and false-positives).  No per-op count
  is summed; we only compare three independent reads of the SAME settled object,
  which must be equal for any non-corrupt OrderedDict regardless of how the
  concurrent splices interleaved.  The keyspace, op mix, and interleaving are all
  irrelevant to the invariant -- only a torn C link can break it.

  The crash signal is load-bearing too: worker goroutines also iterate the OD
  WHILE siblings splice it.  A benign `RuntimeError: OrderedDict mutated during
  iteration` is EXPECTED and swallowed (it is CPython correctly detecting the
  concurrent resize, not a bug); but a DANGLING node->next pointer makes that
  same walk dereference freed memory -> SIGSEGV / `Fatal Python error`, the gold
  corruption signal.  The harness watchdog also catches a self-cyclic order list
  (a walk that never terminates -> no forward progress -> EXIT_HANG).

Stresses: collections.OrderedDict C doubly-linked-list mutation, move_to_end /
popitem / setitem splice churn across hubs, preempt-mid-splice, concurrent
iteration over a list being relinked, structural order-list==hash-table identity.

Good TSan / controlled-M:N-replay target: the node relink (linkprev/linknext) vs
a concurrent unlink/walk is a pure shared-memory race on a CPython-internal C
container; a data-race report on the node pointer store is often the first
signal, before the structural oracle even fires.
"""
import collections
import sys

import harness
import runloom

# Keyspace bounded so the OD stays modestly sized (a few thousand live nodes)
# while EVERY op touches the order list: setitem appends, move_to_end re-splices,
# popitem unlinks.  Small enough that the splice/unlink paths run hot and overlap
# constantly across hubs, not so small that the dict is trivially tiny.
KEYSPACE = 4096
OPS_PER_ROUND = 3000     # hot loop so the relink path runs under sustained load
TARGET_FILL = 1500       # popitem-pressure threshold: keep the OD churning, not
                         # monotonically growing, so unlink/relink races overlap


def worker(H, wid, rng, state):
    """Hammer the SHARED OrderedDict with the three order-list-splicing ops --
    setitem (append), move_to_end (re-splice), popitem (unlink) -- plus a walk
    that iterates the list WHILE siblings relink it.  A yield_now mid-loop invites
    sysmon to resume this goroutine on another hub partway between ops, maximizing
    the chance a preempt lands inside the C linkprev/linknext relink the next op
    drives."""
    od = state["od"]
    for _ in H.round_range():
        if not H.running():
            break
        for i in range(OPS_PER_ROUND):
            if not H.running():
                break
            k = rng.randrange(KEYSPACE)
            r = rng.randrange(8)
            if r < 4:
                # APPEND/overwrite: a brand-new key links a node at the tail;
                # an existing key just rebinds the value (no relink) -- the mix
                # keeps the order list both growing and stable-overwriting.
                od[k] = k * k
            elif r < 6:
                # RE-SPLICE: unlink the node and relink it at an end (the
                # 4-pointer move_to_end splice -- the hottest corruption path).
                try:
                    od.move_to_end(k, last=(r == 5))
                except KeyError:
                    od[k] = k * k         # not present yet -> append instead
            elif r == 6:
                # UNLINK from an end: pops the head/tail node off the list.
                try:
                    od.popitem(last=(i & 1) == 0)
                except KeyError:
                    pass                  # benign: OD raced empty (another popper)
            else:
                # WALK the order list while siblings splice it.  This is the
                # SEGFAULT canary: a dangling node->next dereferences freed
                # memory -> crash.  A RuntimeError ("mutated during iteration")
                # OR a KeyError (the iterator resolved a node whose key a sibling
                # popitem just removed from the dict mid-walk) is CPython / FT
                # CORRECTLY surfacing the concurrent mutation -- expected and
                # benign, NOT a corruption signal -- so we swallow exactly those.
                # A torn link does NOT raise here; it SEGFAULTs (the gold signal)
                # or shows up structurally in post().
                try:
                    n = 0
                    for _key in od:
                        n += 1
                        if n >= 256:
                            break         # bounded walk: enough to hit a torn link
                except (RuntimeError, KeyError):
                    pass                  # benign concurrent-mutation during iter

            # Pressure valve: when the OD grows past TARGET_FILL, bias toward
            # popitem so it churns (constant unlink/relink overlap) instead of
            # growing without bound.  len() is a single atomic read, not a summed
            # counter -- it is only used to steer the workload, never as the oracle.
            if len(od) > TARGET_FILL:
                try:
                    od.popitem(last=(i & 1) == 0)
                except KeyError:
                    pass

            H.op(wid)
            if (i & 15) == 0:
                runloom.yield_now()       # invite a cross-hub resume mid-splice
        H.task_done(wid)


SKIP_REASON = (
    "OrderedDict concurrent-splice SIGSEGV is UPSTREAM CPython gh-125996 "
    "(https://github.com/python/cpython/issues/125996 -- 'nogil segmentation fault "
    "on ordered dict operations'), FIXED in 3.14 via GH-133734; reproduced with "
    "PLAIN threads (no runloom) -- 3.13.13t crashes ~25/27, 3.14.6t is 0/55 clean. "
    "NOT a runloom bug; auto-skipped on free-threaded Python < 3.14 to avoid the crash.")


def bug_unfixed():
    """True iff this interpreter still has the gh-125996 OrderedDict crash: it needs
    free-threading (GIL OFF -- a GIL build serialises the C splices and is safe) AND
    Python < 3.14 (the GH-133734 fix landed in 3.14).  On 3.14+ we RUN the stress --
    a crash there would be a genuine new regression worth catching, not a known bug."""
    gil_off = hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()
    return gil_off and sys.version_info < (3, 14)


def setup(H):
    # Version-gated skip: AVOID THE CRASH only where the bug actually exists
    # (free-threaded < 3.14; see docstring + gh-125996).  The skip MUST happen here
    # in setup() -- body() spawns the workers that hammer the OD and segfault, so we
    # set state=None and never run them.  On 3.14+ (fix present) we run normally.
    if bug_unfixed():
        H.note_scale_limit(SKIP_REASON)
        H.state = None
        return
    # ONE shared C OrderedDict, mutated by every fiber across every hub.  This is
    # the genuinely-shared CPython C object whose internal doubly-linked list is
    # under test.  Pre-seed it so move_to_end/popitem have nodes to splice from
    # the very first op (otherwise the first round is all appends).
    od = collections.OrderedDict()
    for k in range(min(TARGET_FILL, KEYSPACE)):
        od[k] = k * k
    H.state = {"od": od}


def body(H):
    if H.state is None:
        return                          # version-gated skip (upstream CPython gh-125996)
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if H.state is None:
        H.log("SKIPPED (free-threaded < 3.14): "
              + (H.scale_limit_reason or "upstream CPython gh-125996"))
        return
    od = H.state["od"]
    # QUIESCENT structural read: the scheduler has fully drained (run() joined
    # every goroutine) before post() runs, so nothing mutates `od` now.  Walk the
    # C order list FORWARD (node->next from head) and BACKWARD (node->prev from
    # tail), and read the dict hash-table count.  All three MUST agree for any
    # non-corrupt OrderedDict; a torn/dropped/duplicated link breaks the identity.
    dict_len = len(od)
    fwd = list(od)                 # forward link walk (iter -> follows next)
    bwd = list(reversed(od))       # backward link walk (reversed -> follows prev)
    fwd_len = len(fwd)
    bwd_len = len(bwd)

    H.log("od dict_len={0} fwd_link_walk={1} bwd_link_walk={2} "
          "(all three must be EQUAL; a mismatch => torn/dropped/duplicated node "
          "in the C order list)".format(dict_len, fwd_len, bwd_len))
    H.log("ops={0} funcs={1}".format(H.total_ops(), H.funcs))

    H.check(H.total_ops() > 0, "no OrderedDict ops happened")

    # STRUCTURAL invariant 1: forward order-list walk length == dict hash-table
    # count.  A node that leaked OUT of the order list (dropped unlink) or got
    # linked TWICE (duplicated splice) makes the forward walk diverge from len().
    H.check(fwd_len == dict_len,
            "OrderedDict forward link walk {0} != dict length {1} -- the C order "
            "list diverged from the hash table (a node dropped from / duplicated "
            "in the order list during a concurrent splice)".format(
                fwd_len, dict_len))

    # STRUCTURAL invariant 2: backward order-list walk length == dict count.  The
    # prev-pointer chain must thread exactly the same nodes as the next chain; a
    # half-relinked node (next set but prev dangling, or vice versa) breaks ONE
    # direction only, which this catches even if invariant 1 happened to hold.
    H.check(bwd_len == dict_len,
            "OrderedDict backward link walk {0} != dict length {1} -- the prev "
            "pointer chain diverged from the hash table (a half-relinked node: "
            "one direction threaded, the other dangling)".format(
                bwd_len, dict_len))

    # STRUCTURAL invariant 3: forward and backward walks visit the SAME set of
    # keys.  Equal lengths could still hide a cross-spliced order (a node threaded
    # into next from one position but into prev from another); the key SETS must
    # match exactly.  Compare as sets (order legitimately reverses) -- a key that
    # appears in one walk but not the other is a torn link, not a benign reorder.
    H.check(set(fwd) == set(bwd),
            "OrderedDict forward/backward link walks visit DIFFERENT keys -- a "
            "node is threaded into one direction of the C list but not the other "
            "(cross-spliced / half-unlinked order list)")

    # The walks must also exactly cover the dict's keys (no node threading a key
    # the hash table dropped, and no dict key absent from the order list).
    H.check(set(fwd) == set(od.keys()),
            "OrderedDict order-list keys != hash-table keys -- a node threads a "
            "key the dict no longer holds (or a dict key with no order node): a "
            "key/value desync between the two coupled C structures")

    H.require_no_lost("OrderedDict link walkers")


if __name__ == "__main__":
    harness.main("p347_ordereddict_link_race", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="collections.OrderedDict C doubly-linked list hammered "
                          "across hubs with setitem/move_to_end/popitem splice "
                          "churn + concurrent iteration (segfault canary); "
                          "STRUCTURAL oracle: forward link walk == backward link "
                          "walk == dict length (torn/dropped/duplicated node "
                          "breaks the identity)")
