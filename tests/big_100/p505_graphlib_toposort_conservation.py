"""big_100 / 505 -- graphlib.TopologicalSorter emit-exactly-once conservation under M:N.

graphlib.TopologicalSorter drives a small STATE MACHINE over per-node _NodeInfo
records.  Each node carries an `npredecessors` countdown and a `successors` list;
the sorter also holds a `_ready_nodes` list and a `_npassedout` / `_nfinished`
bookkeeping pair.  The protocol is:

    ts.prepare()                       # freezes the graph, seeds ready set
    while ts.is_active():
        ready = ts.get_ready()         # returns nodes with npredecessors==0
        ... work ...
        for n in ready: ts.done(n)     # decrements successors' countdowns,
                                       # re-arming the ready set

Every one of those calls MUTATES the shared per-node countdown/ready bookkeeping.
The load-bearing hazard for an M:N runtime: a fiber that parks (yields) BETWEEN
get_ready() and done() -- exactly where this program yields -- and is resumed on a
different hub could, if the sorter's internal state were not fiber-isolated (a
torn `_ready_nodes` list, a re-entered `get_ready`, a lost `npredecessors`
decrement, or a `_NodeInfo` spliced from a sibling sorter), cause a node to be
emitted TWICE, ZERO times, or BEFORE one of its predecessors has been marked done.
Any of those breaks the two closed-world laws below.

WHERE M:N BREAKS IT (the gap this program probes).  runloom gives each fiber its
own Python frame stack, but a TopologicalSorter is a MUTABLE object carrying live
protocol state across the get_ready()->done() boundary.  Because the yield sits
squarely inside that boundary, a sibling fiber driving its OWN sorter interleaves
here; if the runtime aliased frame locals, migrated the sorter's countdown state,
or dropped/duplicated a done() wake, the resumed fiber would observe a corrupted
ready set.  The sorter and its DAG are strictly FIBER-LOCAL (built in a fiber-local
variable, never shared), so under a CORRECT runtime the closed-world laws hold with
zero tolerance -- a violation is a real runtime desync, not documented Python
shared-object semantics.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

  Each fiber builds its OWN random DAG over a finite, WID-TAGGED node universe
  (node id = wid*NODE_SCALE + local_index, so any node from a sibling's sorter is
  provably OUT-OF-UNIVERSE).  It drives the full get_ready()/done() protocol with a
  yield_now() inserted between get_ready() and done() (the hazard boundary), and
  enforces:

    * CONSERVATION (exact count):  every node in the finite universe is emitted by
      get_ready() EXACTLY ONCE -- a per-node seen-count array (single-owner,
      race-free by construction) must read all-ones at the end, and the number of
      distinct emitted nodes == the universe size.  A dropped emit (count 0), a
      doubled emit (count 2), or an out-of-universe node (a sibling's node leaking
      in) is a HARD FAULT.
    * TOPO ORDER (precedence): a node is only returned by get_ready() AFTER every
      one of its predecessors has been passed to done().  We verify this at the
      moment each node is handed out (all its predecessors are already in the
      done-set).  A node emitted early is a torn-countdown bug.
    * CROSS-CHECK: an independent second sorter over the SAME edges yields a
      static_order() that is a permutation of the universe (each node once) and is
      itself a valid topological order (each node after all its predecessors).
    * CYCLE ARM (single-owner): a deliberately cyclic fiber-local graph must raise
      graphlib.CycleError from prepare() -- the error path is exercised too.

  All state (the DAG edges, both sorters, the seen-count array, the done-set) is
  fiber-local; nothing is shared between fibers.  On a correct runtime every law
  holds deterministically, so the program exits 0 when there is no bug.  A FAIL
  means a node was emitted the wrong number of times, out of order, out of
  universe, or the cycle went undetected -- a graphlib protocol-state desync in the
  runtime.

ORACLES:
  * LOAD-BEARING -- EMIT-EXACTLY-ONCE + TOPO-ORDER (worker, HARD, fail-fast).
    Single-owner DAG + sorter; per-node seen-count conservation and predecessor
    precedence checked across a yield taken between get_ready() and done().
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-protocol
    (parked inside the get_ready/done window on a desynced sorter) never returns;
    the watchdog + require_no_lost catch it.
  * NON-VACUITY (post, HARD): the load-bearing arm actually completed DAGs
    (dags_completed > 0).

FAIL ON: a universe node emitted zero or two+ times, a node emitted before a
predecessor was done, an out-of-universe node handed out by get_ready(), a
static_order() that is not a valid permutation/topo order, or a cyclic graph whose
prepare() does NOT raise CycleError.

Stresses: graphlib.TopologicalSorter get_ready()/done() protocol state machine,
_NodeInfo npredecessors countdown + successors under a yield at the get_ready->done
boundary, _ready_nodes list mutation across hub migration, static_order() C-loop,
CycleError detection -- all on a strictly single-owner fiber-local DAG so any
violation is a runtime desync, not shared-object semantics.
"""
import graphlib

import harness
import runloom

# Each fiber's nodes live in a private numeric band so a sibling's node is
# provably out-of-universe: node id = wid * NODE_SCALE + local_index.
NODE_SCALE = 1 << 20

# DAG size bounds per fiber.  Small enough that thousands of fibers each complete
# many DAGs under the timeout, large enough to push the sorter through several
# get_ready()/done() rounds (multiple ready "waves") and grow its internal lists.
MIN_NODES = 6
MAX_NODES = 28

# Inner DAGs per round -- sustained protocol churn so a sibling reliably interleaves
# at the get_ready()->done() yield before this fiber resumes.
INNER_CAP = 100000


def build_dag(rng, wid, nnodes):
    """Build a random fiber-local DAG over `nnodes` nodes.

    Node local index i in [0, nnodes) maps to the wid-tagged id base+i.  To
    guarantee acyclicity, node i may only depend on strictly-earlier nodes (j < i).
    Returns (base, preds) where preds[i] is the list of local predecessor indices
    of node i.  A skewed fan-in (some isolated roots, some nodes with several
    predecessors) forces multiple ready-waves so get_ready() returns real batches.
    """
    base = wid * NODE_SCALE
    preds = [[] for _ in range(nnodes)]
    for i in range(1, nnodes):
        # How many predecessors this node draws from {0..i-1}.  Bias toward a few
        # so the DAG stays wide (many independent nodes ready at once) rather than
        # a single chain.
        maxp = min(i, 4)
        k = rng.randint(0, maxp)
        if k:
            preds[i] = rng.sample(range(i), k)
    return base, preds


def load_dag(ts, base, preds):
    """Register every node + edge into the sorter.  add(node, *predecessors) means
    `node` depends on `predecessors`, so predecessors must be emitted first.  Nodes
    with no predecessors are still registered via add(node) so the universe is
    complete."""
    for i, plist in enumerate(preds):
        node = base + i
        if plist:
            ts.add(node, *[base + p for p in plist])
        else:
            ts.add(node)


def valid_topo(order, preds, base):
    """True iff `order` lists every node exactly once and each node appears after
    all of its predecessors (a valid topological order over the fiber-local DAG)."""
    nnodes = len(preds)
    if len(order) != nnodes:
        return False
    pos = {}
    for idx, node in enumerate(order):
        if node in pos:
            return False                    # duplicate
        pos[node] = idx
    if len(pos) != nnodes:
        return False
    for i, plist in enumerate(preds):
        node = base + i
        if node not in pos:
            return False                    # out of universe / missing
        for p in plist:
            if pos[base + p] > pos[node]:
                return False                # predecessor emitted AFTER node
    return True


def dag_check(H, wid, idx, state):
    """Single-owner graphlib protocol conservation check (LOAD-BEARING, fail-fast).

    Drives the full prepare()/get_ready()/done() protocol over a fiber-local DAG
    with a yield at the get_ready()->done() hazard boundary, enforcing emit-
    exactly-once, predecessor precedence, out-of-universe rejection, and a
    static_order() cross-check.  All state is fiber-local; a violation is a runtime
    desync."""
    rng = H.derive("dag", wid, idx)
    nnodes = rng.randint(MIN_NODES, MAX_NODES)
    base, preds = build_dag(rng, wid, nnodes)

    lo = base
    hi = base + nnodes                       # universe is [lo, hi)

    ts = graphlib.TopologicalSorter()
    load_dag(ts, base, preds)
    ts.prepare()

    # Per-node emit count (single-owner array, race-free by construction) and the
    # set of nodes already handed to done() (for the precedence check).
    seen = [0] * nnodes
    done_local = [False] * nnodes
    emitted_total = 0

    while ts.is_active():
        ready = ts.get_ready()

        # HAZARD BOUNDARY: park BETWEEN get_ready() and done().  A sibling driving
        # its own sorter interleaves here; a torn ready-set / migrated countdown
        # state would surface as a wrong node on resume.
        runloom.yield_now()
        if idx & 1:
            runloom.sleep(0.0002)

        for node in ready:
            # Out-of-universe: a node id outside THIS fiber's band means a sibling's
            # _NodeInfo leaked into this sorter's ready set.
            if node < lo or node >= hi:
                H.fail("graphlib get_ready() yielded OUT-OF-UNIVERSE node {0} "
                       "(universe [{1},{2}) for wid {3}) -- a sibling sorter's node "
                       "leaked into this fiber's ready set across the get_ready->"
                       "done yield".format(node, lo, hi, wid))
                return
            li = node - base
            # Emit-exactly-once: a second emit of the same node is a doubled ready.
            if seen[li]:
                H.fail("graphlib emitted node {0} TWICE (wid {1}) -- a doubled "
                       "get_ready() from a torn ready-set / re-armed countdown "
                       "across the get_ready->done yield".format(node, wid))
                return
            # Precedence: every predecessor must ALREADY be done() before this node
            # is handed out.
            for p in preds[li]:
                if not done_local[p]:
                    H.fail("graphlib emitted node {0} before predecessor {1} was "
                           "done (wid {2}) -- a lost npredecessors decrement / early "
                           "ready across the get_ready->done yield".format(
                               node, base + p, wid))
                    return
            seen[li] = 1
            emitted_total += 1

        # Now mark them done (re-arms successors' countdowns).
        for node in ready:
            ts.done(node)
            done_local[node - base] = True

    # ---- CONSERVATION: every universe node emitted EXACTLY once ---------------
    if emitted_total != nnodes:
        H.fail("graphlib conservation broken: emitted {0} nodes but universe has "
               "{1} (wid {2}) -- a node was dropped or doubled".format(
                   emitted_total, nnodes, wid))
        return
    for li in range(nnodes):
        if seen[li] != 1:
            H.fail("graphlib conservation broken: node {0} emitted {1} times, "
                   "expected exactly 1 (wid {2})".format(
                       base + li, seen[li], wid))
            return
        if not done_local[li]:
            H.fail("graphlib node {0} was emitted but never marked done (wid {1}) "
                   "-- the sorter went inactive with an un-finished node".format(
                       base + li, wid))
            return

    # ---- CROSS-CHECK: independent static_order() over the SAME edges ----------
    ts2 = graphlib.TopologicalSorter()
    load_dag(ts2, base, preds)
    order = list(ts2.static_order())
    if not valid_topo(order, preds, base):
        H.fail("graphlib static_order() is not a valid permutation/topological "
               "order of the fiber-local DAG (wid {0}): {1}".format(
                   wid, order[:16]))
        return

    state["dags_completed"][wid & 1023] += 1


def cycle_check(H, wid, idx, state):
    """Single-owner CYCLE arm: a deliberately cyclic fiber-local graph MUST raise
    graphlib.CycleError from prepare().  Exercises the error path across a yield;
    all state fiber-local."""
    base = wid * NODE_SCALE
    ts = graphlib.TopologicalSorter()
    # Small cycle a->b->c->a (each depends on the next), fully fiber-local.
    a, b, c = base, base + 1, base + 2
    ts.add(a, c)
    ts.add(b, a)
    ts.add(c, b)
    runloom.yield_now()
    try:
        ts.prepare()
    except graphlib.CycleError:
        state["cycle_checks"][wid & 1023] += 1
        return
    H.fail("graphlib did NOT raise CycleError on a fiber-local cyclic graph "
           "(wid {0}) -- the cycle-detection state was corrupted".format(wid))


def worker(H, wid, rng, state):
    """Each fiber sustains the load-bearing single-owner protocol check (fail-fast),
    with an occasional cycle-arm check.  All DAGs/sorters are fiber-local, so the
    only cross-fiber interaction is scheduler interleaving at the get_ready->done
    yield -- exactly the hazard boundary."""
    for _ in H.round_range():
        if not H.running():
            break
        idx = 0
        while H.running() and idx < INNER_CAP:
            dag_check(H, wid, idx, state)
            if H.failed:
                return
            if idx % 16 == 0:
                cycle_check(H, wid, idx, state)
                if H.failed:
                    return
            H.op(wid)
            idx += 1
        H.task_done(wid)


def setup(H):
    H.state = {
        "dags_completed": [0] * 1024,    # LOAD-BEARING DAGs fully driven + verified
        "cycle_checks": [0] * 1024,      # CycleError-detection checks
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    dags = sum(H.state["dags_completed"])
    cycles = sum(H.state["cycle_checks"])
    H.log("graphlib[single-owner LOAD-BEARING]: {0} DAGs driven through the full "
          "prepare/get_ready/done protocol with emit-exactly-once + topo-order + "
          "static_order cross-check (all passed fail-fast); {1} CycleError-"
          "detection checks; ops={2}".format(dags, cycles, H.total_ops()))

    # NON-VACUITY: the load-bearing protocol arm actually ran to completion.
    H.check(dags > 0,
            "no graphlib DAGs completed -- the get_ready()->done() protocol-state "
            "hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the
    # get_ready/done window on a desynced sorter).
    H.require_no_lost("graphlib toposort conservation")


if __name__ == "__main__":
    harness.main(
        "p505_graphlib_toposort_conservation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="graphlib.TopologicalSorter drives a get_ready()/done() state "
                 "machine over per-node npredecessors countdowns.  Under M:N, a "
                 "fiber parked BETWEEN get_ready() and done() and resumed on "
                 "another hub could, if the sorter's protocol state were not "
                 "fiber-isolated, emit a node twice, zero times, or before a "
                 "predecessor.  LOAD-BEARING: each fiber builds its OWN wid-tagged "
                 "DAG over a finite node universe and drives the full protocol with "
                 "a yield at the get_ready->done boundary; closed-world laws -- "
                 "every universe node emitted EXACTLY once, only after all its "
                 "predecessors, no out-of-universe node, static_order() a valid "
                 "permutation, and a cyclic graph raises CycleError.  A violation "
                 "is a graphlib protocol-state desync in the runtime")
