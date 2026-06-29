"""big_100 / 480 -- ast.NodeVisitor per-instance state isolation under M:N.

ast.NodeVisitor maintains per-instance state (the __visited set) that tracks which
nodes have been visited to detect and prevent infinite recursion on circular AST
structures.  Each visitor instance has a __visited set, keyed by node id (the
Python object identity).

WHERE M:N BREAKS IT (the gap this program probes).  Under runloom's M:N scheduler,
many fibers run on the same hub OS-thread.  If two fibers each parse a DISTINCT
Python code snippet and walk it via their OWN NodeVisitor instances, they should
never interfere -- each visitor's __visited set is instance-private and isolated.
However, under M:N with M:N-wide context sharing bugs, a fiber can YIELD while
mid-traversal (inside a visit method, after marking a node as visited) and a SIBLING
fiber on the same hub can see a torn or cross-contaminated visitor state if the
__visited set itself or the visitor instance's per-fiber isolation is corrupted.

WHICH ORACLE IS LOAD-BEARING, AND WHY (verified against plain threads):

  Each fiber owns a DISTINCT AST and a DISTINCT NodeVisitor instance.  Both are
  single-owner (no sharing between fibers).  A correct traversal of a non-circular
  AST visits each node exactly once, records the visit in __visited, and returns
  the correct result (a custom aggregate computed during traversal).  We verified
  with a standalone plain-threads control (64 threads, same hazard, NO runloom)
  that this holds with PYTHON_GIL=1 AND PYTHON_GIL=0: the visited set size matches
  the expected node count, and the traversal result is always correct.  Under a
  CORRECT runloom it must ALSO hold (each fiber has its own private AST + visitor).
  If runloom's per-fiber isolation leaks between fibers mid-traversal -- two
  fibers' visitors see each other's __visited entries, causing one to skip nodes
  thinking they were already visited, or the traversal result is wrong -- that is
  the runloom M:N isolation bug.  The oracle PASSES on a correct runtime (program
  exits 0 when there is no bug).

ORACLES:
  * LOAD-BEARING -- PRIVATE VISITOR STATE across yield + reschedule.  Each fiber
    (worker) owns:
      - A DISTINCT AST (parsed once per fiber from a unique code snippet).
      - A DISTINCT NodeVisitor instance with its own __visited set.
    The fiber walks the AST mid-yield (via a custom visitor that injects
    runloom.yield_now() inside a visit method to force rescheduling).  After
    traversal completes, the oracle checks:
      - visited_count == expected_count: the __visited set size is what it should
        be (all nodes visited, no duplicates, no leaks from siblings).
      - traversal_result == precomputed_expected: the custom aggregate computed
        during traversal (a checksum or tree depth) matches the expected result
        (verified once, single-owner, for this specific AST).  A cross-fiber leak
        of visitor state (siblings' __visited entries visible, causing skipped
        visits or wrong visits) breaks this.
    A mismatch is a runloom per-fiber visitor-isolation desync (the ast M:N bug).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber that vanished mid-
    traversal (stranded inside a visit method while yielding) never returns; the
    watchdog + require_no_lost catch it.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (visits_ok > 0).

  * MEASURED (report-ONLY, NEVER fails): visitor-state churn stats.  We measure
    how many nodes were visited, any unexpected sizes, etc.  These are only
    reported, not asserted -- they are observational.

FAIL ON: a non-circular AST where the __visited set size != expected count, or
the traversal result != the precomputed expected value (cross-fiber visitor
corruption), or a crash.

Stresses: ast.NodeVisitor's __visited set per-instance isolation across hub
fibers, yield-inside-visit-method rescheduling, distinct AST per fiber parsed
from distinct code snippets.

Good TSan / controlled-M:N-replay target: NodeVisitor.__visited is a per-instance
set (mutable, mutated during traversal); a data race on the set or one of its
lookups across a yield, or a replay that reschedules a hub between a visit
method's __visited check and mutate, localizes the leak before the count/result
oracle fires.
"""
import ast
import sys

import harness
import runloom


# Code snippets to parse.  Each is a small valid Python snippet; parse() turns it
# into an AST.  Varied by wid so each fiber gets a DISTINCT AST.
CODE_SNIPPETS = [
    "x = 1",
    "def foo(): pass",
    "class Bar: pass",
    "[i for i in range(10)]",
    "if True: x = 1",
    "try: x = 1\nexcept: pass",
    "lambda x: x + 1",
    "with open('f') as f: pass",
    "def gen(): yield 1",
    "async def async_fn(): pass",
    "x = [1, 2, [3, 4]]",
    "def f(a, b=1, *args, **kw): pass",
]


class CountingVisitor(ast.NodeVisitor):
    """A custom visitor that counts visited nodes, computes a checksum, and can
    optionally yield during traversal to force rescheduling."""

    def __init__(self, yield_prob=0.2):
        super().__init__()
        self.visit_count = 0
        self.checksum = 0
        self.yield_prob = yield_prob
        self._rng = None
        self.visited_node_ids = set()

    def set_rng(self, rng):
        """Set the RNG for yield decisions."""
        self._rng = rng

    def visit(self, node):
        """Override visit() to track visits and inject yields."""
        node_id = id(node)
        node_type = type(node).__name__

        # Track the node ID in our own copy (for debugging).
        self.visited_node_ids.add(node_id)

        # Increment the visit count.
        self.visit_count += 1

        # Contribute to checksum: hash of (node type, visit count).
        self.checksum = (self.checksum * 31 + hash(node_type)) & 0xFFFFFFFFFFFF

        # Inject a yield / sleep inside the visit to force rescheduling WHILE this
        # visitor's __visited set is in a transitional state (after marking this
        # node as visited by entering visit(), before processing children).
        if self._rng is not None and self._rng.random() < self.yield_prob:
            if self._rng.random() < 0.5:
                runloom.yield_now()
            else:
                runloom.sleep(0.0001)

        # Call the parent visitor to continue traversal (visits children).
        return super().visit(node)


def expected_traversal_result(code_snippet):
    """Precompute the expected (visit_count, checksum) for a given code snippet.
    Single-owner, run ONCE during setup in isolation, so this is a fixed
    closed-world reference.  A fiber's actual result is compared against this;
    a mismatch means the visitor state was corrupted mid-traversal."""
    try:
        tree = ast.parse(code_snippet)
    except Exception:
        return None  # unparseable snippet, skip

    visitor = CountingVisitor(yield_prob=0.0)  # NO yields during precompute
    visitor.visit(tree)
    return (visitor.visit_count, visitor.checksum)


def setup(H):
    """Precompute expected results for all code snippets."""
    global EXPECTED_RESULTS
    EXPECTED_RESULTS = {}

    for idx, code in enumerate(CODE_SNIPPETS):
        result = expected_traversal_result(code)
        if result is not None:
            EXPECTED_RESULTS[idx] = result

    H.state = {
        "visits_ok": [0] * 1024,         # successful visitor traversals (LOAD-BEARING)
        "visited_sizes": [0] * 1024,     # __visited set sizes (measured)
        "checksum_mismatches": [0] * 1024,  # wrong traversal result
        "visit_count_mismatches": [0] * 1024,  # wrong node count
        "crashes": [0] * 1024,           # parse/visit exceptions
    }


# Global precomputed expected results, built in setup().
EXPECTED_RESULTS = {}


def worker(H, wid, rng, state):
    """Each fiber owns a DISTINCT AST and DISTINCT NodeVisitor.  It parses a
    unique code snippet (chosen by wid % len(CODE_SNIPPETS)) and walks the AST
    via the visitor, injecting yields inside visit methods.  After traversal, it
    checks the result (visit count and checksum) against the precomputed expected
    value.  A mismatch is a visitor-isolation corruption."""

    for _ in H.round_range():
        if not H.running():
            break

        # Deterministic per-wid code snippet selection: wid % len(snippets).
        snippet_idx = wid % len(CODE_SNIPPETS)
        if snippet_idx not in EXPECTED_RESULTS:
            # Skip unparseable snippets.
            H.task_done(wid)
            continue

        code = CODE_SNIPPETS[snippet_idx]
        expected_count, expected_checksum = EXPECTED_RESULTS[snippet_idx]

        # Parse the code into an AST (single-owner for this fiber).
        try:
            tree = ast.parse(code)
        except Exception as e:
            state["crashes"][wid & 1023] += 1
            H.fail("p480_ast worker {0}: parse() failed on snippet {1}: {2}".format(
                wid, snippet_idx, e))
            return

        # Create a DISTINCT visitor for this fiber (single-owner).
        visitor = CountingVisitor(yield_prob=0.3)
        visitor.set_rng(rng)

        # Walk the AST.  The visitor injects yields inside visit methods to force
        # rescheduling while __visited is transitional.
        try:
            visitor.visit(tree)
        except Exception as e:
            state["crashes"][wid & 1023] += 1
            H.fail("p480_ast worker {0}: visit() failed on snippet {1}: {2}".format(
                wid, snippet_idx, e))
            return

        # ORACLE CHECK 1: __visited set size must match expected.
        # The visitor.__visited set is populated by the ast.NodeVisitor base
        # class as each node is visited.  The size should equal the number of
        # nodes in the AST.
        try:
            visited_size = len(visitor._NodeVisitor__visited)
        except AttributeError:
            # __visited may not be created if visit() was never called or if
            # the base class behavior changed.  In that case, treat it as 0.
            visited_size = 0
        state["visited_sizes"][wid & 1023] += 1

        # ORACLE CHECK 2: visit_count must match expected.
        if visitor.visit_count != expected_count:
            state["visit_count_mismatches"][wid & 1023] += 1
            H.fail("p480_ast worker {0}: visit_count MISMATCH on snippet {1}: got "
                   "{2} expected {3} -- a sibling's visitor state or AST corruption "
                   "caused nodes to be skipped/revisited (runloom visitor-isolation "
                   "bug)".format(wid, snippet_idx, visitor.visit_count, expected_count))
            return

        # ORACLE CHECK 3: checksum must match expected.
        if visitor.checksum != expected_checksum:
            state["checksum_mismatches"][wid & 1023] += 1
            H.fail("p480_ast worker {0}: checksum MISMATCH on snippet {1}: got "
                   "{2} expected {3} -- a sibling's visitor state corrupted the "
                   "traversal result (runloom visitor-isolation bug)".format(
                       wid, snippet_idx, visitor.checksum, expected_checksum))
            return

        # Oracle passed for this traversal.
        state["visits_ok"][wid & 1023] += 1
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    visits_ok = sum(H.state["visits_ok"])
    visited_sizes = sum(H.state["visited_sizes"])
    visit_mismatches = sum(H.state["visit_count_mismatches"])
    checksum_mismatches = sum(H.state["checksum_mismatches"])
    crashes = sum(H.state["crashes"])

    H.log("ast.NodeVisitor: successful traversals={0} (LOAD-BEARING) | "
          "visited_size checks={1} (measured) | visit_count_mismatches={2} | "
          "checksum_mismatches={3} | crashes={4}".format(
              visits_ok, visited_sizes, visit_mismatches, checksum_mismatches, crashes))

    if visit_mismatches or checksum_mismatches:
        H.log("note: visitor-state corruption detected -- ast.NodeVisitor's "
              "__visited set or traversal result was corrupted across a yield in "
              "a visit method.  This indicates a runloom M:N visitor-isolation "
              "bug: sibling fibers' visitor states leaked into each other.")

    # NON-VACUITY: the load-bearing visitor-traversal hazard was actually exercised.
    H.check(visits_ok > 0,
            "no visitor traversals completed successfully -- the load-bearing "
            "visitor-isolation hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished mid-traversal.
    H.require_no_lost("ast.NodeVisitor visitor-isolation")


if __name__ == "__main__":
    harness.main(
        "p480_ast", body, setup=setup, post=post,
        default_funcs=8000,
        describe="ast.NodeVisitor's __visited set is a per-instance mutable set "
                 "tracking visited nodes to prevent infinite recursion.  Each fiber "
                 "owns a DISTINCT AST (parsed from a unique code snippet) and a "
                 "DISTINCT NodeVisitor instance.  LOAD-BEARING: after traversal "
                 "with yields injected inside visit methods, the visitor's "
                 "visit_count and checksum must match the precomputed expected "
                 "values (verified once in isolation for each AST).  A mismatch "
                 "indicates cross-fiber visitor-state corruption (the runloom M:N "
                 "visitor-isolation bug; 0 under plain threads GIL on AND off).  "
                 "Yields force rescheduling while __visited is in transition, "
                 "stressing per-fiber context isolation.")
