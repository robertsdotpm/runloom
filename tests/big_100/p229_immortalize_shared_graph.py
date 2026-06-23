"""big_100 / 229 -- immortalize a hot shared object graph (cross-hub refcount elision).

ONE immutable object graph (a nested tuple/dict of constants) is built in
setup() and frozen with runloom_c.immortalize(graph).  A pool of N goroutines
fans out across --hubs 16 and, for the whole run, hammers that single shared
graph: each op walks its fields and folds them into a deterministic checksum.
Touching the shared object hard would normally drive a cross-hub
_Py_TryIncRefShared / _Py_DecRefShared atomic per reference push -- the
documented #1 free-threading scaling limiter (~20-28% on p207).  Immortalizing
freezes the refcount so those become no-ops, which is the lever
docs/dev/HUB_SCALING.md showed actually removes that tax.

Oracle (correctness, the point of the test -- immortalize must be transparent):
  * Every goroutine's per-round checksum equals the expected value derived from
    the graph: immortalizing must NOT corrupt or change what the object reads as.
  * The graph's refcount is captured before the storm and again after.  An
    immortal object has a FROZEN refcount, so it must be unchanged across the
    hot cross-hub fan-out (only a transient local borrow differs, hence the
    tolerance).  Guarded: sys.getrefcount is skipped if the build lacks it.
  * A control sub-mode WITHOUT immortalize (--no-immortalize) asserts the SAME
    checksums -- proving immortalize is correctness-transparent, only perf differs.
  * peak ops must equal expected (every goroutine's op() landed).

Stresses: Stresses: runloom_c.immortalize freezing a hot shared object graph so cross-hub incref/decref are no-ops under a pure-compute fan-out across many hubs; correctness (immortal object still readable/usable, refcount does not change) and the _Py_DecRefShared elision.
"""
import sys

import harness
import runloom_c


# How hard each round touches the shared graph.  Bounded per-worker work
# (box-safe): a fixed number of field reads, no allocation growth.
TOUCHES_PER_ROUND = 512


def build_graph():
    """ONE nested immutable graph of constants.  Tuples + frozenset + a
    read-only dict-ish view materialized as a tuple of items so the whole thing
    is hashable/immutable and safe to freeze and share read-only across hubs."""
    leaves = tuple(i * 2654435761 & 0xFFFFFFFF for i in range(64))
    nested = tuple((k, tuple(leaves[(k + j) % len(leaves)] for j in range(8)))
                   for k in range(32))
    return (
        ("magic", 0xC0FFEE),
        ("leaves", leaves),
        ("nested", nested),
        ("fs", frozenset(leaves[:16])),
    )


def graph_checksum(graph):
    """Deterministic fold over the whole graph.  Pure compute; reads only.
    Every field access pushes a reference to a shared (immortal-or-not) object,
    which is the cross-hub refcount traffic we are exercising."""
    cs = 0
    magic = graph[0][1]
    cs = (cs * 1000003 + magic) & 0xFFFFFFFFFFFF
    leaves = graph[1][1]
    for v in leaves:
        cs = (cs * 1000003 + v) & 0xFFFFFFFFFFFF
    nested = graph[2][1]
    for k, row in nested:
        cs = (cs * 1000003 + k) & 0xFFFFFFFFFFFF
        for v in row:
            cs = (cs * 1000003 + v) & 0xFFFFFFFFFFFF
    fs = graph[3][1]
    cs = (cs * 1000003 + (len(fs) ^ sum(fs))) & 0xFFFFFFFFFFFF
    return cs


def add_args(ap):
    ap.add_argument("--no-immortalize", dest="immortalize",
                    action="store_false", default=True,
                    help="control mode: DON'T freeze the graph.  The checksums "
                         "must be identical to the immortalized run -- only the "
                         "_Py_DecRefShared tax (perf) differs.")


def setup(H):
    graph = build_graph()
    expected = graph_checksum(graph)

    do_immortal = getattr(H.args, "immortalize", True)
    if do_immortal:
        runloom_c.immortalize(graph)
        # Freeze the hot sub-objects too: the checksum walks into them every
        # touch, so each leaf/row push is itself a cross-hub refcount.
        runloom_c.immortalize(graph[1][1])     # leaves tuple
        runloom_c.immortalize(graph[2][1])     # nested tuple
        for _, row in graph[2][1]:
            runloom_c.immortalize(row)
        runloom_c.immortalize(graph[3][1])     # frozenset

    # Refcount-stability oracle: capture BEFORE the storm.  sys.getrefcount may
    # be absent/unreliable on some builds; guard it.  An immortal object's
    # refcount is frozen at a huge sentinel, so we only assert "unchanged",
    # which holds for the immortal case and (modulo borrow) for the control.
    rc_before = None
    try:
        rc_before = sys.getrefcount(graph)
    except (AttributeError, TypeError):
        rc_before = None

    H.state = {
        "graph": graph,
        "expected": expected,
        "immortal": do_immortal,
        "rc_before": rc_before,
    }
    H.log("graph built: immortalize={0} expected_checksum={1} rc_before={2}".format(
        do_immortal, expected, rc_before))


def worker(H, wid, rng, state):
    graph = state["graph"]
    expected = state["expected"]
    for _ in H.round_range():
        last = None
        for _ in range(TOUCHES_PER_ROUND):
            if not H.running():
                break
            last = graph_checksum(graph)
            H.op(wid)
        if last is not None and not H.check(
                last == expected,
                "checksum mismatch wid={0}: {1} != {2} (immortalize "
                "corrupted/changed the shared graph)".format(
                    wid, last, expected)):
            return
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # Refcount-stability oracle: capture AFTER the storm fully drained.  An
    # immortal graph's refcount must be unchanged (frozen => incref/decref are
    # no-ops); the control run touches only borrowed refs that are released, so
    # its steady-state refcount is also unchanged once drained.  A small
    # tolerance covers the live local borrow held by this very frame.
    st = H.state
    rc_before = st.get("rc_before")
    if rc_before is None:
        H.log("getrefcount unavailable -- skipping refcount-stability oracle")
        return
    try:
        rc_after = sys.getrefcount(st["graph"])
    except (AttributeError, TypeError):
        H.log("getrefcount unavailable at post -- skipping refcount oracle")
        return
    H.log("rc_before={0} rc_after={1} immortal={2}".format(
        rc_before, rc_after, st["immortal"]))
    # tolerance for the transient local borrow(s) on the stack of this check.
    H.check(abs(rc_after - rc_before) <= 4,
            "shared-graph refcount drifted {0} -> {1} (immortal={2}); a frozen "
            "refcount must be a cross-hub incref/decref no-op".format(
                rc_before, rc_after, st["immortal"]))
    # Every spawned worker must have landed at least one op.
    H.check(H.total_ops() >= H.expected,
            "peak ops {0} < expected goroutines {1} (a worker never ran)".format(
                H.total_ops(), H.expected))


if __name__ == "__main__":
    # Availability guard: immortalize is a 3.13t free-threaded lever.  On a
    # GIL-enabled build (or a build missing the symbol) skip cleanly with exit 0
    # so the program is always safe to run in the sweep.
    gil_on = True
    try:
        gil_on = sys._is_gil_enabled()
    except AttributeError:
        gil_on = True
    if gil_on or not hasattr(runloom_c, "immortalize"):
        why = ("GIL enabled (immortalize is a free-threaded lever)"
               if gil_on else "runloom_c.immortalize missing")
        sys.stdout.write("SKIP: " + why + "\n")
        sys.stdout.flush()
        sys.exit(0)

    harness.main("p229_immortalize_shared_graph", body, setup=setup, post=post,
                 default_funcs=4000, add_args=add_args,
                 describe="immortalize a hot shared object graph so cross-hub "
                          "incref/decref are no-ops; assert checksum + "
                          "refcount-stability correctness across --hubs N")
