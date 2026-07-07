"""big_100 / 609 -- tabnanny.Whitespace indentation-algebra PURITY under M:N.

tabnanny's core is the pure value class ``Whitespace(ws)``.  Given a leading-
whitespace string ws (a run of spaces ' ' and tabs '\\t'), its __init__ computes a
handful of DERIVED, DETERMINISTIC properties of that one string:

    n          -- number of leading whitespace chars consumed (stops at first
                  non-whitespace)
    nt         -- number of tabs among those n chars
    norm       -- the "normal form" (count_tuple, trailing): count[i] is how many
                  times the pattern  S*i + T  occurs, trailing is the count of
                  trailing spaces
    is_simple  -- True iff raw[:n] is of the form (T*)(S*)  (len(count) <= 1)

and derived pure methods:

    indent_level(tabsize)        -- the column an editor with that tab stop shows
    longest_run_of_spaces()      -- max(len(count)-1, trailing)
    equal(other)                 -- norm == other.norm
    less(other)                  -- indent_level(t) < other.indent_level(t) for ALL t>=1
    not_equal_witness(other)     -- witnessing tab sizes where indent_level differs
    not_less_witness(other)      -- witnessing tab sizes where NOT strictly less

Every one of these is a PURE function of the input string(s): no globals, no shared
state, no I/O.  A ``Whitespace`` instance built by a fiber is a SINGLE-OWNER object
(a fiber-local variable, never shared).  The load-bearing law: computing these
derived properties on a fiber-local string, then RE-computing them across a yield
(possibly after hub migration while thousands of siblings churn the scheduler),
MUST return BIT-IDENTICAL results that also satisfy the module's own closed-form
identities.  If a fiber's frame/stack is torn across a hub migration, or a
sibling's computation leaks into this fiber's locals, a derived property would
diverge from its pre-yield baseline or from the independent closed-form.

WHY THIS IS A LEGITIMATE SINGLE-OWNER ORACLE (not a shared-object race):

  The Whitespace objects, the input strings, and every intermediate are created in
  fiber-local variables and never handed to another fiber.  tabnanny.Whitespace
  holds NO module-global mutable state (the module globals `verbose`/`filename_only`
  are read-only ints we never touch; check()/main()/process_tokens() -- the I/O and
  token paths -- are NOT exercised).  So this is pure arithmetic over private data,
  exactly like p490's single-owner arm.  On plain OS threads (GIL on or off) the
  same computation is deterministic and thread-safe; under a CORRECT runloom it must
  be too.  A divergence is therefore a runloom frame/stack-isolation bug, never
  documented Python semantics.

TWO INDEPENDENT CROSS-CHECKS make a FAIL mean a real bug, not a tautology:

  (1) CLOSED-FORM anchor.  n/nt/norm/is_simple and indent_level are re-derived by a
      SEPARATE reimplementation in this file and asserted bit-equal to what the
      module computed -- proves the module actually ran and the result is the
      genuine value (non-vacuous), and pins the expected value we compare across the
      yield to a known constant.

  (2) STRUCTURAL theorem.  The module documents "It's A Theorem that
      m.indent_level(t) == n.indent_level(t) for all t >= 1  iff  m.norm == n.norm".
      We check this TWO ways that must agree: the norm-based equal() versus a
      brute-force sweep of indent_level(t) over t in 1..N (N = the module's own
      sharp bound max(longest_run_of_spaces)+1).  Likewise less() versus a
      brute-force strict-less sweep, and the *_witness() lists against the same
      sweep.  These are value-domain vs structure-domain computations of the same
      truth; they can only agree if BOTH computations are intact across the yield.

ORACLES:
  * LOAD-BEARING -- WHITESPACE PURITY (worker, HARD, fail-fast).  Each fiber makes
    two fiber-local whitespace strings, builds their Whitespace objects, snapshots
    every derived property, YIELDS (yield_now + occasional sleep so a sibling
    reliably interleaves on another hub), then rebuilds fresh objects and re-reads
    the old ones -- asserting every property is bit-identical to the snapshot, equal
    to the independent closed-form, and consistent with the brute-force theorem
    sweep.  Single-owner: nothing is shared.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-computation
    across a yield never returns; the watchdog + require_no_lost catch it.

FAIL ON: a derived Whitespace property that changes across a yield, disagrees with
the independent closed-form, or violates the module's own indent-level theorem --
i.e. a torn frame / cross-fiber locals leak / lost-wakeup in the runloom runtime.

Stresses: pure integer/tuple arithmetic in a stdlib value class across hub
migration + yield, fiber-frame isolation of Python locals and small-int/tuple
intermediates, deterministic recompute stability under sustained M:N churn.
"""
import tabnanny

import harness
import runloom


# ---- independent closed-form reimplementation (the anchor) ----------------
# A SEPARATE computation of Whitespace's derived properties, written from the
# module's documented definitions.  Used only to pin the expected constants;
# it never touches the module's objects.
def ref_scan(ws):
    """Recompute (n, nt, norm) for a leading-whitespace string, independently."""
    count = []
    b = n = nt = 0
    for ch in ws:
        if ch == ' ':
            n += 1
            b += 1
        elif ch == '\t':
            n += 1
            nt += 1
            if b >= len(count):
                count = count + [0] * (b - len(count) + 1)
            count[b] += 1
            b = 0
        else:
            break
    norm = (tuple(count), b)
    return n, nt, norm


def ref_indent_level(norm, nt, tabsize):
    """Independent closed-form indent_level from (norm, nt)."""
    count, trailing = norm
    il = 0
    for i in range(tabsize, len(count)):
        il += i // tabsize * count[i]
    return trailing + tabsize * (il + nt)


def ref_longest_run(norm):
    count, trailing = norm
    return max(len(count) - 1, trailing)


# ---- fiber-local whitespace-string generator ------------------------------
# Strings mix spaces and tabs (some in the simple (T*)(S*) form, some tangled)
# and sometimes end in a non-whitespace char (which Whitespace must stop at).
# Runs are bounded so the module's sharp bound N = longest_run+1 stays small.
MAX_WS = 40


def gen_ws(rng):
    """A fiber-local leading-whitespace string (private, single-owner)."""
    kind = rng.randrange(4)
    if kind == 0:
        # simple (T*)(S*)
        t = rng.randrange(0, 6)
        s = rng.randrange(0, 10)
        body = '\t' * t + ' ' * s
    elif kind == 1:
        # tangled mix
        length = rng.randrange(0, MAX_WS)
        body = ''.join('\t' if rng.random() < 0.4 else ' ' for _ in range(length))
    elif kind == 2:
        # spaces then tabs then spaces (forces multi-entry count tuple)
        body = ' ' * rng.randrange(0, 6) + '\t' + ' ' * rng.randrange(0, 6) + '\t' \
               + ' ' * rng.randrange(0, 8)
    else:
        body = ''
    # Sometimes append a non-whitespace tail; Whitespace must ignore it entirely.
    if rng.random() < 0.5:
        body = body + rng.choice(['x', '#', 'def', '1', '\tx '])
    return body


def check_one_string(H, wid, ws):
    """Full purity oracle for ONE fiber-local whitespace string across a yield.

    Returns the built Whitespace object (so a sibling comparison can reuse it),
    or None if a check failed."""
    w = tabnanny.Whitespace(ws)

    # Independent closed-form anchor -- must match the module exactly, pre-yield.
    exp_n, exp_nt, exp_norm = ref_scan(ws)
    if w.n != exp_n or w.nt != exp_nt or w.norm != exp_norm:
        H.fail("Whitespace closed-form MISMATCH (pre-yield) for ws={0!r} (wid {1}): "
               "module (n={2}, nt={3}, norm={4}) != reference (n={5}, nt={6}, "
               "norm={7})".format(ws, wid, w.n, w.nt, w.norm,
                                  exp_n, exp_nt, exp_norm))
        return None
    exp_simple = len(exp_norm[0]) <= 1
    if w.is_simple != exp_simple:
        H.fail("Whitespace.is_simple wrong (pre-yield) for ws={0!r} (wid {1}): "
               "module {2} != reference {3}".format(ws, wid, w.is_simple, exp_simple))
        return None
    exp_lr = ref_longest_run(exp_norm)
    if w.longest_run_of_spaces() != exp_lr:
        H.fail("longest_run_of_spaces wrong (pre-yield) for ws={0!r} (wid {1}): "
               "module {2} != reference {3}".format(
                   ws, wid, w.longest_run_of_spaces(), exp_lr))
        return None

    # Baseline: indent_level over a range that covers the module's sharp bound,
    # plus common editor tab stops.  Snapshot before the yield.
    tsmax = max(exp_lr + 1, 8)
    base_il = [w.indent_level(ts) for ts in range(1, tsmax + 1)]
    for idx, ts in enumerate(range(1, tsmax + 1)):
        exp_il = ref_indent_level(exp_norm, exp_nt, ts)
        if base_il[idx] != exp_il:
            H.fail("indent_level({0}) closed-form MISMATCH (pre-yield) for "
                   "ws={1!r} (wid {2}): module {3} != reference {4}".format(
                       ts, ws, wid, base_il[idx], exp_il))
            return None

    # ---- YIELD: let siblings run / migrate this fiber to another hub ----------
    runloom.yield_now()
    if w.nt & 1:
        runloom.sleep(0.0002)

    # ---- re-derive on a FRESH object and RE-READ the old one -----------------
    w2 = tabnanny.Whitespace(ws)
    if (w2.n, w2.nt, w2.norm, w2.is_simple) != (exp_n, exp_nt, exp_norm, exp_simple):
        H.fail("Whitespace RECOMPUTE diverged across a yield for ws={0!r} (wid {1}): "
               "post-yield (n={2}, nt={3}, norm={4}, simple={5}) != expected "
               "(n={6}, nt={7}, norm={8}, simple={9}) -- torn frame / cross-fiber "
               "locals leak in the runtime".format(
                   ws, wid, w2.n, w2.nt, w2.norm, w2.is_simple,
                   exp_n, exp_nt, exp_norm, exp_simple))
        return None
    # The original object's attributes must be unchanged too (single-owner object
    # read across the yield).
    if (w.n, w.nt, w.norm, w.is_simple) != (exp_n, exp_nt, exp_norm, exp_simple):
        H.fail("original Whitespace object MUTATED across a yield for ws={0!r} "
               "(wid {1}): now (n={2}, nt={3}, norm={4}, simple={5}) != expected "
               "-- single-owner object state was corrupted".format(
                   ws, wid, w.n, w.nt, w.norm, w.is_simple))
        return None
    post_il = [w.indent_level(ts) for ts in range(1, tsmax + 1)]
    if post_il != base_il:
        H.fail("indent_level sweep changed across a yield for ws={0!r} (wid {1}): "
               "pre {2} != post {3} -- pure function returned a different value "
               "after the yield (torn frame)".format(ws, wid, base_il, post_il))
        return None
    return w


def worker(H, wid, rng, state):
    """Each fiber: build two fiber-local Whitespace objects, verify each is pure
    and stable across a yield, then cross-check the module's indent-level theorem
    (equal/less/witness) against an independent brute-force sweep -- all on private
    single-owner data."""
    counts = state["checks"]
    for _ in H.round_range():
        if not H.running():
            break

        ws_a = gen_ws(rng)
        ws_b = gen_ws(rng)

        wa = check_one_string(H, wid, ws_a)
        if H.failed:
            return
        if wa is None:
            continue
        wb = check_one_string(H, wid, ws_b)
        if H.failed:
            return
        if wb is None:
            continue

        # ---- STRUCTURAL theorem cross-check (single-owner objects) -----------
        # Sharp bound from the module's own definition; a brute-force sweep of
        # indent_level over 1..n is the value-domain truth that equal()/less()
        # (structure-domain, norm-based) must exactly agree with.
        n_sharp = max(wa.longest_run_of_spaces(), wb.longest_run_of_spaces()) + 1
        rng_ts = range(1, n_sharp + 1)

        brute_equal = all(wa.indent_level(t) == wb.indent_level(t) for t in rng_ts)
        norm_equal = wa.equal(wb)
        if norm_equal != brute_equal:
            H.fail("equal() theorem VIOLATED for ws_a={0!r} ws_b={1!r} (wid {2}): "
                   "equal()={3} (norm {4} vs {5}) but brute-force indent_level "
                   "equality over t in 1..{6} = {7}".format(
                       ws_a, ws_b, wid, norm_equal, wa.norm, wb.norm,
                       n_sharp, brute_equal))
            return
        # equal() must be exactly norm==norm (its definition).
        if norm_equal != (wa.norm == wb.norm):
            H.fail("equal() != (norm==norm) for ws_a={0!r} ws_b={1!r} (wid {2})".format(
                ws_a, ws_b, wid))
            return

        if not norm_equal:
            wit = wa.not_equal_witness(wb)
            if not wit:
                H.fail("not_equal_witness EMPTY though not equal for ws_a={0!r} "
                       "ws_b={1!r} (wid {2}) -- the sharp bound failed".format(
                           ws_a, ws_b, wid))
                return
            for ts, i1, i2 in wit:
                if i1 == i2 or wa.indent_level(ts) != i1 or wb.indent_level(ts) != i2:
                    H.fail("bad not_equal witness {0} for ws_a={1!r} ws_b={2!r} "
                           "(wid {3}): recompute a={4} b={5}".format(
                               (ts, i1, i2), ws_a, ws_b, wid,
                               wa.indent_level(ts), wb.indent_level(ts)))
                    return

        # less(): documented as strict-less-for-all-t; brute-force the same sweep.
        brute_less = all(wa.indent_level(t) < wb.indent_level(t) for t in rng_ts)
        algo_less = wa.less(wb)
        if algo_less != brute_less:
            H.fail("less() theorem VIOLATED for ws_a={0!r} ws_b={1!r} (wid {2}): "
                   "less()={3} but brute-force strict-less over t in 1..{4} = {5} "
                   "(a.norm={6} b.norm={7})".format(
                       ws_a, ws_b, wid, algo_less, n_sharp, brute_less,
                       wa.norm, wb.norm))
            return
        if not algo_less:
            wit = wa.not_less_witness(wb)
            if not wit:
                H.fail("not_less_witness EMPTY though not less for ws_a={0!r} "
                       "ws_b={1!r} (wid {2})".format(ws_a, ws_b, wid))
                return
            for ts, i1, i2 in wit:
                if not (i1 >= i2) or wa.indent_level(ts) != i1 \
                        or wb.indent_level(ts) != i2:
                    H.fail("bad not_less witness {0} for ws_a={1!r} ws_b={2!r} "
                           "(wid {3})".format((ts, i1, i2), ws_a, ws_b, wid))
                    return

        # Documented special case: for two SIMPLE forms, less() iff
        # len < other.len and num_tabs <= other.num_tabs.
        if wa.is_simple and wb.is_simple:
            special = (wa.n < wb.n) and (wa.nt <= wb.nt)
            if algo_less != special:
                H.fail("simple-form less() special case VIOLATED for ws_a={0!r} "
                       "ws_b={1!r} (wid {2}): less()={3} but (n {4}<{5}) and "
                       "(nt {6}<={7}) = {8}".format(
                           ws_a, ws_b, wid, algo_less, wa.n, wb.n,
                           wa.nt, wb.nt, special))
                return

        counts[wid] += 1                 # single-writer-per-slot, race-free
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # One race-free slot per worker (wid-indexed), allocated where H.funcs is known.
    H.state = {"checks": [0] * H.funcs}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    total = sum(H.state["checks"])
    H.log("tabnanny.Whitespace purity: {0} single-owner check batches passed "
          "(closed-form anchor + cross-yield stability + indent-level theorem "
          "sweep, all fail-fast); ops={1}".format(total, H.total_ops()))
    # NON-VACUITY: the load-bearing purity arm actually ran.
    H.check(total > 0,
            "no Whitespace purity checks ran -- the load-bearing arm was never "
            "exercised (oracle would be vacuous)")
    # COMPLETENESS: no fiber parked-then-vanished mid-computation.
    H.require_no_lost("tabnanny.Whitespace purity")


if __name__ == "__main__":
    harness.main(
        "p609_tabnanny_whitespace", body, setup=setup, post=post,
        default_funcs=8000,
        describe="tabnanny.Whitespace is a pure indentation-algebra value class "
                 "(n/nt/norm/is_simple/indent_level/equal/less/witness) with no "
                 "shared state.  LOAD-BEARING: each fiber builds fiber-local "
                 "Whitespace objects, snapshots every derived property, yields "
                 "(hub migration), then re-derives on fresh objects and re-reads "
                 "the old ones -- every property must be bit-identical to the "
                 "snapshot, equal to an independent closed-form, and consistent "
                 "with the module's own indent-level theorem (equal/less/witness "
                 "vs a brute-force indent_level sweep over the sharp tab-size "
                 "bound).  A divergence is a torn frame / cross-fiber locals leak "
                 "in the runtime")
