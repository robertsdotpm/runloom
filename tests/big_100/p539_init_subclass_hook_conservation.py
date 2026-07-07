"""big_100 / 539 -- __init_subclass__ hook-fire CONSERVATION under M:N.

THE CPYTHON PRIMITIVE + ITS NON-ATOMIC EXECUTION POINT
------------------------------------------------------
``__init_subclass__`` is an implicit classmethod hook that CPython invokes from
INSIDE ``type.__new__`` (Objects/typeobject.c, ``type_new_impl`` ->
``type_new_init_subclass``) while a subclass is being constructed.  It runs USER
CODE at a delicate mid-type-construction point: the new subclass object already
exists and its ``tp_dict`` is populated, and CPython walks the MRO one step above
the new class to find the nearest ``__init_subclass__`` and calls it as

    super().__init_subclass__(cls=<the new subclass>, **class_keyword_args)

exactly ONCE per subclass definition.  The two pieces of information the hook
receives -- ``cls`` (the freshly built subclass) and ``**kwargs`` (the class
keyword arguments from ``class Sub(Base, tag=7): ...``) -- are handed over at the
tail of ``type.__new__``, after the class-keyword dict has been parsed out of the
metaclass call.

WHERE M:N COULD BREAK IT (the hazard this program probes)
---------------------------------------------------------
``type.__new__`` is a long C routine.  If a hub-migration / preemption point
lands inside two fibers' concurrent ``type_new`` runs, the hook-dispatch machinery
(the MRO walk that locates ``__init_subclass__``, and the class-keyword-args dict
that is threaded through to it) could -- IF runloom desynchronised the fibers'
type-construction state -- cause the hook to:

  * fire the WRONG NUMBER of times for a given base (a lost fire => a subclass
    silently skipped its hook; a doubled fire => one subclass ran the hook twice);
  * receive a SIBLING FIBER's class-keyword arguments (``kwargs`` from a
    different fiber's ``class Sub(..., tag=X)`` leaking into this fiber's hook);
  * receive the wrong ``cls`` (a sibling's subclass object).

Any of those is a torn hook-dispatch -- a real runtime bug -- and is directly
falsifiable by a single-owner closed-world counting law.

THE CLOSED-WORLD CONSERVATION LAW (single-owner, LOAD-BEARING)
-------------------------------------------------------------
Each fiber builds its OWN private base class whose ``__init_subclass__`` appends
``(cls.__name__, dict(kwargs))`` into a FIBER-LOCAL list ``log`` (captured by
closure -- created inside the fiber, never shared, single-writer).  The fiber then
defines exactly ``K`` subclasses of that base, each with a DISTINCT, per-fiber-
unique class-keyword payload (``tag = wid*TAG_SCALE + i``, plus ``wid`` itself),
yielding between definitions so siblings reliably interleave their own
type-construction on other hubs.

After building all K subclasses the fiber -- the SOLE owner of ``Base`` and
``log`` -- asserts the closed-world law:

  * ``len(log) == K``  -- the hook fired EXACTLY once per subclass: no lost fire,
    no doubled fire, and no sibling fiber's subclass leaked a fire into this
    fiber's base (a foreign fire would push len past K or, if it displaced one of
    ours, mismatch the ordered payloads below);
  * ``log[i] == (expected_name_i, expected_kwargs_i)`` for every i -- each hook
    invocation saw the RIGHT subclass name and the RIGHT (this-fiber-unique)
    class-keyword arguments.  A cross-fiber kwargs leak shows up as a ``tag`` that
    is not ``wid*TAG_SCALE + i`` (out of this fiber's private value band), i.e. a
    sibling's payload; a wrong ``cls`` shows up as a name mismatch.

``K`` is then written to ``offered[wid]`` (ONE writer per slot, race-free) so
post() sums the closed-world global total of hook-fires-that-were-verified.  A
lost/doubled/leaked fire fails FAST inside the worker; reaching post with no
failure already proves every per-round law held, and post() asserts the run was
non-vacuous (``sum(offered) > 0``) and complete (``require_no_lost``).

WHY THIS IS SINGLE-OWNER (not a shared-mutable false positive)
--------------------------------------------------------------
``Base``, its closed-over ``log``, and the K subclasses are ALL created inside one
fiber and never handed to any sibling.  Only this fiber's ``type.__new__`` calls
trigger this ``Base.__init_subclass__``, so ``log`` has exactly one writer.  A
correct runtime therefore makes the oracle PASS deterministically (exit 0); it can
only fail if the runtime itself mis-dispatches the hook across concurrent
type-construction -- a genuine bug (lost/doubled hook fire, cross-fiber
kwargs/cls leak), never documented Python semantics.

Stresses: ``type.__new__`` -> ``type_new_init_subclass`` hook dispatch, the MRO
walk locating ``__init_subclass__``, class-keyword-argument threading into the
hook, and per-fiber type-construction isolation across hub migration + yields.
A single-owner conservation miscount, or a foreign ``tag`` in a fiber's private
value band, is the runloom hook-dispatch bug.
"""
import types

import harness
import runloom

# Per-fiber class-keyword values live in a private band [wid*TAG_SCALE,
# wid*TAG_SCALE + K).  A foreign tag observed in a fiber's own log is a
# cross-fiber kwargs leak.  TAG_SCALE > K so bands never overlap between fibers.
TAG_SCALE = 100000

# Subclasses built per fiber per round.  Enough that a lost/doubled hook fire
# moves len(log) off K detectably, and there are several yield-separated
# type-construction points for a sibling to interleave into; small enough that
# many rounds complete under the timeout.
K = 8


def make_base(log):
    """Create a FIBER-LOCAL base class whose ``__init_subclass__`` records every
    subclass fire into ``log`` (captured by closure, single-writer).

    ``__init_subclass__`` is auto-wrapped as an implicit classmethod; ``cls`` is
    the freshly built subclass and ``kwargs`` are that subclass's class-keyword
    arguments.  We forward to ``super().__init_subclass__()`` with NO args because
    ``object.__init_subclass__`` rejects keyword arguments (the standard pattern:
    the hook consumes the kwargs it understands and passes the rest up -- here we
    consume all of them)."""
    class Base:
        def __init_subclass__(cls, **kwargs):
            log.append((cls.__name__, dict(kwargs)))
            super().__init_subclass__()
    return Base


def run_round(H, wid, idx, state):
    """One conservation round: build a private Base + K subclasses with distinct
    per-fiber-unique class-keyword payloads across yields, then assert the hook
    fired exactly once per subclass with the right name and (this-fiber-private)
    kwargs.  Returns True on success (records K into offered[wid]), False after a
    fail-fast."""
    log = []
    Base = make_base(log)

    expected = []
    for i in range(K):
        name = "Sub_W{0}_R{1}_I{2}".format(wid, idx, i)
        tag = wid * TAG_SCALE + i
        kw = {"tag": tag, "wid": wid, "i": i}
        # types.new_class(name, bases, kwds) threads `kwds` into type.__new__ as
        # the class keyword arguments -> Base.__init_subclass__(cls=Sub, **kw).
        # This is the exact hook-dispatch point the hazard targets.
        types.new_class(name, (Base,), dict(kw))
        expected.append((name, kw))
        # YIELD at the hazard boundary: a sibling fiber's own type.__new__ /
        # __init_subclass__ dispatch reliably interleaves here on another hub.
        runloom.yield_now()
        if i & 1:
            runloom.sleep(0.0001)

    # ---- closed-world conservation law (single-owner log) --------------------
    # 1) exact fire count: the hook fired once per subclass -- no lost fire, no
    #    doubled fire, no sibling fire leaked into this fiber's base.
    if len(log) != K:
        H.fail("__init_subclass__ fire count WRONG: base for wid {0} round {1} "
               "logged {2} hook fires, expected exactly K={3} -- a hook fire was "
               "{4} (lost/doubled hook dispatch, or a sibling fiber's subclass "
               "leaked a fire into this fiber's single-owner base) log={5!r}".format(
                   wid, idx, len(log), K,
                   "LOST" if len(log) < K else "DOUBLED/LEAKED", log))
        return False

    # 2) per-fire identity: each hook invocation saw the right subclass name and
    #    this-fiber-private class-keyword arguments (a foreign tag / name is a
    #    cross-fiber kwargs/cls leak).
    for i in range(K):
        exp_name, exp_kw = expected[i]
        got_name, got_kw = log[i]
        if got_name != exp_name:
            H.fail("__init_subclass__ saw WRONG cls name at fire {0} (wid {1} "
                   "round {2}): got {3!r} expected {4!r} -- a sibling fiber's "
                   "subclass object was passed to this fiber's hook (torn cls "
                   "dispatch)".format(i, wid, idx, got_name, exp_name))
            return False
        if got_kw != exp_kw:
            H.fail("__init_subclass__ saw WRONG kwargs at fire {0} (wid {1} "
                   "round {2}): got {3!r} expected {4!r} -- a sibling fiber's "
                   "class-keyword arguments leaked into this fiber's hook (the "
                   "tag {5} is outside this fiber's private band "
                   "[{6},{7}))".format(
                       i, wid, idx, got_kw, exp_kw, got_kw.get("tag"),
                       wid * TAG_SCALE, wid * TAG_SCALE + K))
            return False
        # Defensive band check: the tag MUST lie in this fiber's private band.
        if got_kw.get("tag") != wid * TAG_SCALE + i:
            H.fail("__init_subclass__ tag OUT OF BAND at fire {0} (wid {1}): "
                   "tag {2} is not wid*TAG_SCALE+i={3} -- cross-fiber kwargs "
                   "leak".format(i, wid, got_kw.get("tag"), wid * TAG_SCALE + i))
            return False

    # Every offered hook-fire landed exactly once, correctly attributed.  Record
    # the K verified fires into the race-free per-worker slot.
    state["offered"][wid] += K
    return True


def worker(H, wid, rng, state):
    idx = 0
    for _ in H.round_range():
        if not H.running():
            break
        if not run_round(H, wid, idx, state):
            return
        H.op(wid)
        H.task_done(wid)
        idx += 1


def setup(H):
    # offered: ONE slot per worker (wid-indexed, single-writer, race-free).
    # Allocated here where H.funcs is known.
    H.state = {
        "offered": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    offered = sum(H.state["offered"])
    H.log("__init_subclass__ hook fires conserved this run: {0} (every per-round "
          "exact-fire-count + per-fire identity check passed fail-fast); "
          "ops={1}".format(offered, H.total_ops()))

    # CONSERVATION self-consistency: every verified round contributed exactly K
    # fires, so the global total must be a whole multiple of K (a lost/doubled
    # fire would have failed fast; this asserts the aggregate is self-consistent).
    if offered:
        H.check(offered % K == 0,
                "hook-fire conservation broken in aggregate: {0} total verified "
                "fires is not a whole multiple of K={1} -- a fire was lost or "
                "doubled across the run".format(offered, K))

    # NON-VACUITY: the load-bearing single-owner hook-dispatch hazard actually ran.
    H.check(offered > 0,
            "no __init_subclass__ conservation rounds completed -- the hook-"
            "dispatch race window was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside a
    # type.__new__ / __init_subclass__ dispatch on a desynced construction).
    H.require_no_lost("init_subclass-hook conservation")


if __name__ == "__main__":
    harness.main(
        "p539_init_subclass_hook_conservation", body, setup=setup, post=post,
        default_funcs=4000,
        describe="__init_subclass__ fires from inside type.__new__ during subclass "
                 "creation, running user code mid-type-construction and receiving "
                 "the new subclass (cls) plus its class-keyword arguments.  Under "
                 "M:N, if a hub-migration interleaves two fibers' concurrent "
                 "type_new runs, the hook could fire the wrong number of times or "
                 "receive a sibling's cls/kwargs.  LOAD-BEARING single-owner "
                 "conservation: each fiber's OWN base whose __init_subclass__ "
                 "appends (cls.__name__, kwargs) into a fiber-local list; the "
                 "fiber builds K subclasses with per-fiber-unique class-keyword "
                 "payloads across yields, then asserts the list has EXACTLY K "
                 "entries each matching what it created (a foreign tag is a cross-"
                 "fiber kwargs leak; a wrong count is a lost/doubled hook fire)")
