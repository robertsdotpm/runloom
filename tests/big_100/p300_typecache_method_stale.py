"""big_100 / 300 -- adaptive-interpreter type-cache staleness across hubs.

The single most 3.13t-specific free-threaded hazard, and one no existing
program touches: CPython's adaptive interpreter specializes LOAD_ATTR /
LOAD_METHOD / CALL inline caches per *code object*, and the type method-
resolution cache keys on `tp_version_tag`.  When the SAME hot bytecode runs
concurrently across many M:N hubs (GIL off) while another hub MUTATES the class
(reassigning an attribute, swapping a method, adding/removing an attr -- each
bumps `tp_version_tag` and must deopt the inline caches), a hub can read through
a STALE specialized cache and observe a value the class state never produced --
a silent wrong result, or a torn/half-published pointer -> SIGSEGV.

We make that detectable with the strongest oracle there is: the read can only
ever legally be one of two sentinels.  Each reader, in a hot loop (warmed so the
inline caches are armed), reads three specialization surfaces of a shared object:

  * `inst.tag`  -- a class (type) attribute        -> LOAD_ATTR via the type
  * `inst.m()`  -- an instance method that is swapped -> LOAD_METHOD / CALL cache
  * `inst.dyn`  -- an instance __dict__ attribute   -> LOAD_ATTR_INSTANCE cache

and asserts every value is in {VAL_A, VAL_B}.  The mutators only ever set those
surfaces to VAL_A or VAL_B (and churn a private scratch attr to bump the version
tag harder), so ANY other value -- a stale read, a torn word, a freed object --
means a version-tag/deopt ordering bug under M:N.  `tag` is never deleted, so an
AttributeError on it is itself a failure (a half-applied type mutation).

Invariant (hot, fail-fast): every specialized read in {VAL_A, VAL_B}; no
AttributeError on a never-deleted attr; no SIGSEGV/torn read.

Stresses: adaptive specialization, tp_version_tag invalidation, inline-cache
deopt ordering, cross-hub type mutation, preempt-mid-specialization.

Good TSan / controlled-M:N-replay target: the version-tag-vs-cache ordering is a
pure memory-ordering race; a data-race report on the inline-cache write/read is
often the first signal, before the value oracle even fires.
"""
import random

import harness
import runloom

VAL_A = 0x5A5A0001
VAL_B = 0xA5A50002
LEGAL = frozenset((VAL_A, VAL_B))

WARMUP_READS = 600          # > 256 so LOAD_ATTR / LOAD_METHOD / CALL specialize
READS_PER_ROUND = 2000


def make_class():
    """A fresh class + one shared instance, all surfaces seeded to VAL_A."""

    def m_a(self):
        return VAL_A

    cls = type("Probe", (object,), {"tag": VAL_A, "m": m_a})
    inst = cls()
    inst.dyn = VAL_A
    return cls, inst


def _m_a(self):
    return VAL_A


def _m_b(self):
    return VAL_B


def reader(H, wid, inst, cls):
    """Hot-read the three specialized surfaces; every value must be legal."""
    # Warm the inline caches on THIS reader's bytecode before mutation matters.
    acc = 0
    for _ in range(WARMUP_READS):
        acc ^= inst.tag
        acc ^= inst.m()
        acc ^= inst.dyn
    rno = 0
    for _ in H.round_range():
        if not H.running():
            break
        rno += 1
        for _ in range(READS_PER_ROUND):
            if not H.running():
                break
            # 1) type-attribute LOAD_ATTR
            try:
                v = inst.tag
            except AttributeError:
                H.fail("stale type-cache: AttributeError on never-deleted "
                       "class attr 'tag' (half-applied type mutation)")
                return
            if v not in LEGAL:
                H.fail("stale specialized LOAD_ATTR(tag): {0!r} -- a value no "
                       "class state ever produced".format(v))
                return
            # 2) method LOAD_METHOD / CALL cache
            w = inst.m()
            if w not in LEGAL:
                H.fail("stale specialized CALL(m()): {0!r}".format(w))
                return
            # 3) instance __dict__ LOAD_ATTR cache
            try:
                d = inst.dyn
            except AttributeError:
                H.fail("stale instance-cache: AttributeError on never-deleted "
                       "instance attr 'dyn'")
                return
            if d not in LEGAL:
                H.fail("stale specialized LOAD_ATTR(dyn): {0!r}".format(d))
                return
            H.op(wid)
        H.task_done(wid)


def mutator(H, mid, inst, cls, rng):
    """Toggle every surface between the two legal sentinels and churn a private
    scratch attr to bump tp_version_tag.  A short head-start sleep lets readers
    arm their inline caches before invalidation begins."""
    scratch = "scratch_{0}".format(mid)
    runloom.sleep(0.02)
    for _ in H.round_range():
        if not H.running():
            break
        for _ in range(4000):
            if not H.running():
                break
            val = VAL_A if (rng.getrandbits(1)) else VAL_B
            cls.tag = val                       # invalidate type LOAD_ATTR cache
            cls.m = _m_a if val == VAL_A else _m_b   # invalidate method cache
            inst.dyn = val                      # invalidate instance cache
            setattr(cls, scratch, 1)            # add-attr -> harder version bump
            delattr(cls, scratch)               # remove-attr -> version bump
            if (rng.getrandbits(4)) == 0:
                runloom.yield_now()


def worker(H, wid, rng, state):
    # First few wids are mutators; the rest are readers hammering the SAME class.
    if wid < state["nmut"]:
        mutator(H, wid, state["inst"], state["cls"], rng)
    else:
        reader(H, wid, state["inst"], state["cls"])


def setup(H):
    cls, inst = make_class()
    nmut = min(8, max(2, H.funcs // 50))
    H.state = {"cls": cls, "inst": inst, "nmut": nmut}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.log("specialized reads (ops)={0} mutators={1}".format(
        H.total_ops(), H.state["nmut"]))
    H.check(H.total_ops() > 0, "no specialized reads happened")


if __name__ == "__main__":
    harness.main("p300_typecache_method_stale", body, setup=setup, post=post,
                 default_funcs=4000,
                 describe="hot specialized LOAD_ATTR/CALL across hubs while "
                          "another hub invalidates tp_version_tag; every read "
                          "in {VAL_A,VAL_B} or it's a stale-cache bug")
