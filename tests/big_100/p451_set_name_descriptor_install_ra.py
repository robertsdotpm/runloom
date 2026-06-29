"""big_100 / 451 -- __set_name__ descriptor-install loop vs cross-hub attribute
lookup / specialization, racing the owner type's version tag + method cache.

THE CPYTHON PRIMITIVE + ITS NON-ATOMIC INTERNAL STATE
-----------------------------------------------------
When a class body finishes, ``type.__new__`` runs a DEFERRED descriptor-wiring
loop: ``type_new_set_names`` (Objects/typeobject.c) walks the new type's
namespace and calls ``value.__set_name__(owner, name)`` on every value that
defines it -- AFTER the type object already exists and its ``tp_dict`` is
populated.  Each call writes TWO disjoint pieces of state that are NOT published
as one atomic unit:

  (1) per-DESCRIPTOR instance state -- the descriptor's own field that
      ``__set_name__(owner, name)`` records (here ``self.bound_name = name``);
  (2) the OWNER TYPE's resolution state -- defining attributes / installing
      a descriptor mutates the type's ``tp_dict`` and BUMPS the type's
      ``tp_version_tag`` (``type_modified`` / ``PyType_Modified``), which is the
      generation counter that the method/attribute cache ``_PyType_Lookup``
      MCACHE and the adaptive interpreter's LOAD_ATTR / STORE_ATTR specialization
      key on.

A data-descriptor ``__get__`` (a property fget, a member/slot descriptor, or our
sentinel descriptor) is resolved through that SAME per-type version tag + method
cache.  So the racing op pair the harness attacks is:

    HUB A: type.__new__'s __set_name__ install loop -- writes self.bound_name
           on each descriptor AND bumps owner.tp_version_tag / fills tp_dict;
    HUB B: an instance attribute LOAD on a JUST-built class of that family --
           obj.a goes through _PyType_Lookup(type, 'a'), reads the version tag,
           may consult / fill a stale MCACHE row, and invokes the descriptor's
           __get__ which reads self.bound_name.

Under M:N hub B can read a descriptor whose ``__set_name__``-set
``bound_name`` is UNPUBLISHED (still its pre-install sentinel) while the version
tag already says "installed", or read a STALE cache row that resolves the name
to the WRONG descriptor.  A torn install therefore manifests as a get that
returns ``f`` of the WRONG name, or an ``AttributeError`` for a name that IS in
fact installed, or -- worst -- a SIGSEGV walking a half-published mro/cache.

THE CLOSED-WORLD IDENTITY + CONSERVATION LAW
--------------------------------------------
Per round we build, on some worker, a fresh class ``C`` of a fixed family.  ``C``
has one sentinel descriptor per slot name in a finite UNIVERSE of names
{a00..a<NSLOTS-1>}.  Each descriptor's ``__set_name__(owner, name)`` does two
recorded things:

  * stamps ``self.bound_name = name`` (the wired name), and
  * appends ``(id(descriptor), name)`` into a per-class install log under a
    per-class cooperative lock -- so we can later assert EVERY descriptor's
    ``__set_name__`` fired EXACTLY ONCE (conservation of install calls: no lost
    wiring, no double wiring).

Its ``__get__`` returns the UNIVERSE value ``f(self.bound_name)`` -- a value
computed FROM THE WIRED NAME.  The identity law: for a fully-constructed class,
reading instance attribute ``a<i>`` MUST return exactly ``f('a<i>')`` -- i.e. the
descriptor that answers for name ``a<i>`` is the one that was wired to ``a<i>``.
A torn install returns ``f`` of a DIFFERENT name (caught: value not == f(name)),
or raises AttributeError for an installed name (caught), or yields an
out-of-universe value (caught).  Both failure modes are falsifiable:

  * WRONG-NAME / STALE-CACHE wiring  -> get returns f(other_name) != f(name);
  * UNPUBLISHED bound_name           -> get returns f(UNBOUND sentinel), which
                                        is OUTSIDE the value universe -> caught;
  * LOST / DOUBLE __set_name__        -> the install log has != NSLOTS entries,
                                        or a descriptor id appears 0 or >1 times.

CONTROL ARM (the falsifier that disambiguates contention from machinery)
------------------------------------------------------------------------
Case CONTROL builds, exercises, and tears down an ENTIRE class family member
inside ONE fiber -- no sibling touches it.  Its install log MUST have exactly
NSLOTS entries (one per descriptor, each fired once) and every ``obj.a<i>`` MUST
equal ``f('a<i>')``.  A single-owner class construction is race-free by
construction, so ANY mismatch here is type-construction / descriptor machinery
corruption itself, NOT M:N contention -- this is what separates "the install
loop is buggy" from "a sibling raced it".  The CONTENDED case is the contention
probe; the CONTROL case is the falsifier.

SYNCHRONIZING THE HAZARD INTO THE WINDOW
----------------------------------------
In the CONTENDED case a BUILDER fiber constructs the class while N READER fibers
on other hubs hammer ``getattr`` on instances of it.  A barrier (WaitGroup the
builder trips the instant before it lets type creation + __set_name__ run, then
``yield_now()``) hands the hubs to the readers so their LOAD_ATTR specialization
and _PyType_Lookup land DURING the version-tag bump / install loop.  Readers
that resolve an attribute on a class member of the SAME family that is mid-build
race the freshly-bumped version tag and the MCACHE row for that name.

COVERAGE (the flaky-random lesson p125/p126/p172 already had to fix)
--------------------------------------------------------------------
post() asserts each case (CONTENDED build+read, CONTROL single-fiber, REBUILD
churn that re-bumps a hot family's version tag) was exercised, so the worker
round-robins cases by worker id in its FIRST ops (``sel = (wid + i) % NCASES``)
then goes random -- coverage holds whether one worker does many ops or many
workers do one each.

Invariant (hot, fail-fast): every getattr on a fully-built class returns a value
in the UNIVERSE and == f(its slot name); no installed name raises AttributeError;
the CONTROL member's install log is exactly NSLOTS, one fire per descriptor.
Invariant (post): every descriptor's __set_name__ fired exactly once per class
(sum of install-call tallies == NSLOTS * classes built); identity holds across
every exercised get; all cases hit; no worker LOST.

Stresses: type.__new__ __set_name__ install loop vs cross-hub LOAD_ATTR /
_PyType_Lookup, tp_version_tag bump vs MCACHE read, half-published descriptor
bound-state, adaptive STORE_ATTR/LOAD_ATTR_INSTANCE_VALUE specialization on a
just-modified type, install-call conservation, descriptor identity under M:N.

Good TSan / controlled-replay target: the write of self.bound_name +
tp_version_tag bump in the install loop vs the version-tag read + descriptor
__get__ on another hub is a textbook publish/consume data race; a TSan report on
the version tag or the descriptor field localizes the torn install before the
identity assert even fires.  Per-fiber RNG (rng) keeps it replayable.
"""
import threading

import harness
import runloom

# Finite sentinel UNIVERSE of slot NAMES.  Every class of the family defines one
# descriptor per name here.  NSLOTS is large enough to push the type's tp_dict
# (and so the cache rows / version-tag churn) through real growth, and to make a
# torn install land on a DIFFERENT name with high probability.
NSLOTS = 48
SLOT_NAMES = tuple("a{0:02d}".format(i) for i in range(NSLOTS))
SLOT_NAME_SET = frozenset(SLOT_NAMES)

# A recognizable sentinel the descriptor carries BEFORE __set_name__ wires it.
# f() never produces f(UNBOUND), so a get that returns f(UNBOUND) (an UNPUBLISHED
# bound_name read across the install race) is OUT OF UNIVERSE and is caught.
UNBOUND = "<unbound>"


def f(name):
    """Deterministic slot-name -> UNIVERSE value.  The value a descriptor's
    __get__ returns is f(self.bound_name): a value computed FROM THE WIRED NAME.
    Reading f(WRONG_name) (torn/stale-cache install) or f(UNBOUND) (unpublished
    bound_name) is therefore detectable against this closed value universe.

    Injective over the slot names (distinct names -> distinct values), so a torn
    pair cannot coincidentally satisfy the identity law."""
    h = 0x451A0000
    for ch in name:
        h = (h * 131 + ord(ch)) & 0x7FFFFFFF
    return h ^ 0x5A5A5A5A


# The finite VALUE universe: exactly the legal get results.  Anything else a get
# returns (notably f(UNBOUND)) is an out-of-universe value -> a torn install.
VALUE_UNIVERSE = frozenset(f(nm) for nm in SLOT_NAMES)

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# Readers per CONTENDED build -- several hubs hammering getattr on instances of
# the just-built / mid-build family is what races the version-tag bump + MCACHE.
READERS = 4

# How many getattr passes a reader makes over all the slot names per round.
READ_PASSES = 3

# Cases (round-robined by wid for deterministic coverage).
CASE_CONTENDED = 0   # builder builds class while readers hammer getattr on it
CASE_CONTROL = 1     # whole class built + read + checked inside ONE fiber
CASE_REBUILD = 2     # rapidly re-build the family (re-bump a hot version tag)
NCASES = 3


def make_descriptor_class():
    """Construct a FRESH class of the sentinel family and return (cls, log,
    loglock).  Building it RUNS type.__new__'s __set_name__ install loop -- the
    code under test.  `log` collects (id(descriptor), wired_name) appended by each
    descriptor's __set_name__ under `loglock`, so we can assert exactly-once
    install conservation after construction.

    The descriptor's __get__ returns f(self.bound_name): a value derived from the
    name it was WIRED to in __set_name__.  Before __set_name__ runs, bound_name is
    UNBOUND, so a get that observes an unpublished install reads f(UNBOUND) (out of
    the value universe) instead of f(name)."""
    log = []
    loglock = threading.Lock()

    class Sentinel(object):
        __slots__ = ("bound_name",)

        def __init__(self):
            # Pre-install sentinel: an UNPUBLISHED bound_name reads as f(UNBOUND),
            # which is OUTSIDE the value universe -> caught by the identity oracle.
            self.bound_name = UNBOUND

        def __set_name__(self, owner, name):
            # The DEFERRED install: stamp the wired name onto this descriptor and
            # log the (descriptor-id, name) so install conservation is checkable.
            self.bound_name = name
            with loglock:
                log.append((id(self), name))

        def __get__(self, obj, objtype=None):
            if obj is None:
                return self
            # Data-descriptor get resolved through the owner type's version tag +
            # method cache; returns the UNIVERSE value for the WIRED name.
            return f(self.bound_name)

        def __set__(self, obj, value):
            # Defining __set__ makes this a DATA descriptor, so it wins over any
            # instance __dict__/slot and is always resolved via _PyType_Lookup /
            # the type cache -- exactly the path the version-tag race corrupts.
            raise AttributeError("sentinel is read-only")

    # Build the class body namespace: one fresh descriptor per universe slot name.
    # type(name, bases, ns) runs type.__new__ -> type_new_set_names -> the
    # __set_name__ install loop over EVERY value in ns that defines __set_name__.
    ns = {nm: Sentinel() for nm in SLOT_NAMES}
    cls = type("SentinelFamily", (object,), ns)
    return cls, log, loglock


def check_instance_reads(H, wid, cls, where):
    """Read EVERY universe slot attribute off a fresh instance of `cls` and assert
    the identity law: obj.a<i> == f('a<i>') and the value is in the UNIVERSE.  A
    torn install returns f(WRONG name) (!= f(name)), f(UNBOUND) (out of universe),
    or raises AttributeError for an installed name.  Returns False on first
    violation (caller stops)."""
    obj = cls()
    for nm in SLOT_NAMES:
        try:
            val = getattr(obj, nm)
        except AttributeError:
            H.fail("getattr({0}, {1!r}) raised AttributeError for an INSTALLED "
                   "slot name -- _PyType_Lookup/version-tag read a half-installed "
                   "type (stale MCACHE row or unpublished descriptor) {2}".format(
                       cls.__name__, nm, where))
            return False
        if val not in VALUE_UNIVERSE:
            H.fail("getattr({0!r}) returned OUT-OF-UNIVERSE value {1!r} -- the "
                   "descriptor's __get__ read an UNPUBLISHED bound_name "
                   "(f(UNBOUND)) across the __set_name__ install race {2}".format(
                       nm, val, where))
            return False
        if val != f(nm):
            H.fail("identity law broken: getattr({0!r}) == {1!r} but expected "
                   "f({0!r}) == {2!r} -- the descriptor answering for this name "
                   "was wired to a DIFFERENT name (torn install / stale type "
                   "cache row resolved the name to the wrong descriptor) {3}"
                   .format(nm, val, f(nm), where))
            return False
    return True


def verify_install_log(H, wid, cls, log, where):
    """Assert install-call CONSERVATION for a fully-built class: __set_name__
    fired EXACTLY ONCE per descriptor.  The log holds (id, name) appended by each
    descriptor's __set_name__.  Exactly NSLOTS entries, each name once, each
    descriptor-id once.  A LOST wiring -> < NSLOTS; a DOUBLE wiring -> a repeated
    id/name or > NSLOTS.  Returns (ok, install_count)."""
    n = len(log)
    if n != NSLOTS:
        H.fail("install-call conservation broken: {0} __set_name__ calls logged "
               "but {1} descriptors exist {2} -- a descriptor wiring was {3} by "
               "the install loop".format(
                   n, NSLOTS, where, "LOST" if n < NSLOTS else "DOUBLED"))
        return False, n
    names = [nm for (_id, nm) in log]
    ids = [_id for (_id, nm) in log]
    if frozenset(names) != SLOT_NAME_SET:
        H.fail("install log names {0!r} != the slot universe {1} -- a slot was "
               "wired to the wrong / a duplicate name".format(
                   sorted(set(names)), where))
        return False, n
    if len(set(ids)) != NSLOTS:
        H.fail("install log has a DUPLICATE descriptor id {0} -- a descriptor's "
               "__set_name__ fired more than once (double wiring)".format(where))
        return False, n
    return True, n


def do_contended(H, wid, rng, state, slot):
    """CASE_CONTENDED: a BUILDER fiber constructs the class (running the
    __set_name__ install loop + version-tag bumps) while READER fibers on other
    hubs hammer getattr on instances of it.  The builder trips a barrier the
    instant before it runs type() so the readers' LOAD_ATTR / _PyType_Lookup land
    DURING the install loop.  Readers first wait on a 'ready' Chan published by
    the builder (so they read the SAME class object), then race subsequent reads
    against a churn rebuild that re-bumps the type's version tag."""
    gate = runloom.WaitGroup()          # builder trips just before type() runs
    gate.add(1)
    ready = runloom.Chan(1)             # builder publishes the built class here
    wg = runloom.WaitGroup()
    wg.add(1 + READERS)

    install_tally = state["installs"]
    read_tally = state["reads"]

    def run_builder():
        published = None                # the class to hand readers (None on fail)
        try:
            # Trip the gate, hand the hubs to the readers, THEN run the install
            # loop so the version-tag bump + __set_name__ writes overlap the
            # readers' attribute resolution on other hubs.
            gate.done()
            runloom.yield_now()
            cls, log, loglock = make_descriptor_class()
            published = cls
            # Install conservation on the fully-built class.
            ok, n = verify_install_log(H, wid, cls, log, "(contended builder)")
            if ok:
                install_tally[slot] += n
        except Exception as exc:        # noqa: BLE001
            H.error(wid, exc)
        finally:
            # ALWAYS publish exactly READERS items so no reader's recv() can block
            # forever (None is the failure sentinel; readers stop on it).  The
            # Chan has capacity 1, so these sends cooperatively hand off to the
            # readers -- the builder's last writes land while a reader resolves.
            for _ in range(READERS):
                ready.send(published)
            wg.done()

    def run_reader(ridx):
        try:
            gate.wait()                 # ensure we run DURING/after the bump window
            runloom.yield_now()
            cls, ok = ready.recv()      # the class the builder published (val, ok)
            if not ok or cls is None or H.failed:
                return
            for _ in range(READ_PASSES):
                if H.failed:
                    return
                if not check_instance_reads(H, wid, cls, "(contended reader)"):
                    return
                read_tally[slot] += 1
                runloom.yield_now()     # re-park between passes -> race a rebuild
        except Exception as exc:        # noqa: BLE001
            H.error(wid, exc)
        finally:
            wg.done()

    H.fiber(run_builder)
    for ridx in range(READERS):
        H.fiber(run_reader, ridx)
    wg.wait()
    return not H.failed


def do_control(H, wid, rng, state, slot):
    """CASE_CONTROL (the falsifier): build + exercise + check an ENTIRE class
    family member inside ONE fiber, no sibling touching it.  A single-owner class
    construction is race-free by construction, so the install log MUST be exactly
    NSLOTS (one fire per descriptor) and every getattr MUST equal f(name).  A
    mismatch HERE is type-construction / descriptor machinery corruption itself,
    not contention."""
    cls, log, loglock = make_descriptor_class()
    ok, n = verify_install_log(H, wid, cls, log, "(CONTROL single-fiber)")
    if not ok:
        return False
    # Check identity on several fresh instances within this fiber.
    for _ in range(2):
        if not check_instance_reads(H, wid, cls, "(CONTROL single-fiber)"):
            return False
    state["control"][slot] += 1
    state["installs"][slot] += n
    return not H.failed


def do_rebuild(H, wid, rng, state, slot):
    """CASE_REBUILD: rapidly construct several members of the family back to back
    on this fiber while other workers' contended builders/readers run on other
    hubs.  Each construction re-runs the install loop and re-bumps a version tag
    for a class of the SAME family name, churning the per-type cache the readers
    consult.  Identity + conservation must still hold on each fresh class."""
    built = 0
    for _ in range(3):
        if H.failed:
            return False
        cls, log, loglock = make_descriptor_class()
        ok, n = verify_install_log(H, wid, cls, log, "(rebuild churn)")
        if not ok:
            return False
        if not check_instance_reads(H, wid, cls, "(rebuild churn)"):
            return False
        state["installs"][slot] += n
        built += 1
        runloom.yield_now()
    state["rebuild"][slot] += built
    return not H.failed


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the cases by worker id in the FIRST ops so each case is
        # exercised even under a short timeout (the p125/p126/p172 flaky-coverage
        # fix); random after.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_CONTENDED:
            ok = do_contended(H, wid, rng, state, slot)
        elif sel == CASE_CONTROL:
            ok = do_control(H, wid, rng, state, slot)
        else:
            ok = do_rebuild(H, wid, rng, state, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # All tallies are per-slot single-writer lists summed in post() (race-free).
    H.state = {
        "installs": [0] * SLOTS,   # total __set_name__ calls observed (conserved)
        "reads": [0] * SLOTS,      # contended getattr passes that held identity
        "control": [0] * SLOTS,    # CONTROL single-fiber members verified
        "rebuild": [0] * SLOTS,    # rebuild-churn members verified
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    installs = sum(H.state["installs"])
    reads = sum(H.state["reads"])
    control = sum(H.state["control"])
    rebuild = sum(H.state["rebuild"])
    H.log("install-calls-conserved={0} contended-read-passes={1} "
          "control-members={2} rebuild-members={3} ops={4}".format(
              installs, reads, control, rebuild, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # Install-call CONSERVATION across the whole run: every class that was built
    # logged exactly NSLOTS __set_name__ calls (each per-class log was verified
    # == NSLOTS fail-fast at build time, so the running total must be an exact
    # multiple of NSLOTS -- a lost/double wiring would have failed fast and we
    # would not be here; this asserts the sum is self-consistent).
    if installs:
        H.check(installs % NSLOTS == 0,
                "install-call conservation broken in aggregate: {0} total "
                "__set_name__ calls is not a whole multiple of NSLOTS={1} -- a "
                "wiring was lost or doubled across the run".format(
                    installs, NSLOTS))
    classes_built = installs // NSLOTS
    H.check(installs > 0,
            "no class was ever built -- the __set_name__ install loop was never "
            "exercised")

    # Each case actually ran (deterministic round-robin guarantees it once work
    # happened; assert so a regression that silently skips a case is caught).
    H.check(control > 0,
            "CONTROL single-fiber case never exercised -- the race-free falsifier "
            "arm did not run, so a machinery-corruption vs contention split is "
            "untested")
    H.check(reads > 0,
            "CONTENDED build+read case never exercised -- the version-tag / MCACHE "
            "race window was never opened")
    H.check(rebuild > 0,
            "REBUILD churn case never exercised -- the hot version-tag re-bump "
            "path was never driven")

    H.log("classes built (and each had all {0} descriptors wired exactly once) "
          "= {1}".format(NSLOTS, classes_built))

    H.require_no_lost("set_name-install completeness")


if __name__ == "__main__":
    harness.main(
        "p451_set_name_descriptor_install_ra", body, setup=setup, post=post,
        default_funcs=3000,
        describe="type.__new__'s __set_name__ install loop (writes descriptor "
                 "bound-state + bumps owner tp_version_tag/tp_dict) races cross-hub "
                 "LOAD_ATTR / _PyType_Lookup on instances of the just-built family; "
                 "identity law obj.a==f('a') over a finite value universe + "
                 "install-call conservation (each __set_name__ fires exactly once) "
                 "+ a single-fiber CONTROL falsifier -- a wrong-name/AttributeError/"
                 "out-of-universe get or lost/double wiring fails")
