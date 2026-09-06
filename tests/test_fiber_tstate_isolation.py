"""Per-fiber interpreter state: exception state, and trace/profile hooks.

Ported from greenlet's suite (``test_greenlet.py::test_exc_state`` and
``test_tracing.py``), because runloom's fibers have the same underlying hazard
and none of it was covered: a fiber saves and restores a slice of
``PyThreadState`` across every switch (``runloom_sched_pystate.c.inc``), and
until this file there was NO test touching ``exc_info`` / ``current_exception``
or the trace/profile hooks at all.  They were implemented, decided in the
tstate manifest, and never once executed.

VERIFIED BY MUTATION, because a passing test proves nothing on its own:

  * disabling the exception-chain restore in ``runloom_sched_pystate.c.inc``
    kills the three exception tests and leaves the rest green;
  * disabling the ``c_traceobj``/``c_profileobj`` restore kills
    ``test_a_fiber_does_not_leak_its_tracer_back_to_the_caller``,
    ``test_profile_hook_is_saved_and_restored_too`` and
    ``test_each_fiber_keeps_its_own_tracer_across_a_switch``.

Note WHICH mutation was needed for the second one: disabling the
``c_tracefunc``/``c_profilefunc`` restore alone changes nothing observable,
because ``sys.gettrace()`` reads ``c_traceobj``, a separate slot restored a few
lines further down.  The remaining three tests here are smoke and
documentation; they survive both mutations and are not load-bearing.

That gap is not theoretical here.  Two bugs found on 2026-09-06 were exactly
this shape -- a saved-state path that existed, was never exercised, and was
wrong where nobody could see.  And ``runloom_sched.h`` carries a live warning
next to the very field this file exercises: a use-after-free in the saved
``exc_info`` chain ("snap->exc_info ... is wild -> AV on the next raise").
"""
import sys

import pytest

sys.path.insert(0, "src")
import runloom_c as rc  # noqa: E402


# ---------------------------------------------------------------------------
# Exception state.  greenlet's test_exc_state, and then the cases runloom has
# that greenlet does not: a real park, and an exception crossing a park.
# ---------------------------------------------------------------------------
def test_in_flight_exception_is_not_visible_to_another_fiber():
    """A fiber inside `except:` must not leak sys.exc_info() to its peers.

    greenlet's test_exc_state, verbatim in intent.  If the snap shared one
    exception stack across fibers, the peer would observe a ValueError it never
    raised -- and, worse, could clear it out from under the handler.
    """
    seen = {}

    def raiser():
        try:
            raise ValueError("fun")
        except ValueError:
            seen["before"] = sys.exc_info()
            rc.sched_yield()                     # peer runs inside our except:
            seen["after"] = sys.exc_info()

    def peer():
        seen["peer"] = sys.exc_info()

    rc.fiber(raiser)
    rc.fiber(peer)
    rc.run()

    assert seen["peer"] == (None, None, None), (
        "a peer fiber saw the raiser's in-flight exception: %r" % (seen["peer"],))
    assert seen["after"] == seen["before"], (
        "sys.exc_info() was not restored across the switch: %r -> %r"
        % (seen["before"], seen["after"]))
    assert seen["before"][0] is ValueError


def test_exception_state_survives_a_real_park():
    """Same contract across a netpoll park, not just a cooperative yield.

    sched_yield and a parked wait_fd take different routes back into the
    scheduler; the exception state has to survive both.  This is the one the
    signal machinery leans on -- delivery restores an exception into a fiber
    that is resuming from exactly this park.
    """
    seen = {}

    def raiser():
        try:
            raise KeyError("parked")
        except KeyError:
            before = sys.exc_info()
            rc.sched_sleep(0.02)                 # a real park, not a yield
            seen["restored"] = sys.exc_info() == before
            seen["type"] = sys.exc_info()[0]

    def peer():
        rc.sched_sleep(0.005)
        seen["peer"] = sys.exc_info()

    rc.fiber(raiser)
    rc.fiber(peer)
    rc.run()

    assert seen["peer"] == (None, None, None)
    assert seen["restored"] is True, "exc_info lost across a park"
    assert seen["type"] is KeyError


def test_nested_handlers_across_switches_keep_their_own_context():
    """Two fibers each inside their own except: see only their own exception."""
    seen = {}

    def one():
        try:
            raise ValueError("one")
        except ValueError:
            rc.sched_yield()
            seen["one"] = sys.exc_info()[1].args[0]

    def two():
        try:
            raise TypeError("two")
        except TypeError:
            rc.sched_yield()
            seen["two"] = sys.exc_info()[1].args[0]

    rc.fiber(one)
    rc.fiber(two)
    rc.run()
    assert seen == {"one": "one", "two": "two"}


# ---------------------------------------------------------------------------
# Trace / profile hooks.  The snap saves AND restores c_tracefunc,
# c_profilefunc and tracing (runloom_sched_pystate.c.inc:139-143, 377-379), so
# the rule these tests pin down is:
#
#     a fiber INHERITS whatever trace state is current at its FIRST run,
#     and from then on saves and restores its own across switches.
#
# That is deliberate enough to be useful (a debugger's tracer reaches into
# fibers) but it has a consequence worth having written down: WHICH tracer a
# fiber inherits depends on which fiber ran before it, and under M:N that is
# not deterministic.  These tests document the behaviour rather than bless it;
# if the rule is ever changed to strict per-fiber isolation, they should fail
# and say so.
# ---------------------------------------------------------------------------
def _mk(name):
    def t(frame, event, arg):
        return t
    t.__name__ = name
    return t


@pytest.fixture(autouse=True)
def _restore_tracing():
    before_t, before_p = sys.gettrace(), sys.getprofile()
    yield
    sys.settrace(before_t)
    sys.setprofile(before_p)


def test_a_fiber_inherits_the_tracer_active_when_it_starts():
    """A debugger's tracer, set before run(), reaches into fibers."""
    T = _mk("T")
    seen = {}
    sys.settrace(T)
    try:
        rc.fiber(lambda: seen.__setitem__("in_fiber", sys.gettrace()))
        rc.run()
    finally:
        sys.settrace(None)
    assert seen["in_fiber"] is T, (
        "a fiber did not inherit the tracer that was active at spawn: %r"
        % (seen["in_fiber"],))


def test_a_fiber_does_not_leak_its_tracer_back_to_the_caller():
    """run() must return with the caller's trace state intact.

    This is the one that would break a debugger outright: if a fiber's tracer
    escaped, every frame after run() would be traced by a function the caller
    never installed, and the caller's own tracer would be gone.
    """
    T = _mk("T")
    outer = _mk("OUTER")
    sys.settrace(outer)
    try:
        rc.fiber(lambda: sys.settrace(T))
        rc.run()
        assert sys.gettrace() is outer, (
            "a fiber's tracer escaped into the caller: %r" % (sys.gettrace(),))
    finally:
        sys.settrace(None)


def test_profile_hook_is_saved_and_restored_too():
    """c_profilefunc rides the same snap slot as c_tracefunc."""
    P = _mk("P")
    outer = _mk("OUTER_P")
    sys.setprofile(outer)
    try:
        rc.fiber(lambda: sys.setprofile(P))
        rc.run()
        assert sys.getprofile() is outer, (
            "a fiber's profile hook escaped into the caller: %r"
            % (sys.getprofile(),))
    finally:
        sys.setprofile(None)


def test_setting_a_tracer_inside_a_fiber_survives_its_own_switches():
    """The setting fiber keeps its tracer across a park; it is snap state."""
    T = _mk("T")
    seen = {}

    def a():
        sys.settrace(T)
        rc.sched_sleep(0.02)
        seen["kept"] = sys.gettrace()
        sys.settrace(None)

    rc.fiber(a)
    rc.fiber(lambda: rc.sched_yield())
    rc.run()
    assert seen["kept"] is T, (
        "the setting fiber lost its own tracer across a park: %r"
        % (seen["kept"],))


def test_tracing_across_switches_does_not_corrupt_frames():
    """An active tracer must see coherent frames as fibers switch under it.

    The hazard is structural rather than cosmetic: the tracer is handed
    frame objects while the scheduler is swapping tstate->current_frame
    underneath, so a bad save/restore surfaces here as a crash or a frame
    from the wrong fiber, not as a wrong return value.
    """
    events = []

    def tracer(frame, event, arg):
        events.append(frame.f_code.co_name)
        return tracer

    def worker(tag):
        for _ in range(3):
            rc.sched_yield()
        return tag

    sys.settrace(tracer)
    try:
        for i in range(4):
            rc.fiber(lambda i=i: worker(i))
        rc.run()
    finally:
        sys.settrace(None)
    # The assertion that matters is that we got here at all, with the tracer
    # having run inside the fibers.
    assert any(n == "worker" for n in events), (
        "the tracer never fired inside a fiber; it saw only %r"
        % (sorted(set(events))[:8],))


def test_each_fiber_keeps_its_own_tracer_across_a_switch():
    """Two fibers with DIFFERENT tracers must not inherit each other's.

    This is the test with teeth for the snap's trace slots: A sets TA and
    yields, B sets TB and yields, then A resumes.  A must see TA -- which is
    only true if its snap restored it.  Without the restore A comes back to
    whatever B left installed, so this fails while the weaker "does it leak to
    the caller" checks still pass.  (Verified by mutation: disabling
    c_tracefunc/c_profilefunc restore in runloom_sched_pystate.c.inc leaves
    every other test in this file green and kills this one.)
    """
    TA, TB = _mk("TA"), _mk("TB")
    seen = {}

    def a():
        sys.settrace(TA)
        rc.sched_yield()
        seen["a"] = sys.gettrace()
        sys.settrace(None)

    def b():
        sys.settrace(TB)
        rc.sched_yield()
        seen["b"] = sys.gettrace()
        sys.settrace(None)

    rc.fiber(a)
    rc.fiber(b)
    rc.run()
    assert seen["a"] is TA, (
        "fiber A resumed with the wrong tracer (%r) -- its snap did not restore "
        "c_tracefunc" % (getattr(seen["a"], "__name__", seen["a"]),))
    assert seen["b"] is TB, (
        "fiber B resumed with the wrong tracer (%r)"
        % (getattr(seen["b"], "__name__", seen["b"]),))
