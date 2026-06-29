"""big_100 / 447 -- generator gi_frame_state send/throw/close cross-hub race.

The subject is a single Python generator object and its ONE embedded execution
frame.  In CPython 3.12+ a generator (Objects/genobject.c, struct PyGenObject)
embeds the frame inline -- `gi_iframe` is a single _PyInterpreterFrame stored in
the generator's own allocation -- plus a single `gi_frame_state` BYTE that names
where that one frame is: FRAME_CREATED / FRAME_EXECUTING / FRAME_SUSPENDED /
FRAME_COMPLETED / FRAME_CLEARED.  The three entry points all do a CHECK-then-SET
against that one byte and then mutate the SAME frame (frame->stacktop, the
frame's owner field, the data stack) with NO GIL serialization in the
free-threaded build:

  * gen_send_ex2() (drives .send()/next()/__next__): rejects FRAME_EXECUTING with
    "ValueError: generator already executing", sets gi_frame_state =
    FRAME_EXECUTING, resumes the embedded frame, on yield sets FRAME_SUSPENDED.
  * gen_throw_ex() (drives .throw()): also rejects FRAME_EXECUTING, then either
    injects the exception into the suspended frame (a re-entry into gi_iframe) or,
    for an un-started/closing generator, raises it directly.
  * gen_close() (drives .close() and tp_finalize / dealloc): throws
    GeneratorExit into the frame and on return CLEARS it (gen_clear_frame ->
    gi_frame_state = FRAME_CLEARED, frees the frame's locals/stack).

Under M:N a generator object can be parked SUSPENDED on ONE hub's grown-down C
stack (the driver fiber yielded with the generator mid-suspend) while a SIBLING
fiber on ANOTHER hub calls .throw()/.close()/.send() on the SAME generator.
Because each of the three is a check-then-mutate on the single gi_frame_state
byte and the single embedded frame, two entries can interleave:

  * a torn state byte lets a re-entrant resume drive a frame already EXECUTING
    (double-resume of one frame), or makes .send() see EXECUTING when the frame
    is really SUSPENDED and raise a SPURIOUS "generator already executing";
  * a .close() that wins the race CLEARS / frees the embedded frame while the
    driver is resuming it -> use-after-free of gi_iframe -> SIGSEGV;
  * a .throw() that injects GeneratorExit into a frame mid-send tears the data
    stack -> an out-of-universe / non-monotonic yielded value, or a SIGSEGV.

A NOTE ON WHAT IS LEGITIMATE TO ATTACK.  Generators are documented as NOT
concurrently reentrant: two threads literally inside one generator frame at the
SAME instant (a driver in next() running simultaneously with a sibling's
throw()/close()) is undefined in ANY free-threaded Python -- verified here with
plain threading.Thread, GIL-off, no runloom: two threads in one frame -> SIGSEGV;
the same code GIL-on -> clean.  That is caller misuse / an upstream-CPython
free-threaded limitation, not a runloom invariant, so this program does NOT do
that.  Instead the driver PARKS strictly OUT of next() while the sibling fires
its single op, so the sibling strikes a generator that is genuinely SUSPENDED on
the driver's grown-down C stack, on another hub.  The race that remains is the
real one: the sibling's entry into the single embedded gi_iframe and its write of
the single gi_frame_state byte (FRAME_SUSPENDED -> EXECUTING/CLEARED, plus
frame->owner / frame->stacktop) racing the M:N machinery that owns that suspended
frame across the park.  The serialized strike is provably clean on plain
GIL-off threads, so any failure HERE is a runloom M:N frame-state defect.

CLOSED-WORLD UNIT-CONSERVATION ORACLE (one generator per round).  The generator
yields a fixed sentinel sequence -- UNIVERSE values v_i = BASE + i for
i in 0..SEQ_LEN -- inside a try/finally that bumps a per-slot finalized[] tally
EXACTLY ONCE.  A DRIVER fiber owns the generator and is the ONLY caller of
send()/next(); it pulls a couple of values, then PARKS: it releases the sibling
via a `go` WaitGroup and waits on a `strike_done` WaitGroup WITHOUT touching the
generator, so it is provably not re-entering the frame during the strike.  A
SIBLING fiber, released by `go`, fires exactly ONCE during that park: it calls
throw(Marker) or close() (round-robined by worker id) on the SAME generator from
another hub, then signals strike_done; only THEN does the driver resume.
Falsifiable invariants:

  (a) every value the driver ever pulls is in UNIVERSE and strictly INCREASING
      (no torn slot, no replayed/duplicated value, no out-of-universe garbage);
  (b) the try/finally runs EXACTLY ONCE per generator created -- sum(finalized)
      == generators created at the end.  A double-resume that re-enters and
      re-runs the finally OVER-counts; a lost finalize (a frame cleared without
      its GeneratorExit reaching the finally) UNDER-counts.  Either breaks
      conservation;
  (c) the ONLY tolerated exceptions are ValueError("generator already
      executing") (the legal detection by either side that the M:N frame-state
      showed EXECUTING) and the injected Marker (the legal .throw payload
      escaping).  ANY other exception type, an out-of-universe value, a
      non-monotonic value, or a SIGSEGV is the bug.

CONTROL ARM (case CONTROL, no sibling).  An IDENTICAL generator is driven
send/throw/close by ONE owner fiber with NO concurrent sibling -- a race-free
single-owner sequence.  It MUST finalize exactly once and, until the explicit
throw/close, yield the exact monotonic UNIVERSE prefix.  If the CONTROL ever
loses or doubles a finalize, or sees an out-of-universe / non-monotonic value,
the fault is in the generator machinery ITSELF (gi_frame_state / gen_clear_frame
mishandling under FT), NOT cross-hub contention -- this disambiguates "generator
state machine is broken" from "M:N raced it".

Coverage is round-robined by worker id in the first ops (sel = (wid + i) %
NCASES) -- never flaky random, which reliably misses a case at low op-count
under the timeout (the p125/p126/p172 flaky-coverage bug).  post() asserts every
case ran and finalize-conservation holds globally.

Stresses: PyGenObject gi_frame_state FRAME_EXECUTING/SUSPENDED/CLEARED
check-then-set, the single embedded gi_iframe resume vs GeneratorExit-inject vs
gen_clear_frame, send-vs-throw-vs-close on ONE generator across hubs, torn state
byte ("generator already executing" spurious/lost), use-after-free of a cleared
frame, finally exactly-once conservation, monotonic finite-universe value
publication.

Good TSan / controlled-M:N-replay target: the concurrent read/write of the one
gi_frame_state byte (and of frame->stacktop / frame->owner) by gen_send_ex2 vs
gen_close on the same PyGenObject is a textbook data race; a TSan report on
gi_frame_state, or a single doubled/lost finalize under replay, localizes the
corruption before the conservation sum even closes.
"""
import harness
import runloom

# Finite sentinel UNIVERSE of yielded values: v_i = BASE + i.  A value the driver
# pulls that is NOT BASE + (its expected index) is a torn/replayed/garbage slot --
# a hard fault.  SEQ_LEN is long enough that the suspended frame's data stack and
# the per-generator local `i` survive several park/resume cycles across hubs, and
# that there is real room between the driver's pulls and the sibling's strike.
BASE = 0x44700000
SEQ_LEN = 24
UNIVERSE = tuple(BASE + i for i in range(SEQ_LEN))
UNIVERSE_SET = frozenset(UNIVERSE)

# The sibling injects this exact exception via .throw().  It is the ONLY non-
# ValueError exception the driver is allowed to observe escaping the generator
# (a GeneratorExit from .close() is swallowed by close() itself and never
# escapes to the driver).  A distinct subclass so a coincidental builtin can
# never masquerade as the injected payload.
class Marker(Exception):
    pass


# Cases, round-robined by worker id (NEVER flaky random -- a timeout-bound run
# completes only a handful of ops and pure random selection reliably misses a
# case, the coverage bug the suite already had to fix in p125/p126/p172).
CASE_THROW = 0      # sibling fires gen.throw(Marker()) during the driver's park
CASE_CLOSE = 1      # sibling fires gen.close() during the driver's park
CASE_CONTROL = 2    # single owner, NO sibling: race-free send/throw/close oracle
NCASES = 3

SLOTS = 1024

# How many values the driver pulls BEFORE tripping the gate + parking, so the
# generator is provably SUSPENDED (gi_frame_state == FRAME_SUSPENDED, gi_iframe
# live) when the sibling strikes.  >=1 guarantees the embedded frame has actually
# started and suspended (FRAME_CREATED would take a different gen_throw path).
PULLS_BEFORE_PARK = 2


def make_gen(slot, finalized):
    """A generator that yields the exact monotonic UNIVERSE sequence inside a
    try/finally.  The finally bumps THIS round's per-slot finalized tally exactly
    once -- whether the generator is exhausted, thrown into, or closed.  It does
    NOT call any scheduler primitive inside the frame: a GeneratorExit thrown by
    a racing .close()/dealloc must unwind straight to the finally without
    re-entering the M:N scheduler from a dealloc path (the p147 invariant)."""
    fired = [False]

    def gen():
        try:
            for i in range(SEQ_LEN):
                yield BASE + i          # suspend point inside the try
        finally:
            # EXACTLY ONCE per generator created.  A double-resume that re-enters
            # the frame and re-runs this finally would bump twice (over-count); a
            # frame cleared without the GeneratorExit reaching here under-counts.
            if not fired[0]:
                fired[0] = True
            finalized[slot] += 1        # own slot -> single-writer, race-free

    return gen()


def drive_pulls(H, g, want):
    """Pull `want` values from generator g via next(), checking each is the exact
    next UNIVERSE value (monotonic, in-universe).  Returns the index reached, or
    -1 on an invariant fault (H.fail already recorded), or a 2-tuple
    ('stop', idx) if the generator exhausted early.  The driver is the ONLY
    caller of next() on g."""
    idx = 0
    while idx < want:
        try:
            v = next(g)
        except StopIteration:
            return ("stop", idx)
        if v not in UNIVERSE_SET:
            H.fail("driver pulled OUT-OF-UNIVERSE value {0!r} at index {1} -- a "
                   "torn generator data-stack slot under concurrent "
                   "send/throw/close on gi_iframe".format(v, idx))
            return -1
        if v != BASE + idx:
            H.fail("driver pulled NON-MONOTONIC value {0!r} at index {1} "
                   "(expected {2!r}) -- a replayed/double-resumed frame or torn "
                   "gi_frame_state let the same frame yield out of order".format(
                       v, idx, BASE + idx))
            return -1
        idx += 1
    return idx


def run_raced(H, wid, rng, state, slot, do_close):
    """One CROSS-HUB round: the driver owns g, pulls PULLS_BEFORE_PARK values,
    then PARKS -- it releases the sibling (go gate) and waits on strike_done WHILE
    STAYING OUT of next() -- so the sibling fires its single throw(Marker)/close()
    into a generator that is genuinely SUSPENDED on the driver's grown-down C
    stack, on ANOTHER hub, with the driver provably NOT re-entering the frame.

    This is the engineered M:N hazard: the sibling's entry into the single
    gi_iframe / its write of the single gi_frame_state byte races the driver's
    SUSPENDED frame across the park (frame->owner, frame->stacktop, the
    FRAME_SUSPENDED->{EXECUTING for throw, CLEARED for close} transition) on a
    different OS thread.  It does NOT manufacture two threads literally executing
    one frame at the SAME instant -- generators are documented as not concurrently
    reentrant, so a driver next() running simultaneously with the sibling's op is
    undefined in ANY free-threaded Python (verified: plain threading.Thread,
    GIL-off, two threads in one frame -> SIGSEGV; the serialized strike here ->
    clean).  We attack the real torn-state-across-a-park bug, not that.

    Conservation: the generator is CREATED here; its try/finally must run exactly
    once.  After the sibling's strike (throw -> finally ran, Marker propagated;
    close -> finally ran, frame CLEARED), the driver resumes: it must see either a
    finished generator (StopIteration), the injected Marker, or -- if the sibling
    raced into FRAME_EXECUTING -- a legal 'generator already executing'
    ValueError; never an out-of-universe / non-monotonic value, never a stray
    exception, never a UAF SIGSEGV."""
    created = state["created"]
    finalized = state["finalized"]
    raced = state["raced"]

    g = make_gen(slot, finalized)
    created[slot] += 1                  # this generator now owes exactly one finalize

    # Pre-park pulls: the generator must yield the exact monotonic prefix and end
    # SUSPENDED with gi_iframe live.
    idx = drive_pulls(H, g, PULLS_BEFORE_PARK)
    if idx == -1:
        return False
    if isinstance(idx, tuple):
        # Exhausted before we even parked (SEQ_LEN > PULLS_BEFORE_PARK, so this
        # should never happen on a clean machine; treat as a fault).
        H.fail("generator exhausted after only {0} pulls (< PULLS_BEFORE_PARK "
               "{1}) -- the embedded frame ended early".format(idx[1],
                                                               PULLS_BEFORE_PARK))
        return False

    go = runloom.WaitGroup()            # driver releases the sibling to strike
    go.add(1)
    strike_done = runloom.WaitGroup()   # sibling signals its single op finished
    strike_done.add(1)
    wg = runloom.WaitGroup()            # both fibers join here before we read state
    wg.add(2)

    # The sibling's verdict is published into a 1-slot box (single sibling writer,
    # single driver reader after the join -- race-free by the WaitGroup barrier).
    sib_outcome = [None]

    def run_sibling():
        try:
            go.wait()                   # released once the driver has parked
            # Fire the racing op EXACTLY ONCE into the SAME generator from this
            # (other) hub, straight at the suspended gi_iframe.  The driver is
            # parked OUT of next() across this, so the only concurrency is the
            # M:N machinery (frame on the driver's grown-down stack, this op on
            # another hub).  Legal outcomes: a clean land, or -- only if the M:N
            # frame-state genuinely showed EXECUTING -- a 'generator already
            # executing' ValueError.  Any OTHER exception is a state-machine fault.
            try:
                if do_close:
                    g.close()           # GeneratorExit -> finally -> gen_clear_frame
                    sib_outcome[0] = ("close_clean", None)
                else:
                    g.throw(Marker())   # inject Marker into the suspended frame
                    # Our generator does NOT catch Marker, so throw() must
                    # propagate Marker out to HERE; a quiet return would mean the
                    # frame swallowed an exception it never catches.
                    sib_outcome[0] = ("throw_noraise", None)
            except Marker:
                # throw(Marker) propagated back out of the (now-finalized)
                # generator -- the legal, expected outcome for the throw case.
                sib_outcome[0] = ("throw_marker", None)
            except ValueError as ve:
                sib_outcome[0] = ("value_error", str(ve))
            except StopIteration:
                # close()/throw landing exactly as the gen completes can surface
                # StopIteration -- benign.
                sib_outcome[0] = ("stopiter", None)
        except BaseException as exc:    # noqa: BLE001
            sib_outcome[0] = ("other", "{0}: {1}".format(type(exc).__name__, exc))
        finally:
            strike_done.done()          # tell the driver the single op is done
            wg.done()

    def run_driver():
        try:
            # PARK: release the sibling, then wait for it to FINISH its single op
            # before touching g again.  The generator is SUSPENDED on this fiber's
            # grown-down C stack throughout -- the sibling's gi_iframe entry on
            # another hub races a frame whose driver is provably not executing it.
            go.done()
            strike_done.wait()          # the strike landed; only NOW do we resume
            # Resume the SAME generator.  After a throw/close it is finished, so
            # this should yield StopIteration (or surface the injected Marker / a
            # legal 'already executing' ValueError if the M:N state showed
            # EXECUTING).  If it instead keeps yielding, every value must still be
            # in UNIVERSE and strictly monotonic -- a torn data stack or a
            # double-resumed frame is caught here.
            try:
                while True:
                    try:
                        v = next(g)
                    except StopIteration:
                        break           # generator finished (closed/thrown/exhausted)
                    if v not in UNIVERSE_SET:
                        H.fail("driver (post-park) pulled OUT-OF-UNIVERSE value "
                               "{0!r} after index {1} -- torn frame data stack "
                               "from the racing close/throw on gi_iframe".format(
                                   v, cursor[0]))
                        return
                    if v != BASE + cursor[0]:
                        H.fail("driver (post-park) pulled NON-MONOTONIC value "
                               "{0!r} (expected {1!r}) -- a double-resumed / "
                               "torn-state frame".format(v, BASE + cursor[0]))
                        return
                    cursor[0] += 1      # own-to-this-fiber monotonic cursor
            except Marker:
                # The sibling's injected Marker propagated through the frame and
                # out of our own next() -- legal (the throw payload).
                pass
            except ValueError as ve:
                # "generator already executing": the driver's resume collided
                # with the sibling's entry -- the legal torn-byte detection.
                msg = str(ve)
                if "already executing" not in msg:
                    H.fail("driver next() raised unexpected ValueError {0!r} -- "
                           "not the legal 'generator already executing' "
                           "collision".format(msg))
                    return
            except GeneratorExit:
                # A GeneratorExit must NOT escape next() to the driver (close()
                # swallows it inside the generator); if it does, the state machine
                # mis-routed the exit.
                H.fail("driver next() saw GeneratorExit escape -- close()'s "
                       "GeneratorExit leaked out of the frame instead of being "
                       "swallowed by the finally")
                return
            except BaseException as exc:    # noqa: BLE001
                H.fail("driver next() raised UNEXPECTED {0}: {1} -- only "
                       "'generator already executing' ValueError or the injected "
                       "Marker are legal".format(type(exc).__name__, exc))
                return
        finally:
            wg.done()

    # Own-to-the-driver-fiber monotonic cursor (a 1-list so the closure mutates
    # it in place; never shared-written across fibers).  Seeded at the pre-park
    # index so the post-park pulls continue the exact monotonic sequence.
    cursor = [idx]

    H.fiber(run_sibling)
    H.fiber(run_driver)
    wg.wait()                           # both joined -> generator provably quiescent

    if H.failed:
        return False

    # Vet the sibling outcome: only the documented-legal verdicts are allowed.
    oc, info = sib_outcome[0]
    if oc == "other":
        H.fail("sibling {0} op raised UNEXPECTED {1} -- only ValueError "
               "'generator already executing', the injected Marker, or a clean "
               "land are legal".format("close" if do_close else "throw", info))
        return False
    if oc == "value_error" and "already executing" not in (info or ""):
        H.fail("sibling op raised unexpected ValueError {0!r} -- not the legal "
               "'generator already executing' collision".format(info))
        return False

    # Make sure the generator is now COMPLETED/CLEARED: a final next() must raise
    # StopIteration (never resume the frame again, never SIGSEGV).  This also
    # forces finalization for any path that left it merely suspended (e.g. a
    # ValueError-collision that aborted both entries without finishing the frame),
    # so the finally is guaranteed to have run before we count.
    try:
        leftover = next(g)
        # If it yields again it was NOT actually finished -- another in-universe
        # monotonic value is still legal (the race left it resumable); drain it.
        while True:
            if leftover not in UNIVERSE_SET:
                H.fail("final-drain pulled OUT-OF-UNIVERSE value {0!r} -- torn "
                       "frame survived the race".format(leftover))
                return False
            try:
                leftover = next(g)
            except StopIteration:
                break
    except StopIteration:
        pass
    except Marker:
        pass
    except ValueError as ve:
        if "already executing" not in str(ve):
            H.fail("final-drain raised unexpected ValueError {0!r}".format(
                str(ve)))
            return False
    except BaseException as exc:        # noqa: BLE001
        H.fail("final-drain raised UNEXPECTED {0}: {1}".format(
            type(exc).__name__, exc))
        return False

    # Force a real close() so the finally is guaranteed run even if the frame was
    # left suspended by a ValueError collision (close on a finished gen is a no-op
    # and never double-runs the finally; the `fired` guard would also catch a
    # double, but the per-slot tally is the conservation oracle).
    try:
        g.close()
    except BaseException:               # noqa: BLE001
        pass

    raced[slot] += 1
    return True


def run_control(H, wid, rng, state, slot):
    """CONTROL arm: ONE owner fiber drives an IDENTICAL generator send/next then
    throw/close -- NO concurrent sibling.  Race-free by construction: it MUST
    yield the exact monotonic UNIVERSE prefix and finalize EXACTLY ONCE.  If THIS
    loses or doubles a finalize, or sees an out-of-universe / non-monotonic
    value, the bug is in the generator state machine itself, not contention."""
    created = state["created"]
    finalized = state["finalized"]
    controls = state["controls"]

    g = make_gen(slot, finalized)
    created[slot] += 1

    # Pull a deterministic prefix; must be exact and monotonic.
    want = PULLS_BEFORE_PARK + (wid % 4)     # vary the prefix length, still < SEQ_LEN
    idx = drive_pulls(H, g, want)
    if idx == -1:
        return False
    if isinstance(idx, tuple):
        H.fail("control generator exhausted after {0} pulls (< {1}) -- the "
               "embedded frame ended early with no contention".format(
                   idx[1], want))
        return False

    # Single-owner throw OR close (round-robined by wid), both race-free here.
    if (wid & 1) == 0:
        # throw(Marker): our generator does not catch it, so throw MUST propagate
        # Marker back out and run the finally exactly once.
        try:
            g.throw(Marker())
            H.fail("control throw(Marker) did NOT propagate Marker out -- the "
                   "generator swallowed an exception it never catches (state "
                   "machine mis-routed the throw)")
            return False
        except Marker:
            pass
        except StopIteration:
            # Legal only if the generator had already finished; here it had not,
            # so a StopIteration would mean the throw was dropped.
            H.fail("control throw(Marker) raised StopIteration on a still-"
                   "suspended generator -- the injected exception was dropped")
            return False
        except BaseException as exc:    # noqa: BLE001
            H.fail("control throw(Marker) raised UNEXPECTED {0}: {1}".format(
                type(exc).__name__, exc))
            return False
    else:
        # close(): GeneratorExit -> finally -> CLEARED, swallowed (no escape).
        try:
            g.close()
        except BaseException as exc:    # noqa: BLE001
            H.fail("control close() raised {0}: {1} -- close() must swallow "
                   "GeneratorExit and run the finally cleanly".format(
                       type(exc).__name__, exc))
            return False

    # The generator is finished: any further next() MUST be StopIteration, and
    # MUST NOT re-run the finally (the `fired` guard + per-slot tally prove it).
    for _ in range(3):
        try:
            v = next(g)
            H.fail("control next() after throw/close yielded {0!r} -- a finished "
                   "generator resumed its cleared frame (double-resume)".format(v))
            return False
        except StopIteration:
            pass
        except BaseException as exc:    # noqa: BLE001
            H.fail("control post-finish next() raised {0}: {1} (expected "
                   "StopIteration)".format(type(exc).__name__, exc))
            return False

    controls[slot] += 1
    return True


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases by worker id in the first ops so every case
        # is exercised even when each worker manages only a few ops under the
        # timeout; random after that (preserving the concurrent mix).
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_THROW:
            ok = run_raced(H, wid, rng, state, slot, do_close=False)
        elif sel == CASE_CLOSE:
            ok = run_raced(H, wid, rng, state, slot, do_close=True)
        else:
            ok = run_control(H, wid, rng, state, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Per-slot single-writer tallies (summed in post).  Built here in the root
    # where the cooperative M:N primitives are live; the generators themselves are
    # created per round inside the worker.
    H.state = {
        "created": [0] * SLOTS,         # generators created (each owes 1 finalize)
        "finalized": [0] * SLOTS,       # try/finally runs -- conservation oracle
        "raced": [0] * SLOTS,           # contended throw/close rounds completed
        "controls": [0] * SLOTS,        # single-owner control rounds completed
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    created = sum(H.state["created"])
    finalized = sum(H.state["finalized"])
    raced = sum(H.state["raced"])
    controls = sum(H.state["controls"])
    H.log("generators created={0} finalized={1} (try/finally exactly-once) "
          "raced(throw/close)={2} control={3} ops={4}".format(
              created, finalized, raced, controls, H.total_ops()))

    H.check(H.total_ops() > 0,
            "no rounds completed -- the gi_frame_state send/throw/close race "
            "window was never exercised")

    # The conservation law: every generator created ran its try/finally EXACTLY
    # once.  finalized < created == a LOST finalize (a frame cleared without its
    # GeneratorExit reaching the finally); finalized > created == a DOUBLE finalize
    # (a double-resume re-entered the frame and re-ran the finally).  Either is a
    # torn-gi_frame_state corruption.
    H.check(finalized == created,
            "finalize conservation BROKEN: finalized={0} != created={1} -- a "
            "generator's try/finally ran {2} (torn gi_frame_state let a frame be "
            "double-resumed or cleared without its GeneratorExit reaching the "
            "finally)".format(finalized, created,
                              "TWICE (double-resume)" if finalized > created
                              else "ZERO times for some gen (lost finalize)"))

    # Both arms actually ran (so the conservation law was not vacuous, and the
    # control falsifier was actually evaluated).
    H.check(raced > 0,
            "the contended throw/close race arm never completed a round -- the "
            "cross-hub send-vs-throw-vs-close window was never exercised")
    H.check(controls > 0,
            "the single-owner CONTROL arm never ran -- the generator-machinery "
            "falsifier was never evaluated")

    H.require_no_lost("gen-state send/throw/close conservation")


if __name__ == "__main__":
    harness.main(
        "p447_gen_state_send_throw_close_rac", body, setup=setup, post=post,
        default_funcs=3000,
        describe="one generator's single gi_frame_state byte + embedded gi_iframe "
                 "raced by a driver's send() against a sibling's throw()/close() "
                 "during the driver's park; closed-world conservation: every value "
                 "in a monotonic finite universe, try/finally runs exactly once "
                 "(finalized==created), only 'generator already executing' / the "
                 "injected Marker tolerated -- a double-resume, lost finalize, "
                 "torn value, or SIGSEGV fails; single-owner control arm falsifies "
                 "machinery-vs-contention")
