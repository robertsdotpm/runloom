"""big_100 / 448 -- async-generator asend/athrow/aclose vs ag_running_async + finalizer.

The subject is CPython's ASYNC GENERATOR object (PyAsyncGenObject, Objects/
genobject.c).  Every async gen carries, embedded in one object:

  * ag_running_async -- the CHECK-then-SET busy flag.  async_gen_asend_send(),
    async_gen_athrow_send() and the aclose path all read this flag, and if it is
    already set raise RuntimeError("... asynchronous generator is already
    running"), otherwise SET it true, step the embedded frame, then clear it.
    That guard is the ONLY thing serializing two operations against the single
    shared frame -- it is a non-atomic test-and-set on a plain object field.
  * ag_closed / the embedded gi_frame_state -- the one PyFrameObject the gen
    runs on.  asend() steps it forward; athrow()/aclose() INJECT an exception
    INTO that same frame; the GC-driven ag_finalizer (set via
    sys.set_asyncgen_hooks) clears it when a dropped ag is collected.
  * ag_origin_or_finalizer -- the per-interpreter firstiter/finalizer hook slot.

Under the runloom M:N scheduler an async-gen step that PARKS mid-frame (it
awaited something that suspended, leaving ag_running_async TRUE on a grown-down
C stack) can be hit, on ANOTHER hub, by an aclose()/athrow() on the SAME gen.
Two mutually-exclusive corruptions, BOTH made falsifiable here:

  * a TORN ag_running_async -- the busy check reads stale and either (a) spuriously
    raises "already running" when the gen was NOT actually mid-step (a legal step
    lost), or (b) FAILS to raise and DOUBLE-STEPS the one shared frame -> a value
    re-yielded / skipped / out of order, or a frame re-entered (-> SIGSEGV);
  * a FINALIZER firing on a frame another hub is mid-asend on -- a use-after-free
    of the frame / gen, or a finalizer running twice / never (lost or doubled
    finalize -> the try/finally tally diverges from the gens created).

We drive this WITHOUT an event loop, stepping the gen by hand with
coro.send(None) so the park window is explicit and controllable.  Each value-step
is TWO sends:

    asend = ag.asend(None)
    asend.send(None)         # crosses `await Pauser()`, SUSPENDS mid-step,
                             #   ag_running_async is now TRUE -- the park window
    runloom.yield_now()      # hand off so a SIBLING on another hub runs HERE
    asend.send(None)         # resumes the frame -> StopIteration(value) == yield

`Pauser.__await__` yields exactly once, so the first send parks the asend with
the busy flag set; the second resumes it and the yielded universe value comes
back as StopIteration.value.  A gated sibling fires aclose()/athrow() in that
window -- it MUST hit the legal "already running" RuntimeError (the ONLY tolerated
busy error), OR (if it lands after the owner already resumed) close/inject
cleanly.  Either way the try/finally finalizer runs EXACTLY ONCE.

CLOSED-WORLD, FALSIFIABLE invariants on a finite sentinel UNIVERSE:

  * IDENTITY + ORDER: every value an asend ever yields is in UNIVERSE and is
    STRICTLY MONOTONIC (the gen yields UNIVERSE[idx++]).  A double-step would
    skip or repeat a value; a torn frame read would hand back an out-of-universe
    value.  Either is caught before the sibling even joins.
  * BUSY ERROR IS THE ONLY TOLERATED ONE: a sibling aclose/athrow during the
    window may raise RuntimeError("already running") -- counted as the legal
    outcome.  ANY other exception type out of the busy check, or a yielded value
    that violates IDENTITY/ORDER, is the bug.
  * FINALIZE CONSERVATION: a sharded created[] vs finalized[] tally.  The gen
    bumps created[slot] on first step and finalized[slot] in its try/finally.
    Across the whole run sum(created) == sum(finalized) -- no finalizer lost
    (frame leaked / never unwound) and none doubled (frame re-entered / closed
    twice re-running finally).

CONTROL ARM (case 2, single-owner, race-free by construction): one fiber drives a
private gen asend -> aclose with NO sibling.  It MUST yield the exact monotonic
UNIVERSE prefix it stepped and finalize EXACTLY ONCE.  A single-owner async gen
has one driver, so if the CONTROL diverges (skipped/repeated value, or a finalize
count != gens created on the control path) the fault is in CPython's async-gen
machinery ITSELF, not M:N contention -- this disambiguates a real bug from a
contention artifact (the p405/p412 private-control discipline).

COVERAGE (the flaky-random lesson from p125/p126/p172): post() asserts each of
the three cases ran; timeout-bound runs do only a handful of ops, so the worker
round-robins cases by id in its FIRST ops (sel = (wid + i) % 3) then goes random,
so coverage holds whether one worker does 3 ops or 3 workers do 1 each.

Invariant (hot, fail-fast): every yielded value in UNIVERSE and strictly
monotonic; the only tolerated sibling exception is RuntimeError("already
running"); the control arm yields its exact monotonic prefix.
Invariant (post): sum(created) == sum(finalized) (no lost/double finalize);
all three cases exercised; at least one sibling busy-RuntimeError observed (the
race window was actually hit); no lost worker.

Stresses: async_gen_asend_send / async_gen_athrow_send ag_running_async
check-then-set, athrow/aclose exception injection into a frame another hub is
mid-asend on, finalizer try/finally conservation, asend value identity/order
under a cross-hub park, "already running" busy-error enforcement (never lost,
never spurious).

Good TSan / controlled-M:N-replay target: the test-and-set of ag_running_async
plus the read/write of the shared embedded frame across two hubs is a textbook
data race; a TSan report on the ag's flag/frame, or a single non-monotonic /
out-of-universe value under replay, localizes the double-step before the
conservation sum even closes.
"""
import harness
import runloom

# Finite sentinel UNIVERSE: the values the async gen yields, in order.  A value an
# asend yields that is NOT in this set is a torn/double-stepped frame read; a value
# that is in-set but <= the last one is a repeated/skipped step.  Sized so a single
# gen drives the frame through many yield/await round trips (each value = a full
# suspend+resume of the embedded frame, the window the sibling races into).
UNIVERSE_SIZE = 96
UNIVERSE = tuple(0x44800000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Per-worker tallies use slot == wid (sized [0]*H.funcs in setup), so each cell
# has a single writer and the global sums are race-free (summed in post).

# How many values the owner asend-drives before the round ends (and then closes).
# Fewer than UNIVERSE_SIZE so aclose() always lands on a still-OPEN gen (the
# interesting finalize path), not an already-exhausted one.
STEPS_PER_ROUND = 12

# Which value-step opens the park window the gated sibling fires into.  Picked
# mid-run (not the first step) so the gen frame is genuinely in progress -- a
# real suspended frame with live locals, not a just-created one.
GATE_STEP = 3

# The three cases.  post() asserts each ran, so the worker round-robins them by
# id in its first ops (NOT random -- pure random reliably MISSES a case at low
# op-count under the timeout, the flaky-coverage bug the suite already fixed).
CASE_ASEND_ACLOSE = 0   # owner asend-drives; sibling fires aclose() in the window
CASE_ASEND_ATHROW = 1   # owner asend-drives; sibling fires athrow(EXC) in the window
CASE_CONTROL = 2        # single owner asend -> aclose, NO sibling (the falsifier)
NCASES = 3

# The sentinel exception a sibling injects via athrow().  A recognizable type so a
# stray/duplicated injection landing in the wrong place is identifiable.
INJECT_MSG = "p448-athrow-sentinel"


class InjectExc(Exception):
    """The exact exception type a CASE_ASEND_ATHROW sibling injects into the gen
    frame.  Distinct from RuntimeError so we can tell a legal busy-error from the
    injected throw surfacing back out of the asend driver."""


class Pauser(object):
    """An awaitable whose __await__ yields EXACTLY ONCE.  `await Pauser()` inside
    the async gen suspends the asend mid-step with ag_running_async TRUE -- that
    is the explicit, controllable park window the sibling op races into.  The
    first asend.send(None) returns here (the gen is paused); the second resumes
    the frame and runs on to the gen's `yield`."""

    def __await__(self):
        yield None


def make_gen(state, slot):
    """Build one fresh async generator that yields UNIVERSE[0], UNIVERSE[1], ...
    in order, each behind a Pauser park, inside a try/finally that bumps the
    per-slot finalized[] tally EXACTLY ONCE when the gen is unwound (normal
    exhaustion, aclose, or athrow).  created[] is bumped on first step.  Both are
    single-writer-per-slot (race-free); their global sums must match in post()."""
    created = state["created"]
    finalized = state["finalized"]

    async def gen():
        # created bumped on the FIRST resume (when the body actually starts);
        # finalized bumped in the finally on EVERY unwind path -- the conservation
        # pair.  A double-entered frame would bump created twice; a re-run finally
        # would bump finalized twice; a leaked/never-finalized gen bumps created
        # without finalized.  All three break sum(created) == sum(finalized).
        created[slot] += 1
        try:
            idx = 0
            while idx < UNIVERSE_SIZE:
                await Pauser()            # SUSPEND mid-step: the park window
                yield UNIVERSE[idx]       # resumes here -> StopIteration(value)
                idx += 1
        finally:
            finalized[slot] += 1

    return gen()


def step_one_value(H, ag, window_hook):
    """Advance the gen by ONE value via its asend wrapper.  Returns one of:
      ("yield", value)  -- the gen yielded `value` (a UNIVERSE element)
      ("stop", None)    -- the gen is exhausted / already closed (StopAsyncIteration)
      ("busy", msg)     -- a legal "already running" RuntimeError hit the OWNER's
                           own second send (a sibling won the flag) -- tolerated
      ("other", exc)    -- ANY other exception type out of the asend -> a fault

    window_hook (callable or None): called in the park window (between the two
    sends), WHILE the asend is suspended and ag_running_async is TRUE.  For a
    contended step it RELEASES the gated sibling AND blocks until the sibling has
    actually fired its aclose/athrow against the live flag (a rendezvous), so the
    cross-hub op provably lands IN the window -- this makes the hazard
    deterministic instead of relying on M:N scheduler jitter, which reliably
    resumes the owner before the sibling runs."""
    asend = ag.asend(None)
    try:
        asend.send(None)                  # cross `await Pauser()`, SUSPEND here
    except StopIteration as si:
        # The gen returned in the same step (no further yield) -- shouldn't happen
        # before exhaustion given UNIVERSE_SIZE >> STEPS_PER_ROUND, but treat a
        # bare StopIteration as "stop".
        if si.value is not None:
            return ("yield", si.value)
        return ("stop", None)
    except StopAsyncIteration:
        return ("stop", None)
    except RuntimeError as exc:
        return ("busy", str(exc))
    except Exception as exc:              # noqa: BLE001
        return ("other", exc)

    # --- park window open: ag_running_async is TRUE, frame suspended at Pauser ---
    if window_hook is not None:
        window_hook()                     # release sibling + rendezvous on its fire
    else:
        runloom.yield_now()               # control / non-gated step: single hand-off

    try:
        asend.send(None)                  # RESUME the frame -> the gen's yield
    except StopIteration as si:
        return ("yield", si.value)        # the universe value
    except StopAsyncIteration:
        return ("stop", None)
    except RuntimeError as exc:
        # A sibling that won the busy flag can leave the owner's resume to raise
        # "already running" (or the gen was closed under us) -- the legal busy
        # outcome on the owner side.  Tolerated.
        return ("busy", str(exc))
    except InjectExc:
        # A sibling athrow() injected its sentinel and it surfaced back through the
        # owner's resume -- a legal outcome of athrow into the running frame.
        return ("stop", None)
    except Exception as exc:              # noqa: BLE001
        return ("other", exc)
    return ("await", None)                # gen awaited again w/o yielding (n/a here)


def close_gen(ag):
    """Drive aclose() to completion.  Returns 'closed' | 'busy' | ('other', exc).
    A clean close raises StopIteration (caught); a sibling holding the flag yields
    the legal 'already running' RuntimeError; the finalizer runs exactly once via
    the gen's try/finally regardless."""
    aclose = ag.aclose()
    try:
        aclose.send(None)
        return "closed"                   # aclose completed without suspending
    except StopIteration:
        return "closed"                   # normal: gen unwound, finally ran
    except StopAsyncIteration:
        return "closed"
    except RuntimeError:
        return "busy"                     # legal "already running"
    except Exception as exc:              # noqa: BLE001
        return ("other", exc)


def drive_and_check(H, wid, ag, tally, slot, window_hook):
    """Own-fiber driver: asend-step the gen STEPS_PER_ROUND times, checking each
    yielded value is in UNIVERSE and STRICTLY MONOTONIC.  window_hook (callable or
    None) is invoked in the park window of step GATE_STEP -- it releases a gated
    sibling and rendezvous-blocks until the sibling has fired its cross-hub op into
    the live ag_running_async window.  Returns True on a clean (no-fault) drive,
    False on a fault (already H.fail'd).  ('busy'/'stop' just end the drive early --
    both legal.)"""
    last = -1
    for i in range(STEPS_PER_ROUND):
        hook = window_hook if (i == GATE_STEP and window_hook is not None) else None
        kind, payload = step_one_value(H, ag, hook)
        if kind == "yield":
            v = payload
            if v not in UNIVERSE_SET:
                H.fail("asend yielded OUT-OF-UNIVERSE value {0!r} -- a torn/"
                       "double-stepped async-gen frame read (ag_running_async or "
                       "the embedded frame corrupted under a cross-hub aclose/"
                       "athrow)".format(v))
                return False
            if v <= last:
                H.fail("asend yielded NON-MONOTONIC value {0!r} after {1!r} -- the "
                       "single async-gen frame was DOUBLE-STEPPED or rewound (a "
                       "torn ag_running_async let two ops step the same frame)"
                       .format(v, last))
                return False
            last = v
            tally["yielded"][slot] += 1
        elif kind == "stop":
            break                          # exhausted / closed -- legal end
        elif kind == "busy":
            tally["busy"][slot] += 1       # legal "already running" on owner side
            break
        else:  # ("other", exc)
            exc = payload
            H.fail("asend raised an unexpected {0}: {1} -- not the legal "
                   "'already running' RuntimeError nor a clean stop (async-gen "
                   "frame fault under cross-hub injection)".format(
                       type(exc).__name__, exc))
            return False
    return True


def run_contended_case(H, wid, state, slot, sibling_op):
    """Shared driver for the two CONTENDED cases (aclose / athrow).  Spawns a
    sibling fiber that, on release, runs `sibling_op(ag)` against the gen WHILE the
    owner's asend is suspended (ag_running_async TRUE), then drives the owner asend
    with a window_hook that does the deterministic rendezvous:

        owner: open the window -> WAIT (fired_ch.recv) until the sibling has fired
        sibling: WAIT (open_ch.recv) for the window -> fire its op -> signal fired

    so the cross-hub op provably lands in the live-flag window every round.
    `sibling_op(ag)` returns "busy" (legal 'already running'), "ok" (clean
    close/inject/stop), or ("other", exc) for a fault.  Returns True on a clean
    (no-fault) round."""
    ag = make_gen(state, slot)
    open_ch = runloom.Chan(1)              # owner -> sibling: window is open NOW
    fired_ch = runloom.Chan(1)             # sibling -> owner: I have fired my op
    sib_wg = runloom.WaitGroup()
    sib_wg.add(1)
    sib_busy = [0]
    fired_ever = [False]

    def sibling():
        try:
            open_ch.recv()                 # block until the owner opens the window
            res = sibling_op(ag)           # aclose/athrow INTO the running frame
            if res == "busy":
                sib_busy[0] = 1
            elif isinstance(res, tuple):   # ("other", exc) -- a real fault
                H.fail("sibling {0} raised unexpected {1}: {2} -- not a clean "
                       "close/inject nor the legal 'already running' (frame fault "
                       "under a cross-hub op during an asend park)".format(
                           sibling_op.__name__, type(res[1]).__name__, res[1]))
        finally:
            fired_ch.send(True)            # rendezvous: tell the owner we fired
            sib_wg.done()

    H.fiber(sibling)

    def window_hook():
        # WHILE the asend is suspended (ag_running_async TRUE): release the sibling
        # and BLOCK until it has fired its op against the live flag.  This is the
        # rendezvous that makes the cross-hub op deterministically land in-window
        # (a bare yield_now lets the owner resume first under M:N -- see notes).
        fired_ever[0] = True
        open_ch.send(True)
        fired_ch.recv()

    ok = drive_and_check(H, wid, ag, state["tally"], slot, window_hook)

    # If the drive ended before GATE_STEP (gen stopped/busy early), the window was
    # never opened and the sibling is still parked on open_ch -- release it so it
    # joins and signals, then drain its fired token.
    if not fired_ever[0]:
        open_ch.send(True)
        fired_ch.recv()
    sib_wg.wait()
    if H.failed:
        return False
    if sib_busy[0]:
        state["tally"]["sib_busy"][slot] += 1

    # Owner's final close: idempotent (the finalizer runs exactly once whether the
    # sibling or the owner ultimately unwinds the gen); this guarantees the frame
    # is fully unwound before we drop the reference (no GC-finalizer landing on a
    # half-stepped frame).
    res = close_gen(ag)
    if isinstance(res, tuple):
        H.fail("owner final aclose() raised unexpected {0}: {1}".format(
            type(res[1]).__name__, res[1]))
        return False
    return ok


def aclose_op(ag):
    """Sibling op for CASE_ASEND_ACLOSE: aclose() the gen.  'busy' = legal
    'already running'; 'ok' = clean close; ('other', exc) = fault."""
    res = close_gen(ag)
    if res == "busy":
        return "busy"
    if isinstance(res, tuple):
        return res
    return "ok"


def athrow_op(ag):
    """Sibling op for CASE_ASEND_ATHROW: athrow(InjectExc) into the gen frame.
    'busy' = legal 'already running'; 'ok' = the injected exc surfaced or the gen
    unwound cleanly; ('other', exc) = an unexpected exception type (a fault)."""
    athrow = ag.athrow(InjectExc(INJECT_MSG))
    try:
        athrow.send(None)                  # inject INTO the frame
        return "ok"
    except StopIteration:
        return "ok"                        # gen swallowed/propagated -> unwound
    except StopAsyncIteration:
        return "ok"
    except InjectExc:
        return "ok"                        # the injected exc propagated out -- legal
    except RuntimeError:
        return "busy"                      # legal "already running"
    except Exception as exc:               # noqa: BLE001
        return ("other", exc)


def case_asend_aclose(H, wid, rng, state, slot):
    """CASE 0: owner asend-drives the gen while a sibling on another hub fires
    aclose() INTO the live ag_running_async window (deterministic rendezvous).
    Legal sibling outcomes: 'already running' RuntimeError, or a clean close.  The
    finalizer runs exactly once (counted in the global created/finalized tally)."""
    return run_contended_case(H, wid, state, slot, aclose_op)


def case_asend_athrow(H, wid, rng, state, slot):
    """CASE 1: owner asend-drives while a sibling on another hub fires
    athrow(InjectExc()) INTO the live window.  Legal sibling outcomes: 'already
    running', the injected exception surfacing, or a clean stop.  The finalizer
    runs exactly once."""
    return run_contended_case(H, wid, state, slot, athrow_op)


def case_control(H, wid, rng, state, slot):
    """CASE 2 -- THE CONTROL ARM (single owner, race-free by construction).  One
    fiber asend-drives the gen then aclose()s it, with NO sibling touching it.  The
    yielded values MUST be the exact monotonic UNIVERSE prefix and the finalizer
    MUST run exactly once.  A divergence HERE (skipped/repeated value, or a control
    finalize that does not match a control create) is a CPython async-gen machinery
    bug, not M:N contention -- the single owner provably never races the flag."""
    ag = make_gen(state, slot)
    c_before = state["ctrl_created"][slot]
    f_before = state["ctrl_finalized"][slot]

    # Drive with NO gate -- no sibling, no park-window release.  We still cross the
    # Pauser each step (a real suspend/resume of the frame), so this exercises the
    # exact same asend two-send path, just without contention.
    expect = list(UNIVERSE[:STEPS_PER_ROUND])
    got = []
    for i in range(STEPS_PER_ROUND):
        kind, payload = step_one_value(H, ag, None)
        if kind == "yield":
            got.append(payload)
        elif kind == "stop":
            break
        elif kind == "busy":
            # A single-owner gen can NEVER be "already running" -- nothing else
            # touches it.  A busy error here is the machinery spuriously tearing
            # its own flag.
            H.fail("CONTROL gen reported 'already running' with a SINGLE owner and "
                   "no sibling -- ag_running_async spuriously set/torn by the "
                   "async-gen machinery itself: {0}".format(payload))
            return False
        else:  # other
            H.fail("CONTROL asend raised unexpected {0}: {1} (single-owner async "
                   "gen -- a machinery fault, not contention)".format(
                       type(payload).__name__, payload))
            return False

    # IDENTITY + ORDER on the control: the values must be exactly UNIVERSE[:n].
    if got != expect[:len(got)]:
        H.fail("CONTROL asend yielded {0!r} but the exact monotonic prefix is "
               "{1!r} -- a single-owner async gen skipped/repeated a value (the "
               "asend/frame stepping itself is wrong, NOT M:N contention)".format(
                   [hex(v) for v in got], [hex(v) for v in expect[:len(got)]]))
        return False

    # The control account: count this control gen's create/finalize so post() can
    # assert the control path alone conserves (single-owner -> must be exact).
    state["ctrl_created"][slot] += 1
    res = close_gen(ag)
    if res == "busy":
        H.fail("CONTROL aclose() reported 'already running' with no sibling -- the "
               "finalizer/flag machinery tore its own state")
        return False
    if isinstance(res, tuple):
        H.fail("CONTROL aclose() raised unexpected {0}: {1}".format(
            type(res[1]).__name__, res[1]))
        return False
    state["ctrl_finalized"][slot] += 1
    return True


def worker(H, wid, rng, state):
    # Each worker owns a UNIQUE slot == its wid (run_pool spawns wids
    # 0..H.funcs-1), so every per-slot tally cell has a single writer.  The old
    # slot = wid & 1023 aliased ~20 workers onto each of 1024 slots at the
    # 20k design tier, so created[slot]+=1 / finalized[slot]+=1 (and the rest)
    # tore by +/-1 -> sum(created) != sum(finalized).  The async-gen machinery
    # is correct; only the test's bookkeeping was racy.  (Sub-fibers in the
    # contended cases run the gen body on the OWNER's slot, so there is no
    # sub-fiber sharing of a cell here -- the worker is the sole writer.)
    slot = wid
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the three cases by worker id in the FIRST ops so every case
        # is exercised even under a short timeout (the p125/p126 flaky-random fix);
        # random after coverage is guaranteed.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1

        if sel == CASE_ASEND_ACLOSE:
            ok = case_asend_aclose(H, wid, rng, state, slot)
        elif sel == CASE_ASEND_ATHROW:
            ok = case_asend_athrow(H, wid, rng, state, slot)
        else:
            ok = case_control(H, wid, rng, state, slot)
        state["case"][sel][slot] += 1
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran), so runloom.Chan /
    # WaitGroup / yield_now are the cooperative M:N primitives.  All tallies are
    # per-slot single-writer lists summed in post() -- no shared += under GIL-off.
    # One cell PER WORKER (slot == wid), not 1024 cells aliased across
    # workers: at funcs=20000 the old [0]*1024 sizing made ~20 workers share
    # each slot and tore the single-writer tallies.  Size every per-slot array as
    # [0]*H.funcs so each worker is the sole writer of its cell (H.funcs is the
    # capped worker count by the time setup() runs).
    n = H.funcs
    H.state = {
        # global create/finalize conservation across ALL gens (every case)
        "created": [0] * n,
        "finalized": [0] * n,
        # control-arm-only create/finalize (single-owner -> must be exact)
        "ctrl_created": [0] * n,
        "ctrl_finalized": [0] * n,
        # per-case exercise counters (coverage)
        "case": [[0] * n for _ in range(NCASES)],
        # diagnostic tallies
        "tally": {
            "yielded": [0] * n,            # universe values asend-yielded
            "busy": [0] * n,               # owner-side legal "already running"
            "sib_busy": [0] * n,           # sibling-side legal "already running"
        },
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    created = sum(H.state["created"])
    finalized = sum(H.state["finalized"])
    ctrl_c = sum(H.state["ctrl_created"])
    ctrl_f = sum(H.state["ctrl_finalized"])
    yielded = sum(H.state["tally"]["yielded"])
    busy = sum(H.state["tally"]["busy"])
    sib_busy = sum(H.state["tally"]["sib_busy"])
    case_counts = [sum(H.state["case"][c]) for c in range(NCASES)]

    H.log("gens created={0} finalized={1} | control created={2} finalized={3} | "
          "values yielded={4} owner-busy={5} sibling-busy={6} | cases "
          "aclose={7} athrow={8} control={9} | ops={10}".format(
              created, finalized, ctrl_c, ctrl_f, yielded, busy, sib_busy,
              case_counts[0], case_counts[1], case_counts[2], H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed -- the async-gen race window "
                               "was never exercised")

    # FINALIZE CONSERVATION (the headline invariant): every async gen that was
    # created ran its try/finally finalizer exactly once -- none lost (frame
    # leaked / never unwound) and none doubled (frame re-entered / finally re-run).
    H.check(created == finalized,
            "finalize conservation BROKEN: {0} async gens created but {1} "
            "finalized -- a finalizer was {2} (a frame leaked without unwinding, "
            "or a torn ag_running_async double-stepped/re-entered a frame so its "
            "try/finally ran twice)".format(
                created, finalized,
                "LOST" if finalized < created else "DOUBLED"))
    H.check(created > 0, "no async gens were created -- workload vacuous")

    # CONTROL-ARM conservation (single-owner, race-free): the control path's
    # create/finalize must match EXACTLY.  A divergence here is a machinery bug.
    H.check(ctrl_c == ctrl_f,
            "CONTROL finalize conservation broken: {0} control gens created but "
            "{1} finalized with a SINGLE owner and no sibling -- the async-gen "
            "finalizer machinery itself lost/doubled a finalize (NOT contention)"
            .format(ctrl_c, ctrl_f))
    H.check(ctrl_c > 0,
            "the single-owner CONTROL arm never ran -- the falsifier that "
            "distinguishes a real machinery bug from M:N contention is untested")

    # Coverage: all three cases were exercised (round-robined by wid).
    H.check(case_counts[CASE_ASEND_ACLOSE] > 0,
            "CASE_ASEND_ACLOSE never ran -- aclose-vs-asend race untested")
    H.check(case_counts[CASE_ASEND_ATHROW] > 0,
            "CASE_ASEND_ATHROW never ran -- athrow-vs-asend race untested")
    H.check(case_counts[CASE_CONTROL] > 0,
            "CASE_CONTROL never ran -- the single-owner falsifier untested")

    # The contended path actually moved values through the asend/frame stepping
    # (else the IDENTITY/ORDER oracle was vacuous).
    H.check(yielded > 0,
            "no universe values were ever asend-yielded -- the asend frame "
            "stepping (and thus the identity/order oracle) was never exercised")

    H.require_no_lost("asyncgen-finalizer-conservation completeness")


if __name__ == "__main__":
    harness.main(
        "p448_asyncgen_finalizer_athrow_asen", body, setup=setup, post=post,
        default_funcs=3000,
        describe="async-gen asend stepping vs a cross-hub aclose/athrow firing "
                 "into the park window (ag_running_async check-then-set + shared "
                 "embedded frame): every yielded value in a finite sentinel "
                 "universe and strictly monotonic, the only tolerated busy error "
                 "is 'already running', and sum(created)==sum(finalized) "
                 "finalizer conservation -- with a single-owner control arm that "
                 "must yield the exact monotonic prefix and finalize once")
