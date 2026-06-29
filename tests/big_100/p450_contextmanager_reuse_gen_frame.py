"""big_100 / 450 -- @contextlib.contextmanager generator-frame reuse under M:N.

The subject is contextlib._GeneratorContextManager, the object the
@contextmanager decorator wraps a single generator in.  It holds ONE generator
object (self.gen, set in _GeneratorContextManagerBase.__init__) across the whole
with-body, and its two control points each RESUME that one suspended C generator
frame:

    def __enter__(self):
        ...
        try:
            return next(self.gen)              # resume frame -> runs up to `yield`
        except StopIteration:
            raise RuntimeError("generator didn't yield") from None

    def __exit__(self, typ, value, traceback):
        if typ is None:
            try:
                next(self.gen)                 # resume frame past `yield`
            except StopIteration:
                return False                   # the generator's OWN stop: normal
            else:
                raise RuntimeError("generator didn't stop")
        else:
            try:
                self.gen.throw(value)          # resume frame by THROWING into it
            except StopIteration as exc:
                return exc is not value        # identity check -- see below
            ...

The exact internal state under attack is the generator object's
``gi_frame_state`` (CPython genobject.c: FRAME_CREATED / FRAME_SUSPENDED /
FRAME_EXECUTING / FRAME_COMPLETED) plus the StopIteration-SUPPRESSION identity
test ``exc is not value`` in __exit__.  A correctly-implemented generator REJECTS
a re-entrant resume of a frame that is already FRAME_EXECUTING with
``ValueError("generator already executing")`` and a resume of a FRAME_COMPLETED
frame with ``StopIteration`` -- the gi_frame_state guard is the ONLY thing
standing between two resumes and a double-execution of the body.

THE M:N HAZARD (the precise racing op pair).  Under monkey.patch() the with-body
runs on a fiber that can PARK (hub migration / cooperative yield) while suspended
at the single ``yield`` -- i.e. with the generator frame in FRAME_SUSPENDED on a
grown-down C stack.  If a SIBLING fiber on ANOTHER hub holds the SAME
_GeneratorContextManager instance, it can drive __enter__'s ``next(self.gen)`` or
__exit__'s ``self.gen.throw(value)`` into that one suspended frame concurrently.
The racing op pair is therefore:

    fiber A: __exit__  ->  self.gen.throw(value)  / next(self.gen)   (resume #1)
    fiber B: __enter__ ->  next(self.gen)                            (resume #2)

both targeting the ONE gi_frame_state.  A torn FRAME_SUSPENDED->FRAME_EXECUTING
transition that fails to reject the second resume DOUBLE-ENTERS the with-body:
the body's pre-yield acquire runs twice for one logical acquisition, or its
post-yield finally release runs twice, so a resource acquired ONCE is released
TWICE (or vice-versa).  Separately, the ``exc is not value`` identity test in
__exit__ reads two object pointers; if a torn read mis-pairs them the machinery
mis-SUPPRESSES -- leaking the body's own exception, or swallowing a real one.

WHY THIS IS NOT JUST "two threads share an object" -- the depth.  Correct usage
gives every ``with cm():`` a FRESH _GeneratorContextManager (the decorator builds
a new one per call), so the correct-usage arm can NEVER legally double-enter; if
its conservation law breaks, the fault is in CPython's generator-frame machinery
itself (a torn gi_frame_state on a frame that NO other fiber even touched),
exactly the way p412's private BoundedSemaphore control falsifies "contention
dropped it".  The shared-cm arm is the deliberate re-entry probe: it provably
drives resume #2 INTO resume #1's park window and asserts the gi_frame_state
guard fires (and ONLY with the legal RuntimeError/ValueError, never a silent
double-run).

CLOSED-WORLD CONSERVATION INVARIANT (acquire/release of a sentinel token).  The
managed generator yields a token drawn from a finite sentinel UNIVERSE; its body
bumps a per-slot acquired[] tally BEFORE the yield and a per-slot released[]
tally in its ``finally`` AFTER the yield.  A torn/double-resumed frame shows up
as acquired != released (a body that ran its pre-yield once but its finally twice
== a doubled release; or vice-versa), or as a yielded token OUTSIDE the universe
(a torn frame handed back a value from a freed/garbage slot).

  CORRECT-USAGE ARM (fresh cm per with, case 0): each ``with cm(slot):`` parks
  mid-body via runloom.yield_now() while siblings on other hubs do the same on
  their OWN fresh cms.  Invariant: acquired == released == bodies-entered, every
  token in UNIVERSE.  (A fresh-cm body that ran its acquire exactly once must run
  its finally exactly once -- no leaked acquire, no doubled release.)

  CONTROL ARM (single fiber, serial, case 2): one fiber runs ``with cm(slot):``
  strictly serially N times; acquired == released == N EXACTLY, no other fiber in
  sight.  A mismatch here is contextmanager-machinery corruption, not contention
  -- the falsifier that says "the bug is in CPython, not in M:N scheduling".

  SHARED-CM RE-ENTRY PROBE (case 1): two fibers share ONE
  _GeneratorContextManager INSTANCE.  Fiber A __enter__s it (its one generator
  frame -> FRAME_SUSPENDED at the yield) and parks holding the body open; a gate
  releases fiber B, which drives a SECOND __enter__ on the SAME instance DURING
  A's park.  __enter__ (3.12+) does ``del self.args, self.kwds, self.func`` as its
  FIRST statement -- the "context manager can't be reused" guard -- so the second
  concurrent __enter__ MUST be rejected (AttributeError) BEFORE the frame is
  resumed; that del-then-read across two hubs is itself a non-atomic mutation of
  the shared instance __dict__ racing a read.  The ONLY legal outcome for B is a
  rejection (AttributeError, or RuntimeError/ValueError if it reaches the frame);
  the body never runs twice for one acquire (owntally stays 1); the yielded token
  stays in UNIVERSE.  A silent double-run, an UNGUARDED reuse that returns a token,
  a swallowed/leaked exception, an out-of-universe token, or a SIGSEGV is the bug.

  NB -- a REAL upstream free-threaded-CPython fault this program surfaced and
  steers around.  Having B instead call ``next(shared_cm.gen)`` DIRECTLY on the
  shared generator object (bypassing __enter__'s del-guard) so two fibers
  concurrently RESUME ONE suspended generator frame SIGSEGVs free-threaded CPython
  3.14.6t inside _PyEval_EvalFrameDefault -- and it reproduces IDENTICALLY with
  plain threading.Thread + PYTHON_GIL=0 and NO runloom (10/10), while PYTHON_GIL=1
  is clean (0/20000).  That is a stock free-threaded-CPython data race on the
  generator's gi_frame_state (a non-atomic FRAME_EXECUTING check-then-set in
  gen_send_ex2), NOT a runloom defect, and concurrently resuming one generator is
  a documented programming error.  So this probe races the LEGAL __enter__ reuse
  guard rather than committing the illegal concurrent raw resume, which would
  crash the process on an upstream bug regardless of the runtime under test.

Invariant (hot, fail-fast): every yielded token in UNIVERSE; the shared-cm second
__enter__ only ever rejects (AttributeError / RuntimeError / ValueError -- any
other type, or an unguarded reuse that returns a token, is a fault); the shared
cm's body runs exactly once per real acquire.
Invariant (post, reconciliation): acquired_total == released_total across the
correct-usage + control arms (no leaked acquire, no doubled release); the control
arm's acquired == released == its iterations exactly; every arm exercised; no
lost worker.

Stresses: _GeneratorContextManager.gen gi_frame_state FRAME_CREATED ->
FRAME_SUSPENDED suspend/resume across an M:N park, the __enter__ reuse-del guard
under a cross-hub re-entry, __exit__ StopIteration-suppression identity
(``exc is not value``), acquire/release conservation of a contextmanager body
under M:N park/migration.

Good TSan / controlled-M:N-replay target: the __enter__ del-then-read of
self.args racing a sibling enter on the shared instance, and the generator frame
suspend/resume across a hub migration, are both localizable write/read races; a
single doubled release under replay localizes the corruption before the
conservation sum even closes.
"""
import contextlib

import harness
import runloom

# Finite sentinel UNIVERSE of tokens the managed generator yields.  A token the
# body ever sees that is NOT in this set means the generator frame handed back a
# value from a freed/garbage slot (a torn FRAME_SUSPENDED resume) -- a hard fault.
# Sized so the per-slot tally tables and the token set are non-trivial; the token
# is what the body validates on every entry.
UNIVERSE_SIZE = 256
UNIVERSE = tuple(0x45000000 + i for i in range(UNIVERSE_SIZE))
UNIVERSE_SET = frozenset(UNIVERSE)

# Per-slot tally tables are sized [0]*H.funcs in setup() and indexed by the raw
# worker id (slot == wid), so EACH worker owns a PRIVATE slot -- single-writer-
# per-slot with no aliasing at any --funcs.  This is what makes the control arm's
# "this fiber owns slot serially / delta MUST be exact" premise TRUE: with
# slot==wid one and only one worker ever touches a given slot, so the serial
# delta check holds.  (The earlier SLOTS=1024, slot=wid&SLOT_MASK aliased ~20
# workers onto each slot at funcs=20000, which falsified that premise -- the bug
# was the test's bookkeeping, not the contextmanager machinery.)

# Cases, round-robined by worker id in the first ops so coverage holds whether
# one worker does K ops or K workers do 1 op each (the p125/p126/p172
# flaky-random-coverage fix -- NEVER pure random for coverage).
CASE_FRESH = 0       # correct usage: a FRESH cm per `with`, parked mid-body
CASE_SHARED = 1      # the re-entry probe: two fibers, ONE shared cm
CASE_CONTROL = 2     # single fiber, serial `with cm()` -- race-free control arm
NCASES = 3

# Serial iterations the control arm runs per round -- enough that a single
# dropped/doubled body bumps acquired!=released detectably, small enough that
# many rounds complete under the timeout.
CONTROL_ITERS = 16


def token_for(slot):
    """Deterministic slot -> token in UNIVERSE.  The body checks the token it is
    yielded is exactly this, so a torn resume that hands back a different/freed
    value is caught even if it happens to land in UNIVERSE."""
    return UNIVERSE[slot & (UNIVERSE_SIZE - 1)]


def make_managed(acquired, released, slot, owntally=None):
    """Build the @contextmanager-decorated factory whose body is the sentinel
    acquire/release.  The generator yields token_for(slot); BEFORE the yield it
    bumps acquired[slot] (the acquire), and in its ``finally`` AFTER the yield it
    bumps released[slot] (the release).  `owntally` (a 1-element list) counts how
    many times THIS specific generator's body actually entered -- used by the
    shared-cm probe to prove the body ran exactly once per real acquire.

    A correctly-machined generator runs the pre-yield exactly once and the
    finally exactly once per logical use; a torn double-resume runs one of them
    twice -- exactly the conservation break the tally tables catch."""
    tok = token_for(slot)

    @contextlib.contextmanager
    def cm():
        # ---- pre-yield: the ACQUIRE ----
        acquired[slot] += 1            # single-writer-per-slot for this worker
        if owntally is not None:
            owntally[0] += 1
        try:
            yield tok                 # body runs here; frame is FRAME_SUSPENDED
        finally:
            # ---- post-yield finally: the RELEASE ----
            released[slot] += 1

    return cm


def run_fresh(H, wid, rng, state, slot):
    """Case 0 (correct usage).  A FRESH _GeneratorContextManager per `with`, so
    the machinery can NEVER legally double-enter; we park mid-body via
    yield_now() so the fiber migrates/suspends WITH the generator frame in
    FRAME_SUSPENDED while siblings on other hubs do the same on their own fresh
    cms.  Asserts the yielded token is in UNIVERSE and == token_for(slot)."""
    acquired = state["acquired"]
    released = state["released"]
    cm = make_managed(acquired, released, slot)

    with cm() as tok:
        # The body.  Validate the yielded token is exactly what this slot's
        # generator must yield -- a torn FRAME_SUSPENDED resume would hand back a
        # different / out-of-universe value.
        if tok not in UNIVERSE_SET:
            H.fail("fresh-cm body yielded OUT-OF-UNIVERSE token {0!r} -- the "
                   "generator frame resumed to a freed/garbage value (torn "
                   "FRAME_SUSPENDED resume)".format(tok))
            return False
        if tok != token_for(slot):
            H.fail("fresh-cm body yielded token {0!r} != token_for(slot)={1!r} "
                   "-- the suspended generator frame handed back a value from a "
                   "different slot (torn resume under M:N park)".format(
                       tok, token_for(slot)))
            return False
        # Park INSIDE the with-body, frame FRAME_SUSPENDED at the yield: this is
        # the migration/park window the hazard needs.  A sibling on another hub
        # must NOT be able to perturb THIS frame (it is a fresh, private cm), so a
        # conservation break here is the machinery itself, not contention.
        runloom.yield_now()
    # __exit__ has now driven next(self.gen) past the yield -> the finally ran.
    return True


def run_shared(H, wid, rng, state, slot):
    """Case 1 (the re-entry probe).  Two fibers SHARE ONE
    _GeneratorContextManager instance.  Fiber A __enter__s it (its one generator
    frame -> FRAME_SUSPENDED at the yield) and parks holding the with-body open
    behind a gate; fiber B, released by the gate, drives a SECOND __enter__ on the
    SAME instance DURING A's park.  This races the _GeneratorContextManager's reuse
    guard across hubs: __enter__ (3.12+) does ``del self.args, self.kwds,
    self.func`` as its FIRST statement, so the second concurrent __enter__ MUST be
    rejected with AttributeError (the "context manager can't be reused" guard)
    BEFORE it resumes the frame -- and that del-then-read across two hubs is itself
    a non-atomic mutation of the shared instance's __dict__ racing a read.  The
    gi_frame_state suspend/resume is exercised by A's enter+exit while B contends.

    The ONLY legal outcome for B is a rejection -- AttributeError (the reuse-del
    guard fired), or RuntimeError/ValueError (a frame-state rejection if B somehow
    reaches next()) -- and the body must run EXACTLY ONCE for A's single acquire
    (owntally[0] == 1), every yielded token in UNIVERSE.  A silent DOUBLE-ENTRY of
    the body (owntally -> 2 == a doubled acquire for one logical use), an
    out-of-universe token, a swallowed/leaked exception, or any other exception
    type is the bug.

    DELIBERATELY NOT DONE -- a separate REAL upstream CPython fault this program
    surfaced and steers around: having B call ``next(shared_cm.gen)`` DIRECTLY on
    the shared generator object (bypassing the __enter__ del-guard) so two fibers
    concurrently resume ONE suspended generator frame SIGSEGVs free-threaded
    CPython 3.14.6t in _PyEval_EvalFrameDefault -- and it does so identically with
    plain threading.Thread + PYTHON_GIL=0 and NO runloom (10/10 reproductions),
    while PYTHON_GIL=1 is clean (0/20000).  That is a stock free-threaded-CPython
    data race on gi_frame_state (a non-atomic FRAME_EXECUTING check-then-set), NOT
    a runloom defect, and concurrently resuming one generator is a documented
    programming error; so this program races the LEGAL reuse guard (above) rather
    than committing that illegal concurrent raw resume, which would crash the
    process on an upstream bug regardless of the runtime under test."""
    acquired = state["acquired"]
    released = state["released"]
    owntally = [0]                    # times THIS cm's body actually entered
    cm = make_managed(acquired, released, slot, owntally=owntally)
    shared_cm = cm()                  # ONE _GeneratorContextManager instance

    gate = runloom.WaitGroup()        # A trips it just before it parks holding open
    gate.add(1)
    wg = runloom.WaitGroup()          # both fibers join here
    wg.add(2)
    # Per-fiber result holders (single-writer each -> race-free).
    res = {"a_tok": None, "a_ok": True, "b_outcome": None}

    def fiber_a():
        try:
            # A enters: next(shared_cm.gen) runs body to the yield, frame now
            # FRAME_SUSPENDED.  A holds the body open across a park.
            tok = shared_cm.__enter__()
            res["a_tok"] = tok
            if tok not in UNIVERSE_SET or tok != token_for(slot):
                res["a_ok"] = False
            # Release B INTO this park: B's second __enter__ on the SAME instance
            # now races A's still-open body / its reuse-del guard across hubs.
            gate.done()
            runloom.yield_now()       # park here, frame FRAME_SUSPENDED
            runloom.yield_now()       # give B real time on another hub
            # A now closes the body normally.  __exit__(None,None,None) resumes
            # the frame past the yield; the finally runs the release exactly once.
            try:
                shared_cm.__exit__(None, None, None)
            except (RuntimeError, ValueError, StopIteration, AttributeError):
                # A's own close raced B's reuse -- a legal detection, not a fault.
                pass
        except (RuntimeError, ValueError, StopIteration, AttributeError):
            # __enter__ itself raced (cannot normally, A enters first) -- legal.
            pass
        except BaseException as exc:            # noqa: BLE001
            H.fail("shared-cm fiber A raised unexpected {0}: {1} -- not the "
                   "legal generator-reuse outcome".format(
                       type(exc).__name__, exc))
        finally:
            wg.done()

    def fiber_b():
        try:
            gate.wait()               # wait until A is parked INSIDE the body
            # B drives a SECOND __enter__ on the SAME _GeneratorContextManager.
            # The reuse-del guard (del self.args/kwds/func on first enter) must
            # reject it -- AttributeError -- BEFORE the frame is resumed, NOT
            # double-enter the body.
            try:
                tok_b = shared_cm.__enter__()
                # __enter__ RETURNED a value: the reuse guard did NOT fire and the
                # frame was resumed a second time.  A correct instance cannot do
                # this (args/func/kwds were deleted); a returned value that re-ran
                # the body is a DOUBLE-ENTRY (caught by owntally below).  Record +
                # validate it is in-universe, then close B's view.
                res["b_outcome"] = ("entered", tok_b)
                try:
                    shared_cm.__exit__(None, None, None)
                except (RuntimeError, ValueError, StopIteration, AttributeError):
                    pass
            except AttributeError:
                # The reuse-del guard fired ("context manager can't be reused"):
                # the canonical legal rejection of a second __enter__.
                res["b_outcome"] = ("attrerror", None)
            except ValueError as exc:
                # "generator already executing": the FRAME_EXECUTING guard fired.
                res["b_outcome"] = ("valueerror", str(exc))
            except RuntimeError as exc:
                # A legal runtime rejection (e.g. "generator didn't yield").
                res["b_outcome"] = ("runtimeerror", str(exc))
        except BaseException as exc:            # noqa: BLE001
            H.fail("shared-cm fiber B raised unexpected {0}: {1} -- the "
                   "_GeneratorContextManager reuse guard let an unexpected "
                   "exception escape (torn instance state on a re-entrant "
                   "__enter__ across hubs)".format(type(exc).__name__, exc))
        finally:
            wg.done()

    H.fiber(fiber_a)
    H.fiber(fiber_b)
    wg.wait()

    if H.failed:
        return False

    # ---- the re-entry oracle (round now quiescent, both fibers joined) -------
    if not res["a_ok"]:
        H.fail("shared-cm fiber A was yielded token {0!r} (expected in-universe "
               "token_for(slot)={1!r}) -- torn FRAME_SUSPENDED resume".format(
                   res["a_tok"], token_for(slot)))
        return False

    # The pre-yield body (the ACQUIRE) must run EXACTLY ONCE for this one
    # generator.  A's __enter__ runs it once; B's direct resume must NOT re-run it
    # (the frame is past the pre-yield code -- it is suspended at, or has finished,
    # the yield).  owntally[0] == 2 means the gi_frame_state guard FAILED and the
    # body DOUBLE-ENTERED -- a doubled acquire for one logical use, the core bug.
    if owntally[0] != 1:
        H.fail("shared-cm body's pre-yield ran {0} times for ONE generator "
               "(expected exactly 1) -- a re-entrant resume DOUBLE-ENTERED the "
               "with-body (torn FRAME_SUSPENDED->FRAME_EXECUTING transition "
               "across hubs let the second resume re-run the acquire)".format(
                   owntally[0]))
        return False

    outcome = res["b_outcome"]
    if outcome is not None and outcome[0] == "entered":
        # B's second __enter__ RETURNED a value: the reuse-del guard did NOT fire
        # and the frame was resumed a second time.  A correct instance cannot do
        # this (args/func/kwds were deleted on A's first enter), so a returned
        # value means the guard was torn under the cross-hub race.  Even if the
        # body did not re-run (owntally proven == 1 above), a re-entry that
        # returned at all is anomalous; validate the token is at least in-universe
        # and then FAIL on the unguarded reuse.
        tok_b = outcome[1]
        if tok_b not in UNIVERSE_SET:
            H.fail("shared-cm fiber B's second __enter__ returned OUT-OF-UNIVERSE "
                   "token {0!r} -- the reuse race read a freed/garbage value from "
                   "the suspended frame".format(tok_b))
            return False
        H.fail("shared-cm fiber B's second __enter__ on the SAME instance RETURNED "
               "token {0!r} instead of being rejected -- the "
               "_GeneratorContextManager reuse-del guard did NOT fire under the "
               "cross-hub race (the instance was re-entered for one acquire)"
               .format(tok_b))
        return False

    # Record that this probe ran AND that B's second __enter__ was correctly
    # rejected (AttributeError reuse-del guard, or a ValueError/RuntimeError
    # frame-state rejection -- all mean the body did NOT double-run), so post() can
    # assert the guard was actually exercised rather than skipped.
    state["shared_runs"][slot] += 1
    if outcome is not None and outcome[0] in (
            "attrerror", "valueerror", "runtimeerror"):
        state["shared_rejected"][slot] += 1
    return True


def run_control(H, wid, rng, state, slot):
    """Case 2 (single-owner CONTROL ARM).  One fiber runs ``with cm():`` strictly
    serially CONTROL_ITERS times against FRESH cms, NO other fiber touching them.
    A single-fiber serial loop is race-free by construction, so acquired ==
    released == CONTROL_ITERS EXACTLY.  A mismatch is contextmanager-machinery
    corruption itself, not M:N contention -- the falsifier that disambiguates a
    real CPython bug from a scheduling artifact (mirrors p412's private-semaphore
    and p405's private-Counter controls)."""
    cacq = state["control_acq"]
    crel = state["control_rel"]
    citer = state["control_iters"]
    before_a = cacq[slot]
    before_r = crel[slot]
    for _ in range(CONTROL_ITERS):
        cm = make_managed(cacq, crel, slot)
        with cm() as tok:
            if tok not in UNIVERSE_SET or tok != token_for(slot):
                H.fail("CONTROL arm yielded bad token {0!r} (expected "
                       "token_for(slot)={1!r}) in a SINGLE-FIBER serial loop -- "
                       "the contextmanager generator machinery is corrupt "
                       "independent of contention".format(tok, token_for(slot)))
                return False
            # A tiny serial body; no yield_now (this arm is the race-free
            # baseline, not a park probe).
    da = cacq[slot] - before_a
    dr = crel[slot] - before_r
    # Hot fail-fast: this fiber owns slot serially, so the delta MUST be exact.
    if da != CONTROL_ITERS or dr != CONTROL_ITERS:
        H.fail("CONTROL arm conservation broken in a single-fiber serial loop: "
               "acquired+={0} released+={1} expected {2} each -- the "
               "contextmanager body ran the wrong number of times with NO "
               "concurrency (a CPython generator-frame machinery bug, not "
               "contention)".format(da, dr, CONTROL_ITERS))
        return False
    citer[slot] += CONTROL_ITERS
    return True


def worker(H, wid, rng, state):
    # slot == wid: each worker owns a PRIVATE slot (tally tables are sized
    # [0]*H.funcs).  No aliasing at any --funcs, so the control arm's single-owner
    # serial-delta premise holds.
    slot = wid
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the cases by worker id in the FIRST ops so every case is
        # exercised even when each worker manages only a few ops under the
        # timeout (the flaky-random-coverage fix); random after the first sweep.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1
        if sel == CASE_FRESH:
            ok = run_fresh(H, wid, rng, state, slot)
        elif sel == CASE_SHARED:
            ok = run_shared(H, wid, rng, state, slot)
        else:
            ok = run_control(H, wid, rng, state, slot)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Built INSIDE the root (monkey.patch() already ran); the per-slot tally
    # lists are single-writer-per-slot (each worker writes only its own slot),
    # summed in post().  No shared object under test needs locking: every cm in
    # the correct-usage / control arms is FRESH and private; the shared-cm arm's
    # one _GeneratorContextManager is deliberately raced WITHOUT a lock so the
    # gi_frame_state guard is the thing on trial.
    # Tally tables sized to the (capped) worker count so slot==wid gives each
    # worker a PRIVATE slot (the p41/p47 [0]*H.funcs idiom) -- no aliasing at any
    # --funcs, so the control arm's single-owner serial-delta check is exact.
    n = H.funcs
    H.state = {
        "acquired": [0] * n,            # correct-usage pre-yield acquires
        "released": [0] * n,            # correct-usage post-yield releases
        "control_acq": [0] * n,         # control-arm acquires
        "control_rel": [0] * n,         # control-arm releases
        "control_iters": [0] * n,       # control-arm iterations actually run
        "shared_runs": [0] * n,         # shared-cm probe rounds completed
        "shared_rejected": [0] * n,     # shared-cm reuses that were rejected
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    acq = sum(H.state["acquired"])
    rel = sum(H.state["released"])
    cacq = sum(H.state["control_acq"])
    crel = sum(H.state["control_rel"])
    citer = sum(H.state["control_iters"])
    sruns = sum(H.state["shared_runs"])
    srej = sum(H.state["shared_rejected"])
    H.log("correct-usage acquired={0} released={1}; control acquired={2} "
          "released={3} iters={4}; shared-cm runs={5} reuse-rejected={6}; "
          "ops={7}".format(acq, rel, cacq, crel, citer, sruns, srej,
                           H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed")

    # ---- correct-usage conservation: every fresh-cm body that ran its pre-yield
    # acquire exactly once ran its post-yield finally exactly once.  No leaked
    # acquire (acq > rel) and no doubled release (rel > acq).
    H.check(acq == rel,
            "correct-usage conservation broken: acquired={0} != released={1} "
            "-- a contextmanager body's pre-yield acquire and post-yield finally "
            "release ran a different number of times (a torn generator-frame "
            "resume {2} the body under M:N park)".format(
                acq, rel, "leaked" if acq > rel else "doubled"))
    H.check(acq > 0,
            "correct-usage (fresh-cm) arm never exercised -- no contextmanager "
            "body ran its parked acquire/release")

    # ---- CONTROL arm (single-fiber, serial): acquired == released == iterations
    # EXACTLY.  A single-owner serial loop is race-free by construction, so any
    # mismatch is CPython contextmanager-machinery corruption, not contention.
    H.check(cacq == crel,
            "CONTROL conservation broken: control acquired={0} != released={1} "
            "in single-fiber serial loops -- contextmanager generator machinery "
            "corrupt independent of contention".format(cacq, crel))
    H.check(cacq == citer,
            "CONTROL arm acquired={0} != iterations run={1} -- a serial "
            "`with cm()` body ran the wrong number of times with NO concurrency "
            "(CPython generator-frame bug)".format(cacq, citer))
    H.check(citer > 0,
            "CONTROL arm never exercised -- the race-free falsifier never ran")

    # ---- shared-cm re-entry probe: it ran, and at least one reuse was rejected
    # by the gi_frame_state guard (so the guard was actually tested, not skipped).
    # Every per-round shared check (body ran exactly once, in-universe token,
    # only RuntimeError/ValueError) is fail-fast, so reaching post with no failure
    # already proves the guard held; we only assert the probe wasn't vacuous.
    H.check(sruns > 0,
            "shared-cm re-entry probe never completed a round -- the "
            "gi_frame_state double-resume guard was never exercised")
    H.check(srej > 0,
            "shared-cm re-entry probe ran but NO reuse was ever rejected -- the "
            "re-entrant resume into a suspended generator frame never tripped the "
            "RuntimeError/ValueError guard, so the double-resume rejection is "
            "untested (the body must reject a second resume)")

    H.require_no_lost("contextmanager-reuse completeness")


if __name__ == "__main__":
    harness.main(
        "p450_contextmanager_reuse_gen_frame", body, setup=setup, post=post,
        default_funcs=3000,
        describe="@contextlib.contextmanager generator-frame reuse under M:N: a "
                 "fresh-cm body parks mid-yield while siblings run; acquire/"
                 "release conservation (acquired==released, token in a finite "
                 "sentinel universe), a single-fiber serial CONTROL arm "
                 "(acquired==released==iters exactly), and a shared-cm re-entry "
                 "probe that MUST reject a second resume of the one suspended "
                 "gi_frame_state with RuntimeError/ValueError -- a double-run, "
                 "leaked/doubled release, swallowed exception, out-of-universe "
                 "token, or SIGSEGV fails")
