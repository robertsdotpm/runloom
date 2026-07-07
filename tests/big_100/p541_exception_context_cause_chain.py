"""big_100 / 541 -- implicit exception __context__ / explicit __cause__ chain
isolation under M:N (per-fiber exc_info must not cross fibers on hub migration).

When a NEW exception is raised while another exception is being HANDLED, the C
RAISE machinery stamps the new exception's implicit ``__context__`` from the
interpreter's CURRENT exception -- the per-thread ``exc_info`` that the ``except``
block made "live".  ``raise B from C`` additionally stamps the EXPLICIT
``__cause__`` to C.  So inside::

    try:
        raise A          # A becomes the handled exception (exc_info = A)
    except A:
        raise B from C   # B.__cause__ IS C (explicit)
                         # B.__context__ IS A (implicit, read from live exc_info)

the exact object identities are load-bearing: ``B.__cause__`` must be the very C
object we wrote, and ``B.__context__`` must be the very A object THIS fiber is
handling.

WHERE M:N BREAKS IT (the gap this program probes).  ``exc_info`` (the current-
exception state read by the implicit-context stamp) is per-PyThreadState.  A
runloom fiber runs on a hub's OS thread; when it PARKS at a cooperative yield it
may resume on a DIFFERENT hub, and its exc_info stack must travel with the FIBER,
not stay pinned to the hub's tstate.  If a hub-migration -- or a sibling fiber
running on the same tstate while this fiber is parked INSIDE its ``except A:``
handler -- leaked its own in-flight exception into the current-exception slot,
then when this fiber resumes and raises B, the C RAISE machinery would stamp
B.__context__ with a SIBLING'S exception instead of this fiber's A.  That is a
cross-fiber leak of the per-thread exception state: a real runtime isolation bug.

We put the yield EXACTLY where the hazard lives: INSIDE the ``except A:`` handler,
between the point where A becomes the live current-exception and the point where B
is raised (which reads that live state).  Every exception object is fiber-local
and tagged with the fiber's ``wid`` plus a per-iteration nonce, so a chain link
that points at a sibling's exception is caught THREE ways: identity (``is``), the
wid tag, and the nonce.

WHICH ORACLE IS LOAD-BEARING, AND WHY (matches plain-thread semantics):

  On a correct interpreter -- verified by the documented CPython semantics of
  implicit exception chaining and by plain OS threads (each thread raising its own
  A/B/C chain observes B.__context__ IS its own A and B.__cause__ IS its own C,
  0 cross-thread leaks, GIL on or off) -- the single-owner chain oracle below
  ALWAYS holds.  Under a correct runloom it must also hold across an arbitrary hub
  migration parked inside the handler.  If B.__context__ is NOT this fiber's A (or
  carries another fiber's wid/nonce), or B.__cause__ is NOT this fiber's C, the
  per-fiber exc_info isolation is broken -- a runloom bug -- and the oracle FAILS
  fast.  With no bug the program exits 0.

ORACLES:
  * LOAD-BEARING -- EXCEPTION-CHAIN ISOLATION (worker, HARD, fail-fast).  Each
    fiber, per iteration, builds three FIBER-LOCAL exception instances A, C (and
    later B), each tagged (wid, nonce).  It raises A, catches it, YIELDS inside the
    handler (the hub-migration window), then ``raise B from C`` and catches the
    escaping B.  It then asserts, by object identity AND by tag:
      - B.__cause__ IS the exact C it created           (explicit cause intact)
      - B.__context__ IS the exact A it is handling      (implicit context intact)
      - both links carry THIS fiber's wid and nonce      (no sibling leak)
    Single-owner: A, B, C are locals of one fiber, never shared.  A failure is a
    cross-fiber leak of per-thread exc_info -- a runloom isolation desync.

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (chain_checks > 0),
    so the implicit-context stamp was really exercised across the yield.

  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded inside its
    handler across the yield (parked, never resumed) never returns; the watchdog +
    require_no_lost catch it.

FAIL ON: B.__context__ that is not this fiber's own A (identity, wid, or nonce
mismatch), or B.__cause__ that is not this fiber's own C -- i.e. a sibling's
in-flight exception stamped as this fiber's chain link across a hub migration.

Stresses: the C RAISE machinery's implicit __context__ stamp reading per-thread
exc_info, ``raise ... from ...`` explicit __cause__, the exc_info stack save/
restore across a cooperative park+resume and hub migration, per-fiber exception-
state isolation under tens of thousands of goroutines.

Good TSan / controlled-M:N-replay target: the current-exception slot in the
PyThreadState is written when the ``except`` block goes live and read by the raise
of B; under the single-owner arm only ONE fiber touches its own chain, so a data
race on the exc_info slot -- or a replay that resumes this fiber on a tstate whose
current-exception was left set by a sibling -- is the cleanest signal before the
identity/tag oracle fires.
"""
import harness
import runloom


# Three distinct fiber-local exception types.  Distinct types make the intent
# explicit (A handled, B raised-from-C) and let the ``except`` clauses be precise;
# isolation is enforced by per-instance (wid, nonce) tags + object identity, not by
# the type.
class ExcA(Exception):
    """The exception a fiber RAISES then HANDLES; becomes the live current-
    exception whose identity must show up as B.__context__."""
    pass


class ExcB(Exception):
    """The exception raised INSIDE the handler via ``raise B from C``; its
    __cause__/__context__ chain links are the load-bearing oracle."""
    pass


class ExcC(Exception):
    """The explicit cause object passed to ``raise B from C``; must appear as
    B.__cause__ by exact identity."""
    pass


def make_exc(cls, wid, nonce):
    """Build one fiber-local, tagged exception instance.  The (wid, nonce) tag
    lets a cross-fiber leak be detected even if two instances happened to share an
    id (they never do while both are live, but the tag is a second, independent
    witness)."""
    e = cls("wid={0} nonce={1}".format(wid, nonce))
    e.wid = wid
    e.nonce = nonce
    return e


def chain_check(H, wid, nonce, state):
    """Single-owner exception-chain isolation check.

    Raises A, catches it, YIELDS inside the handler (hub-migration window), then
    ``raise B from C`` and catches B.  Asserts B's __cause__/__context__ are the
    EXACT fiber-local C/A this fiber created (identity + wid + nonce).  A sibling's
    exception leaking into this fiber's exc_info would corrupt B.__context__."""
    a = make_exc(ExcA, wid, nonce)
    c = make_exc(ExcC, wid, nonce)

    try:
        raise a
    except ExcA as caught_a:
        # caught_a IS a; it is now this fiber's live current-exception (exc_info).
        # YIELD here: this is the hazard boundary.  If a hub-migration or a sibling
        # running on this tstate overwrote the current-exception slot, the raise of
        # B below would stamp the wrong __context__.
        runloom.yield_now()
        if nonce & 1:
            runloom.sleep(0.0002)

        b = make_exc(ExcB, wid, nonce)
        try:
            raise b from c
        except ExcB as caught_b:
            # ---- LOAD-BEARING assertions (identity + tag) ------------------

            # 1. Explicit cause: B.__cause__ must be the EXACT C we created.
            cause = caught_b.__cause__
            if cause is not c:
                H.fail("exception __cause__ LEAK: B.__cause__ is not this fiber's "
                       "own C -- got {0!r} (id {1}) expected id {2} (wid {3} "
                       "nonce {4}); a sibling's exception was stamped as this "
                       "fiber's explicit cause across the yield".format(
                           cause, id(cause) if cause is not None else 0,
                           id(c), wid, nonce))
                return
            if getattr(cause, "wid", None) != wid or getattr(cause, "nonce", None) != nonce:
                H.fail("exception __cause__ TAG MISMATCH: B.__cause__ carries "
                       "wid={0} nonce={1}, expected wid={2} nonce={3} -- a cross-"
                       "fiber cause leak".format(
                           getattr(cause, "wid", None), getattr(cause, "nonce", None),
                           wid, nonce))
                return

            # 2. Implicit context: B.__context__ must be the EXACT A this fiber is
            #    handling (read from the live per-thread exc_info at raise time).
            ctx = caught_b.__context__
            if ctx is not a:
                H.fail("exception __context__ LEAK: B.__context__ is not this "
                       "fiber's own A (the exception it was handling) -- got "
                       "{0!r} (id {1}) expected id {2} (wid {3} nonce {4}); the "
                       "implicit-context stamp read a SIBLING'S in-flight "
                       "exception from a leaked per-thread exc_info across the "
                       "hub-migration yield".format(
                           ctx, id(ctx) if ctx is not None else 0,
                           id(a), wid, nonce))
                return
            if getattr(ctx, "wid", None) != wid or getattr(ctx, "nonce", None) != nonce:
                H.fail("exception __context__ TAG MISMATCH: B.__context__ carries "
                       "wid={0} nonce={1}, expected wid={2} nonce={3} -- a sibling "
                       "fiber's exception leaked into this fiber's exc_info and was "
                       "stamped as the implicit context".format(
                           getattr(ctx, "wid", None), getattr(ctx, "nonce", None),
                           wid, nonce))
                return

            # 3. caught_b must be our own B (sanity: the handler caught what we
            #    raised, not something re-raised by the machinery).
            if caught_b is not b:
                H.fail("exception SELF LEAK: the except ExcB handler caught {0!r} "
                       "(id {1}), not this fiber's own B (id {2}) (wid {3} nonce "
                       "{4})".format(caught_b, id(caught_b), id(b), wid, nonce))
                return

    state["chain_checks"][wid] += 1        # single-writer-per-slot, race-free


# Sustained checks per worker, bounded by H.running().  The exc_info-migration
# hazard only manifests under SUSTAINED churn: many fibers simultaneously parked
# INSIDE their except handlers (current-exception live) while the scheduler
# migrates them across hubs.  A single check per fiber barely overlaps a sibling's
# live handler window and does NOT reliably reproduce.
INNER_CAP = 100000


def worker(H, wid, rng, state):
    for _ in H.round_range():
        if not H.running():
            break
        nonce = 0
        while H.running() and nonce < INNER_CAP:
            chain_check(H, wid, nonce, state)
            if H.failed:
                return
            H.op(wid)
            nonce += 1
        H.task_done(wid)


def setup(H):
    # chain_checks: ONE slot per worker (wid-indexed, single-writer -> race-free).
    # Allocated here where H.funcs is known.
    H.state = {
        "chain_checks": [0] * H.funcs,
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["chain_checks"])
    H.log("exception-chain[single-owner LOAD-BEARING]: {0} __context__/__cause__ "
          "isolation checks (all passed fail-fast across the in-handler hub-"
          "migration yield); ops={1}".format(checks, H.total_ops()))

    # NON-VACUITY: the implicit-context stamp was really exercised across a yield.
    H.check(checks > 0,
            "no exception-chain isolation checks ran -- the in-handler exc_info "
            "hub-migration hazard was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished inside its except handler.
    H.require_no_lost("exception-chain isolation")


if __name__ == "__main__":
    harness.main(
        "p541_exception_context_cause_chain", body, setup=setup, post=post,
        default_funcs=6000,
        describe="implicit exception __context__ is stamped by the C RAISE "
                 "machinery from the per-thread current-exception (exc_info) at "
                 "the moment a new exception is raised inside an except handler; "
                 "explicit __cause__ from `raise B from C`.  Under M:N a hub-"
                 "migration while a fiber is parked INSIDE its handler could stamp "
                 "a sibling's in-flight exception as this fiber's __context__.  "
                 "LOAD-BEARING: each fiber raises a chain of its OWN fiber-local, "
                 "(wid,nonce)-tagged exceptions -- try: raise A; except A: "
                 "yield_now(); raise B from C -- and asserts B.__cause__ IS the "
                 "exact C and B.__context__ IS the exact A (identity + wid + "
                 "nonce).  Any chain link pointing at a sibling's exception is the "
                 "runloom exc_info-isolation bug")
