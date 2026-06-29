"""big_100 / 439 -- nested memoryview slice-chain export-count conservation
under out-of-order release vs a sibling exporter resize-realloc.

The subject is the SHARED export count of a bytearray exporter
(``bytearray.ob_exports`` in Objects/bytearrayobject.c) as seen through a NESTED
memoryview SLICE CHAIN, and the increment/decrement path in
Objects/memoryobject.c.  No existing big_100 program drives a parent/child slice
CHAIN: p404 takes two INDEPENDENT memoryviews of one array; p434 takes ONE
``base.cast('Q')`` child of one base; p302/p415 hold a single getbuffer.  None
exercises ``mv1 = mv0[a:b]; mv2 = mv1[c:d]`` -- a 3-deep slice chain whose links
are RELEASED OUT OF ORDER while a sibling races the exporter.

What a sub-slice actually is (verified against this CPython, not assumed).  A
sliced memoryview does NOT keep a reference to its PARENT view object and does
NOT re-acquire a fresh buffer from the parent: ``memory_subscript`` ->
``init_slice`` copies the parent view's ``Py_buffer`` and calls
``mbuf_add_view`` on the SAME ``_PyManagedBufferObject``, so every link in the
chain has ``link.obj is ba`` (the ROOT exporter) and each link INDEPENDENTLY
bumps the exporter's export count:

    ba = bytearray(...)          # exporter, ob_exports == 0
    mv0 = memoryview(ba)         # mbuf_add_view -> ob_exports == 1   (mv0.obj is ba)
    mv1 = mv0[8:-8]              # init_slice    -> ob_exports == 2   (mv1.obj is ba)
    mv2 = mv1[8:-8]              # init_slice    -> ob_exports == 3   (mv2.obj is ba)

Each ``mvK.release()`` does exactly ONE ``mbuf_release`` decrement of that shared
count, in ANY order (releasing a parent before a child is allowed here -- it just
drops the count by 1; the child still pins the exporter on its own).  The
exporter's resize-realloc (``bytearray_resize`` -> ``PyMem_Realloc`` of ob_start,
reached via append/extend/clear/del-slice/pop) is gated on that count: it raises
``BufferError`` IFF ob_exports != 0.

THE HAZARD -- the exact C state + racing op pair.  The shared ob_exports word is
the contended field.  Under M:N an owner fiber builds the 3-deep chain, PARKS
holding it (the three views' raw bufs captured on its grown-down C stack), while
on ANOTHER hub two siblings race: (a) a RESIZER reads ob_exports to decide
BufferError-vs-realloc, and (b) the owner itself decrements the SAME count once
per ``release()``.  The racing pair is therefore the per-link release DECREMENT
of the shared pin vs the sibling's resize-check READ of that count, AND vs a
different link's decrement.  Two mutually-exclusive corruptions, BOTH falsifiable:

  * TORN-LOW / DOUBLE-DECREMENT -> UAF.  A release decrements twice (or a
    decrement races a sibling's check and the count is read/written torn), the
    count hits 0 while a STILL-LIVE link's buf points into ob_start, the resize
    frees/moves ob_start, and the live link reads freed memory -- a value OUTSIDE
    the closed byte universe, or a SIGSEGV.  Caught two ways: every byte read
    through the DEEPEST still-live child must equal f(its absolute index); and
    while ANY link is live a sibling resize MUST raise BufferError -- a resize
    that SUCCEEDS while a link is live means the count was torn to 0.

  * TORN-HIGH / LOST-DECREMENT -> LEAK.  A release's decrement is lost, the count
    never returns to 0, and the exporter is PERMANENTLY un-resizable.  Caught
    directly: after ALL three links are released (in whatever order this round
    chose) the very next resize MUST succeed exactly once (ob_exports provably
    back to 0 -- releasing N links decremented EXACTLY N times).

TARGET INVARIANT -- CONSERVATION over the chain.  Fresh ``bytearray`` ba[i]=f(i)
from a finite byte universe; mv0=memoryview(ba); mv1=mv0[8:-8]; mv2=mv1[8:-8].
While ANY of the three is live every sibling ``ba.append`` (etc.) MUST raise
BufferError (refusals counted).  Read through the deepest live child must equal
f(its absolute index).  Release in a deterministic ROUND-ROBIN of the six orders
(child-first 2,1,0 / parent-first 0,1,2 / middle-first 1,0,2 / 1,2,0 / 0,2,1 /
2,0,1).  At each release boundary, while links remain live, a sibling resize MUST
still be refused; after the FINAL release ``ba.append`` MUST succeed exactly once
(no double-free -> no premature resizable window; no lost decrement -> no stuck-
un-resizable leak).

SINGLE-OWNER CONTROL ARM (the falsifier).  An identical chain built, partially
verified, and released in the same chosen order by ONE fiber with NO sibling
touching the exporter.  A single owner cannot race itself, so the increment-on-
slice / decrement-on-release bookkeeping runs race-free; at each boundary the
control checks the count via try-resize (refused while links live, succeeds after
the last).  If only the CONTENDED arm leaks/UAFs it is the cross-hub race; if the
CONTROL also breaks, the lost/doubled decrement is in memoryview's OWN slice/
release machinery, not M:N contention.

CLOSED-WORLD oracle (per round, fail-fast + post):
  * len(mv1) == NBYTES-16, len(mv2) == NBYTES-32, and mv2[k] == f(k+16) for all k
    (the slice geometry/offset is intact across the park; a torn re-slice shifts
    the offset and reads the wrong absolute index);
  * while >=1 link live: sibling resize refused (BufferError) -- counted;
  * after the last release: resize succeeds exactly once -- counted;
  * reading a RELEASED link raises ValueError (the legal "operation forbidden on
    released memoryview" -- caught, never SIGSEGV);
  * CONSERVATION across the run: every gated round (>=1 refusal) ended resizable
    (refusals-seen rounds == resize-after-final rounds; a shortfall == a leaked
    export = a lost decrement; never a premature success = a doubled decrement).

Round-robin the RELEASE ORDER and the resize-trigger CASE by worker id in the
first ops so coverage holds under the timeout (the p125/p126/p172 flaky-random-
coverage fix), then random.

Stresses: nested memoryview slice-chain shared _PyManagedBufferObject export
count, per-link mbuf_release decrement vs exporter PyMem_Realloc resize-check,
out-of-order parent/child release, double-decrement UAF vs lost-decrement leak,
slice offset/geometry stability across a park, read-after-release ValueError.

Good TSan / controlled-replay target: the ob_exports decrement in mbuf_release
and the resize's read of ob_exports race across hubs -- a TSan report on
ba->ob_exports localizes the torn count before the universe assert or the
BufferError gate even fires.
"""
import harness
import runloom


# Finite BYTE universe.  ba[i] = f(i) draws from a recognizable spread (not the
# dense 0..255), so a torn/freed read through a live child reconstructs a byte
# NOT equal to f(its index) -- out of universe.  NBYTES is a multiple that leaves
# the deepest child a comfortable span and is large enough that the resize realloc
# genuinely moves ob_start under churn, small enough that many rounds complete.
NBYTES = 96

# Each slice trims 8 off each end, so the chain offsets are mv1 -> +8, mv2 -> +16.
TRIM = 8
LEN0 = NBYTES               # mv0 covers [0, NBYTES)
LEN1 = NBYTES - 2 * TRIM    # mv1 covers [8, NBYTES-8)        -> absolute +8
LEN2 = NBYTES - 4 * TRIM    # mv2 covers [16, NBYTES-16)      -> absolute +16

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024

# The six release orders of the 3-link chain (index 0 = mv0 root, 2 = mv2 deepest).
# Coverage of all six is asserted indirectly via the deterministic round-robin;
# each order is a distinct decrement-ordering of the shared export count.
RELEASE_ORDERS = (
    (2, 1, 0),   # child-first  (innermost released first)
    (0, 1, 2),   # parent-first (root released first)
    (1, 0, 2),   # middle-first
    (1, 2, 0),   # middle, then child, then parent
    (0, 2, 1),   # parent, then child, then middle
    (2, 0, 1),   # child, then parent, then middle
)
NORDERS = len(RELEASE_ORDERS)

# Resize-trigger CASES -- every one reaches bytearray_resize -> PyMem_Realloc of
# ob_start, the realloc the export count gates.  post()'s coverage relies on the
# deterministic round-robin so all four are hit even under the timeout.
CASE_APPEND = 0       # ba.append(b)      -- grow by 1
CASE_EXTEND = 1       # ba.extend(b"...") -- grow by k
CASE_CLEAR = 2        # ba.clear()        -- shrink to 0
CASE_DELSLICE = 3     # del ba[:2]        -- shrink by slice
NCASES = 4


def f(i):
    """Deterministic absolute-index -> byte.  ba[i] == f(i); a byte read through a
    live child that is not f(its absolute index) is a torn/freed read.  A mixed
    function over a 64-value sub-range so a coincidental match is unlikely, while
    staying a legal byte."""
    return (0x40 + (((i * 37) ^ (i >> 1) ^ 0x2B) & 0x3F)) & 0xFF


def fresh_bytearray():
    """A fresh bytearray with ba[i] == f(i) for i in [0, NBYTES)."""
    return bytearray(f(i) for i in range(NBYTES))


def try_resize(ba, case, rng):
    """Apply the round's resize-trigger CASE to the exporter.  Returns True if the
    resize SUCCEEDED (BufferError NOT raised), False if it raised BufferError (the
    exporter was pinned by >=1 live export).  Every case reaches PyMem_Realloc of
    ob_start, gated on ob_exports."""
    try:
        if case == CASE_APPEND:
            ba.append(rng.getrandbits(8))
        elif case == CASE_EXTEND:
            ba.extend(bytes(rng.getrandbits(8) for _ in range(3)))
        elif case == CASE_CLEAR:
            ba.clear()
        else:  # CASE_DELSLICE
            del ba[:2]
        return True
    except BufferError:
        return False


def build_chain(ba):
    """Build the 3-deep nested slice chain over ba.  Each link's .obj is the ROOT
    exporter ba and each independently bumps ba's export count (verified: a slice
    does not re-acquire from its parent view -- init_slice re-exports from the same
    managed buffer).  Returns [mv0, mv1, mv2]."""
    mv0 = memoryview(ba)        # ob_exports 0 -> 1
    mv1 = mv0[TRIM:-TRIM]       # ob_exports 1 -> 2
    mv2 = mv1[TRIM:-TRIM]       # ob_exports 2 -> 3
    return [mv0, mv1, mv2]


def check_geometry(H, chain):
    """The chain's lengths and the deepest child's offset must be intact (stable
    across the upcoming park).  A torn re-slice shifts an offset so the deepest
    child reads the wrong absolute index.  Returns False on the first violation."""
    mv0, mv1, mv2 = chain
    if len(mv0) != LEN0:
        H.fail("nested-slice geometry torn: len(mv0) == {0} != {1} -- the root "
               "view length changed (export/slice recompute corrupted)".format(
                   len(mv0), LEN0))
        return False
    if len(mv1) != LEN1:
        H.fail("nested-slice geometry torn: len(mv1=mv0[8:-8]) == {0} != {1} -- "
               "the mid slice offset/length is torn".format(len(mv1), LEN1))
        return False
    if len(mv2) != LEN2:
        H.fail("nested-slice geometry torn: len(mv2=mv1[8:-8]) == {0} != {1} -- "
               "the deepest slice offset/length is torn".format(len(mv2), LEN2))
        return False
    return True


def check_deep_values(H, mv2):
    """Every byte read through the DEEPEST live child mv2 must equal f(its ABSOLUTE
    index).  mv2 covers absolute [16, NBYTES-16), so mv2[k] == f(k + 16).  A torn
    read / UAF (a doubled decrement freed/moved ob_start while mv2 was still live)
    yields a byte != f(k+16) -- out of universe.  Returns False on first violation.
    A ValueError here (read on a released view) is caught by the caller as the legal
    outcome, never reached while mv2 is held live."""
    base = 2 * TRIM             # absolute offset of mv2[0] == 16
    for k in range(len(mv2)):
        got = mv2[k]
        want = f(base + k)
        if got != want:
            H.fail("deepest-child TORN/UAF read: mv2[{0}] == {1} != f({2}) == {3} "
                   "-- the shared export count was torn to 0 (a doubled release "
                   "decrement), the exporter resize freed/moved ob_start, and the "
                   "still-live nested child read freed memory through its stale "
                   "buf pointer".format(k, got, base + k, want))
            return False
    return True


def run_round_impl(H, wid, rng, order, case, slot, state):
    """One CONTENDED round.  Build a fresh seeded bytearray, take the 3-deep chain
    on it, and spawn a sibling RESIZER on ANOTHER hub that must be REFUSED while
    >=1 link is live.  Each export-state transition is a strict RENDEZVOUS over two
    Chans -- the owner tells the sibling "try now" only after it has reached the
    intended live-link count, and BLOCKS on the result before releasing the next
    link -- so each resize attempt PROVABLY lands in its intended export-count
    window (a bare yield_now() handoff does NOT guarantee that; it could land after
    the release, the p311-style "synchronize the hazard into the window"
    requirement).  The cross-hub race is still real: the sibling reads ba's
    ob_exports on its hub while the owner decrements it (release) and reads mv2
    across a park on its hub.

    Sequence (the export-count drama, `order` chooses the release sequence):
      1. build chain                 -> ob_exports 3 (all three links live)
      2. signal go; sibling resizes  -> MUST be refused; owner reads+verifies mv2
                                        across the park (3 links pin).
      3. release order[0]            -> ob_exports 2; signal; resize MUST be refused
      4. release order[1]            -> ob_exports 1; signal; resize MUST be refused
      5. release order[2]            -> ob_exports 0; the next resize (owner)
                                        MUST SUCCEED exactly once (count back to 0)
    """
    tally = state
    ba = fresh_bytearray()
    chain = build_chain(ba)

    if not check_geometry(H, chain):
        for mv in chain:
            mv.release()
        return

    # Rendezvous channels: one go/res pair per resize attempt the sibling makes
    # while a link is live.  The sibling makes THREE attempts (3 links, then 2,
    # then 1 live); each MUST be refused.  The owner sets the live-link state, then
    # signals, then blocks on the result before releasing the next link.
    go = [runloom.Chan(1) for _ in range(3)]
    res = [runloom.Chan(1) for _ in range(3)]
    wg = runloom.WaitGroup()
    wg.add(1)

    # Per-sibling RNG seeded from this fiber's rng (a SHARED random.Random corrupts
    # GIL-off -- each fiber needs its own).
    sib_seed = rng.getrandbits(48)

    def resizer():
        try:
            import random
            srng = random.Random(sib_seed)
            for j in range(3):
                go[j].recv()                       # owner: >=1 link is live now
                res[j].send(try_resize(ba, case, srng))   # MUST be refused -> False
        finally:
            wg.done()

    H.fiber(resizer)

    refused = 0
    fault = False

    # Phase 0: all three links live (ob_exports == 3).  Tell the sibling to try the
    # resize and, WHILE it races our exporter on its hub, read+verify every byte
    # through the deepest live child mv2 across a park.  A True (resized) result is
    # the count torn to 0 under three live exports (UAF risk for mv2's reads).
    mv2 = chain[2]
    if not check_deep_values(H, mv2):
        fault = True
    go[0].send(True)
    runloom.yield_now()                            # park with all 3 links LIVE
    if not fault and not check_deep_values(H, mv2):  # re-verify across the park
        fault = True
    # Chan.recv() returns Go-style (value, ok); unpack the bool the sibling sent.
    resized0, _ = res[0].recv()                    # sibling's 3-live attempt result
    if resized0:
        H.fail("sibling RESIZE SUCCEEDED while all THREE nested links were live "
               "(ob_exports should be 3) -- the shared export count was torn to 0 "
               "and the exporter was re-sized under three live exports (UAF risk "
               "for the deepest child's shared buf pointer)")
        fault = True
    else:
        refused += 1

    # Phases 1 and 2: release the links in the chosen ORDER, one at a time.  After
    # each of the first two releases >=1 link still pins the exporter, so the
    # sibling's resize MUST still be refused.  The deepest child mv2 is read again
    # whenever it is still live (a released link is NOT read -- that would be a
    # legal ValueError, not a fault).
    live = {0, 1, 2}
    for step in range(2):           # release order[0], then order[1]
        idx = order[step]
        chain[idx].release()        # ob_exports decremented by exactly 1
        live.discard(idx)
        if fault:
            # Don't strand the sibling on the remaining go channels.
            go[step + 1].send(True)
            res[step + 1].recv()
            continue
        # If mv2 is still live, its bytes must STILL be intact (no link gone has
        # changed the deepest child's window; a doubled decrement would have freed
        # the buffer under it).
        if 2 in live and not check_deep_values(H, mv2):
            fault = True
        go[step + 1].send(True)
        runloom.yield_now()         # park with the remaining links LIVE
        resized_step, _ = res[step + 1].recv()   # Go-style (value, ok); take the bool
        if resized_step:
            H.fail("sibling RESIZE SUCCEEDED after releasing link {0} (order "
                   "{1!r}, {2} link(s) still live, ob_exports should be {3}) -- a "
                   "release decremented the SHARED export count more than once (a "
                   "doubled decrement / torn ob_exports under M:N), unpinning the "
                   "exporter while a nested child still dereferences it".format(
                       idx, order, len(live), len(live)))
            fault = True
        else:
            refused += 1

    # Final release: the last remaining link.  ob_exports 1 -> 0.
    last = order[2]
    chain[last].release()
    live.discard(last)

    # A read through ANY link now must raise ValueError (released) -- the legal
    # "operation forbidden on released memoryview".  Confirm mv2 is properly dead
    # (a SIGSEGV or a stale read here would be the bug); count the legal ValueError.
    legal_release_read = False
    try:
        _ = chain[2][0]
    except ValueError:
        legal_release_read = True
    except Exception as exc:        # noqa: BLE001
        H.fail("read through a RELEASED nested child raised {0} (not the legal "
               "ValueError): {1} -- the release left the view in a torn state "
               "instead of cleanly forbidding access".format(
                   type(exc).__name__, exc))
        fault = True

    # Join the sibling so it has fully returned and ba is quiescent.
    wg.wait()
    if fault or H.failed:
        return

    if refused == 3:
        tally["refuse"][slot] += 1     # this round saw the gate hold all 3 times
    if legal_release_read:
        tally["release_read_ok"][slot] += 1

    # ---- export count returned to 0: the next resize MUST succeed exactly once ---
    # All three links released; ob_exports must be back to exactly 0.  A resize STILL
    # refused means a release decrement was LOST (TORN-HIGH leak) and the exporter is
    # permanently un-resizable; the N==3 decrements did not balance the 3 increments.
    import random
    frng = random.Random(sib_seed ^ 0xABCDEF)
    if not try_resize(ba, case, frng):
        H.fail("after releasing ALL THREE nested links (order {0!r}, ob_exports "
               "must be 0) the resize is STILL refused with BufferError -- a "
               "release decrement was LOST: releasing N=3 links did not decrement "
               "the shared export count exactly 3 times, the exporter is "
               "permanently un-resizable (export LEAK)".format(order))
        return
    # And a SECOND resize must also succeed (the count is at 0, not negative/torn).
    if not try_resize(ba, case, frng):
        H.fail("the SECOND post-release resize is refused -- the export count did "
               "not settle at exactly 0 after all releases (torn ob_exports)")
        return
    tally["resize_ok"][slot] += 1


def control_round(H, wid, rng, order, case, slot, state):
    """SINGLE-OWNER CONTROL ARM.  An identical 3-deep chain built, verified, and
    released in the same chosen ORDER by THIS fiber with NO sibling touching the
    exporter.  A single owner cannot race itself, so the per-link increment/
    decrement bookkeeping is exercised race-free.  At each boundary a self-issued
    try_resize probes the count: refused while >=1 link is live, succeeds after the
    last.  If THIS leaks (stays refused) or UAFs (resizes early), the lost/doubled
    decrement is in memoryview's OWN slice/release machinery, NOT M:N contention --
    the falsifier that distinguishes a primitive bug from a race."""
    tally = state
    ba = fresh_bytearray()
    chain = build_chain(ba)
    if not check_geometry(H, chain):
        for mv in chain:
            mv.release()
        return
    if not check_deep_values(H, chain[2]):
        for mv in chain:
            mv.release()
        return

    # While all three live, a resize MUST be refused even with no sibling.
    if try_resize(ba, case, rng):
        H.fail("CONTROL: resize succeeded while all 3 nested links live with NO "
               "sibling -- the chain did not pin the exporter (export count never "
               "reached 3); a memoryview slice/export machinery bug, not contention")
        for mv in chain:
            mv.release()
        return

    # Release order[0] and order[1]; after each, >=1 link still live -> still refused.
    live = {0, 1, 2}
    for step in range(2):
        idx = order[step]
        chain[idx].release()
        live.discard(idx)
        if 2 in live and not check_deep_values(H, chain[2]):
            for mv in chain:
                mv.release()
            return
        if try_resize(ba, case, rng):
            H.fail("CONTROL: resize succeeded after releasing link {0} ({1} still "
                   "live, no sibling) -- a single-owner release decremented the "
                   "export count more than once (a doubled decrement in "
                   "memoryview's own release machinery), not contention".format(
                       idx, len(live)))
            for j in live:
                chain[j].release()
            return

    # Release the last; export count must be 0; resize must now succeed.
    chain[order[2]].release()
    if not try_resize(ba, case, rng):
        H.fail("CONTROL: resize STILL refused after releasing all 3 nested links "
               "in order {0!r} (no sibling) -- a release decrement was LOST in "
               "memoryview's own machinery; the single-owner chain leaked an "
               "export, so the loss is NOT contention".format(order))
        return
    tally["control_ok"][slot] += 1


def worker(H, wid, rng, state):
    slot = wid & (SLOTS - 1)
    i = 0
    for _ in H.round_range():
        if not H.running():
            break
        # Round-robin the RELEASE ORDER and the resize-trigger CASE by worker id in
        # the first ops so every order (6) and every PyMem_Realloc-reaching case (4)
        # is covered even under the timeout (the p125/p126/p172 flaky-random-
        # coverage fix); random after.  Independent indices so their product is swept.
        if i < NORDERS * NCASES:
            order = RELEASE_ORDERS[(wid + i) % NORDERS]
            case = (wid + i) % NCASES
        else:
            order = RELEASE_ORDERS[rng.randrange(NORDERS)]
            case = rng.randrange(NCASES)
        # Every few rounds also run the single-owner CONTROL arm (the falsifier),
        # round-robined deterministically by (wid + i).
        do_control = ((wid + i) % 3 == 0)
        i += 1

        run_round_impl(H, wid, rng, order, case, slot, state)
        if H.failed:
            return
        if do_control:
            control_round(H, wid, rng, order, case, slot, state)
            if H.failed:
                return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # All per-slot tallies allocated here, inside the root (single writer per slot
    # -> race-free; summed in post()).  No shared object under test lives at module
    # scope; each round builds its own fresh bytearray exporter + slice chain.
    H.state = {
        "refuse": [0] * SLOTS,            # rounds where all 3 gated resizes refused
        "resize_ok": [0] * SLOTS,         # rounds where resize succeeded after final release
        "release_read_ok": [0] * SLOTS,   # released-view read -> legal ValueError
        "control_ok": [0] * SLOTS,        # single-owner control rounds passed
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    refuse = sum(H.state["refuse"])
    resize_ok = sum(H.state["resize_ok"])
    release_read_ok = sum(H.state["release_read_ok"])
    control_ok = sum(H.state["control_ok"])
    H.log("rounds-all-3-refused={0} resize_ok_after_final_release={1} "
          "released-read-ValueError={2} control_ok={3} ops={4}".format(
              refuse, resize_ok, release_read_ok, control_ok, H.total_ops()))

    H.check(H.total_ops() > 0, "no rounds completed -- the nested slice-chain "
            "release-vs-resize race window was never exercised")

    # The export-count GATE was real: while >=1 nested link was live the exporter
    # resize was actually refused at every boundary (the count was genuinely > 0;
    # the test was not vacuous).
    H.check(refuse > 0,
            "no round saw all three gated resizes refused while nested links were "
            "live -- the shared export-count gate was never exercised (the "
            "contended arm did no work)")

    # CONSERVATION: every contended round that reached the gate (saw refusals while
    # links were live) also became resizable after the FINAL release.  resize_ok
    # counts rounds that completed the release->resize; a round that refused while
    # live must end resizable.  A shortfall == a leaked export (a lost decrement);
    # a premature success would already have failed fast in run_round_impl (a
    # doubled decrement).  Every fully-completed contended round must satisfy both.
    H.check(resize_ok >= refuse,
            "export-count conservation broken: {0} round(s) refused the resize "
            "while a nested link was live but only {1} round(s) became resizable "
            "after the final release -- {2} round(s) leaked an export (a release "
            "decrement was lost; releasing N=3 links did not decrement the shared "
            "count exactly 3 times, the exporter stayed permanently un-resizable)"
            .format(refuse, resize_ok, refuse - resize_ok))

    # The released-view access path returned the legal ValueError (not a SIGSEGV /
    # stale read) at least once -- the read-after-release safety net was exercised.
    H.check(release_read_ok > 0,
            "no round confirmed a read through a fully-released nested child raises "
            "ValueError -- the read-after-release safety path was never exercised")

    # The single-owner CONTROL arm ran and never leaked/UAFed (a break HERE would
    # be a memoryview slice/release machinery bug, not contention).
    H.check(control_ok > 0,
            "the single-owner control arm never completed a round -- the falsifier "
            "that distinguishes a memoryview machinery bug from M:N contention was "
            "never exercised")

    H.require_no_lost()


if __name__ == "__main__":
    harness.main(
        "p439_nested_subview_release_orderin", body, setup=setup, post=post,
        default_funcs=3000,
        describe="a 3-deep nested memoryview slice chain (mv1=mv0[8:-8]; "
                 "mv2=mv1[8:-8]) shares ONE bytearray export count, each link "
                 "pinning it independently; under M:N a sibling resize-realloc must "
                 "be REFUSED while ANY link is live and must SUCCEED exactly once "
                 "after the last of N=3 links is released in a round-robin of out-"
                 "of-order release sequences.  Closed-world: deepest child reads "
                 "f(absolute index) (torn/UAF out-of-universe), resize refused while "
                 "live / succeeds after final release (no doubled decrement -> no "
                 "premature resizable UAF window; no lost decrement -> no stuck-un-"
                 "resizable leak); single-owner control falsifies memoryview "
                 "slice/release machinery loss vs contention")
