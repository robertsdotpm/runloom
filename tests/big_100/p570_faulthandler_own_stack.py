"""big_100 / 570 -- faulthandler.dump_traceback own-thread stack fidelity under M:N.

faulthandler is a PROCESS-GLOBAL module (its enable()/register()/dump_later hooks
are singletons, NOT single-owner), so the load-bearing oracle is NOT the hook.  It
is the SINGLE-OWNER ARTIFACT the module PRODUCES: the text of
faulthandler.dump_traceback(file=fd, all_threads=False), which serializes the C
frame chain of the CURRENT thread's live PyThreadState.

WHERE M:N COULD BREAK IT (the gap this program probes).  runloom is an M:N
scheduler: a goroutine is a stackful coroutine whose entire Python frame chain is
swapped into a hub OS thread's PyThreadState when it runs, and swapped back out at
a cooperative yield (possibly resuming on a DIFFERENT hub thread).  faulthandler's
C traversal (_Py_DumpTraceback over tstate->current_frame) walks exactly that live
frame chain with the GIL OFF while sibling fibers on other hubs are concurrently
(a) running their own dump_traceback traversals and (b) swapping their own frame
chains in/out of their hubs' tstates.  If a fiber's frame chain is not perfectly
isolated across a yield/hub-migration -- if the traversal tears, if another fiber's
frames leak into this thread's chain, if a stale frame pointer survives the swap --
the produced traceback for THIS fiber would not match the stack THIS fiber built.

CLOSED-FORM SINGLE-OWNER ORACLE (no shared mutable state at all):

  Each fiber owns ONE temp file (its private dump sink; created in setup's tmpdir,
  opened per-fiber, closed in a finally -- never shared).  Per check the fiber
  descends a recursion of a UNIQUELY-NAMED helper `fh_probe_recurse` to a
  fiber-local depth D (D = MIN_DEPTH + wid % DEPTH_SPAN, so siblings build
  DIFFERENT-height stacks).  At the bottom it takes TWO dumps of its OWN current
  thread (all_threads=False), FROM THE SAME CALL SITE, with a cooperative yield
  BETWEEN them (so a sibling reliably interleaves and migrates hubs mid-check):

    for i in range(2):
        if i == 1: runloom.yield_now()   # sibling runs / this fiber may migrate hub
        dumps.append(do_dump(fd))        # SAME source line both iterations

  Because both dumps issue from the identical call site and every frame BENEATH is
  a SUSPENDED frame whose "current line" is its (unchanged) call site, a correct
  runtime MUST produce two BYTE-IDENTICAL dumps -- the stackful coroutine restored
  this fiber's frame chain exactly across the yield.  The closed-form laws:

    L1  well-formed: each dump decodes as UTF-8 and starts with faulthandler's
        "Stack (most recent call first):" header (a torn/garbage write fails).
    L2  frame conservation: the number of `fh_probe_recurse` frame lines equals
        EXACTLY D+1 (the recursion we built: n = D,D-1,...,0), in BOTH dumps.  A
        wrong count means the traversal dropped/duplicated one of OUR frames, or
        leaked a sibling's frames into this thread's chain.  D+1 is far below
        faulthandler's 100-frame print cap and our frames are the MOST-RECENT, so
        cap-truncation (which drops the OLDEST scheduler frames) never touches them.
    L3  cross-yield identity: dump[0] == dump[1] byte-for-byte.  This is the
        single-owner isolation law: this fiber's own frame chain, as walked by
        faulthandler on the CURRENT tstate, is preserved perfectly across a
        cooperative yield + possible hub migration + concurrent sibling dumps.

  Single-owner: the sink fd, the recursion, and both dump strings are fiber-local;
  nothing is shared, so a FAIL cannot be documented shared-object semantics -- it is
  a torn traversal, a cross-fiber frame leak, a lost/stale frame after a hub swap,
  or a SIGSEGV in the C traversal (caught by the watchdog + faulthandler itself).

  * NON-VACUITY (post, HARD): the load-bearing arm actually ran (checks > 0).
  * COMPLETENESS (post, HARD): require_no_lost -- a fiber stranded mid-traversal
    (parked-then-vanished inside dump_traceback) never returns.

  * MEASURED arm (report-ONLY, NEVER fails): every so often a fiber also calls
    dump_traceback(all_threads=TRUE), which enumerates EVERY thread state in the
    interpreter -- faulthandler's signature operation and the genuinely concurrent
    global-enumeration path (many hubs walking the whole thread list while fibers
    are created/destroyed).  We dump it into the SAME private fd, decode it
    (errors="replace"), and merely COUNT that it produced a non-empty, well-formed
    blob.  We NEVER assert on its CONTENT: an all-threads snapshot taken while other
    hubs mutate their stacks is inherently a racy VIEW of shared state (documented),
    so its exact bytes are not a single-owner law.  It exists to exercise the
    concurrent thread-list traversal (where a real crash, if any, would surface)
    without turning documented shared-view raciness into a false FAIL.

Stresses: faulthandler.dump_traceback C frame-chain traversal (_Py_DumpTraceback)
on the live current-thread PyThreadState under GIL-off M:N, frame-chain isolation
and restoration across a cooperative yield + hub migration, the 100-frame print
cap, and the concurrent all-threads interpreter thread-list enumeration.

This is CPU/stdlib-only except for ONE private temp file per fiber (faulthandler
requires a real fileno; StringIO has none), so max_funcs is capped -- the file sink
is fiber-local, opened once per worker and closed in a finally.
"""
import os

import faulthandler

import harness
import runloom

# File/fd-per-fiber: cap the forever loop's --funcs 1000000 so we never open a
# million sink files at once.  One small temp file + fd per worker, held for the
# worker's life, closed in a finally.
MAX_FUNCS = 2000

# Per-fiber recursion height.  Siblings build DIFFERENT-height stacks so a
# cross-fiber frame leak changes the observed frame count.  D+1 frames total,
# kept well under faulthandler's 100-frame print cap (and our frames are the
# most-recent, so the cap -- which drops the oldest scheduler frames -- never
# reaches them).
MIN_DEPTH = 4
DEPTH_SPAN = 20

# faulthandler's header for a single-thread dump (all_threads=False).
HEADER = "Stack (most recent call first):"

# The unique frame-name marker.  Counting occurrences of this substring in the
# dump yields exactly the number of fh_probe_recurse frames on the chain.
RECURSE_MARK = " in fh_probe_recurse\n"

# Sustained checks per worker so many fibers dump concurrently while others are
# parked across their yield -- the interleave that makes the traversal race a
# sibling's frame swap.  Bounded by H.running().
INNER_CAP = 100000

# How often (in inner iterations) a fiber also runs the report-only all-threads
# enumeration arm.  Kept sparse: it is coverage of the concurrent global
# thread-list walk, not the load-bearing law.
MEASURE_EVERY = 32


def do_dump(fd, all_threads):
    """Dump the CURRENT thread's traceback (or all threads) into this fiber's
    PRIVATE fd and return the produced bytes.  Rewinds + truncates first so the
    fd holds exactly this one dump.  faulthandler writes to the raw fileno
    (bypassing any Python-level buffer), so we read it back at the os level."""
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    faulthandler.dump_traceback(file=fd, all_threads=all_threads)
    os.lseek(fd, 0, os.SEEK_SET)
    chunks = []
    while True:
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def fh_probe_recurse(n, fd):
    """Descend a recursion of KNOWN height, then at the bottom take TWO dumps of
    THIS fiber's own thread from the SAME call site with a yield between them.

    Returns (dump0_bytes, dump1_bytes).  Because both do_dump calls are the same
    source line and every frame beneath is suspended at its (unchanged) call
    site, a correct runtime returns two byte-identical dumps -- the frame chain
    was restored exactly across the yield + any hub migration."""
    if n > 0:
        return fh_probe_recurse(n - 1, fd)
    dumps = []
    for i in range(2):
        if i == 1:
            # Park here so a sibling interleaves and THIS fiber may resume on a
            # different hub before the second (identical-call-site) dump.
            runloom.yield_now()
        dumps.append(do_dump(fd, False))       # SAME source line both iterations
    return dumps[0], dumps[1]


def probe_check(H, wid, depth, fd, state):
    """LOAD-BEARING single-owner check: build a depth-D own stack, dump it twice
    across a yield, assert L1 well-formed, L2 frame conservation (== depth+1
    fh_probe_recurse frames), L3 cross-yield byte identity."""
    d0, d1 = fh_probe_recurse(depth, fd)
    expected = depth + 1                        # frames for n = depth,...,0

    try:
        t0 = d0.decode("utf-8")
        t1 = d1.decode("utf-8")
    except UnicodeDecodeError as exc:
        H.fail("faulthandler dump was NOT valid UTF-8 (wid {0}, depth {1}): {2} "
               "-- a torn/garbage byte in the traversal output".format(
                   wid, depth, exc))
        return

    # L1: well-formed header.
    if not t0.startswith(HEADER) or not t1.startswith(HEADER):
        H.fail("faulthandler dump missing the '{0}' header (wid {1}, depth {2}): "
               "pre={3!r} post={4!r} -- torn/garbage dump output".format(
                   HEADER, wid, depth, t0[:60], t1[:60]))
        return

    # L2: frame conservation -- exactly depth+1 of OUR uniquely-named frames, in
    # BOTH dumps.  A wrong count = a dropped/duplicated own frame or a sibling's
    # frames leaked into this thread's chain by a torn traversal.
    c0 = t0.count(RECURSE_MARK)
    c1 = t1.count(RECURSE_MARK)
    if c0 != expected or c1 != expected:
        H.fail("faulthandler own-stack frame count WRONG (wid {0}): expected "
               "{1} fh_probe_recurse frames, got pre={2} post={3} -- the C "
               "traversal dropped/duplicated one of THIS fiber's frames, or "
               "leaked a sibling's frames into this thread's chain".format(
                   wid, expected, c0, c1))
        return

    # L3: cross-yield byte identity -- the single-owner isolation law.  Same call
    # site, suspended frames unchanged; a correct runtime restores this fiber's
    # frame chain exactly across the yield + hub migration + sibling dumps.
    if d0 != d1:
        H.fail("faulthandler own-stack dump CHANGED across a yield (wid {0}, "
               "depth {1}): the two same-call-site dumps differ -- this fiber's "
               "frame chain was not preserved/isolated across the cooperative "
               "yield + hub migration (torn traversal or cross-fiber frame "
               "leak).\n--- pre ---\n{2}\n--- post ---\n{3}".format(
                   wid, depth, t0[:400], t1[:400]))
        return

    state["checks"][wid & 1023] += 1


def measured_all_threads(H, wid, fd, state):
    """MEASURED, report-ONLY: exercise the concurrent all-threads enumeration
    (faulthandler's signature global thread-list walk) and merely COUNT that it
    produced a non-empty, well-formed blob.  NEVER asserts on content -- an
    all-threads snapshot taken while other hubs mutate their own stacks is an
    inherently racy VIEW of shared state (documented), not a single-owner law."""
    blob = do_dump(fd, True)
    text = blob.decode("utf-8", errors="replace")
    if text and ("Thread" in text or "Stack" in text):
        state["measured"][wid & 1023] += 1


def worker(H, wid, rng, state):
    depth = MIN_DEPTH + (wid % DEPTH_SPAN)
    path = os.path.join(state["dir"], "fh_w{0}".format(wid))
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        for _ in H.round_range():
            if not H.running():
                break
            idx = 0
            while H.running() and idx < INNER_CAP:
                probe_check(H, wid, depth, fd, state)     # LOAD-BEARING
                if H.failed:
                    return
                if idx % MEASURE_EVERY == 0:
                    measured_all_threads(H, wid, fd, state)   # report-only
                H.op(wid)
                idx += 1
            H.task_done(wid)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def setup(H):
    # faulthandler is enabled by the harness watchdog already; enabling is
    # idempotent and unrelated to dump_traceback (which works regardless).
    H.state = {
        "dir": H.make_tmpdir(prefix="big100_fh_"),
        "checks": [0] * 1024,        # LOAD-BEARING single-owner checks (non-vacuity)
        "measured": [0] * 1024,      # report-only all-threads enumerations
    }


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    checks = sum(H.state["checks"])
    measured = sum(H.state["measured"])
    H.log("faulthandler[own-stack LOAD-BEARING]: {0} dump-fidelity checks "
          "(L1 header + L2 frame-count + L3 cross-yield byte-identity, all "
          "passed fail-fast) | faulthandler[all-threads MEASURED]: {1} "
          "concurrent enumerations (report-only, content not asserted)".format(
              checks, measured))

    # NON-VACUITY: the single-owner dump-fidelity hazard was actually exercised.
    H.check(checks > 0,
            "no faulthandler own-stack checks ran -- the dump-fidelity hazard "
            "was never exercised (oracle would be vacuous)")

    # COMPLETENESS: no fiber parked-then-vanished (e.g. stranded inside the C
    # frame-chain traversal).
    H.require_no_lost("faulthandler own-stack fidelity")


if __name__ == "__main__":
    harness.main(
        "p570_faulthandler_own_stack", body, setup=setup, post=post,
        default_funcs=2000, max_funcs=MAX_FUNCS,
        describe="faulthandler.dump_traceback(all_threads=False) serializes the C "
                 "frame chain of the CURRENT thread's live PyThreadState.  Under "
                 "M:N, a goroutine's whole frame chain is swapped in/out of a hub "
                 "OS thread's tstate across a cooperative yield/hub-migration while "
                 "sibling fibers run their own dump traversals GIL-off.  "
                 "LOAD-BEARING single-owner: each fiber owns a private dump sink, "
                 "builds a uniquely-named recursion of known depth D, and dumps its "
                 "OWN thread twice from the same call site with a yield between; the "
                 "two dumps MUST be byte-identical and show EXACTLY D+1 of this "
                 "fiber's frames -- a differing/miscounted dump is a torn traversal "
                 "or a cross-fiber frame leak.  MEASURED all-threads enumeration is "
                 "report-only (racy shared view).")
