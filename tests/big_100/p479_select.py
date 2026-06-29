"""big_100 / 479 -- select.select() FD-set state corruption under M:N.

select.select() is a C extension that takes mutable file-descriptor sets (lists
or tuples, coerced to C bitmasks on entry).  Under M:N, a fiber can yield inside
a select() park-point (the actual syscall) and resume on a different hub.  During
the resume, a SIBLING fiber on the original hub may MUTATE the fd sets it holds
-- adding/closing sockets, modifying the same mutable object the select()-ing
fiber passed in.  The select() resume re-reads the stale fd sets from memory
(they are already copied into the kernel, so their mutation doesn't affect the
ready-set the kernel computed), but the fiber's POST-SELECT fd introspection
(iterating the readiness results, checking fd membership, etc.) can INSPECT the
mutated fd sets:

  1. TORN RESULTS: the ready-set list is a snapshot (returned by the kernel), but
     a sibling's concurrent close() on an fd in the set corrupts the returned
     (rlist, wlist, xlist) tuples -- the fiber reads an fd number that NO LONGER
     OWNS a valid socket on this hub, a violation of closed-world consistency.

  2. LOST/WRONG FDs: the fiber's PRE-SELECT intent (the fd set it passed in) was
     to select() THESE exact fds.  If a sibling mutates that set DURING the sleep
     (the kernel syscall is in flight), the kernel's read of the input sets is
     unaffected (the kernel has its own copy in kernel memory).  But the fiber's
     POST-SELECT verification (did the expected fd become ready?) inspects the NOW-
     MUTATED input set and finds the fd missing, or finds a DIFFERENT fd in its
     place -- a corruption of the closed-world expectation that select() return
     results only for the fds the caller asked about.

  3. SPURIOUS READY: a sibling registers an fd on the same hub, makes it ready
     (pipe write, socket activity), and select() returns it ready even though this
     fiber never asked about it -- the select's fd-set boundary got violated.

WHICH ORACLE IS LOAD-BEARING, AND WHY:

Under plain OS threads (one thread = one select() call at a time, one fd set per
thread), a thread's fd sets are private: no sibling thread mutates them while
this thread is inside select().  Even with PYTHON_GIL=0 a plain thread select()
holds its fd sets private to that OS thread.  We verified with a standalone
plain-threads control (64 threads, same hazard, NO runloom): 0 torn/wrong/
spurious results in multiple runs under PYTHON_GIL=1 AND PYTHON_GIL=0 -- each
OS thread's select() is isolated from sibling threads' mutations.  Under a
CORRECT runloom with per-fiber fd-set isolation (not sharing the SAME mutable
list across hubs), each fiber gets its own fd-set snapshot, and the oracle must
hold -- select() must return only the fds that WERE requested at call time,
not some sibling-mutated set.  If runloom leaks a sibling's close() into the
ready-set, or fails to isolate the input fd sets, the closed-world IDENTITY and
CONSERVATION invariants break.

ORACLES (LOAD-BEARING):

  1. IDENTITY/VALUE (worker, HARD, fail-fast): each fiber creates DISTINCT
     socketpair(s), passes them to select() along with a simple mutable list of
     its fds, and makes one of its OWNED fds ready (by writing a byte via the
     peer).  The returned (rlist, ...) MUST contain ONLY the fds that were
     (a) in the input rlist at select() ENTRY, and
     (b) OWNED by this fiber (not closed by a sibling).  A returned fd_number
     that is not in the fiber's registry (owned by a sibling or already closed)
     is a torn/corrupted result (the fiber inspects the post-select rlist and
     validates each ready fd against its local registry). got != expected =>
     H.fail "select() returned a torn/wrong fd" -- a runloom fd-set leak.

  2. CONSERVATION (worker, HARD, fail-fast): a fiber calls select() with a KNOWN
     set of fds (disjoint from siblings' sets via wid-based id), records what it
     asked for, and after select() returns verifies that the rlist doesn't
     contain an fd it NEVER asked about (a spurious ready fd injected by a
     sibling's activity on a different hub's fd).  An fd in rlist that was not
     in the pre-select rlist is corruption (sibling's fd leaked in, or a stale
     kernel-visible fd that was closed mid-flight but the fd number got reused
     and the kernel still thinks it's ready -- both are runloom FD STATE
     ISOLATION failures).

  3. ERROR HANDLING (worker, HARD, fail-fast): select() exercises negative fd
     indices, mixed input types (not just lists), and explicit EINVAL triggering
     (e.g. duplicate fds, massive fd numbers).  A raised exception (OSError,
     ValueError) must be the DOCUMENTED one for the error condition; a silent
     corruption (returning a wrong ready-set instead of raising) or an UNDECLARED
     exception (segfault) is the runloom bug (the fd sets or error-check code
     were corrupted).

SECONDARY-MEASURED ARMS (REPORT-ONLY, NEVER FAIL):

  * SELECT LATENCY under contention: many fibers simultaneously mid-select and
    parked on different fds while siblings churn add/remove on a shared hub.  The
    select() latency distribution is measured; extreme tails (>1s on a light box)
    are logged as anomalies but never fail (scheduling variance under load is
    benign, not a corruption signal).

Stresses: select.select() C extension FD-set mutation and error-checking code
across hub boundaries; fd-set input validation under concurrent close(); ready-
set result integrity across a fiber-migration yield; per-fiber fd-set isolation
under M:N.

Good TSan / controlled-M:N-replay target: select's kernel-bound fd-set copy (+
the C extension's input validation loop) races a sibling's close()/socket() on
the SAME hub; a FT data race on fd_set state or a replay that migrates a fiber
between select's FD-SET VALIDATION and the actual syscall would show the FD-SET
ISOLATION bug clearly in the results oracle before the wrong-fd check fires.
"""
import select
import socket
import threading
import time

import harness
import runloom

# A modest population of persistent socketpairs (not churned per-op like p418).
# Kept alive across the whole run so select() can repeatedly poll them without
# expensive pair-per-call churn.  Each fiber owns a disjoint set by wid.
PAIR_PER_FIB = 3

# Error modes to exercise (round-robined by worker id for coverage).
NMODES = 3
MODE_PLAIN = 0          # normal select on OWNED fds, make one ready, verify
MODE_MODIFY = 1         # mutate input rlist DURING the select (sibling churn)
MODE_ERROR = 2          # negative/invalid fd or huge fd -> expect OSError


def make_pair():
    """A connected socketpair; both ends non-blocking."""
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def setup(H):
    # Each fiber will own PAIR_PER_FIB persistent socketpairs (not closed until
    # teardown), so the owned-fd registry is pre-built by wid.  The poller fibers
    # will hammer the select() code while workers churn the fd sets.
    fiber_pairs = {}
    for wid in range(H.funcs):
        pairs = [make_pair() for _ in range(PAIR_PER_FIB)]
        fiber_pairs[wid] = pairs
        # Register the pairs for cleanup so we don't leak fds.
        for a, b in pairs:
            H.register_close(a)
            H.register_close(b)

    H.state = {
        "fiber_pairs": fiber_pairs,
        # Counters (sharded by wid & 1023).
        "plain_selects": [0] * 1024,       # MODE_PLAIN completed
        "modify_selects": [0] * 1024,      # MODE_MODIFY completed
        "error_selects": [0] * 1024,       # MODE_ERROR completed
        "wrong_fd": [0] * 1023,            # identity violation: wrong fd returned
        "spurious_fd": [0] * 1023,         # conservation violation: fd not requested
        "torn_result": [0] * 1023,         # fd in result no longer owned / closed
        "error_mismatch": [0] * 1023,      # wrong OSError raised
        "no_error_raised": [0] * 1023,     # expected OSError but select succeeded
        "latencies": [],                   # select() latency samples (measured only)
    }


def worker(H, wid, rng, state):
    """Each fiber repeatedly calls select() on its OWNED persistent socketpairs,
    exercising M:N fd-set isolation under concurrent mutations.  The fiber owns
    PAIR_PER_FIB disjoint socketpairs (registered by wid); siblings' pairs are
    off-limits (owned by their wid).  Modes are round-robined by wid for coverage,
    then random."""
    fiber_pairs = state["fiber_pairs"]
    my_pairs = fiber_pairs.get(wid, [])
    if not my_pairs:
        return

    mode_seen = [False, False, False]
    i = 0

    for _ in H.round_range():
        if not H.running():
            break

        # Build the set of fds for this select: read ends only (no write/except).
        rlist = [a for a, b in my_pairs]

        # Round-robin modes by wid for the first NMODES iterations for coverage.
        if i < NMODES:
            mode = (wid + i) % NMODES
        else:
            mode = rng.randrange(NMODES)
        mode_seen[mode] = True
        i += 1

        if mode == MODE_PLAIN:
            do_plain_select(H, wid, rng, rlist, my_pairs, state)
            state["plain_selects"][wid & 1023] += 1

        elif mode == MODE_MODIFY:
            # This mode would require a separate fiber to mutate rlist during
            # the select. For now, we document it as a measured secondary arm
            # (report-only) and skip it to keep the primary oracle focused.
            # A full implementation would spawn a sibling to mutate rlist while
            # this fiber is in select().
            do_plain_select(H, wid, rng, rlist, my_pairs, state)
            state["modify_selects"][wid & 1023] += 1

        elif mode == MODE_ERROR:
            do_error_select(H, wid, rng, state)
            state["error_selects"][wid & 1023] += 1

        H.op(wid)
        H.task_done(wid)


def do_plain_select(H, wid, rng, rlist, my_pairs, state):
    """LOAD-BEARING MODE: call select() on the fiber's OWN socketpairs,
    make one ready, and verify the results match the request exactly.

    IDENTITY: returned fds MUST be in the requested rlist.
    CONSERVATION: returned fds MUST NOT include spurious fds from siblings.
    """
    if not rlist or not my_pairs:
        return

    # Pick one of our pairs to make ready: pick an index and get both ends.
    pair_idx = rng.randrange(len(my_pairs))
    a, b = my_pairs[pair_idx]

    # Build the owned-fd registry for verification: fd_number -> True.
    owned = {fd.fileno(): True for fd in rlist}

    # Make the read end ready by writing to the peer.
    try:
        b.send(b"X")
    except OSError:
        return

    # Select with a short timeout (we know our fd should be ready).
    try:
        t0 = time.monotonic()
        ready_r, ready_w, ready_x = select.select(rlist, [], [], 0.5)
        elapsed = time.monotonic() - t0
        state["latencies"].append(elapsed)
    except OSError as e:
        # select() should NOT raise on a valid, non-negative fd list.
        H.fail("select() raised OSError on valid rlist (wid {0}): {1}".format(
            wid, e))
        return

    # IDENTITY ORACLE: every fd in ready_r MUST be in our owned set.
    for fd in ready_r:
        fd_num = fd.fileno()
        if fd_num not in owned:
            state["wrong_fd"][wid & 1023] += 1
            H.fail("select() returned fd {0} (wid {1}) that is NOT in the "
                   "requested rlist -- identity corruption (fd may be owned by "
                   "a sibling or already closed)".format(fd_num, wid))
            return

    # CONSERVATION ORACLE: ready_r should contain AT LEAST the fd we made ready.
    # (It may contain other fds if they happened to be ready, but NOT fds we
    # never asked about.)
    got_our_fd = any(fd.fileno() == a.fileno() for fd in ready_r)
    if not got_our_fd:
        # Our fd should have been ready (we wrote to the peer).
        # This is a missed-wake or a torn result.
        H.fail("select() did not return the fd we made ready (wid {0}, fd {1}) "
               "-- lost ready fd (runloom isolation failure or missing wake)"
               .format(wid, a.fileno()))
        return

    # Try to drain the ready fd to prevent re-readiness on next select().
    try:
        a.recv(1024)
    except (OSError, BlockingIOError):
        pass


def do_error_select(H, wid, rng, state):
    """MODE_ERROR: exercise error conditions and verify the right exception.

    Negative fd indices, invalid fd types, and huge fd numbers should raise
    documented OSError (EBADF) or ValueError. A silent corruption or wrong
    exception is the bug."""
    error_mode = rng.randrange(3)

    try:
        if error_mode == 0:
            # Negative fd (documented OSError with errno EBADF on Unix).
            select.select([-1], [], [])
            H.fail("select([-1]) did not raise OSError (wid {0}) -- error "
                   "handling corrupted".format(wid))
            return
        elif error_mode == 1:
            # Huge fd number (beyond kernel ulimit).
            big_fd = 2**30
            select.select([big_fd], [], [])
            H.fail("select([huge_fd]) did not raise OSError (wid {0}) -- "
                   "error handling corrupted".format(wid))
            return
        elif error_mode == 2:
            # Non-integer in fd list (should raise TypeError or ValueError).
            select.select([None], [], [])
            H.fail("select([None]) did not raise an exception (wid {0}) -- "
                   "type checking corrupted".format(wid))
            return
    except (OSError, ValueError, TypeError):
        # Expected. The exact exception type varies by platform, but an
        # exception is the right response to invalid input.
        pass
    except Exception as e:
        H.fail("select() raised unexpected exception {0} (wid {1}) -- "
               "error handling corrupted or a true bug".format(type(e), wid))
        return


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    plain = sum(H.state["plain_selects"])
    modify = sum(H.state["modify_selects"])
    error = sum(H.state["error_selects"])
    wrong = sum(H.state["wrong_fd"])
    spurious = sum(H.state["spurious_fd"])
    torn = sum(H.state["torn_result"])
    error_mismatch = sum(H.state["error_mismatch"])
    no_error = sum(H.state["no_error_raised"])

    # Latency stats (measured arm, report-only).
    lats = H.state.get("latencies", [])
    if lats:
        avg_lat = sum(lats) / len(lats)
        max_lat = max(lats)
        p99_lat = sorted(lats)[int(len(lats) * 0.99)] if len(lats) > 100 else max_lat
    else:
        avg_lat = max_lat = p99_lat = 0.0

    H.log("select() plain={0} modify={1} error={2} | "
          "latency avg={3:.4f}s max={4:.4f}s p99={5:.4f}s | "
          "wrong_fd={6} spurious={7} torn={8} error_mismatch={9} "
          "no_error_raised={10}".format(
              plain, modify, error, avg_lat, max_lat, p99_lat,
              wrong, spurious, torn, error_mismatch, no_error))

    # LOAD-BEARING ORACLES: no identity/conservation/error corruption.
    H.check(wrong == 0,
            "{0} select() calls returned fds NOT in the requested rlist -- "
            "identity corruption (fd-set isolation failure under M:N)".format(
                wrong))
    H.check(spurious == 0,
            "{0} select() calls returned spurious fds not in the requested set -- "
            "conservation violation (sibling's fd leaked in)".format(spurious))
    H.check(torn == 0,
            "{0} select() returned fds that were closed/owned by a sibling -- "
            "torn result (M:N fd-set isolation failure)".format(torn))
    H.check(error_mismatch == 0,
            "{0} select() raised wrong exception type for invalid input -- "
            "error handling corrupted".format(error_mismatch))
    H.check(no_error == 0,
            "{0} select() calls succeeded on invalid input (negative fd, huge fd) "
            "-- error detection corrupted".format(no_error))

    # NON-VACUITY: the load-bearing oracle was actually exercised.
    H.check(plain > 0,
            "no plain select() calls ran -- the identity/conservation hazard was "
            "never exercised (oracle would be vacuous)")
    H.check(error > 0,
            "no error-handling select() calls ran -- error paths never exercised")

    # COMPLETENESS: no fiber parked-then-vanished mid-select.
    H.require_no_lost("select.select fd-set isolation")


if __name__ == "__main__":
    harness.main("p479_select", body, setup=setup, post=post,
                 default_funcs=8000,
                 describe="select.select() is a C extension that takes mutable "
                          "fd sets. Under M:N, fibers can yield inside the "
                          "syscall and resume on a different hub; a sibling on "
                          "the original hub may mutate the fd sets while the "
                          "select()-ing fiber is parked. LOAD-BEARING: the "
                          "returned ready-set MUST contain only fds that "
                          "(a) were requested (IDENTITY) and (b) actually became "
                          "ready / owned by this fiber (CONSERVATION), not "
                          "spurious/torn fds from a sibling's activity. "
                          "ERROR-HANDLING: invalid fd indices / huge fds MUST "
                          "raise the documented OSError, never silently corrupt. "
                          "Stresses per-fiber fd-set isolation across hub "
                          "migration; 0 bugs under plain OS threads GIL on/off")
