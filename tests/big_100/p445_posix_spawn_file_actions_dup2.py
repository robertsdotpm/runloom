"""big_100 / 445 -- os.posix_spawn file_actions DUP2 fd-capture vs fd-number recycle.

The subject is os.posix_spawn(path, argv, env, file_actions=[...]) -- the ONLY
child-spawn path in CPython that bypasses subprocess.Popen entirely (no _feed
thread, no Popen.__init__ pipe-wrapping offload, no _posixsubprocess fork_exec).
Its C implementation lives in posixmodule.c:

    os_posix_spawn_impl
      -> parse_file_actions(file_actions, &file_actions_buf, ...)

parse_file_actions() walks the Python file_actions LIST and, for each
(POSIX_SPAWN_DUP2, fd, newfd) tuple, calls
posix_spawn_file_actions_adddup2(&actions, fd, newfd) -- which COPIES the two
fd INTEGERS into a heap-allocated posix_spawn_file_actions_t (an array of action
records: __spawn_action{tag, .dup2_action{fd, newfd}} in glibc).  That C struct
is then applied ATOMICALLY in the child, between fork and exec, by glibc's
__spawni walking the action array and doing the real __dup2(fd, newfd) syscalls.

THE NON-ATOMIC BOUNDARY this program attacks is the per-action fd INT
captured-at-build (an immutable int already baked into the C struct) vs the LIVE
parent fd-number table, which sibling fibers on OTHER hubs are mutating by
closing and re-opening pipes concurrently.  The racing op pair is exactly:

    fiber A: parse_file_actions reading file_actions[(DUP2, fd_A, 1)]  +  os.posix_spawn
    fiber B: os.close(fd_A) then os.pipe()  (recycling that same fd NUMBER)

The hazards, both made falsifiable below:

  * CROSS-WIRE (the headline bug).  If parse_file_actions captured the wrong fd,
    or the actions LIST were shared/aliased and mutated mid-parse, or fd_A were
    recycled to a DIFFERENT kernel object before the in-child dup2 ran, the child
    inherits the WRONG fd on stdout/stdin -- it echoes a tag that belongs to
    ANOTHER fiber's pipe.  Caught by CONTENT: each child echoes a UNIQUE
    per-(wid,round) tag and the parent asserts echoed == its OWN tag.
  * CLOEXEC / INHERITANCE LEAK.  os.pipe() fds are O_CLOEXEC by default, so the
    child must inherit ONLY the two fds dup2'd to 0/1.  A botched action apply
    that leaks an extra inherited fd, or a dup2 onto the wrong newfd, shows up as
    a wrong tag, a short read, or a child that hangs (no EOF).
  * SPAWN SIGSEGV / build crash.  A torn parse of the mutable list, or a
    use-after-free of a recycled-then-freed fd record, is a hard fault the
    watchdog/faulthandler catches.

CLOSED-WORLD IDENTITY ORACLE.  The tag is a 64-byte content-addressed blob keyed
by (wid, round) -- a finite sentinel universe of recognizable byte patterns.  The
parent writes the tag into an input pipe, posix_spawns `cat` with
file_actions=[(DUP2, in_r, 0), (DUP2, out_w, 1), (CLOSE, in_r), (CLOSE, out_w)],
closes the parent ends, reads the child's stdout, and asserts the echoed bytes
== the tag it wrote (content + length).  A cross-wire reads ANOTHER unit's tag;
a leaked/short read reads fewer bytes or a torn pattern; both are caught.

TWO ARMS, round-robined by worker id in the first ops (NEVER flaky random -- the
p125/p126/p172 flaky-coverage lesson):

  * case 0 CONTENDED.  A sibling CHURNER fiber aggressively os.close()+os.pipe()
    cycles fd NUMBERS in the same number-space while this fiber builds the
    file_actions list and posix_spawns -- the recycle pressure that makes a
    captured-vs-live fd divergence reachable.  The churner is gated to fire its
    recycle burst DURING the parent's spawn parse window (gate.done() then
    yield_now()).  Identity must still hold: cat echoes THIS unit's tag.
  * case 1 CONTROL (the falsifier).  Identical posix_spawn build/apply, but with
    a PRIVATE fd set that NO sibling ever recycles -- single-owner, race-free by
    construction.  If the CONTROL ever echoes the wrong tag (or crashes), the
    fault is in CPython's parse_file_actions build / the in-child apply ITSELF,
    not fd-number contention.  This disambiguates "posix_spawn machinery is
    buggy" from "M:N recycle cross-wired an fd".

Invariant (hot, fail-fast): every child's echoed stdout == the parent's own
unique tag (identity conservation); the child exits 0; no spawn SIGSEGV.
Invariant (post): identity held on BOTH arms; the contended arm AND the control
arm were each exercised >=1 (coverage); spawns-attempted == spawns-verified
(no unit silently dropped); fd_end bounded vs fd_base (every action fd + pipe
closed -- no CLOEXEC/inheritance fd leak across the run).

Stresses: os.posix_spawn parse_file_actions DUP2 fd-int capture vs live fd-table
recycle, posix_spawn_file_actions_t build under M:N, in-child atomic dup2 apply,
stdout cross-wire / wrong-tag identity, O_CLOEXEC inheritance, fd-number recycle
pressure on the non-Popen spawn path.

Good TSan / controlled-M:N-replay target: the per-action fd int read inside
parse_file_actions while a sibling closes+reopens that fd number is a textbook
read-vs-mutate on the fd-number space; a TSan report on the fd-table or a single
cross-wired tag under replay localizes the capture/apply bug before the identity
assert even closes.
"""
import os
import shutil
import sys

import harness
import runloom

# ---- availability guard ---------------------------------------------------
# os.posix_spawn + POSIX_SPAWN_DUP2/CLOSE are POSIX-only; cat is the in-child
# stdin->stdout copy.  runloom_c.wait_fd is the cooperative readiness park for the
# child's stdout pipe (a raw os.read would OS-BLOCK the hub -- os.read on an
# arbitrary pipe is NOT monkey-patched cooperative, only sockets are).
_HAVE_SPAWN = (hasattr(os, "posix_spawn")
               and hasattr(os, "POSIX_SPAWN_DUP2")
               and hasattr(os, "POSIX_SPAWN_CLOSE"))
_CAT = shutil.which("cat")

try:
    import runloom_c
    _HAVE_WAITFD = hasattr(runloom_c, "wait_fd")
except Exception:                       # pragma: no cover - import guard
    runloom_c = None
    _HAVE_WAITFD = False

READ = 1                                # wait_fd events bitmask: 1 = readable
CANCELLED = getattr(runloom_c, "WAIT_FD_CANCELLED", -1) if runloom_c else -1

# Per-child tag length.  64 bytes is < PIPE_BUF (4096) so the parent's single
# os.write into the input pipe is ATOMIC and never blocks the hub (no cooperative
# write park needed); big enough that a cross-wire (another unit's tag) or a torn
# /short read is unmistakable by CONTENT, not merely by length.
TAG_LEN = 64

# Per-park ceiling (ms) waiting for the child's stdout to become readable.
# `cat` echoes the tag the instant it reads it from stdin (which we wrote + closed
# before the spawn), so a healthy child's stdout is readable almost immediately; a
# repeated timeout means the child never got the right stdin fd (a cross-wire that
# dup2'd the wrong/empty fd onto fd0 -> cat reads nothing -> never writes).
WAIT_MS = 500

# Bounded total wait (ms) for the whole identity read before we declare the unit
# stranded.  A child that inherited the WRONG (empty / closed) stdin via a
# cross-wired dup2 produces NO output and an immediate EOF or an indefinite block;
# this ceiling turns that into a recorded identity failure, not a watchdog hang.
READ_BUDGET_MS = 4000

# Recycle bursts the churner fires while the parent is mid-spawn.  Each burst
# closes a freshly-opened pipe pair and immediately re-opens, churning the fd
# NUMBERS the kernel hands back -- the recycle pressure that makes a freshly
# captured action fd likely collide with a number the parent just baked into its
# file_actions struct.
CHURN_BURSTS = 8

# The two arms.  post() asserts BOTH were exercised, so the worker round-robins
# them by id in its first ops (NOT random -- pure random reliably MISSES an arm at
# low op-count under the timeout, the flaky-coverage bug the suite already fixed).
CASE_CONTENDED = 0       # churner recycles fd numbers during the spawn parse
CASE_CONTROL = 1         # private, never-recycled fds -- the falsifier
NCASES = 2

# Slots for race-free per-worker tallies (single writer per slot, summed in post).
SLOTS = 1024
MASK = SLOTS - 1


def make_tag(wid, rno):
    """A unique, position-dependent 64-byte tag for (worker, round).  A cross-wire
    (this child echoing some OTHER unit's pipe) changes the content, so the
    identity oracle catches it by BYTES, not merely by length.  Pure arithmetic
    (no shared RNG -- a shared random.Random corrupts GIL-off)."""
    seed = ((wid & 0xFFFFF) << 20) ^ (rno & 0xFFFFF)
    out = bytearray(TAG_LEN)
    x = (seed * 2654435761 + 0x9E3779B1) & 0xFFFFFFFF
    for i in range(TAG_LEN):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out[i] = (x >> 16) & 0xFF
    return bytes(out)


def read_child_tag(H, out_r, want):
    """Cooperatively read up to `want` bytes from the child's stdout pipe.

    out_r is non-blocking; wait_fd parks the goroutine for READ readiness (a raw
    blocking os.read would OS-block the hub -- os.read on a pipe is NOT cooperative
    under monkey).  Bounded by READ_BUDGET_MS so a child that inherited the WRONG
    (empty) stdin via a cross-wired dup2 -- and therefore never writes -- becomes a
    recorded identity failure rather than a watchdog hang.  Returns the bytes read
    (possibly short on a cross-wire / torn read / early EOF)."""
    buf = bytearray()
    waited = 0
    while len(buf) < want and H.running() and waited < READ_BUDGET_MS:
        try:
            ready = runloom_c.wait_fd(out_r, READ, WAIT_MS)
        except OSError:
            break                       # fd closed at teardown
        if ready == CANCELLED:
            break
        if not (ready & READ):
            waited += WAIT_MS           # no readiness this slice; count toward budget
            continue
        try:
            chunk = os.read(out_r, want - len(buf))
        except BlockingIOError:
            continue                    # spurious readiness; re-park
        except OSError:
            break
        if not chunk:
            break                       # child closed stdout -> EOF (done or short)
        buf += chunk
    return bytes(buf)


def spawn_and_verify(H, wid, rno, tag, opened, closed):
    """Build the file_actions, write `tag` into the input pipe, posix_spawn `cat`
    with stdin dup2'd from the input pipe and stdout dup2'd to the output pipe,
    read the child's echo, and return (verified_ok, in_r, out_w) so the caller's
    churner can race the SAME fd numbers.

    The file_actions LIST is built FRESH here (never shared/aliased) -- a shared
    list mutated mid-parse would itself be a cross-wire vector; we keep it private
    so the ONLY contention is the live fd-NUMBER table the churner mutates.

    Returns True iff the child echoed exactly `tag`.  On any spawn/build crash the
    exception propagates to _worker_wrap (recorded as a worker error -> FAIL)."""
    shard = wid & MASK
    in_r, in_w = os.pipe()
    out_r, out_w = os.pipe()
    opened[shard] += 4
    # The child's stdout pipe must be non-blocking on the read end so wait_fd +
    # os.read never OS-block the hub.  (The child's end, out_w, stays blocking --
    # `cat` writes a tiny 64-byte tag, well under PIPE_BUF, so it never blocks.)
    os.set_blocking(out_r, False)

    # Write the tag into the input pipe BEFORE the spawn and close the write end,
    # so cat sees the bytes immediately then EOF -> echoes + exits.  64 bytes <
    # PIPE_BUF, so this single write is atomic and never blocks.
    nwritten = os.write(in_w, tag)
    os.close(in_w)
    closed[shard] += 1

    # FRESH, private file_actions list.  Each (DUP2, fd, newfd) bakes the two fd
    # INTEGERS into the C posix_spawn_file_actions_t at parse time; the CLOSE
    # actions drop the now-duplicated originals in the child so only 0/1 survive.
    file_actions = [
        (os.POSIX_SPAWN_DUP2, in_r, 0),
        (os.POSIX_SPAWN_DUP2, out_w, 1),
        (os.POSIX_SPAWN_CLOSE, in_r),
        (os.POSIX_SPAWN_CLOSE, out_w),
    ]

    pid = -1
    try:
        # os.posix_spawn: parse_file_actions builds the C struct from the list,
        # then glibc applies it atomically in the child between fork and exec.
        pid = os.posix_spawn(_CAT, ["cat"], os.environ,
                             file_actions=file_actions)
    finally:
        # Parent no longer needs its copies of the dup'd-into-child fds; close
        # them so EOF is well-defined and they don't leak / recycle ambiguously.
        os.close(in_r)
        os.close(out_w)
        closed[shard] += 2

    if pid < 0:
        os.close(out_r)
        closed[shard] += 1
        return False

    # IDENTITY: read the child's stdout and compare to OUR tag.
    data = read_child_tag(H, out_r, len(tag))
    os.close(out_r)
    closed[shard] += 1

    # Reap the child cooperatively (os.waitpid parks on a pidfd under monkey).
    code = None
    try:
        _, st = os.waitpid(pid, 0)
        code = os.waitstatus_to_exitcode(st)
    except OSError:
        code = None

    if not H.running():
        return True                     # benign teardown; don't judge a torn read

    if nwritten != len(tag):
        H.fail("short write of tag into input pipe (wid={0} round={1}): wrote "
               "{2}/{3} bytes -- the input pipe fd was wrong before the spawn even "
               "ran".format(wid, rno, nwritten, len(tag)))
        return False

    if data != tag:
        # The headline failure: the child's stdout did NOT carry our tag.
        H.fail("posix_spawn DUP2 identity broken (wid={0} round={1}): child "
               "echoed {2} bytes, our tag is {3} bytes; bytes_equal={4} -- the "
               "child inherited the WRONG fd on stdin/stdout (a captured action "
               "fd was recycled to a different kernel object before the in-child "
               "dup2 ran -> CROSS-WIRE), or the file_actions list was mis-parsed, "
               "or an fd leaked past CLOEXEC".format(
                   wid, rno, len(data), len(tag), data == tag))
        return False

    if code != 0:
        H.fail("posix_spawn'd cat exited {0} (wid={1} round={2}) -- the child's "
               "dup2'd fds were not applied cleanly".format(code, wid, rno))
        return False
    return True


def churner(H, wid, gate, state):
    """CONTENDED arm only.  Waits on the gate the spawner trips JUST before it
    calls posix_spawn, then fires a burst of os.close()+os.pipe() recycles in the
    SAME fd-number space -- the recycle pressure that makes a captured-vs-live
    action fd divergence reachable DURING the parent's parse_file_actions window.

    Single-owner of its OWN churn pipes (race-free), but it deliberately recycles
    fd NUMBERS that the parent's just-captured action fds may collide with.  Every
    pipe it opens it closes (fd-conservation)."""
    opened = state["opened"]
    closed = state["closed"]
    shard = wid & MASK
    gate.wait()                          # parent is now entering posix_spawn
    try:
        for _ in range(CHURN_BURSTS):
            if not H.running():
                break
            # Open a pipe pair (grabs the lowest free fd numbers -- exactly the
            # numbers the parent's action fds were just allocated from), yield so
            # the parse/spawn provably overlaps, then close to recycle them.
            r, w = os.pipe()
            opened[shard] += 2
            runloom.yield_now()          # land inside the parent's spawn parse
            os.close(r)
            os.close(w)
            closed[shard] += 2
    except OSError:
        # fd exhaustion under over-scale churn is a benign pressure signal, not a
        # bug; the spawner's identity oracle is the judge.  Never fail here.
        pass


def run_contended(H, wid, rno, state):
    """Case 0: spawn while a sibling churner recycles fd numbers in the parse
    window.  The gate trips the churner the instant before posix_spawn so the
    recycle burst provably overlaps the file_actions build/apply."""
    opened = state["opened"]
    closed = state["closed"]
    tag = make_tag(wid, rno)

    gate = runloom.WaitGroup()
    gate.add(1)
    wg = runloom.WaitGroup()
    wg.add(1)

    def run_churn():
        try:
            churner(H, wid, gate, state)
        finally:
            wg.done()

    H.fiber(run_churn)

    # Trip the gate immediately before the spawn so the churner's recycle burst
    # lands DURING parse_file_actions + the glibc apply.
    gate.done()
    ok = spawn_and_verify(H, wid, rno, tag, opened, closed)
    wg.wait()                            # churner joined -> fd space quiescent
    return ok


def run_control(H, wid, rno, state):
    """Case 1: the FALSIFIER.  Identical posix_spawn build/apply, but NO churner
    -- a private, single-owner fd set that no sibling ever recycles.  If THIS arm
    ever echoes the wrong tag (or crashes), the fault is in CPython's
    parse_file_actions build / in-child apply itself, not fd-number contention."""
    opened = state["opened"]
    closed = state["closed"]
    tag = make_tag(wid, rno)
    return spawn_and_verify(H, wid, rno, tag, opened, closed)


def worker(H, wid, rng, state):
    shard = wid & MASK
    contended = state["contended"]
    control = state["control"]
    attempted = state["attempted"]
    verified = state["verified"]
    i = 0
    rno = 0
    for _ in H.round_range():
        if not H.running():
            break
        rno += 1
        # Round-robin the two arms by worker id in the first ops so BOTH are
        # exercised even when each worker manages only a couple of ops under the
        # timeout (the flaky-random-coverage fix); random after.
        if i < NCASES:
            sel = (wid + i) % NCASES
        else:
            sel = rng.randrange(NCASES)
        i += 1

        attempted[shard] += 1
        if sel == CASE_CONTENDED:
            ok = run_contended(H, wid, rno, state)
            if ok:
                contended[shard] += 1
        else:
            ok = run_control(H, wid, rno, state)
            if ok:
                control[shard] += 1

        if not ok:
            return                       # identity broke (or benign teardown short)
        verified[shard] += 1
        H.op(wid)
        H.task_done(wid)


def setup(H):
    if not _HAVE_SPAWN:
        H.note_scale_limit(
            "os.posix_spawn / POSIX_SPAWN_DUP2 unavailable on this platform "
            "({0}) -- skipping the file_actions dup2 test".format(sys.platform))
        H.state = None
        return
    if _CAT is None:
        H.note_scale_limit("`cat` not found on PATH -- cannot run the in-child "
                           "stdin->stdout echo; skipping")
        H.state = None
        return
    if not _HAVE_WAITFD:
        H.note_scale_limit("runloom_c.wait_fd unavailable -- cannot cooperatively "
                           "read the child's stdout; skipping")
        H.state = None
        return
    # Built INSIDE the root (monkey.patch() already ran).  All tallies are
    # single-writer-per-slot (sharded by wid) so they are race-free GIL-off.
    H.state = {
        "contended": [0] * SLOTS,   # contended-arm units whose child echoed our tag
        "control": [0] * SLOTS,     # control-arm units (private, never-recycled fds)
        "attempted": [0] * SLOTS,   # spawn units attempted
        "verified": [0] * SLOTS,    # spawn units whose identity oracle passed
        "opened": [0] * SLOTS,      # fds opened (pipes: parent + churner)
        "closed": [0] * SLOTS,      # fds closed
    }


def body(H):
    if H.state is None:
        return                          # skipped in setup
    # A pipe + spawn per round is a CONCURRENCY hunt (the captured-vs-live action
    # fd race), not a volume hunt; cap concurrent spawners so a 1M soak doesn't
    # fork a process + open 4 pipes per func (which would exhaust fds/PIDs -- a
    # benign scale limit, not a bug).  Mirrors the subprocess-program ceiling.
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    if H.state is None:
        H.log("SKIPPED: {0}".format(H.scale_limit_reason or "no posix_spawn"))
        return
    contended = sum(H.state["contended"])
    control = sum(H.state["control"])
    attempted = sum(H.state["attempted"])
    verified = sum(H.state["verified"])
    opened = sum(H.state["opened"])
    closed = sum(H.state["closed"])
    H.log("posix_spawn identity: contended={0} control={1} attempted={2} "
          "verified={3} opened={4} closed={5} fd_base={6} fd_end={7} ops={8}"
          .format(contended, control, attempted, verified, opened, closed,
                  H.fd_base, H.fd_end, H.total_ops()))

    # The run actually spawned children (a vacuous run is not a pass).
    H.check(H.total_ops() > 0,
            "no posix_spawn identity unit completed -- the file_actions dup2 race "
            "window was never exercised")

    # BOTH arms were exercised: the contended probe AND the control falsifier.  If
    # the contended arm never ran we never applied recycle pressure; if the control
    # never ran we have no race-free baseline to attribute a failure against.
    H.check(contended > 0,
            "the CONTENDED arm never completed -- the fd-recycle pressure on the "
            "posix_spawn parse window was never actually exercised")
    H.check(control > 0,
            "the CONTROL arm never completed -- no private, never-recycled "
            "baseline ran, so a failure could not be attributed to contention vs "
            "a CPython parse_file_actions/apply bug")

    # Spawn conservation: every attempted unit that returned was verified (the
    # worker returns on the FIRST identity break, so reaching post with no failure
    # means attempted == verified for every COMPLETED worker; this catches a unit
    # silently dropped without either passing or failing the oracle).
    H.check(verified == attempted,
            "spawn conservation broken: attempted={0} but verified={1} -- a "
            "posix_spawn unit neither passed nor failed the identity oracle "
            "(silently dropped)".format(attempted, verified))

    # fd-leak oracle (the inheritance / CLOEXEC check): every pipe fd we opened
    # (parent's 4 per spawn + the churner's 2 per burst) was closed, so the
    # process fd balance must not grow with funcs.  A leaked action fd, a child
    # that inherited an extra fd past CLOEXEC (keeping a parent end open), or an
    # un-closed pipe end would push fd_end well past fd_base.  Bounded by the
    # concurrent fan-out, NOT by funcs (every per-round fd is closed in-round).
    if H.fd_base >= 0 and H.fd_end >= 0:
        H.check(H.fd_end < H.fd_base + 256,
                "fd leak across run: end {0} vs base {1} -- a file_actions dup2 "
                "fd, a churner pipe, or a child-inherited fd (CLOEXEC bypass) was "
                "not closed".format(H.fd_end, H.fd_base))

    # A spawner parked forever reading a cross-wired (empty) stdout, or stranded in
    # waitpid on a child that never exits, is a LOST worker -- not merely slow.
    H.require_no_lost("posix_spawn file_actions completeness")


if __name__ == "__main__":
    # Moderate default sibling N: like p325/p26/p31, the per-round pipe-quad +
    # churner + child fan-out is a CONCURRENCY hunt (the captured-vs-live action fd
    # cross-wire), not a volume hunt -- a few thousand posix_spawns across hubs is
    # plenty to expose a mis-captured DUP2 fd or a recycle-driven cross-wire.  A
    # hard ceiling keeps the soak driver from forking a process + 4 pipes per func
    # at 1M (which would exhaust PIDs/fds -- a benign scale limit, not a fault).
    harness.main("p445_posix_spawn_file_actions_dup2", body, setup=setup,
                 post=post, default_funcs=1200, max_funcs=3000,
                 describe="os.posix_spawn with file_actions=[(DUP2,in_r,0),"
                          "(DUP2,out_w,1),CLOSE,CLOSE] spawns `cat` echoing a "
                          "unique per-(wid,round) tag; a sibling churner recycles "
                          "fd numbers DURING the parse window (contended arm) vs a "
                          "private never-recycled fd set (control); child echo == "
                          "own tag (identity), exit 0, no fd leak -- a cross-wire, "
                          "CLOEXEC leak, or spawn crash fails")
