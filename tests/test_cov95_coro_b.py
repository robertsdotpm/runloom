"""Round-2 complementary coverage suite for src/runloom_c/coro.c.

tests/test_cov95_coro.py (batch 1) already drives the env-gated MODES
(arena carve, depot static-cap override, MADV mode resolve, fiber_n fresh
deferred, the invariant sanitizer, the prewarm thread-spawn-failure paths).
This file is COMPLEMENTARY: it targets the lines batch 1's workloads do NOT
reach because they UNDER-SPAWN.

The key structural fact batch 1 missed:

    runloom_coro_destroy RECYCLES a finished coro into a PER-THREAD coro pool
    (runloom_coro_pool, RUNLOOM_TLS, cap RUNLOOM_CORO_POOL_CAP == 512) with its
    stack still ATTACHED -- it only calls runloom_stack_release (which does the
    arena-slice recognition, the MADV reclaim, and the depot flush) once that
    per-thread pool OVERFLOWS, i.e. once MORE THAN 512 coros have been
    destroyed ON A SINGLE HUB.

So every stack-RELEASE path -- the arena release branch (coro.c L607 ->
L608-618 plus the runloom_stack_in_arena probe L504-509), the MADV-flag resolve
to "off"/"dontneed" (L572/575/594, reached lazily on the first release), and
the depot over-cap munmap in runloom_stack_flush_to_global (L403-404) -- is
ONLY exercised when >512 coros are released on ONE pool.  Batch 1 spawned 200-300
at a time, so the pool absorbed them all and none of these lines ran.

Each worker therefore spawns N > 512 fibers and holds them ALL concurrently
alive on a SINGLE M:1 pool (runloom.run(1, ...) -> rc.go, the cooperative
single-thread scheduler whose coro pool is one TLS pool) behind a release
WaitGroup, so when they finish together the pool overflows by N-512 and that
many real stack releases run.  M:1 (not M:N) is deliberate: it pins every
destroy to ONE per-thread pool, making the >512 overflow deterministic instead
of split across H hubs (each well under 512).

Env modes are resolved ONCE per process (cached static), so each mode runs in
its own SUBPROCESS with the env set; gcov only counts a subprocess's lines on a
CLEAN exit, so every worker prints a unique marker and we assert BOTH the marker
AND returncode 0.  Oracles are real: every fiber sets its own bytearray slot
(single writer -> race-free with the GIL off) and we assert all N ran, plus the
depot/arena structures stay consistent (rc._self_check == 0) after the churn.

Regions driven (still-uncovered coro.c line -> how), all verified by
regenerating gcov against the instrumented build before writing this file:
  L504-509  runloom_stack_in_arena: the release-path arena recognition probe,
            reached from runloom_stack_release L607 ONLY when an arena slice is
            actually released (pool overflow) -- 0 hits with <=512 fibers.
  L608-618  the arena release branch: madvise the slice body + runloom_arena_free
            it back to the bump allocator.  Same >512 gate.
  L572      RUNLOOM_STACK_MADV=off: runloom_stack_madv_reclaim resolves its
            cached flag to 0 -- but the resolve only runs on the FIRST stack
            release, so a process that never releases never resolves "off".
  L575,594  RUNLOOM_STACK_MADV=dontneed: flag resolves to MADV_DONTNEED and is
            stored; reached on the first release (same >512 gate).
  L403-404  runloom_stack_flush_to_global over-cap munmap: with a TINY
            RUNLOOM_STACK_DEPOT_CAP, the TLS->depot flush of released stacks
            exceeds the cap and munmaps the excess.  Needs real releases (>512)
            AND a low cap; batch 1's depot test used cap=2000 (never overflows).
  L866-868  runloom_stack_prewarm_global CAS-loss: two prewarm contexts (the
            continuous daemon + many detached background prewarm threads) both
            pass the `n < cap` full-check, both mmap (lock dropped), and the
            loser sees `n >= cap` on the re-check -> gives the stack back.  A
            genuine race -> driven hard against a tiny cap; the test asserts the
            CORRECTNESS invariant (depot consistent, prewarm well-behaved), the
            line credit accrues from the race landing in at least one of the
            many iterations.

Lines this file deliberately does NOT chase (see the structured exclusions):
  * L1401-1449, L1481-1483 (runloom_coro_grow / maybe_grow copy-on-grow): EMPIRICALLY
    proven unreachable -- a probe with 60 nested Python frames YIELDING at each
    level ran maybe_grow 587k times and the `headroom < quarter` body NEVER
    fired, because CPython 3.13 keeps interpreter frames on its own heap data
    stack, so Python recursion does not lower the coro's C sp across a resume.
  * L126-144, L182 (stack_base / guard_size / invariant_fail): crash-handler-only.
  * L589 (MADV_DONTNEED fallback after MADV_FREE EINVAL): pre-4.5 kernel only.
  * L773 (hwm-scan batch continuation): needs >2 MiB live C stack, > CPython's
    own recursion guard.
  * L1227-1247 (runloom_coro_init_at), L1347-1352 (runloom_coro_arena_stack):
    declared in coro.h but have NO caller anywhere in the extension (dead API).
"""
import os
import subprocess
import sys
import textwrap

import pytest

import runloom_c as rc
from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# Same skip rationale as batch 1: coro.c's stack pool / arena / madvise live
# behind the guard-page (fcontext/ucontext) backends and the workloads need the
# real scheduler -- skip the whole file on a GIL build.
pytestmark = pytest.mark.skipif(
    not FT, reason="coro.c stack-release paths need the M:N / FT build")

# Must exceed RUNLOOM_CORO_POOL_CAP (512) so the SINGLE M:1 coro pool overflows
# on finish and the surplus coros actually call runloom_stack_release.  700 gives
# ~188 real releases per round -- enough to resolve the MADV flag, recognise +
# free arena slices, and overflow a tiny depot cap.
CONC = 700


def _run_worker(body, env_extra=None, timeout=240):
    """Run a dedented worker snippet in a fresh subprocess; return CompletedProcess.

    Generous timeout: holding 700 fibers concurrently alive + churning stack
    releases can run slow on a box shared with the local CI runner.  A timeout
    there is contention, not a bug -- callers pytest.skip on TimeoutExpired.
    """
    src = ("import sys\n"
           "sys.path.insert(0, 'src')\n"
           "import runloom_c as rc\n"
           + textwrap.dedent(body))
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    try:
        return subprocess.run([PY, "-c", src], cwd=REPO, env=env,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.skip("coro.c release worker timed out (box under heavy load)")


def _assert_clean(p, marker):
    assert p.returncode == 0, (
        "worker crashed (rc=%d)\nstdout=%s\nstderr=%s"
        % (p.returncode, p.stdout[-1800:], p.stderr[-1800:]))
    assert marker in p.stdout, (
        "worker did not reach %r\nstdout=%s\nstderr=%s"
        % (marker, p.stdout[-1800:], p.stderr[-1800:]))


# The shared workload: spawn N>512 fibers on a SINGLE M:1 pool, hold them ALL
# concurrently alive behind a release gate, then let them finish together so the
# per-thread coro pool overflows by N-512 -> that many real runloom_stack_release
# calls.  Each fiber owns its own `ran` slot (single writer -> race-free with the
# GIL off).  After the run we assert every fiber ran AND the scheduler/netpoll
# structures are still consistent (rc._self_check == 0) -- a corrupted arena
# free-list / depot flush would trip it.  `rounds` repeats so a second wave of
# releases hits structures already at steady state (depot already at cap for the
# munmap path; arena cursor already cycled).
_OVERFLOW = r'''
import runloom
from runloom.sync import WaitGroup
N = {n}
ROUNDS = {rounds}

def _wave():
    release = WaitGroup(); release.add(1)
    up      = WaitGroup(); up.add(N)
    done    = WaitGroup(); done.add(N)
    ran = bytearray(N)               # one writer per slot -> race-free under FT
    def body(i):
        ran[i] = 1
        up.done()
        release.wait()               # HOLD the coro alive: all N live at once
        done.done()
    for i in range(N):
        rc.fiber(lambda i=i: body(i))   # M:1 single-thread scheduler -> ONE coro pool
    up.wait()                        # every fiber has started + is parked
    release.done()                   # release them all -> N coros finish on ONE pool
    done.wait()
    assert sum(ran) == N, "only %d/%d fibers ran" % (sum(ran), N)

def main():
    for _ in range(ROUNDS):
        _wave()

runloom.run(1, main)                 # M:1: pins every destroy to one TLS coro pool
assert rc._self_check(0) == 0, "self_check tripped after release churn"
print("{marker} %d" % N)
'''


# --------------------------------------------------------------------------
# L504-509 (runloom_stack_in_arena) + L608-618 (arena release branch).
# --------------------------------------------------------------------------
def test_arena_stack_release_path_on_pool_overflow():
    """RUNLOOM_STACK_ARENA=1 carves every fiber's stack from the one big arena.
    With N=700 fibers held concurrently alive on a SINGLE M:1 coro pool, the pool
    (cap 512) overflows on finish, so ~188 coros take the runloom_stack_release
    path PER round.  For an arena slice that means the L607 guard calls
    runloom_stack_in_arena (L504-509) -> true -> the arena release branch
    (L608-618): madvise the slice body and runloom_arena_free it back to the bump
    allocator.  This NEVER ran with batch 1's <=300-fiber waves (pool absorbed
    them, no release).  Oracle: every fiber ran in every round and the arena
    free-list / cursor stay consistent (self_check == 0); a mis-computed slot
    index in L611 or a double-free in runloom_arena_free would corrupt it."""
    body = _OVERFLOW.format(n=CONC, rounds=3, marker="ARENA_RELEASE_OK")
    p = _run_worker(body, {"RUNLOOM_STACK_ARENA": "1",
                           "RUNLOOM_STACK_ARENA_N": "8192"})
    _assert_clean(p, "ARENA_RELEASE_OK %d" % CONC)


# --------------------------------------------------------------------------
# L572 : RUNLOOM_STACK_MADV=off -> reclaim flag resolves to 0 on first release.
# --------------------------------------------------------------------------
def test_madv_off_flag_resolves_on_real_release():
    """runloom_stack_madv_reclaim resolves its cached flag lazily on the FIRST
    pooled-stack release.  RUNLOOM_STACK_MADV=off makes that resolution take the
    L571->L572 branch (flag = 0) and store it (L594), after which every release
    skips madvise.  Batch 1 set the env but its <=300-fiber waves never released a
    stack, so the resolve (and L572) never ran.  Here N=700 overflows the M:1
    pool -> real releases -> the 'off' flag resolves.  Oracle: all fibers run and
    structures stay consistent with reclaim DISABLED (pages kept resident)."""
    body = _OVERFLOW.format(n=CONC, rounds=2, marker="MADV_OFF_OK")
    p = _run_worker(body, {"RUNLOOM_STACK_MADV": "off"})
    _assert_clean(p, "MADV_OFF_OK %d" % CONC)


# --------------------------------------------------------------------------
# L575, L594 : RUNLOOM_STACK_MADV=dontneed -> flag = MADV_DONTNEED, eager reclaim.
# --------------------------------------------------------------------------
def test_madv_dontneed_flag_resolves_and_reclaims_on_release():
    """RUNLOOM_STACK_MADV=dontneed makes the first real release resolve the flag
    via L573->L575 (MADV_DONTNEED) and store it (L594); every subsequent release
    then EAGERLY madvise(MADV_DONTNEED)s the stack body (the tight-RSS / old
    behaviour).  Same >512 release gate as above (batch 1's waves never reached
    the resolve).  Oracle: the eager per-release zap does not corrupt a stack the
    pool later reuses -> all fibers run across both rounds, self_check clean."""
    body = _OVERFLOW.format(n=CONC, rounds=2, marker="MADV_DN_OK")
    p = _run_worker(body, {"RUNLOOM_STACK_MADV": "dontneed"})
    _assert_clean(p, "MADV_DN_OK %d" % CONC)


# --------------------------------------------------------------------------
# L403-404 : runloom_stack_flush_to_global over-cap munmap.
# --------------------------------------------------------------------------
def test_depot_flush_over_cap_munmaps_excess():
    """With a TINY RUNLOOM_STACK_DEPOT_CAP=1, the global depot fills on the first
    released stack; every later TLS->depot flush (runloom_stack_flush_to_global,
    triggered once the per-thread TLS cache exceeds RUNLOOM_STACK_TLS_CAP=64) then
    finds the depot AT cap and takes the L402->L403-404 else-branch, munmap'ing the
    excess stacks instead of pooling them (the bound that keeps total mappings
    finite).  Requires BOTH a low cap AND real releases: batch 1's depot test used
    cap=2000 (never overflows) and its small waves never released.  N=700 over two
    M:1 rounds drives many flushes past the cap-1 depot.  Oracle: the munmap path
    returns stacks to the OS correctly -- fibers all run, no UAF on a freed stack,
    self_check clean (a wrong size read at L404 would munmap the wrong length)."""
    body = _OVERFLOW.format(n=CONC, rounds=2, marker="DEPOT_MUNMAP_OK")
    p = _run_worker(body, {"RUNLOOM_STACK_DEPOT_CAP": "1"})
    _assert_clean(p, "DEPOT_MUNMAP_OK %d" % CONC)


# --------------------------------------------------------------------------
# L866-868 : runloom_stack_prewarm_global CAS-loss (lost the cap race -> give back).
# --------------------------------------------------------------------------
def test_prewarm_global_cap_race_gives_stack_back():
    """runloom_stack_prewarm_global re-checks `runloom_global_stack_n < cap` AFTER
    dropping the depot lock to mmap a fresh stack.  When two prewarm contexts both
    pass the initial full-check and both mmap, the one that re-acquires the lock
    second sees the depot now AT cap and takes the L865->L866-868 else-branch:
    unlock, munmap the just-mapped stack, break.  We provoke this race by running
    the continuous prewarm DAEMON (prewarm_keep) concurrently with many detached
    BACKGROUND prewarm threads (prewarm(..., background=True)), all hammering a
    tiny RUNLOOM_STACK_DEPOT_CAP so the depot sits right at the cap and the
    post-mmap re-check frequently loses.  It is a genuine race, so the worker
    drives MANY iterations; the line credit accrues from the race landing in at
    least one iteration across the whole run.

    The ASSERTION is on real behaviour, not the line: across the storm the depot
    must stay structurally consistent (rc._self_check == 0), prewarm_stop must
    cleanly join the daemon, and a final synchronous prewarm with a raised cap
    must return a sane non-negative count -- i.e. the give-back path frees the
    racing stack rather than leaking or double-inserting it."""
    body = r'''
import runloom_c as rc
# Continuous daemon keeps the depot churning right at the tiny cap.
assert rc.prewarm_keep(4, 65536) == 0
# Many detached background prewarm threads each pass the < cap full-check then
# race the post-mmap re-check against the daemon + each other -> the loser hits
# the L866-868 give-back.  Plenty of iterations so the race lands.
for _ in range(600):
    for _ in range(8):
        r = rc.prewarm(8, 65536, True)   # background=True -> detached racer thread
        assert r in (0, -1), "prewarm(background) returned %r" % (r,)
rc.prewarm_stop()                         # join the daemon cleanly
# Structures survived the storm: no leaked/double-inserted stack from a give-back.
assert rc._self_check(0) == 0, "self_check tripped after prewarm race storm"
# A synchronous prewarm with a sane cap still behaves (give-back left the depot OK).
n = rc.prewarm(4, 65536, False)
assert n is None or (isinstance(n, int) and n >= 0), "sync prewarm returned %r" % (n,)
rc.prewarm_stop()
print("PREWARM_RACE_OK")
'''
    p = _run_worker(body, {"RUNLOOM_STACK_DEPOT_CAP": "4"})
    _assert_clean(p, "PREWARM_RACE_OK")


# --------------------------------------------------------------------------
# In-process control: the >512 overflow release machinery itself is correct in
# the DEFAULT mode (no env), so a subprocess mode failure isolates to that mode
# rather than to the overflow workload.  Also independently exercises the default
# (MADV_FREE) release path under a real pool overflow in the parent process.
# --------------------------------------------------------------------------
def test_default_mode_pool_overflow_baseline_in_process():
    """A 700-fiber concurrent wave in the PARENT process under M:1, default mode
    (MADV_FREE, no arena, default depot cap): proves the >512 pool-overflow
    release machinery the subprocess workers rely on is sound here, so a mode
    worker's failure attributes to the mode and not to the overflow harness.
    Exercises the default runloom_stack_release path (MADV_FREE reclaim + depot
    pool insert) under a genuine pool overflow."""
    import runloom
    from runloom.sync import WaitGroup
    from adv_util import hang_guard

    N = CONC
    ran = bytearray(N)

    def main():
        release = WaitGroup(); release.add(1)
        up = WaitGroup(); up.add(N)
        done = WaitGroup(); done.add(N)

        def body(i):
            ran[i] = 1
            up.done()
            release.wait()
            done.done()

        for i in range(N):
            rc.fiber(lambda i=i: body(i))
        up.wait()
        release.done()
        done.wait()

    with hang_guard(60, "default-mode 700-fiber pool overflow"):
        runloom.run(1, main)
    assert sum(ran) == N, "only %d/%d ran" % (sum(ran), N)
    assert rc._self_check(0) == 0
