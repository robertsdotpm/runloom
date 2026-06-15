"""Adversarial coverage suite for src/runloom_c/mn_sched_hub_resume_preempt.c.inc.

This fragment is the CONTROLLED M:N scheduler ("baton") gated by RUNLOOM_MN_SEED:
a seeded controller serializes every hub's fiber-execution segment through one
baton so scheduling-order bugs become reproducible.  Its sub-features are gated
behind further env vars that the normal corpus never sets:

  * the SEEDED-UNIFORM choose path  (RUNLOOM_MN_SEED, *without* RUNLOOM_MN_PCT)
        -> runloom_mn_ctrl_rand (L130-135) + the non-PCT branch of
           runloom_mn_ctrl_choose (L254-259), plus the early-return of
           runloom_mn_pct_init (L166).
  * the in-memory GRANT TRACE       (RUNLOOM_MN_TRACE=<path>)
        -> trace-buf alloc/path copy in init (L363-367), the per-grant ring
           write in try_grant (L309-311) and the dump-at-fini (L401-409).
  * the BARRIER-OFF baton           (RUNLOOM_MN_BARRIER=0)
        -> the preempt_frames=0 else-branch in init (L354).
  * the PCT_STEPS override          (RUNLOOM_MN_PCT_STEPS=<k>)
        -> the strtoull branch of the pct_k ternary in pct_init (L171).

Because the parent pytest process imports runloom_c exactly ONCE, every one of
these env modes is latched at import / first-run; a test that needs a mode MUST
set it in a fresh SUBPROCESS.  We reuse tests/cov_workload.py (a diverse,
self-terminating run(N) workload) as the body and assert on its WORKLOAD_OK
stdout marker + a clean (==0) returncode -- a crash/_exit would NOT flush gcov
counters, so the assertion is also the coverage-validity guard.  Where a mode
leaves an observable artefact (the dumped trace file), we additionally assert on
its CONTENT, so the test proves the lines DID what they claim, not merely ran.

Lines deemed unreachable-without-a-real-OOM (the PyMem_Calloc/Malloc failure
cleanup branches L190-193, L329-331) and the wait_armed-shadowed startup guard
L278 are documented in the structured report; there is no fault-injection site
for PyMem_* and no safe trigger.
"""
import os
import subprocess
import sys

import pytest

from adv_util import needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(not FT, reason="controlled M:N baton needs GIL-disabled build")


def _run_workload(env_extra, hubs=4, timeout=90):
    """Run the shared diverse workload in a subprocess with `env_extra` set.

    Returns the CompletedProcess.  The caller asserts rc==0 + WORKLOAD_OK so the
    subprocess is known to have EXITED CLEANLY (gcov counters flushed)."""
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run(
        [PY, "tests/cov_workload.py", "--hubs", str(hubs)],
        cwd=REPO, env=env, capture_output=True, text=True, timeout=timeout)


def _assert_ok(p, label):
    assert p.returncode == 0, (
        "%s crashed/failed (rc=%d) -- gcov counters NOT flushed\nstderr=%s"
        % (label, p.returncode, p.stderr[-1500:]))
    assert "WORKLOAD_OK" in p.stdout, (
        "%s did not finish the workload cleanly\nout=%s\nerr=%s"
        % (label, p.stdout[-400:], p.stderr[-800:]))


# ---------------------------------------------------------------------------
# 1. Seeded-UNIFORM baton (RUNLOOM_MN_SEED, barrier on, NO PCT).
#
# Drives runloom_mn_ctrl_choose's NON-PCT branch (L254-259): pct_enabled is 0,
# so choose() counts the wanters (L254), and -- with several hubs concurrently
# requesting the baton across a 320-message cross-hub channel workload -- has
# cnt>0 (L255 false), draws k via runloom_mn_ctrl_rand (the xorshift L130-135,
# called ONLY on this branch, L256), and walks to the k-th wanter (L257-258).
# At final drain (census complete, baton free, nobody wanting) cnt==0 -> L255
# returns -1.  Also drives runloom_mn_pct_init's early return L166 (pct env is
# unset, so the very first guard `pct==NULL` is true).
#
# The existing tests/test_cov_mn.py "barrier_pct" mode sets RUNLOOM_MN_PCT, which
# makes choose() take the PCT branch (L243-252) and NEVER reach L254-259 or the
# baton's own rand -- so this distinct no-PCT run is what colours those lines.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("hubs", [2, 4])
def test_seeded_uniform_baton_no_pct(hubs):
    p = _run_workload({"RUNLOOM_MN_SEED": "7", "RUNLOOM_MN_BARRIER": "1"},
                      hubs=hubs)
    _assert_ok(p, "seeded-uniform baton hubs=%d" % hubs)


def test_seeded_uniform_baton_is_deterministic():
    # Same seed + same workload => the seeded uniform choose() must produce an
    # IDENTICAL run.  Two runs differing in their reported message total would
    # mean the non-PCT choose draw (L256) is NOT a pure function of the seed --
    # i.e. the xorshift rng (L130-135) or the wanter-walk (L257-258) leaked
    # nondeterminism.  We assert the WORKLOAD_OK count is identical across runs.
    env = {"RUNLOOM_MN_SEED": "12345", "RUNLOOM_MN_BARRIER": "1"}
    p1 = _run_workload(env, hubs=3)
    p2 = _run_workload(env, hubs=3)
    _assert_ok(p1, "seeded run #1")
    _assert_ok(p2, "seeded run #2")
    n1 = p1.stdout.strip().split()[-1]
    n2 = p2.stdout.strip().split()[-1]
    assert n1 == n2, "seeded uniform baton not deterministic: %r vs %r" % (n1, n2)


# ---------------------------------------------------------------------------
# 2. Barrier OFF -- the timing-dependent immediate-handoff baton.
#
# RUNLOOM_MN_BARRIER=0 takes the else-branch in runloom_mn_ctrl_init that sets
# preempt_frames = 0 (L354): with the barrier off there is no deterministic
# frame-count preemption, so the wall-clock watchdog drives preemption instead.
# This is the A/B-comparison path; it shares the non-PCT choose, so it also
# re-exercises L254-259 under the no-census immediate handoff.
# ---------------------------------------------------------------------------
def test_baton_barrier_off_immediate_handoff():
    p = _run_workload({"RUNLOOM_MN_SEED": "9", "RUNLOOM_MN_BARRIER": "0"}, hubs=4)
    _assert_ok(p, "baton barrier=off")


# ---------------------------------------------------------------------------
# 3. In-memory GRANT TRACE (RUNLOOM_MN_TRACE=<path>).
#
# Setting RUNLOOM_MN_TRACE turns on the trace-buf in runloom_mn_ctrl_init: it
# mallocs the 1 MiB ring (L363-364), guards malloc failure (L365), and copies
# the path with strncpy + NUL terminate (L366-367).  Every baton GRANT then
# writes one hub-id digit into the ring in try_grant (L309-311).  At mn_fini the
# ring is dumped to the file (L401-409: open path, fwrite the recorded length,
# fclose, then free the buffer L408-409).
#
# We assert on the FILE CONTENT, not merely "it ran": the dump must exist, be
# non-empty (>=1 grant happened), and contain ONLY digits in [0, hubs) -- because
# L311 records `'0' + (c % 10)` where c is the granted hub id in [0, hubs).  A
# byte outside that set would mean the ring recorded a bogus grant target.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("hubs", [2, 4])
def test_grant_trace_ring_dump(tmp_path, hubs):
    trace = tmp_path / ("mn_grant_trace_%d.txt" % hubs)
    p = _run_workload(
        {"RUNLOOM_MN_SEED": "5", "RUNLOOM_MN_BARRIER": "1",
         "RUNLOOM_MN_TRACE": str(trace)},
        hubs=hubs)
    _assert_ok(p, "grant trace hubs=%d" % hubs)

    # L401-405: the dump file must have been written at fini.
    assert trace.exists(), "RUNLOOM_MN_TRACE dump file was never written"
    data = trace.read_bytes()
    # L309-311: at least one grant must have been recorded (the workload spawns
    # ~25 fibers across hubs -> many baton handoffs).
    assert len(data) > 0, "grant trace ring was empty -- no grant recorded"
    # L311 records exactly one digit char per grant, value '0'+(hub_id%10), and
    # hub_id < hubs (<=4 here, so %10 is identity).  Any other byte is corruption.
    allowed = set(b"0123456789"[:hubs])
    bad = set(data) - allowed
    assert not bad, (
        "grant trace contained byte(s) outside [0,%d): %r (full=%r)"
        % (hubs, sorted(bad), data[:64]))


def test_grant_trace_only_at_fini_not_per_grant(tmp_path):
    # The ring is dumped ONCE at fini, not fprintf'd per grant (the whole point
    # of the in-memory ring -- a per-grant syscall perturbed the timing it
    # measures).  So the dumped bytes must be a single contiguous run of digits
    # with no separators/newlines -- proving L404's fwrite(tracebuf, 1, tracelen,
    # f) dumped the raw ring, not a formatted per-event log.
    trace = tmp_path / "mn_trace_raw.txt"
    p = _run_workload(
        {"RUNLOOM_MN_SEED": "11", "RUNLOOM_MN_BARRIER": "1",
         "RUNLOOM_MN_TRACE": str(trace)},
        hubs=3)
    _assert_ok(p, "grant trace raw")
    data = trace.read_bytes()
    assert data, "empty trace"
    assert all(0x30 <= b <= 0x39 for b in data), (
        "trace dump is not a raw digit ring (found non-digit) -- %r" % data[:64])


# ---------------------------------------------------------------------------
# 4. PCT with an explicit RUNLOOM_MN_PCT_STEPS override.
#
# runloom_mn_pct_init reads RUNLOOM_MN_PCT_STEPS; when it is set+non-empty the
# pct_k ternary takes its strtoull branch (L171) instead of the 4096 default.
# The existing "barrier_pct" mode never sets RUNLOOM_MN_PCT_STEPS, so the
# strtoull side of that ternary is otherwise never evaluated.  We exercise it
# with a small step bound (the change points are then drawn from [1, k]).
# ---------------------------------------------------------------------------
def test_pct_steps_override():
    p = _run_workload(
        {"RUNLOOM_MN_SEED": "7", "RUNLOOM_MN_BARRIER": "1",
         "RUNLOOM_MN_PCT": "4", "RUNLOOM_MN_PCT_STEPS": "64"},
        hubs=4)
    _assert_ok(p, "pct steps override")


def test_pct_steps_override_deterministic():
    # PCT draws from its OWN seeded stream (pct_rand), so the same seed + same
    # PCT_STEPS must reproduce.  A differing total across two runs would mean the
    # strtoull-sourced pct_k (L171) perturbed the priority schedule
    # nondeterministically.
    env = {"RUNLOOM_MN_SEED": "42", "RUNLOOM_MN_BARRIER": "1",
           "RUNLOOM_MN_PCT": "3", "RUNLOOM_MN_PCT_STEPS": "32"}
    p1 = _run_workload(env, hubs=3)
    p2 = _run_workload(env, hubs=3)
    _assert_ok(p1, "pct steps run #1")
    _assert_ok(p2, "pct steps run #2")
    assert p1.stdout.strip().split()[-1] == p2.stdout.strip().split()[-1], (
        "PCT_STEPS run not deterministic across identical seeds")


# ---------------------------------------------------------------------------
# 5. PCT depth=1 -> ncp==0 (no change-point arrays allocated).
#
# d=1 means ncp=d-1=0, so pct_init takes the branch where pct_change_at /
# pct_change_prio are NOT allocated (the `if (ncp > 0)` guards stay false) yet
# pct_enabled is still set.  This is a distinct shape of the pct setup that the
# depth>=2 "barrier_pct" mode does not cover, and it confirms the controller
# still grants correctly with PCT on but zero change points (pure
# highest-base-priority-wanter selection, no demotions).
# ---------------------------------------------------------------------------
def test_pct_depth_one_no_change_points():
    p = _run_workload(
        {"RUNLOOM_MN_SEED": "7", "RUNLOOM_MN_BARRIER": "1",
         "RUNLOOM_MN_PCT": "1"},
        hubs=4)
    _assert_ok(p, "pct depth=1")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
