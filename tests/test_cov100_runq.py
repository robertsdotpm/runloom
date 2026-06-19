"""Adversarial coverage suite for src/runloom_c/mn_sched_runq.c.inc.

WHAT THIS FRAGMENT IS
---------------------
mn_sched_runq.c.inc holds the *global stealable run-queue* (push/pull) plus the
migratable-mode interlock that decides whether that queue is used at all:

  * runloom_mn_global_runq_push  (L108-122)  -- uncovered
  * runloom_mn_global_runq_pull  (L127-148, body L137-148) -- uncovered
  * runloom_per_g_tstate_flag / runloom_steal_woken_flag /
    runloom_unsafe_migration_acked / runloom_resolve_migratable_mode /
    runloom_use_global_runq  (the interlock)

REACHABILITY OF THE UNCOVERED LINES (push/pull)
-----------------------------------------------
Every call site of runloom_mn_global_runq_push (mn_api.c.inc:224, the
sweep-release at mn_api.c.inc:310, hub_main.c.inc:926/1166) and the sole call
site of runloom_mn_global_runq_pull (hub_main.c.inc:478) is gated behind
`runloom_use_global_runq()`, which returns `runloom_get_per_g_tstate_mode()`.

That mode is set in exactly ONE place -- mn_sched_init_fini.c.inc:66:
    runloom_set_per_g_tstate_mode(runloom_resolve_migratable_mode());
and runloom_resolve_migratable_mode() (this fragment, L232-244) returns 1 ONLY
when a migratable flag is requested AND runloom_unsafe_migration_acked() is true,
i.e. ONLY when RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 is in the environment.

RUNLOOM_ALLOW_UNSAFE_MIGRATION is a hard-forbidden knob for this QA work: per-g
tstate / steal-woken are KNOWN-CRASH migration modes (a per-g PyThreadState's
mimalloc heap migrates across hub OS threads -> SEGV under churn at H>=2). A
crashing subprocess does NOT flush gcov counters anyway, so even setting it would
not legitimately *cover* the lines. There is no safe trigger: RUNLOOM_PER_G_TSTATE
(or RUNLOOM_STEAL_WOKEN) WITHOUT the ack is gated OFF -- the runtime warns and
runs the default per-hub-tstate scheduler, which routes woken gs through
runloom_mn_hub_submit and NEVER touches push/pull. (Confirmed empirically below:
the warn fires and a cross-hub channel wake is still delivered correctly.)

So the push/pull bodies (L108-122, L137-148) are classified UNREACHABLE for this
suite -- see the structured `unreachable` report.

WHAT THIS SUITE *DOES* ASSERT (real behavior, this fragment's interlock)
------------------------------------------------------------------------
The load-bearing safety property of this file is the interlock: a migratable-mode
request that lacks the unsafe ack must (a) warn once and (b) fall back to the
default scheduler so that runloom_use_global_runq() stays FALSE and the global
runq is bypassed entirely -- woken gs go via hub_submit, not push/pull. These
tests drive runloom_resolve_migratable_mode()'s gated-off branch (L237-243) for
BOTH flags and assert the resulting default-scheduler behavior is correct,
proving the global runq is NOT engaged.

Each subprocess sets the mode env (read once at C import/init time), runs a real
cross-hub channel + cross-hub fd-park workload to completion, exits 0, and prints
a marker -- so gcov counters flush and we assert on stdout + returncode + the
warn line, never on a crash.
"""
import os
import subprocess
import sys

import pytest

from adv_util import hang_guard, needs_free_threading

FT = needs_free_threading()
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

# The exact, stable substring the gated-off warn prints (mn_sched_runq.c.inc
# L237-243). If the interlock ever silently flipped to ON, this line would
# vanish AND the workload would crash -- either way the test fails loudly.
_WARN_NEEDLE = "GATED OFF"
_ACK_HINT = "RUNLOOM_ALLOW_UNSAFE_MIGRATION=1 to enable anyway"


# A self-contained child program. It runs a workload that, under the *default*
# scheduler, exercises the very wakeups that WOULD route through the global runq
# push/pull if per-g-tstate were active:
#   * a cross-hub unbuffered channel rendezvous (consumer parks on hub A, sender
#     on hub B wakes it -> runloom_mn_wake_g),
#   * a cross-hub socketpair fd park (reader parks in netpoll on one hub, writer
#     on another wakes it).
# It asserts every value/byte arrived (no lost/dup/stranded wake) and prints
# CHILD_OK. The parent asserts the warn fired (gated-off branch taken) AND the
# work completed -- i.e. the default path carried the wakes, the global runq did
# not. NEVER sets RUNLOOM_ALLOW_UNSAFE_MIGRATION.
_CHILD = r'''
import os, sys, socket
sys.path.insert(0, "src")
import runloom
import runloom_c as rc
from runloom.sync import WaitGroup

HUBS = int(sys.argv[1])
PAIRS = 64

# Per-consumer single-writer slots: consumer i is the ONLY writer of recv_val[i]
# (race-free with the GIL off). We verify the MULTISET of received values equals
# {0..PAIRS-1} -- i.e. every sent value was delivered exactly once, none lost or
# duplicated -- rather than pairing-by-index (an unbuffered chan pairs sends and
# recvs in arbitrary order, so consumer i need not get value i).
recv_val = [None] * PAIRS
recv_ok = bytearray(PAIRS)
fd_got = bytearray(PAIRS)

def main():
    wg = WaitGroup()

    # --- cross-hub channel rendezvous storm ---
    ch = rc.Chan(0)           # unbuffered: forces a real park+wake handoff
    wg.add(PAIRS)
    def consumer(i):
        try:
            v, ok = ch.recv()
            recv_val[i] = v       # sole writer of slot i
            recv_ok[i] = 1 if ok else 0
        finally:
            wg.done()
    for i in range(PAIRS):
        rc.mn_fiber(lambda i=i: consumer(i))
    # senders on (potentially) other hubs wake the parked consumers
    for i in range(PAIRS):
        rc.mn_fiber(lambda i=i: ch.send(i))
    wg.wait()

    # --- cross-hub fd park/wake storm (netpoll wake path) ---
    wg2 = WaitGroup()
    wg2.add(PAIRS)
    socks = []
    def fd_pair(i):
        a, b = socket.socketpair()
        a.setblocking(False); b.setblocking(False)
        socks.append((a, b))
        def reader():
            try:
                buf = bytearray(1)
                rc.tcp_recv(a.fileno(), buf, 1)   # park in netpoll
                if buf[0] == (i & 0x7f):
                    fd_got[i] = 1
            finally:
                rc.netpoll_unregister(a.fileno())
                wg2.done()
        def writer():
            rc.sched_yield()
            rc.tcp_send(b.fileno(), bytes([i & 0x7f]))
        rc.mn_fiber(reader)
        rc.mn_fiber(writer)
    for i in range(PAIRS):
        fd_pair(i)
    wg2.wait()
    for a, b in socks:
        try: rc.netpoll_unregister(b.fileno())
        except Exception: pass
        a.close(); b.close()

runloom.run(HUBS, main)

assert sum(recv_ok) == PAIRS, "channel recv not ok: %d/%d" % (sum(recv_ok), PAIRS)
assert sorted(recv_val) == list(range(PAIRS)), (
    "channel wake lost/dup: got multiset %s" % sorted(recv_val))
assert sum(fd_got) == PAIRS, "lost fd wake(s): %d/%d" % (sum(fd_got), PAIRS)
print("CHILD_OK", sum(recv_ok), sum(fd_got))
'''


def _run_child(env_extra, hubs, timeout=60):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src", **env_extra)
    return subprocess.run(
        [PY, "-c", _CHILD, str(hubs)],
        cwd=REPO, env=env, capture_output=True, text=True, timeout=timeout)


# --------------------------------------------------------------------------
# 1. RUNLOOM_PER_G_TSTATE requested WITHOUT the unsafe ack.
#    Drives runloom_resolve_migratable_mode()'s gated-off branch (L234-243):
#    runloom_per_g_tstate_flag()==1, runloom_unsafe_migration_acked()==0 ->
#    warn + return 0 -> per_g_tstate_mode stays 0 -> runloom_use_global_runq()
#    FALSE -> the wakes route through hub_submit, not the global runq.
#    Asserts: warn fired AND the cross-hub workload completed (default path
#    carried every wake). Multi-hub so cross-hub wake_g is genuinely exercised.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_per_g_tstate_gated_off_uses_default_sched():
    with hang_guard(70, "per_g_tstate gated-off"):
        p = _run_child({"RUNLOOM_PER_G_TSTATE": "1"}, hubs=4)
    assert p.returncode == 0, (
        "gated-off per-g-tstate child crashed (rc=%d) -- the interlock must run "
        "the DEFAULT scheduler, never the known-crash migration mode.\nstderr=%s"
        % (p.returncode, p.stderr[-2000:]))
    assert _WARN_NEEDLE in p.stderr and _ACK_HINT in p.stderr, (
        "resolve_migratable_mode did NOT take the gated-off warn branch "
        "(L237-243); a silent flip to per-g-tstate would engage the global runq."
        "\nstderr=%s" % p.stderr[-2000:])
    assert "CHILD_OK" in p.stdout, (
        "default scheduler did not deliver every cross-hub wake "
        "(channel + fd) -> work stranded.\nout=%s\nerr=%s"
        % (p.stdout, p.stderr[-1200:]))


# --------------------------------------------------------------------------
# 2. RUNLOOM_STEAL_WOKEN requested WITHOUT the unsafe ack.
#    Same gated-off branch via the OTHER flag: runloom_steal_woken_flag()==1
#    feeds the `want` in runloom_resolve_migratable_mode (L234). Proves the
#    redirect (steal-woken -> per-g-tstate) is ALSO gated, so RUNLOOM_STEAL_WOKEN
#    never reaches the unsound snap branch and never engages push/pull.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_steal_woken_gated_off_uses_default_sched():
    with hang_guard(70, "steal_woken gated-off"):
        p = _run_child({"RUNLOOM_STEAL_WOKEN": "1"}, hubs=4)
    assert p.returncode == 0, (
        "gated-off steal-woken child crashed (rc=%d).\nstderr=%s"
        % (p.returncode, p.stderr[-2000:]))
    assert _WARN_NEEDLE in p.stderr and _ACK_HINT in p.stderr, (
        "resolve_migratable_mode did NOT warn for RUNLOOM_STEAL_WOKEN "
        "(its flag must feed the same gated-off branch).\nstderr=%s"
        % p.stderr[-2000:])
    assert "CHILD_OK" in p.stdout, (
        "default scheduler did not deliver every cross-hub wake under "
        "steal-woken.\nout=%s\nerr=%s" % (p.stdout, p.stderr[-1200:]))


# --------------------------------------------------------------------------
# 3. BOTH migratable flags + a benign falsy RUNLOOM_ALLOW_UNSAFE_MIGRATION="0".
#    runloom_unsafe_migration_acked (L213-223) treats e[0]=='0' as NOT acked, so
#    the interlock STILL gates off. This asserts the ack parser rejects "0"
#    (a real adversarial input: a user who set the var to "0" must NOT trip the
#    known-crash mode) -> warn fires, default sched runs.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_unsafe_ack_zero_is_not_acked():
    with hang_guard(70, " unsafe-ack=0 gated-off"):
        p = _run_child(
            {"RUNLOOM_PER_G_TSTATE": "1", "RUNLOOM_ALLOW_UNSAFE_MIGRATION": "0"},
            hubs=4)
    assert p.returncode == 0, (
        "ack='0' child crashed (rc=%d) -- '0' must be read as NOT acked, "
        "keeping the default scheduler.\nstderr=%s"
        % (p.returncode, p.stderr[-2000:]))
    assert _WARN_NEEDLE in p.stderr, (
        "RUNLOOM_ALLOW_UNSAFE_MIGRATION='0' was wrongly treated as acked: the "
        "gated-off warn did not fire -> the known-crash migration mode would "
        "have engaged.\nstderr=%s" % p.stderr[-2000:])
    assert "CHILD_OK" in p.stdout, (
        "default sched did not finish the workload with ack='0'.\nout=%s\nerr=%s"
        % (p.stdout, p.stderr[-1200:]))


# --------------------------------------------------------------------------
# 4. Neither flag set: no warn, default scheduler, work completes.
#    Negative control -- runloom_resolve_migratable_mode returns 0 at L235
#    (`if (!want) return 0;`) WITHOUT printing the warn. Confirms the warn is
#    specifically the gated-off-request signal, not unconditional noise, and
#    that the same cross-hub workload is correct on the plain default path.
# --------------------------------------------------------------------------
@pytest.mark.skipif(not FT, reason="M:N needs GIL-disabled build")
def test_no_flag_no_warn_default_sched():
    with hang_guard(70, "no-flag default"):
        p = _run_child({}, hubs=4)
    assert p.returncode == 0, (
        "plain default child crashed (rc=%d).\nstderr=%s"
        % (p.returncode, p.stderr[-2000:]))
    assert _WARN_NEEDLE not in p.stderr, (
        "the gated-off warn fired with NO migratable flag set -- the `!want` "
        "early-return (L235) must precede the warn.\nstderr=%s"
        % p.stderr[-2000:])
    assert "CHILD_OK" in p.stdout, (
        "plain default sched did not finish the workload.\nout=%s\nerr=%s"
        % (p.stdout, p.stderr[-1200:]))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
