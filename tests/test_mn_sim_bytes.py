"""MN_SIM_DST_PLAN.md I2 -- the sim byte plane, native on the M:N scheduler.

Under RUNLOOM_SIM + RUNLOOM_SIM_MN + RUNLOOM_MN_SEED the netpoll pump no-ops
for hubs (bounded detached nap) and the ready ledger is dispatched by the
quiescent census (try_grant c<0): dispatch-before-advance, ledger total order,
wakes via the standard claim/unlink/mn_wake_g primitive, cross-hub interleave
by the seeded choose().  wait_fd under this mode enforces the sim conn
registry (unregistered fds have no wake source -- probe P4's silent collapse
is now a loud error) and rejects finite timeouts until I4.

Env is per-subprocess (mn_digest.hermetic_env); assertions on printed output.
"""
import hashlib
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tools", "dst"))

import mn_digest  # noqa: E402

from adv_util import needs_free_threading  # noqa: E402

pytestmark = pytest.mark.skipif(
    not needs_free_threading(),
    reason="the M:N scheduler is only real on free-threaded builds")

SIM_ENV = {"RUNLOOM_SIM": "1", "RUNLOOM_SIM_MN": "1"}


def run_sim(code, seed=12345, extra=None, timeout=60):
    env_extra = dict(SIM_ENV)
    env_extra["RUNLOOM_MN_SEED"] = str(seed)
    if extra:
        env_extra.update(extra)
    env = mn_digest.hermetic_env(env_extra)
    return subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                          timeout=timeout, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)


# A multi-conn byte workload whose printed FULL event trace is the digest
# surface: K socketpair conns (sender + receiver fibers each) plus pure-yield
# mixer fibers.  The mixers matter: round-robin placement puts all receivers
# on one hub and all senders on the other, and the rendezvous structure then
# fully constrains the schedule (zero choose() freedom -> seed-INsensitive,
# found empirically); the yielders keep both hubs wanting the baton so the
# seeded cross-hub interleave is actually exercised.
BYTES_WORKLOAD = r"""
import socket, runloom_c as rc
K = 6
order = []
conns = []
for k in range(K):
    a, b = socket.socketpair()
    a.setblocking(False); b.setblocking(False)
    cid = rc.sim_conn_register(a.fileno(), b.fileno())
    conns.append((a, b, cid))
def sender(k, a, b, cid):
    def w():
        for j in range(3):
            order.append(("s", k, j))
            a.send(bytes([65 + k]) * (j + 1))
            rc.sim_deliver_ready(cid, b.fileno(), 1)
            rc.sched_yield()
    return w
def receiver(k, a, b, cid):
    def w():
        got = b""
        while len(got) < 1 + 2 + 3:
            rc.wait_fd(b.fileno(), 1)
            try:
                got += b.recv(64)
            except BlockingIOError:
                pass
        order.append(("r", k, got.decode()))
    return w
def yielder(y):
    def w():
        for j in range(4):
            order.append(("y", y, j))
            rc.sched_yield()
    return w
rc.mn_init(2)
for k, (a, b, cid) in enumerate(conns):
    rc.mn_fiber(receiver(k, a, b, cid))
    rc.mn_fiber(sender(k, a, b, cid))
for y in range(4):
    rc.mn_fiber(yielder(y))
rc.mn_run()
recvs = [(k, payload) for tag, k, payload in
         (e for e in order if e[0] == "r")]
ok = (len(recvs) == K and
      all(payload == chr(65 + k) * 6 for k, payload in recvs))
print("BYTES", "OK" if ok else "BAD", sorted(recvs))
print("ORDER_SIG", order)
rc.mn_fini()
"""


class TestMnSimBytes:
    def test_p4_scenario_fixed(self):
        """The probe-P4 corruption, healed: a receiver parked on wait_fd at
        H=2 is woken by the census-dispatched ledger with the right mask and
        the exact bytes (not a silent timeout-collapse)."""
        p = run_sim(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "got = {}\n"
            "def sender():\n"
            "    a.send(b'hello-mn-sim')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "def receiver():\n"
            "    got['mask'] = rc.wait_fd(b.fileno(), 1)\n"
            "    got['data'] = b.recv(64)\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(receiver)\n"
            "rc.mn_fiber(sender)\n"
            "rc.mn_run(); rc.mn_fini()\n"
            "print('P4', got.get('mask'), got.get('data'))\n")
        assert "P4 1 b'hello-mn-sim'" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_byte_plane_digest_deterministic(self):
        """Same (seed, H=2) x3: bit-identical completion order across the
        multi-conn byte workload; a different seed differs."""
        def digest(seed):
            p = run_sim(BYTES_WORKLOAD, seed=seed)
            assert "BYTES OK" in p.stdout, (p.stdout, p.stderr[-1200:])
            sig = [ln for ln in p.stdout.splitlines()
                   if ln.startswith("ORDER_SIG")][0]
            return hashlib.md5(sig.encode()).hexdigest()

        runs = [digest(12345) for i in range(3)]
        assert len(set(runs)) == 1, "same-seed byte digests diverged: {0}".format(runs)
        assert digest(999) != runs[0], "seed-insensitive byte schedule"

    def test_self_wake_corner_h1(self):
        """mn_init(1): the dispatching hub owns the woken parker (the one
        unsimulated corner in the design trace) -- must complete, not wedge."""
        p = run_sim(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "got = {}\n"
            "def receiver():\n"
            "    rc.wait_fd(b.fileno(), 1)\n"
            "    got['data'] = b.recv(64)\n"
            "def sender():\n"
            "    a.send(b'self-wake')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "rc.mn_init(1)\n"
            "rc.mn_fiber(receiver)\n"
            "rc.mn_fiber(sender)\n"
            "rc.mn_run(); rc.mn_fini()\n"
            "print('SELFWAKE', got.get('data'))\n")
        assert "SELFWAKE b'self-wake'" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_delayed_delivery_clock_compression(self):
        """A shuttler sleeps 50ms logical then delivers: the receiver's bytes
        land at logical 50ms with wall time far below 50s-scale -- and the
        order vs a competing same-deadline sleeper is census-decided and
        stable across runs."""
        code = (
            "import socket, time, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "order = []\n"
            "def shuttler():\n"
            "    rc.sched_sleep(0.05)\n"
            "    a.send(b'late')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "def receiver():\n"
            "    rc.wait_fd(b.fileno(), 1)\n"
            "    b.recv(16)\n"
            "    order.append(('recv', rc._logical_ns()))\n"
            "def sleeper():\n"
            "    rc.sched_sleep(0.05)\n"
            "    order.append(('sleep', rc._logical_ns()))\n"
            "t0 = time.monotonic()\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(receiver)\n"
            "rc.mn_fiber(shuttler)\n"
            "rc.mn_fiber(sleeper)\n"
            "rc.mn_run(); rc.mn_fini()\n"
            "wall = time.monotonic() - t0\n"
            "print('DELAYED', order, 'WALL_OK', wall < 5.0)\n")
        outs = set()
        for i in range(3):
            p = run_sim(code)
            assert "WALL_OK True" in p.stdout, (p.stdout, p.stderr[-800:])
            line = [ln for ln in p.stdout.splitlines()
                    if ln.startswith("DELAYED")][0]
            assert "50000000" in line, line     # both events at logical 50ms
            outs.add(line)
        assert len(outs) == 1, "delayed-delivery order unstable: {0}".format(outs)

    def test_unregistered_fd_raises(self):
        """gate [11a]: wait_fd on an fd outside the sim registry raises
        (EPERM) instead of parking forever."""
        p = run_sim(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "def w():\n"
            "    try:\n"
            "        rc.wait_fd(b.fileno(), 1)\n"
            "        print('NO_RAISE')\n"
            "    except OSError as e:\n"
            "        print('UNREG_RAISED', e.errno)\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(w)\n"
            "rc.mn_run(); rc.mn_fini()\n")
        assert "UNREG_RAISED" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_finite_timeout_works_since_i4(self):
        """gate [11b] RETIRED by I4: a finite wait_fd timeout now fires on the
        logical clock (was: EINVAL between I2 and I4 -- the temporary guard
        against the silent parked-forever wedge)."""
        p = run_sim(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "def w():\n"
            "    m = rc.wait_fd(b.fileno(), 1, 100)\n"
            "    print('TIMEOUT_WORKS', m, rc._logical_ns())\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(w)\n"
            "rc.mn_run(); rc.mn_fini()\n")
        assert "TIMEOUT_WORKS 0 100000000" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_stw_churn_under_gated_pump(self):
        """gate [1] STW-safety: hubs sitting in the gated pump's detached nap
        must not wedge a stop-the-world -- gc churn + collections from fibers
        while a sim conn is mid-traffic completes cleanly."""
        p = run_sim(
            "import gc, socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "done = []\n"
            "def receiver():\n"
            "    rc.wait_fd(b.fileno(), 1)\n"
            "    done.append(b.recv(16))\n"
            "def churner():\n"
            "    for i in range(60):\n"
            "        junk = [bytearray(64) for j in range(50)]\n"
            "        junk.append(junk)          # cycle\n"
            "        del junk\n"
            "        if i % 10 == 0:\n"
            "            gc.collect()\n"
            "        rc.sched_yield()\n"
            "    a.send(b'after-churn')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(receiver)\n"
            "rc.mn_fiber(churner)\n"
            "rc.mn_run(); rc.mn_fini()\n"
            "print('STW_OK', done)\n")
        assert "STW_OK [b'after-churn']" in p.stdout, (p.stdout, p.stderr[-800:])


class TestTimedParksI4:
    def test_wait_fd_timeout_fires_at_logical_deadline(self):
        """I4 plane (a): a 5000ms wait_fd timeout with NO delivery returns
        mask 0 at EXACTLY logical 5e9 ns, wall-compressed -- the full P4
        regression healed end-to-end (the I2 EINVAL guard is retired).  Order
        vs a 4999ms sleeper is deterministic across runs."""
        code = (
            "import socket, time, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "order = []\n"
            "def waiter():\n"
            "    m = rc.wait_fd(b.fileno(), 1, 5000)\n"
            "    order.append(('timeout', m, rc._logical_ns()))\n"
            "def sleeper():\n"
            "    rc.sched_sleep(4.999)\n"
            "    order.append(('sleeper', rc._logical_ns()))\n"
            "t0 = time.monotonic()\n"
            "rc.mn_init(2); rc.mn_fiber(waiter); rc.mn_fiber(sleeper)\n"
            "rc.mn_run()\n"
            "print('T2', order, 'WALL_OK', time.monotonic() - t0 < 3.0)\n"
            "rc.mn_fini()\n")
        outs = set()
        for i in range(3):
            p = run_sim(code)
            assert "WALL_OK True" in p.stdout, (p.stdout, p.stderr[-800:])
            assert p.returncode == 0, (p.stdout, p.stderr[-800:])
            line = [ln for ln in p.stdout.splitlines() if ln.startswith("T2")][0]
            assert "('timeout', 0, 5000000000)" in line, line
            assert "('sleeper', 4999000000)" in line, line
            outs.add(line)
        assert len(outs) == 1, "timeout-vs-sleeper order unstable: {0}".format(outs)

    def test_timeout_vs_post_advance_delivery(self):
        """I4 (review-corrected): a shuttler whose SEND is unlocked by the same
        census advance that makes the wait_fd deadline due -- the TIMEOUT wins
        deterministically (at the advance instant the ledger is still empty;
        the shuttler runs only after; its delivery lands as a stashed wake on
        a dead parker).  Pinned explicitly: 'TIE 0 None'."""
        code = (
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "out = {}\n"
            "def waiter():\n"
            "    m = rc.wait_fd(b.fileno(), 1, 50)\n"     # deadline: 50ms
            "    out['mask'] = m\n"
            "    out['data'] = b.recv(16) if m == 1 else None\n"
            "def shuttler():\n"
            "    rc.sched_sleep(0.05)\n"                   # same logical instant
            "    a.send(b'tie')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "rc.mn_init(2); rc.mn_fiber(waiter); rc.mn_fiber(shuttler)\n"
            "rc.mn_run()\n"
            "print('TIE', out.get('mask'), out.get('data'))\n"
            "rc.mn_fini()\n")
        for i in range(3):
            p = run_sim(code)
            assert p.returncode == 0, (p.stdout, p.stderr[-800:])
            assert "TIE 0 None" in p.stdout, \
                ("timeout must win the post-advance-enqueue race", p.stdout)

    def test_true_tie_ready_beats_timeout(self):
        """I4 t3, the REAL pin (review fix): a ledger entry DUE at the same
        instant as the wait_fd deadline -- constructed via timeout 0 + a
        same-instant delivery already in the ledger.  Census step 1 runs
        dispatch_due BEFORE drain_expired, so READY wins: mask 1 + payload.
        Swapping that order would flip this to a timeout -- the load-bearing
        ordering finally has teeth."""
        code = (
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "out = {}\n"
            "def sender():\n"
            "    a.send(b'tie')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"   # due at logical 0
            "def waiter():\n"
            "    m = rc.wait_fd(b.fileno(), 1, 0)\n"           # deadline logical 0
            "    out['mask'] = m\n"
            "    out['data'] = b.recv(16) if m == 1 else None\n"
            "rc.mn_init(2); rc.mn_fiber(sender); rc.mn_fiber(waiter)\n"
            "rc.mn_run()\n"
            "print('ZTIE', out.get('mask'), out.get('data'), rc._logical_ns())\n"
            "rc.mn_fini()\n")
        for i in range(3):
            p = run_sim(code)
            assert p.returncode == 0, (p.stdout, p.stderr[-800:])
            assert "ZTIE 1 b'tie' 0" in p.stdout, \
                ("ready must beat a same-instant timeout", p.stdout)

    def test_park_timeout_on_logical_plane(self):
        """I4 plane (b): park(timeout=2.5) with no waker times out at EXACTLY
        logical 2.5e9 ns (wall-instant); a woken park returns False at the
        waker's logical instant.  (Regression: monotonic entry/verdict
        compares declared a logical deadline expired-at-birth -- 9ms wall,
        logical 0.)"""
        p = run_sim(
            "import time, runloom_c as rc\n"
            "out = {}\n"
            "def parker():\n"
            "    out['r'] = rc.park(timeout=2.5)\n"
            "    out['ns'] = rc._logical_ns()\n"
            "t0 = time.monotonic()\n"
            "rc.mn_init(2); rc.mn_fiber(parker); rc.mn_run()\n"
            "print('PT', out['r'], out['ns'], time.monotonic() - t0 < 2.0)\n"
            "rc.mn_fini()\n")
        assert "PT True 2500000000 True" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_park_woken_before_logical_timeout(self):
        p = run_sim(
            "import runloom_c as rc\n"
            "out = {}\n"
            "def parker():\n"
            "    out['h'] = rc.current_g()\n"
            "    out['r'] = rc.park(timeout=9.0)\n"
            "    out['ns'] = rc._logical_ns()\n"
            "def waker():\n"
            "    rc.sched_sleep(0.003)\n"
            "    out['h'].wake()\n"
            "rc.mn_init(2); rc.mn_fiber(parker); rc.mn_fiber(waker)\n"
            "rc.mn_run()\n"
            "print('PW', out['r'], out['ns'])\n"
            "rc.mn_fini()\n")
        assert "PW False 3000000" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])


class TestReviewRegressions:
    def test_late_parker_gets_stashed_wake(self):
        """I2-review lost-wake regression: a delivery dispatched while its
        receiver is NOT yet parked (receiver sleeps first) must be stashed as
        a pending wake (fd_pending_wake_set, parity with real backends) and
        consumed at the receiver's pre-park points -- dropping it was a
        silent permanent hang with all H=1 self-heals gated off."""
        p = run_sim(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "got = {}\n"
            "def sender():\n"
            "    a.send(b'x')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "def receiver():\n"
            "    rc.sched_sleep(0.001)\n"       # parks AFTER the dispatch
            "    rc.wait_fd(b.fileno(), 1)\n"
            "    got['d'] = b.recv(16)\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(receiver)\n"
            "rc.mn_fiber(sender)\n"
            "rc.mn_run(); rc.mn_fini()\n"
            "print('LATE_PARKER', got.get('d'))\n")
        assert "LATE_PARKER b'x'" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_barrier_zero_fenced(self):
        """I2-review fence regression: RUNLOOM_MN_BARRIER=0 under the mn-sim
        opt-in must raise -- without the barrier the census never arms, every
        gate goes dark, and the P4 silent corruption returns."""
        p = run_sim(
            "import runloom_c as rc\n"
            "try:\n"
            "    rc.mn_init(2)\n"
            "    print('FENCE_MISSING')\n"
            "except RuntimeError as e:\n"
            "    print('BARRIER_FENCED' if 'BARRIER' in str(e) else 'WRONG_MSG')\n",
            extra={"RUNLOOM_MN_BARRIER": "0"}, seed=1)
        assert "BARRIER_FENCED" in p.stdout, (p.stdout, p.stderr[-800:])


class TestCrossPlane:
    def test_h1_sim_beside_live_armed_pool(self):
        """The pump/wait_fd gates are keyed on HUB CONTEXT, not global armed
        state: `armed` persists on a live pool, so a global key would steal
        the pump from a main-thread rc.run() H=1-sim scenario coexisting with
        an idle mn pool (its clock never advancing = hang).

        The witness MUST park a receiver on a registered sim conn through the
        H=1 plane (review fix: a sched_sleep-only fiber is advanced by the
        drain's direct logical jump and never reaches the pump, so it passes
        even under a broken global-armed gate) -- this wait_fd park is woken
        only by the H=1 sim pump's ledger dispatch, exercising both gates."""
        p = run_sim(
            "import socket, runloom_c as rc\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(lambda: None)\n"
            "rc.mn_run()\n"                     # pool stays live + armed
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "got = {}\n"
            "def receiver():\n"
            "    rc.wait_fd(b.fileno(), 1)\n"   # woken ONLY by the H=1 pump
            "    got['data'] = b.recv(16)\n"
            "def sender():\n"
            "    rc.sched_sleep(0.002)\n"
            "    a.send(b'h1-plane')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "rc.fiber(receiver)\n"
            "rc.fiber(sender)\n"
            "rc.run()\n"                        # H=1 sim plane must pump
            "print('CROSSPLANE_OK', got.get('data'))\n", timeout=30)
        assert "CROSSPLANE_OK b'h1-plane'" in p.stdout, \
            (p.stdout, p.stderr[-800:])


class TestSimMnFenceExtension:
    def test_optin_without_seed_raises(self):
        """RUNLOOM_SIM_MN without RUNLOOM_MN_SEED: no census exists to
        dispatch the ledger -- mn_init must raise naming the seed."""
        env = mn_digest.hermetic_env(dict(SIM_ENV))   # note: NO seed
        p = subprocess.run(
            [sys.executable, "-c",
             "import runloom_c as rc\n"
             "try:\n"
             "    rc.mn_init(2)\n"
             "    print('FENCE_MISSING')\n"
             "except RuntimeError as e:\n"
             "    print('SEED_FENCE' if 'RUNLOOM_MN_SEED' in str(e)\n"
             "          else 'WRONG_MSG', str(e)[:100])\n"],
            cwd=REPO, env=env, timeout=60,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert "SEED_FENCE" in p.stdout, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
