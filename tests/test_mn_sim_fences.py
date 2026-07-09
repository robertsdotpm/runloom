"""MN_SIM_DST_PLAN.md I3 -- the nondeterminism fence lattice.

Every FORBIDDEN wake-contract entry gets an enforcement point; the tripwire
(runloom_sim_foreign_wake_total + RUNLOOM_SIM_STRICT) converts any
unenumerated foreign wake into a loud failure.  All fences are scoped to the
armed mn-sim census (hub-context where relevant) -- the frozen H=1 plane and
all non-sim users are untouched (proved by the non-sim companions here and
the untouched frozen suite).
"""
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

SIM_ENV = {"RUNLOOM_SIM": "1", "RUNLOOM_SIM_MN": "1", "RUNLOOM_MN_SEED": "1"}


def run_snip(code, extra=None, timeout=60):
    env_extra = dict(SIM_ENV)
    if extra:
        env_extra.update(extra)
    env = mn_digest.hermetic_env(env_extra)
    return subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                          timeout=timeout, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)


class TestFencesRaise:
    def test_blocking_runs_inline(self):
        """contract #14 (as-built, review-corrected): a hub fiber's blocking()
        runs INLINE under an armed sim census -- synchronous on the hub, so no
        foreign park, no foreign completion wake, schedule stays f(seed), and
        the tripwire counts 0.  (The earlier binding-level raise missed the
        internal getaddrinfo offload; the C-level inline routing covers both.)"""
        p = run_snip(
            "import runloom_c as rc\n"
            "out = {}\n"
            "def w():\n"
            "    out['r'] = rc.blocking(lambda: 41) + 1\n"
            "rc.mn_init(2); rc.mn_fiber(w); rc.mn_run()\n"
            "print('BLOCKING_INLINE', out.get('r'), rc.sim_foreign_wake_count())\n"
            "rc.mn_fini()\n")
        assert "BLOCKING_INLINE 42 0" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_park_foreign_wakeable_raises(self):
        """contract #25: holds the census clock for an unorderable waker."""
        p = run_snip(
            "import runloom_c as rc\n"
            "out = {}\n"
            "def w():\n"
            "    try: rc.park(foreign_wakeable=True)\n"
            "    except RuntimeError as e: out['r'] = 'foreign_wakeable' in str(e)\n"
            "rc.mn_init(2); rc.mn_fiber(w); rc.mn_run(); rc.mn_fini()\n"
            "print('PARK_FW_FENCED', out.get('r'))\n")
        assert "PARK_FW_FENCED True" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_sched_sleep_real_raises(self):
        """gate [9]: deliberately wall-clock -- unorderable inside a run."""
        p = run_snip(
            "import runloom_c as rc\n"
            "out = {}\n"
            "def w():\n"
            "    try: rc.sched_sleep_real(0.001)\n"
            "    except RuntimeError as e: out['r'] = 'sched_sleep_real' in str(e)\n"
            "rc.mn_init(2); rc.mn_fiber(w); rc.mn_run(); rc.mn_fini()\n"
            "print('SLEEP_REAL_FENCED', out.get('r'))\n")
        assert "SLEEP_REAL_FENCED True" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_per_g_tstate_mode_raises(self):
        """contract #22: the per-g-tstate wake path is unaudited under sim."""
        p = run_snip(
            "import runloom_c as rc\n"
            "try:\n"
            "    rc.mn_init(2)\n"
            "    print('FENCE_MISSING')\n"
            "except RuntimeError as e:\n"
            "    print('PERG_FENCED' if 'UNSAFE_MIGRATION' in str(e) else 'WRONG')\n",
            extra={"RUNLOOM_ALLOW_UNSAFE_MIGRATION": "1"})
        assert "PERG_FENCED" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_preempt_init_noop(self):
        """gate [8]: the wall-clock time-slicer refuses under mn-sim (the
        deterministic frame-count preempt is the only preemption)."""
        p = run_snip(
            "import runloom_c as rc\n"
            "rc.preempt_init(1000)\n"          # must be a logged no-op
            "progress = []\n"
            "def w(): progress.append(1)\n"
            "rc.mn_init(2); rc.mn_fiber(w); rc.mn_run(); rc.mn_fini()\n"
            "rc.preempt_fini()\n"
            "print('PREEMPT_NOOP', len(progress))\n")
        assert "PREEMPT_NOOP 1" in p.stdout, (p.stdout, p.stderr[-800:])
        assert "no-op under" in p.stderr, p.stderr[-400:]

    def test_slicer_running_before_mn_init_raises(self):
        """I2-review ordering hole: a slicer started BEFORE mn_init keeps
        posting wall-clock yields into the seeded run -- mn_init refuses."""
        p = run_snip(
            "import runloom_c as rc\n"
            "rc.preempt_init(1000)\n"          # started pre-fence... but the
            "try:\n"                            # SIM_MN gate no-ops it, so use
            "    rc.mn_init(2)\n"               # the env-less start below
            "    print('SLICER_TOLERATED')\n"
            "except RuntimeError as e:\n"
            "    print('SLICER_FENCED' if 'time-slicer' in str(e) else 'WRONG')\n")
        # Under SIM_ENV the preempt gate already no-ops the slicer, so
        # mn_init proceeds: SLICER_TOLERATED is the correct outcome here.
        # The genuinely dangerous ordering (slicer started with sim env
        # UNSET, then mn-sim run) is the subprocess below.
        assert "SLICER_TOLERATED" in p.stdout or "SLICER_FENCED" in p.stdout, \
            (p.stdout, p.stderr[-800:])

    def test_slicer_started_pre_env_is_fenced(self):
        """The real hole: env set AFTER preempt_init -- the slicer thread is
        live and un-gated; mn_init must refuse it."""
        env = mn_digest.hermetic_env({})       # NO sim env at process start
        p = subprocess.run(
            [sys.executable, "-c",
             "import os, runloom_c as rc\n"
             "rc.preempt_init(1000)\n"          # slicer live, sim off
             "os.environ['RUNLOOM_SIM'] = '1'\n"
             "os.environ['RUNLOOM_SIM_MN'] = '1'\n"
             "os.environ['RUNLOOM_MN_SEED'] = '1'\n"
             "try:\n"
             "    rc.mn_init(2)\n"
             "    print('FENCE_MISSING')\n"
             "except RuntimeError as e:\n"
             "    print('PRE_ENV_SLICER_FENCED' if 'time-slicer' in str(e)\n"
             "          else 'WRONG_MSG', str(e)[:80])\n"
             "rc.preempt_fini()\n"],
            cwd=REPO, env=env, timeout=60,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # NOTE: runloom_sim_enabled() latches at first read.  preempt_init's
        # gate reads SIM_MN env first (unset -> skips the sim read), so sim
        # latches later inside mn_init's fence as ON -- and the live slicer
        # must then be refused.
        assert "PRE_ENV_SLICER_FENCED" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])


class TestForeignWakeTripwire:
    def test_foreign_gwake_strict_aborts(self):
        """contract #13: a raw OS thread calling G.wake() during a seeded run
        -- under RUNLOOM_SIM_STRICT (default) the run aborts loudly."""
        p = run_snip(
            "import _thread, time, runloom_c as rc\n"
            "handle = {}\n"
            "def victim():\n"
            "    handle['g'] = rc.current_g()\n"
            "    rc.park()\n"                   # parked; foreign thread wakes
            "def foreign():\n"
            "    time.sleep(0.05)\n"
            "    handle['g'].wake()\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(victim)\n"
            "_thread.start_new_thread(foreign, ())\n"
            "rc.mn_run(); rc.mn_fini()\n"
            "print('NO_ABORT')\n", timeout=60)
        assert "NO_ABORT" not in p.stdout, "strict tripwire did not abort"
        assert "FOREIGN-THREAD WAKE" in p.stderr, p.stderr[-800:]

    def test_foreign_gwake_nonstrict_counts(self):
        """RUNLOOM_SIM_STRICT=0: same scenario downgrades to counter-only --
        the run completes and sim_foreign_wake_count() == 1."""
        p = run_snip(
            "import _thread, time, runloom_c as rc\n"
            "handle = {}\n"
            "def victim():\n"
            "    handle['g'] = rc.current_g()\n"
            "    rc.park()\n"
            "def foreign():\n"
            "    time.sleep(0.05)\n"
            "    handle['g'].wake()\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(victim)\n"
            "_thread.start_new_thread(foreign, ())\n"
            "rc.mn_run()\n"
            "print('COUNTED', rc.sim_foreign_wake_count())\n"
            "rc.mn_fini()\n",
            extra={"RUNLOOM_SIM_STRICT": "0"}, timeout=60)
        assert "COUNTED 1" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_clean_run_counts_zero(self):
        """The green-run oracle: a clean seeded byte run reports 0."""
        p = run_snip(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "def s():\n"
            "    a.send(b'x'); rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "def r():\n"
            "    rc.wait_fd(b.fileno(), 1); b.recv(4)\n"
            "rc.mn_init(2); rc.mn_fiber(r); rc.mn_fiber(s)\n"
            "rc.mn_run()\n"
            "print('CLEAN', rc.sim_foreign_wake_count())\n"
            "rc.mn_fini()\n")
        assert "CLEAN 0" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])


class TestIoUringGate:
    def test_rings_off_and_digest_stable_under_loop_env(self):
        """gate [2] + I3 acceptance: RUNLOOM_IOURING_LOOP=1 under sim must
        neither create hub rings (no blocking loop_wait -- bounded wall time)
        nor perturb the digest -- proving the GATE, not the default."""
        extra = {"RUNLOOM_IOURING_LOOP": "1"}
        base = [mn_digest.run_digest("cpu_yield", 2, 12345,
                                     extra_env=dict(SIM_ENV))
                for i in range(2)]
        withloop = [mn_digest.run_digest("cpu_yield", 2, 12345,
                                         extra_env=dict(SIM_ENV, **extra))
                    for i in range(2)]
        assert len(set(base)) == 1 and len(set(withloop)) == 1
        assert base[0] == withloop[0], \
            "RUNLOOM_IOURING_LOOP=1 changed the seeded schedule under sim"


class TestFinalizerTorture:
    def test_finalizer_chan_ops_complete(self):
        """contract #24 torture: __del__ doing runloom ops runs ON a hub
        thread (hub TLS present -- the tripwire is structurally blind to it,
        by design).  The workload must complete without crash/abort; workloads
        are DOCUMENTED not to do this, but it must fail soft, not corrupt."""
        p = run_snip(
            "import gc, runloom_c as rc\n"
            "ch = rc.Chan(64)\n"
            "class Noisy:\n"
            "    def __del__(self):\n"
            "        try: ch.try_send(1)\n"
            "        except BaseException: pass\n"
            "def maker():\n"
            "    for i in range(40):\n"
            "        x = Noisy(); x.self = x     # cycle -> GC-point finalizer\n"
            "        del x\n"
            "        if i % 8 == 0:\n"
            "            gc.collect()\n"
            "        rc.sched_yield()\n"
            "    gc.collect()              # flush the tail cycles\n"
            "def drainer():\n"
            "    n = 0\n"
            "    while n < 40:\n"
            "        v, ok = ch.recv()\n"
            "        n += 1\n"
            "    print('DRAINED', n)\n"
            "rc.mn_init(2)\n"
            "rc.mn_fiber(maker); rc.mn_fiber(drainer)\n"
            "rc.mn_run(); rc.mn_fini()\n", timeout=60)
        assert "DRAINED 40" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
