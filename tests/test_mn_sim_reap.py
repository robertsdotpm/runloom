"""MN_SIM_DST_PLAN.md I5 -- the settled census settle-reap + reap-count oracle.

SETTLED = census complete + baton free + no wanters + no timer/ledger/netpoll
deadline + no foreign park: every remaining hub-homed netpoll parker is
unwakeable.  The census reaps them (wait_fd returns -1/cancelled), the run
TERMINATES instead of wedging, and rc.sim_reap_count() is the netpoll-plane
lost-wake oracle: the workload asserts its EXPECTED infra reaps; any excess is
a real strand the chan census cannot see.
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

SIM_ENV = {"RUNLOOM_SIM": "1", "RUNLOOM_SIM_MN": "1", "RUNLOOM_MN_SEED": "12345"}


def run_snip(code, extra=None, timeout=60):
    env_extra = dict(SIM_ENV)
    if extra:
        env_extra.update(extra)
    env = mn_digest.hermetic_env(env_extra)
    return subprocess.run([sys.executable, "-c", code], cwd=REPO, env=env,
                          timeout=timeout, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=True)


class TestSettleReap:
    def test_stranded_parkers_reaped_and_run_terminates(self):
        """Two shuttler-style fibers parked forever after traffic drains: the
        run exits fast (no RUNLOOM_DEADLOCK_MS wedge), reap count == 2 exactly
        (the workload's expected infra total), and the completion digest is
        deterministic across runs."""
        code = (
            "import socket, time, runloom_c as rc\n"
            "conns = []\n"
            "for k in range(2):\n"
            "    a, b = socket.socketpair()\n"
            "    a.setblocking(False); b.setblocking(False)\n"
            "    cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "    conns.append((a, b, cid))\n"
            "order = []\n"
            "def shuttler(k, b):\n"
            "    def w():\n"
            "        try:\n"
            "            rc.wait_fd(b.fileno(), 1)\n"     # idle forever
            "        except OSError:\n"
            "            order.append(('reaped', k))\n"   # the expected exit
            "    return w\n"
            "def worker(k):\n"
            "    def w():\n"
            "        rc.sched_sleep(0.001 * (k + 1))\n"
            "        order.append(('did', k))\n"
            "    return w\n"
            "t0 = time.monotonic()\n"
            "rc.mn_init(2)\n"
            "for k, (a, b, cid) in enumerate(conns):\n"
            "    rc.mn_fiber(shuttler(k, b))\n"
            "for k in range(3):\n"
            "    rc.mn_fiber(worker(k))\n"
            "rc.mn_run()\n"
            "print('REAP', sorted(order), rc.sim_reap_count(),\n"
            "      'WALL_OK', time.monotonic() - t0 < 3.0)\n"
            "rc.mn_fini()\n")
        outs = set()
        for i in range(3):
            p = run_snip(code)
            assert p.returncode == 0, (p.stdout, p.stderr[-800:])
            assert "WALL_OK True" in p.stdout, (p.stdout, p.stderr[-800:])
            line = [ln for ln in p.stdout.splitlines() if ln.startswith("REAP")][0]
            assert " 2 " in line and "('reaped', 0)" in line \
                and "('reaped', 1)" in line, line
            outs.add(line)
        assert len(outs) == 1, "reap runs diverged: {0}".format(outs)

    def test_no_premature_reap_while_event_pending(self):
        """A parked reader whose delivery arrives AFTER a logical delay must
        get its BYTES, never a premature reap -- the settle predicate requires
        no timer pending, and the sleeping shuttler holds one."""
        p = run_snip(
            "import socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "out = {}\n"
            "def reader():\n"
            "    m = rc.wait_fd(b.fileno(), 1)\n"
            "    out['m'] = m\n"
            "    out['data'] = b.recv(16)\n"
            "def shuttler():\n"
            "    rc.sched_sleep(0.25)\n"            # a long logical delay
            "    a.send(b'late-but-real')\n"
            "    rc.sim_deliver_ready(cid, b.fileno(), 1)\n"
            "rc.mn_init(2); rc.mn_fiber(reader); rc.mn_fiber(shuttler)\n"
            "rc.mn_run()\n"
            "print('LATE', out.get('m'), out.get('data'), rc.sim_reap_count())\n"
            "rc.mn_fini()\n")
        assert "LATE 1 b'late-but-real' 0" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_reap_errno_is_ecanceled(self):
        """I5-review regression: the reap resume must carry a DEFINED errno
        (ECANCELED) -- the drains cannot set the parker thread's errno, and
        stale errno made PyErr_SetFromErrno surface RANDOM OSError subclasses
        (FileNotFoundError was measured on the H=1 plane)."""
        p = run_snip(
            "import errno, socket, runloom_c as rc\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "out = {}\n"
            "def stranded():\n"
            "    try:\n"
            "        rc.wait_fd(b.fileno(), 1)\n"
            "    except OSError as e:\n"
            "        out['errno'] = e.errno\n"
            "rc.mn_init(2); rc.mn_fiber(stranded); rc.mn_run()\n"
            "print('ERRNO_OK', out.get('errno') == errno.ECANCELED)\n"
            "rc.mn_fini()\n")
        assert "ERRNO_OK True" in p.stdout, (p.stdout, p.stderr[-800:])
        assert p.returncode == 0, (p.stdout, p.stderr[-800:])

    def test_repark_loop_hits_loud_deadlock_not_livelock(self):
        """I5-review regression (livelock, repro'd at ~800 reaps/sec): a fiber
        retrying its wait after every reap (the robust-recv idiom) must NOT
        cycle reap->repark forever with an inflating oracle -- one reap wave
        per logical instant leaves the re-park for the deadlock census, which
        fires its LOUD dump/raise.  Reap count stays exactly 1."""
        p = run_snip(
            "import socket, runloom_c as rc\n"
            "rc.set_deadlock_mode(2)\n"
            "a, b = socket.socketpair()\n"
            "a.setblocking(False); b.setblocking(False)\n"
            "cid = rc.sim_conn_register(a.fileno(), b.fileno())\n"
            "def stubborn():\n"
            "    while True:\n"
            "        try:\n"
            "            rc.wait_fd(b.fileno(), 1)\n"
            "        except OSError:\n"
            "            continue\n"
            "rc.mn_init(2); rc.mn_fiber(stubborn)\n"
            "try:\n"
            "    rc.mn_run()\n"
            "    print('NO_DIAGNOSTIC')\n"
            "except RuntimeError:\n"
            "    print('LOUD_DEADLOCK', rc.sim_reap_count())\n"
            "rc.mn_fini()\n",
            extra={"RUNLOOM_DEADLOCK_MS": "150"}, timeout=90)
        assert "LOUD_DEADLOCK 1" in p.stdout, (p.stdout, p.stderr[-800:])

    def test_chan_deadlock_still_raises(self):
        """The chan-plane deadlock census is untouched: set_deadlock_mode(2)
        still raises on a genuine chan deadlock (reap covers only the netpoll
        plane)."""
        p = run_snip(
            "import runloom_c as rc\n"
            "rc.set_deadlock_mode(2)\n"
            "ch = rc.Chan(0)\n"
            "def stuck():\n"
            "    ch.recv()\n"                        # nobody ever sends
            "rc.mn_init(2); rc.mn_fiber(stuck)\n"
            "try:\n"
            "    rc.mn_run()\n"
            "    print('NO_RAISE')\n"
            "except RuntimeError as e:\n"
            "    print('CHAN_DEADLOCK_RAISED')\n"
            "rc.mn_fini()\n",
            extra={"RUNLOOM_DEADLOCK_MS": "150"}, timeout=90)
        assert "CHAN_DEADLOCK_RAISED" in p.stdout, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
