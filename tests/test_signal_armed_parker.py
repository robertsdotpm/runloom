"""Bug 3: a Python signal collected while a wait_fd parker is still ARMED.

`runloom_netpoll_signal_wake` hands a raised signal-handler exception to a
fiber parked in a cooperative recv/send_all/connect/select so it propagates
through THAT fiber's stack, the way a signal interrupting a real recv() does.
It claims a parker with the same commit CAS the pump uses, and a parker has
two claimable states: ARMED (linked and registered, the g still running) and
PARKED (committed and yielded).  Whether a signal lands on one or the other is
pure timing -- and the ARMED half was broken: the walk claimed the parker and
then abandoned it, so nobody delivered, the g's own commit CAS failed against
the claim, and it took wait_fd's abort path with the exception dangling.  What
the user saw was the interrupt swallowed as "Exception ignored in: <function
...>" and the fiber parked forever.

HOW OFTEN DOES THAT HAPPEN: as far as anything here can measure, never yet.
signal_wake runs only from the scheduler drain, keyed on the caller's sched,
and on the single-thread plane the drain only runs when no fiber of that sched
is running -- so every parker it walks has already yielded (measured: 40/40
real signal runs on epoll report `armed=0`, and the macOS traces in
.check-logs report the same).  Under M:N a hub fiber's parker belongs to the
HUB's sched and the main thread's walk skips it as wrong_owner.

That is the point of these tests rather than an argument against them.  The
branch is latent, not absent: the claim CAS is unconditional, so the first
caller or ownership change that does walk an ARMED parker eats a signal
silently.  It went through three fix attempts and two reverts without once
being executed, on reasoning alone.  RUNLOOM_FAULT_SIGWAKE_AT_ARM=N (test-only,
inert unless set) executes it: on the Nth reach of the site it raises a SIGALRM
and collects it right there, inside the ARMED window, running the scheduler's
own delivery block at that point instead of at the scheduler's poll.

The competing constraint, from the revert of 6b378681, is that an ARMED parker
may not simply be SKIPPED either -- burning it is supposedly what makes the g
abort its park early and is how the CoPoll selector path gets served.  That
attribution is unproven (see the note in the C file), but claiming AND
delivering satisfies it either way, and that is what these tests pin.
"""
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

needs_sigalrm = pytest.mark.skipif(
    not hasattr(__import__("signal"), "SIGALRM"),
    reason="the fault site raises SIGALRM; Windows has none")


# One fiber, one park, no timing.  connect() to 240.0.0.1 (class-E, never
# routable) parks on WRITE and stays there, and it is the FIRST wait_fd in the
# process -- so N=1 names that parker exactly.
_ARMED = r'''
import signal, sys, faulthandler
sys.path.insert(0, "src")
import runloom_c as rc

box = {}
def raiser(signum, frame):
    raise KeyboardInterrupt("alarm")
signal.signal(signal.SIGALRM, raiser)

def client():
    try:
        rc.TCPConn.connect("240.0.0.1", 9)
        box["rv"] = "connected"
    except KeyboardInterrupt:
        box["interrupt"] = True
    except OSError as e:
        box["oserror"] = e.errno

faulthandler.dump_traceback_later(30, exit=True)
rc.fiber(client)
try:
    rc.run()
except KeyboardInterrupt:
    box["out_of_run"] = True
faulthandler.cancel_dump_traceback_later()
sys.stdout.write("ARMED %r\n" % (sorted(box.items()),))
'''


def _run(script, env_extra=None, timeout=90):
    env = dict(os.environ, PYTHON_GIL="0", PYTHONPATH="src")
    if env_extra:
        env.update(env_extra)
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=timeout)


@needs_sigalrm
def test_signal_delivered_to_armed_parker():
    """The signal must come out of the cooperative call that was parking.

    Before the fix this printed ARMED [] -- the KeyboardInterrupt reached
    nobody -- with CPython reporting the swallowed exception as
    "SystemError: ... returned a result with an exception set", because
    wait_fd's abort path returned the RUNLOOM_NETPOLL_SIGNALED sentinel raw.
    """
    p = _run(_ARMED, {"RUNLOOM_FAULT_SIGWAKE_AT_ARM": "1"})
    assert p.returncode == 0, (p.stdout, p.stderr[-1500:])
    assert "SystemError" not in p.stderr, (
        "the sentinel escaped wait_fd as a return value with an exception "
        "still set -- the abort path is not taking the signal\n%s"
        % p.stderr[-1500:])
    assert "ARMED [('interrupt', True)]" in p.stdout, (
        "a signal collected while this parker was ARMED did not propagate out "
        "of connect(); it was claimed and then dropped\nstdout=%s\nstderr=%s"
        % (p.stdout, p.stderr[-1500:]))


@needs_sigalrm
def test_armed_fault_site_is_inert_when_unset():
    """The knob must not change anything unless it is armed.

    Without it the fiber has no signal to take and simply stays parked, so the
    child is killed by its own faulthandler -- which is what proves the site
    injected nothing.
    """
    p = _run(_ARMED, timeout=90)
    assert "sigwake-fault" not in p.stderr, (
        "the fault site fired with RUNLOOM_FAULT_SIGWAKE_AT_ARM unset\n%s"
        % p.stderr[-800:])
    assert "ARMED" not in p.stdout, (
        "the fiber returned from a connect() that can never complete\n%s"
        % p.stdout)
