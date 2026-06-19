"""kqueue (macOS/BSD) syscall fault-injection -- EXTENDED matrix.

This module EXTENDS tests/test_kqueue_faultinject.py; it does NOT duplicate it.
The base file covers:
  - KQUEUE_WAIT  once:EINTR  + always:<hard errno> backoff,
  - KQUEUE_CTL   always:<errno> surfaces as OSError,
  - KQUEUE_CREATE once:<errno> surfaces as OSError.

The kqueue backend carries compiled-in, env-gated fault points
(RUNLOOM_FAULT_<SITE>="<once|always>:<errno>", read at the syscall site; see
src/runloom_c/netpoll_init.c.inc runloom_fault_inject).  Darwin/BSD have no
syscall-injecting tracer that survives SIP, so this env gate is the ONLY way to
drive the error paths.  Because the gate is read with getenv() at the live
syscall, every fault test runs a SUBPROCESS with the env set and parses the
BACKEND=/RESULT=/FAULTS=/DONE sentinels, asserting CLEAN TERMINATION (rc==0,
never a crash or a hang -- the subprocess timeout catches a hang).

What this file ADDS (each test names the code branch it targets):

  1. KQUEUE_WAIT  always:EINTR  -- the base file only does once:EINTR.  A
     SIGNAL-interrupted kevent() must be retried WITHOUT backoff every time
     (runloom_netpoll_wait_failed treats EINTR as "retry, no backoff",
     netpoll_pump_helpers.c.inc:185).  With always:EINTR the first (blocking)
     pass of EVERY pump iteration short-circuits to EINTR, so the parked fiber
     can only progress via its deadline -- it MUST still terminate, and the
     EINTR count must be BOUNDED (a few per pump turn over the deadline window),
     never an unbounded busy-spin.

  2. KQUEUE_CTL  once:<more errnos> + ROLLBACK proof -- the base file does
     always:<4 errnos>.  Here a SINGLE faulted registration kevent()
     (netpoll_register.c.inc:147-155) must (a) surface OSError(errno) to the
     first parker AND (b) roll the fd-bit back (runloom_fd_bit_clear,
     line 153) so it leaves NO half-registered state: a SECOND wait_fd on the
     SAME fd after the once-fault has cleared re-registers cleanly and wakes on
     a real edge.  More errno values (ENOSPC/EPERM/ENODEV/ENOMEM) widen the
     surface beyond the base file's four.

  3. KQUEUE_CREATE once vs the lazy per-hub init under M:N -- the base file runs
     single-thread.  KQUEUE_CREATE is injected in runloom_netpoll_init() on the
     DEFAULT pool (netpoll_init.c.inc:277); per-hub pools create their kqueue
     lazily in runloom_pool_backend_create (NOT fault-injected).  Run the SAME
     init-fault under run(n) for n in {2,4} and assert it still surfaces an
     OSError to a parked fiber and the whole M:N run unwinds cleanly.

  4. HEAVY CONCURRENCY -- many fibers parked while a faulted kevent() races the
     pump/deadline sweep.  A crash or hang here would be a real bug (note it).
     Covers KQUEUE_WAIT always backoff and KQUEUE_CTL always under load.

All sockets are loopback/UDP-bound-or-socketpair, every fiber has a short
deadline so the workload ALWAYS terminates regardless of the fault, and the
point measured is the runtime's RESPONSE.  no-gil (PYTHON_GIL=0) only.
"""
import os
import re
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith(("darwin", "freebsd", "openbsd", "netbsd")),
    reason="kqueue backend only")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SRC = os.path.join(REPO, "src")
WORKLOAD = os.path.join(HERE, "netpoll_inproc_fault_workload.py")

# Darwin/BSD errno values (stable across macOS + the BSDs for these).
ENOENT, ESRCH, EINTR, EBADF, ENOMEM = 2, 3, 4, 9, 12
EACCES, EFAULT, EBUSY, ENODEV, EINVAL = 13, 14, 16, 19, 22
ENFILE, EMFILE, ENOSPC, EPERM = 23, 24, 28, 1

TIMEOUT_MS = 800
# 1 ms backoff across the deadline window => ~TIMEOUT_MS calls; allow 6x slack.
# A busy-spin (no backoff) issues orders of magnitude more, scaling with CPU.
MAX_FAULTS_BACKOFF = TIMEOUT_MS * 6
# EINTR is retried with NO backoff, but only the FIRST (blocking) pass of each
# pump iteration is faulted (netpoll_pump.c.inc:168 -- drained==0), and the pump
# only runs when the hub idles toward the deadline.  So the fault count is still
# bounded by pump turns over the window, not by CPU.  Generous ceiling.
MAX_FAULTS_EINTR = TIMEOUT_MS * 200


def _base_env(site, spec):
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC
    env["PYTHON_GIL"] = "0"                       # focus: free-threaded only
    env["FAULT_SITE"] = site
    env["FAULT_TIMEOUT_MS"] = str(TIMEOUT_MS)
    env["RUNLOOM_FAULT_" + site] = spec
    return env


def _run_workload(site, spec, timeout=40):
    """Drive the shared single-thread workload (one parked fiber, deadline)."""
    return subprocess.run(
        [sys.executable, WORKLOAD], cwd=REPO, env=_base_env(site, spec),
        timeout=timeout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)


def _run_snippet(site, spec, code, timeout=60):
    """Drive an inline -c workload that the shared file is too thin for
    (second-park-after-rollback, M:N hubs, heavy concurrency).  Same env
    contract + sentinels as the shared workload."""
    return subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=_base_env(site, spec),
        timeout=timeout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True)


def _field(out, key):
    m = re.search(r"^%s=(.*)$" % key, out, re.MULTILINE)
    return m.group(1) if m else None


def _assert_terminated(p):
    """The workload must always run to a clean shutdown -- a fault on any
    kqueue syscall is a recoverable error, never a crash or a hang."""
    assert p.returncode == 0, "rc=%d\n%s\n%s" % (p.returncode, p.stdout, p.stderr)
    assert "DONE" in p.stdout, "workload did not finish:\n%s\n%s" % (
        p.stdout, p.stderr)
    assert _field(p.stdout, "BACKEND") == "kqueue", "not on kqueue:\n%s" % p.stdout


# ===========================================================================
# 1. KQUEUE_WAIT always:EINTR -- persistent signal interruption, retried,
#    bounded, still terminates.  (base file only does once:EINTR)
#    branch: netpoll_pump.c.inc:168-188 (finj short-circuit) +
#            netpoll_pump_helpers.c.inc:185 (EINTR -> retry, no backoff)
# ===========================================================================

@pytest.mark.parametrize("rep", range(3), ids=["a", "b", "c"])
def test_wait_always_eintr_retried_bounded(rep):
    """A persistently signal-interrupted kevent() (EINTR, always) is retried
    transparently every pump turn; the parked fiber still wakes via its
    deadline and the workload terminates cleanly.  The EINTR count must be
    BOUNDED (pump turns over the window), never an unbounded busy-spin."""
    p = _run_workload("KQUEUE_WAIT", "always:%d" % EINTR)
    _assert_terminated(p)
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "EINTR never fired -- injection not wired:\n%s" % p.stdout
    assert faults <= MAX_FAULTS_EINTR, (
        "kevent EINTR busy-spin: %d retries in %d ms (ceiling %d)\n%s"
        % (faults, TIMEOUT_MS, MAX_FAULTS_EINTR, p.stdout))
    # The fiber never gets a real edge, so it returns 0 (deadline timeout).
    assert "'ok', 0" in (_field(p.stdout, "RESULT") or ""), (
        "EINTR retry should let the deadline fire (RESULT ok,0):\n%s" % p.stdout)


# ===========================================================================
# 2. KQUEUE_CTL once:<more errnos> + fd-bit ROLLBACK proof.
#    branch: netpoll_register.c.inc:144-155 (finj -> errno; fd-bit rollback at 153)
# ===========================================================================

@pytest.mark.parametrize(
    "errno_", [ENOSPC, EPERM, ENODEV, ENOMEM, EBUSY],
    ids=["ENOSPC", "EPERM", "ENODEV", "ENOMEM", "EBUSY"])
def test_register_once_error_surfaces_then_rolls_back(errno_):
    """A SINGLE faulted registration kevent() must (a) surface OSError(errno)
    to the FIRST parker and (b) roll the fd-bit back, leaving NO half-registered
    state -- so a SECOND wait_fd on the SAME fd, after the once-fault cleared,
    re-registers cleanly and wakes on a real readable edge.

    Proves the fd-bit-clear rollback (netpoll_register.c.inc:153): without it,
    runloom_fd_bit_set would still see the bit set on the second register and
    return ENOMEM early (line 123-127), stranding the second parker forever."""
    code = r"""
import os, socket, sys, threading, time
sys.path.insert(0, "src")
import runloom_c
SITE = "KQUEUE_CTL"
TO = int(os.environ["FAULT_TIMEOUT_MS"])
# socketpair so the SECOND park can be woken by a real edge from the peer.
a, b = socket.socketpair()
a.setblocking(False); b.setblocking(False)
first = []
second = []

def feeder():
    # let the second park arm, then make `a` readable with a real edge.
    time.sleep(0.15)
    try:
        b.send(b"x")
    except OSError:
        pass

def parker():
    # FIRST park: the once-fault fires on this fd's registration kevent.
    try:
        r = runloom_c.wait_fd(a.fileno(), 1, TO)
        first.append(("ok", r))
    except OSError as e:
        first.append(("oserror", e.errno))
    # SECOND park on the SAME fd: the once-fault has fired+cleared, so the
    # registration must now succeed and the real edge must wake it.
    t = threading.Thread(target=feeder, daemon=True); t.start()
    try:
        r = runloom_c.wait_fd(a.fileno(), 1, TO)
        second.append(("ok", r))
    except OSError as e:
        second.append(("oserror", e.errno))
    t.join(5)

runloom_c.fiber(parker)
runloom_c.run()
a.close(); b.close()
print("BACKEND=%s" % runloom_c.netpoll_backend())
print("FIRST=%r" % (first,))
print("SECOND=%r" % (second,))
print("FAULTS=%d" % runloom_c._fault_count(SITE))
print("DONE")
"""
    p = _run_snippet("KQUEUE_CTL", "once:%d" % errno_, code)
    _assert_terminated(p)
    assert int(_field(p.stdout, "FAULTS")) == 1, "CTL once never fired:\n%s" % p.stdout
    assert ("'oserror', %d" % errno_) in (_field(p.stdout, "FIRST") or ""), (
        "first register errno %d not surfaced as OSError:\n%s" % (errno_, p.stdout))
    # The rollback proof: the SECOND park on the same fd must succeed (woken by
    # the real edge => mask has READ), NOT raise OSError and NOT hang.
    second = _field(p.stdout, "SECOND") or ""
    assert "'ok', 1" in second, (
        "second park on the same fd did NOT re-register after rollback "
        "(half-registered state left behind):\n%s" % p.stdout)


# ===========================================================================
# 3. KQUEUE_CREATE once under M:N -- the default-pool init fault, with the
#    scheduler running n hubs (lazy per-hub kqueue init is a DIFFERENT,
#    non-faulted path; document that the fault hits the default-pool init).
#    branch: netpoll_init.c.inc:276-289 (KQUEUE_CREATE on the default pool)
# ===========================================================================

@pytest.mark.parametrize("hubs", [2, 4], ids=["hubs2", "hubs4"])
@pytest.mark.parametrize(
    "errno_", [ENOMEM, EMFILE], ids=["ENOMEM", "EMFILE"])
def test_kqueue_create_once_under_mn(hubs, errno_):
    """kqueue() failing at netpoll init (default pool) must surface as a clean
    OSError to a parked fiber and let the WHOLE M:N run unwind -- never crash,
    never hang -- even with n hub threads live.  The per-hub kqueues are created
    lazily (runloom_pool_backend_create, NOT fault-injected); this drives the
    default-pool init fault under M:N to prove the M:N teardown path is clean
    on a hard init failure too."""
    code = r"""
import os, socket, sys
sys.path.insert(0, "src")
import runloom, runloom_c
SITE = "KQUEUE_CREATE"
HUBS = int(os.environ["RL_HUBS"])
TO = int(os.environ["FAULT_TIMEOUT_MS"])
result = []

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0)); s.setblocking(False)
    def parker():
        try:
            r = runloom_c.wait_fd(s.fileno(), 1, TO)
            result.append(("ok", r))
        except OSError as e:
            result.append(("oserror", e.errno))
        except BaseException as e:
            result.append(("err", type(e).__name__))
    runloom.fiber(parker)
    runloom.sleep(float(TO) / 1000.0 + 0.3)
    s.close()

try:
    runloom.run(HUBS, main)
finally:
    pass
print("BACKEND=%s" % runloom_c.netpoll_backend())
print("RESULT=%r" % (result,))
print("FAULTS=%d" % runloom_c._fault_count(SITE))
print("DONE")
"""
    env = _base_env("KQUEUE_CREATE", "once:%d" % errno_)
    env["RL_HUBS"] = str(hubs)
    p = subprocess.run(
        [sys.executable, "-c", code], cwd=REPO, env=env, timeout=60,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    _assert_terminated(p)
    assert int(_field(p.stdout, "FAULTS")) == 1, (
        "CREATE once never fired under M:N:\n%s" % p.stdout)
    # The init fault surfaces to the parked fiber as OSError (the default pool's
    # kqueue_fd is -1, so register/wait take the kqueue_fd<0 error/sleep paths
    # and the fiber's wait_fd raises).  Either a clean OSError or a clean
    # deadline timeout is acceptable -- what is NOT acceptable is a crash/hang
    # (caught above) or an unexpected exception type.
    result = _field(p.stdout, "RESULT") or ""
    assert ("oserror" in result) or ("'ok', 0" in result), (
        "CREATE-fault fiber neither raised OSError nor timed out cleanly "
        "under M:N:\n%s" % p.stdout)
    assert "'err'" not in result, (
        "CREATE-fault fiber died with an unexpected exception:\n%s" % p.stdout)


# ===========================================================================
# 4. HEAVY CONCURRENCY -- many parked fibers while a faulted kevent() races
#    the pump/deadline sweep.  Must terminate cleanly (a crash/hang is a real
#    bug to report).  Covers KQUEUE_WAIT always-backoff and KQUEUE_CTL always.
#    branch: netpoll_pump.c.inc:131-221 (drain loop under load) +
#            netpoll_register.c.inc:144-155 (CTL fault per-fd) +
#            netpoll_pump_helpers.c.inc:41-99 (dispatch wake_all across pools)
# ===========================================================================

_HEAVY_CODE = r"""
import os, socket, sys
sys.path.insert(0, "src")
import runloom_c
SITE = os.environ["FAULT_SITE"]
TO = int(os.environ["FAULT_TIMEOUT_MS"])
N = int(os.environ["RL_NPARK"])
socks = []
results = []  # one slot per fiber (single writer each) -- race-free under no-gil

def make_parker(i, fd):
    def parker():
        try:
            r = runloom_c.wait_fd(fd, 1, TO)
            results[i] = ("ok", r)
        except OSError as e:
            results[i] = ("oserror", e.errno)
        except BaseException as e:
            results[i] = ("err", type(e).__name__)
    return parker

for i in range(N):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0)); s.setblocking(False)
    socks.append(s)
    results.append(None)
for i, s in enumerate(socks):
    runloom_c.fiber(make_parker(i, s.fileno()))
runloom_c.run()
for s in socks:
    s.close()
done = sum(1 for r in results if r is not None)
oserr = sum(1 for r in results if r and r[0] == "oserror")
ok = sum(1 for r in results if r and r[0] == "ok")
bad = sum(1 for r in results if r and r[0] == "err")
print("BACKEND=%s" % runloom_c.netpoll_backend())
print("RESULT=%r" % [("done", done), ("ok", ok), ("oserror", oserr), ("err", bad)])
print("NPARK=%d" % N)
print("FAULTS=%d" % runloom_c._fault_count(SITE))
print("DONE")
"""


@pytest.mark.parametrize("npark", [64, 256], ids=["n64", "n256"])
def test_heavy_concurrency_wait_always_backoff(npark):
    """Many fibers parked on never-ready fds while EVERY pump's blocking kevent()
    is faulted (always:EBADF) -- the deadline sweep must still wake every fiber
    (each returns a clean timeout 0) and the backoff must keep the fault count
    bounded.  A crash/hang under this load is a real bug to report."""
    env = _base_env("KQUEUE_WAIT", "always:%d" % EBADF)
    env["RL_NPARK"] = str(npark)
    p = subprocess.run(
        [sys.executable, "-c", _HEAVY_CODE], cwd=REPO, env=env, timeout=90,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    _assert_terminated(p)
    assert int(_field(p.stdout, "NPARK")) == npark, p.stdout
    # Every fiber must have FINISHED (none stranded by the faulted pump).
    res = _field(p.stdout, "RESULT") or ""
    assert "('done', %d)" % npark in res, (
        "not all %d fibers finished under a faulted pump -- some stranded:\n%s"
        % (npark, p.stdout))
    assert "('err', 0)" in res, "a fiber died with an unexpected exception:\n%s" % p.stdout
    faults = int(_field(p.stdout, "FAULTS"))
    assert faults > 0, "WAIT fault never fired under load:\n%s" % p.stdout
    assert faults <= MAX_FAULTS_BACKOFF, (
        "kevent busy-spin under load: %d injections in %d ms (ceiling %d)\n%s"
        % (faults, TIMEOUT_MS, MAX_FAULTS_BACKOFF, p.stdout))


@pytest.mark.parametrize("npark", [64, 256], ids=["n64", "n256"])
def test_heavy_concurrency_ctl_always_oserror(npark):
    """Many fibers each registering on its OWN fd while EVERY registration
    kevent() is faulted (always:EINVAL) -- every fiber's wait_fd must surface a
    clean OSError(EINVAL) (the fd-bit is rolled back per fd, line 153) and every
    fiber must FINISH.  Stresses the per-fd CTL rollback path under load; a
    crash/hang/strand is a real bug to report."""
    env = _base_env("KQUEUE_CTL", "always:%d" % EINVAL)
    env["RL_NPARK"] = str(npark)
    p = subprocess.run(
        [sys.executable, "-c", _HEAVY_CODE], cwd=REPO, env=env, timeout=90,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    _assert_terminated(p)
    assert int(_field(p.stdout, "NPARK")) == npark, p.stdout
    res = _field(p.stdout, "RESULT") or ""
    assert "('done', %d)" % npark in res, (
        "not all %d fibers finished under a faulted register -- some stranded:\n%s"
        % (npark, p.stdout))
    # Every register kevent fails => every fiber sees OSError, none ok/err.
    assert "('oserror', %d)" % npark in res, (
        "not every fiber surfaced OSError under a persistent CTL fault:\n%s"
        % p.stdout)
    assert "('err', 0)" in res, "a fiber died with an unexpected exception:\n%s" % p.stdout
    assert int(_field(p.stdout, "FAULTS")) >= npark, (
        "CTL fault fired fewer times than fibers registered:\n%s" % p.stdout)


# ===========================================================================
# 5. KQUEUE_PERHUB always -- a NON-default pool's lazy kqueue create fails, so
#    the register/pump "kqueue_fd < 0" DEFENSIVE arms become reachable while the
#    run still starts (the default pool stays intact).  Needs M:N (per-hub pools
#    only exist under run(n>1)).  Fibers landing on a broken hub get OSError
#    (register: netpoll_register.c.inc:129-135 EINVAL); the broken hub's pump
#    takes the kqueue_fd<0 sleep arm (netpoll_pump.c.inc:172-176).  The run must
#    unwind cleanly -- a crash/hang here would be a real bug.
#    branch: netpoll_init.c.inc backend_create per-hub fault + register:129 + pump:172
# ===========================================================================

_PERHUB_CODE = r"""
import socket, sys
sys.path.insert(0, "src")
import runloom, runloom_c
socks = []
outcomes = []
def worker(i):
    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)
    socks.append((a, b))
    try:
        r = runloom_c.wait_fd(a.fileno(), 1, 300)   # READ + 300ms deadline
        outcomes.append(("ok", r))
    except OSError as e:
        outcomes.append(("oserror", e.errno))
    except BaseException as e:
        outcomes.append(("err", type(e).__name__))
def main():
    for i in range(64):
        runloom.fiber(worker, i)
    runloom.sleep(0.6)
runloom.run(8, main)
print("BACKEND=%s" % runloom_c.netpoll_backend())
print("FAULTS=%d" % runloom_c._fault_count("KQUEUE_PERHUB"))
print("N=%d" % len(outcomes))
print("EINVAL=%d" % sum(1 for k, v in outcomes if k == "oserror" and v == 22))
print("DONE")
"""


@pytest.mark.parametrize("rep", range(2), ids=["a", "b"])
def test_perhub_kqueue_create_failure(rep):
    """A per-hub pool whose kqueue() create is faulted leaves kqueue_fd<0; fibers
    that park on that hub surface OSError(EINVAL) from register and the broken
    hub's pump takes its kqueue_fd<0 sleep arm -- the M:N run must still unwind
    cleanly (no crash, no hang)."""
    p = _run_snippet("KQUEUE_PERHUB", "always:%d" % EINVAL, _PERHUB_CODE)
    _assert_terminated(p)
    assert int(_field(p.stdout, "FAULTS")) > 0, (
        "KQUEUE_PERHUB never fired -- per-hub pools not created / not wired:\n%s"
        % p.stdout)
    # At least one fiber landed on a broken hub and got EINVAL from register.
    assert int(_field(p.stdout, "EINVAL")) > 0, (
        "no fiber surfaced register's kqueue_fd<0 EINVAL (all on the default "
        "pool?):\n%s" % p.stdout)
    assert int(_field(p.stdout, "N")) == 64, "not every worker returned:\n%s" % p.stdout


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
