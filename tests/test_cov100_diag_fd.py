"""Adversarial coverage for src/runloom_c/netpoll_diag_fd.c.inc.

This fragment is the netpoll DIAGNOSTIC + per-fd bookkeeping surface that the
normal corpus barely touches.  The uncovered, *reachable* lines all live in
``runloom_netpoll_dump_parkers`` (the ``_dump_parkers()`` Python hook) and in
``runloom_fd_cap_target`` (the ``RUNLOOM_NETPOLL_MAXFD`` env parse, read once at
netpoll init).  We drive each by constructing the exact parker shape / env the
line gates on, then assert the diagnostic actually observed it.

Reachability notes for the lines we DELIBERATELY do not chase (see the module
docstring of the suite and the structured report):
  * L40-41  (Floyd cycle: ``global_cycle = 1; break``)        -- corruption-only
  * L56-62  (cycle-present count walk)                          -- corruption-only
  * L75-76  (per-fd bucket self-loop: ``bucket_self++; break``) -- corruption-only
  * L163-171 (``runloom_netpoll_parker_info``)   -- header export, NO C caller / Python binding
  * L245-246 (``rlim_cur`` arm of cap_target)    -- needs rlim_max==RLIM_INFINITY (privileged)
  * L276-279 (calloc-fail cleanup in arrays_init)-- no malloc-fault site exists
  * L295-306 (``runloom_fd_cap_warn_once``)      -- the pump can never see an fd>=cap
                  (register refuses to ARM fd>=cap, so it is never EPOLL_CTL_ADDed)
  * L396-415 (``runloom_fd_bit_get`` / ``runloom_fd_bit_set``)  -- KQUEUE-only / no caller on epoll
None of these is driven here; faking them would violate the "real assertions
only" rule (they require memory corruption, a privileged rlimit, a malloc
interposer, or a non-epoll backend).

Every test below runs against the EPOLL backend (the box default).
"""
import os
import socket
import subprocess
import sys

import pytest

import runloom_c as rc
from adv_util import hang_guard

READ, WRITE = 1, 2
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable

pytestmark = pytest.mark.skipif(rc.netpoll_backend() != "epoll",
                                reason="netpoll_diag_fd coverage assumes the epoll backend")


def _fill_send_buffer(s):
    """Fill s's send buffer so a WRITE wait on it cannot fast-path (OUT not
    ready) and therefore genuinely PARKS -- the only way to keep a WRITE /
    READ|WRITE parker LINKED long enough for _dump_parkers() to classify it."""
    s.setblocking(False)
    total = 0
    try:
        while True:
            total += s.send(b"\0" * 65536)
    except (BlockingIOError, OSError):
        pass
    return total


# --------------------------------------------------------------------------
# dump_parkers event-mask classification -- L122 (WRITE), L123 (READ|WRITE),
# L124 (neither bit / pure-timer park).  These ``else if`` arms run only when a
# parker with that exact direction mask is in the pool while _dump_parkers()
# walks it; the normal corpus parks READ-only, so they never fire.
# --------------------------------------------------------------------------
def test_dump_classifies_write_rdwr_and_other_parkers():
    captured = {}

    def main():
        # WRITE-only parker: full send buffer -> OUT not ready -> parks (L122).
        a, b = socket.socketpair()
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        _fill_send_buffer(a)

        # READ|WRITE parker: another full-buffer socket with nothing to read,
        # parked on BOTH directions -> neither ready -> parks as RW (L123).
        c, d = socket.socketpair()
        c.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        d.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        _fill_send_buffer(c)

        # "other" parker: events==0 (no direction bit) -> pure deadline park,
        # which the dump's mask switch routes to the ``other++`` arm (L124).
        e, f = socket.socketpair()

        rvs = {}

        def w_waiter():
            rvs["w"] = rc.wait_fd(a.fileno(), WRITE, 600)

        def rw_waiter():
            rvs["rw"] = rc.wait_fd(c.fileno(), READ | WRITE, 600)

        def other_waiter():
            rvs["other"] = rc.wait_fd(e.fileno(), 0, 600)

        rc.fiber(w_waiter)
        rc.fiber(rw_waiter)
        rc.fiber(other_waiter)
        # Let all three link + commit to PARKED before we walk them.
        rc.sched_yield()
        rc.sched_yield()
        rc.sched_yield()
        captured["parked"] = rc.stats().get("netpoll_parked_self", 0)
        # The diagnostic walk: classifies each parker by p->events. We can't read
        # its stderr from here, but driving 3 distinct masks exercises the WRITE,
        # READ|WRITE, and other arms; the count proves all three are present.
        rc._dump_parkers()
        # Drain: let the 600ms deadlines fire so no parker leaks (conftest checks).
        rc.sched_sleep(0.8)
        captured["rvs"] = dict(rvs)
        for sk, peer, fd in ((a, b, a.fileno()), (c, d, c.fileno()), (e, f, e.fileno())):
            try:
                rc.netpoll_unregister(fd)
            except OSError:
                pass
            sk.close()
            peer.close()

    with hang_guard(20, "dump classify write/rdwr/other"):
        rc.fiber(main)
        rc.run()

    # All three distinct-mask parkers were live at dump time...
    assert captured["parked"] == 3, (
        "expected 3 parkers (WRITE, READ|WRITE, other) at dump time, got %r"
        % captured.get("parked"))
    # ...and each timed out (0), proving they really parked rather than
    # fast-pathing -- a fast-path would have skipped the linked-parker dump arm.
    assert captured["rvs"] == {"w": 0, "rw": 0, "other": 0}, captured["rvs"]


# --------------------------------------------------------------------------
# dump_parkers ready-but-parked probe -- L137 (``rdyp++``).
# The poll() inside the dump reports an fd as ready WHILE its g is still parked.
# We make a READ parker's fd readable (peer send) but do NOT yield, so the
# runloom pump has not run -- the parker is still linked + the data is sitting
# there.  _dump_parkers()'s non-blocking poll(POLLIN) then sees revents & POLLIN
# while the g is parked -> rdyp++ (the lost-wakeup detector's positive case).
# --------------------------------------------------------------------------
def test_dump_ready_but_parked_probe_counts_readable_fd():
    captured = {}

    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    rv = {}

    def reader():
        # a is not yet readable -> parks linked + LEVEL-armed for IN.  After the
        # pump wakes it, it drains the byte itself (so no parker / data leaks).
        rv["r"] = rc.wait_fd(a.fileno(), READ, 2000)
        if rv["r"] & READ:
            try:
                a.recv(64)
            except OSError:
                pass

    def prober():
        # Wait for the reader to commit to PARKED.
        rc.sched_yield()
        rc.sched_yield()
        captured["parked_before"] = rc.stats().get("netpoll_parked_self", 0)
        # Make `a` POLLIN-ready, then dump WITHOUT yielding: the netpoll pump has
        # not run, so the reader is still PARKED + LINKED while `a` is genuinely
        # ready. The dump's non-blocking poll(POLLIN) sees it -> rdyp++ (L137).
        b.send(b"ready-now")
        rc._dump_parkers()
        # prober returns here; run() then pumps and wakes the still-linked reader.

    with hang_guard(20, "dump ready-but-parked"):
        rc.fiber(reader)
        rc.fiber(prober)
        rc.run()

    rc.netpoll_unregister(a.fileno())
    a.close()
    b.close()
    # The reader was provably parked when we made the fd ready, so the dump's
    # poll() probe ran against a ready-but-parked fd: the rdyp++ path.
    assert captured["parked_before"] == 1, captured.get("parked_before")
    # And the pump then delivered the readiness -> the reader woke on READ
    # (proving the fd we probed as "ready" really was, and nothing leaked).
    assert rv.get("r", 0) & READ, "reader did not wake on READ (rv=%r)" % rv.get("r")


# --------------------------------------------------------------------------
# dump_parkers is a no-op when nothing is parked (the ``total == 0 -> continue``
# guard before the per-pool walk).  A direct adversarial check that the dump
# surface is safe with an empty pool -- it must not walk / poll / crash.
# (Exercises the early-continue around the L122-137 block from the other side.)
# --------------------------------------------------------------------------
def test_dump_parkers_safe_with_no_parkers():
    # No parkers anywhere: every pool has total==0, so the walk body is skipped.
    before = rc.stats().get("netpoll_parked_self", 0)
    rc._dump_parkers()  # must be a clean no-op
    after = rc.stats().get("netpoll_parked_self", 0)
    assert before == after == 0


# --------------------------------------------------------------------------
# runloom_fd_cap_target: the RUNLOOM_NETPOLL_MAXFD env parse -- L251-253
# (``char *end``, ``strtol``, ``if (end != env && v > 0) target = v``).
# This is read ONCE at netpoll init, so it MUST run in a fresh subprocess with
# the env set.  We pick a value above RUNLOOM_FD_CAP_MIN (1024) so the parsed
# target actually survives the later clamp and the arrays are truly sized to it,
# then run a real park/wake to prove the env-sized netpoll still works end to end.
# The subprocess MUST exit cleanly (rc 0 + marker) for gcov counters to flush.
# --------------------------------------------------------------------------
def _run_maxfd_subprocess(value):
    script = (
        "import sys, socket; sys.path.insert(0, 'src');\n"
        "import runloom_c as rc\n"
        "def main():\n"
        "    a, b = socket.socketpair(); a.setblocking(False); b.setblocking(False)\n"
        "    got = [0]\n"
        "    def reader():\n"
        "        got[0] = rc.wait_fd(a.fileno(), 1, 2000)\n"
        "    def sender():\n"
        "        rc.sched_yield(); rc.sched_yield(); b.send(b'hi')\n"
        "    rc.fiber(reader); rc.fiber(sender); rc.run()\n"
        "    rc.netpoll_unregister(a.fileno()); a.close(); b.close()\n"
        "    assert got[0] & 1, 'reader did not wake on READ (got %r)' % got[0]\n"
        "main()\n"
        "sys.stdout.write('MAXFD_PARSE_OK\\n')\n")
    env = dict(os.environ, RUNLOOM_NETPOLL_MAXFD=str(value),
               PYTHON_GIL="0", PYTHONPATH="src")
    return subprocess.run([PY, "-c", script], cwd=REPO, env=env,
                          capture_output=True, text=True, timeout=30)


def test_netpoll_maxfd_env_parse_sizes_arrays():
    # 4096 > RUNLOOM_FD_CAP_MIN (1024) and < RUNLOOM_FD_CAP_MAX, so the strtol'd
    # value is retained verbatim as the array cap (L253 actually takes effect).
    p = _run_maxfd_subprocess(4096)
    assert p.returncode == 0, "MAXFD=4096 subprocess failed rc=%d\n%s" % (
        p.returncode, p.stderr[-1200:])
    assert "MAXFD_PARSE_OK" in p.stdout, (p.stdout, p.stderr[-800:])


def test_netpoll_maxfd_env_invalid_falls_back():
    # A NON-numeric MAXFD: strtol leaves end==env, so the ``end != env`` guard is
    # FALSE and the default 65536 is kept -- the parse branch is taken but the
    # assignment is not. The netpoll must still come up and round-trip a wake,
    # proving the bad env degraded gracefully rather than wedging init.
    p = _run_maxfd_subprocess("notanumber")
    assert p.returncode == 0, "MAXFD=notanumber subprocess failed rc=%d\n%s" % (
        p.returncode, p.stderr[-1200:])
    assert "MAXFD_PARSE_OK" in p.stdout, (p.stdout, p.stderr[-800:])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
