"""big_100 / 224 -- per-hub epoll cross-hub lost-park (RUNLOOM_PERHUB_EPOLL).

The primary Linux netpoll architecture is PER-HUB epoll (default ON since
2026-06-16): each hub owns its own epoll fd plus a per-hub wake-eventfd, and a
socket fd is registered/parked in whatever hub the parking goroutine lands on.
The exact bug fixed at b46a586 was *lost-park-across-hubs*: an fd armed on hub
A but whose readiness is driven from a goroutine that lands on hub B, where the
wake must route back to A's pump via A's wake-eventfd.  Nothing in the campaign
names per-hub epoll as the system under test, parks an fd across hubs, or
compares the =1 (per-hub) vs =0 (legacy shared) backends for parity.

This program builds many socketpairs.  For each pair it spawns TWO goroutines
(spawned in two separate passes so they fan out across different hubs): a
'parker' that blocks in recv on one end -- arming/parking that fd on whatever
hub it lands on -- and a 'writer' that jitter-sleeps then writes a tagged byte
to the partner end (driven from, with high probability, a DIFFERENT hub).  The
oracle: every parker must wake with the EXACT byte its partner wrote, inside the
watchdog window.  A lost cross-hub wake strands the parker, no byte is observed,
and the watchdog/_dump_parkers fires (readyParked>0 = lost wakeup).

It is meaningful only on Linux-epoll (the toggle is a no-op under kqueue/iocp/
select), so it SKIPs cleanly off epoll.  Because the backend is resolved ONCE
per process from RUNLOOM_PERHUB_EPOLL (read before the hubs start), the =1-vs-=0
parity comparison can't happen in one scheduler: the top-level invocation
re-execs itself as two child sub-runs (one per backend) and asserts both PASS
with sane throughput; a child sub-run (RUNLOOM_PERHUB_SUBMODE set) runs the
workload under exactly one backend.

Stresses: per-hub epoll fd registration + per-hub wake-eventfd routing under cross-hub fd parking; the lost-park-across-hubs path (fd armed on hub A, readiness/peer-write driven from a goroutine landing on hub B); =0 vs =1 backend toggle parity.
"""
import os
import socket
import sys

# A child sub-run carries the chosen backend in RUNLOOM_PERHUB_EPOLL already;
# the only thing we must guarantee is that env is set BEFORE runloom_c is
# imported (the backend is resolved-once via getenv before the hubs start).
# A top-level (non-child) run leaves the inherited default (=1) in place for its
# own quick self-probe and drives the real comparison via re-exec children.
_SUBMODE = os.environ.get("RUNLOOM_PERHUB_SUBMODE")

import harness        # noqa: E402  (harness imports runloom_c after env is set)
import runloom        # noqa: E402
import runloom_c      # noqa: E402

# One tagged byte travels each socketpair; the parker checks it equals the byte
# its writer partner sent (tag = pair index, low 8 bits -- enough to catch a
# cross-pair / cross-hub mis-route, which is what a lost-park bug looks like).
PAIRS_PER_ROUND = 1     # one rendezvous per round per worker; --rounds scales it


def _make_pairs(n):
    """n connected socketpairs, both ends non-blocking (cooperative recv/send
    park the goroutine under monkey.patch())."""
    pairs = []
    for _ in range(n):
        a, b = socket.socketpair()
        a.setblocking(False)
        b.setblocking(False)
        pairs.append((a, b))
    return pairs


def parker(H, wid, rng, pairs):
    """Block in recv on end-A of this worker's pair; assert the byte that
    arrives is exactly the tag the writer partner will send.  recv parks the
    A-end fd on whatever hub THIS goroutine lands on."""
    a, _b = pairs[wid]
    expect = bytes([wid & 0xFF])
    for _ in H.round_range():
        try:
            # Park on the A-end.  Under per-hub epoll this arms the fd in this
            # goroutine's hub; the wake will be driven from the writer's hub.
            got = a.recv(1)
        except OSError:
            if not H.running():
                break
            continue
        if not got:
            # EOF: only legitimate at teardown (closeables closed).
            if not H.running():
                break
            H.check(False, "parker wid={0} got EOF before tag".format(wid))
            return
        if not H.check(got == expect,
                       "cross-hub wake mis-route wid={0}: got {1!r} want "
                       "{2!r}".format(wid, got, expect)):
            return
        H.op(wid)
        H.task_done(wid)


def writer(H, wid, rng, pairs):
    """Sleep a jittered moment (so the parker is parked first), then write the
    tagged byte to end-B.  Spawned in a separate pass to maximise landing on a
    DIFFERENT hub than its parker -> exercises the cross-hub wake route."""
    _a, b = pairs[wid]
    tag = bytes([wid & 0xFF])
    for _ in H.round_range():
        # Jitter so the parker is reliably parked when the write lands -- the
        # write must then DRIVE the wake (the cross-hub path), not merely fill a
        # buffer the parker drains synchronously.
        H.sleep(0.001 + rng.random() * 0.02)
        if not H.running():
            break
        try:
            b.sendall(tag)
        except OSError:
            if not H.running():
                break


def worker(H, wid, rng, pairs):
    # Unused: spawning is split into two explicit passes in body() so parkers
    # and writers fan out across hubs independently.  Kept for run_pool's
    # worker(H, wid, rng, *extra) signature when invoked directly is not used.
    parker(H, wid, rng, pairs)


def setup(H):
    n = H.funcs
    pairs = _make_pairs(n)
    for a, b in pairs:
        H.register_close(a)
        H.register_close(b)
    H.state = pairs


def body(H):
    pairs = H.state
    n = len(pairs)
    # Pass 1: all parkers.  Pass 2: all writers.  Two separate run_pool calls
    # spawn the two roles in distinct waves, so a parker[wid] and writer[wid]
    # very likely land on different hubs -> the byte writer[wid] sends must wake
    # parker[wid] across hubs (the lost-park-across-hubs path).
    H.run_pool(n, parker, pairs)
    H.run_pool(n, writer, pairs)


def post(H):
    H.check(H.total_ops() > 0,
            "no cross-hub wakes completed (every parker stranded?)")
    H.log("perhub_epoll={0} cross_hub_wakes={1}".format(
        os.environ.get("RUNLOOM_PERHUB_EPOLL", "1"), H.total_ops()))


# ---------------------------------------------------------------------------
# Top-level comparison driver: re-exec this module twice (perhub=1 and =0),
# assert both PASS and both moved a sane number of cross-hub wakes.  A child
# sub-run (RUNLOOM_PERHUB_SUBMODE set) skips this and runs the workload above
# under whatever backend its inherited RUNLOOM_PERHUB_EPOLL selected.
# ---------------------------------------------------------------------------
def _run_child(backend):
    """Re-exec this exact program as a sub-run with RUNLOOM_PERHUB_EPOLL=backend.
    Returns (exit_code, ops) parsed from the child's RESULTS block."""
    import subprocess
    env = dict(os.environ)
    env["RUNLOOM_PERHUB_EPOLL"] = backend
    env["RUNLOOM_PERHUB_SUBMODE"] = "1"
    # Pass through the same CLI args so --hubs/--funcs/--rounds/--duration etc.
    # apply identically to both backends.
    argv = [sys.executable, os.path.abspath(__file__)] + sys.argv[1:]
    proc = subprocess.run(argv, env=env, stderr=subprocess.PIPE,
                          stdout=subprocess.DEVNULL)
    err = proc.stderr.decode("utf-8", "replace")
    sys.stderr.write(err)
    sys.stderr.flush()
    ops = 0
    for line in err.splitlines():
        s = line.strip()
        if s.startswith("ops ") and ":" in s:
            try:
                ops = int(s.split(":", 1)[1].strip())
            except ValueError:
                pass
    return proc.returncode, ops


def _compare_driver():
    """Drive both backends and assert =1/=0 parity.  Exit 0 PASS / 1 FAIL /
    2 error, matching the harness exit-code contract."""
    if runloom_c.netpoll_backend() != "epoll":
        print("SKIP: per-hub epoll toggle is meaningless off Linux-epoll "
              "(backend={0})".format(runloom_c.netpoll_backend()))
        return 0
    sys.stderr.write("[p224 driver] comparing RUNLOOM_PERHUB_EPOLL=1 vs =0 "
                     "via two child sub-runs\n")
    sys.stderr.flush()
    rc1, ops1 = _run_child("1")
    rc0, ops0 = _run_child("0")
    sys.stderr.write(
        "[p224 driver] perhub=1 -> exit {0} ops {1}; perhub=0 -> exit {2} "
        "ops {3}\n".format(rc1, ops1, rc0, ops0))
    sys.stderr.flush()
    ok = True
    if rc1 != 0:
        sys.stderr.write("[p224 driver] FAIL: per-hub epoll (=1) sub-run "
                         "exit {0}\n".format(rc1))
        ok = False
    if rc0 != 0:
        sys.stderr.write("[p224 driver] FAIL: legacy shared epoll (=0) sub-run "
                         "exit {0}\n".format(rc0))
        ok = False
    # Throughput-sane parity: both backends must have moved cross-hub wakes, and
    # neither may be wildly starved relative to the other (a stranded-parker
    # regression on one backend would show as ~0 ops there while the other ran).
    if ok and (ops1 <= 0 or ops0 <= 0):
        sys.stderr.write("[p224 driver] FAIL: a backend completed 0 cross-hub "
                         "wakes (ops1={0} ops0={1})\n".format(ops1, ops0))
        ok = False
    if ok:
        lo, hi = sorted((ops1, ops0))
        if lo * 50 < hi:
            sys.stderr.write(
                "[p224 driver] FAIL: throughput parity off >50x (ops1={0} "
                "ops0={1}) -- one backend likely stranding parkers\n".format(
                    ops1, ops0))
            ok = False
    verdict = "PASS" if ok else "FAIL"
    sys.stderr.write("[p224 driver] VERDICT: {0}\n".format(verdict))
    sys.stderr.flush()
    return 0 if ok else 1


if __name__ == "__main__":
    # Availability guard (both the driver and a directly-run child honour it).
    if runloom_c.netpoll_backend() != "epoll":
        print("SKIP: per-hub epoll toggle is meaningless off Linux-epoll "
              "(backend={0})".format(runloom_c.netpoll_backend()))
        sys.exit(0)

    if _SUBMODE:
        # Child sub-run: execute the workload under the inherited backend.
        harness.main(
            "p224_perhub_epoll_cross_hub_wake", body, setup=setup, post=post,
            default_funcs=2000,
            describe="cross-hub fd park/wake under one RUNLOOM_PERHUB_EPOLL "
                     "backend (sub-run): every parker must wake with its "
                     "writer's exact tagged byte")
    else:
        # Top-level: compare =1 vs =0 across two child sub-runs.
        sys.exit(_compare_driver())
