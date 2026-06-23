"""big_100 / 230 -- netpoll churn-sweep cap + MAXFD arm-table boundary.

A high-churn short-lived-connection storm against one local echo server, run
with the netpoll dead-fd link sweep deliberately THROTTLED and the per-fd
arm-cache table deliberately SMALL, so two normally-invisible mechanisms get
pushed to their edges at once:

  * RUNLOOM_SWEEP_MAX_CHURN caps how many dead-fd registration links the
    periodic sweep may reap per second.  Each worker connects, does ONE tagged
    round-trip, and closes -- churning fd numbers (and netpoll registrations)
    far faster than the capped sweep can reap them, so the dead-link list grows
    a backlog.  Forward progress must stay sustained: a wedged sweep would stall
    every fresh registration behind the backlog and the watchdog would fire.

  * RUNLOOM_NETPOLL_MAXFD sizes the per-fd arm-cache / pending-wake tables (set
    to 4096 here -- well below the raised RLIMIT_NOFILE).  A pool of K long-lived
    idle connections is held open to push the LIVE fd numbers up toward that cap,
    exercising the high end of the arm table where an off-by-one in the bounds
    check would drop a readiness event.

The oracle is the round-trip itself: every short-lived connection sends a tag
unique to (wid, seq) and must read back exactly those bytes.  A stale arm-cache
entry for a reused or high-numbered fd, or a dropped event from a table
off-by-one, loses the readiness -> the recv hangs (watchdog) or returns the
wrong bytes (mismatch).  Bounded fd_end at teardown proves the throttled sweep
still reaps the backlog (no link/fd leak).  RUNLOOM_DBG_NETPOLL=1 traces the
stale-arm tripwire if set.

Linux/epoll-specific: the per-fd arm cache and one-shot re-arm live only on the
epoll backend.  On any other backend the program prints SKIP and exits 0.

Stresses: Stresses: netpoll dead-fd link sweep under a high-churn short-lived-connection storm with RUNLOOM_SWEEP_MAX_CHURN capping the sweep rate, and the per-fd arm-cache table (RUNLOOM_NETPOLL_MAXFD) exercised with fd numbers near the configured cap; correctness of every round-trip despite sweep backlog.
"""
import os
import socket
import struct
import sys

# Drive the two netpoll knobs.  These are read via getenv() deep in the C
# extension at netpoll init / sweep time, so they MUST be set before runloom_c
# is imported (which the harness does on import).  setdefault so an explicit
# environment override (a sweep-tuning run) still wins.
#
#   MAXFD 4096 sizes the arm-cache / pending-wake tables small (>= the 1024 cap
#   floor, << the 8M+ raised RLIMIT_NOFILE), so the K idle-conn pool can push
#   live fd numbers toward the high end of the table.
#
#   SWEEP_MAX_CHURN 64 throttles the dead-fd link sweep to 64 links/sec -- far
#   below the connect/close churn rate -- so a reap backlog accumulates and the
#   "does the throttle wedge fresh registration?" path gets exercised.
os.environ.setdefault("RUNLOOM_NETPOLL_MAXFD", "4096")
os.environ.setdefault("RUNLOOM_SWEEP_MAX_CHURN", "64")

import harness          # noqa: E402
import netutil          # noqa: E402
import runloom          # noqa: E402
import runloom_c        # noqa: E402

# How many long-lived idle connections to hold open, pushing live fd numbers up
# toward the small MAXFD cap.  Bounded (box-safe): each is one parked recv
# goroutine + one held socket, well under the 4096 table.
IDLE_POOL = 1500


def idle_holder(H, addr, hold):
    """Open one connection and hold it idle (parked in recv) for the whole run,
    keeping its fd number allocated so fresh churn fds climb toward MAXFD."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(addr)
        hold.append(s)
        H.register_close(s)
        # Park on a never-arriving byte (re-probe so we re-check running() at
        # teardown and don't strand the join; close() also unblocks us).
        while H.running():
            if runloom_c.wait_fd(s.fileno(), 1, 250) & 1:
                if not s.recv(1):
                    break    # server closed -> stop holding
    except OSError:
        pass
    finally:
        netutil.close_quiet(s)


def churn_round_trip(addr, tag):
    """Connect, send ONE tag, read it back, close.  Churns one fd number and one
    netpoll registration (which becomes a dead link the throttled sweep reaps).
    Returns the echoed bytes, or None on a connection-level OSError."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(addr)
        s.sendall(tag)
        return netutil.recv_exact(s, len(tag))
    except OSError:
        return None
    finally:
        netutil.close_quiet(s)


def worker(H, wid, rng, state):
    addr = (state["host"], state["port"])
    # Spread the initial connect storm deterministically.
    H.sleep(rng.random() * 0.4)
    seq = 0
    for _ in H.round_range():
        seq += 1
        # A tag unique to this (wid, seq): if a stale arm-cache entry for a
        # reused/high fd lost a readiness or a table off-by-one dropped an
        # event, we'd either hang or read someone else's bytes.
        tag = struct.pack("<III", 0xC0DE0000 ^ (wid & 0xFFFF), seq, wid)
        got = churn_round_trip(addr, tag)
        if got is None:
            if not H.running():
                break
            continue
        if not H.check(got == tag,
                       "round-trip mismatch (stale arm / dropped event) "
                       "wid={0} seq={1}: sent {2!r} got {3!r}".format(
                           wid, seq, tag, got)):
            return
        H.op(wid)
        H.task_done(wid)


def setup(H):
    # Bind the echo server on the SAME explicit loopback IP the workers dial
    # (netutil's _DEFAULT_HOST is frozen at import time and may not match this
    # job's net_ips[0]).  RLIMIT_NOFILE is already raised by the harness.
    host = H.net_ip(0)
    port = netutil.start_echo_server(H, host=host)
    H.state = {"host": host, "port": port}

    # Hold a pool of long-lived idle connections to push live fd numbers toward
    # the small RUNLOOM_NETPOLL_MAXFD cap.  Bounded by IDLE_POOL and never grows.
    hold = []
    addr = (host, port)
    for _ in range(IDLE_POOL):
        H.fiber(idle_holder, H, addr, hold)
    H.state["idle_hold"] = hold


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # The throttled sweep must still have made forward progress: round-trips
    # completed AND the dead-link backlog was reaped (bounded fd growth).  The
    # harness reports leaked_fds; here we just assert the workload ran.
    H.check(H.total_ops() > 0, "no round-trips completed (sweep wedged?)")
    H.log("round_trips={0} maxfd={1} sweep_max_churn={2} idle_pool={3}".format(
        H.total_ops(), os.environ.get("RUNLOOM_NETPOLL_MAXFD"),
        os.environ.get("RUNLOOM_SWEEP_MAX_CHURN"), IDLE_POOL))


if __name__ == "__main__":
    if runloom_c.netpoll_backend() != "epoll":
        print("SKIP: netpoll churn-sweep + MAXFD arm-table test is Linux/epoll "
              "specific (backend={0})".format(runloom_c.netpoll_backend()))
        sys.exit(0)
    harness.main("p230_netpoll_churn_sweep_maxfd", body, setup=setup, post=post,
                 default_funcs=3000,
                 describe="high-churn one-RT connection storm with a THROTTLED "
                          "dead-fd sweep (RUNLOOM_SWEEP_MAX_CHURN) and a SMALL "
                          "arm-table (RUNLOOM_NETPOLL_MAXFD); idle pool pushes "
                          "live fds toward the cap; every round-trip echoes exact")
