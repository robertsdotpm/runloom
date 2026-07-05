"""Canary client fleet driver (docs/dev/RELIABILITY_PROGRAM.md R6).

Drives continuous, modest, realistic load at the canary server so its gauges
have work to reflect: steady echo round-trips + chat participation, plus a
periodic CHURN BURST (a wave of short-lived connections) that ages the
connection create/destroy cycle -- the shape a real service sees daily.

Runs its OWN runloom scheduler (it is itself a runloom program), so a canary
deployment is runloom-on-both-ends.  Can also run on the Windows VMs via the
existing SSH tooling, or locally in a netns (tools/soak/netns_chaos.sh) for a
lossy-path canary.

Usage:
  python3 examples/canary/client.py --host 127.0.0.1 --seconds 0   # until ^C
      [--echo-clients 16] [--chat-clients 8] [--burst-every 60] [--burst-n 200]
"""
import argparse
import os
import socket
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import time as _time
import runloom
import runloom.monkey
runloom.monkey.patch()
import runloom_c

_STOP = [False]


def _deadline_expired(start, seconds):
    return seconds and (_time.monotonic() - start) >= seconds


def _echo_client(host, port, start, seconds):
    while not _STOP[0] and not _deadline_expired(start, seconds):
        try:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((host, port))
            for _ in range(50):
                if _STOP[0] or _deadline_expired(start, seconds):
                    break
                c.sendall(b"ping")
                if c.recv(64) != b"ping":
                    break
                runloom_c.sched_sleep(0.02)
            c.close()
        except OSError:
            runloom_c.sched_sleep(0.5)   # server not up yet / transient


def _chat_client(host, port, start, seconds, cid):
    while not _STOP[0] and not _deadline_expired(start, seconds):
        try:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((host, port))
            # a reader fiber drains broadcasts; the main loop posts messages.
            def drain():
                try:
                    while not _STOP[0]:
                        if not c.recv(4096):
                            break
                except OSError:
                    pass
            runloom.fiber(drain)
            for i in range(30):
                if _STOP[0] or _deadline_expired(start, seconds):
                    break
                c.sendall(b"hello from %d msg %d\n" % (cid, i))
                runloom_c.sched_sleep(0.1)
            c.close()
        except OSError:
            runloom_c.sched_sleep(0.5)


def _churn_burst(host, port, n):
    # a wave of short-lived connect/echo/close -- ages the connection lifecycle.
    def one():
        try:
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect((host, port))
            c.sendall(b"x")
            c.recv(16)
            c.close()
        except OSError:
            pass
    for _ in range(n):
        if _STOP[0]:
            break
        runloom.fiber(one)


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--echo-port", type=int, default=8801)
    ap.add_argument("--chat-port", type=int, default=8802)
    ap.add_argument("--echo-clients", type=int, default=16)
    ap.add_argument("--chat-clients", type=int, default=8)
    ap.add_argument("--burst-every", type=float, default=60.0)
    ap.add_argument("--burst-n", type=int, default=200)
    ap.add_argument("--seconds", type=float, default=0)
    args = ap.parse_args(argv)

    import signal
    signal.signal(signal.SIGTERM, lambda *_: _STOP.__setitem__(0, True))
    signal.signal(signal.SIGINT, lambda *_: _STOP.__setitem__(0, True))

    start = _time.monotonic()

    def root():
        for _ in range(args.echo_clients):
            runloom.fiber(lambda: _echo_client(
                args.host, args.echo_port, start, args.seconds))
        for i in range(args.chat_clients):
            runloom.fiber(lambda i=i: _chat_client(
                args.host, args.chat_port, start, args.seconds, i))

        def burster():
            nxt = _time.monotonic() + args.burst_every
            while not _STOP[0] and not _deadline_expired(start, args.seconds):
                if _time.monotonic() >= nxt:
                    _churn_burst(args.host, args.echo_port, args.burst_n)
                    nxt = _time.monotonic() + args.burst_every
                runloom_c.sched_sleep(0.5)
        runloom.fiber(burster)
    runloom.fiber(root)
    runloom_c.run()
    print("[canary-client] stopped after %.0fs" % (_time.monotonic() - start))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
