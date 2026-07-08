"""Partition-fault timeout coverage (QA-steal-V2 #19, blackhole vs RST).

A TCP peer can vanish two ways.  A *reset* (RST) or a clean *close* (FIN) delivers
an event -- the netpoll wakes the parked reader immediately (recv returns b'' or
raises ConnectionReset).  A *blackhole* delivers **nothing**: the connection stays
ESTABLISHED in the kernel's view, no FIN, no RST, no data ever arrives.  A reader
parked on such a socket has exactly one thing that can free it -- its **own**
netpoll timeout (the deadline timer), never a network event.  If the netpoll only
re-arms on readiness and the timer path is broken, the reader hangs forever.

Two realizations of the same assertion (a parked reader is released by its own
timer, not by any peer event):

* ``test_silent_established_reader_times_out`` -- ALWAYS runs, deterministic, no
  privilege.  The peer sends partial data then goes silent holding the connection
  open (no FIN).  The reader, having consumed the data, parks with a socket
  timeout; only the netpoll timer can release it.

* ``test_netns_blackhole_reader_times_out`` -- OPT-IN (``RUNLOOM_NETNS_TESTS=1``),
  the stronger environmental realization.  Inside an isolated network namespace
  (``unshare -rn``, host firewall untouched) the loopback is blackholed with
  ``tc netem loss 100%`` *after* a byte flows, so even the server's FIN is
  dropped -- the connection is truly dead-but-established and the kernel can
  deliver the reader nothing.  A ``TIMEOUT`` (not ``b''``) proves liveness came
  from the timer alone.  Skips cleanly when unshare/tc/netem are unavailable;
  default-skip keeps host-firewall / privilege-sensitive machinery out of the
  gate (see the remote-net suite's opt-in discipline).
"""
import os
import subprocess
import sys
import textwrap
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO, "src")
PY = sys.executable

# Each scenario runs runloom.run() in its own subprocess (a clean runtime per
# case, matching the one-run()-per-process model of the isolated runner).  The
# BLACKHOLE env toggle selects the dead-but-established variant (bring lo up +
# netem 100% loss + server close) over the silent-hold variant.
WORKER = textwrap.dedent("""\
    import os, sys, subprocess, time
    sys.path.insert(0, {src!r})
    import runloom, runloom_c
    runloom.monkey.patch()
    import socket

    BLACKHOLE = os.environ.get("PG_BLACKHOLE") == "1"
    if BLACKHOLE:
        subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    out = {{}}

    def worker():
        srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
        port = srv.getsockname()[1]
        cli = socket.socket(); cli.connect(("127.0.0.1", port))
        conn, _ = srv.accept()
        conn.sendall(b"X")                      # partial data flows first
        assert cli.recv(1) == b"X", "pre-partition flow failed"
        if BLACKHOLE:
            # Drop ALL loopback traffic: the server's FIN below never arrives, so
            # the connection is dead-but-ESTABLISHED and the kernel delivers the
            # reader nothing -- only its own timer can free it.
            subprocess.run(["tc", "qdisc", "replace", "dev", "lo", "root",
                            "netem", "loss", "100%"], check=True)
            conn.close()
        # else: server holds the connection open, silent (no FIN either).
        cli.settimeout(1.0)
        t0 = time.monotonic()
        try:
            r = cli.recv(1)                     # parks: no data, no FIN -> timer
            out["r"] = "EOF" if r == b"" else "DATA"
        except socket.timeout:
            out["r"] = "TIMEOUT"
        except Exception as e:
            out["r"] = type(e).__name__
        out["dt"] = time.monotonic() - t0

    runloom.run(2, main_fn=lambda: runloom.fiber(worker))
    print("RESULT", out.get("r"), "%.2f" % out.get("dt", -1))
    """).format(src=SRC)


def _base_env():
    return dict(os.environ, PYTHON_GIL="0", PYTHON_TLBC="0", PYTHONPATH=SRC)


def _parse(stdout):
    line = [ln for ln in stdout.splitlines() if ln.startswith("RESULT")]
    if not line:
        return None, None
    _, verdict, dt = line[-1].split()
    return verdict, float(dt)


class TestBlackholeTimeout(unittest.TestCase):
    def test_silent_established_reader_times_out(self):
        """No privilege: peer silent after partial data, timer must free the reader."""
        r = subprocess.run([PY, "-c", WORKER], capture_output=True, text=True,
                           timeout=60, env=_base_env())
        verdict, dt = _parse(r.stdout)
        self.assertEqual(verdict, "TIMEOUT",
                         "reader not released by its own timer on a silent "
                         "established conn: verdict={0!r} rc={1} err={2!r}"
                         .format(verdict, r.returncode, r.stderr[-400:]))
        # Released by the ~1.0s timer, not a stray immediate wake or a hang.
        self.assertLess(dt, 8.0, "timeout fired far too late ({0:.2f}s)".format(dt))
        self.assertGreater(dt, 0.5, "woke before the deadline ({0:.2f}s)".format(dt))

    def test_netns_blackhole_reader_times_out(self):
        """Opt-in: real blackhole (netem 100% loss) drops even the FIN; only the
        netpoll timer can free the reader.  A TIMEOUT (not EOF) is the proof."""
        if os.environ.get("RUNLOOM_NETNS_TESTS") != "1":
            self.skipTest("netns blackhole is opt-in: set RUNLOOM_NETNS_TESTS=1")
        for tool in ("unshare", "tc", "ip"):
            if subprocess.run(["sh", "-c", "command -v " + tool],
                              capture_output=True).returncode != 0:
                self.skipTest(tool + " unavailable")
        # Probe: can we make a userns netns + netem on lo here at all?
        probe = subprocess.run(
            ["unshare", "-rn", "sh", "-c",
             "ip link set lo up && tc qdisc add dev lo root netem loss 100%"],
            capture_output=True, text=True)
        if probe.returncode != 0:
            self.skipTest("no userns netns / netem here: " + probe.stderr.strip()[-200:])
        env = dict(_base_env(), PG_BLACKHOLE="1")
        r = subprocess.run(["unshare", "-rn", PY, "-c", WORKER],
                           capture_output=True, text=True, timeout=90, env=env)
        verdict, dt = _parse(r.stdout)
        self.assertEqual(verdict, "TIMEOUT",
                         "blackholed reader not released by its own timer "
                         "(EOF => a FIN slipped through; hang => timer dead): "
                         "verdict={0!r} rc={1} err={2!r}"
                         .format(verdict, r.returncode, r.stderr[-400:]))
        self.assertLess(dt, 8.0, "blackhole timeout fired far too late ({0:.2f}s)".format(dt))
        self.assertGreater(dt, 0.5, "blackhole reader woke before deadline ({0:.2f}s)".format(dt))


if __name__ == "__main__":
    unittest.main()
