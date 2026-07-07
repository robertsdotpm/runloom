"""Pytest entry for the remote-internet suite.

The OFFLINE codec/loader self-tests always run (pure, no network -- safe wherever
pytest collects this file).  The LIVE probe is gated behind RUNLOOM_NET_TESTS=1
AND @pytest.mark.network, so a bare `pytest` never touches the wire.  This file
is NOT reached by tests/run_isolated.py (the merge gate), which discovers only
top-level tests/test_*.py.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import protocols   # noqa: E402
import netlist     # noqa: E402


def test_protocols_offline_roundtrip():
    """STUN/NTP/MQTT encode+decode round-trips (no network)."""
    protocols._selftest()


def test_netlist_offline_fallback():
    """Loader pinned-fallback + score-ordering + finding emit (no network)."""
    netlist._selftest()


@pytest.mark.network
@pytest.mark.skipif(os.environ.get("RUNLOOM_NET_TESTS") != "1",
                    reason="opt-in: set RUNLOOM_NET_TESTS=1 to run live network probes")
def test_live_stun_udp_smoke():
    """One live STUN/UDP round-trip through the netpoll path (opt-in)."""
    import subprocess
    src = os.path.join(os.path.dirname(os.path.dirname(HERE)), "src")
    env = dict(os.environ)
    env["PYTHON_GIL"] = "0"
    env["RUNLOOM_NET_TESTS"] = "1"
    env["PYTHONPATH"] = src + os.pathsep + env.get("PYTHONPATH", "")
    rc = subprocess.call(
        [sys.executable, os.path.join(HERE, "n05_stun_udp_testnat.py"),
         "--hubs", "4", "--top", "8", "--timeout", "3"], env=env)
    # PASS(0) or SKIP(77) are both acceptable (SKIP = all servers ENV / no net);
    # only a real FINDING/CRASH/HANG (1/2/3) fails the smoke.
    assert rc in (0, netlist.SKIP), "live STUN/UDP smoke returned %d" % rc
