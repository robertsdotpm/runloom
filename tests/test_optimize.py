"""runloom.optimize(*goals, max_fibers): one call, named trade-offs, that maps to
the internal RUNLOOM_* tuning knobs.  Pins the contract: valid goals, precedence
(secure > memory > latency > throughput), shell-env wins, and that the runtime
still runs after a call.
"""
import os

import pytest

import runloom
from runloom._optimize import _GOAL_ENV, GOALS


def test_unknown_goal_raises():
    with pytest.raises(ValueError):
        runloom.optimize("turbo")


def test_no_goals_applies_nothing():
    assert runloom.optimize() == {}


def test_memory_bundle():
    applied = runloom.optimize("memory")
    assert applied["RUNLOOM_STACK_MADV"] == "dontneed"        # eager reclaim
    assert applied["RUNLOOM_STACK_PARK_DONTNEED"] == "1"      # drop idle parked pages


def test_throughput_bundle():
    applied = runloom.optimize("throughput")
    assert applied["RUNLOOM_TCPCONN_IOURING"] == "auto"
    assert applied["RUNLOOM_GON_BULK"] == "1"
    # pool size is AUTO now (sizes to live high-water) -- throughput sets no static cap,
    # and must NOT disable reclaim (the keep-alive OOM footgun)
    assert "RUNLOOM_STACK_DEPOT_CAP" not in applied
    assert "RUNLOOM_STACK_MADV" not in applied


def test_compose_is_the_union_of_bundles():
    applied = runloom.optimize("throughput", "memory")
    assert applied["RUNLOOM_TCPCONN_IOURING"] == "auto"       # from throughput
    assert applied["RUNLOOM_STACK_MADV"] == "dontneed"        # from memory


def test_secure_scrub_lands_when_composed():
    applied = runloom.optimize("throughput", "secure")
    assert applied["RUNLOOM_STACK_SCRUB"] == "1"


def test_max_fibers():
    applied = runloom.optimize(max_fibers=12345)
    assert applied["RUNLOOM_MAX_GOROUTINES"] == "12345"


def test_all_goal_values_are_well_formed():
    # every bundled value is a non-empty str (env vars), goals are the 4 trades.
    assert set(GOALS) == {"throughput", "latency", "memory", "secure"}
    for g, env in _GOAL_ENV.items():
        for k, v in env.items():
            assert k.startswith("RUNLOOM_") and isinstance(v, str) and v


def test_shell_env_wins(monkeypatch):
    monkeypatch.setenv("RUNLOOM_STACK_MADV", "free")
    runloom.optimize("memory")                 # wants dontneed
    assert os.environ["RUNLOOM_STACK_MADV"] == "free"   # explicit shell export wins


def test_runs_after_optimize():
    runloom.optimize("memory")
    done = bytearray(200)

    def main():
        def w(i):
            done[i] = 1
        for i in range(200):
            runloom.fiber(w, i)

    runloom.run(4, main)
    assert sum(done) == 200
