"""pytest-benchmark front-end for the core pygo microbenchmarks.

This gives the *standard* Python benchmarking tool -- calibrated rounds,
auto warmup, JSON save/compare -- alongside bench.harness (which exists for
the richer environment provenance + custom stats the gate needs).  Use this
when you want the familiar pytest-benchmark workflow:

    PYTHONPATH=src PYTHON_GIL=0 python3 -m pytest bench/test_bench.py \
        --benchmark-only --benchmark-sort=name \
        --benchmark-columns=min,median,ops,rounds,stddev

    # save a labelled baseline and compare a later run against it:
    ... --benchmark-save=baseline
    ... --benchmark-compare=0001 --benchmark-compare-fail=median:10%

PYTHON_GIL=0 is MANDATORY: pytest-benchmark's collection pulls a Brotli
codec that otherwise re-enables the GIL.  bench/conftest.py fails the
session loudly if you forget it (see bench/gil.py).

Requires the free-threaded 3.13t interpreter (== `python3` here) and
PYTHONPATH=src for the in-place pygo_core build.  Inner counts are smaller
than bench.micro's because pytest-benchmark runs many calibrated rounds.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest  # noqa: E402

from bench.harness import default_pin_set, pin  # noqa: E402
from bench.micro import (  # noqa: E402
    make_buffered, make_pingpong, make_spawn, make_yield)


@pytest.fixture(scope="session", autouse=True)
def _pin_to_one_numa_node():
    """Pin the whole pytest process to one NUMA node, same as bench.harness,
    so these numbers are comparable to the harness suite."""
    pin(default_pin_set(n=8, node=1))
    yield


def test_spawn_run(benchmark):
    benchmark(make_spawn(1_000))


def test_yield_100coro(benchmark):
    benchmark(make_yield(100, 100))


def test_yield_1000coro(benchmark):
    benchmark(make_yield(1_000, 10))


def test_chan_unbuffered_pingpong(benchmark):
    benchmark(make_pingpong(10_000))


def test_chan_buffered(benchmark):
    benchmark(make_buffered(50_000, 64))
