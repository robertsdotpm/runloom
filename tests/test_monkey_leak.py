"""Leak / resource-balance regression tests for the monkey layer.

Wraps tools/leak_check.check_leak as pytest tests: each cooperative-stdlib
workload, run many times, must return to its post-warmup object- and fd-count
baseline.  Targets runloom's leak history (FD leaks, task<->driver cycles) now
extended to the monkey layer's new allocation surface (thread-pool offload,
DNS cache, subprocess pipes, cooperative wrappers).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import runloom.monkey

runloom.monkey.patch()

from tools.leak_check import (check_leak, _wl_socketpair, _wl_simplequeue,
                              _wl_file_offload, _wl_subprocess)


def test_no_leak_socketpair():
    check_leak(_wl_socketpair, iters=60, name="socketpair")


def test_no_leak_simplequeue():
    check_leak(_wl_simplequeue, iters=60, name="simplequeue")


def test_no_leak_file_offload():
    check_leak(_wl_file_offload, iters=50, name="file_offload")


def test_no_leak_subprocess():
    check_leak(_wl_subprocess, iters=25, name="subprocess")
