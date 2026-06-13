"""prewarm(): fill the GLOBAL cross-hub stack depot so a later spawn burst pops
pooled stacks instead of mmap'ing on the latency-critical path.

Covers the synchronous and background (detached-thread) paths and that fibers
still spawn+run correctly afterwards.  The win itself (a cold burst of long-lived
fibers spawns ~4x slower than a prewarmed one) is a perf property measured out of
band; here we pin the API contract and that it does no harm.
"""
import time

import runloom
import runloom_c


def test_prewarm_synchronous_retains():
    # Under the default depot cap (1024), a 500-stack sync prewarm retains them all.
    n = runloom_c.prewarm(500, 512 * 1024, False)
    assert n == 500, n


def test_prewarm_background_returns_immediately():
    # Fire-and-forget: the detached helper does the mmap'ing; the call returns 0 now.
    t0 = time.monotonic()
    r = runloom_c.prewarm(500, 512 * 1024, True)
    dt = time.monotonic() - t0
    assert r == 0, r
    assert dt < 0.05, dt        # returned without doing the work inline


def test_prewarm_default_args():
    # n only; stack_size defaults to 512 KiB, background defaults to True.
    assert runloom_c.prewarm(100) == 0


def test_fibers_run_after_prewarm():
    runloom_c.prewarm(800, 512 * 1024, False)   # seed the depot
    done = bytearray(400)

    def main():
        def w(i):
            done[i] = 1
        for i in range(400):
            runloom.go(w, i)

    runloom.run(8, main)
    assert sum(done) == 400


def test_prewarm_zero_and_negative_are_noops():
    assert runloom_c.prewarm(0, 512 * 1024, False) == 0
    assert runloom_c.prewarm(-5, 512 * 1024, False) == 0
