"""pytest wrapper: build + run the deterministic cldeque C unit test.

tests_c/test_cldeque_lifo.c asserts the ISOLATED single-threaded semantics of
the Chase-Lev deque (owner push/pop LIFO, steal FIFO from the top, size 1:1,
push returns -1 / full at CAP) that the racing torture (test_cldeque.c) and the
Python work-stealing driver (test_cov95_gap_cldeque_c.py) leave unpinned.  It
compiles cldeque.c directly (no Python), so we build it via tests_c/Makefile and
assert PASS.  Skipped only if no C compiler / make is available.
"""
import os
import shutil
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_C = os.path.join(REPO, "tests", "tests_c")
BIN = os.path.join(TESTS_C, "test_cldeque_lifo")

pytestmark = pytest.mark.skipif(
    shutil.which("make") is None or
    (shutil.which("cc") is None and shutil.which("gcc") is None),
    reason="no C compiler / make available")


def test_cldeque_single_threaded_semantics():
    build = subprocess.run(["make", "test_cldeque_lifo"], cwd=TESTS_C,
                           capture_output=True, text=True, timeout=120)
    assert build.returncode == 0, (build.stdout, build.stderr)
    run = subprocess.run([BIN], cwd=TESTS_C, capture_output=True, text=True,
                         timeout=30)
    assert run.returncode == 0, (run.stdout, run.stderr)
    assert "PASS" in run.stdout, (run.stdout, run.stderr)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
