#!/usr/bin/env bash
# cov_measure.sh -- whole-extension C coverage via gcov, driven by the ISOLATED
# runner (one file per subprocess, serial) so cross-file state leaks don't
# suppress files and .gcda counters accumulate cleanly (parallel subprocesses
# would race the shared .gcda merge -- hence -j1).
#
# Differs from coverage.sh: uses tests/run_isolated.py -j1 (not in-process
# pytest, which flakes under state leaks and undercounts), excludes the heavy
# scale/stress files (they add NO new C lines, only hours + hang risk), and
# reports the manifest-aware coverable surface via cov_subsystem.py.
#
# Usage:  tools/cov_measure.sh [extra run_isolated args]
# Leaves raw *.gcov in build/coverage/ ; restores a NORMAL .so at the end.
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PYTHON="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
RM="$(command -v safe-rm || echo rm)"
OBJDIR="build/temp.coverage"
COVOUT="build/coverage"

echo "[cov] python: $PYTHON"
$RM -rf "$OBJDIR" "$COVOUT" build/temp.* build/lib.* src/runloom_c*.so 2>/dev/null
mkdir -p "$COVOUT"

echo "[cov] building instrumented extension (-O0 --coverage) ..."
RUNLOOM_DEBUG=1 \
RUNLOOM_EXTRA_CFLAGS="-fprofile-arcs -ftest-coverage" \
RUNLOOM_EXTRA_LDFLAGS="-fprofile-arcs -ftest-coverage" \
"$PYTHON" setup.py build_ext --inplace --build-temp "$OBJDIR" \
    > "$COVOUT/build.log" 2>&1 || { echo "[cov] BUILD FAILED -- see $COVOUT/build.log"; tail -25 "$COVOUT/build.log"; exit 1; }

GCNODIR="$(dirname "$(find "$OBJDIR" -name 'netpoll.gcno' | head -1)")"
echo "[cov] gcno/gcda dir: $GCNODIR"

# Corpus = every tests/test_*.py EXCEPT test_soak.py (pure steady-state
# repetition: ~15min for ZERO new lines).  The 3 stress files ARE included --
# they drive the contended netpoll/mn/chan interleavings (real new lines) that
# a quiet corpus misses.
HEAVY='test_soak.py'
mapfile -t FILES < <(ls tests/test_*.py | sed 's#tests/##' | grep -vE "^($HEAVY)\$")
echo "[cov] driving ${#FILES[@]} test files via run_isolated -j1 (serial) ..."
PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_TEST_TIMEOUT="${RUNLOOM_TEST_TIMEOUT:-600}" \
    "$PYTHON" tests/run_isolated.py -j1 "${FILES[@]}" "$@" \
    >> "$COVOUT/workloads.log" 2>&1
echo "[cov]   run_isolated rc=$? (full log: $COVOUT/workloads.log)"

# The M:N fuzzer drives contended scheduler/netpoll paths the deterministic
# tests under-count (idle-hub wakeup, wake-CAS retry, handoff adopt).
echo "[cov] driving mn_stress fuzzer (--iters 200 --stable) ..."
PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tools/mn_stress.py --iters 200 --stable \
    >> "$COVOUT/workloads.log" 2>&1
echo "[cov]   mn_stress rc=$?"

# NB: a global RUNLOOM_TCPCONN_IOURING=1 / RUNLOOM_IOURING_LOOP=1 re-drive was
# tried to light up the io_uring eventfd/ring/pump lines, but forcing io_uring
# recv DEADLOCKS a backpressured loopback transfer (see
# tests/regressions/iouring_recv_backpressure_deadlock.py) -- so those lines
# cannot be driven by a clean-exit test today. They are handled as BLOCKED
# exclusions in the manifest instead (see COVERAGE.md).

echo "[cov] collecting gcov (from repo root so .c.inc fragment paths resolve) ..."
( cd "$ROOT"
  for src in src/runloom_c/*.c; do
      gcov -b -o "$GCNODIR" "$src" >/dev/null 2>&1
  done
  mv -f ./*.gcov "$COVOUT/" 2>/dev/null )
echo "[cov]   $(ls "$COVOUT"/*.gcov 2>/dev/null | wc -l) .gcov fragments emitted"

echo "[cov] ===== subsystem (manifest-aware coverable) ====="
"$PYTHON" tools/cov_subsystem.py "$COVOUT"
echo "[cov] ===== raw per-file (all TUs) ====="
"$PYTHON" tools/cov_summary.py "$COVOUT"

echo "[cov] restoring a NORMAL (non-instrumented) .so ..."
$RM -rf build/temp.* src/runloom_c*.so 2>/dev/null
"$PYTHON" setup.py build_ext --inplace > "$COVOUT/rebuild_normal.log" 2>&1 \
    && echo "[cov] normal .so restored" \
    || { echo "[cov] WARN: normal rebuild failed -- see $COVOUT/rebuild_normal.log"; tail -15 "$COVOUT/rebuild_normal.log"; }
echo "[cov] DONE"
