#!/usr/bin/env bash
# coverage.sh -- C-line/branch coverage of the runloom_c extension over the
# whole test corpus, using gcov (no gcovr dependency).
#
# Why: runloom has a large test suite but no measurement of which C lines --
# especially error/cleanup paths (ENOMEM, EAGAIN, epoll_ctl failure, partial
# reads, fd exhaustion) -- are actually exercised.  Those are exactly where
# the nastiest lock-free + error-path bugs hide.  This report shows the dark
# corners so they can be targeted (by the fault-injection harness, new tests,
# or KLEE on the straight-line pieces).
#
# What it does:
#   1. rebuilds runloom_c --inplace, instrumented (-fprofile-arcs
#      -ftest-coverage, -O0 so line mapping is exact);
#   2. runs the workloads that drive the C (pytest suite + the M:N fuzzer +
#      the C deque/blockpool stress) so .gcda counters accumulate;
#   3. runs gcov on every src/runloom_c/*.c and summarizes per-file line
#      coverage + the uncovered error-handling lines.
#
# Usage:  tools/coverage.sh            # full corpus
#         tools/coverage.sh quick      # pytest suite only (faster)
#
# Env:  PYTHON=...   interpreter (default: free-threaded 3.13t if present)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MODE="${1:-full}"

if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.13.13t/bin/python3" python3.13t python3; do
        command -v "$cand" >/dev/null 2>&1 && { PYTHON="$cand"; break; }
    done
fi
echo "[cov] python: $PYTHON"

RM="$(command -v safe-rm || echo rm)"
OBJDIR="build/temp.coverage"
COVOUT="build/coverage"
$RM -rf "$OBJDIR" "$COVOUT" build/temp.* build/lib.* src/runloom_c*.so 2>/dev/null
mkdir -p "$COVOUT"

echo "[cov] building instrumented extension (-O0 --coverage) ..."
RUNLOOM_DEBUG=1 \
RUNLOOM_EXTRA_CFLAGS="-fprofile-arcs -ftest-coverage" \
RUNLOOM_EXTRA_LDFLAGS="-fprofile-arcs -ftest-coverage" \
"$PYTHON" setup.py build_ext --inplace --build-temp "$OBJDIR" \
    > "$COVOUT/build.log" 2>&1 || { echo "[cov] BUILD FAILED -- see $COVOUT/build.log"; tail -20 "$COVOUT/build.log"; exit 1; }

# locate the dir holding the .gcno files (setuptools mirrors the src tree)
GCNODIR="$(dirname "$(find "$OBJDIR" -name 'netpoll.gcno' | head -1)")"
echo "[cov] gcno/gcda dir: $GCNODIR"

run() { echo "[cov] run: $*"; PYTHONPATH=src "$@" >>"$COVOUT/workloads.log" 2>&1 || echo "[cov]   (workload returned nonzero -- counters still recorded)"; }

echo "[cov] driving workloads ..."
run "$PYTHON" -m pytest tests/ -q -p no:cacheprovider --no-header
if [ "$MODE" != quick ]; then
    run "$PYTHON" tools/mn_stress.py --iters "${MN_ITERS:-100}" --stable
fi

# gcov records source paths relative to the compile cwd (repo root), so it
# must run from $ROOT to read the sources and interleave per-line counts.
echo "[cov] collecting gcov ..."
( cd "$ROOT"
  for src in src/runloom_c/*.c; do
      gcov -b -o "$GCNODIR" "$src" >/dev/null 2>&1
  done
  mv -f ./*.gcov "$COVOUT/" 2>/dev/null )

echo "[cov] summary:"
"$PYTHON" tools/cov_summary.py "$COVOUT"
echo
echo "[cov] per-file .gcov reports in: $COVOUT/"
echo "[cov] NOTE: this left an instrumented .so in src/. Rebuild a normal one with:"
echo "        $PYTHON setup.py build_ext --inplace"
