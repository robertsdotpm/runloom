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
PYTHON="${PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
RM="$(command -v safe-rm || echo rm)"
OBJDIR="build/temp.coverage"
COVOUT="build/coverage"

echo "[cov] python: $PYTHON"
command -v gcov >/dev/null 2>&1 || { echo "[cov] gcov not found -- install gcc/gcov; cannot measure coverage"; exit 2; }
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
# RUNLOOM_COV_SMOKE=1: plumbing check (coverage_night.sh --smoke) -- a few fast
# files + tiny fuzz/sweep slices; the same pipeline end to end in ~a minute.
if [ "${RUNLOOM_COV_SMOKE:-0}" = "1" ]; then
    FILES=(test_chan.py test_time.py test_tcpconn.py)
fi
echo "[cov] driving ${#FILES[@]} test files via run_isolated -j1 (serial) ..."
PYTHON_GIL=0 PYTHONPATH=src RUNLOOM_TEST_TIMEOUT="${RUNLOOM_TEST_TIMEOUT:-600}" \
    "$PYTHON" tests/run_isolated.py -j1 "${FILES[@]}" "$@" \
    >> "$COVOUT/workloads.log" 2>&1
echo "[cov]   run_isolated rc=$? (full log: $COVOUT/workloads.log)"

# The M:N fuzzer drives contended scheduler/netpoll paths the deterministic
# tests under-count (idle-hub wakeup, wake-CAS retry, handoff adopt).
MN_ITERS=200; LF_SEEDS=25
[ "${RUNLOOM_COV_SMOKE:-0}" = "1" ] && { MN_ITERS=20; LF_SEEDS=3; }
echo "[cov] driving mn_stress fuzzer (--iters $MN_ITERS --stable) ..."
PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tools/mn_stress.py --iters "$MN_ITERS" --stable \
    >> "$COVOUT/workloads.log" 2>&1
echo "[cov]   mn_stress rc=$?"

# lifefuzz slice: generative lifecycle programs reach chan/select/timer/nested-
# spawn combinations the fixed suite doesn't enumerate.
echo "[cov] driving lifefuzz ($LF_SEEDS seeds) ..."
lf_fail=0
for i in $(seq 1 "$LF_SEEDS"); do
    seed=$(( (i * 2654435761) % 2000000000 ))
    PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tools/lifefuzz/lifefuzz.py run "$seed" \
        --timeout 20 >> "$COVOUT/workloads.log" 2>&1 || lf_fail=$((lf_fail+1))
done
echo "[cov]   lifefuzz: $LF_SEEDS seeds, $lf_fail failures"

# Counted-exhaustive fault sweep: executes the ERROR/cleanup paths -- exactly
# the "dark corners" this report exists to expose -- so they count as covered
# only when genuinely driven.
SWEEP_SITES=""
[ "${RUNLOOM_COV_SMOKE:-0}" = "1" ] && SWEEP_SITES="FD_READ FD_WRITE"
echo "[cov] driving counted fault sweep ${SWEEP_SITES:-(all Linux sites)} ..."
# shellcheck disable=SC2086
PYTHON_GIL=0 "$PYTHON" tools/fault_sweep_counted.py $SWEEP_SITES \
    >> "$COVOUT/workloads.log" 2>&1
echo "[cov]   fault sweep rc=$?"

# NB: a global RUNLOOM_TCPCONN_IOURING=1 / RUNLOOM_IOURING_LOOP=1 re-drive was
# tried to light up the io_uring eventfd/ring/pump lines, but forcing io_uring
# recv DEADLOCKS a backpressured loopback transfer (see
# tests/regressions/iouring_recv_backpressure_deadlock.py) -- so those lines
# cannot be driven by a clean-exit test today. They are handled as BLOCKED
# exclusions in the manifest instead (see tests/COVERAGE.md).

echo "[cov] collecting gcov (from repo root so .c.inc fragment paths resolve) ..."
( cd "$ROOT"
  for src in src/runloom_c/*.c; do
      gcov -b -o "$GCNODIR" "$src" >/dev/null 2>&1
  done
  mv -f ./*.gcov "$COVOUT/" 2>/dev/null )
echo "[cov]   $(ls "$COVOUT"/*.gcov 2>/dev/null | wc -l) .gcov fragments emitted"

# lcov HTML heat map (line-by-line red/green over the C source).  Optional:
# skipped quietly when lcov/genhtml are absent; the gcov text + subsystem
# summary below remain the canonical numbers either way.
if command -v lcov >/dev/null 2>&1 && command -v genhtml >/dev/null 2>&1; then
    echo "[cov] rendering lcov HTML heat map ..."
    lcov --quiet --capture --directory "$GCNODIR" --base-directory "$ROOT" \
         --output-file "$COVOUT/cov.info" \
         --rc branch_coverage=1 --ignore-errors mismatch,negative,unused,empty,source \
         > "$COVOUT/lcov.log" 2>&1 \
      && lcov --quiet --extract "$COVOUT/cov.info" "*/src/runloom_c/*" \
              --output-file "$COVOUT/cov.info" \
              --rc branch_coverage=1 --ignore-errors unused,empty \
              >> "$COVOUT/lcov.log" 2>&1 \
      && genhtml --quiet --branch-coverage --output-directory "$COVOUT/html" \
                 --title "runloom_c coverage" "$COVOUT/cov.info" \
                 >> "$COVOUT/lcov.log" 2>&1 \
      && echo "[cov]   heat map: $COVOUT/html/index.html" \
      || echo "[cov]   WARN: lcov/genhtml failed (see $COVOUT/lcov.log) -- text report still valid"
else
    echo "[cov] lcov/genhtml not installed -- skipping HTML heat map"
fi

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
