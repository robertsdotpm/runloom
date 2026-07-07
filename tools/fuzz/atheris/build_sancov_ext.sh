#!/usr/bin/env bash
# build_sancov_ext.sh -- build runloom_c with SanitizerCoverage so the Atheris
# fuzzer (fuzz_api.py) gets REAL native C-coverage feedback, not just Python-level.
#
# Atheris instruments Python bytecode; the C ext is opaque to it UNLESS the ext is
# compiled with -fsanitize=fuzzer-no-link (the trace-pc-guard + trace-cmp sancov
# callbacks libFuzzer/Atheris consume). With that, Atheris's libFuzzer core sees
# the C ext's edges and CmpLog-style comparisons, so it steers into the deep
# arg-parse / error branches of the C validation surface -- the limitation noted
# in B1. Built with clang + ASan; runs fuzz_api.py under the asan runtime; then
# REBUILDS the normal ext (clean restore, ASan env dropped first).
#
# Usage:  tools/fuzz/atheris/build_sancov_ext.sh [seconds]
# Exit: 0 = clean fuzz run (native coverage active); 1 = crash; 2 = setup.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
SECS="${1:-20}"
command -v clang >/dev/null 2>&1 || { echo "build_sancov_ext: clang absent. SKIP."; exit 0; }
"$PY" -c "import atheris" 2>/dev/null || { echo "build_sancov_ext: atheris absent. SKIP."; exit 0; }
ASAN_RT="$(clang -print-file-name=libclang_rt.asan-x86_64.so 2>/dev/null)"

echo "== Atheris with native C coverage (SanCov-instrumented ext) =="
echo "-- building ext: clang -fsanitize=address,fuzzer-no-link -fsanitize-coverage=trace-pc-guard,trace-cmp --"
CC=clang CXX=clang++ \
CFLAGS="-fsanitize=address,fuzzer-no-link -fsanitize-coverage=trace-pc-guard,trace-cmp -O1 -g -fno-omit-frame-pointer" \
LDFLAGS="-fsanitize=address,fuzzer-no-link" \
PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/tmp/runloom_sancov_build.log 2>&1 \
  || { echo "  BUILD FAILED -- /tmp/runloom_sancov_build.log"; tail -15 /tmp/runloom_sancov_build.log; \
       env -u LD_PRELOAD PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/dev/null 2>&1; exit 2; }
echo "  build OK"

restore() {
    echo "-- rebuilding the normal ext --"
    env -u LD_PRELOAD -u ASAN_OPTIONS PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force \
      >/tmp/runloom_normal_rebuild.log 2>&1 && echo "  restored" || echo "  WARN: restore failed"
}
trap restore EXIT

export ASAN_OPTIONS="detect_leaks=0:verify_asan_link_order=0:halt_on_error=1"
[ -f "$ASAN_RT" ] && export LD_PRELOAD="$ASAN_RT"
export PYTHON_GIL=0 PYTHONPATH="$ROOT/src"
echo "-- running fuzz_api.py ${SECS}s (native coverage should now be active: no 'is the code instrumented?' warning) --"
"$PY" "$HERE/fuzz_api.py" -max_total_time="$SECS" -rss_limit_mb=0 -close_fd_mask=3 \
    -artifact_prefix="$HERE/" "$HERE/corpus" 2>&1 | grep -aiE 'cov:|INITED|Done|interesting|instrument|crash|ERROR' | tail -8
rc=${PIPESTATUS[0]}
echo "fuzz exit: $rc"
exit $([ "$rc" = "0" ] && echo 0 || echo 1)
