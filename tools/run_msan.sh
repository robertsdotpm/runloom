#!/usr/bin/env bash
# run_msan.sh -- build runloom's C extension under MemorySanitizer against the
# MSan-instrumented free-threaded CPython (tools/build_msan_cpython.sh) and run a
# workload so uninitialized-memory reads in runloom's C abort/report at the source
# line.  The MSan complement to run_pydebug.sh / run_sanitizers_ext.sh.
#
# MSan needs EVERY linked object instrumented.  The interpreter is (build_msan_
# cpython.sh) and we build runloom_c with -fsanitize=memory here, but system
# libc/openssl/_socket are NOT -- so values they return read as uninit unless MSan
# intercepts them.  TRIAGE RULE: a report whose top runloom_c/* frame is the use
# is REAL; one rooted only in libc/_ssl/_socket interceptors is the uninstrumented-
# lib floor.  This script greps the report and classifies on that basis.
#
# setarch -R: MSan binaries (the interpreter + our ext) abort under Linux 6.x
# high-entropy ASLR, same as TSan -- run everything under it.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
# RUN under the MSan interpreter; BUILD with a clean python that HAS setuptools
# and is NOT MSan -- setuptools imports hashlib -> _hashopenssl (uninstrumented
# OpenSSL) and terminates under MSan, but building the .so itself only needs a
# setuptools-capable driver + clang with MSan flags.  Same 3.13t free-threaded
# ABI, so the MSan-instrumented .so loads in the MSan interpreter.
PY="${RUNLOOM_MSAN_PYTHON:-$HOME/cpython-msan/bin/python3.13t}"
[ -x "$PY" ] || PY="$HOME/cpython-msan/bin/python3"
BUILD_PY="${RUNLOOM_BUILD_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
ITERS="${1:-4}"
RM="$(command -v safe-rm || echo rm)"
SA=""; command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R"
export CC=clang CXX=clang++
# halt_on_error=0: keep running past the uninstrumented-lib FP floor so we collect
# ALL reports; origin tracking is baked into the interpreter CFLAGS.
export MSAN_OPTIONS="halt_on_error=0:exitcode=0:print_stats=0:origin_history_size=0"

[ -x "$PY" ] || { echo "no MSan interpreter at $PY -- run tools/build_msan_cpython.sh first"; exit 2; }
echo "MSan interpreter: $PY"
$SA "$PY" -c 'import sysconfig;print("GIL_DISABLED",sysconfig.get_config_var("Py_GIL_DISABLED"))'

echo "=== build runloom_c under MSan (clang -fsanitize=memory), driven by the clean build python ==="
$RM -rf build/lib.* build/temp.* src/runloom_c*.so 2>/dev/null
env PYTHON_GIL=0 \
   CC=clang CXX=clang++ \
   CFLAGS="-fsanitize=memory -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer -g -O1" \
   LDFLAGS="-fsanitize=memory" \
   "$BUILD_PY" setup.py build_ext --inplace >msan_build_ext.log 2>&1
echo "build_ext_rc=$?"; tail -3 msan_build_ext.log
echo "--- the .so loads in the MSan interp? (ABI smoke) ---"
$SA env PYTHON_GIL=0 PYTHONPATH=src "$PY" -c \
   "import runloom_c; f=getattr(runloom_c,'__file__',None); assert f and f.endswith('.so'); print('MSAN_EXT_LOADS', f)" 2>&1 | grep -E 'MSAN_EXT_LOADS|Error|Segmentation|MemorySanitizer' | head -3

echo "=== run workload(s) under MSan ==="
LOG="$(mktemp)"
# CRITICAL: drive runloom_c DIRECTLY -- do NOT `import runloom` (the high-level
# package eagerly loads the aio/TLS layer -> _ssl -> uninstrumented OpenSSL, whose
# FPs are NOT runloom's code and would drown the signal + terminate the run).
# (1) goroutine churn via the C API: g-stack alloc/recycle + coro stack-switch.
$SA env PYTHON_GIL=0 PYTHONPATH=src "$PY" -c \
   'import runloom_c
for _ in range('"$ITERS"'):
    for _ in range(128):
        runloom_c.go(lambda: None)
    runloom_c.run()
print("LIFECYCLE_DONE")' >>"$LOG" 2>&1
# (2) mn_stress -- the M:N scheduler fuzzer (TLS-free): deque / pool / cross-hub
#     struct handoff -- the paths where an uninitialized read would live.
$SA env PYTHON_GIL=0 PYTHONPATH=src "$PY" tools/mn_stress.py --iters "$ITERS" >>"$LOG" 2>&1
echo "MNSTRESS_DONE_MARKER $?" >>"$LOG"

echo "=== classify MSan reports ==="
total=$(grep -c 'WARNING: MemorySanitizer: use-of-uninitialized-value' "$LOG")
echo "MSan uninit reports: $total"
if [ "$total" -gt 0 ]; then
    echo "--- reports with a runloom_c/* frame in the TOP 6 (LIKELY REAL) ---"
    awk '/WARNING: MemorySanitizer/{n=NR} n && NR<=n+6 && /runloom_c/{print FILENAME": "$0; n=0}' "$LOG" | head -20
    echo "--- (reports rooted only in libc/_ssl/_socket = uninstrumented-lib floor) ---"
fi
grep -q 'LIFECYCLE_DONE' "$LOG" && echo "lifecycle: completed" || echo "lifecycle: DID NOT COMPLETE (an MSan abort or a real wedge -- inspect $LOG)"
echo "full report: $LOG"
