#!/usr/bin/env bash
# run_refleak.sh -- build the pydebug-ABI ext, then hunt per-iteration refcount /
# alloc drift in runloom's hot ops (tools/refleak_hunt.py).  Companion to
# run_pydebug.sh: that uses CPython's internal ASSERTS as the oracle; this uses
# the gettotalrefcount/getallocatedblocks DELTAS (steady drift = a leak or, for
# the biased-refcount merge, an over-release).
#
# Usage:  tools/run_refleak.sh
# Env:    RUNLOOM_PYDEBUG_PYTHON  the --with-pydebug --disable-gil interpreter
#                                 (default: /home/x/projects/cpython-pydebug/python)
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PYD="${RUNLOOM_PYDEBUG_PYTHON:-/home/x/projects/cpython-pydebug/python}"

[ -x "$PYD" ] || { echo "run_refleak: no pydebug interpreter at $PYD (build: "
  "./configure --with-pydebug --disable-gil && make -j). SKIP."; exit 0; }
"$PYD" -c "import sys; assert hasattr(sys,'gettotalrefcount'); assert not sys._is_gil_enabled()" \
  2>/dev/null || { echo "run_refleak: $PYD is not a pydebug free-threaded build. SKIP."; exit 0; }

echo "== refleak hunt under $PYD ($("$PYD" -V 2>&1)) =="
echo "-- building the extension against the pydebug ABI --"
PYTHON_GIL=0 "$PYD" setup.py build_ext --inplace >/tmp/runloom_refleak_build.log 2>&1 \
  || { echo "  BUILD FAILED -- /tmp/runloom_refleak_build.log"; tail -15 /tmp/runloom_refleak_build.log; exit 2; }
echo "  build OK"
PYTHON_GIL=0 "$PYD" tools/refleak_hunt.py
