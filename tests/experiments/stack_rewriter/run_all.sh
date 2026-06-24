#!/usr/bin/env bash
#
# run_all.sh -- full end-to-end demonstration of the pure-Python static
# stack-overflow rewriter. Runs every stage and prints real output.
#
#   stage 0  build the test CPython C extension (gcc, for the INPUT only)
#   stage 1  validate the length decoder against objdump (oracle)
#   stage 2  baseline: uninstrumented overflow = silent hardware SIGSEGV
#   stage 3  run the pure-Python rewriter -> instrumented .so (+ coverage log)
#   stage 4  verify instrumented ELF with readelf (oracle) + dlopen
#   stage 5  e2e: normal within-limit calls STILL WORK
#   stage 6  e2e: overflow -> clean SOFTWARE report + abort (SIGILL, not SEGV)
#
# gcc/objdump/readelf are used ONLY to build the input and to CROSS-CHECK.
# The rewriter tool itself (rewriter/*.py) is pure-Python stdlib.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

PY_FT="${PY_FT:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
PY="${PY:-python3}"   # any python3 with stdlib for the rewriter itself

EXT="ext/stacktest.cpython-313t-x86_64-linux-gnu.so"
INST="ext/stacktest_instrumented.cpython-313t-x86_64-linux-gnu.so"
SIDECAR="$INST.inject.json"

hr() { printf '\n========== %s ==========\n' "$1"; }

hr "STAGE 0: build test C extension (gcc -> INPUT .so)"
bash ext/build.sh

hr "STAGE 1: validate length decoder vs objdump (oracle)"
$PY tests/check_lendec.py "$EXT"

hr "STAGE 2: BASELINE uninstrumented overflow (expect SIGSEGV / exit 139)"
set +e
PYTHON_GIL=0 "$PY_FT" tests/baseline_overflow.py "$EXT"
echo "[stage2] baseline exit code = $?  (139 = SIGSEGV = silent hardware crash)"
set -e 2>/dev/null

hr "STAGE 3: run pure-Python rewriter (NO pip deps)"
$PY rewriter/stackrewrite.py "$EXT" "$INST"

hr "STAGE 4a: verify instrumented ELF with readelf (oracle)"
readelf -hl "$INST" | sed -n '1,6p;/Program Headers/,/Section to/p'

hr "STAGE 4b: dlopen instrumented .so + run functions (limit unset)"
PYTHON_GIL=0 "$PY_FT" -c "
import importlib.util
s=importlib.util.spec_from_file_location('stacktest','$INST')
m=importlib.util.module_from_spec(s); s.loader.exec_module(m)
print('[dlopen] loaded OK; big_frame(0)=',m.big_frame(0),' recurse(3)=',m.recurse(3))
print('[dlopen] PASS: instrumented binary loads + executes through trampolines')
"

hr "STAGE 5: e2e NORMAL within-limit calls STILL WORK"
PYTHON_GIL=0 "$PY_FT" tests/e2e_overflow.py "$INST" "$SIDECAR" normal

hr "STAGE 6: e2e OVERFLOW -> SOFTWARE report + abort (expect SIGILL/exit 132)"
set +e
PYTHON_GIL=0 "$PY_FT" tests/e2e_overflow.py "$INST" "$SIDECAR" overflow
rc=$?
echo "[stage6] overflow exit code = $rc  (132 = SIGILL = our ud2 = DETECTED)"
set -e 2>/dev/null

hr "DONE"
echo "baseline crashed silently (139 SIGSEGV); instrumented caught it in"
echo "software (132 SIGILL) AFTER printing the report -- detection before"
echo "corruption. Coverage on the test extension: see stage 3."
