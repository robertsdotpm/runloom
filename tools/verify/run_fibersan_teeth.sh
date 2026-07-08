#!/usr/bin/env bash
# run_fibersan_teeth.sh -- planted-bug teeth for the fiber-aware sanitizer
# annotations (docs/dev/soak/fiber_sanitizer_annotations.md).
#
# Builds the ASan-instrumented ext, then proves the fiber start/finish_switch
# brackets did NOT break ASan across the arch/swap_*.S stack switch:
#   * uaf   -> ASan MUST report heap-use-after-free (positive control fires)
#   * clean -> ASan MUST stay silent            (negative control: no false pos)
# Restores the normal ext on exit.  Exit 0 iff both controls behaved.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
LIBASAN="$(gcc -print-file-name=libasan.so 2>/dev/null)"
[ -f "$LIBASAN" ] || { echo "libasan.so not found -- SKIP"; exit 0; }
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R" || SA=""

build_normal() {
  env -u LD_PRELOAD -u ASAN_OPTIONS PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force \
    >/tmp/runloom_fibersan_normal.log 2>&1 \
    && echo "  normal ext restored" || echo "  WARN: normal rebuild failed (/tmp/runloom_fibersan_normal.log)"
}
trap 'build_normal' EXIT

echo "== building ASan ext (slow) =="
CFLAGS="-fsanitize=address -O1 -g -fno-omit-frame-pointer" LDFLAGS="-fsanitize=address" \
  PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/tmp/runloom_fibersan_asan.log 2>&1 \
  || { echo "  BUILD FAILED (/tmp/runloom_fibersan_asan.log)"; tail -15 /tmp/runloom_fibersan_asan.log; exit 2; }

export LD_PRELOAD="$LIBASAN"
export ASAN_OPTIONS="detect_leaks=0:verify_asan_link_order=0:halt_on_error=1:abort_on_error=1:exitcode=1"
export PYTHON_GIL=0 PYTHONPATH="$ROOT/src"
run() { $SA "$PY" tools/verify/fibersan_teeth.py "$1" 2>&1; }

rc=0
echo "-- teeth: uaf (expect heap-use-after-free) --"
OUT="$(run uaf)"; UAF_RC=$?
if echo "$OUT" | grep -q "heap-use-after-free"; then
  echo "  PASS: ASan fired (rc=$UAF_RC)"
else
  echo "  FAIL: ASan did NOT report the planted UAF"; echo "$OUT" | tail -8; rc=1
fi

echo "-- teeth: clean (expect silence) --"
OUT="$(run clean)"; CLEAN_RC=$?
if [ "$CLEAN_RC" = "0" ] && echo "$OUT" | grep -q "completed with no ASan abort" \
   && ! echo "$OUT" | grep -q "AddressSanitizer:"; then
  echo "  PASS: no false positive"
else
  echo "  FAIL: negative control tripped ASan (rc=$CLEAN_RC)"; echo "$OUT" | tail -8; rc=1
fi

echo "-- fibersan teeth: $([ $rc = 0 ] && echo ALL PASS || echo FAIL) --"
exit $rc
