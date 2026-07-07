#!/usr/bin/env bash
# matrix.sh -- the soak MATRIX (docs/dev/RELIABILITY_PROGRAM.md R2):
# duration x sanitizer x scheduler-mode.  Drives the R1 soak (tools/soak/soak.py)
# under each build/mode so the slope oracle AND a sanitizer see hours of real
# interleavings -- the regime that catches "passes the tests, dies at hour 30".
#
# TSan is the tool for that class: it flags a racy access PATTERN statistically,
# without needing the unlucky interleaving to actually fire.  ASan catches the
# heap UAF/overflow class in runloom's own C under sustained churn.  The normal
# build's slope oracle catches leaks/unbounded growth.
#
# Presets (arg 1):
#   smoke        60s mixed, normal build            -- verify the plumbing
#   asan-smoke   60s mixed under the ASan ext       -- verify sanitizer capture
#   normal-72h   72h mixed, full workers
#   asan-24h     24h mixed under ASan, N/4 (ASan ~2x)
#   tsan-24h     24h mixed under TSan, N/8 (TSan ~5-10x; needs the TSan ext)
#   iouring-24h  24h mixed, RUNLOOM_IOURING_LOOP=1
#   perhub-24h   24h mixed, RUNLOOM_PERHUB_EPOLL=1
#
# Sanitizer reports are captured via ASAN_OPTIONS/TSAN_OPTIONS log_path=<dir>/<tag>
# (one file per pid); tools/soak/triage_san.py scans + dedups them and the ledger
# records PASS iff BOTH the slope oracle passed AND no non-suppressed sanitizer
# report appeared.  Machine-days accumulate in docs/dev/soak/LEDGER.md -- the
# project's quantitative MTBF claim.
#
# setarch -R (ASLR off) wraps the whole soak so the sanitizer's shadow mapping
# is stable AND every worker child inherits it (personality is inherited on
# exec).  The normal ext is rebuilt on exit so the tree is left usable.
set -u

PRESET="${1:-smoke}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)/.."
ROOT="$(cd "$ROOT" && pwd)"
cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
OUT="$ROOT/docs/dev/soak"
LEDGER="$OUT/LEDGER.md"
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R" || SA=""

# preset -> (duration_flag build_mode workers env...)
WORKLOAD="mixed"
case "$PRESET" in
  smoke)        DUR="--seconds 60"  BUILD=normal WORKERS=2 ENVS=() ;;
  asan-smoke)   DUR="--seconds 60"  BUILD=asan   WORKERS=1 ENVS=() ;;
  tsan-smoke)   DUR="--seconds 60"  BUILD=tsan   WORKERS=1 ENVS=() ;;
  normal-72h)   DUR="--hours 72"    BUILD=normal WORKERS=4 ENVS=() ;;
  asan-24h)     DUR="--hours 24"    BUILD=asan   WORKERS=2 ENVS=() ;;
  tsan-24h)     DUR="--hours 24"    BUILD=tsan   WORKERS=1 ENVS=() ;;
  iouring-24h)  DUR="--hours 24"    BUILD=normal WORKERS=4 ENVS=(--env RUNLOOM_IOURING_LOOP=1) ;;
  perhub-24h)   DUR="--hours 24"    BUILD=normal WORKERS=4 ENVS=(--env RUNLOOM_PERHUB_EPOLL=1) ;;
  *) echo "unknown preset: $PRESET"; echo "presets: smoke asan-smoke tsan-smoke normal-72h asan-24h tsan-24h iouring-24h perhub-24h"; exit 2 ;;
esac

STAMP="$PRESET"
RUNDIR="$OUT/matrix_${PRESET}"
mkdir -p "$RUNDIR"
SANLOG_TAG=""

build_normal() {
  env -u LD_PRELOAD -u ASAN_OPTIONS -u TSAN_OPTIONS -u PYTHONMALLOC \
    PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force \
    >/tmp/runloom_matrix_normal.log 2>&1 \
    && echo "  normal ext restored" || echo "  WARN: normal rebuild failed (/tmp/runloom_matrix_normal.log)"
}
trap 'build_normal' EXIT

case "$BUILD" in
  normal)
    echo "== matrix $PRESET: normal build =="
    ;;
  asan)
    LIB="$(gcc -print-file-name=libasan.so 2>/dev/null)"
    [ -f "$LIB" ] || { echo "libasan.so not found -- SKIP"; exit 0; }
    echo "== matrix $PRESET: building ASan ext (slow) =="
    CFLAGS="-fsanitize=address -O1 -g -fno-omit-frame-pointer" LDFLAGS="-fsanitize=address" \
      PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/tmp/runloom_matrix_asan.log 2>&1 \
      || { echo "  BUILD FAILED (/tmp/runloom_matrix_asan.log)"; tail -15 /tmp/runloom_matrix_asan.log; exit 2; }
    export LD_PRELOAD="$LIB"
    export ASAN_OPTIONS="detect_leaks=0:verify_asan_link_order=0:halt_on_error=1:exitcode=1:log_path=$RUNDIR/asan"
    SANLOG_TAG="asan"
    ;;
  tsan)
    LIB="$(gcc -print-file-name=libtsan.so 2>/dev/null)"
    [ -f "$LIB" ] || { echo "libtsan.so not found -- SKIP"; exit 0; }
    echo "== matrix $PRESET: building TSan ext (slow) =="
    CFLAGS="-fsanitize=thread -O1 -g -fno-omit-frame-pointer" LDFLAGS="-fsanitize=thread" \
      PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/tmp/runloom_matrix_tsan.log 2>&1 \
      || { echo "  BUILD FAILED (/tmp/runloom_matrix_tsan.log)"; tail -15 /tmp/runloom_matrix_tsan.log; exit 2; }
    export LD_PRELOAD="$LIB"
    export TSAN_OPTIONS="halt_on_error=0:exitcode=0:suppressions=$ROOT/tools/tsan_suppressions.txt:log_path=$RUNDIR/tsan"
    SANLOG_TAG="tsan"
    ;;
esac

export PYTHON_GIL=0 PYTHONPATH="$ROOT/src"
echo "-- soak: $WORKLOAD $DUR workers=$WORKERS build=$BUILD ${ENVS[*]:-} --"
$SA "$PY" tools/soak/soak.py --workload "$WORKLOAD" $DUR --workers "$WORKERS" \
    --interval 30 --out "$OUT" --stamp "$STAMP" "${ENVS[@]}"
SOAK_RC=$?

# sanitizer log triage (if a sanitizer build)
SAN_VERDICT="n/a"
if [ -n "$SANLOG_TAG" ]; then
  SAN_VERDICT="$("$PY" tools/soak/triage_san.py "$RUNDIR" --tag "$SANLOG_TAG" 2>&1 | tail -1)"
fi

# ledger append (machine-days = workers * duration)
"$PY" tools/soak/triage_san.py --ledger "$LEDGER" \
    --preset "$PRESET" --build "$BUILD" --workers "$WORKERS" \
    --dur "$DUR" --soak-rc "$SOAK_RC" --san "$SAN_VERDICT" \
    --report "$OUT/soak_${WORKLOAD}_${STAMP}/REPORT.md"

echo "-- matrix $PRESET done: soak_rc=$SOAK_RC san=$SAN_VERDICT --"
[ "$SOAK_RC" = "0" ] && [ "$SAN_VERDICT" != "FAIL" ]
