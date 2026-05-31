#!/usr/bin/env bash
# run_sanitizers.sh -- build + run pygo's C concurrency harnesses under
# AddressSanitizer / ThreadSanitizer / UndefinedBehaviorSanitizer.
#
# Hunts use-after-free, out-of-bounds, data races, and UB in the
# lock-free core (currently the Chase-Lev deque stress, test_cldeque).
# This is the "real hardware, real threads, high volume" complement to
# the exhaustive-but-bounded checks in verify/ (CBMC + Spin).
#
# TSan vs ASLR: on Linux 6.x with high-entropy ASLR, TSan aborts at
# startup with "unexpected memory mapping".  We auto-wrap TSan runs in
# `setarch -R` (disable randomization) when available.
#
# Usage: tools/run_sanitizers.sh [PUSHES] [THIEVES] [ROUNDS]
# Defaults tuned to run in a few seconds even under the ~10x sanitizer
# slowdown; pass bigger numbers for an overnight soak.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
TC="$ROOT/tests_c"

PUSHES=${1:-20000}
THIEVES=${2:-4}
ROUNDS=${3:-3}

pass=0; fail=0
green() { printf '\033[32m%s\033[0m' "$1"; }
red()   { printf '\033[31m%s\033[0m' "$1"; }

SETARCH=""
if command -v setarch >/dev/null 2>&1; then
    SETARCH="setarch $(uname -m) -R"
fi

run_one() {  # label, needs_setarch(0/1), env, binary, args...
    local label="$1" use_sa="$2" envv="$3"; shift 3
    printf '  %-26s ' "$label"
    local pre=""
    [ "$use_sa" = 1 ] && pre="$SETARCH"
    if env $envv $pre "$@" >"/tmp/pygo_san_$label.log" 2>&1; then
        green "PASS"; echo; pass=$((pass+1))
    else
        red "FAIL"; echo " (rc=$? -- see /tmp/pygo_san_$label.log)"
        tail -8 "/tmp/pygo_san_$label.log" | sed 's/^/      /'
        fail=$((fail+1))
    fi
}

echo "================ pygo sanitizer harnesses ================"
echo "  deque stress: $PUSHES pushes x $THIEVES thieves x $ROUNDS rounds"
[ -z "$SETARCH" ] && echo "  (setarch not found; TSan may abort under high-entropy ASLR)"
echo "-- building --"
make -C "$TC" test_cldeque test_cldeque-asan test_cldeque-tsan test_cldeque-ubsan \
    >/tmp/pygo_san_build.log 2>&1 \
    && echo "  build OK" || { echo "  BUILD FAILED -- see /tmp/pygo_san_build.log"; tail -20 /tmp/pygo_san_build.log; exit 2; }

echo "-- running --"
run_one cldeque-plain 0 "" "$TC/test_cldeque" "$PUSHES" "$THIEVES" "$ROUNDS"
run_one cldeque-asan  0 "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1" \
        "$TC/test_cldeque-asan" "$PUSHES" "$THIEVES" "$ROUNDS"
run_one cldeque-tsan  1 "TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1" \
        "$TC/test_cldeque-tsan" "$PUSHES" "$THIEVES" "$ROUNDS"
run_one cldeque-ubsan 0 "UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1" \
        "$TC/test_cldeque-ubsan" "$PUSHES" "$THIEVES" "$ROUNDS"

echo "----------------------------------------------------------"
echo "  $pass passed, $fail failed"
echo "=========================================================="
[ "$fail" -eq 0 ]
