#!/usr/bin/env bash
# build_afl.sh -- fuzz the real cldeque under AFL++ (persistent mode + CmpLog).
#
# Complements the libFuzzer harness (../libfuzzer): a SECOND coverage-guided
# engine on the same FSM, with AFL++'s CmpLog (input-to-state) to crack the
# capacity/epoch/sentinel comparisons that plain coverage struggles past. Reuses
# ../libfuzzer/cldeque_fuzz.c VERBATIM (it exposes LLVMFuzzerTestOneInput;
# AFL++'s libAFLDriver.a supplies the persistent-mode main), so there is ZERO
# harness duplication -- only the compiler + driver differ.
#
# In-process, ASan+assert oracle, BOUNDED by -V <seconds>; cannot wedge the box.
#
#   build_afl.sh                # build + a 20s bounded fuzz run
#   build_afl.sh --run 120
#   build_afl.sh --teeth        # broken deque must be found (negative control)
#   build_afl.sh --build-only
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SRC="$ROOT/src/runloom_c"
HARNESS="$HERE/../libfuzzer/cldeque_fuzz.c"
DRIVER=/usr/lib/afl/libAFLDriver.a
CAP="${RUNLOOM_CLDEQUE_CAP:-8}"
CFLAGS="-O1 -g -DRUNLOOM_CLDEQUE_CAP=$CAP -I $SRC"

command -v afl-clang-fast >/dev/null 2>&1 || { echo "build_afl: afl-clang-fast absent (apt install afl++). SKIP."; exit 0; }
command -v afl-fuzz       >/dev/null 2>&1 || { echo "build_afl: afl-fuzz absent. SKIP."; exit 0; }
[ -f "$DRIVER" ] || { echo "build_afl: $DRIVER absent (afl++ libFuzzer driver). SKIP."; exit 0; }

mode="run"; secs=20
case "${1:-}" in
    --run) secs="${2:-20}";;
    --build-only) mode="build";;
    --teeth) mode="teeth";;
esac

build() {  # $1=src-deque $2=out [extra-env-for-cmplog]
    AFL_USE_ASAN=1 ${3:-} afl-clang-fast $CFLAGS "$HARNESS" "$1" "$DRIVER" -o "$2" 2>/dev/null
}

# AFL env to run headless in a container/VM (no TUI, skip governor/core_pattern checks)
export AFL_NO_UI=1 AFL_SKIP_CPUFREQ=1 AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1 AFL_USE_ASAN=1

seed() { mkdir -p "$1"; printf '\x00\x01\x02\x03' > "$1/a"; printf '\x02\x02\x03\x00\x01' > "$1/b"; }

if [ "$mode" = "teeth" ]; then
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    sed 's/b - t >= RUNLOOM_CLDEQUE_CAP/b - t > RUNLOOM_CLDEQUE_CAP/' "$SRC/cldeque.c" > "$tmp/bug.c"
    grep -q 'b - t > RUNLOOM_CLDEQUE_CAP' "$tmp/bug.c" || { echo "teeth: mutation didn't apply"; exit 2; }
    build "$tmp/bug.c" "$tmp/afl_bug" || { echo "teeth: build failed"; exit 2; }
    seed "$tmp/in"
    echo "teeth: fuzzing a DELIBERATELY broken deque under AFL++; expect a crash..."
    AFL_BENCH_UNTIL_CRASH=1 timeout 90 afl-fuzz -i "$tmp/in" -o "$tmp/out" -V 80 -- "$tmp/afl_bug" >/dev/null 2>&1
    if ls "$tmp"/out/*/crashes/id* >/dev/null 2>&1; then
        echo "teeth: PASS -- AFL++ found the injected bug ($(ls "$tmp"/out/*/crashes/id* | wc -l) crashes)"
        exit 0
    fi
    echo "teeth: FAIL -- AFL++ did not find the injected bug in 80s"
    exit 1
fi

echo "build: afl-clang-fast (+ASan) + CmpLog, CAP=$CAP"
build "$SRC/cldeque.c" "$HERE/cldeque_afl" || { echo "build failed"; exit 2; }
build "$SRC/cldeque.c" "$HERE/cldeque_afl_cmplog" "AFL_LLVM_CMPLOG=1" || { echo "cmplog build failed"; exit 2; }
echo "built: $HERE/cldeque_afl (+ _cmplog)"
[ "$mode" = "build" ] && exit 0

seed "$HERE/in"
echo "run: ${secs}s AFL++ (persistent + CmpLog), bounded"
timeout "$((secs + 30))" afl-fuzz -i "$HERE/in" -o "$HERE/out" -c "$HERE/cldeque_afl_cmplog" \
    -V "$secs" -- "$HERE/cldeque_afl" >/tmp/afl_run.log 2>&1
echo "  $(grep -aoE 'execs_done +: [0-9]+|corpus count +: [0-9]+' "$HERE/out/default/fuzzer_stats" 2>/dev/null | tr '\n' ' ')"
if ls "$HERE"/out/*/crashes/id* >/dev/null 2>&1; then
    echo "afl: CRASH(es) found -> $HERE/out/default/crashes/ (investigate)"
    exit 1
fi
echo "afl: clean (no crash) over ${secs}s on the real deque"
exit 0
