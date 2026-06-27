#!/usr/bin/env bash
# build.sh -- build + run the coverage-guided (libFuzzer) deque fuzzer.
#
# libFuzzer ships inside clang (-fsanitize=fuzzer); no external download needed.
# In-process, single-threaded, ASan+UBSan-backed, BOUNDED by -max_total_time, so
# it cannot wedge the box.  A small CAP makes the full/wrap boundaries reachable
# fast (the logic is capacity-independent -- CBMC proves it at CAP=4).
#
#   build.sh                 # build, then a short bounded run (15s)
#   build.sh --run 120       # build, run 120s
#   build.sh --teeth         # NEGATIVE CONTROL: build against a deliberately
#                            #   broken cldeque.c and assert the fuzzer FINDS the
#                            #   bug fast (proves the harness has teeth)
#   build.sh --build-only
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SRC="$ROOT/src/runloom_c"
CC="${CC:-clang}"
CAP="${RUNLOOM_CLDEQUE_CAP:-8}"
CORPUS="$HERE/corpus"
BIN="$HERE/cldeque_fuzz"
SAN="-fsanitize=fuzzer,address,undefined -fno-sanitize-recover=undefined"
CFLAGS="-O1 -g -DRUNLOOM_CLDEQUE_CAP=$CAP -I $SRC"

mkdir -p "$CORPUS"
mode="run"; secs=15
while [ $# -gt 0 ]; do
    case "$1" in
        --run) mode="run"; secs="${2:-15}"; shift 2;;
        --build-only) mode="build"; shift;;
        --teeth) mode="teeth"; shift;;
        *) echo "unknown arg: $1"; exit 2;;
    esac
done

build() {  # $1=src-deque  $2=out
    $CC $SAN $CFLAGS "$HERE/cldeque_fuzz.c" "$1" -o "$2"
}

if [ "$mode" = "teeth" ]; then
    # Break the deque: off-by-one in the FULL check (>= -> >) lets push overflow
    # buf[] by one -> the harness's full-assert / ASan must catch it.
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    sed 's/b - t >= RUNLOOM_CLDEQUE_CAP/b - t > RUNLOOM_CLDEQUE_CAP/' \
        "$SRC/cldeque.c" > "$tmp/cldeque_bug.c"
    if ! grep -q 'b - t > RUNLOOM_CLDEQUE_CAP' "$tmp/cldeque_bug.c"; then
        echo "teeth: could not apply the mutation (source moved?)"; exit 2
    fi
    build "$tmp/cldeque_bug.c" "$tmp/cldeque_fuzz_bug" || { echo "teeth build failed"; exit 2; }
    echo "teeth: fuzzing a DELIBERATELY broken deque (full-check off-by-one); expect a crash..."
    if "$tmp/cldeque_fuzz_bug" -max_total_time=30 -close_fd_mask=3 -artifact_prefix="$HERE/" "$CORPUS" >/dev/null 2>&1; then
        echo "teeth: FAIL -- fuzzer did NOT find the injected bug in 30s (harness is toothless)"
        exit 1
    fi
    echo "teeth: PASS -- fuzzer found the injected bug (harness has teeth)"
    exit 0
fi

echo "build: $CC $SAN -DRUNLOOM_CLDEQUE_CAP=$CAP"
build "$SRC/cldeque.c" "$BIN" || { echo "build failed"; exit 2; }
echo "built: $BIN"
[ "$mode" = "build" ] && exit 0

echo "run: $secs s (bounded; in-process; ASan+UBSan)"
"$BIN" -max_total_time="$secs" -close_fd_mask=3 -artifact_prefix="$HERE/" "$CORPUS"
rc=$?
if [ $rc -eq 0 ]; then
    echo "fuzz: clean (no crash) over the run; corpus in $CORPUS"
else
    echo "fuzz: CRASH (rc=$rc) -- a reproducing input was written above; investigate"
fi
exit $rc
