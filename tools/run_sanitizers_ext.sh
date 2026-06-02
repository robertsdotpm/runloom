#!/usr/bin/env bash
# run_sanitizers_ext.sh -- build the pygo_core EXTENSION under ThreadSanitizer
# and run real workloads under the free-threaded interpreter to hunt data races
# in pygo's own C (scheduler / chan / select / netpoll / coro).
#
# This complements tools/run_sanitizers.sh, which TSans only the standalone C
# deque harness (test_cldeque).  Here the *whole runtime* runs under TSan while
# driven by real goroutines on real OS threads with the GIL off -- the regime
# where pygo's lock-free park/wake/select bugs actually live.
#
# How: build only the ext with -fsanitize=thread and force-load libtsan into a
# stock free-threaded CPython.  TSan instruments every load/store in the ext
# (including inlined Py_INCREF / atomics) -- exactly pygo's code -- and is blind
# to the uninstrumented interpreter's internals, the few of which that surface
# are filtered by tools/tsan_suppressions.txt.  A fully TSan-built interpreter
# is the gold standard but currently hits an upstream getpath miscompile on this
# toolchain (see tools/build_tsan_cpython.sh); this preload path needs no patched
# interpreter and is correctly scoped to pygo's C.
#
# TSan + ASLR: TSan's shadow mapping aborts under high-entropy ASLR on Linux
# 6.x ("unexpected memory mapping"); every run is wrapped in `setarch -R`.
#
# Usage:  tools/run_sanitizers_ext.sh [MN_ITERS]
# Env:    PYTHON=...       interpreter (default: free-threaded 3.13t if present)
#         TSAN_PYTEST=...  space-separated pytest targets (default: a focused
#                          chan/select/sched subset); empty to skip pytest
#         KEEP_TSAN_SO=1   leave the instrumented .so in place (default: rebuild
#                          a normal .so at the end so the tree stays usable)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
MN_ITERS="${1:-100}"
SUPP="$ROOT/tools/tsan_suppressions.txt"

if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.13.13t/bin/python3" python3.13t python3; do
        command -v "$cand" >/dev/null 2>&1 && { PYTHON="$cand"; break; }
    done
fi
LIBTSAN="$(gcc -print-file-name=libtsan.so)"
[ -f "$LIBTSAN" ] || { echo "libtsan.so not found (need gcc with TSan)"; exit 2; }
RM="$(command -v safe-rm || echo rm)"

SA=""
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R"
[ -z "$SA" ] && echo "WARNING: setarch not found; TSan may abort under ASLR"

echo "================ pygo ext under ThreadSanitizer ================"
echo "  python : $PYTHON"
echo "  libtsan: $LIBTSAN"
echo "  supp   : $SUPP"

echo "-- building instrumented extension (-fsanitize=thread -O1) --"
$RM -f src/pygo_core*.so
$RM -rf build/temp.tsan
PYGO_EXTRA_CFLAGS="-fsanitize=thread -g -O1 -fno-omit-frame-pointer" \
PYGO_EXTRA_LDFLAGS="-fsanitize=thread" \
    "$PYTHON" setup.py build_ext --inplace --build-temp build/temp.tsan \
    >/tmp/pygo_tsan_ext_build.log 2>&1 \
    || { echo "  BUILD FAILED -- see /tmp/pygo_tsan_ext_build.log"; tail -20 /tmp/pygo_tsan_ext_build.log; exit 2; }
ldd src/pygo_core*.so | grep -q tsan \
    && echo "  build OK (links libtsan)" \
    || { echo "  ext is NOT instrumented (no libtsan in ldd)"; exit 2; }

LOGDIR="$(mktemp -d /tmp/pygo_tsan.XXXX)"
# halt_on_error=0: collect ALL races across the run, don't stop at the first.
export TSAN_OPTIONS="halt_on_error=0:report_bugs=1:history_size=7:suppressions=$SUPP:log_path=$LOGDIR/tsan"
run() {  # label, cmd...
    local label="$1"; shift
    printf -- "-- %-24s " "$label"
    if $SA env PYTHON_GIL=0 LD_PRELOAD="$LIBTSAN" PYTHONPATH=src "$@" \
            >"$LOGDIR/$label.out" 2>&1; then
        echo "ran (exit 0)"
    else
        echo "ran (exit $? -- TSan exitcode 66 = races found, see summary)"
    fi
}

echo "-- driving workloads under TSan --"
run mn_stress   "$PYTHON" tools/mn_stress.py --iters "$MN_ITERS"
run lincheck_plain  "$PYTHON" tools/lincheck/record_history.py "$LOGDIR/h_plain.json"  4 3 8 2 0
run lincheck_select "$PYTHON" tools/lincheck/record_history.py "$LOGDIR/h_select.json" 4 3 8 2 3

PYTEST_TARGETS="${TSAN_PYTEST-tests/test_chan_stress.py tests/test_sched_fairness.py tests/test_chan_queue.py}"
if [ -n "$PYTEST_TARGETS" ]; then
    run pytest_subset "$PYTHON" -m pytest $PYTEST_TARGETS -q -p no:cacheprovider --no-header
fi

echo "----------------------------------------------------------------"
echo "  race reports (non-suppressed), by site:"
shopt -s nullglob
reports=("$LOGDIR"/tsan.*)
if [ ${#reports[@]} -eq 0 ]; then
    echo "    NONE -- pygo ext is TSan-clean across all workloads"
    rc=0
else
    cat "$LOGDIR"/tsan.* | grep "SUMMARY: ThreadSanitizer" | sort | uniq -c | sort -rn | sed 's/^/    /'
    # only pygo-frame races count as failures (CPython noise is suppressed, but
    # double-check nothing in src/pygo_core slipped through)
    if cat "$LOGDIR"/tsan.* | grep -q "src/pygo_core/"; then
        echo "  >>> data races in pygo's own C -- see $LOGDIR/tsan.*"
        rc=1
    else
        echo "  (all in CPython internals; no pygo-frame races)"
        rc=0
    fi
fi
echo "  logs: $LOGDIR"
echo "================================================================"

if [ "${KEEP_TSAN_SO:-0}" != 1 ]; then
    echo "-- restoring a normal (non-TSan) extension --"
    # Clean ALL build artifacts: setuptools won't relink from a cached .o set,
    # so a partial clean leaves an instrumented .so behind (libtsan TLS error).
    $RM -f src/pygo_core*.so
    $RM -rf build/temp.* build/lib.*
    "$PYTHON" setup.py build_ext --inplace >/tmp/pygo_tsan_restore_build.log 2>&1 \
        && echo "  normal .so restored" \
        || echo "  WARNING: restore build failed -- rebuild with: $PYTHON setup.py build_ext --inplace"
fi
exit "$rc"
