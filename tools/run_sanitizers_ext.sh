#!/usr/bin/env bash
# run_sanitizers_ext.sh -- build the runloom_c EXTENSION under ThreadSanitizer
# and run real workloads under the free-threaded interpreter to hunt data races
# in runloom's own C (scheduler / chan / select / netpoll / coro).
#
# This complements tools/run_sanitizers.sh, which TSans only the standalone C
# deque harness (test_cldeque).  Here the *whole runtime* runs under TSan while
# driven by real goroutines on real OS threads with the GIL off -- the regime
# where runloom's lock-free park/wake/select bugs actually live.
#
# Default (ext-only): build the ext with -fsanitize=thread and force-load
# libtsan into a stock free-threaded CPython.  TSan instruments every load/store
# in the ext (including inlined Py_INCREF / atomics) -- exactly runloom's code --
# and is blind to the uninstrumented interpreter's internals, the few of which
# that surface are filtered by tools/tsan_suppressions.txt.  Needs no patched
# CPython.
#
# Gold standard (RUNLOOM_TSAN_PYTHON=/path/to/tsan/python3.13t): build + run under
# a FULLY TSan-instrumented free-threaded interpreter (tools/build_tsan_cpython.sh)
# so races crossing into CPython internals are attributed too.  Set
# RUNLOOM_TSAN_CPYTHON_SUPP to that tree's suppressions_free_threading.txt to mute
# the known CPython free-threading races.  (Verified: runloom's C is TSan-clean
# under it; the only reports are CPython's own _Py_ExplicitMergeRefcount /
# tstate_activate internals.)
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
    for cand in "$HOME/.pyenv/versions/3.14.4t/bin/python3" python3.13t python3; do
        command -v "$cand" >/dev/null 2>&1 && { PYTHON="$cand"; break; }
    done
fi
LIBTSAN="$(gcc -print-file-name=libtsan.so)"
[ -f "$LIBTSAN" ] || { echo "libtsan.so not found (need gcc with TSan)"; exit 2; }
RM="$(command -v safe-rm || echo rm)"

SA=""
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R"
[ -z "$SA" ] && echo "WARNING: setarch not found; TSan may abort under ASLR"

# Optional gold-standard mode: a FULLY TSan-instrumented free-threaded
# interpreter (build it with tools/build_tsan_cpython.sh).  Point
# RUNLOOM_TSAN_PYTHON at it and the ext is built against + run UNDER it directly --
# no libtsan preload -- so races crossing into CPython internals are attributed
# too.  Set RUNLOOM_TSAN_CPYTHON_SUPP to that tree's
# Tools/tsan/suppressions_free_threading.txt to fold in the known CPython
# free-threading races.  Unset = the default preload path (instruments only the
# ext; needs no patched interpreter).
FULL_PY="${RUNLOOM_TSAN_PYTHON:-}"
SUPP_EFF="$SUPP"
if [ -n "$FULL_PY" ]; then
    [ -x "$FULL_PY" ] || { echo "RUNLOOM_TSAN_PYTHON=$FULL_PY not executable"; exit 2; }
    BUILD_PY="$FULL_PY"; RUN_PY="$FULL_PY"; PRELOAD=""; MODE="fully-instrumented interpreter (no preload)"
    if [ -n "${RUNLOOM_TSAN_CPYTHON_SUPP:-}" ] && [ -f "${RUNLOOM_TSAN_CPYTHON_SUPP}" ]; then
        SUPP_EFF="$(mktemp /tmp/runloom_tsan_supp.XXXX.txt)"
        cat "$SUPP" "$RUNLOOM_TSAN_CPYTHON_SUPP" > "$SUPP_EFF"
    fi
else
    BUILD_PY="$PYTHON"; RUN_PY="$PYTHON"; PRELOAD="$LIBTSAN"; MODE="ext-only (libtsan preload)"
fi

echo "================ runloom ext under ThreadSanitizer ================"
echo "  build/run python : $RUN_PY"
echo "  mode             : $MODE"
echo "  suppressions     : $SUPP_EFF"

echo "-- building instrumented extension (-fsanitize=thread -O1) --"
# Clean build/lib.* too: with a stale cached .so there, setuptools skips
# compilation entirely and just copies it (a non-instrumented .so sneaks in).
$RM -f src/runloom_c*.so
$RM -rf build/temp.tsan build/lib.*
# setarch -R: in full-interpreter mode BUILD_PY is itself TSan-instrumented and
# would abort under ASLR while running setup.py; harmless for a normal BUILD_PY.
$SA env RUNLOOM_EXTRA_CFLAGS="-fsanitize=thread -g -O1 -fno-omit-frame-pointer" \
RUNLOOM_EXTRA_LDFLAGS="-fsanitize=thread" \
    "$BUILD_PY" setup.py build_ext --inplace --build-temp build/temp.tsan \
    >/tmp/runloom_tsan_ext_build.log 2>&1 \
    || { echo "  BUILD FAILED -- see /tmp/runloom_tsan_ext_build.log"; tail -20 /tmp/runloom_tsan_ext_build.log; exit 2; }
ldd src/runloom_c*.so | grep -q tsan \
    && echo "  build OK (links libtsan)" \
    || { echo "  ext is NOT instrumented (no libtsan in ldd)"; exit 2; }

LOGDIR="$(mktemp -d /tmp/runloom_tsan.XXXX)"
# halt_on_error=0: collect ALL races across the run, don't stop at the first.
export TSAN_OPTIONS="halt_on_error=0:report_bugs=1:history_size=7:suppressions=$SUPP_EFF:log_path=$LOGDIR/tsan"
run() {  # label, cmd...
    local label="$1"; shift
    printf -- "-- %-24s " "$label"
    local pre=""
    [ -n "$PRELOAD" ] && pre="LD_PRELOAD=$PRELOAD"
    if $SA env PYTHON_GIL=0 $pre PYTHONPATH=src "$@" \
            >"$LOGDIR/$label.out" 2>&1; then
        echo "ran (exit 0)"
    else
        echo "ran (exit $? -- TSan exitcode 66 = races found, see summary)"
    fi
}

echo "-- driving workloads under TSan --"
run mn_stress   "$RUN_PY" tools/mn_stress.py --iters "$MN_ITERS"
run lincheck_plain  "$RUN_PY" tools/lincheck/record_history.py "$LOGDIR/h_plain.json"  4 3 8 2 0
run lincheck_select "$RUN_PY" tools/lincheck/record_history.py "$LOGDIR/h_select.json" 4 3 8 2 3
# monkey-offload cross-thread unpark: a patched Lock/Queue on a non-goroutine
# feed thread wakes a parked goroutine -- exercises the foreign-thread wake path
# under TSan at seconds cost (was orphaned; never ran under any sanitizer).
run monkey_offload  "$RUN_PY" tools/monkey_offload_stress.py 48 15 1

# Default set: the chan/sched stressors PLUS the blocking shims that touch real
# OS threads -- the goroutine-backed executor's cross-thread Future delivery,
# the backend pool's self-pipe parkers (offload / heavy / file syscalls), and
# the cooperative threading primitives.  Those are the data-race-prone paths the
# monkey layer added.  Fork-based files (process/mp/fcntl) are left out (fork
# under TSan is unreliable).  Override with TSAN_PYTEST="...".
PYTEST_TARGETS="${TSAN_PYTEST-tests/test_chan_stress.py tests/test_sched_fairness.py tests/test_chan_queue.py tests/test_futures_compat.py tests/test_threading_compat.py tests/test_heavy_compat.py tests/test_blocking.py tests/test_os_io_compat.py}"
if [ -n "$PYTEST_TARGETS" ]; then
    run pytest_subset "$RUN_PY" -m pytest $PYTEST_TARGETS -q -p no:cacheprovider --no-header
fi

echo "----------------------------------------------------------------"
echo "  race reports (non-suppressed), by site:"
shopt -s nullglob
reports=("$LOGDIR"/tsan.*)
if [ ${#reports[@]} -eq 0 ]; then
    echo "    NONE -- runloom ext is TSan-clean across all workloads"
    rc=0
else
    cat "$LOGDIR"/tsan.* | grep "SUMMARY: ThreadSanitizer" | sort | uniq -c | sort -rn | sed 's/^/    /'
    # Judge by the SUMMARY line -- the authoritative racing site -- NOT any stack
    # frame.  In full-interpreter mode runloom frames legitimately appear deep in
    # the call stack of a CPython-internal race (e.g. a refcount merge triggered
    # from a goroutine); only a SUMMARY pointing at src/runloom_c is a runloom bug.
    if cat "$LOGDIR"/tsan.* | grep "SUMMARY: ThreadSanitizer" | grep -q "src/runloom_c/"; then
        echo "  >>> data races in runloom's own C -- see $LOGDIR/tsan.*"
        rc=1
    else
        echo "  (all race summaries are in CPython internals; runloom's C is clean)"
        rc=0
    fi
fi
echo "  logs: $LOGDIR"
echo "================================================================"

if [ "${KEEP_TSAN_SO:-0}" != 1 ]; then
    echo "-- restoring a normal (non-TSan) extension --"
    # Clean ALL build artifacts: setuptools won't relink from a cached .o set,
    # so a partial clean leaves an instrumented .so behind (libtsan TLS error).
    $RM -f src/runloom_c*.so
    $RM -rf build/temp.* build/lib.*
    "$PYTHON" setup.py build_ext --inplace >/tmp/runloom_tsan_restore_build.log 2>&1 \
        && echo "  normal .so restored" \
        || echo "  WARNING: restore build failed -- rebuild with: $PYTHON setup.py build_ext --inplace"
fi
exit "$rc"
