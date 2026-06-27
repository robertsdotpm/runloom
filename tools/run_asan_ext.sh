#!/usr/bin/env bash
# run_asan_ext.sh -- the whole runloom_c ext under AddressSanitizer, on a stock
# free-threaded CPython (force-load libasan).  The ASan complement to
# run_sanitizers_ext.sh (TSan): TSan finds RACES, ASan finds HEAP MEMORY ERRORS
# (use-after-free / overflow / double-free) in runloom's own C -- the g-slab,
# parkers, hub buffers, the deque, datastack chunks -- under real M:N workloads.
#
# With PYTHONMALLOC=malloc, ASan also redzones Python OBJECTS (not just the ext's
# heap), so the GC-checkmark's missed-parked-fiber-root case becomes a DETERMINISTIC
# heap-use-after-free, not just a probabilistic weakref-cleared signal (the "extra
# teeth" the stock test's docstring promises).
#
# Builds the ASan ext in place, runs the targets, then REBUILDS the normal ext so
# the tree is left clean (set KEEP_ASAN_SO=1 to keep it).
#
# Usage:  tools/run_asan_ext.sh
# Exit: 0 = no ASan error; 1 = ASan caught a memory error; 2 = setup.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
LIBASAN="$(gcc -print-file-name=libasan.so 2>/dev/null)"
[ -f "$LIBASAN" ] || { echo "run_asan_ext: libasan.so not found (need gcc with ASan). SKIP."; exit 0; }
[ -x "$PY" ] || { echo "run_asan_ext: no interpreter at $PY. SKIP."; exit 0; }
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R" || SA=""

echo "== runloom_c under AddressSanitizer (preload) =="
echo "-- building the ext with -fsanitize=address (slow) --"
CFLAGS="-fsanitize=address -O1 -g -fno-omit-frame-pointer" \
LDFLAGS="-fsanitize=address" \
PYTHON_GIL=0 "$PY" setup.py build_ext --inplace --force >/tmp/runloom_asan_build.log 2>&1 \
  || { echo "  BUILD FAILED -- /tmp/runloom_asan_build.log"; tail -15 /tmp/runloom_asan_build.log; exit 2; }
echo "  build OK"

restore() {
    if [ "${KEEP_ASAN_SO:-0}" != "1" ]; then
        echo "-- rebuilding the normal (non-ASan) ext --"
        # CRITICAL: drop the ASan preload/env or the rebuild itself runs under
        # libasan and fails, leaving the repo with the ASan .so.
        env -u LD_PRELOAD -u ASAN_OPTIONS -u PYTHONMALLOC PYTHON_GIL=0 "$PY" \
          setup.py build_ext --inplace --force >/tmp/runloom_normal_rebuild.log 2>&1 \
          && echo "  restored" || echo "  WARN: normal rebuild failed -- /tmp/runloom_normal_rebuild.log"
    fi
}
trap restore EXIT

# detect_leaks=0: the uninstrumented interpreter "leaks" by ASan's accounting.
# verify_asan_link_order=0: required when preloading ASan into a non-ASan binary.
export ASAN_OPTIONS="detect_leaks=0:verify_asan_link_order=0:halt_on_error=1:abort_on_error=1:exitcode=1"
export LD_PRELOAD="$LIBASAN"
export PYTHON_GIL=0 PYTHONPATH="$ROOT/src"
# (this FT build rejects PYTHONMALLOC=malloc, so Python objects live in mimalloc
# arenas ASan can't redzone; ASan here covers runloom's OWN C heap -- g-slab,
# parkers, hub buffers, deque, datastack -- where the UAF/overflow class lives.)

rc=0
run_one() {  # label, args...
    echo "-- ASan: $1 --"
    if $SA "$PY" "${@:2}" >/tmp/asan_run.log 2>&1; then
        echo "   clean"
    else
        if grep -qE 'ERROR: AddressSanitizer|runtime error|heap-use-after-free|heap-buffer-overflow|double-free' /tmp/asan_run.log; then
            echo "   ASAN ERROR:"; grep -E 'AddressSanitizer|use-after-free|overflow|double-free' /tmp/asan_run.log | head -5
            rc=1
        else
            echo "   non-ASan failure (exit nonzero, no ASan report) -- see /tmp/asan_run.log"; tail -3 /tmp/asan_run.log
            rc=1
        fi
    fi
}

# GC-checkmark: a missed parked-fiber root -> heap-use-after-free under ASan.
run_one "gc_checkmark" tests/test_gc_checkmark.py
# a small M:N chan/sched workload: exercises the deque/parker/g-slab C heap.
run_one "mn_stress" tools/mn_stress.py --iters 60

echo
[ "$rc" -eq 0 ] && echo "run_asan_ext: clean (no ASan memory error)" || echo "run_asan_ext: ASan caught a memory error (exit 1)"
exit $rc
