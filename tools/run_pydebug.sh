#!/usr/bin/env bash
# run_pydebug.sh -- run runloom under a --with-pydebug free-threaded CPython so
# CPython's OWN internal assertions are the oracle.  A debug interpreter compiles
# in the asserts that a release build strips: the tstate attach/detach machine,
# the stop-the-world handshake, the gilstate-TSS + biased-refcount thread
# bindings, mimalloc's per-thread heap.  A boundary-contract violation that a
# release build hides as a rare use-after-free aborts HERE, at the exact source
# line, on a trivial workload.  (This is how the gilstate-TSS teardown bug was
# found -- a bare mn_init/run/fini aborted at pystate.c:345; see
# docs/dev/cpython_boundary.md.)
#
# Build the interpreter once with:
#   ./configure --with-pydebug --disable-gil --prefix=<dir>/install && make -j
# then point RUNLOOM_PYDEBUG_PYTHON at its ./python (in-tree is fine).
#
# Usage:  tools/run_pydebug.sh [ITERS]
# Env:    RUNLOOM_PYDEBUG_PYTHON   the pydebug free-threaded interpreter
#                                  (default: /home/x/projects/cpython-pydebug/python)
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
ITERS="${1:-8}"
PYD="${RUNLOOM_PYDEBUG_PYTHON:-/home/x/projects/cpython-pydebug/python}"
RM="$(command -v safe-rm || echo rm)"

[ -x "$PYD" ] || { echo "no pydebug interpreter at $PYD"; echo \
  "  build: ./configure --with-pydebug --disable-gil --prefix=PFX/install && make -j"; exit 2; }
"$PYD" -c "import sys; assert hasattr(sys,'gettotalrefcount'), 'not a --with-pydebug build'; \
assert not sys._is_gil_enabled(), 'GIL is enabled (need --disable-gil)'" \
  || { echo "  $PYD is not a pydebug free-threaded build"; exit 2; }
echo "================ runloom under pydebug free-threaded CPython ================"
echo "  interpreter : $PYD ($("$PYD" -V 2>&1))"

echo "-- building the extension against the pydebug ABI --"
PYTHON_GIL=0 "$PYD" setup.py build_ext --inplace >/tmp/runloom_pydebug_build.log 2>&1 \
  || { echo "  BUILD FAILED -- see /tmp/runloom_pydebug_build.log"; tail -15 /tmp/runloom_pydebug_build.log; exit 2; }
echo "  build OK"

# gc.collect() stop-the-world churn across hubs -- the shape that exposed the
# STW-monopoly deadlock and the gc-churn UAFs; the boundary stressor.
SOAK=$(cat <<'PY'
import gc, os, sys
sys.path.insert(0, os.path.join(os.environ["RUNLOOM_ROOT"], "src"))
import runloom_c
NHUB=int(os.environ.get("HH_NHUB","2")); NWORK=int(os.environ.get("HH_NWORK","96"))
ROUNDS=int(os.environ.get("HH_ROUNDS","120")); NCOLL=int(os.environ.get("HH_NCOLL","2"))
done=runloom_c.Chan(NWORK+NCOLL); stop=[False]
def worker():
    for _ in range(ROUNDS):
        a={};b={}; a["b"]=b;b["a"]=a;a["self"]=a; del a,b
        runloom_c.sched_yield_classic()
    done.send(1)
def collector():
    n=0
    while not stop[0]:
        gc.collect(); n+=1; runloom_c.sched_yield_classic()
    done.send(("gc",n))
def stopper():
    for _ in range(NWORK): done.recv()
    stop[0]=True
    for _ in range(NCOLL): done.recv()
runloom_c.mn_init(NHUB)
for _ in range(NCOLL): runloom_c.mn_fiber(collector)
for _ in range(NWORK): runloom_c.mn_fiber(worker)
runloom_c.mn_fiber(stopper)
runloom_c.mn_run(); runloom_c.mn_fini()
assert runloom_c._self_check(0)==0, "self_check"
print("PASS")
PY
)

echo "-- gc-churn soak x $ITERS (assertions live; an abort = a boundary-contract violation) --"
fails=0
for i in $(seq 1 "$ITERS"); do
    out=$(PYTHON_GIL=0 RUNLOOM_ROOT="$ROOT" timeout 240 "$PYD" -c "$SOAK" 2>&1 | tail -2)
    if echo "$out" | grep -q "^PASS"; then
        printf "  iter %2d: PASS\n" "$i"
    else
        printf "  iter %2d: FAIL -- %s\n" "$i" "$out"; fails=$((fails+1))
    fi
done

echo "-- mn_stress under pydebug --"
PYTHON_GIL=0 PYTHONPATH=src timeout 240 "$PYD" tools/mn_stress.py --iters "$ITERS" >/tmp/runloom_pydebug_mn.log 2>&1 \
    && echo "  mn_stress: PASS" || { echo "  mn_stress: FAIL"; tail -4 /tmp/runloom_pydebug_mn.log; fails=$((fails+1)); }

# STW (M2) TRACE CONFORMANCE: if this pydebug interp is also instrumented with the
# stop-the-world trace (tools/verify/cpython_patches/pystate_stw_trace.patch), validate
# the REAL handshake against RunloomCPythonSTW.tla under TLC.  The pydebug-ABI ext
# is built above, so this is its natural home.  Skips cleanly (not a failure) when
# the interp isn't STW-instrumented or a clean trace can't be captured.
echo "-- STW (M2) trace conformance vs the real stop_the_world --"
RUNLOOM_PYDEBUG_PYTHON="$PYD" bash tools/stw_trace_conform_demo.sh 2>&1 | sed 's/^/  /' \
    || { echo "  STW conformance: FAIL"; fails=$((fails+1)); }

echo "----------------------------------------------------------------------------"
[ "$fails" -eq 0 ] && echo "  CLEAN -- runloom satisfies CPython's internal assertions" \
                   || echo "  $fails failure(s) -- a boundary-contract assert fired; see above"

# Leave a normal (regular-ABI) .so so the tree stays usable with the stock python.
if [ -n "${RUNLOOM_PYTHON:-}" ] || command -v python3.13t >/dev/null 2>&1; then
    STOCK="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
    [ -x "$STOCK" ] && PYTHON_GIL=0 "$STOCK" setup.py build_ext --inplace >/dev/null 2>&1 \
        && echo "  (restored a regular-ABI .so for $STOCK)"
fi
echo "============================================================================"
exit "$fails"
