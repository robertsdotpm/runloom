#!/usr/bin/env bash
# c11tester.sh -- C11Tester on the REAL netpoll claim protocol, a third weak-memory
# angle complementing GenMC (RC11) and Nidhugg (source-DPOR).
#
# C11Tester (Luo & Demsky, the ARM-fragment-supporting successor to CDSChecker)
# is a CONSTRAINT-BASED stateless checker that actively controls memory-order
# choices, exploring weak-memory executions on real C11 atomics. Running it on the
# SAME genmc/netpoll_claim.c harness (pthreads + C11 atomics) gives a third
# algorithmically-distinct confirmation of the claim protocol, and -- being
# constraint-based rather than enumerative -- scales to larger real C than GenMC's
# hand-extracted harnesses. Agreement across GenMC + Nidhugg + C11Tester is much
# stronger than any one alone.
#
# C11Tester builds from source against LLVM/clang (its own clang plugin + runtime,
# not in apt). This script skips cleanly when it's absent -- runnable the moment
# it's installed, like nidhugg.sh.
#
# Install: https://plrg.ics.uci.edu/c11tester/  (git: github.com/c11tester/c11tester)
#   build it, then either put its `cc`/`c++` wrappers on PATH as `c11tester-cc`,
#   or set C11TESTER_HOME to the build dir (expects $C11TESTER_HOME/libmodel.so +
#   the instrumenting clang wrapper).
# Run: tools/verify/extra/c11tester.sh
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC="$ROOT/verify/genmc/netpoll_claim.c"   # reuse the GenMC pthreads + C11 harness
[ -f "$SRC" ] || SRC="$ROOT/tools/verify/genmc/netpoll_claim.c"

# locate a C11Tester toolchain: a c11tester-cc wrapper on PATH, or C11TESTER_HOME
CC11=""
if command -v c11tester-cc >/dev/null 2>&1; then
    CC11="c11tester-cc"
elif [ -n "${C11TESTER_HOME:-}" ] && [ -x "$C11TESTER_HOME/cc" ]; then
    CC11="$C11TESTER_HOME/cc"
fi
if [ -z "$CC11" ]; then
    echo "[c11tester] not installed -- skipping (see https://plrg.ics.uci.edu/c11tester/; "
    echo "            build it, then put c11tester-cc on PATH or set C11TESTER_HOME)"
    exit 0
fi
[ -f "$SRC" ] || { echo "[c11tester] harness $SRC missing -- skip"; exit 0; }

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
INCL="-I $(dirname "$SRC")"

run_one() {  # label, extra-cflags, expect (clean|bug)
    local label="$1" extra="$2" expect="$3"
    if ! $CC11 $INCL $extra "$SRC" -o "$tmp/t_$label" 2>"$tmp/build_$label.log"; then
        echo "[c11tester] $label: BUILD FAILED -- $tmp/build_$label.log"; return 2
    fi
    # C11Tester explores executions on run; multiple passes shake out schedules.
    local out; out="$($tmp/t_$label 2>&1)"
    if echo "$out" | grep -qiE 'bug|data race|assertion|uninitialized load'; then
        found=bug
    else
        found=clean
    fi
    if [ "$found" = "$expect" ]; then
        echo "[c11tester] $label: OK (expected $expect)"
        return 0
    fi
    echo "[c11tester] $label: MISMATCH (expected $expect, got $found)"; return 1
}

rc=0
# positive: the real claim protocol must be clean under C11Tester's MO search.
run_one claim "" clean || rc=1
# negative control (matches GenMC's -DBUG_NO_LOCK): the no-lock read race must be FOUND.
run_one neg "-DBUG_NO_LOCK" bug || rc=1

[ $rc -eq 0 ] && echo "[c11tester] all checks held" || echo "[c11tester] a check failed (exit 1)"
exit $rc
