#!/usr/bin/env bash
# nidhugg.sh -- second weak-memory stateless model checker on the REAL netpoll
# claim protocol, complementing GenMC.
#
# GenMC (verify/genmc) already explores every RC11 execution of
# genmc/netpoll_claim.c.  Nidhugg is an INDEPENDENT stateless model checker
# (LLVM + a different DPOR algorithm, source-DPOR / optimal-DPOR) supporting
# SC/TSO/PSO/POWER.  Running it on the SAME C harness gives a second,
# algorithmically-distinct confirmation: agreement across GenMC + Nidhugg on
# the claim protocol is much stronger evidence than either alone.  (It also
# checks TSO/PSO, which GenMC's RC11 run does not isolate.)
#
# Nidhugg builds from source against an LLVM/clang dev toolchain (not in apt);
# this script skips cleanly when nidhugg/clang are absent.
#
# Install: https://github.com/nidhugg/nidhugg  (needs llvm-dev + clang)
# Run:     tools/extra/nidhugg.sh
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SRC="$ROOT/verify/genmc/netpoll_claim.c"   # reuse the GenMC pthreads+C11 harness

if ! command -v nidhugg >/dev/null 2>&1; then
    echo "[nidhugg] not installed -- skipping (see https://github.com/nidhugg/nidhugg)"; exit 0
fi
if ! command -v clang >/dev/null 2>&1; then
    echo "[nidhugg] clang not found -- skipping (nidhugg needs the LLVM toolchain)"; exit 0
fi

rc=0
for mm in sc tso pso; do
    printf '[nidhugg] %-4s ' "$mm"
    if nidhugg "--$mm" -- -I "$ROOT/verify/genmc" "$SRC" 2>&1 | grep -q "No errors were detected"; then
        echo "PASS -- no error under $mm"
    else
        echo "FAIL -- see output"; rc=1
    fi
done
# negative control: the no-lock variant must be caught (matches GenMC's -DBUG_NO_LOCK)
printf '[nidhugg] %-4s ' "neg"
if nidhugg --sc -- -DBUG_NO_LOCK -I "$ROOT/verify/genmc" "$SRC" 2>&1 | grep -q "Error"; then
    echo "PASS -- correctly DETECTS the ready_out race without the lock round-trip"
else
    echo "FAIL -- expected the no-lock bug to be caught"; rc=1
fi
exit $rc
