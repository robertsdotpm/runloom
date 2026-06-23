#!/usr/bin/env bash
# run_cldeque_disjoint.sh -- CBMC monitor for INV_race (segment-disjointness +
# TAKEN-once) on the REAL src/runloom_c/cldeque.c (compiled with the zero-cost
# RUNLOOM_CLDEQUE_VERIFY ghost hooks).  Correct run must be SUCCESSFUL; the
# -DBUG_SELFTEST run must FAIL (proving the monitor has teeth).
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="${RUNLOOM_ROOT:-$(cd "$HERE/../.." && pwd)}"
SRC="$ROOT/src/runloom_c"
echo "-- CBMC INV_race disjointness monitor (real cldeque.c) --"
if ! command -v cbmc >/dev/null 2>&1; then
  echo "  (cbmc not found -- skipping; apt-get install cbmc)"; exit 0
fi
pass=0; fail=0
run() { cbmc "$HERE/cldeque_disjoint.c" "$SRC/cldeque.c" \
        -I "$HERE/stubs" -I "$SRC" -DRUNLOOM_CLDEQUE_CAP=4 -DRUNLOOM_CLDEQUE_VERIFY \
        --unwind 8 --unwinding-assertions $1 2>&1; }

printf '  [cbmc] %-40s ' "INV_race monitor (expect SUCCESSFUL)"
if run "" | grep -q "VERIFICATION SUCCESSFUL"; then
  echo "PASS"; pass=$((pass+1)); else echo "FAIL"; fail=$((fail+1)); fi

printf '  [cbmc] %-40s ' "-DBUG_SELFTEST (expect FAILED = teeth)"
if run "-DBUG_SELFTEST" | grep -q "VERIFICATION FAILED"; then
  echo "PASS (monitor correctly trips)"; pass=$((pass+1)); else echo "FAIL"; fail=$((fail+1)); fi

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
