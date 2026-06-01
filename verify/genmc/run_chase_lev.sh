#!/usr/bin/env bash
# run_chase_lev.sh -- GenMC (RC11) oracle for the Chase-Lev work-stealing deque.
# This is the de-risking oracle for the iRC11 proof effort and the source of the
# "which fences does the algorithm actually need" map.  Each correct model must
# report "No errors"; each negative control must FIND its bug (loss/dup/race),
# proving the spec has teeth.  Prints "N passed, M failed".
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
G="${GENMC:-}"
if [ -z "$G" ]; then
  for c in "$(command -v genmc 2>/dev/null)" /usr/local/bin/genmc/genmc \
           /usr/local/bin/genmc /tmp/genmc/build2/bin/genmc; do
    [ -n "$c" ] && [ -x "$c" ] && [ ! -d "$c" ] && { G="$c"; break; }
  done
fi
echo "-- GenMC (RC11) Chase-Lev deque oracle --"
if [ -z "$G" ] || [ ! -x "$G" ]; then
  echo "  (genmc not found -- skipping; set GENMC=/path/to/genmc)"; exit 0
fi
cd "$HERE"; pass=0; fail=0
ok()  { printf '  [genmc] %-44s ' "$1"; }
# want_clean FILE [CPPFLAGS...]
want_clean() { ok "$1 $2"; if "$G" -- $2 "$1" 2>&1 | grep -q "No errors were detected"; then
  echo "PASS (no loss/dup/race under RC11)"; pass=$((pass+1)); else echo "FAIL"; fail=$((fail+1)); fi; }
# want_bug FILE LABEL [CPPFLAGS...]
want_bug() { ok "$1 $3 ($2)"; if "$G" -- $3 "$1" 2>&1 | grep -qiE "violation|race"; then
  echo "PASS (correctly found the bug)"; pass=$((pass+1)); else echo "FAIL (bug not found!)"; fail=$((fail+1)); fi; }

# --- single-element take/steal race ---
want_clean chase_lev.c ""
want_bug   chase_lev.c "duplication" "-DBUG_NO_CAS"
# finding: SC fence redundant at 1 element -> this control is EXPECTED to be clean
ok "chase_lev.c -DBUG_NO_FENCE (fence redundant @1elt)"
if "$G" -- -DBUG_NO_FENCE chase_lev.c 2>&1 | grep -q "No errors were detected"; then
  echo "PASS (CAS alone arbitrates; finding)"; pass=$((pass+1)); else echo "FAIL"; fail=$((fail+1)); fi

# --- two-element: SC fence becomes necessary ---
want_clean chase_lev2.c ""
want_bug   chase_lev2.c "duplication" "-DBUG_NO_FENCE"

# --- resize / grow-and-copy concurrent with thieves ---
want_clean chase_lev_resize.c ""
want_bug   chase_lev_resize.c "data race on new buffer" "-DBUG_RLX_ARR"

# --- the ACTUAL production deque (src/pygo_core/cldeque.c), driven verbatim ---
# Locate the repo from this script so the sweep works in any checkout/worktree.
ROOT="$(cd "$HERE/../.." && pwd)"
SRC="${PYGO_SRC:-$ROOT/src/pygo_core}"
if [ -f "$SRC/cldeque.c" ]; then
  ok "chase_lev_real.c (REAL cldeque.c, 2elt pop+2steal)"
  if "$G" -- -I"$SRC" chase_lev_real.c 2>&1 | grep -q "No errors were detected"; then
    echo "PASS (production code clean under RC11)"; pass=$((pass+1))
  else echo "FAIL"; fail=$((fail+1)); fi
else echo "  (production cldeque.c not found at $SRC -- skipping real-code check)"; fi

echo "  $pass passed, $fail failed"
[ "$fail" -eq 0 ]
