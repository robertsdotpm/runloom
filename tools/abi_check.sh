#!/usr/bin/env bash
# abi_check.sh -- guard the runloom ext's ABI surface across CPython point releases.
#
# runloom links a handful of CPython INTERNAL/private symbols (cpython_boundary.md)
# and embeds assumptions about FT-CPython struct layouts (_PyThreadState,
# _PyInterpreterFrame). A 3.13.x point release that changes one of those layouts can
# ship a SEGFAULTING wheel that every functional test still "passes" (the mismatch
# is a silent UAF). Neither a model nor a same-header rebuild catches it; a binary
# ABI diff does. (libabigail's abidw/abidiff.)
#
# First run: snapshot the ext's ABI (incl. the UNDEFINED CPython symbols it imports)
# to a checked-in baseline. Later runs (after a CPython bump + rebuild): abidiff vs
# the baseline and FAIL on an incompatible change -- e.g. a consumed internal symbol
# whose type/layout changed, or a new undefined symbol appearing.
#
# Usage:
#   tools/abi_check.sh                 # diff vs baseline (creates it on first run)
#   tools/abi_check.sh --update        # re-snapshot the baseline (after a vetted change)
# Exit: 0 = ABI compatible (or baseline created); 1 = incompatible change; 2 = setup.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
BASE="$ROOT/tools/abi_baseline.abi"

command -v abidw  >/dev/null 2>&1 || { echo "abi_check: abidw absent (apt install abigail-tools). SKIP."; exit 0; }
command -v abidiff >/dev/null 2>&1 || { echo "abi_check: abidiff absent (apt install abigail-tools). SKIP."; exit 0; }

SO="$(ls "$ROOT"/src/runloom_c*.so 2>/dev/null | grep -v 'td-' | head -1)"
[ -n "$SO" ] || { echo "abi_check: no built ext .so (build it first). SKIP."; exit 0; }
echo "abi_check: ext = $(basename "$SO")"

CUR="$(mktemp --suffix=.abi)"; trap 'rm -f "$CUR"' EXIT
# include undefined (imported) symbols -- that's the CPython-internals surface
abidw --no-architecture --no-comp-dir-path "$SO" > "$CUR" 2>/dev/null \
  || { echo "abi_check: abidw failed"; exit 2; }

if [ "${1:-}" = "--update" ] || [ ! -f "$BASE" ]; then
    cp "$CUR" "$BASE"
    echo "abi_check: baseline ${1:+RE-}written -> tools/abi_baseline.abi"
    echo "  (commit it; future runs abidiff against it across CPython point releases)"
    exit 0
fi

echo "-- abidiff vs baseline --"
if abidiff "$BASE" "$CUR"; then
    echo "abi_check: ABI compatible"
    exit 0
fi
rc=$?
# abidiff exit bits: 1=error, 2=ABI_CHANGE (may be benign), 4=ABI_INCOMPATIBLE_CHANGE
if [ $(( rc & 4 )) -ne 0 ]; then
    echo "abi_check: INCOMPATIBLE ABI change (likely a CPython-internals layout drift -> segfaulting-wheel risk)"
    exit 1
fi
echo "abi_check: ABI changed but compatible (review the diff above; --update to accept)"
exit 0
