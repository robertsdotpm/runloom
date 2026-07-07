#!/usr/bin/env bash
# build_faultsites.sh <TU>  -- instrument EVERY fallible call in one TU with a
# runtime-selectable realistic-errno fault, and build the whole extension ONCE.
# The systematic (no hand-picked sites) counterpart of the compiled-in
# RUNLOOM_FAULT_* hooks.  Runs in the isolated mutant worktree.
#
#   1. flatten TU (reach the .inc fragments -- reuse the schemata flattener);
#   2. inject_rewrite.py wraps every fallible call site (libclang AST);
#   3. swap in + build once.  Then fault_sweep.py enables id=0..N-1 in turn.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
MAIN="$(cd "$HERE/../../.." && pwd)"
TU="${1:?usage: build_faultsites.sh <TU e.g. netpoll>}"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
PYINC="$("$PY" -c 'import sysconfig; print(sysconfig.get_path("include"))')"
WT="${RUNLOOM_MUT_WORKTREE:-$HOME/projects/pygo-mutants}"
RESDIR="$(clang-18 -print-resource-dir)"
RM="$(command -v safe-rm || echo rm)"
FLATTEN="$MAIN/tools/mutate/schemata/flatten.py"

if [ ! -d "$WT" ]; then git -C "$MAIN" worktree add --detach "$WT" HEAD; fi
git -C "$WT" reset --hard "$(git -C "$MAIN" rev-parse HEAD)" >/dev/null 2>&1
git -C "$WT" clean -fdxq
cd "$WT"

SRC="src/runloom_c/$TU.c"
FLAT="src/runloom_c/${TU}.flat.c"
INJ="src/runloom_c/${TU}.fi.c"
SITES="src/runloom_c/${TU}.fisites.json"
[ -f "$SRC" ] || { echo "no such TU: $SRC"; exit 1; }

echo "=== [1/3] flatten $TU.c ==="
"$PY" "$FLATTEN" "$SRC" "$FLAT"

echo "=== [2/3] instrument fallible call sites (libclang) ==="
"$PY" "$HERE/inject_rewrite.py" "$FLAT" "$INJ" "$SITES" \
    --mapfile "${FLAT%.c}.map.json" -- \
    -I"$WT/src/runloom_c" -I"$PYINC" -isystem "$RESDIR/include" \
    -D_GNU_SOURCE -DNDEBUG || { echo "instrument failed"; exit 1; }
NSITES="$("$PY" -c "import json;print(len(json.load(open('$SITES'))))")"

echo "=== [3/3] swap in + build the whole extension ONCE ($NSITES sites) ==="
cp "$INJ" "$SRC"
$RM -f src/runloom_c*.so 2>/dev/null
PYTHON_GIL=0 "$PY" setup.py build_ext --inplace > "$WT/faultsite_build.log" 2>&1 \
  || { echo "BUILD FAILED -- see $WT/faultsite_build.log"; tail -25 "$WT/faultsite_build.log"; exit 1; }
PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "import runloom_c" || { echo "IMPORT FAILED"; exit 1; }
echo "OK: $TU instrumented + built.  $NSITES fallible call sites."
echo "  sites: $SITES"
echo "  sweep: tools/mutate/faultsites/fault_sweep.py $TU"
