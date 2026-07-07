#!/usr/bin/env bash
# build_target.sh <TU_basename>  (e.g. netpoll, mn_sched, chan)
#
# Produce a schemata-mutated build of ONE translation unit, in an ISOLATED git
# worktree (never the live tree -- the soaks keep running):
#   1. flatten the TU (inline its .inc fragments so dredd can reach them);
#   2. dredd-rewrite the flat file (every op/expr -> runtime-selectable mutant);
#   3. drop dredd's `thread_local` prelude qualifier (a dlopen'd .so can't get
#      the static-TLS surge block; the mutant set is process-global via the env
#      var, so a plain global is equivalent);
#   4. swap it in as the TU and build the WHOLE extension ONCE.
#
# Leaves in the worktree: the built .so, and next to it
#   <TU>.mutants.json  -- dredd's mutation info (mutant id -> flat file:line)
#   <TU>.flat.map.json -- flat line -> real .inc file:line (from flatten.py)
# which tools/mutate/schemata/sweep.py consumes.
#
# Env:  RUNLOOM_MUT_WORKTREE (default ~/projects/pygo-mutants)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
MAIN="$(cd "$HERE/../../.." && pwd)"
TU="${1:?usage: build_target.sh <TU_basename e.g. netpoll>}"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
PYINC="$("$PY" -c 'import sysconfig; print(sysconfig.get_path("include"))')"
WT="${RUNLOOM_MUT_WORKTREE:-$HOME/projects/pygo-mutants}"
DREDD="$HERE/dredd/dredd/bin/dredd"
RESDIR="$(clang-18 -print-resource-dir)"
RM="$(command -v safe-rm || echo rm)"

[ -x "$DREDD" ] || { echo "dredd not installed -- run tools/mutate/schemata/setup_dredd.sh"; exit 1; }

# --- isolated worktree at live HEAD ----------------------------------------
if [ ! -d "$WT" ]; then git -C "$MAIN" worktree add --detach "$WT" HEAD; fi
git -C "$WT" reset --hard "$(git -C "$MAIN" rev-parse HEAD)" >/dev/null 2>&1
git -C "$WT" clean -fdxq
cd "$WT"

SRC="src/runloom_c/$TU.c"
[ -f "$SRC" ] || { echo "no such TU: $SRC"; exit 1; }
FLAT="$WT/src/runloom_c/${TU}.flat.c"
MUTJSON="$WT/src/runloom_c/${TU}.mutants.json"

echo "=== [1/4] flatten $TU.c (inline .inc fragments) ==="
"$PY" "$HERE/flatten.py" "$SRC" "$FLAT"

echo "=== [2/4] dredd-rewrite the flat file ==="
"$DREDD" "$FLAT" --mutation-info-file "$MUTJSON" -- \
    -fno-strict-overflow -DNDEBUG -O2 -fPIC \
    -I"$WT/src/runloom_c" -I"$PYINC" -isystem "$RESDIR/include" \
    -std=gnu11 -D_GNU_SOURCE || { echo "dredd failed"; exit 1; }
NMUT="$("$PY" -c "import json;print(json.dumps(json.load(open('$MUTJSON'))).count('mutationId'))")"

echo "=== [3/4] de-TLS the prelude + swap in as the TU ==="
sed -i 's/static thread_local/static/g' "$FLAT"
cp "$FLAT" "$SRC"                       # the flat file IS self-contained now

echo "=== [4/4] build the whole extension ONCE ($NMUT mutants embedded) ==="
$RM -f src/runloom_c*.so 2>/dev/null
PYTHON_GIL=0 "$PY" setup.py build_ext --inplace > "$WT/mutant_build.log" 2>&1 \
  || { echo "BUILD FAILED -- see $WT/mutant_build.log"; tail -20 "$WT/mutant_build.log"; exit 1; }
PYTHON_GIL=0 PYTHONPATH=src "$PY" -c "import runloom_c" \
  || { echo "IMPORT FAILED"; exit 1; }
echo "OK: mutated $TU built + imports.  $NMUT mutants."
echo "  mutants json: $MUTJSON"
echo "  line map:     ${FLAT%.c}.map.json"
echo "  sweep it:     tools/mutate/schemata/sweep.py $TU"
