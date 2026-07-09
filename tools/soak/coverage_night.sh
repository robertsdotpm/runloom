#!/usr/bin/env bash
# coverage_night.sh -- nightly C-coverage recompute (01:00 systemd timer).
#
# Answers "have we actually tested all the logic?" with a NUMBER and a heat
# map, every night: rebuilds the extension instrumented in an ISOLATED git
# worktree (never touching the live tree's .so -- the forever soak + duty
# rotation keep running unperturbed), drives the corpus (isolated suite +
# mn_stress + lifefuzz slice + counted fault sweep -- gcda counters accumulate
# across ALL of it), then:
#   - copies the summary + lcov HTML heat map to docs/dev/soak/coverage/<date>/
#   - appends one line to docs/dev/soak/COVERAGE_LEDGER.md (the trend)
#   - inboxes a regression (whole-extension % dropping >0.5 vs the last entry)
#
# Usage:  tools/soak/coverage_night.sh            # the nightly run
#         tools/soak/coverage_night.sh --smoke    # ~1 min plumbing check
set -u

MAIN="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
WT="${RUNLOOM_COV_WORKTREE:-$HOME/projects/pygo-covnight}"
DATE="$(date +%F)"
OUTDIR="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/coverage/$DATE"
LEDGER="${RUNLOOM_SOAK_DIR:-$HOME/runloom-soak}/COVERAGE_LEDGER.md"
SMOKE=0
[ "${1:-}" = "--smoke" ] && SMOKE=1

# --- isolated worktree at the live tree's HEAD ------------------------------
if [ ! -d "$WT" ]; then
  git -C "$MAIN" worktree add --detach "$WT" HEAD || exit 1
fi
HEAD_SHA="$(git -C "$MAIN" rev-parse HEAD)"
git -C "$WT" checkout --detach "$HEAD_SHA" >/dev/null 2>&1
git -C "$WT" reset --hard "$HEAD_SHA" >/dev/null 2>&1
git -C "$WT" clean -fdxq >/dev/null 2>&1   # stale .so/objs from prior nights

# --- run the measurement in the worktree ------------------------------------
mkdir -p "$OUTDIR"
RUNLOOM_COV_SMOKE="$SMOKE" PYTHON="$PY" \
    bash "$WT/tools/cov_measure.sh" > "$OUTDIR/run.log" 2>&1
rc=$?
cp -f "$WT/build/coverage/workloads.log" "$OUTDIR/" 2>/dev/null
[ -d "$WT/build/coverage/html" ] && cp -r "$WT/build/coverage/html" "$OUTDIR/html"

# --- extract the number + write the ledger line -----------------------------
# cov_subsystem prints: "  WHOLE EXTENSION (coverable)  <total> <cov>  <pct>%"
PCT="$(grep -F "WHOLE EXTENSION (coverable)" "$OUTDIR/run.log" \
       | grep -oE '[0-9]+\.[0-9]+' | tail -1)"
BELOW="$(grep -cE '<95 !!' "$OUTDIR/run.log" 2>/dev/null)"
if [ ! -f "$LEDGER" ]; then
  {
    echo "# Nightly C-coverage ledger (tools/soak/coverage_night.sh, 01:00)"
    echo
    echo "Whole-extension COVERABLE line coverage (exclusion-marker-aware,"
    echo "tools/cov_subsystem.py) over the full corpus: isolated suite +"
    echo "mn_stress + lifefuzz slice + counted fault sweep.  Heat map + logs"
    echo "in coverage/<date>/.  A drop >0.5 points lands in INBOX.md."
    echo
    echo "| date | head | coverable % | TUs <95% | mode |"
    echo "|---|---|---:|---:|---|"
  } > "$LEDGER"
fi
PREV="$(grep -E '^\| [0-9]{4}-' "$LEDGER" | grep -v smoke | tail -1 \
        | awk -F'|' '{gsub(/ /,"",$4); print $4}')"
MODE="full"; [ "$SMOKE" = "1" ] && MODE="smoke"
echo "| $DATE | ${HEAD_SHA:0:8} | ${PCT:-?} | ${BELOW:-?} | $MODE |" >> "$LEDGER"

# --- regression check (full runs only; smoke drives 3 files by design) ------
if [ "$SMOKE" = "0" ] && [ -n "${PCT:-}" ] && [ -n "${PREV:-}" ]; then
  DROP="$(awk -v a="$PREV" -v b="$PCT" 'BEGIN{print (a-b > 0.5) ? 1 : 0}')"
  if [ "$DROP" = "1" ]; then
    "$PY" "$MAIN/tools/soak/inbox.py" --add --kind coverage-regression \
        --title "coverage dropped $PREV% -> $PCT% ($DATE)" \
        --artifact "$OUTDIR/run.log" --date "$DATE"
  fi
fi

echo "coverage-night: rc=$rc pct=${PCT:-?}% TUs<95=${BELOW:-?} out=$OUTDIR"
exit "$rc"
