#!/usr/bin/env bash
# status.sh -- the one-command answer to "how reliable is it this week?"
# (docs/dev/RELIABILITY_PROGRAM.md R4).  Prints the machine-days accrued per
# matrix preset, the open triage-inbox count, and the most recent duty-cycle
# rotation's summary.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
SOAK="$ROOT/docs/dev/soak"
LEDGER="$SOAK/LEDGER.md"
INBOX="$SOAK/INBOX.md"

echo "==================== runloom reliability status ===================="

echo
echo "-- machine-days (soak matrix ledger) --"
if [ -f "$LEDGER" ]; then
  grep -E '^\| ' "$LEDGER" | grep -v '^| preset' | grep -v '^|---' \
    | awk -F'|' '{gsub(/ /,"",$2); gsub(/ /,"",$6); gsub(/ /,"",$9);
                 printf "  %-14s %8s mdays   %s\n", $2, $6, $9}'
  grep -m1 '^\*\*Total' "$LEDGER" | sed 's/\*\*//g; s/^/  /'
else
  echo "  (no ledger yet -- run tools/soak/matrix.sh <preset>)"
fi

echo
echo "-- triage inbox --"
if [ -f "$INBOX" ]; then
  OPEN="$("$PY" "$ROOT/tools/soak/inbox.py" --count 2>/dev/null || echo '?')"
  echo "  $OPEN open item(s)  ($INBOX)"
  grep -m5 '^- \[ \] ' "$INBOX" 2>/dev/null | sed 's/^- \[ \] /  · /' | cut -c1-90
else
  echo "  (empty)"
fi

echo
echo "-- last duty-cycle rotations --"
if [ -d "$SOAK/inbox_artifacts" ]; then
  ls -1dt "$SOAK"/inbox_artifacts/*/ 2>/dev/null | head -3 | while read -r d; do
    echo "  $(basename "$d"): $(ls "$d" 2>/dev/null | tr '\n' ' ')"
  done
else
  echo "  (none yet -- tools/soak/duty_cycle.sh)"
fi
echo
echo "===================================================================="
