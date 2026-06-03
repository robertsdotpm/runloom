#!/usr/bin/env bash
# check_lin.sh -- linearizability pipeline for runloom channels.
#
#   1. record a concurrent send/recv/close history from a real M:N run
#      -- twice: once with plain recv consumers, once with select() consumers;
#   2. check both against the sequential FIFO-channel spec with Porcupine
#      (expect LINEARIZABLE).  The select run proves select-recv linearizes
#      identically to recv while driving chan.c's multi-waiter Phase-2 path;
#   3. teeth: corrupt the history (phantom delivery) and re-check
#      (expect NOT LINEARIZABLE);
#   4. run the stateful Hypothesis model of the channel API (send/recv/close
#      plus a genuine two-channel select rule).
#
# Usage:  tools/lincheck/check_lin.sh
# Env:    PYTHON=...  interpreter (default: free-threaded 3.13t if present)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.13.13t/bin/python3" python3.13t python3; do
        command -v "$cand" >/dev/null 2>&1 && { PYTHON="$cand"; break; }
    done
fi
export PYTHON PYTHON_GIL=0
RM="$(command -v safe-rm || echo rm)"
HIST="$(mktemp /tmp/runloom_hist.XXXX.json)"
HSEL="$(mktemp /tmp/runloom_hist_sel.XXXX.json)"
BAD="$(mktemp /tmp/runloom_hist_bad.XXXX.json)"
rc=0

echo "== 1a. record concurrent history -- plain recv consumers (real M:N) =="
PYTHONPATH="$ROOT/src" "$PYTHON" "$HERE/record_history.py" "$HIST" 4 3 8 2 0 || rc=1
echo "== 1b. record concurrent history -- select() consumers (real M:N) =="
# All 3 consumers receive via select() over [ch, never-ready idle chan]: drives
# chan.c's multi-waiter Phase-2 install/abort/cleanup, recorded as recv events.
PYTHONPATH="$ROOT/src" "$PYTHON" "$HERE/record_history.py" "$HSEL" 4 3 8 2 3 || rc=1

echo "== 2. Porcupine check both histories (expect LINEARIZABLE) =="
if [ ! -x "$HERE/porcupine/lincheck" ]; then
    ( cd "$HERE/porcupine" && go build -o lincheck . ) || { echo "go build failed"; exit 2; }
fi
echo "  -- plain --";  "$HERE/porcupine/lincheck" "$HIST" || rc=1
echo "  -- select --"; "$HERE/porcupine/lincheck" "$HSEL" || rc=1

echo "== 3. teeth: phantom delivery (expect NOT LINEARIZABLE) =="
"$PYTHON" - "$HIST" "$BAD" <<'PY'
import json, sys
h = json.load(open(sys.argv[1]))
for e in h["events"]:
    if e["op"] == "recv" and e["result"] == "ok":
        e["value"] = 999999          # never sent
        break
json.dump(h, open(sys.argv[2], "w"))
PY
if "$HERE/porcupine/lincheck" "$BAD"; then
    echo "  >>> FAIL: checker accepted a corrupted history (no teeth)"; rc=1
else
    echo "  >>> OK: corrupted history correctly rejected"
fi

echo "== 4. stateful Hypothesis model of the channel API =="
PYTHONPATH="$ROOT/src" "$PYTHON" -m pytest "$HERE/stateful_chan.py" -q -p no:cacheprovider || rc=1

$RM -f "$HIST" "$HSEL" "$BAD"
echo "== linearizability pipeline rc=$rc =="
exit $rc
