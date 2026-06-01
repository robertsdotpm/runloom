#!/usr/bin/env bash
# rr_chaos.sh -- record/replay chaos testing of a pygo workload with rr.
#
# Mozilla rr's chaos mode (rr record --chaos) actively perturbs thread
# scheduling to provoke rare races, and gives PERFECT deterministic replay
# with reverse execution -- the fastest way to capture AND root-cause the
# residual cross-file leaked-parker flake (project_pygo_crossfile_thread_leak)
# without waiting on the deterministic-sim harness to cover the M:N path.
#
# rr needs ptrace + a low perf_event_paranoid and usually does not run inside
# a sandbox/container; this script skips cleanly when rr is unavailable.
#
# Install:  sudo apt-get install rr  &&  sudo sysctl kernel.perf_event_paranoid=1
# Capture:  tools/extra/rr_chaos.sh 200       # 200 chaos recordings
# Replay :  rr replay <dir-printed-on-failure>   # then `continue`, reverse-cont
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
N="${1:-100}"
PYBIN="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
WORKLOAD="$ROOT/tools/faultinj/workload.py"

if ! command -v rr >/dev/null 2>&1; then
    echo "[rr] rr not installed -- skipping (sudo apt-get install rr)"; exit 0
fi
if ! rr record true >/dev/null 2>&1; then
    echo "[rr] rr present but cannot record here (need ptrace / perf_event_paranoid<=1)"; exit 0
fi

echo "[rr] $N chaos recordings of the pygo workload; first crash/hang is kept for replay"
for i in $(seq 1 "$N"); do
    dir="/tmp/pygo_rr_$i"
    if ! PYTHON_GIL=0 PYTHONPATH="$ROOT/src" timeout 30 \
         rr record --chaos -o "$dir" "$PYBIN" "$WORKLOAD" >/dev/null 2>&1; then
        echo "[rr] FAILURE on recording $i -- replay with: rr replay $dir"
        echo "[rr]   then: (rr) continue ; reverse-continue ; watch -l <addr>"
        exit 1
    fi
    "$(command -v safe-rm || echo rm)" -rf "$dir" 2>/dev/null
done
echo "[rr] $N chaos recordings clean -- no crash/hang reproduced"
