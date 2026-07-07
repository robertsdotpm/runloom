#!/usr/bin/env bash
# run.sh -- bounded Atheris (coverage-guided) run of the runloom C-API fuzzer.
# Safe: scheduler-free surface, one transient object/iter, bounded by -max_total_time.
#   tools/fuzz/atheris/run.sh [seconds]   (default 30)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
PY="${RUNLOOM_PYTHON:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
SECS="${1:-30}"
CORPUS="$HERE/corpus"; mkdir -p "$CORPUS"

if ! PYTHON_GIL=0 "$PY" -c "import atheris" >/dev/null 2>&1; then
    echo "atheris not importable in $PY -- pip install atheris (SKIP)"; exit 0
fi
echo "atheris fuzz: ${SECS}s (scheduler-free API surface; a crash = a finding)"
# -rss_limit_mb guards against a runaway alloc (the harness bounds sizes, but be safe).
PYTHON_GIL=0 PYTHONPATH="$ROOT/src" "$PY" "$HERE/fuzz_api.py" \
    -max_total_time="$SECS" -rss_limit_mb=3072 -close_fd_mask=3 -artifact_prefix="$HERE/" "$CORPUS"
rc=$?
if [ $rc -eq 0 ]; then
    echo "atheris: clean (no crash) -- API validation held over the run"
else
    echo "atheris: CRASH (rc=$rc) -- reproducing input written above; investigate"
fi
exit $rc
