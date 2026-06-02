#!/usr/bin/env bash
# scalene_profile.sh -- native-vs-Python time & memory split with Scalene.
#
# The central pygo performance question is "how much of this is the C
# extension vs the interpreter?"  Scalene answers it directly: it attributes
# time and memory line-by-line and, crucially, separates *native* (C ext /
# system) from *Python* execution -- exactly the boundary pygo straddles.
#   Berger et al, "Triangulating Python Performance Issues with Scalene",
#   OSDI 2023.
#
# Caveat: Scalene's signal/threading instrumentation may not yet support a
# free-threaded (3.13t) interpreter.  If it errors there, set PYTHON to a
# stock GIL build to profile the Python<->C split (the C hot paths are the
# same), or wait for free-threaded Scalene support.
#
# Install: pip install scalene
# Run:     tools/bench/profile/scalene_profile.sh
# Output:  profile.html (open in a browser) + a CLI summary
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-$HOME/.pyenv/versions/3.13.13t/bin/python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python3

if ! "$PY" -c "import scalene" >/dev/null 2>&1; then
    echo "[scalene] not installed -- skipping (pip install scalene)"
    exit 0
fi

OUT="${SCALENE_OUTPUT:-profile.html}"
echo "[scalene] profiling target.py (native-vs-Python split) -> $OUT"
PYTHON_GIL=0 "$PY" -m scalene --html --outfile "$OUT" --- "$HERE/target.py"
rc=$?
if [ $rc -ne 0 ]; then
    echo "[scalene] scalene exited $rc -- if this is a free-threaded-interp"
    echo "          incompatibility, retry with PYTHON=<stock cpython>."
    # Don't fail the run on a known interpreter-support gap.
    exit 0
fi
echo "[scalene] done -- open $OUT"
exit 0
