#!/usr/bin/env bash
# Build the test extension against free-threaded CPython 3.13t.
# Uses -O2 -fno-stack-protector so the stack-allocation encoding is the
# clean `sub rsp, imm32` we target (the stack protector would add a canary
# but does not change the sub-rsp encoding; we disable it to keep frames
# tidy and predictable for the prototype).
set -euo pipefail

PY="${PY:-$HOME/.pyenv/versions/3.14.4t/bin/python3}"
HERE="$(cd "$(dirname "$0")" && pwd)"

INCDIR="$("$PY" -c 'import sysconfig; print(sysconfig.get_path("include"))')"
EXTSUFFIX="$("$PY" -c 'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')"

OUT="$HERE/stacktest${EXTSUFFIX}"

echo "[build] PY        = $PY"
echo "[build] INCDIR    = $INCDIR"
echo "[build] EXTSUFFIX = $EXTSUFFIX"
echo "[build] OUT       = $OUT"

# -fno-stack-protector       : no canary (keeps frames tidy; encoding unchanged)
# -fno-stack-clash-protection: allocate frames in ONE `sub rsp,imm32`, not a
#                              probing loop -> clean single-shot target.
gcc -O2 -fno-stack-protector -fno-stack-clash-protection -fPIC -shared \
    -I"$INCDIR" \
    "$HERE/stacktest.c" \
    -o "$OUT"

echo "[build] built $OUT"
ls -l "$OUT"
