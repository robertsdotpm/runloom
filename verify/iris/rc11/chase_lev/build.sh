#!/usr/bin/env bash
# build.sh -- compile the chase_lev experiment under the runloom-weakmem gpfsl switch.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
SW="${RUNLOOM_WEAKMEM_SWITCH:-runloom-weakmem}"
eval "$(opam env --switch="$SW" --set-switch 2>/dev/null)"
export PATH="$HOME/.opam/$SW/bin:$PATH"
if command -v rocq >/dev/null 2>&1; then COMPILE() { rocq compile -q "$1"; }
else COMPILE() { coqc -q "$1"; }; fi
cd "$HERE"
F="${1:-StealClaim.v}"
COMPILE "$F"
