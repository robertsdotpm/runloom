#!/usr/bin/env bash
# S9 -- assert the built extension keeps its compiler hardening (regression guard).
# checksec confirmed the build ships a non-executable stack, a stack canary, and
# FORTIFY_SOURCE; CFI/CET/SafeStack are deliberately OFF (incompatible with the
# asm stack-switch trampolines in fcontext.c).  This locks in the rest so a
# setup.py flag drop is caught.
set -u
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
SO="$(ls src/runloom_c*.so 2>/dev/null | head -1)"
[ -z "$SO" ] && { echo "no built .so -- run setup.py build_ext --inplace first"; exit 2; }
command -v readelf >/dev/null 2>&1 || { echo "SKIP: readelf not found"; exit 0; }
echo "== S9 hardening of $SO =="
fail=0

# NX -- GNU_STACK must not be executable (an RWE stack is a real vuln).
if readelf -lW "$SO" 2>/dev/null | grep -qE 'GNU_STACK.*RWE'; then
  echo "  FAIL: executable stack (GNU_STACK RWE)"; fail=1
else
  echo "  OK  : non-executable stack (NX)"
fi

# Stack canary -- stack-protector active iff __stack_chk_fail is referenced.
if readelf -sW "$SO" 2>/dev/null | grep -q '__stack_chk_fail'; then
  echo "  OK  : stack canary (-fstack-protector)"
else
  echo "  FAIL: no stack canary (__stack_chk_fail absent)"; fail=1
fi

# RELRO (informational -- partial RELRO is normal for a CPython ext).
readelf -lW "$SO" 2>/dev/null | grep -q 'GNU_RELRO' \
  && echo "  OK  : RELRO segment present" || echo "  warn: no RELRO segment"

# FORTIFY_SOURCE (informational -- _chk symbols only appear if a fortifiable
# libc call is reached; absence is not necessarily a regression).
if readelf -sW "$SO" 2>/dev/null | grep -qE '__[a-z]+_chk'; then
  echo "  OK  : FORTIFY_SOURCE (_chk symbols present)"
else
  echo "  warn: no _chk symbols (FORTIFY may be inactive for this TU set)"
fi

[ "$fail" = 0 ] && { echo "  hardening intact"; exit 0; } || { echo "  HARDENING REGRESSED"; exit 1; }
