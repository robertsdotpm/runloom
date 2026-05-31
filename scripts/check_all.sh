#!/usr/bin/env bash
# check_all.sh -- run every layer of pygo's correctness stack.
#
# Layers, fastest first:
#   tests       Python test suite (pytest tests/)               ~seconds
#   mn          M:N scheduler fuzzer (tools/mn_stress.py)        ~seconds-min
#   ctest       C deque concurrency stress (tests_c/test_cldeque) ~seconds
#   sanitizers  same harness under ASan/TSan/UBSan               ~seconds-min
#   verify      formal proofs: Spin models + CBMC on real C      ~3-4 min
#
# Usage:
#   scripts/check_all.sh                 # tests + mn + ctest  (the fast set)
#   scripts/check_all.sh all             # everything incl. sanitizers + verify
#   scripts/check_all.sh verify          # just the formal proofs
#   scripts/check_all.sh tests ctest     # pick phases
#
# Env:
#   PYTHON=...   interpreter for the Python suite + fuzzer
#                (default: a free-threaded 3.13t if found, else python3)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Prefer a free-threaded build -- that's where the M:N scheduler is real.
if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.13.13t/bin/python3" python3.13t python3; do
        if command -v "$cand" >/dev/null 2>&1; then PYTHON="$cand"; break; fi
    done
fi

phases=("$@")
[ ${#phases[@]} -eq 0 ] && phases=(tests mn ctest)
if [ "${phases[0]}" = all ]; then phases=(tests mn ctest sanitizers verify); fi

rc=0
hr() { printf '\n========== %s ==========\n' "$1"; }

for ph in "${phases[@]}"; do
  case "$ph" in
    tests)
      hr "Python test suite"
      "$PYTHON" -m pytest tests/ -q -p no:cacheprovider || rc=1
      ;;
    mn)
      hr "M:N scheduler fuzzer (stable gate)"
      # --stable: known-good patterns, so this is a clean regression gate.
      # For full fuzzing (which reproduces the contended-select crash,
      # finding A in tools/README.md) run: tools/mn_stress.py --iters N
      "$PYTHON" tools/mn_stress.py --iters "${MN_ITERS:-150}" --stable || rc=1
      ;;
    ctest)
      hr "C deque concurrency stress"
      make -C tests_c test_cldeque >/dev/null && \
        tests_c/test_cldeque "${CLDEQUE_PUSHES:-100000}" 4 4 || rc=1
      ;;
    sanitizers)
      hr "C sanitizer harnesses (ASan/TSan/UBSan)"
      tools/run_sanitizers.sh || rc=1
      ;;
    verify)
      hr "Formal verification (Spin + CBMC)"
      verify/run_verify.sh || rc=1
      ;;
    *)
      echo "unknown phase: $ph (want: tests mn ctest sanitizers verify all)"; rc=2 ;;
  esac
done

hr "summary"
if [ "$rc" -eq 0 ]; then echo "ALL GREEN"; else echo "FAILURES (rc=$rc)"; fi
exit "$rc"
