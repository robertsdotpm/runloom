#!/usr/bin/env bash
# check_all.sh -- run every layer of runloom's correctness stack.
#
# Layers, fastest first:
#   static      security SAST on the C core (parallel across cores):     ~15s
#               gates  = gcc -fanalyzer+taint, seclint (banned unbounded fns)
#               advise = clang analyzer/ArrayBound, clang-tidy cert-*, cppcheck
#   tests       Python test suite (pytest tests/)               ~seconds
#   mn          M:N scheduler fuzzer (tools/mn_stress.py)        ~seconds-min
#   replay      controlled-M:N deterministic replay probes       ~seconds-min
#   lincheck    linearizability (Porcupine + stateful select)   ~seconds
#   dst         deterministic simulation seed sweep             ~seconds
#   ctest       C deque concurrency stress (tests/tests_c/test_cldeque) ~seconds
#   sanitizers  C deque harness under ASan/TSan/UBSan            ~seconds-min
#   exttsan     WHOLE ext under ThreadSanitizer (real runtime)  ~30s-min
#   verify      formal proofs: Spin models + CBMC on real C      ~3-4 min
#   ftconform   REAL CPython stop-the-world (M2) conformed to the TLA+ model under
#               TLC, via the instrumented --with-pydebug interp (skips cleanly if
#               that oracle isn't present); in `all`, not in fast    ~seconds
#   bench       rigorous microbench sweep (informational)        ~1-3 min
#   combo       pairwise config-matrix interaction sweep          ~1-2 min
#
# Usage:
#   scripts/check_all.sh                 # tests + mn + lincheck + dst + ctest
#   scripts/check_all.sh all             # everything incl. sanitizers + verify
#   scripts/check_all.sh verify          # just the formal proofs (parallel)
#   scripts/check_all.sh verify-fast     # proofs minus the 3 slow CBMC monsters
#   scripts/check_all.sh tests ctest     # pick phases
#   scripts/check_all.sh bench           # perf only (NOT in `all` -- machine-dependent)
#   scripts/check_all.sh combo           # config-matrix sweep (candidate for `all`)
#
# Two convenience wrappers wrap the common tiers (see scripts/check_all_fast,
# scripts/check_all_extensive):
#   check_all_fast       = tests mn replay lincheck dst ctest verify-fast ftconform
#                          (the routine PRE-MERGE gate -- full Spin + cheap CBMC,
#                           skips the 3 slow proofs; ~minutes)
#   check_all_extensive  = all  (every proof + sanitizers; run before a risky
#                          merge / periodically; the verify phase is now parallel)
# The ftconform phase is in BOTH lanes but SKIPS CLEANLY where the instrumented
# --with-pydebug oracle isn't set up (so it only actually runs on a dev box that
# has one -- NOT on a hosted CI runner); it adds ~a few seconds there.  It is ordered AFTER
# verify(-fast) so the TLA+ jar that phase fetches is already present.
#
# The verify / verify-fast phases run their checks through a parallel worker pool
# (VERIFY_JOBS, default nproc).  See tools/verify/run_verify.sh.
#
# Env:
#   PYTHON=...   interpreter for the Python suite + fuzzer
#                (default: a free-threaded 3.13t if found, else python3)
#   VERIFY_JOBS=N  formal-verification worker pool size (default: nproc; 1=serial)
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Skip pytest's third-party plugin autoload for every phase that shells into
# pytest.  ~20 unrelated plugins are installed in this env and pytest imports
# all of them per process (~4s/file of pure overhead), and one pulls _brotli
# which re-enables the GIL -- wrong for the free-threaded target.  The suite
# uses none of them.  Opt back in with RUNLOOM_TEST_PYTEST_PLUGINS=1.
if [ "${RUNLOOM_TEST_PYTEST_PLUGINS:-}" != "1" ]; then
    export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
fi

# Prefer a free-threaded build -- that's where the M:N scheduler is real.
if [ -z "${PYTHON:-}" ]; then
    for cand in "$HOME/.pyenv/versions/3.14.4t/bin/python3" python3.13t python3; do
        if command -v "$cand" >/dev/null 2>&1; then PYTHON="$cand"; break; fi
    done
fi
# One clear message rather than N opaque "command not found"s per phase.
if [ -z "${PYTHON:-}" ]; then
    echo "check_all: no python interpreter found -- set PYTHON=/path/to/python3 (want a free-threaded 3.13t)"; exit 2
fi

phases=("$@")
[ ${#phases[@]} -eq 0 ] && phases=(tests mn replay lincheck dst ctest)
if [ "${phases[0]}" = all ]; then
  phases=(tests mn replay lincheck dst ctest static sanitizers exttsan verify ctxcheck dbgnetpoll migdelay chess ftconform aioconform mr combo security supplychain refleak racerd)
fi

# ---- preflight: is the C extension built for THIS interpreter? -------------
#
# check_all.sh does NOT build anything.  If src/ holds no runloom_c matching
# $PYTHON's ABI tag, `import runloom_c` does not fail -- it resolves to the
# SOURCE DIRECTORY src/runloom_c/ as a namespace package, giving an empty
# module with __file__ = None.  What you get then is not a clean "not built"
# error but 200+ files failing on
#
#     AttributeError: module 'runloom_c' has no attribute '_fiber_register'
#
# and, worse, lanes that tolerate import errors reporting PASS while testing
# NOTHING.  Observed 2026-08-21: a full check_all_fast came back with 216
# "failures" and 24 bogus MR3 violations, all of which evaporated once the
# extension was built for the right interpreter -- the MR3 probes had been
# returning INVARIANT because the module was a stub, which reads as a stale
# model rather than a missing build.  Several hours went into the wrong
# question.  Fail loudly and say exactly what to run instead.
#
# Only enforced for phases that actually import the extension: the formal
# (verify/verify-fast), supply-chain and security lanes are pure source/tool
# analysis and are legitimately runnable on an unbuilt tree.  The sanitizer
# lanes build their own instrumented copy, so they are exempt too.
needs_ext=0
for _ph in "${phases[@]}"; do
  case "$_ph" in
    tests|mn|replay|lincheck|dst|ftconform|aioconform|aioconform-fast|mr|chess|\
ctxcheck|dbgnetpoll|migdelay|combo|refleak|racerd)
      needs_ext=1 ;;
  esac
done
if [ "$needs_ext" = 1 ] && [ "${RUNLOOM_ALLOW_UNBUILT:-}" != "1" ]; then
  _ext_probe="$(PYTHONPATH=src "$PYTHON" - <<'PY' 2>&1
import sys, _imp
try:
    import runloom_c
except Exception as exc:
    print("IMPORTFAIL|%s: %s" % (type(exc).__name__, exc)); raise SystemExit(0)
path = getattr(runloom_c, "__file__", None)
if not path or not any(path.endswith(s) for s in _imp.extension_suffixes()):
    # A namespace package over src/runloom_c/, not the compiled module.
    print("NOTBUILT|%s" % (path,)); raise SystemExit(0)
if not hasattr(runloom_c, "_fiber_register"):
    print("INCOMPLETE|%s" % (path,)); raise SystemExit(0)
print("OK|%s" % (path,))
PY
)"
  case "$_ext_probe" in
    OK\|*) : ;;
    *)
      _want="$(PYTHONPATH=src "$PYTHON" -c 'import _imp; print(_imp.extension_suffixes()[0])' 2>/dev/null)"
      _have="$(ls -1 src/runloom_c*.so 2>/dev/null | tr '\n' ' ')"
      echo "check_all: the runloom_c extension is not usable by this interpreter." >&2
      echo "" >&2
      echo "  interpreter : $PYTHON" >&2
      echo "  version     : $("$PYTHON" -VV 2>&1 | head -1)" >&2
      echo "  needs       : src/runloom_c${_want:-<unknown ABI suffix>}" >&2
      echo "  has         : ${_have:-<no built extension in src/>}" >&2
      echo "  probe       : ${_ext_probe}" >&2
      echo "" >&2
      echo "check_all.sh does not build. Without a matching extension," >&2
      echo "'import runloom_c' silently becomes a namespace package over the" >&2
      echo "SOURCE directory, and the suite reports mass AttributeErrors or" >&2
      echo "-- worse -- passes while testing nothing." >&2
      echo "" >&2
      echo "  build it:  $PYTHON setup.py build_ext --inplace" >&2
      echo "" >&2
      echo "Formal/supply-chain lanes need no build and can be run directly:" >&2
      echo "  scripts/check_all.sh verify-fast        (Spin/CBMC/TLA+)" >&2
      echo "  scripts/check_all.sh supplychain-fast" >&2
      echo "Override (you know the phase does not need it): RUNLOOM_ALLOW_UNBUILT=1" >&2
      exit 2
      ;;
  esac
fi

# ---- developer log dir -----------------------------------------------------
#
# Every phase's output goes to a per-run directory as well as the terminal.
# The tool-specific lanes already scatter logs across /tmp (runloom_verify.*,
# runloom_tsan.*, runloom_tlc.*, /tmp/runloom_<lint>.log) which is fine while
# you are staring at a live run and useless a day later -- you cannot tell
# which /tmp dir belonged to which invocation, and a reboot takes them all.
# So: one timestamped dir per run, `latest` pointing at the newest, the full
# transcript, a per-phase split, and a summary that records what actually ran.
#
# RUNLOOM_LOG_DIR moves it; RUNLOOM_NO_LOG=1 turns it off.
RUNDIR=""
if [ "${RUNLOOM_NO_LOG:-}" != "1" ]; then
    LOGROOT="${RUNLOOM_LOG_DIR:-$ROOT/.check-logs}"
    RUNDIR="$LOGROOT/$(date -u +%Y%m%dT%H%M%SZ)"
    if mkdir -p "$RUNDIR" 2>/dev/null; then
        ln -sfn "$RUNDIR" "$LOGROOT/latest" 2>/dev/null || true
        RUN_START=$(date +%s)
        # tee the whole transcript. A brace group, not a pipe: `rc` is assigned
        # by the phases below and a pipeline would run them in a subshell and
        # silently lose every failure.
        exec > >(tee "$RUNDIR/full.log") 2>&1
        echo "log dir: $RUNDIR  (also $LOGROOT/latest)"
    else
        RUNDIR=""
    fi
fi

# ---- preflight: which OPTIONAL tools are missing? --------------------------
#
# The verify lanes drive a dozen external engines and each one SKIPS CLEANLY
# when its tool is absent -- which is the right behaviour (the gate has to be
# runnable on a laptop) but means a run can report "132 passed" while quietly
# checking far less than it looks like.  Today's counts are only meaningful
# next to what did not run at all, and those skip lines are scattered hundreds
# of lines apart in the output.
#
# So: say it once, up front, in one place.  Missing tools ONLY -- listing the
# dozen that are present is the noise this is trying to remove.  Warning only;
# never changes rc.  RUNLOOM_NO_TOOLCHECK=1 silences it.
if [ "${RUNLOOM_NO_TOOLCHECK:-}" != "1" ]; then
  _tool_rows=""
  _tool_missing=0
  # Probe the SAME PATH the lanes do, or this lies. tools/supplychain/scan.sh
  # prepends ~/.local/bin and Go's bin before looking for semgrep/gitleaks, so
  # a bare `command -v` reported them missing while the lane ran them happily
  # -- a false "missing" being precisely the noise this block exists to remove.
  _saved_path="$PATH"
  export PATH="$HOME/.local/bin:$(go env GOPATH 2>/dev/null)/bin:$PATH"
  # name|probe-command|what is skipped without it|how to get it
  while IFS='|' read -r _t _probe _loses _hint; do
    [ -z "$_t" ] && continue
    if ! eval "$_probe" >/dev/null 2>&1; then
      _tool_rows="$_tool_rows$(printf '  %-12s %-34s %s' "$_t" "$_loses" "$_hint")
"
      _tool_missing=$((_tool_missing + 1))
    fi
  done <<'TOOLS'
cbmc|command -v cbmc|CBMC bounded proofs|apt-get install cbmc
genmc|command -v genmc|GenMC RC11 deque oracle|set GENMC=/path/to/genmc
spin|command -v spin|Promela/Spin models|apt-get install spin
java|command -v java|TLA+ (TLC) and Alloy|apt-get install default-jre
coqc|command -v coqc|Coq + Iris machine-checked proofs|opam install -y coq coq-iris-heap-lang
herd7|command -v herd7|herd litmus / fence sweeps|opam install herdtools7
setarch|command -v setarch|TSan aborts under ASLR without it|apt-get install util-linux
libclang|python3 -c "import clang.cindex"|tstate_manifest lint (PyThreadState fields)|pip install clang
semgrep|command -v semgrep|supply-chain backdoor-pattern scan|pipx install semgrep
gitleaks|command -v gitleaks|supply-chain secret scan|see tools/security/README
cppcheck|command -v cppcheck|static analysis lane|apt-get install cppcheck
TOOLS

  export PATH="$_saved_path"

  # The gold TSan interpreter is an env pointer, not a PATH lookup. Distinguish
  # "never built" from "built but not exported" -- they need different actions,
  # and telling someone to spend 20 minutes rebuilding what is already on disk
  # is how a warning earns itself a filter rule.
  if [ -z "${RUNLOOM_TSAN_PYTHON:-}" ] || [ ! -x "${RUNLOOM_TSAN_PYTHON:-/nonexistent}" ]; then
    _tsan_built="$(ls "$HOME"/cpython-tsan/bin/python3.*t 2>/dev/null | tail -1)"
    if [ -n "$_tsan_built" ]; then
      _tsan_hint="built already -- export RUNLOOM_TSAN_PYTHON=$_tsan_built"
    else
      _tsan_hint="tools/build_tsan_cpython.sh  (PY_VER= the version you SHIP)"
    fi
    _tool_rows="$_tool_rows$(printf '  %-12s %-34s %s' "tsan-python" \
      "gold TSan (cross-boundary races)" "$_tsan_hint")
"
    _tool_missing=$((_tool_missing + 1))
  fi

  if [ "$_tool_missing" -gt 0 ]; then
    printf '\n========== optional tools MISSING (%d) -- these phases will SKIP ==========\n' "$_tool_missing"
    printf '%s' "$_tool_rows"
    echo "  (skips are counted per-lane as 'N skipped'; a green run with tools"
    echo "   absent verifies LESS than the same run with them present.)"
    echo "  Silence with RUNLOOM_NO_TOOLCHECK=1."
  fi
fi

rc=0
hr() { printf '\n========== %s ==========\n' "$1"; }

for ph in "${phases[@]}"; do
  case "$ph" in
    tests)
      hr "Python test suite (per-file subprocesses)"
      # Use run_isolated.py (one file per subprocess), NOT in-process
      # `pytest tests/`: the latter accumulates cross-file state leaks -- a
      # prior file can leave an M:N runtime / threads wedged on a lock -- and
      # deadlocks the whole run (observed: an 11-hour hang on this phase).
      # run_isolated starts each file clean, so a real hang is one file's.
      PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tests/run_isolated.py || rc=1
      ;;
    mn)
      hr "M:N scheduler fuzzer (stable gate)"
      # --stable: known-good patterns, so this is a clean regression gate.
      # For full fuzzing (which reproduces the contended-select crash,
      # finding A in tools/README.md) run: tools/mn_stress.py --iters N
      "$PYTHON" tools/mn_stress.py --iters "${MN_ITERS:-150}" --stable || rc=1
      ;;
    replay)
      hr "Controlled M:N deterministic replay (RUNLOOM_MN_BARRIER)"
      # Same seed must reproduce one signature across reps; each probe exits
      # non-zero if any seed varies.  Guards the five replay levers
      # (tools/mn_controlled/README.md) against silent regression.
      "$PYTHON" tools/mn_controlled/repro_probe.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      "$PYTHON" tools/mn_controlled/repro_select.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      "$PYTHON" tools/mn_controlled/repro_timer.py "${REPLAY_SEEDS:-8}" "${REPLAY_REPS:-6}" || rc=1
      ;;
    static)
      hr "Static + security analysis (gcc -fanalyzer+taint & seclint gates; clang/cert/cppcheck advisory)"
      PYTHON="$PYTHON" bash tools/static_analysis.sh || rc=1
      hr "Wake-protocol lint (every wake_state transition is NOTE-witnessed)"
      bash scripts/check_wake_protocol.sh || rc=1
      ;;
    lincheck)
      hr "Linearizability (Porcupine + stateful select model)"
      PYTHON="$PYTHON" bash tools/lincheck/check_lin.sh || rc=1
      ;;
    dst)
      hr "Deterministic simulation (seed sweep)"
      PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tools/dst/dst.py sweep "${DST_SEEDS:-200}" || rc=1
      ;;
    exttsan)
      hr "Whole-ext ThreadSanitizer (real runtime under TSan)"
      PYTHON="$PYTHON" tools/run_sanitizers_ext.sh "${MN_ITERS:-150}" || rc=1
      ;;
    ctest)
      hr "C deque concurrency stress"
      make -C tests/tests_c test_cldeque >/dev/null && \
        tests/tests_c/test_cldeque "${CLDEQUE_PUSHES:-100000}" 4 4 || rc=1
      ;;
    patches)
      # The CPython patches are the one artefact whose failure is SILENT: a hunk
      # that stops matching can be fuzzed back in with -F3 and yield a working
      # build of a subtly wrong interpreter (see src/patches/README.md).  This
      # proves all of them still apply at ZERO fuzz, in seconds, without
      # building anything.
      #
      # Needs the pinned CPython tarballs.  They are cached under
      # $RL_CI_WORK (default ~/.cache/runloom-ci), so this is offline after the
      # first run -- and SKIPS rather than fails when the cache is cold and the
      # network is unavailable, so the local gate stays runnable on a plane.
      hr "CPython patch integrity (zero-fuzz apply)"
      if [ ! -x tools/ci/check_patches.sh ]; then
        echo "  tools/ci/check_patches.sh missing -- SKIPPED"
      elif [ ! -d "${RL_CI_WORK:-$HOME/.cache/runloom-ci}" ] && \
           ! curl -fsS --max-time 5 -o /dev/null https://www.python.org 2>/dev/null; then  # download-pin-lint: allow -- reachability probe, -o /dev/null, fetches nothing
        echo "  no cached CPython sources and no network -- SKIPPED"
      else
        tools/ci/check_patches.sh || rc=1
      fi
      ;;
    sanitizers)
      hr "C sanitizer harnesses (ASan/TSan/UBSan)"
      tools/run_sanitizers.sh || rc=1
      ;;
    verify)
      hr "Formal verification (Spin + CBMC, parallel)"
      PYTHON="$PYTHON" tools/verify/run_verify.sh || rc=1
      ;;
    verify-fast)
      hr "Formal verification -- fast lane (all Spin + cheap CBMC; skips 3 slow proofs)"
      VERIFY_FAST=1 PYTHON="$PYTHON" tools/verify/run_verify.sh || rc=1
      ;;
    ctxcheck)
      hr "Lock-order + park/yield-safety checker (RUNLOOM_CTXCHECK build slice)"
      # Activates the previously-never-built rank checker + the item-10 park
      # assert; fails on any lock-order inversion or yield-while-lock-held.
      PYTHON="$PYTHON" bash scripts/check_ctxcheck.sh || rc=1
      ;;
    chess)
      hr "CHESS/PCT coverage-theorem gate (systematic interleaving search)"
      PYTHON="$PYTHON" bash scripts/check_chess.sh || rc=1
      ;;
    migdelay)
      hr "Migration-window perturbation (RUNLOOM_DELAY on snap/load/adopt)"
      PYTHON="$PYTHON" bash scripts/check_migration_delay.sh || rc=1
      ;;
    dbgnetpoll)
      hr "Stale-arm tripwire across the broad suite (RUNLOOM_DBG_NETPOLL=1)"
      # Runs netpoll/mn/aio under the inline arm-cache-vs-kernel check so
      # stale-cache drift surfaces anywhere, not just in the fd-reuse tests.
      PYTHON="$PYTHON" bash scripts/check_dbg_netpoll.sh || rc=1
      ;;
    ftconform)
      hr "STW (M2) trace conformance -- real CPython stop-the-world vs the model"
      tools/stw_conform_ci.sh || rc=1
      ;;
    aioconform)
      hr "Vendored CPython asyncio suite on the runloom bridge (tests/aio/, full)"
      # tests/aio/ = pinned CPython test_asyncio bodies run on RunloomEventLoop,
      # green on the DEFAULT bridge (divergences skipped in tests/aio/skips.py).
      # Per-file subprocess isolation (run_isolated --suite aio); NOT in the
      # default `tests` phase (discover() scans tests/ top-level only), so it runs
      # only here (extensive) + the lean aioconform-fast slice.
      PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tests/run_isolated.py --suite aio || rc=1
      ;;
    aioconform-fast)
      hr "Vendored asyncio bridge conformance -- lean slice (fast gate)"
      PYTHON_GIL=0 PYTHONPATH=src "$PYTHON" tests/run_isolated.py --suite aio \
          test_sock_lowlevel.py test_server.py test_buffered_proto.py test_locks.py || rc=1
      ;;
    bench)
      hr "Rigorous microbench sweep (informational -- bootstrap CIs)"
      PYTHON="$PYTHON" bash tools/bench/bench.sh || rc=1
      ;;
    combo)
      hr "Combinatorial config-matrix sweep (pairwise interactions)"
      PYTHON_GIL=0 "$PYTHON" tools/combinatorial/covering.py --iters "${COMBO_ITERS:-40}" || rc=1
      ;;
    mr)
      hr "Metamorphic MR3 hub-count invariance (conservation corpus)"
      # The SAME program at different --hubs must all PASS and agree on its
      # conservation metric -- catches scheduler-shape-dependent bugs a fixed
      # oracle / differential / lincheck structurally cannot.  Bounded via MR_*.
      # SKIPS CLEANLY if the corpus isn't present (set -u safe via the -e test).
      mr_progs=(tests/big_100/*conservation*.py)
      if [ ! -e "${mr_progs[0]}" ]; then
        echo "  SKIP: no conservation corpus (tests/big_100/*conservation*.py)"
      else
        for mp in "${mr_progs[@]}"; do
          RUNLOOM_PYTHON="$PYTHON" "$PYTHON" tools/metamorphic/mr_runner.py "$mp" \
            --hubs "${MR_HUBS:-2,8}" --seed "${MR_SEED:-7}" \
            --funcs "${MR_FUNCS:-150}" --rounds "${MR_ROUNDS:-1}" \
            --duration "${MR_DURATION:-3}" || rc=1
        done
      fi
      ;;
    security)
      hr "Security -- deterministic subset S1-S4 (fuzzers S6-S9 -> daemon; cc/valgrind skip cleanly)"
      RUNLOOM_SEC_FAST=1 PYTHON="$PYTHON" tools/security/run_all.sh || rc=1
      ;;
    supplychain)
      hr "Supply-chain / backdoor scan -- semgrep+gitleaks+bandit + osv-scanner (deps)"
      # Scan the tree for a planted backdoor / secret / vulnerable dep.  Each tool
      # SELF-SKIPS if absent; DEPS=1 adds the network osv-scanner dep audit.
      RUNLOOM_SC_DEPS=1 RUNLOOM_PYTHON="$PYTHON" bash tools/supplychain/scan.sh || rc=1
      ;;
    supplychain-fast)
      hr "Supply-chain / backdoor scan -- OFFLINE subset (semgrep+gitleaks+bandit)"
      RUNLOOM_SC_FAST=1 RUNLOOM_PYTHON="$PYTHON" bash tools/supplychain/scan.sh || rc=1
      ;;
    refleak)
      hr "Refcount/alloc leak hunt (--with-pydebug ABI; self-skips cleanly off the dev box)"
      tools/run_refleak.sh || rc=1
      ;;
    racerd)
      hr "Infer RacerD + Pulse static race/mem-safety (advisory; self-skips if infer absent)"
      PYTHON="$PYTHON" tools/racerd.sh || rc=1
      ;;
    *)
      echo "unknown phase: $ph (want: tests mn replay lincheck dst ctest patches static sanitizers exttsan verify verify-fast ctxcheck dbgnetpoll migdelay chess ftconform aioconform aioconform-fast mr bench combo security supplychain supplychain-fast refleak racerd all)"; rc=2 ;;
  esac
done

hr "summary"
if [ "$rc" -eq 0 ]; then echo "ALL GREEN"; else echo "FAILURES (rc=$rc)"; fi

if [ -n "$RUNDIR" ]; then
    # Let the tee drain before reading full.log back.
    sleep 0.3
    # Split the transcript on the `========== NAME ==========` banners hr()
    # prints, so each phase is greppable on its own without hunting through
    # a few thousand lines.
    awk -v d="$RUNDIR" '
        /^========== .* ==========$/ {
            name = $0
            gsub(/^========== | ==========$/, "", name)
            gsub(/[^A-Za-z0-9._-]+/, "_", name)
            name = substr(name, 1, 40)
            sub(/_+$/, "", name)
            # Numbered so `ls` reads in execution order -- hr() banners carry a
            # full description, not the phase name, so alphabetical is useless.
            n++
            out = sprintf("%s/%02d_%s.log", d, n, name)
            print > out; next
        }
        out { print >> out }
    ' "$RUNDIR/full.log" 2>/dev/null
    {
        echo "runloom check_all"
        echo "  when     : $(date -u +%FT%TZ)"
        echo "  duration : $(( $(date +%s) - ${RUN_START:-$(date +%s)} ))s"
        echo "  phases   : ${phases[*]}"
        echo "  python   : $PYTHON"
        echo "  rc       : $rc"
        echo "  host     : $(uname -sr)  cores=$(nproc 2>/dev/null)"
        [ "${_tool_missing:-0}" -gt 0 ] && echo "  NOTE     : ${_tool_missing} optional tool(s) missing -- phases SKIPPED (see full.log head)"
        echo
        echo "  external logs this run may have produced:"
        echo "    /tmp/runloom_verify.*   formal verification workdir"
        echo "    /tmp/runloom_tlc.*      TLC per-check logs + heap dumps"
        echo "    /tmp/runloom_tsan.*     TSan race reports"
        echo "    /tmp/runloom_*.log      per-lint output"
    } > "$RUNDIR/summary.txt" 2>/dev/null
    echo
    echo "logs: $RUNDIR"
    echo "      summary.txt, full.log, phase_*.log"
fi
exit "$rc"
