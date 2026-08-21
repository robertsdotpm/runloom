#!/usr/bin/env bash
# test_patched_cpython.sh -- run BOTH suites against a patched interpreter built
# by build_patched_cpython.sh, and require BOTH to be fully green:
#
#   A. CPython's own stdlib suite  (python -m test)  -- must report SUCCESS
#   B. runloom's suite             (tests/run_isolated.py) -- validation only
#
# Suite A closes a gap the patches shipped with: src/patches/README.md records
# exec-home as "VALIDATED end-to-end ... **Not** run against the CPython test
# suite".  It is now run against it, and each released version is pinned to a
# release where the stdlib suite passes clean.
#
# NO EXPECTED-FAILURE LISTS.  The contract is simple: a released, patched
# interpreter passes all the tests.  If a pinned release does not, the fix is to
# pin a release that does (or, for a genuine UPSTREAM bug unrelated to the
# patches that no release fixes, exclude that one test in versions.env with a
# written reason -- see PY314_TEST_EXCLUDE).  There is deliberately no mechanism
# for carrying runloom-caused failures forward.
#
# PHASES (each REQUIRED -- any failure fails the run):
#   cpython       CPython's own stdlib suite       (--only=cpython)
#   build-ext     build the runloom C extension + migration capability check
#                                                   (--only=build-ext)
#   runloom-tests runloom's suite, tests/run_isolated.py
#                                                   (--only=runloom-tests)
# `--only=runloom` = build-ext + runloom-tests; no --only (default) = all three.
# The workflow runs them as SEPARATE, individually-required steps; this script
# runs any subset for local use.
#
# Usage:  tools/ci/test_patched_cpython.sh <version> [--only=cpython|build-ext|runloom-tests|runloom]
# Env:    RL_CI_WORK, RL_CI_PREFIX (as build_patched_cpython.sh)
#         RL_CI_CPYTHON_TEST_ARGS  extra args for `python -m test`
#         RL_CI_TEST_TIMEOUT       per-test timeout, seconds (default 900)
#         RUNLOOM_TIMEOUT_MULT     run_isolated deadline scaler (default 1)
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=lib.sh
. "$HERE/lib.sh"
# shellcheck source=versions.env
. "$HERE/versions.env"

VERSION="${1:-}"
[ -n "$VERSION" ] || rl_die "usage: $0 <version> [--only=cpython|build-ext|runloom-tests|runloom]"
rl_validate_version "$VERSION"
shift

ONLY=all
for a in "$@"; do
    case "$a" in
        --only=cpython|--only=build-ext|--only=runloom-tests|--only=runloom|--only=all)
                   ONLY="${a#--only=}" ;;
        --only=*)  rl_die "unknown --only phase '${a#--only=}' (cpython|build-ext|runloom-tests|runloom|all)" ;;
        *)         rl_die "unknown argument: $a" ;;
    esac
done

# Which phases run for this ONLY selection.
run_cpython=no; run_buildext=no; run_runtests=no
case "$ONLY" in
    all)           run_cpython=yes; run_buildext=yes; run_runtests=yes ;;
    cpython)       run_cpython=yes ;;
    build-ext)     run_buildext=yes ;;
    runloom-tests) run_runtests=yes ;;
    runloom)       run_buildext=yes; run_runtests=yes ;;
esac

WORK="${RL_CI_WORK:-$HOME/.cache/runloom-ci}"
PLATFORM="$(rl_platform)"
TIMEOUT="${RL_CI_TEST_TIMEOUT:-900}"
JOBS="$(rl_nproc)"

PYBIN="$(cat "$WORK/pybin-$VERSION-$PLATFORM.txt" 2>/dev/null || true)"
[ -x "${PYBIN:-}" ] || rl_die "no built interpreter for $VERSION on $PLATFORM -- run tools/ci/build_patched_cpython.sh $VERSION first"

# Free-threaded means free-threaded; a stray PYTHON_GIL=1 in the environment
# would silently re-enable the GIL and make the whole run meaningless.
export PYTHON_GIL=0
rc_total=0

# ---- A. CPython stdlib suite ------------------------------------------------

SERIES="$(rl_series_of_version "$VERSION")"
if [ "$run_cpython" = yes ]; then
    rl_step "CPython $VERSION stdlib suite (this takes a while)"

    # Per-series upstream-only exclusions (PY<series>_TEST_EXCLUDE in
    # versions.env).  These are documented UPSTREAM bugs that no pinned release
    # fixes -- never a place to hide a runloom-caused failure.
    EXCLUDE="$(rl_exclude_of_version "$VERSION")"
    EXCLUDE_ARGS=""
    for t in $EXCLUDE; do EXCLUDE_ARGS="$EXCLUDE_ARGS -x $t"; done
    [ -n "$EXCLUDE_ARGS" ] && rl_log "excluding (upstream-only, see versions.env):$EXCLUDE"

    # shellcheck disable=SC2086
    "$PYBIN" -m test \
        -j"$JOBS" \
        --timeout="$TIMEOUT" \
        $EXCLUDE_ARGS \
        ${RL_CI_CPYTHON_TEST_ARGS:-} \
        > "$WORK/cpython-tests-$VERSION.log" 2>&1
    regrtest_rc=$?

    # regrtest exits 0 iff every non-skipped test passed.  That IS the gate:
    # a released patched interpreter must pass the stdlib suite clean.
    okcount="$(grep -oE '[0-9]+ tests OK' "$WORK/cpython-tests-$VERSION.log" | tail -1)"
    if [ "$regrtest_rc" -eq 0 ]; then
        rl_log "CPython stdlib suite: SUCCESS ($okcount)"
        rl_ci_summary "✅ **CPython $VERSION stdlib** ($PLATFORM): SUCCESS — $okcount${EXCLUDE:+ · excluded: $EXCLUDE}"
    else
        rl_warn "CPython stdlib suite FAILED (regrtest exit=$regrtest_rc)"
        _failed="$(grep -E '^    test_' "$WORK/cpython-tests-$VERSION.log" | tr -s ' ' | paste -sd' ' -)"
        grep -E "tests failed:|^    test_|Result: FAILURE" "$WORK/cpython-tests-$VERSION.log" | tail -30 >&2
        rl_warn "full log: $WORK/cpython-tests-$VERSION.log"
        rl_warn "If this is an UPSTREAM bug no release fixes, exclude it in versions.env (PY${SERIES}_TEST_EXCLUDE) with a reason. Otherwise pin a release that passes."
        rl_ci_summary "❌ **CPython $VERSION stdlib** ($PLATFORM): FAILED (regrtest exit=$regrtest_rc) —$_failed"
        rc_total=1
    fi
fi

# ---- B1. build the runloom C extension + migration capability ---------------

if [ "$run_buildext" = yes ]; then
    rl_step "build runloom C extension against $VERSION"
    # pytest is REQUIRED -- without it run_isolated.py reports every one of the
    # ~240 files as failed, which reads like a catastrophic regression rather
    # than a missing dependency.  Installed here so the runloom-tests phase (a
    # separate step against the same interpreter) inherits it.
    "$PYBIN" -m pip install -q pytest \
        || rl_die "could not install pytest into the patched interpreter"
    # hypothesis is best-effort and MUST be installed separately: below 3.14 its
    # Rust/PyO3 core refuses to build against a free-threaded interpreter, and a
    # combined `pip install pytest hypothesis` fails the whole transaction --
    # taking pytest down with it.  Exactly one test file uses it.
    if "$PYBIN" -m pip install -q hypothesis 2>/dev/null; then
        rl_log "hypothesis installed"
    else
        rl_warn "hypothesis unavailable on this interpreter (PyO3 has no free-threaded support below 3.14) -- the one dependent test is deselected"
    fi
    ( cd "$ROOT" && "$PYBIN" setup.py build_ext --inplace ) > "$WORK/runloom-build-$VERSION.log" 2>&1 \
        || { tail -40 "$WORK/runloom-build-$VERSION.log" >&2; rl_die "runloom failed to build against the patched interpreter"; }
    rl_log "runloom C extension built"

    # The end-to-end proof that the patches reached an EXTENSION MODULE, not just
    # CPython's own TUs.  If pyconfig.h had not been armed, these read 0 while
    # the interpreter itself still worked -- exactly the silent-mismatch case.
    rl_step "verify migration capability bits"
    if ( cd "$ROOT" && PYTHONPATH=src "$PYBIN" - <<'PYEOF'
import sys, runloom_c
# src/runloom_c/ is the C SOURCE directory, so if the extension failed to build,
# `import runloom_c` silently succeeds as an implicit namespace package with no
# attributes -- which surfaces later as a baffling AttributeError deep in
# runtime.py rather than "the extension is missing".  Catch it here.
if getattr(runloom_c, "__file__", None) is None:
    sys.exit("FAIL: 'runloom_c' resolved to the src/runloom_c/ SOURCE directory as a "
             "namespace package -- the extension module was not built")
import runloom
status = runloom.migration_status()
print("migration_status():", status)
print("alloc_home_available:", runloom_c.alloc_home_available)
print("exec_home_available: ", runloom_c.exec_home_available)
missing = [k for k in ("alloc_home", "exec_home") if not status.get(k)]
if missing:
    sys.exit("FAIL: patched build does not advertise: %s -- the patch did not "
             "reach the extension module (check pyconfig.h)" % ", ".join(missing))
if not runloom.migration_available():
    sys.exit("FAIL: migration_available() is False on a fully patched build")
print("OK: both halves present, migration_available() is True")
PYEOF
    ); then
        rl_ci_summary "✅ **runloom extension** ($VERSION, $PLATFORM): built + migration_available()"
    else
        rl_warn "capability check FAILED"
        rl_ci_summary "❌ **runloom extension** ($VERSION, $PLATFORM): build/capability FAILED"
        rc_total=1
    fi
fi

# ---- B2. runloom's own test suite (REQUIRED) --------------------------------

if [ "$run_runtests" = yes ]; then
    # Ensure pytest even when this phase runs standalone (the build-ext phase
    # installs it; a separate --only=runloom-tests invocation may not have).
    "$PYBIN" -m pip install -q pytest 2>/dev/null || true
    # Tests that need an optional dep (hypothesis, unavailable on free-threaded
    # < 3.14) pytest.importorskip themselves, so they SKIP rather than fail here.

    rl_step "runloom suite (tests/run_isolated.py) -- REQUIRED"
    if ( cd "$ROOT" && PYTHONPATH=src "$PYBIN" tests/run_isolated.py ); then
        rl_ci_summary "✅ **runloom suite** ($VERSION, $PLATFORM): passed"
    else
        rl_warn "runloom suite FAILED"
        rl_ci_summary "❌ **runloom suite** ($VERSION, $PLATFORM): FAILED"
        rc_total=1
    fi
fi

if [ "$rc_total" -eq 0 ]; then
    rl_step "TESTS OK -- $VERSION on $PLATFORM"
else
    rl_step "TESTS FAILED -- $VERSION on $PLATFORM"
fi
exit "$rc_total"
