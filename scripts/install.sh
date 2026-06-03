#!/usr/bin/env sh
# install.sh -- detect / bootstrap / build / install runloom.
#
# Convenience wrapper around `pip install .` that runs the compiler
# bootstrapper first if no C compiler is on PATH.  Aimed at users who
# clone the repo and just want to get runloom working without thinking
# about toolchains.
#
# Usage:
#   ./scripts/install.sh                  # install for the active python
#   ./scripts/install.sh --user           # user-site install
#   ./scripts/install.sh --editable       # pip install -e .
#   PYTHON=python3.12 ./scripts/install.sh  # build against a specific interp
#
# Honours all RUNLOOM_* env vars consumed by setup.py.

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON="${PYTHON:-python3}"
EXTRA_ARGS="$*"

log() { printf '%s\n' "[install] $*"; }
have() { command -v "$1" >/dev/null 2>&1; }

# 1. Check Python is callable.
if ! have "$PYTHON"; then
    if have python; then
        PYTHON="python"
    else
        log "no python interpreter on PATH; install Python 3.11+ first"
        exit 1
    fi
fi
PY_VER="$("$PYTHON" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
log "using $PYTHON (Python $PY_VER)"

# 2. Quick version check (matches requires-python = >=3.11 in pyproject.toml).
"$PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)' \
    || { log "runloom requires Python 3.11+; you have $PY_VER"; exit 1; }

# 3. Bootstrap a compiler if none is present.
if ! (have cc || have gcc || have clang); then
    log "no compiler on PATH; running bootstrap_compiler.sh"
    sh "$SCRIPT_DIR/bootstrap_compiler.sh" || {
        log "compiler bootstrap failed; install a C compiler manually and retry"
        exit 2
    }
fi

# 4. Make sure pip + setuptools are present.
"$PYTHON" -m pip --version >/dev/null 2>&1 || {
    log "pip not available; trying ensurepip"
    "$PYTHON" -m ensurepip --upgrade || {
        log "ensurepip failed; install pip manually"
        exit 3
    }
}
"$PYTHON" -m pip install --upgrade --quiet pip setuptools wheel

# 5. Run the install.
log "running pip install $EXTRA_ARGS ."
cd "$REPO_DIR"
# shellcheck disable=SC2086    # we want word splitting on $EXTRA_ARGS
"$PYTHON" -m pip install $EXTRA_ARGS .

# 6. Sanity-check the import + report backend.
log "validating install"
"$PYTHON" -c "import runloom_c; print('coro=', runloom_c.backend(), ' netpoll=', runloom_c.netpoll_backend())"
log "done"
