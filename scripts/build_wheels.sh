#!/usr/bin/env sh
# build_wheels.sh -- build redistributable wheels + sdist locally.
#
# This project uses NO hosted CI, so prebuilt wheels (the "pip install just
# works, no compiler" experience) are built by hand.  Run this ONCE PER
# PLATFORM you want wheels for; each run builds the whole CPython matrix
# (3.11-3.14) for the platform it runs on:
#
#   Linux   (needs Docker):     ./scripts/build_wheels.sh
#   macOS   (on a Mac):         ./scripts/build_wheels.sh
#   Windows (Git Bash / WSL):   sh scripts/build_wheels.sh
#
# Output:
#   ./dist        the source distribution (.tar.gz) -- platform-independent
#   ./wheelhouse  the prebuilt wheels (.whl) for this platform
#
# Then publish everything at once (see RELEASING.md):
#   twine upload dist/*.tar.gz wheelhouse/*.whl
#
# Requires the dev tools:  pip install "pygo-runtime[dev]"   (build + cibuildwheel)
set -eu

cd "$(dirname "$0")/.."

# Pick a Python launcher.
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python

run_tool() {
    # Run an installed module, falling back to `pipx run` if it isn't installed.
    mod="$1"; shift
    if "$PY" -c "import $1" >/dev/null 2>&1; then
        "$PY" -m "$mod" "$@"
    elif command -v pipx >/dev/null 2>&1; then
        pipx run "$mod" "$@"
    else
        echo "error: '$mod' is not installed. Run:  $PY -m pip install \"pygo-runtime[dev]\"" >&2
        echo "       (or install pipx)" >&2
        exit 1
    fi
}

echo ">> building source distribution (dist/) ..."
# The sdist is the same on every platform; building it repeatedly is harmless.
run_tool build build --sdist --outdir dist

echo ">> building wheels for this platform (wheelhouse/) ..."
# Matrix + manylinux images + per-OS settings live in pyproject [tool.cibuildwheel].
run_tool cibuildwheel cibuildwheel --output-dir wheelhouse

echo
echo ">> done."
echo "   sdist:  $(ls dist/*.tar.gz 2>/dev/null || echo '(none)')"
echo "   wheels: $(ls wheelhouse/*.whl 2>/dev/null | wc -l) in ./wheelhouse"
echo
echo "   publish with:  twine upload dist/*.tar.gz wheelhouse/*.whl"
