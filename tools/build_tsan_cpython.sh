#!/usr/bin/env bash
# build_tsan_cpython.sh -- build a free-threaded CPython instrumented with
# ThreadSanitizer, the "gold standard" interpreter for TSan-ing pygo (CPython's
# own internals are then instrumented too, so races that cross the
# ext <-> interpreter boundary are attributed precisely).
#
# CPython supports this directly: ./configure --disable-gil
# --with-thread-sanitizer, and ships Tools/tsan/suppressions_free_threading.txt.
#
# STATUS (2026-06-02): on this toolchain (gcc 13.3, glibc 2.39, Linux 6.17) the
# build compiles and links but the freshly-built `./python` aborts in frozen
# getpath with "character U+6f006e is not in range" -- a wchar-width corruption
# in the TSan-instrumented interpreter that blocks `make install`.  Tried:
# clean tree, default vs -O1 CFLAGS, LC_ALL=C.UTF-8, ac_cv_buggy_getaddrinfo.
# Until that upstream quirk is resolved, use tools/run_sanitizers_ext.sh, which
# TSan-instruments only the EXT and runs it under a stock free-threaded
# interpreter (correctly scoped to pygo's own C; CPython noise suppressed).
#
# Usage:  tools/build_tsan_cpython.sh [VERSION]
# Env:    PY_VER (default 3.13.13), SRC_DIR, PREFIX
set -euo pipefail

VER="${1:-${PY_VER:-3.13.13}}"
SRC="${SRC_DIR:-$HOME/projects/cpython-tsan}"
PREFIX="${PREFIX:-$HOME/cpython-tsan}"
RM="$(command -v safe-rm || echo rm)"

SA=""
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R"
[ -z "$SA" ] && echo "WARNING: setarch not found; TSan binaries abort under ASLR on 6.x"

# Clean tree: an ASLR-aborted partial build leaves corrupt frozen-module
# headers a resume won't regenerate.
[ -f /tmp/py-$VER.tgz ] || curl -sSL "https://www.python.org/ftp/python/$VER/Python-$VER.tgz" -o /tmp/py-$VER.tgz
$RM -rf "$SRC"
tar xzf /tmp/py-$VER.tgz -C "$(dirname "$SRC")"
mv "$(dirname "$SRC")/Python-$VER" "$SRC"
cd "$SRC"

# ac_cv_buggy_getaddrinfo=no: skip the network runtime-probe a sandboxed build
# fails (same workaround pyenv uses).  Default optimization (no CFLAGS override).
ac_cv_buggy_getaddrinfo=no \
./configure --disable-gil --with-thread-sanitizer --prefix="$PREFIX" >/dev/null

$SA make -j"$(nproc)"
$SA make install

PY="$PREFIX/bin/python3.13t"; [ -x "$PY" ] || PY="$PREFIX/bin/python3"
$SA "$PY" -c 'import sys; print(sys.version); print("GIL on:", sys._is_gil_enabled())'
$SA "$PY" -m ensurepip >/dev/null 2>&1 || true
$SA "$PY" -m pip install -q pytest hypothesis 2>&1 | tail -1 || true
echo "TSan interpreter: $PY"
