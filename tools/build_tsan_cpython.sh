#!/usr/bin/env bash
# build_tsan_cpython.sh -- build a free-threaded CPython instrumented with
# ThreadSanitizer, the "gold standard" interpreter for TSan-ing runloom (CPython's
# own internals are then instrumented too, so races that cross the
# ext <-> interpreter boundary are attributed precisely).
#
# CPython supports this directly: ./configure --disable-gil
# --with-thread-sanitizer, and ships Tools/tsan/suppressions_free_threading.txt.
#
# THE CRITICAL GOTCHA (cost a day): `configure` must ALSO run under `setarch -R`.
# With --with-thread-sanitizer, configure's own AC_RUN_IFELSE feature-probe
# binaries are compiled with -fsanitize=thread.  On Linux 6.x's high-entropy
# ASLR every TSan binary aborts at startup ("unexpected memory mapping"), so
# each runtime probe "fails" and configure bakes garbage into pyconfig.h --
# notably SIZEOF_WCHAR_T=0 and WORDS_BIGENDIAN=1 on a little-endian x86-64.
# That wrong endianness/width is exactly the "character U+6f006e is not in range"
# getpath crash (a wchar string read byte-swapped at the wrong stride).  Running
# configure under setarch -R disables ASLR for those probe binaries too, so they
# run and detect SIZEOF_WCHAR_T=4 / little-endian correctly.  (make/install were
# already wrapped; configure was the missing one.)
#
# tools/run_sanitizers_ext.sh remains the lighter-weight path (instruments only
# the ext, no patched interpreter needed); this is the gold-standard complement.
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

# Run configure UNDER setarch -R (see header): its TSan-instrumented probe
# binaries must not abort under ASLR, or feature detection silently corrupts
# pyconfig.h (SIZEOF_WCHAR_T=0, WORDS_BIGENDIAN on x86-64 -> getpath U+6f006e).
# ac_cv_buggy_getaddrinfo=no additionally skips the one network probe.
$SA env ac_cv_buggy_getaddrinfo=no \
    ./configure --disable-gil --with-thread-sanitizer --prefix="$PREFIX" >/dev/null

# Sanity-check the detection that the ASLR-abort used to corrupt, before a long
# build: bail loudly if configure still mis-detected (e.g. setarch unavailable).
wsz="$(sed -n 's/^#define SIZEOF_WCHAR_T //p' pyconfig.h)"
if [ "$wsz" != 4 ]; then
    echo "FATAL: configure mis-detected SIZEOF_WCHAR_T=$wsz (expected 4)."
    echo "       configure's TSan probes likely aborted under ASLR -- need setarch -R."
    exit 1
fi
# Robust endianness check: a naive `grep WORDS_BIGENDIAN pyconfig.h` FALSE-POSITIVES
# on every little-endian build, because autoconf's AC_C_BIGENDIAN template always
# emits the macro name in a comment, in an inactive Apple-universal-build
# `#  define WORDS_BIGENDIAN 1` branch, and in a `/* #undef */` line.  PREPROCESS
# pyconfig.h instead and test whether the macro is ACTUALLY defined on this host.
if printf '#include "pyconfig.h"\n#ifdef WORDS_BIGENDIAN\nRUNLOOM_BIG_ENDIAN\n#endif\n' \
     | cc -I. -E -P - 2>/dev/null | grep -q RUNLOOM_BIG_ENDIAN; then
    echo "FATAL: WORDS_BIGENDIAN actively defined on a little-endian host -- ASLR-probe corruption."; exit 1
fi

$SA make -j"$(nproc)"
$SA make install

PY="$PREFIX/bin/python3.13t"; [ -x "$PY" ] || PY="$PREFIX/bin/python3"
$SA "$PY" -c 'import sys; print(sys.version); print("GIL on:", sys._is_gil_enabled())'
$SA "$PY" -m ensurepip >/dev/null 2>&1 || true
$SA "$PY" -m pip install -q pytest hypothesis 2>&1 | tail -1 || true
echo "TSan interpreter: $PY"
