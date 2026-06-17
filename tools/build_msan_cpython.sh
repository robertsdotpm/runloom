#!/usr/bin/env bash
# build_msan_cpython.sh -- build a --disable-gil free-threaded CPython 3.13t under
# MemorySanitizer (clang -fsanitize=memory) so uninitialized-memory reads in
# runloom's C (recycled goroutine stacks, the intrusive free-list pool headers in
# coro.c, structs handed across hubs before full init) are caught.  The MSan
# complement to build_tsan_cpython.sh -- the one bug class ASan/TSan/CBMC do NOT
# cover (confirmed gap, 2026-06-17 OSS-scanner sweep).
#
# MSan is CLANG-ONLY (gcc has no -fsanitize=memory).
#
# WHY NOT --with-memory-sanitizer + setarch (the TSan recipe): that flag
# instruments configure's AC_RUN_IFELSE probe binaries too.  MSan probes FP on
# uninstrumented libc (unlike TSan's single-thread probes, which are race-clean),
# so the probes exit non-zero and configure bakes garbage into pyconfig.h
# (WORDS_BIGENDIAN on x86-64).  Instead: CONFIGURE CLEAN (probes detect features
# correctly), then add MSan ONLY at make time via CFLAGS_NODIST/LDFLAGS_NODIST
# (CPython's hook for flags that build the interpreter but are NOT propagated to
# distutils-built extensions).  --without-pymalloc so MSan tracks real malloc.
# MSAN_OPTIONS=halt_on_error=0 for the freeze step (the half-built python runs
# under MSan and FPs on uninstrumented libc; MSan only reports, never alters
# behavior, so freezing still produces correct output).
#
# CAVEAT for OUTPUT: MSan needs EVERY linked object instrumented.  System
# libc/openssl/_socket are NOT, so values they return read as uninit unless
# intercepted.  Treat reports rooted in runloom_c/* frames as REAL; libc/_ssl/
# _socket-rooted ones are the uninstrumented-lib floor (see tools/run_msan.sh).
set -u
VER=3.13.13
SRC="${SRC_DIR:-$HOME/projects/cpython-msan}"
PREFIX="${PREFIX:-$HOME/cpython-msan}"
RM="$(command -v safe-rm || echo rm)"
export CC="${CC:-clang}" CXX="${CXX:-clang++}"
MSAN_CFLAGS="-fsanitize=memory -fsanitize-memory-track-origins=2 -fno-omit-frame-pointer -g -O1"

# setarch -R (ASLR off) is needed for MAKE: the freeze step runs the just-built
# MSan-instrumented python, which (like TSan binaries) aborts under Linux 6.x
# high-entropy ASLR -> corrupt frozen headers / misdetection.  Harmless on the
# (clean, non-instrumented) configure probes, so we wrap both.
SA=""
command -v setarch >/dev/null 2>&1 && SA="setarch $(uname -m) -R"
[ -z "$SA" ] && echo "WARNING: setarch not found; MSan freeze-step binaries abort under ASLR on 6.x"

echo "=== fetch + unpack pristine Python-$VER ==="
[ -f /tmp/py-$VER.tgz ] || curl -sSL "https://www.python.org/ftp/python/$VER/Python-$VER.tgz" -o /tmp/py-$VER.tgz
$RM -rf "$SRC"
tar xzf /tmp/py-$VER.tgz -C "$(dirname "$SRC")"
mv "$(dirname "$SRC")/Python-$VER" "$SRC"
cd "$SRC" || exit 2

echo "=== configure CLEAN (no MSan in probes -> correct pyconfig.h), clang, GIL off, no pymalloc, under setarch -R ==="
$SA env ac_cv_buggy_getaddrinfo=no ac_cv_c_bigendian=no \
    ./configure --disable-gil --without-pymalloc --prefix="$PREFIX" >configure.out 2>&1
tail -3 configure.out
wsz="$(sed -n 's/^#define SIZEOF_WCHAR_T //p' pyconfig.h)"
if [ "$wsz" != 4 ]; then echo "FATAL: SIZEOF_WCHAR_T=$wsz (expected 4)"; exit 1; fi
# Only a column-0 `#define WORDS_BIGENDIAN` is the autoconf ASLR-probe corruption
# we guard against -- modern pyconfig.h ALWAYS contains an indented `#  define
# WORDS_BIGENDIAN 1` inside an `#if defined(__BIG_ENDIAN__)` compile-time block,
# which is correct and must NOT trip the check.
grep -qE '^#define[[:space:]]+WORDS_BIGENDIAN' pyconfig.h && { echo "FATAL: top-level WORDS_BIGENDIAN on little-endian host (ASLR-probe corruption)"; exit 1; }
echo "configure clean: SIZEOF_WCHAR_T=$wsz, no WORDS_BIGENDIAN"

echo "=== make with MSan added ONLY here (CFLAGS_NODIST/LDFLAGS_NODIST); freeze runs MSan'd python ==="
export MSAN_OPTIONS="halt_on_error=0:exitcode=0:abort_on_error=0"
$SA make -j"$(nproc)" \
     CFLAGS_NODIST="$MSAN_CFLAGS" \
     LDFLAGS_NODIST="-fsanitize=memory" >make.out 2>&1
echo "make_rc=$?"; tail -6 make.out
$SA make install >install.out 2>&1; echo "install_rc=$?"; tail -2 install.out

PY="$PREFIX/bin/python3.13t"; [ -x "$PY" ] || PY="$PREFIX/bin/python3"
echo "=== smoke the MSan interpreter ==="
[ -x "$PY" ] && MSAN_OPTIONS=halt_on_error=0:exitcode=0 $SA "$PY" -c \
   'import sys,sysconfig; print(sys.version); print("GIL_DISABLED", sysconfig.get_config_var("Py_GIL_DISABLED"))' 2>&1 | head -6
echo "MSAN_PYTHON=$PY"
echo "===== build_msan_cpython done ====="
