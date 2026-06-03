#!/usr/bin/env bash
# Phase E validation: cross-compile runloom's asm context switch + coro
# trampoline for aarch64, run under qemu-aarch64 user-mode emulation.
#
# Requires: aarch64-linux-gnu-gcc and qemu-aarch64 (or -static).
# Both are in Debian/Ubuntu's gcc-aarch64-linux-gnu and qemu-user-static
# packages respectively.
set -e
cd "$(dirname "$0")/.."

CC=aarch64-linux-gnu-gcc
QEMU=$(command -v qemu-aarch64-static || command -v qemu-aarch64)
SYSROOT=/usr/aarch64-linux-gnu

if [ -z "$QEMU" ]; then
    echo "qemu-aarch64 not installed; skipping" >&2
    exit 0
fi

OUT=/tmp/runloom_aarch64_test
SRC=src/runloom_c

$CC -std=gnu99 -O2 -Wall -Wextra -Wno-unused-parameter -D_GNU_SOURCE \
    -DRUNLOOM_HAVE_FCONTEXT -DRUNLOOM_ARCH_AARCH64 \
    -I"$SRC" \
    "$SRC/arch/swap_aarch64.S" \
    "$SRC/coro.c" \
    "$SRC/fcontext.c" \
    tests/test_arm64.c \
    -o "$OUT"

file "$OUT"
echo "--- running under $QEMU ---"
"$QEMU" -L "$SYSROOT" "$OUT"
