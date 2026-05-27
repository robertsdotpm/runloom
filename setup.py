"""pygo build script.

Picks platform-specific sources + compiler flags so the same
`pip install .` works on:

  - Linux (epoll + asm fcontext)
  - macOS (kqueue + asm fcontext)
  - FreeBSD / OpenBSD / NetBSD / DragonFly (kqueue + asm fcontext)
  - Solaris / illumos (select + ucontext)
  - Windows (WSAPoll / select + Fibers; via MSVC, clang-cl, MinGW-w64, or
    Clang.  MSVC requires VS 2019 16.8+ for C11 _Generic; clang/MinGW
    work on any version.)
  - Android (uses bionic; epoll + asm fcontext)

Toolchains tested: GCC 4.7+, Clang 3.5+ (incl. clang-cl), MSVC 19.20+
(VS 2019 16.0+), MinGW-w64, ICC.
"""
import os
import platform
import sys

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Teach setuptools that .S is a valid assembler source.  GCC/Clang both
# accept .S and run the C preprocessor first; the preprocessor lets us
# share one file across Linux/macOS/BSD via #ifdef __APPLE__ etc.
# Skipped on Windows: setup.py never compiles .S there (we use Fibers
# instead of the inline-asm fcontext backend on Windows).
try:
    from distutils.unixccompiler import UnixCCompiler
    if UnixCCompiler.src_extensions is None:
        UnixCCompiler.src_extensions = []
    if ".S" not in UnixCCompiler.src_extensions:
        UnixCCompiler.src_extensions.append(".S")
    if hasattr(UnixCCompiler, "language_map"):
        UnixCCompiler.language_map[".S"] = "c"
except Exception:
    pass

SRC_C = "src/pygo_core"

# --------------------------------------------------------------------
# Platform predicates
# --------------------------------------------------------------------
PLAT = sys.platform
IS_WINDOWS = (os.name == "nt") or PLAT.startswith("win") or PLAT == "cygwin"
IS_DARWIN  = PLAT == "darwin"
IS_LINUX   = PLAT.startswith("linux")
IS_BSD     = PLAT.startswith(("freebsd", "openbsd", "netbsd",
                              "dragonfly", "gnukfreebsd"))
IS_SOLARIS = PLAT.startswith(("sunos", "solaris"))
IS_ANDROID = (
    "ANDROID_ROOT" in os.environ
    or "ANDROID_DATA" in os.environ
    or PLAT.startswith("android")
)
IS_POSIX = not IS_WINDOWS

MACHINE = platform.machine().lower()
IS_X86_64  = MACHINE in ("x86_64", "amd64")
IS_AARCH64 = MACHINE in ("aarch64", "arm64")


def detect_sources():
    srcs = [
        os.path.join(SRC_C, "module.c"),
        os.path.join(SRC_C, "coro.c"),
        os.path.join(SRC_C, "fcontext.c"),
        os.path.join(SRC_C, "pygo_sched.c"),
        os.path.join(SRC_C, "netpoll.c"),
        os.path.join(SRC_C, "cldeque.c"),
        os.path.join(SRC_C, "mn_sched.c"),
    ]
    # Arch-specific asm fast path.  POSIX only -- Windows uses Fibers and
    # never needs the .S file.  Other archs (riscv, ppc, ...) fall through
    # to the ucontext POSIX backend.
    if IS_POSIX:
        if IS_X86_64:
            srcs.append(os.path.join(SRC_C, "arch", "swap_x86_64.S"))
        elif IS_AARCH64:
            srcs.append(os.path.join(SRC_C, "arch", "swap_aarch64.S"))
    return srcs


def detect_compile_args():
    args = []

    if IS_WINDOWS and not _using_mingw():
        # MSVC / clang-cl.  /O2 = -O2.  /W3 = moderate warnings.
        # /std:c11 enables _Generic (needed by plat_atomic.h).
        # /experimental:c11atomics not needed because we hand-roll the
        # shim, not stdatomic.h.
        # FD_SETSIZE=1024 raises Winsock select()'s per-call socket cap
        # from 64 (default) to 1024 -- WSAPoll is the primary path so
        # this only matters on XP/Server 2003 where WSAPoll isn't
        # available, but a 64-fd cap there is unusable in production.
        args += [
            "/O2", "/W3",
            "/std:c11",
            "/D_CRT_SECURE_NO_WARNINGS",
            "/DWIN32_LEAN_AND_MEAN",
            "/DNOMINMAX",
            "/D_WIN32_WINNT=0x0600",   # Vista+: enables WSAPoll prototype
            "/DFD_SETSIZE=1024",
        ]
    else:
        # GCC / Clang / MinGW / ICC.
        args += [
            "-O2",
            "-std=gnu99",  # -std=c99 hides cpu_set_t / pthread internals on glibc
            "-Wall",
            "-Wextra",
            "-Wno-unused-parameter",
            "-fno-strict-aliasing",
        ]
        if IS_LINUX or IS_ANDROID:
            args += ["-D_GNU_SOURCE"]
        if IS_DARWIN:
            # macOS ucontext requires _XOPEN_SOURCE; we set it in coro.c too,
            # but be explicit here so callers who include coro.h get it too.
            # -Wno-deprecated-declarations silences the makecontext/swapcontext
            # warnings on macOS 10.6+; we use those only as a fallback.
            args += [
                "-D_XOPEN_SOURCE=600",
                "-D_DARWIN_C_SOURCE",
                "-Wno-deprecated-declarations",
                # Universal-binary support: respect the user's ARCHFLAGS.
            ]
        if IS_BSD:
            # FreeBSD/OpenBSD/NetBSD all behave; just expose all extensions.
            args += ["-D_BSD_SOURCE"]
        if IS_SOLARIS:
            args += ["-D__EXTENSIONS__"]
        if IS_WINDOWS:
            # MinGW-w64 / clang on Windows.
            args += [
                "-DWIN32_LEAN_AND_MEAN",
                "-DNOMINMAX",
                "-D_WIN32_WINNT=0x0600",
                "-DFD_SETSIZE=1024",
            ]
    return args


def detect_link_args():
    libs = []
    if IS_WINDOWS:
        # Fibers + WSAPoll + select + Winsock all live in these two:
        #   kernel32 -- threads, fibers, memory, sync primitives
        #   ws2_32   -- Winsock 2 (WSAPoll, select, sockets)
        # advapi32 isn't required but harmless if a future feature uses it.
        libs += ["kernel32", "ws2_32"]
    elif IS_SOLARIS:
        libs += ["socket", "nsl"]
    elif IS_BSD or IS_DARWIN:
        # libc has everything (kqueue, pthread).  No -lpthread on macOS
        # since pthread is in libSystem.
        pass
    elif IS_LINUX or IS_ANDROID:
        # -lpthread is folded into glibc on modern Linux but linking it
        # explicitly stays safe on older toolchains.
        libs += ["pthread"]
    return libs


def detect_link_flags():
    """Extra linker flags (not libraries)."""
    flags = []
    if IS_LINUX:
        # Optional: keep DT_NEEDED on libpthread for older glibcs that
        # need it.  Modern toolchains do this automatically when -lpthread
        # appears in libraries=.
        pass
    return flags


def _using_mingw():
    """True when setuptools is going to drive a MinGW gcc.exe on Windows."""
    if not IS_WINDOWS:
        return False
    cc = os.environ.get("CC", "").lower()
    if "mingw" in cc or "gcc" in cc:
        return True
    # setuptools picks the compiler based on the --compiler flag too.
    if "--compiler=mingw" in " ".join(sys.argv):
        return True
    return False


ext = Extension(
    name="pygo_core",
    sources=detect_sources(),
    include_dirs=[SRC_C],
    extra_compile_args=detect_compile_args(),
    extra_link_args=detect_link_flags(),
    libraries=detect_link_args(),
)


setup(
    package_dir={"": "src"},
    packages=["pygo"],
    ext_modules=[ext],
)
