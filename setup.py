"""pygo build script.  Picks platform-specific sources + compiler flags
so the same `pip install .` works on Linux, macOS, BSD, Windows,
Android, Solaris -- across GCC, Clang, MSVC, MinGW, ICC, Watcom, Sun.
"""
import os
import sys
import platform

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# Teach setuptools that .S is a valid assembler source.  GCC/Clang both
# accept .S and run the C preprocessor first; the preprocessor lets us
# share one file across Linux/macOS/BSD via #ifdef __APPLE__ etc.
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


def detect_sources():
    """Source files compiled.  Arch-specific .S files are picked by host arch."""
    srcs = [
        os.path.join(SRC_C, "module.c"),
        os.path.join(SRC_C, "coro.c"),
        os.path.join(SRC_C, "fcontext.c"),
        os.path.join(SRC_C, "pygo_sched.c"),
        os.path.join(SRC_C, "netpoll.c"),
    ]
    # Arch-specific asm fast path.  Only compile what matches the host;
    # other archs fall through to ucontext (POSIX) or Fibers (Windows).
    machine = platform.machine().lower()
    posix_unix = (sys.platform in ("linux", "darwin") or
                  sys.platform.startswith(("freebsd", "openbsd", "netbsd",
                                           "dragonfly", "android")))
    if posix_unix:
        if machine in ("x86_64", "amd64"):
            srcs.append(os.path.join(SRC_C, "arch", "swap_x86_64.S"))
        elif machine in ("aarch64", "arm64"):
            srcs.append(os.path.join(SRC_C, "arch", "swap_aarch64.S"))
    return srcs


def detect_compile_args():
    """Compiler flags by toolchain.

    Strategy:
      - Be C99-strict and warnings-friendly on Unix toolchains.
      - Avoid GNU extensions in headers so MSVC / Watcom build clean.
      - Optimise but don't break -fwrapv (we depend on standard wrap).
    """
    args = []
    plat = sys.platform
    cc = os.environ.get("CC", "").lower()

    if os.name == "nt":
        # MSVC: /O2 = -O2-ish, /W3 modest warnings.  /MD = dynamic CRT.
        args += ["/O2", "/W3"]
    else:
        # Unix-y toolchain (gcc/clang/icc).
        args += [
            "-O2",
            "-std=gnu99",  # -std=c99 hides cpu_set_t / pthread internals on glibc
            "-Wall",
            "-Wextra",
            "-Wno-unused-parameter",
            "-fno-strict-aliasing",
            "-D_GNU_SOURCE",
        ]
        # macOS ucontext requires _XOPEN_SOURCE; we set it in coro.c too,
        # but be explicit here so callers who include coro.h get it too.
        if plat == "darwin":
            args += ["-D_XOPEN_SOURCE=600", "-Wno-deprecated-declarations"]
        # Solaris needs an explicit standard.
        if plat.startswith("sunos") or plat.startswith("solaris"):
            args += ["-D__EXTENSIONS__"]
    return args


def detect_link_args():
    """Linker libs by platform."""
    libs = []
    if os.name == "nt":
        # Fibers + IOCP + ws2 live in kernel32 / ws2_32.
        libs += ["kernel32", "ws2_32"]
    elif sys.platform.startswith("sunos") or sys.platform.startswith("solaris"):
        libs += ["socket", "nsl"]
    # No -lucontext anywhere -- ucontext is in libc on all our targets.
    return libs


ext = Extension(
    name="pygo_core",
    sources=detect_sources(),
    include_dirs=[SRC_C],
    extra_compile_args=detect_compile_args(),
    libraries=detect_link_args(),
)


setup(
    name="pygo",
    version="0.0.1",
    description="Go-style coroutines in Python, via a portable C99 extension.",
    author="Matthew Roberts",
    package_dir={"": "src"},
    packages=["pygo"],
    ext_modules=[ext],
    python_requires=">=3.6",
)
