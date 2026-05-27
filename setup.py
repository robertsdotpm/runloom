"""pygo build script.

Goals:
  - One `pip install .` works across Linux, macOS, FreeBSD/OpenBSD/NetBSD/
    DragonFly, Solaris/illumos, Windows (MSVC / clang-cl / MinGW-w64 /
    Clang), and Android (bionic).
  - Auto-detect compiler family + architecture and pick sane defaults.
  - Degrade gracefully:
      * x86_64 / aarch64 POSIX  -> asm fast-path (.S)
      * other POSIX archs        -> ucontext fallback
      * Windows                  -> Fibers (no .S)
  - Honour user overrides:
      PYGO_BACKEND=ucontext   force ucontext on POSIX even if asm is available
      PYGO_NO_ASM=1           same as above
      PYGO_DEBUG=1            -O0 -g
      PYGO_EXTRA_CFLAGS=...   appended to compile args
      PYGO_EXTRA_LDFLAGS=...  appended to link args
      CC, CXX                 picked up by setuptools as usual

Toolchains regularly built against:
  - GCC 4.7+ / Clang 3.5+ / MinGW-w64 / ICC 17+ / MSVC 19.20+ (VS 2019 16.0+)
"""
import os
import platform
import subprocess
import sys
import sysconfig

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext

# --------------------------------------------------------------------
# Teach setuptools that .S is a valid assembler source.  GCC/Clang both
# accept .S and run the C preprocessor first; that lets us share one
# file across Linux/macOS/BSD via #ifdef __APPLE__ etc.  Windows path
# never compiles .S (uses Fibers instead of asm fcontext).
# --------------------------------------------------------------------
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
# macOS universal-binary fix.  CPython on macOS is built universal2,
# so by default it asks the compiler for `-arch arm64 -arch x86_64`
# on every source file -- including our arch-specific .S, which only
# parses as one of the two.  Pin to the current host arch so a single
# .S compiles cleanly.  Honour any user-supplied ARCHFLAGS.
# --------------------------------------------------------------------
if sys.platform == "darwin" and "ARCHFLAGS" not in os.environ:
    host_arch = platform.machine().lower()
    if host_arch in ("arm64", "aarch64"):
        os.environ["ARCHFLAGS"] = "-arch arm64"
    elif host_arch in ("x86_64", "amd64"):
        os.environ["ARCHFLAGS"] = "-arch x86_64"

# --------------------------------------------------------------------
# Platform / arch / compiler detection
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
IS_RISCV   = MACHINE.startswith("riscv")
IS_PPC     = MACHINE.startswith(("ppc", "powerpc"))

PYGO_DEBUG     = os.environ.get("PYGO_DEBUG", "").strip() not in ("", "0", "no", "false")
PYGO_NO_ASM    = os.environ.get("PYGO_NO_ASM", "").strip() not in ("", "0", "no", "false")
PYGO_BACKEND   = os.environ.get("PYGO_BACKEND", "").strip().lower()
PYGO_NO_IOCP   = os.environ.get("PYGO_NO_IOCP", "").strip() not in ("", "0", "no", "false")
PYGO_EXTRA_CFLAGS  = os.environ.get("PYGO_EXTRA_CFLAGS", "").split()
PYGO_EXTRA_LDFLAGS = os.environ.get("PYGO_EXTRA_LDFLAGS", "").split()

USE_UCONTEXT = (
    PYGO_BACKEND == "ucontext"
    or PYGO_NO_ASM
    or (IS_POSIX and not (IS_X86_64 or IS_AARCH64))
)


def _which(prog):
    """Cross-platform shutil.which that returns None if not found."""
    try:
        from shutil import which as _w
        return _w(prog)
    except Exception:
        return None


def _using_mingw():
    """True when setuptools is going to drive a MinGW gcc.exe on Windows."""
    if not IS_WINDOWS:
        return False
    cc = os.environ.get("CC", "").lower()
    if "mingw" in cc or "gcc" in cc:
        return True
    if "--compiler=mingw" in " ".join(sys.argv):
        return True
    return False


def _using_clang_cl():
    """True when setuptools is using clang-cl (MSVC-compat clang) on Windows."""
    if not IS_WINDOWS:
        return False
    cc = os.environ.get("CC", "").lower()
    return "clang-cl" in cc


def _probe_compiler():
    """Print a one-line banner describing the detected toolchain."""
    cc = os.environ.get("CC", "")
    if not cc:
        if IS_WINDOWS and _using_mingw():
            cc = "mingw-gcc"
        elif IS_WINDOWS and _using_clang_cl():
            cc = "clang-cl"
        elif IS_WINDOWS:
            cc = "msvc"
        else:
            cc = sysconfig.get_config_var("CC") or "(default)"
    print("pygo build: platform=%s machine=%s python=%d.%d cc=%s backend=%s"
          % (PLAT, MACHINE, sys.version_info[0], sys.version_info[1],
             cc, "ucontext" if USE_UCONTEXT else "asm/fibers"))


# --------------------------------------------------------------------
# Source list
# --------------------------------------------------------------------
def detect_sources():
    srcs = [
        os.path.join(SRC_C, "module.c"),
        os.path.join(SRC_C, "coro.c"),
        os.path.join(SRC_C, "fcontext.c"),
        os.path.join(SRC_C, "pygo_sched.c"),
        os.path.join(SRC_C, "netpoll.c"),
        os.path.join(SRC_C, "cldeque.c"),
        os.path.join(SRC_C, "mn_sched.c"),
        os.path.join(SRC_C, "chan.c"),
    ]
    # Windows IOCP-AFD source -- compiled but no-op on non-Windows
    # because the whole file is wrapped in #if defined(PYGO_OS_WINDOWS).
    srcs.append(os.path.join(SRC_C, "netpoll_iocp.c"))

    # Arch-specific asm fast path.  POSIX only; Windows uses Fibers.
    # Other archs (riscv, ppc, ...) fall through to the ucontext POSIX
    # backend via #ifdef in coro.c.
    if IS_POSIX and not USE_UCONTEXT:
        if IS_X86_64:
            srcs.append(os.path.join(SRC_C, "arch", "swap_x86_64.S"))
        elif IS_AARCH64:
            srcs.append(os.path.join(SRC_C, "arch", "swap_aarch64.S"))
    return srcs


def detect_compile_args():
    args = []

    if IS_WINDOWS and not _using_mingw():
        # MSVC / clang-cl.
        args += [
            "/W3",
            "/std:c11",                # _Generic, needed by plat_atomic.h
            "/D_CRT_SECURE_NO_WARNINGS",
            "/DWIN32_LEAN_AND_MEAN",
            "/DNOMINMAX",
            "/D_WIN32_WINNT=0x0600",   # Vista+: enables WSAPoll prototype
            "/DFD_SETSIZE=1024",
        ]
        args.append("/Od" if PYGO_DEBUG else "/O2")
        if PYGO_DEBUG:
            args.append("/Zi")
    else:
        # GCC / Clang / MinGW / ICC.
        args += [
            "-std=gnu99",          # -std=c99 hides cpu_set_t / pthread internals on glibc
            "-Wall",
            "-Wextra",
            "-Wno-unused-parameter",
            "-fno-strict-aliasing",
        ]
        args.append("-O0" if PYGO_DEBUG else "-O2")
        if PYGO_DEBUG:
            args.append("-g")
        if USE_UCONTEXT:
            args.append("-DPYGO_FORCE_UCONTEXT=1")
        if IS_LINUX or IS_ANDROID:
            args += ["-D_GNU_SOURCE"]
        if IS_DARWIN:
            args += [
                "-D_XOPEN_SOURCE=600",
                "-D_DARWIN_C_SOURCE",
                "-Wno-deprecated-declarations",
            ]
        if IS_BSD:
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
    if PYGO_NO_IOCP and IS_WINDOWS:
        args.append("-DPYGO_NO_IOCP=1" if not _using_mingw() else "-DPYGO_NO_IOCP=1")
    args += PYGO_EXTRA_CFLAGS
    return args


def detect_link_args():
    """System libraries to link against."""
    libs = []
    if IS_WINDOWS:
        # Fibers + WSAPoll + select + Winsock all live in these two:
        #   kernel32 -- threads, fibers, memory, sync primitives
        #   ws2_32   -- Winsock 2 (WSAPoll, select, sockets)
        # ntdll is loaded dynamically by netpoll_iocp.c (GetProcAddress)
        # so we don't need ntdll.lib at link time.
        libs += ["kernel32", "ws2_32"]
    elif IS_SOLARIS:
        libs += ["socket", "nsl"]
    elif IS_BSD or IS_DARWIN:
        # libc has everything (kqueue, pthread on BSD).
        pass
    elif IS_LINUX:
        libs += ["pthread"]
    elif IS_ANDROID:
        # bionic bundles pthread; no -lpthread needed and on some NDKs
        # it errors out if you try to link it.
        pass
    return libs


def detect_link_flags():
    """Extra linker flags (not libraries)."""
    flags = []
    if IS_WINDOWS and _using_mingw():
        # Static-link the GCC runtime so the resulting .pyd doesn't
        # depend on libgcc_s_seh-1.dll / libwinpthread-1.dll being on
        # PATH when imported.
        flags += [
            "-static-libgcc",
            "-Wl,-Bstatic", "-lwinpthread",
        ]
    flags += PYGO_EXTRA_LDFLAGS
    return flags


# --------------------------------------------------------------------
# Custom build_ext with graceful fallback to ucontext if asm fails
# --------------------------------------------------------------------
class pygo_build_ext(_build_ext):
    """Wrap build_ext to retry with the ucontext backend if asm fails.

    Some toolchains (busybox-on-musl, exotic cross-compilers, very old
    Solaris/illumos assemblers) reject the .S files even on x86_64 /
    aarch64.  Instead of crashing the install, we drop the .S source
    and the asm define, then retry the build with ucontext semantics.
    """

    def run(self):
        global USE_UCONTEXT
        try:
            super().run()
        except Exception as e:
            if USE_UCONTEXT:
                raise
            # Already failed once with asm; switch to ucontext and retry.
            print("pygo build: asm path failed (%s); retrying with ucontext"
                  % e.__class__.__name__)
            USE_UCONTEXT = True
            for e_obj in self.extensions:
                e_obj.sources = [s for s in e_obj.sources
                                 if not s.endswith((".S", ".s"))]
                e_obj.extra_compile_args = detect_compile_args()
            super().run()


_probe_compiler()

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
    cmdclass={"build_ext": pygo_build_ext},
)
