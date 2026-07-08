"""runloom build script.

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
      RUNLOOM_BACKEND=ucontext   force ucontext on POSIX even if asm is available
      RUNLOOM_NO_ASM=1           same as above
      RUNLOOM_DEBUG=1            -O0 -g
      RUNLOOM_EXTRA_CFLAGS=...   appended to compile args
      RUNLOOM_EXTRA_LDFLAGS=...  appended to link args
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

SRC_C = "src/runloom_c"

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

RUNLOOM_DEBUG     = os.environ.get("RUNLOOM_DEBUG", "").strip() not in ("", "0", "no", "false")
RUNLOOM_NO_ASM    = os.environ.get("RUNLOOM_NO_ASM", "").strip() not in ("", "0", "no", "false")
RUNLOOM_BACKEND   = os.environ.get("RUNLOOM_BACKEND", "").strip().lower()
RUNLOOM_NO_IOCP   = os.environ.get("RUNLOOM_NO_IOCP", "").strip() not in ("", "0", "no", "false")
# RUNLOOM_CTXCHECK=1 arms the debug lock-order rank checker AND the park/yield
# safety assert (item 10): a fiber that yields while holding a ranked lock or
# inside a no-yield region reports (or aborts with RUNLOOM_CTXCHECK_ABORT=1).
# Debug lane only -- zero cost in a normal build.
RUNLOOM_CTXCHECK  = os.environ.get("RUNLOOM_CTXCHECK", "").strip() not in ("", "0", "no", "false")
RUNLOOM_CTXCHECK_ABORT = os.environ.get("RUNLOOM_CTXCHECK_ABORT", "").strip() not in ("", "0", "no", "false")
# RUNLOOM_NETPOLL=select forces the select() fallback at build time on POSIX
# (suppresses epoll/kqueue/event_ports in plat.h so netpoll.c uses its
# select path).  On Windows the same env var is honoured at *runtime* by
# netpoll.c, so the build define is a no-op there.
RUNLOOM_FORCE_SELECT = os.environ.get("RUNLOOM_NETPOLL", "").strip().lower() == "select"
# RUNLOOM_SHRINK=1 compiles the lock-free structures (Chase-Lev deque, g-slab,
# handle segments, ready ring, QSBR grace ring) with TINY capacities so
# wraparound / steal-collision / block-exhaustion / segment-growth / epoch-flip
# happen every few ops instead of once in millions -- letting ASan/TSan and the
# fuzzers reach those boundary transitions cheaply.  Test/verify lane only.
RUNLOOM_SHRINK = os.environ.get("RUNLOOM_SHRINK", "").strip() not in ("", "0", "no", "false")
# RUNLOOM_COVER=1 compiles the named reachability ("Sometimes()") counters
# (runloom_cover.h): a fuzz/soak session asserts every interesting concurrent
# state was reached at least once, so a green run can't be vacuous.  Test lane
# only -- a handful of relaxed atomic adds on rare scheduler decision points.
RUNLOOM_COVER = os.environ.get("RUNLOOM_COVER", "").strip() not in ("", "0", "no", "false")
# Force-the-rare-path build flavors (PostgreSQL CLOBBER / Go maymorestack): take
# a dangerous transition on EVERY opportunity so scale/timing Heisenbugs become
# deterministic first-run failures.  RUNLOOM_FORCE_STACKGROW copy-grows the coro
# stack a page every resume (exercises the pointer-rewrite path every time).
RUNLOOM_FORCE_STACKGROW = os.environ.get("RUNLOOM_FORCE_STACKGROW", "").strip() not in ("", "0", "no", "false")
RUNLOOM_EXTRA_CFLAGS  = os.environ.get("RUNLOOM_EXTRA_CFLAGS", "").split()
RUNLOOM_EXTRA_LDFLAGS = os.environ.get("RUNLOOM_EXTRA_LDFLAGS", "").split()

USE_UCONTEXT = (
    RUNLOOM_BACKEND == "ucontext"
    or RUNLOOM_NO_ASM
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
    print("runloom build: platform=%s machine=%s python=%d.%d cc=%s backend=%s"
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
        os.path.join(SRC_C, "runloom_sched.c"),
        os.path.join(SRC_C, "netpoll.c"),
        os.path.join(SRC_C, "cldeque.c"),
        os.path.join(SRC_C, "mn_sched.c"),
        os.path.join(SRC_C, "chan.c"),
        os.path.join(SRC_C, "runloom_diag.c"),
        os.path.join(SRC_C, "runloom_gstate.c"),
        os.path.join(SRC_C, "runloom_introspect.c"),
        os.path.join(SRC_C, "runloom_iframe.c"),
        os.path.join(SRC_C, "runloom_blockpool.c"),
        os.path.join(SRC_C, "runloom_crash.c"),
        os.path.join(SRC_C, "runloom_stackadvice.c"),
        os.path.join(SRC_C, "rl_handle.c"),
    ]
    # Windows IOCP-AFD source -- compiled but no-op on non-Windows
    # because the whole file is wrapped in #if defined(RUNLOOM_OS_WINDOWS).
    srcs.append(os.path.join(SRC_C, "netpoll_iocp.c"))
    srcs.append(os.path.join(SRC_C, "io_uring.c"))
    srcs.append(os.path.join(SRC_C, "runloom_tcp.c"))

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
            "/std:c11",                # C11 baseline (all platforms); plat_atomic.h _Generic
            "/D_CRT_SECURE_NO_WARNINGS",
            "/DWIN32_LEAN_AND_MEAN",
            "/DNOMINMAX",
            "/D_WIN32_WINNT=0x0600",   # Vista+: enables WSAPoll prototype
            "/DFD_SETSIZE=1024",
        ]
        args.append("/Od" if RUNLOOM_DEBUG else "/O2")
        if RUNLOOM_DEBUG:
            args.append("/Zi")
    else:
        # GCC / Clang / MinGW / ICC.
        args += [
            "-std=gnu11",          # C11 baseline (matches MSVC /std:c11); gnu* (not strict
                                   # c11) keeps cpu_set_t / pthread internals visible on glibc
            "-Wall",
            "-Wextra",
            "-Wno-unused-parameter",
            "-fno-strict-aliasing",
            # Security hardening (cheap, portable across GCC/Clang on Linux/
            # macOS/BSD): stack canaries on at-risk frames + flag the classic
            # format-string vulnerability (printf(user_str)) at compile time.
            "-fstack-protector-strong",
            "-Wformat-security",
        ]
        args.append("-O0" if RUNLOOM_DEBUG else "-O2")
        if not RUNLOOM_DEBUG:
            # _FORTIFY_SOURCE bounds-checks libc calls (memcpy/strcpy/sprintf/
            # ...) at runtime; it needs optimization (-O1+) so it is a no-op /
            # warning under RUNLOOM_DEBUG's -O0.  =2 is the widely-portable level.
            args.append("-D_FORTIFY_SOURCE=2")
        if RUNLOOM_DEBUG:
            args.append("-g")
        if USE_UCONTEXT:
            args.append("-DRUNLOOM_FORCE_UCONTEXT=1")
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
    if RUNLOOM_NO_IOCP and IS_WINDOWS:
        args.append("-DRUNLOOM_NO_IOCP=1" if not _using_mingw() else "-DRUNLOOM_NO_IOCP=1")
    if RUNLOOM_FORCE_SELECT:
        # MSVC uses /D; everything else (GCC/Clang/MinGW) uses -D.  Same
        # macro either way; consumed by plat.h's netpoll selector.
        if IS_WINDOWS and not _using_mingw():
            args.append("/DRUNLOOM_FORCE_SELECT=1")
        else:
            args.append("-DRUNLOOM_FORCE_SELECT=1")
    if RUNLOOM_CTXCHECK:
        # CTXCHECK implies LOCKRANK (the park assert reads the held-rank stack).
        args += ["-DRUNLOOM_LOCKRANK=1", "-DRUNLOOM_CTXCHECK=1"]
        if RUNLOOM_CTXCHECK_ABORT:
            args += ["-DRUNLOOM_LOCKRANK_ABORT=1", "-DRUNLOOM_CTXCHECK_ABORT=1"]
    if RUNLOOM_SHRINK:
        args.append("-DRUNLOOM_SHRINK=1")
    if RUNLOOM_COVER:
        args.append("-DRUNLOOM_COVER=1")
    if RUNLOOM_FORCE_STACKGROW:
        args.append("-DRUNLOOM_FORCE_STACKGROW=1")
    args += RUNLOOM_EXTRA_CFLAGS
    return args


def detect_link_args():
    """System libraries to link against."""
    libs = []
    if IS_WINDOWS:
        # Fibers + WSAPoll + select + Winsock all live in these two:
        #   kernel32 -- threads, fibers, memory, sync primitives
        #   ws2_32   -- Winsock 2 (WSAPoll, select, sockets)
        #   winmm    -- timeBeginPeriod (1ms scheduler timer resolution)
        # ntdll is loaded dynamically by netpoll_iocp.c (GetProcAddress)
        # so we don't need ntdll.lib at link time.
        libs += ["kernel32", "ws2_32", "winmm"]
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
    flags += RUNLOOM_EXTRA_LDFLAGS
    return flags


# --------------------------------------------------------------------
# Custom build_ext with graceful fallback to ucontext if asm fails
# --------------------------------------------------------------------
class runloom_build_ext(_build_ext):
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
            print("runloom build: asm path failed (%s); retrying with ucontext"
                  % e.__class__.__name__)
            USE_UCONTEXT = True
            for e_obj in self.extensions:
                e_obj.sources = [s for s in e_obj.sources
                                 if not s.endswith((".S", ".s"))]
                e_obj.extra_compile_args = detect_compile_args()
            super().run()


_probe_compiler()

# Headers and #included `.c.inc` fragments are not in `sources`, so editing one
# would NOT trigger a recompile of the `.c` that #includes it -- build_ext only
# compares each source's mtime to its `.o`.  List them as `depends` so touching
# any header or fragment rebuilds the whole extension.  (The big `.c` files are
# split into `<stem>_*.c.inc` fragments; see e.g. module.c.)
import glob as _glob
_ext_depends = sorted(
    _glob.glob(os.path.join(SRC_C, "*.h"))
    + _glob.glob(os.path.join(SRC_C, "*.c.inc"))
)

ext = Extension(
    name="runloom_c",
    sources=detect_sources(),
    include_dirs=[SRC_C],
    depends=_ext_depends,
    extra_compile_args=detect_compile_args(),
    extra_link_args=detect_link_flags(),
    libraries=detect_link_args(),
)


setup(
    package_dir={"": "src"},
    packages=["runloom", "runloom.monkey", "runloom.aio"],
    # Ship the PEP 561 typing marker + stubs inside the wheel so type
    # checkers see runloom as typed once it's installed.
    package_data={"runloom": ["py.typed", "*.pyi"]},
    ext_modules=[ext],
    cmdclass={"build_ext": runloom_build_ext},
)
