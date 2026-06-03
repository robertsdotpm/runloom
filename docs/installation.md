# Installation

runloom is a C extension that needs a compiler at build time.  Once
prebuilt wheels are uploaded (see roadmap), the simplest install path
will be plain `pip install runloom`; until then build from source.

## Requirements

- **Python 3.11 or newer.**  The per-goroutine `PyThreadState`
  snapshot uses 3.11+ tstate fields (`cframe`, `datastack_chunk`,
  `exc_state`).  Pre-3.11 used a different frame model that runloom
  doesn't cover.
- A C compiler.  Anything reasonably modern works: GCC 4.7+, Clang
  3.5+, MSVC 19.20+ (VS 2019 16.0+), MinGW-w64.
- Free-threaded 3.13t is fully supported and adds the M:N
  work-stealing scheduler + time-sliced preemption features.

## Editable install

```bash
git clone https://github.com/robertsdotpm/runloom
cd runloom
pip install -e .
```

On free-threaded 3.13t:

```bash
~/.pyenv/versions/3.13.13t/bin/python3.13t -m pip install -e .
```

## No compiler? Bootstrap helpers

The `scripts/` directory contains detect-and-install wrappers that
fetch a compiler before invoking pip:

=== "POSIX (Linux/macOS/BSD)"

    ```bash
    ./scripts/install.sh                # detects distro, installs gcc/clang
    ./scripts/install.sh --editable     # passes -e through to pip
    ```

=== "Windows"

    ```bat
    scripts\install.bat                 :: auto-detects MSVC, falls back to MinGW
    scripts\install.bat --editable
    ```

The orchestrator scripts probe for `gcc`/`clang`/`cl` on PATH and
invoke `bootstrap_compiler.{sh,ps1,bat}` if missing.  The bootstrap
installer knows: `apt-get`, `dnf/yum`, `pacman`, `zypper`, `apk`,
`xbps`, FreeBSD/OpenBSD/NetBSD `pkg`, `pkgin`, Haiku `pkgman`, macOS
Command Line Tools, MSVC Build Tools (via `vs_BuildTools.exe`), and
MinGW-w64 (via the WinLibs zip).

## Build-time environment knobs

| Variable | Effect |
| --- | --- |
| `RUNLOOM_BACKEND=ucontext` | Force the ucontext stack-swap backend even on x86_64/aarch64. |
| `RUNLOOM_NO_ASM=1` | Drop the `.S` source from the build (same effect as above). |
| `RUNLOOM_NO_IOCP=1` | Omit the Windows IOCP-AFD backend (falls back to WSAPoll/select). |
| `RUNLOOM_DEBUG=1` | `-O0 -g` (POSIX) or `/Od /Zi` (MSVC). |
| `RUNLOOM_EXTRA_CFLAGS` | Appended to the compile command line. |
| `RUNLOOM_EXTRA_LDFLAGS` | Appended to the link command line. |
| `CC` | Usual setuptools override; controls compiler selection on Windows too. |

## Verifying the install

```python
import runloom
print("backend:", runloom.backend())            # e.g. fcontext-asm
print("netpoll:", runloom.netpoll_backend())    # e.g. epoll
print("stack default:", runloom.get_stack_size(), "bytes")

def hello():
    print("hello from a goroutine!")
runloom.go(hello)
runloom.run(1)
```

If `backend()` returns `"fcontext-asm"`, you're on the fast path (~80
ns per context switch).  `"fibers"` means Windows Fibers (slightly
slower).  `"ucontext"` is the POSIX fallback.

## Platform support

| OS / arch | stack switch | netpoll | tested |
| --- | --- | --- | --- |
| Linux x86_64 (Debian 13, Fedora 39) | fcontext-asm | epoll | yes |
| Linux aarch64 | fcontext-asm | epoll | qemu-aarch64 |
| macOS Big Sur x86_64 | fcontext-asm | kqueue | yes |
| macOS arm64 (Apple Silicon) | fcontext-asm | kqueue | code review |
| FreeBSD 14.3 / GhostBSD x86_64 | fcontext-asm | kqueue | yes |
| OpenBSD / NetBSD / DragonFly | fcontext-asm | kqueue | code review |
| Solaris / illumos | ucontext | select | code review |
| Android (Termux) | fcontext-asm | epoll | code review |
| Windows 11 / 10 / Server 2022 | Fibers | WSAPoll | yes |
| Windows 8.1 (MinGW-w64) | Fibers | select | yes |

Windows backend selection happens at runtime: `WSAPoll` is probed via
`GetProcAddress` at first netpoll init, falling back to `select()` on
hosts where it's missing (XP/Server 2003).  One binary works across
Windows Vista through Windows 11.

## Prebuilt wheels

`pyproject.toml` ships a `[tool.cibuildwheel]` matrix covering CPython
3.11–3.14 on:

- Linux x86_64 + aarch64 (manylinux\_2\_28)
- macOS universal2 (arm64 + x86_64)
- Windows AMD64

Run `cibuildwheel --output-dir wheels` from a CI runner (or locally
with Docker) to populate `wheels/` for upload to PyPI.
