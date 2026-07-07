#!/usr/bin/env python3
"""feature_gate_lint.py -- catch UAPI feature-macro gates that silently compile
to the #else stub because the TU they land in never included the defining header.

THE TRAP (shipped once; see docs/dev memory 'uapi-macro-tu-trap'): a .c.inc
fragment gates a real code path on a kernel UAPI macro --

    #if defined(IORING_ASYNC_CANCEL_FD)
        ... the working cancel-by-fd path ...
    #else
        ... a no-op stub ...
    #endif

-- but the fragment is #included into a parent .c that lacks <linux/io_uring.h>.
The macro is therefore undefined *in that translation unit*, the #else stub
compiles with ZERO diagnostics, and a feature the kernel fully supports is
silently absent at runtime (it shipped as a hang).

WHY A LINT AND NOT -Werror: neither -Wundef nor -Werror fires here -- `#if
defined(X)` / `#ifdef X` are *designed* to be false for an undefined X, that is
their whole purpose, so the compiler cannot tell "forgot the include" from
"legit portability fallback".  Only comparing what the TU sees against what the
system COULD define distinguishes them.

METHOD (toolchain is the oracle -- no guessing about programmer intent):
  1. REFERENCE set = `cc -E -dM` of a probe that includes every relevant UAPI
     header  ->  every feature macro THIS system can define.
  2. Per primary .c TU = `cc -E -dM` of the real TU  ->  what IT actually sees.
  3. Scan each TU's #include-closure source for feature-macro gates.  A gate on a
     macro that is in REFERENCE but NOT in the TU  ==  the include was forgotten
     (the system defines it; this TU does not) -> FAIL.  A macro in NEITHER is a
     genuine portability fallback -> OK.  A macro in BOTH -> OK.

Exit 0 = clean, 1 = at least one forgotten-include gate, 2 = usage/setup error.
House style: %/.format only, no leading underscores, prints kept.
"""
import glob
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
SRC = os.path.join(ROOT, "src", "runloom_c")

# UAPI headers whose feature macros we police, and the macro-name prefixes that
# identify a "feature gate" (vs an ordinary internal macro).  Add rows as new
# kernel-feature surfaces get gated.
REFERENCE_HEADERS = [
    "linux/io_uring.h",
    "sys/epoll.h",
    "sys/eventfd.h",
    "sys/timerfd.h",
    "sys/socket.h",
    "netinet/in.h",
    "netinet/tcp.h",
]
# A gate token counts as a policed feature macro iff it matches one of these.
FEATURE_PREFIXES = ("IORING_", "IOSQE_", "EPOLL", "EFD_", "TFD_", "MSG_",
                    "SOCK_", "TCP_", "SO_")
# Tokens that share a prefix but are NOT kernel feature macros (project-internal).
FEATURE_EXCLUDE = re.compile(r"^(EPOLL_?FALLBACK|SO_FAR)$")

GATE_RE = re.compile(
    r"^\s*#\s*(?:if|ifdef|ifndef|elif)\b(.*)$", re.MULTILINE)
TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]+")
INCLUDE_LOCAL_RE = re.compile(r'^\s*#\s*include\s+"([^"]+)"', re.MULTILINE)


def cc():
    return os.environ.get("CC", "cc")


def py_include_flags():
    py = os.environ.get("RUNLOOM_PYTHON",
                        os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3"))
    try:
        out = subprocess.check_output(
            [py, "-c", "import sysconfig;print(sysconfig.get_path('include'))"],
            text=True).strip()
        return ["-I" + out] if out else []
    except Exception:
        return []


def base_flags():
    # Enough for the preprocessor to resolve the real include graph.  We only run
    # -E, so we never need the full optimizing/-DPy_BUILD_CORE compile contract;
    # _GNU_SOURCE is the one that actually gates the UAPI headers on glibc.
    return (["-E", "-dM", "-D_GNU_SOURCE", "-DPy_BUILD_CORE=1",
             "-I" + SRC] + py_include_flags())


def dump_macros(path_or_probe, is_probe_text=False):
    """Return the set of #defined macro names after preprocessing.  On failure
    (a TU that -E can't chew without the full build contract) return None so the
    caller SKIPS rather than false-fails a merge gate."""
    cmd = [cc()] + base_flags()
    try:
        if is_probe_text:
            p = subprocess.run(cmd + ["-x", "c", "-"], input=path_or_probe,
                               capture_output=True, text=True, cwd=SRC)
        else:
            p = subprocess.run(cmd + [path_or_probe], capture_output=True,
                               text=True, cwd=SRC)
    except Exception as e:
        print("[feature-gate] preprocess spawn failed: %s" % e)
        return None
    if p.returncode != 0:
        return None
    macros = set()
    for line in p.stdout.splitlines():
        m = re.match(r"#define\s+([A-Za-z_][A-Za-z0-9_]+)", line)
        if m:
            macros.add(m.group(1))
    return macros


def reference_macros():
    probe = "".join("#include <%s>\n" % h for h in REFERENCE_HEADERS)
    ref = dump_macros(probe, is_probe_text=True)
    if not ref:
        print("[feature-gate] FATAL: could not build reference macro set "
              "(are the UAPI headers installed?)")
        sys.exit(2)
    return ref


def include_closure(primary):
    """Follow local #include \"x\" recursively from a primary .c to reach the
    .c.inc fragments compiled into it.  Returns ordered unique existing paths."""
    seen, out, stack = set(), [], [primary]
    while stack:
        f = stack.pop(0)
        if f in seen or not os.path.exists(f):
            continue
        seen.add(f)
        out.append(f)
        try:
            text = open(f, encoding="utf-8", errors="replace").read()
        except Exception:
            continue
        for inc in INCLUDE_LOCAL_RE.findall(text):
            cand = os.path.normpath(os.path.join(os.path.dirname(f), inc))
            if os.path.exists(cand):
                stack.append(cand)
    return out


def feature_tokens_in_gates(path):
    """Yield (lineno, macro) for every policed feature macro named in a
    preprocessor conditional in this file."""
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except Exception:
        return
    # map char offset -> line for reporting
    for m in GATE_RE.finditer(text):
        expr = m.group(1)
        lineno = text.count("\n", 0, m.start()) + 1
        for tok in TOKEN_RE.findall(expr):
            if tok == "defined":
                continue
            if tok.startswith(FEATURE_PREFIXES) and not FEATURE_EXCLUDE.match(tok):
                yield lineno, tok


def main(argv):
    only = argv[1:]  # optional list of primary .c basenames to restrict to
    ref = reference_macros()
    primaries = sorted(glob.glob(os.path.join(SRC, "*.c")))
    if only:
        primaries = [p for p in primaries if os.path.basename(p) in only]

    failures = []
    skipped = []
    checked_tus = 0
    for primary in primaries:
        tu_macros = dump_macros(primary)
        if tu_macros is None:
            skipped.append(os.path.basename(primary))
            continue
        checked_tus += 1
        rel_primary = os.path.relpath(primary, ROOT)
        for f in include_closure(primary):
            for lineno, macro in feature_tokens_in_gates(f):
                if macro in tu_macros:
                    continue                      # TU sees it: fine
                if macro not in ref:
                    continue                      # system can't define it: legit fallback
                # in REFERENCE but not in this TU -> forgotten include: the trap.
                failures.append((rel_primary, os.path.relpath(f, ROOT),
                                 lineno, macro))

    print("[feature-gate] %d TUs checked, %d skipped (preprocess needs full "
          "build contract): %s" % (checked_tus, len(skipped),
                                   ", ".join(skipped) or "-"))
    if not failures:
        print("[feature-gate] OK: no UAPI gate silently takes the #else stub.")
        return 0
    print("[feature-gate] FAIL: %d gate(s) reference a macro the system defines "
          "but the compiling TU does NOT (silent #else stub):" % len(failures))
    for tu, frag, lineno, macro in failures:
        note = "" if frag == tu else "  (fragment compiled INTO %s)" % tu
        print("  %s:%d  gates on %s -- undefined in TU %s%s"
              % (frag, lineno, macro, tu, note))
    print("[feature-gate] fix: #include the defining UAPI header in the TU, or "
          "route the gate through a generated always-defined RUNLOOM_HAVE_* macro.")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
