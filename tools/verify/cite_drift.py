#!/usr/bin/env python3
"""cite_drift.py -- model<->source citation-drift linter.

The formal models in `tools/verify/` and the dev docs cite the C source they
correspond to (e.g. `netpoll.c:1158-2195`, `mn_sched_mn_api.c.inc:180-216`, or a
bare `runloom_pump_dispatch_event`).  The `<400-line` code-layout refactor keeps
splitting the monoliths into `*.c.inc` fragments, so line-number citations rot:
`COVERAGE.md` itself flags this as open "Documentation debt".  A stale citation
silently mis-describes what is verified -- the proofs are fine, the *map* lies.

This linter resolves every citation against the live tree and fails on drift:

  * `<file>.c[.inc]:<line>` / `:<l1>-<l2>` where <file> is one of runloom's OWN
    sources (present in src/runloom_c/): the file must exist AND the line(s) must
    be within range.  Out-of-range or a vanished file  ->  HARD FAIL (drift).
  * the same form where <file> is NOT a runloom source (a CPython-internal file
    like pystate.c / brc.c / drain.c): classified EXTERNAL.  Resolved against
    $RUNLOOM_CPYTHON_SRC if set, otherwise reported (not failed) -- those live in
    the patched interpreter, not this repo.
  * a cited `runloom_*` / `m_select`-style symbol that appears nowhere in
    src/runloom_c/  ->  WARN (likely renamed/removed; soft because some are
    macros/CPython symbols).

Cite by **function name**, not line number, to stay drift-proof (per the
verify/README "Add a model" note); this linter is the backstop for the line
citations that remain.

Usage:
    tools/verify/cite_drift.py                  # lint, human report, exit 1 on drift
    tools/verify/cite_drift.py --json           # machine-readable
    RUNLOOM_CPYTHON_SRC=/path/to/cpython tools/verify/cite_drift.py   # also resolve external

Exit: 0 = no hard drift; 1 = >=1 runloom-file citation is out-of-range/missing.
Wire into scripts/check_all_fast.sh (cheap, no build).
"""
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))          # repo root
SRC_C = os.path.join(ROOT, "src", "runloom_c")

# Files/dirs whose comments + prose we scan for citations.
SCAN_DIRS = [HERE]                                     # tools/verify/**
SCAN_FILES = [
    os.path.join(ROOT, "docs", "dev", "cpython_boundary.md"),
    os.path.join(ROOT, "CLAUDE.md"),
]
SCAN_EXTS = (".pml", ".tla", ".c", ".h", ".v", ".als", ".litmus", ".cfg", ".md")

CITE_RE = re.compile(r"\b([A-Za-z_][\w]*\.c(?:\.inc)?):(\d+)(?:-(\d+))?")
SYM_RE = re.compile(r"\b(runloom_[a-z0-9_]+|m_[a-z][a-z0-9_]+)\b")


def runloom_sources():
    """basename -> abspath for every runloom C source (.c / .c.inc / .h)."""
    out = {}
    if not os.path.isdir(SRC_C):
        return out
    for name in os.listdir(SRC_C):
        if name.endswith((".c", ".c.inc", ".h")):
            out[name] = os.path.join(SRC_C, name)
    return out


def line_count(path):
    try:
        with open(path, "rb") as f:
            return sum(1 for _ in f)
    except OSError:
        return -1


def iter_scan_files():
    for base in SCAN_DIRS:
        for dp, _dn, fns in os.walk(base):
            for fn in fns:
                if fn.endswith(SCAN_EXTS):
                    yield os.path.join(dp, fn)
    for f in SCAN_FILES:
        if os.path.isfile(f):
            yield f


def all_source_text():
    """Concatenated text of every runloom source, for symbol-existence checks."""
    chunks = []
    for p in runloom_sources().values():
        try:
            with open(p, encoding="utf-8", errors="replace") as f:
                chunks.append(f.read())
        except OSError:
            pass
    return "\n".join(chunks)


def main(argv):
    as_json = "--json" in argv
    sources = runloom_sources()
    src_line_counts = {n: line_count(p) for n, p in sources.items()}
    cpy_src = os.environ.get("RUNLOOM_CPYTHON_SRC")

    drift = []        # hard failures (runloom file, out of range / missing)
    external = []     # cpython-internal citations
    ok = 0
    cited_syms = set()

    for fpath in iter_scan_files():
        rel = os.path.relpath(fpath, ROOT)
        try:
            with open(fpath, encoding="utf-8", errors="replace") as f:
                text = f.read()
        except OSError:
            continue
        for m in CITE_RE.finditer(text):
            base, l1s, l2s = m.group(1), m.group(2), m.group(3)
            l1 = int(l1s)
            l2 = int(l2s) if l2s else l1
            lineno = text.count("\n", 0, m.start()) + 1
            where = "{0}:{1}".format(rel, lineno)
            if base in sources:
                n = src_line_counts[base]
                if n < 0:
                    drift.append((where, "{0}:{1}".format(base, l1s),
                                  "unreadable source"))
                elif l1 > n or l2 > n:
                    drift.append((where, m.group(0),
                                  "out of range (file has {0} lines)".format(n)))
                else:
                    ok += 1
            else:
                resolved = None
                if cpy_src:
                    cand = os.path.join(cpy_src, base)
                    if os.path.isfile(cand):
                        n = line_count(cand)
                        resolved = (l1 <= n and l2 <= n)
                external.append((where, m.group(0), resolved))
        for m in SYM_RE.finditer(text):
            cited_syms.add(m.group(1))

    src_text = all_source_text()
    missing_syms = sorted(s for s in cited_syms if s not in src_text)

    if as_json:
        print(json.dumps({
            "ok_citations": ok,
            "drift": [{"where": w, "cite": c, "why": y} for w, c, y in drift],
            "external": [{"where": w, "cite": c, "resolved": r}
                         for w, c, r in external],
            "missing_symbols": missing_syms,
        }, indent=1))
    else:
        print("cite_drift: {0} runloom-file citations OK, {1} DRIFTED, "
              "{2} external, {3} unresolved symbols"
              .format(ok, len(drift), len(external), len(missing_syms)))
        if drift:
            print("\nHARD DRIFT (runloom-source citations that no longer resolve):")
            for w, c, y in drift:
                print("  {0:<48} {1:<32} {2}".format(w, c, y))
        ext_unresolved = [(w, c) for w, c, r in external if r is False]
        if ext_unresolved:
            print("\nEXTERNAL citations out of range vs $RUNLOOM_CPYTHON_SRC:")
            for w, c in ext_unresolved:
                print("  {0:<48} {1}".format(w, c))
        if missing_syms:
            print("\nWARN: cited symbols not found in src/runloom_c/ "
                  "(renamed/removed, or a macro/CPython symbol):")
            for s in missing_syms[:40]:
                print("  {0}".format(s))
            if len(missing_syms) > 40:
                print("  ... +{0} more".format(len(missing_syms) - 40))
        if not cpy_src and external:
            print("\n(set RUNLOOM_CPYTHON_SRC=/path/to/cpython to also resolve "
                  "the {0} external cpython-internal citations)".format(len(external)))

    return 1 if drift else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
