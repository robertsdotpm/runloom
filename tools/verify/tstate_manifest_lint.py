#!/usr/bin/env python3
"""tstate_manifest_lint.py -- force a decided disposition for EVERY PyThreadState
field (item 15).

runloom's scheduler snapshots/restores pieces of PyThreadState per fiber
(runloom_sched_pystate.c.inc).  The recurring bug (appendix 9/11/12/72/73): a new
CPython release ADDS a tstate field that carries per-thread state (exc chain,
contextvars, decimal context, the 3.14 c_stack guard), runloom never privatizes
it, and a cross-fiber leak or crash ships.  Nobody DECIDED about the field
because nothing forced the decision.

This lint forces it.  It enumerates every PyThreadState field with libclang (the
same AST tool the fault-site rewriter uses -- reliable, not a regex), and checks
each against a committed manifest of dispositions:

  SNAP        -- snapshotted+restored per fiber (own/borrow noted separately)
  OWNER_ONLY  -- read only on the owning thread; must never be snapped
  SHARED_OK   -- interpreter-wide / immutable across fibers; safe to share
  IGNORE      -- runloom provably does not depend on it

A struct field with NO manifest entry FAILS the lint -> a new CPython field
cannot slip in unclassified.  A field the code TOUCHES but the manifest marks
IGNORE/SHARED_OK also FAILS (the disposition is a lie).  --init seeds/updates the
manifest for the current interpreter (review the REVIEW entries by hand).

House style: %/.format, prints kept.
"""
import argparse
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
MANIFEST = os.path.join(HERE, "tstate_manifest.json")
PYSTATE_TU = os.path.join(ROOT, "src/runloom_c/runloom_sched_pystate.c.inc")
PY = os.environ.get("RUNLOOM_PYTHON",
                    os.path.expanduser("~/.pyenv/versions/3.14.4t/bin/python3"))


def py_include():
    return subprocess.check_output(
        [PY, "-c", "import sysconfig;print(sysconfig.get_path('platinclude'))"],
        text=True).strip()


def struct_fields():
    """Enumerate PyThreadState (struct _ts) top-level fields via libclang.
    Returns None (-> the lint SKIPs, not fails) whenever clang is unavailable:
    the python `clang` package missing, OR the libclang shared lib missing/unusable.
    (The old fallback re-`import clang.cindex` inside the except -- which raises
    AGAIN and uncaught when the package itself is absent, crashing the lint into a
    gate failure instead of the intended SKIP.)"""
    try:
        import clang.cindex as ci
    except Exception:
        return None                          # python clang package not installed
    for cand in ("/usr/lib/llvm-18/lib/libclang.so.1",
                 "/usr/lib/x86_64-linux-gnu/libclang-18.so.1"):
        if os.path.exists(cand):
            try:
                ci.Config.set_library_file(cand)
            except Exception:
                pass                         # already configured, or set_library_file locked
            break
    inc = py_include()
    src = "#define Py_BUILD_CORE 1\n#include <Python.h>\n"
    try:
        idx = ci.Index.create()
        tu = idx.parse("t.c", args=["-I" + inc, "-D_GNU_SOURCE"],
                       unsaved_files=[("t.c", src)],
                       options=ci.TranslationUnit.PARSE_INCOMPLETE)
    except Exception:
        return None                          # libclang shared lib missing/unusable
    fields = []

    def walk(node):
        if (node.kind == ci.CursorKind.STRUCT_DECL and node.spelling == "_ts"):
            for c in node.get_children():
                if c.kind == ci.CursorKind.FIELD_DECL:
                    fields.append(c.spelling)
            return
        for c in node.get_children():
            walk(c)
    walk(tu.cursor)
    return fields or None


def touched_fields():
    """Fields the scheduler actually reads/writes: `ts->X` / `tstate->X`."""
    touched = set()
    for tu in (PYSTATE_TU, os.path.join(ROOT, "src/runloom_c/runloom_iframe.c")):
        if not os.path.exists(tu):
            continue
        for m in re.finditer(r"\b(?:ts|tstate)->([A-Za-z_]\w*)",
                             open(tu, errors="replace").read()):
            touched.add(m.group(1))
    return touched


def load_manifest():
    return json.load(open(MANIFEST)) if os.path.exists(MANIFEST) else {}


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true",
                    help="seed/update the manifest for the current interpreter")
    a = ap.parse_args(argv)

    fields = struct_fields()
    if fields is None:
        print("[tstate-manifest] SKIP: libclang not available")
        return 0
    touched = touched_fields()
    man = load_manifest()

    if a.init:
        for f in fields:
            if f not in man:
                man[f] = {"disposition": "REVIEW" if f in touched else "IGNORE",
                          "note": ("TOUCHED by scheduler -- classify: SNAP / "
                                   "OWNER_ONLY / SHARED_OK") if f in touched
                                  else "not touched by runloom"}
        json.dump(man, open(MANIFEST, "w"), indent=1, sort_keys=True)
        print("[tstate-manifest] seeded %d fields -> %s (review REVIEW entries)"
              % (len(man), MANIFEST))
        return 0

    fails = []
    for f in fields:
        if f not in man:
            fails.append("field %r has NO manifest disposition -- a new "
                         "PyThreadState field nobody decided about" % f)
    for f in touched:
        d = man.get(f, {}).get("disposition")
        if d in ("IGNORE", "SHARED_OK"):
            fails.append("field %r is TOUCHED by the scheduler but classified "
                         "%s -- the disposition is wrong" % (f, d))
        if d == "REVIEW":
            fails.append("field %r still marked REVIEW -- assign a real "
                         "disposition (SNAP/OWNER_ONLY/SHARED_OK)" % f)
    stale = [f for f in man if f not in fields]
    print("[tstate-manifest] %d struct fields, %d touched, %d manifest entries, "
          "%d stale" % (len(fields), len(touched), len(man), len(stale)))
    if stale:
        print("  (stale -- in manifest but not in this interpreter's struct: %s)"
              % ", ".join(sorted(stale)))
    if fails:
        print("[tstate-manifest] FAIL:")
        for x in fails:
            print("  " + x)
        print("  fix: classify in %s (or re-run --init to seed new fields)"
              % os.path.relpath(MANIFEST, ROOT))
        return 1
    print("[tstate-manifest] OK: every PyThreadState field has a decided "
          "disposition; every touched field is classified consistently.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
