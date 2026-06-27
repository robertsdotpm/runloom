#!/usr/bin/env python3
"""model_source_drift.py -- pin each hand-transcribed model to the real source.

The model-source audit (docs/dev/frontier/MODEL_SOURCE_AUDIT.md) found that most
CBMC/GenMC harnesses are HAND TRANSCRIPTIONS of the algorithm, not the shipped C:
only cldeque + io_classify compile real source. A transcribed model can silently
DRIFT from src/runloom_c -- the model-logic analogue of the doc-citation rot
cite_drift.py catches. (Why transcribed: the real functions pull in Python.h +
the platform layer that CBMC/GenMC can't compile standalone.)

This closes that gap WITHOUT rewriting the models: each model declares the source
function(s) it transcribes via a `SOURCE-ANCHOR:` comment; this tool extracts each
function's body from the live source, normalizes it (strips comments + whitespace,
so a comment edit is NOT drift), hashes it, and compares to a committed baseline
(model_source_anchors.json). When the source function changes, the check FAILS:
"re-vet model X against the changed source, then --update". So "faithful today"
becomes "provably still faithful, or flagged".

Annotate a model:  /* SOURCE-ANCHOR: runloom_foo runloom_bar */
(function names; resolved across all src/runloom_c/*.c[.inc] -- split-proof.)

Usage:
  model_source_drift.py            # check vs baseline (fails on drift / missing fn)
  model_source_drift.py --update   # re-baseline after vetting the models vs source
  model_source_drift.py --json
Exit: 0 = all anchors match baseline; 1 = drift / missing function; 2 = setup.
Wire into scripts/check_all.sh (cheap, no build).
"""
import argparse
import hashlib
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
SRC_C = os.path.join(ROOT, "src", "runloom_c")
BASELINE = os.path.join(HERE, "model_source_anchors.json")
ANCHOR_RE = re.compile(r"SOURCE-ANCHOR:\s*([A-Za-z0-9_ \t]+)")
SCAN_DIRS = [os.path.join(HERE, d) for d in ("cbmc", "genmc", "spin")]
SCAN_EXTS = (".c", ".pml")


def _source_files():
    out = []
    if os.path.isdir(SRC_C):
        for n in sorted(os.listdir(SRC_C)):
            if n.endswith((".c", ".c.inc", ".h")):
                out.append(os.path.join(SRC_C, n))
    return out


def _strip(code):
    """Normalize C so comment/whitespace-only edits aren't 'drift'."""
    code = re.sub(r"/\*.*?\*/", "", code, flags=re.S)   # block comments
    code = re.sub(r"//[^\n]*", "", code)                 # line comments
    code = re.sub(r"\s+", " ", code)                     # collapse whitespace
    return code.strip()


def extract_body(func):
    """Return the normalized body text of a top-level definition of `func`
    (col-0 definition line, brace-matched), or None if not found."""
    pat = re.compile(r"^[A-Za-z_][\w \t\*]*\b" + re.escape(func) + r"\s*\(", re.M)
    for path in _source_files():
        try:
            text = open(path, encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        for m in pat.finditer(text):
            # must be a DEFINITION: the next '{' comes before the next ';'
            rest = text[m.start():]
            brace = rest.find("{")
            semi = rest.find(";")
            if brace < 0 or (semi != -1 and semi < brace):
                continue                       # a declaration / call, not a def
            depth = 0
            i = m.start() + brace
            start = i
            while i < len(text):
                c = text[i]
                if c == "{":
                    depth += 1
                elif c == "}":
                    depth -= 1
                    if depth == 0:
                        return _strip(text[start:i + 1])
                i += 1
    return None


def collect_anchors():
    """model path -> [func, ...] from SOURCE-ANCHOR comments."""
    anchors = {}
    for d in SCAN_DIRS:
        if not os.path.isdir(d):
            continue
        for dp, _dn, fns in os.walk(d):
            for fn in fns:
                if not fn.endswith(SCAN_EXTS):
                    continue
                p = os.path.join(dp, fn)
                try:
                    text = open(p, encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                funcs = []
                for m in ANCHOR_RE.finditer(text):
                    funcs += m.group(1).split()
                if funcs:
                    anchors[os.path.relpath(p, ROOT)] = funcs
    return anchors


def current_hashes(anchors):
    out, missing = {}, []
    for model, funcs in anchors.items():
        out[model] = {}
        for f in funcs:
            body = extract_body(f)
            if body is None:
                missing.append((model, f))
                out[model][f] = None
            else:
                out[model][f] = hashlib.sha256(body.encode()).hexdigest()[:16]
    return out, missing


def main(argv):
    ap = argparse.ArgumentParser(description="model<->source drift detector")
    ap.add_argument("--update", action="store_true")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    anchors = collect_anchors()
    if not anchors:
        print("model_source_drift: no SOURCE-ANCHOR annotations found"); return 0
    cur, missing = current_hashes(anchors)

    if args.update:
        json.dump(cur, open(BASELINE, "w"), indent=1, sort_keys=True)
        n = sum(len(v) for v in cur.values())
        print("model_source_drift: baseline written ({0} models, {1} anchors) -> {2}"
              .format(len(cur), n, os.path.relpath(BASELINE, ROOT)))
        if missing:
            print("  WARNING: {0} anchor(s) resolve to no function:".format(len(missing)))
            for model, f in missing:
                print("    {0}: {1}".format(model, f))
        return 0

    base = {}
    if os.path.isfile(BASELINE):
        base = json.load(open(BASELINE))

    drift, gone = [], []
    for model, funcs in cur.items():
        for f, h in funcs.items():
            if h is None:
                gone.append((model, f))
                continue
            bh = base.get(model, {}).get(f)
            if bh is None:
                drift.append((model, f, "new anchor (not in baseline) -- run --update"))
            elif bh != h:
                drift.append((model, f, "SOURCE CHANGED ({0} -> {1})".format(bh, h)))

    if args.json:
        print(json.dumps({"drift": drift, "missing": gone}, indent=1));
    else:
        nfunc = sum(len(v) for v in cur.values())
        print("model_source_drift: {0} models, {1} anchored functions, "
              "{2} drifted, {3} missing".format(len(cur), nfunc, len(drift), len(gone)))
        for model, f in gone:
            print("  MISSING: {0} anchors {1} -- function not found in src/runloom_c "
                  "(renamed/removed -> re-anchor the model)".format(model, f))
        for model, f, why in drift:
            print("  DRIFT: {0} :: {1} -- {2}".format(model, f, why))
        if drift or gone:
            print("\n  -> re-vet the listed model(s) against the changed source, "
                  "then `model_source_drift.py --update` to accept.")
    return 1 if (drift or gone) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
