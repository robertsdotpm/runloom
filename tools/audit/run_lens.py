#!/usr/bin/env python3
"""run_lens.py -- drive the taxonomy audit lenses (item 2).

Each of the 16 root-cause classes from the bug history is encoded as a reusable
lens in lenses.json: a mechanical surfacer (an rg pattern that lists candidate
sites), a reusable model-audit prompt (the judgement a human/agent applies to
each site), the diff-time trigger paths, and the fix-time variant-analysis sweep.

The bug history's lesson (SYSTEMATIC_IMPROVEMENTS.md): the same hole was re-made
2-18 times per class because fixes were one-off. A lens makes "find every sibling
of this bug" mechanical instead of heroic.

Usage:
  run_lens.py list                       # all lenses
  run_lens.py <class>                     # show one lens (prompt, sweep, triggers)
  run_lens.py <class> --run               # execute its rg surfacer -> candidate sites
  run_lens.py --changed <file>...         # which lenses to re-run for these changes
  run_lens.py --audit-brief <class>       # just the model-audit prompt (for an agent)

House style: %/.format, prints kept, no leading underscores.
"""
import fnmatch
import json
import os
import re
import shutil
import subprocess
import sys

GREP = shutil.which("grep") or "/usr/bin/grep"


def to_grep(cmd):
    """Translate an rg surfacer to GNU grep (guaranteed present; rg may be a
    shell alias invisible to /bin/sh).  rg is recursive by default, so only the
    FIRST (path-scanning) pipeline segment gets -r; piped segments filter stdin
    or receive xargs file args and must NOT.  All flags used by the lenses
    (-n -P -l -L -v -i -B) are GNU-grep-compatible."""
    segs = cmd.split("|")
    out = []
    for idx, seg in enumerate(segs):
        s = seg.strip()
        if s.startswith("rg "):
            rest = s[3:]
            out.append("%s %s%s" % (GREP, "-r " if idx == 0 else "", rest))
        elif "xargs rg " in s:
            out.append(s.replace("xargs rg ", "xargs %s " % GREP))
        else:
            out.append(s)
    return " | ".join(out)

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
LENSES = json.load(open(os.path.join(HERE, "lenses.json")))
BY_NAME = {l["class_name"]: l for l in LENSES}


def runnable_prefix(query):
    """Extract the executable surfacer from a grep_or_query string.  The authored
    form mixes a command with inline prose ('rg -nP '...' src  (note...) ; then
    inspect ...').  Walk the string tracking quote state and cut at the first
    prose boundary that occurs OUTSIDE a quoted regex -- so we never truncate a
    pattern, and never feed prose to the shell."""
    q = query.strip()
    if not q.lower().startswith(("rg ", "grep ", "git grep")):
        return None
    quote = None
    out = []
    i = 0
    while i < len(q):
        c = q[i]
        if quote:
            out.append(c)
            if c == quote:
                quote = None
            i += 1
            continue
        if c in "'\"":
            quote = c
            out.append(c)
            i += 1
            continue
        # unquoted: these begin prose, not command
        rest = q[i:]
        if c == "(" or rest.startswith((";", "  ", ". ", " -- ", " then", " and ",
                                        " plus", " Then", " And ", " inspect")):
            break
        out.append(c)
        i += 1
    return "".join(out).strip().rstrip(";").strip()


def trigger_globs(lens):
    """Split trigger_paths into fnmatch globs (relative to repo root)."""
    raw = lens.get("trigger_paths", "")
    out = []
    for tok in re.split(r"[,\s]+", raw):
        tok = tok.strip().strip("`")
        if tok and ("/" in tok or "*" in tok):
            out.append(tok)
    return out


def cmd_list():
    print("audit lenses (%d):" % len(LENSES))
    for l in LENSES:
        surf = runnable_prefix(l.get("grep_or_query", ""))
        print("  %-30s %s" % (l["class_name"], "[runnable]" if surf else "[manual]"))


def cmd_show(name, run=False):
    l = BY_NAME.get(name)
    if not l:
        print("no such lens: %s (try 'list')" % name)
        return 2
    print("=" * 78)
    print("LENS: %s" % name)
    print("=" * 78)
    print("\n[audit prompt]\n%s" % l["audit_prompt"])
    print("\n[variant analysis -- the fix-time sweep]\n%s" % l["variant_analysis"])
    print("\n[trigger paths -- re-run at diff time when these change]\n%s"
          % l["trigger_paths"])
    print("\n[surfacer]\n%s" % l["grep_or_query"])
    if run:
        surf = runnable_prefix(l["grep_or_query"])
        if not surf:
            print("\n(no mechanical surfacer for this class -- audit is manual)")
            return 0
        surf = to_grep(surf)
        print("\n[running surfacer]\n$ %s\n" % surf)
        try:
            p = subprocess.run(surf, shell=True, cwd=ROOT, timeout=60,
                               capture_output=True, text=True)
            out = p.stdout.strip()
            n = len(out.splitlines()) if out else 0
            print(out if out else "(no candidate sites)")
            print("\n-> %d candidate site(s) to audit with the prompt above." % n)
        except Exception as e:
            print("surfacer failed: %s" % e)
    return 0


def cmd_changed(files):
    """Map changed files to the lenses whose trigger paths match -> the diff-time
    triggering set."""
    rels = [os.path.relpath(os.path.abspath(f), ROOT) for f in files]
    hits = {}
    for l in LENSES:
        globs = trigger_globs(l)
        matched = [r for r in rels
                   if any(fnmatch.fnmatch(r, g) or fnmatch.fnmatch(r, "*" + g)
                          or g.rstrip("*") in r for g in globs)]
        if matched:
            hits[l["class_name"]] = matched
    if not hits:
        print("no lens trigger paths match the changed files.")
        return 0
    print("re-run these lenses for the changed files:")
    for name, matched in hits.items():
        print("  %-30s (matched %s)" % (name, ", ".join(sorted(set(matched))[:3])))
    print("\nrun each with:  tools/audit/run_lens.py <class> --run")
    return 0


def main(argv):
    if not argv or argv[0] == "list":
        cmd_list()
        return 0
    if argv[0] == "--changed":
        return cmd_changed(argv[1:])
    if argv[0] == "--audit-brief":
        l = BY_NAME.get(argv[1] if len(argv) > 1 else "")
        if not l:
            print("no such lens")
            return 2
        print(l["audit_prompt"])
        return 0
    name = argv[0]
    run = "--run" in argv[1:]
    return cmd_show(name, run=run)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
