#!/usr/bin/env python3
"""Triage inbox (docs/dev/RELIABILITY_PROGRAM.md R4).

The nightly duty-cycle (tools/soak/duty_cycle.sh) and the matrix
(tools/soak/matrix.sh) drop every finding -- a hang report, a sanitizer hit, a
slope-fail, a fuzz repro -- here as ONE dated entry in docs/dev/soak/INBOX.md,
keyed by a dedup signature so the same bug firing every night is one entry with
a count, not 30.  The next working session starts by emptying the inbox.

Integrates the flake ledger (tools/flake_ledger.py): a signature already marked
a known flake is recorded but flagged [known-flake] so it does not read as a
fresh regression.

Usage:
  inbox.py --add --kind hang --title "..." --artifact PATH [--sig SIG] [--date YYYY-MM-DD]
  inbox.py --count            # number of OPEN (unresolved) entries -> stdout
"""
import argparse
import hashlib
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
INBOX = os.path.join(ROOT, "docs", "dev", "soak", "INBOX.md")

_HEADER = """# Triage inbox

Every finding from the nightly duty-cycle (`tools/soak/duty_cycle.sh`) and the
soak matrix lands here as one dated, deduplicated entry.  **The next working
session starts by emptying this inbox**: for each OPEN entry, reproduce →
root-cause → fix → mark it resolved (change `- [ ]` to `- [x]` and add the
commit), or classify it a known flake (`tools/flake_ledger.py`).

Signature dedup: repeats of one finding bump its count instead of adding rows,
so distinct bugs stand out.  `[known-flake]` entries are recorded but are not
fresh regressions.

## Open + recent

"""


def _entries(text):
    """Split the body into (signature -> raw entry block) for dedup."""
    blocks = {}
    for m in re.finditer(r"- \[[ x]\] .*?\(sig:([0-9a-f]+)\).*?(?=\n- \[|\Z)",
                         text, re.S):
        blocks[m.group(1)] = m.group(0)
    return blocks


def add(args):
    sig = args.sig
    if not sig:
        sig = hashlib.sha1(
            ("%s|%s" % (args.kind, args.title)).encode()).hexdigest()[:12]
    date = args.date or "unknown-date"
    known = _is_known_flake(sig, args.title)
    tag = " `[known-flake]`" if known else ""

    text = ""
    if os.path.exists(INBOX):
        with open(INBOX) as f:
            text = f.read()
    if not text.strip():
        text = _HEADER

    blocks = _entries(text)
    if sig in blocks:
        # bump the count on the existing entry; refresh the trailing "last seen".
        block = blocks[sig]
        cm = re.search(r"\(x(\d+)\)", block)
        n = (int(cm.group(1)) + 1) if cm else 2
        newblock = re.sub(r"(\(sig:%s\))( \(x\d+\))?" % sig,
                          r"\1 (x%d)" % n, block, count=1)
        # strip any prior "· last seen ..." then append a fresh one before the
        # entry's trailing newline (keeps the block on two clean lines).
        newblock = re.sub(r" · last seen [0-9-]+", "", newblock)
        newblock = newblock.rstrip("\n") + " · last seen %s\n" % date
        text = text.replace(block, newblock)
    else:
        art = (os.path.relpath(args.artifact, ROOT)
               if args.artifact and args.artifact.startswith(ROOT) else
               (args.artifact or "—"))
        entry = ("- [ ] **%s** — %s%s  \n"
                 "  (sig:%s) first seen %s · artifact: `%s`\n"
                 % (args.kind, args.title, tag, sig, date, art))
        # insert right after the "## Open + recent" marker
        marker = "## Open + recent\n\n"
        idx = text.find(marker)
        if idx >= 0:
            ins = idx + len(marker)
            text = text[:ins] + entry + text[ins:]
        else:
            text = text + "\n" + entry
    os.makedirs(os.path.dirname(INBOX), exist_ok=True)
    with open(INBOX, "w") as f:
        f.write(text)
    print("inbox += [%s] %s (sig:%s)%s" % (args.kind, args.title, sig, tag))


def _is_known_flake(sig, title):
    """Best-effort: consult the flake ledger if present.  Never fails."""
    try:
        led = os.path.join(ROOT, "tools", "flake_ledger.py")
        if not os.path.exists(led):
            return False
        # flake_ledger stores a JSON ledger; check for the sig/title there.
        import json
        for cand in (os.path.join(ROOT, ".flake_ledger.json"),
                     os.path.join(ROOT, "tools", "flake_ledger.json")):
            if os.path.exists(cand):
                data = json.load(open(cand))
                blob = json.dumps(data)
                if sig in blob or (title and title in blob):
                    return True
    except Exception:
        pass
    return False


def count():
    if not os.path.exists(INBOX):
        print(0)
        return
    with open(INBOX) as f:
        text = f.read()
    print(len(re.findall(r"^- \[ \] ", text, re.M)))


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--add", action="store_true")
    ap.add_argument("--count", action="store_true")
    ap.add_argument("--kind", default="finding")
    ap.add_argument("--title", default="")
    ap.add_argument("--artifact", default="")
    ap.add_argument("--sig", default="")
    ap.add_argument("--date", default="")
    a = ap.parse_args(argv)
    if a.count:
        count()
    elif a.add:
        add(a)
    else:
        ap.error("give --add or --count")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
