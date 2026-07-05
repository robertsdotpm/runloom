#!/usr/bin/env python3
"""flatten.py -- inline a TU's local .inc implementation fragments into ONE .c,
so dredd (which only mutates the primary source file, never #included fragments)
can schemata-mutate the code that actually lives in the fragments.

runloom's C core is thin .c TUs that #include large *.c.inc / *.inc fragments;
dredd leaves those fragments untouched.  Flattening produces a single physical
file whose text IS the fragments, so every mutable expression is now in the
primary file dredd rewrites.  Only local quoted "*.inc" / "*.c.inc" includes are
inlined; <system> and "*.h" headers are left as #includes (they are shared
declarations, not the code under test, and must NOT be mutated or duplicated).

Emits, alongside <out>.c:
  <out>.map.json  -- [{"flat_lo":L, "flat_hi":H, "src":"path", "src_off":N}]
                     so a mutant reported at flat line X maps back to the real
                     src file:line (the survivors report reads this).

Nesting: runloom's fragments do not include other fragments (verified), so a
single pass suffices; a fragment that DID nest would just not get its inner
fragment inlined -- flagged, not silently wrong.

Usage:  flatten.py <tu.c> <out.c>
"""
import json
import os
import re
import sys

INC_RE = re.compile(r'^\s*#\s*include\s*"([^"]+\.(?:c\.inc|inc))"\s*$')


def flatten(tu_path, out_path):
    root = os.path.dirname(tu_path)
    out_lines = []
    spans = []          # provenance of each inlined block
    flat_n = 0          # 1-based line counter in the flattened output

    def emit(text, src, src_off):
        nonlocal flat_n
        lines = text.splitlines(keepends=True)
        if not lines:
            return
        lo = flat_n + 1
        out_lines.extend(lines)
        flat_n += len(lines)
        spans.append({"flat_lo": lo, "flat_hi": flat_n, "src": src,
                      "src_off": src_off})

    with open(tu_path) as f:
        tu_lines = f.readlines()

    run_start = 0        # start of the current run of TU-own lines
    for i, line in enumerate(tu_lines):
        m = INC_RE.match(line)
        if not m:
            continue
        inc_rel = m.group(1)
        inc_path = os.path.join(root, inc_rel)
        if not os.path.exists(inc_path):
            continue     # not a local fragment we own -> leave the #include
        # flush the TU-own run before this include (src line = run_start+1)
        if i > run_start:
            emit("".join(tu_lines[run_start:i]), tu_path, run_start + 1)
        # inline the fragment.  NO #line directive: it would set clang's presumed
        # location back into the .inc, and dredd may then treat that AST node as
        # "not in the primary file" and skip mutating it -- the exact bug we are
        # working around.  Provenance is tracked in map.json instead.
        with open(inc_path) as g:
            frag = g.read()
        if INC_RE.search(frag):
            sys.stderr.write("WARN: %s nests a fragment include -- not "
                             "inlined recursively\n" % inc_rel)
        emit(frag, inc_path, 1)
        run_start = i + 1
    # trailing TU-own run
    if run_start < len(tu_lines):
        emit("".join(tu_lines[run_start:]), tu_path, run_start + 1)

    with open(out_path, "w") as f:
        f.writelines(out_lines)
    map_path = os.path.splitext(out_path)[0] + ".map.json"
    with open(map_path, "w") as f:
        json.dump(spans, f)
    inlined = sum(1 for s in spans if s["src"] != tu_path)
    print("flattened %s -> %s (%d lines, %d fragment blocks inlined)"
          % (tu_path, out_path, flat_n, inlined))
    print("provenance map -> %s" % map_path)


def map_flat_line(spans, flat_line):
    """flat line -> (src_path, src_line).  Used by the survivors report."""
    for s in spans:
        if s["flat_lo"] <= flat_line <= s["flat_hi"]:
            return s["src"], s["src_off"] + (flat_line - s["flat_lo"])
    return None, None


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: flatten.py <tu.c> <out.c>")
    flatten(sys.argv[1], sys.argv[2])
