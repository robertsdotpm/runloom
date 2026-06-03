"""Turn a perf.data into folded stacks + a self-contained SVG flame graph.

No dependency on Brendan Gregg's FlameGraph perl scripts (not installed
here) -- we parse `perf script` ourselves and render a stdlib-only SVG.
The `.folded` output is the universal interchange format: it also loads
directly into https://speedscope.app or flamegraph.pl later.

Usage:
    PYTHONPATH=src python3 -m bench.profile.flamegraph <perf.data> --out NAME
    # writes NAME.folded and NAME.svg

We sample with `perf record -e task-clock` (software timer) because this
VM exposes no hardware PMU; widths are therefore on-CPU wall-time share,
which is exactly what a flame graph wants.
"""
import argparse
import colorsys
import html
import os
import subprocess
import sys


def perf_script(data_path):
    out = subprocess.check_output(["perf", "script", "-i", data_path],
                                  stderr=subprocess.DEVNULL)
    return out.decode("utf-8", "replace")


def parse_symbol(line):
    """A perf-script stack line looks like:
        <addr> symbol+0xoff (/path/to/module)
    Return just the symbol, stripped of offset and module."""
    s = line.strip()
    if not s:
        return None
    parts = s.split(None, 1)
    if len(parts) < 2:
        return None
    sym = parts[1]
    # drop trailing "(module)"
    cut = sym.rfind(" (")
    if cut != -1:
        sym = sym[:cut]
    # drop "+0xoffset"
    plus = sym.rfind("+0x")
    if plus != -1:
        sym = sym[:plus]
    return sym.strip() or "[unknown]"


def fold(script_text):
    """perf script text -> {';'-joined root->leaf stack: sample count}."""
    folded = {}
    stack = []
    have_header = False
    for line in script_text.splitlines():
        if not line.strip():
            if stack:
                key = ";".join(reversed(stack))  # root first
                folded[key] = folded.get(key, 0) + 1
            stack = []
            have_header = False
            continue
        if not line[0].isspace():
            # sample header line ("python3 1234 12.3: task-clock:")
            have_header = True
            stack = []
            continue
        if have_header:
            sym = parse_symbol(line)
            if sym:
                stack.append(sym)
    if stack:
        key = ";".join(reversed(stack))
        folded[key] = folded.get(key, 0) + 1
    return folded


def write_folded(folded, path):
    with open(path, "w") as f:
        for key in sorted(folded):
            f.write("%s %d\n" % (key, folded[key]))


# --------------------------------------------------------------------
# Minimal flame-graph SVG renderer (icicle, root on top).
# --------------------------------------------------------------------
class Node:
    __slots__ = ("name", "value", "children")

    def __init__(self, name):
        self.name = name
        self.value = 0
        self.children = {}


def build_tree(folded):
    root = Node("all")
    for key, cnt in folded.items():
        root.value += cnt
        node = root
        for frame in key.split(";"):
            child = node.children.get(frame)
            if child is None:
                child = Node(frame)
                node.children[frame] = child
            child.value += cnt
            node = child
    return root


def color_for(name):
    # warm palette, hue jittered by name hash (classic flame look)
    h = (hash(name) & 0xffff) / 65535.0
    hue = 0.0 + 0.12 * h            # red->orange
    r, g, b = colorsys.hsv_to_rgb(hue, 0.55 + 0.25 * h, 0.95)
    return "#%02x%02x%02x" % (int(r * 255), int(g * 255), int(b * 255))


def render_svg(root, path, width=1600, frame_h=16):
    total = root.value or 1
    rows = []  # (x, depth, w, name, value)
    maxdepth = [0]

    def layout2(node, x, depth):
        if node is not root:
            rows.append((x, depth, width * node.value / total,
                         node.name, node.value))
            maxdepth[0] = max(maxdepth[0], depth)
        cxx = x
        for child in sorted(node.children.values(), key=lambda c: -c.value):
            layout2(child, cxx, depth + (0 if node is root else 1))
            cxx += width * child.value / total

    layout2(root, 0, 0)
    height = (maxdepth[0] + 1) * frame_h + 40
    parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
             'font-family="monospace" font-size="11">' % (width, height)]
    parts.append('<rect width="%d" height="%d" fill="#f8f8f0"/>' % (width, height))
    parts.append('<text x="6" y="16" font-size="13">runloom flame graph '
                 '(%d samples, task-clock)</text>' % total)
    y0 = 24
    for x, depth, w, name, value in rows:
        if w < 0.4:
            continue
        y = y0 + depth * frame_h
        pct = 100.0 * value / total
        title = "%s  %.2f%% (%d)" % (name, pct, value)
        parts.append('<g><title>%s</title>'
                     '<rect x="%.2f" y="%d" width="%.2f" height="%d" '
                     'fill="%s" stroke="#fff" stroke-width="0.3"/>'
                     % (html.escape(title), x, y, max(w, 0.4), frame_h - 1,
                        color_for(name)))
        if w > 42:
            label = name if len(name) * 6.5 < w else name[:int(w / 6.5)]
            parts.append('<text x="%.2f" y="%d">%s</text>'
                         % (x + 2, y + frame_h - 4, html.escape(label)))
        parts.append('</g>')
    parts.append('</svg>')
    with open(path, "w") as f:
        f.write("\n".join(parts))


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("data", help="perf.data path")
    p.add_argument("--out", default="flame",
                   help="output basename (writes .folded and .svg)")
    args = p.parse_args(argv)

    folded = fold(perf_script(args.data))
    nstacks = sum(folded.values())
    write_folded(folded, args.out + ".folded")
    render_svg(build_tree(folded), args.out + ".svg")
    print("folded stacks: %s.folded  (%d samples, %d unique stacks)"
          % (args.out, nstacks, len(folded)))
    print("flame graph  : %s.svg" % args.out)


if __name__ == "__main__":
    main()
