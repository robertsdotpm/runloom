"""big_100 / 19 -- directory tree crawler.

A deep nested directory tree is generated once, every file holding content
derived deterministically from its path.  Many goroutines crawl random
subtrees concurrently with os.scandir, read each file, and verify its checksum
against what the path implies.

Stresses: os.scandir, recursion, stat, file reads, scheduler fairness.
"""
import hashlib
import os

import harness

BRANCH = 4
DEPTH = 4
FILES_PER_LEAF = 3


def expected(path):
    return hashlib.sha256(path.encode()).digest() * 6     # ~192 bytes


def build(d, depth):
    os.makedirs(d, exist_ok=True)
    if depth == 0:
        for i in range(FILES_PER_LEAF):
            p = os.path.join(d, "f{0}".format(i))
            with open(p, "wb") as f:
                f.write(expected(p))
        return
    for b in range(BRANCH):
        build(os.path.join(d, "d{0}".format(b)), depth - 1)


def setup(H):
    base = H.make_tmpdir("big100_tree_")
    root = os.path.join(base, "root")
    build(root, DEPTH)
    # Collect the mid-level subtree roots crawlers can start from.
    starts = [root]
    for b in range(BRANCH):
        starts.append(os.path.join(root, "d{0}".format(b)))
    H.state = {"starts": starts}


def crawl(H, wid, root):
    count = 0
    stack = [root]
    while stack:
        if not H.running():
            return count        # abort promptly at teardown (drain the pool)
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    if entry.is_dir():
                        stack.append(entry.path)
                    elif entry.is_file():
                        with open(entry.path, "rb") as f:
                            data = f.read()
                        if not H.check(data == expected(entry.path),
                                       "tree checksum mismatch: {0}".format(
                                           entry.path)):
                            return -1
                        count += 1
                        H.op(wid)
        except OSError:
            if not H.running():
                return count
    return count


def worker(H, wid, rng, state):
    H.sleep(rng.random() * 0.5)
    while H.running():
        root = rng.choice(state["starts"])
        if crawl(H, wid, root) < 0:
            return
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p19_tree_crawler", body, setup=setup, default_funcs=6000,
                 describe="concurrent directory-tree crawl + per-file checksum")
