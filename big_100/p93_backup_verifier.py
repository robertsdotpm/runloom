"""big_100 / 93 -- backup verifier.

A directory tree is generated with a manifest of every file's SHA-256.  Worker
goroutines walk subtrees, hash each file and check it against the manifest, and
also gzip-compress the manifest and verify the round-trip.  A shared cancel
context aborts the whole verification the instant any worker sees a hash
mismatch (none should occur, since the tree is static).

Stresses: CPU (hashing) + I/O (reads) + compression + cooperative cancellation.
"""
import gzip
import hashlib
import json
import os

import harness
import cancelutil

BRANCH = 4
DEPTH = 3
FILES_PER_LEAF = 4


def file_bytes(path):
    return hashlib.sha256(path.encode()).digest() * 8


def build(d, depth, manifest):
    os.makedirs(d, exist_ok=True)
    if depth == 0:
        for i in range(FILES_PER_LEAF):
            p = os.path.join(d, "f{0}".format(i))
            data = file_bytes(p)
            with open(p, "wb") as f:
                f.write(data)
            manifest[p] = hashlib.sha256(data).hexdigest()
        return
    for b in range(BRANCH):
        build(os.path.join(d, "d{0}".format(b)), depth - 1, manifest)


def setup(H):
    base = H.make_tmpdir("big100_backup_")
    root = os.path.join(base, "root")
    manifest = {}
    build(root, DEPTH, manifest)
    blob = json.dumps(manifest).encode()
    starts = [root] + [os.path.join(root, "d{0}".format(b))
                       for b in range(BRANCH)]
    ctx, cancel = cancelutil.WithCancel(cancelutil.Background())
    H.state = {"manifest": manifest, "starts": starts,
               "manifest_blob": blob, "ctx": ctx, "cancel": cancel}


def verify_subtree(H, wid, root, manifest, ctx):
    stack = [root]
    n = 0
    while stack:
        if not H.running() or ctx.err() is not None:
            return n
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry in it:
                    if entry.is_dir():
                        stack.append(entry.path)
                    elif entry.is_file():
                        with open(entry.path, "rb") as f:
                            data = f.read()
                        digest = hashlib.sha256(data).hexdigest()
                        if digest != manifest.get(entry.path):
                            H.state["cancel"]()         # abort everyone
                            H.fail("backup mismatch at {0}".format(entry.path))
                            return -1
                        n += 1
                        H.op(wid)
        except OSError:
            if not H.running():
                return n
    return n


def worker(H, wid, rng, state):
    manifest = state["manifest"]
    blob = state["manifest_blob"]
    ctx = state["ctx"]
    while H.running() and ctx.err() is None:
        root = rng.choice(state["starts"])
        if verify_subtree(H, wid, root, manifest, ctx) < 0:
            return
        # Compress + round-trip the manifest.
        comp = gzip.compress(blob)
        if not H.check(gzip.decompress(comp) == blob,
                       "manifest gzip round-trip failed wid={0}".format(wid)):
            return
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p93_backup_verifier", body, setup=setup, default_funcs=2000,
                 describe="walk+hash tree, compress manifest, cancel on mismatch")
