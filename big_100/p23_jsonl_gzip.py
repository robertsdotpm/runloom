"""big_100 / 23 -- JSONL compression farm.

Goroutines read JSONL files, gzip-compress the bytes, write the .gz out, read
it back, decompress, and verify the round-trip is byte-identical (and that the
JSON still parses).  gzip/zlib are GIL-releasing C calls (and the monkey
`heavy` layer may auto-offload large buffers).

Stresses: blocking file I/O, zlib C calls, buffer lifetime, CPU/I-O mix.
"""
import gzip
import json
import os

import harness


def setup(H):
    base = H.make_tmpdir("big100_jsonl_")
    files = []
    for k in range(8):
        p = os.path.join(base, "data{0}.jsonl".format(k))
        with open(p, "w") as f:
            for i in range(500 + k * 100):
                obj = {"id": i, "k": k, "name": "row-{0}".format(i),
                       "vals": [i, i * 2, i * 3]}
                f.write(json.dumps(obj) + "\n")
        files.append(p)
    H.state = {"base": base, "files": files}


def worker(H, wid, rng, state):
    files = state["files"]
    outdir = os.path.join(state["base"], "out")
    os.makedirs(outdir, exist_ok=True)
    H.sleep(rng.random() * 0.5)
    while H.running():
        src = rng.choice(files)
        gzpath = os.path.join(outdir, "w{0}.jsonl.gz".format(wid))
        try:
            with open(src, "rb") as f:
                raw = f.read()
            comp = gzip.compress(raw, compresslevel=6)
            with open(gzpath, "wb") as f:
                f.write(comp)
            with open(gzpath, "rb") as f:
                back = gzip.decompress(f.read())
            if not H.check(back == raw,
                           "gzip round-trip mismatch wid={0}".format(wid)):
                return
            # Spot-check that the decompressed content is still valid JSONL.
            first = back.split(b"\n", 1)[0]
            json.loads(first)
            H.op(wid)
            H.task_done(wid)
        except (OSError, ValueError) as e:
            if not H.running():
                break
            H.fail("jsonl error wid={0}: {1}".format(wid, e))
            return


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p23_jsonl_gzip", body, setup=setup, default_funcs=6000,
                 describe="read JSONL, gzip round-trip, verify byte-identity")
