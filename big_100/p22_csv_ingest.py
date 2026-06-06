"""big_100 / 22 -- CSV ingest pipeline.

A set of CSV files is generated with a known column sum.  Tens of thousands of
goroutines repeatedly parse a random file and aggregate the value column,
verifying the running total matches the precomputed expectation.

Stresses: file I/O plus CPU-bound parsing, mixed on the M:N hubs.
"""
import os

import harness


def setup(H):
    base = H.make_tmpdir("big100_csv_")
    files = []
    expected = {}
    for k in range(8):
        p = os.path.join(base, "data{0}.csv".format(k))
        n = 2000 + k * 750
        total = 0
        with open(p, "w") as f:
            f.write("id,value\n")
            for i in range(n):
                v = (i * (k + 1) + 7) % 100
                total += v
                f.write("{0},{1}\n".format(i, v))
        files.append(p)
        expected[p] = total
    H.state = {"files": files, "expected": expected}


def worker(H, wid, rng, state):
    files = state["files"]
    expected = state["expected"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        p = rng.choice(files)
        total = 0
        rows = 0
        try:
            with open(p, "r") as f:
                header = f.readline()
                if not header.startswith("id,value"):
                    H.fail("bad CSV header in {0}".format(p))
                    return
                for line in f:
                    if not H.running():
                        break
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    _id, _comma, val = line.partition(",")
                    total += int(val)
                    rows += 1
                    if rows % 512 == 0:
                        H.op(wid)
        except OSError:
            if not H.running():
                break
            continue
        if not H.check(total == expected[p],
                       "csv sum mismatch {0}: {1} != {2}".format(
                           p, total, expected[p])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p22_csv_ingest", body, setup=setup, default_funcs=8000,
                 describe="parse CSV files and verify aggregated column sums")
