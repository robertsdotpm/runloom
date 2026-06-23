"""big_100 / 87 -- MapReduce toy engine.

Text files are generated with known word frequencies.  Each job maps a file
(counting words locally) and reduces the partial counts into a shared global
table under a lock.  The map result for a file must match the file's known
counts, and the running global table must stay internally consistent.

Stresses: scheduler + locked reduce + CPU/I-O mix, queues of work.
"""
import os
import threading

import harness
import runloom

VOCAB = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"]


def gen_file(path, nwords, seedrng):
    counts = {}
    with open(path, "w") as f:
        line = []
        for _ in range(nwords):
            w = VOCAB[seedrng.randrange(len(VOCAB))]
            counts[w] = counts.get(w, 0) + 1
            line.append(w)
            if len(line) == 20:
                f.write(" ".join(line) + "\n")
                line = []
        if line:
            f.write(" ".join(line) + "\n")
    return counts


def setup(H):
    import random
    base = H.make_tmpdir("big100_mr_")
    seedrng = random.Random(H.seed)
    files = {}
    for k in range(12):
        path = os.path.join(base, "doc{0}.txt".format(k))
        counts = gen_file(path, 1000 + k * 200, seedrng)
        files[k] = (path, counts)
    H.state = {"files": files, "global": {}, "lock": threading.Lock()}


def map_file(path):
    counts = {}
    with open(path, "r") as f:
        for line in f:
            for w in line.split():
                counts[w] = counts.get(w, 0) + 1
    return counts


def worker(H, wid, rng, state):
    files = state["files"]
    glob = state["global"]
    lock = state["lock"]
    while H.running():
        k = rng.randrange(len(files))
        path, expected = files[k]
        mapped = map_file(path)
        # Map result must equal the file's known word counts.
        if not H.check(mapped == expected,
                       "map result wrong for doc{0} wid={1}".format(k, wid)):
            return
        # Reduce into the shared global table.
        with lock:
            for w, c in mapped.items():
                glob[w] = glob.get(w, 0) + c
        H.op(wid, sum(mapped.values()))
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    # The global table's total must equal the sum of every reduce contribution
    # (which is tracked as ops).  Compare the recorded total words to the table.
    total_in_table = sum(H.state["global"].values())
    H.check(total_in_table == H.total_ops(),
            "reduce lost counts: table total {0} != reduced ops {1}".format(
                total_in_table, H.total_ops()))
    H.log("global_terms={0} total_words_reduced={1}".format(
        len(H.state["global"]), total_in_table))


if __name__ == "__main__":
    harness.main("p87_mapreduce", body, setup=setup, post=post,
                 default_funcs=2000,
                 describe="map files, reduce into a shared table, verify counts")
