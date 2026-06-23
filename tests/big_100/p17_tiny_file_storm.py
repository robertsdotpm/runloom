"""big_100 / 17 -- tiny file storm.

A relentless storm of tiny-file lifecycles: create, write, stat, rename, read
back, delete -- repeated forever across tens of thousands of goroutines, each
in its own subdirectory.  Verifies the content survives the write/rename/read
round-trip and the stat size is right.

Stresses: blocking filesystem syscalls (open/stat/rename/unlink), path
handling, error paths -- all through the monkey offload.
"""
import os

import harness


def setup(H):
    base = H.make_tmpdir("big100_tiny_")
    H.state = {"base": base}


def worker(H, wid, rng, state):
    d = os.path.join(state["base"], "w{0}".format(wid))
    os.makedirs(d, exist_ok=True)
    H.sleep(rng.random() * 0.5)
    i = 0
    while H.running():
        i += 1
        a = os.path.join(d, "a{0}".format(i & 7))
        b = os.path.join(d, "b{0}".format(i & 7))
        try:
            payload = "{0}:{1}".format(wid, i).encode()
            with open(a, "wb") as f:
                f.write(payload)
            st = os.stat(a)
            if not H.check(st.st_size == len(payload),
                           "stat size {0}!={1} wid={2}".format(
                               st.st_size, len(payload), wid)):
                return
            os.rename(a, b)
            with open(b, "rb") as f:
                got = f.read()
            if not H.check(got == payload,
                           "tiny-file content mismatch wid={0}".format(wid)):
                return
            os.remove(b)
            H.op(wid)
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("tiny-file error wid={0}: {1}".format(wid, e))
            return


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p17_tiny_file_storm", body, setup=setup, default_funcs=10000,
                 describe="create/write/stat/rename/read/delete tiny files")
