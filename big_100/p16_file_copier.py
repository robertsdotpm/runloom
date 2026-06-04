"""big_100 / 16 -- concurrent file copier.

Tens of thousands of goroutines each write a source file of random bytes,
copy it to a destination file, and verify the copy's SHA-256 matches the
source, then delete both -- forever.  All file I/O is offloaded by the monkey
layer to the scheduler's blocking-worker pool.

Stresses: open/read/write/close, fd churn, the blocking-offload path.
"""
import hashlib
import os

import harness


def setup(H):
    base = H.make_tmpdir("big100_copy_")
    H.state = {"base": base}
    for sub in ("src", "dst"):
        os.makedirs(os.path.join(base, sub), exist_ok=True)


def worker(H, wid, rng, state):
    base = state["base"]
    src = os.path.join(base, "src", "f{0}".format(wid))
    dst = os.path.join(base, "dst", "f{0}".format(wid))
    H.sleep(rng.random() * 0.5)
    while H.running():
        try:
            data = rng.randbytes(rng.randint(64, 65536))
            digest = hashlib.sha256(data).hexdigest()
            with open(src, "wb") as f:
                f.write(data)
            # Copy in chunks (read source -> write dest).
            with open(src, "rb") as fin, open(dst, "wb") as fout:
                while True:
                    chunk = fin.read(8192)
                    if not chunk:
                        break
                    fout.write(chunk)
            with open(dst, "rb") as f:
                got = f.read()
            if not H.check(hashlib.sha256(got).hexdigest() == digest,
                           "copy hash mismatch wid={0} ({1} bytes)".format(
                               wid, len(got))):
                return
            H.op(wid)
            H.task_done(wid)
            os.remove(dst)
        except OSError as e:
            if not H.running():
                break
            H.fail("file error wid={0}: {1}".format(wid, e))
            return


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p16_file_copier", body, setup=setup, default_funcs=8000,
                 describe="concurrent file copy + sha256 verify, fd churn")
