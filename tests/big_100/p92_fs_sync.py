"""big_100 / 92 -- filesystem sync tool.

Two directories, src and dst.  Writer goroutines atomically rewrite slot files
in src (content + trailing checksum, via tmp+rename); syncer goroutines poll
src, and when a slot changed they copy it to dst (again atomically) and read it
back.  Every read of src OR dst must be checksum-consistent -- atomic rename
must never expose a torn file even while writers churn.

Stresses: stat/poll loops, concurrent file churn, atomicity races.
"""
import hashlib
import os

import harness

NSLOTS = 64


def make_payload(rng):
    body = rng.randbytes(rng.randint(64, 4096))
    return body + hashlib.sha256(body).digest()


def valid(blob):
    return len(blob) >= 32 and hashlib.sha256(blob[:-32]).digest() == blob[-32:]


def setup(H):
    base = H.make_tmpdir("big100_fssync_")
    src = os.path.join(base, "src")
    dst = os.path.join(base, "dst")
    os.makedirs(src)
    os.makedirs(dst)
    import random
    sr = random.Random(H.seed)
    for i in range(NSLOTS):
        with open(os.path.join(src, "s{0}".format(i)), "wb") as f:
            f.write(make_payload(sr))
    H.state = {"src": src, "dst": dst, "base": base}


def writer(H, wid, rng, state):
    src = state["src"]
    tmp = os.path.join(state["base"], "wtmp{0}".format(wid))
    H.sleep(rng.random() * 0.3)
    while H.running():
        slot = os.path.join(src, "s{0}".format(rng.randrange(NSLOTS)))
        try:
            with open(tmp, "wb") as f:
                f.write(make_payload(rng))
            os.replace(tmp, slot)
            H.op(wid)
        except OSError:
            if not H.running():
                break


def syncer(H, wid, rng, state):
    src = state["src"]
    dst = state["dst"]
    tmp = os.path.join(state["base"], "stmp{0}".format(wid))
    H.sleep(rng.random() * 0.3)
    while H.running():
        n = rng.randrange(NSLOTS)
        try:
            with open(os.path.join(src, "s{0}".format(n)), "rb") as f:
                blob = f.read()
        except OSError:
            continue
        if not H.check(valid(blob),
                       "torn read of src/s{0} ({1} bytes) wid={2}".format(
                           n, len(blob), wid)):
            return
        try:
            with open(tmp, "wb") as f:
                f.write(blob)
            os.replace(tmp, os.path.join(dst, "s{0}".format(n)))
            with open(os.path.join(dst, "s{0}".format(n)), "rb") as f:
                back = f.read()
        except OSError:
            continue
        if not H.check(valid(back),
                       "torn read of dst/s{0} wid={1}".format(n, wid)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    writers = max(2, H.funcs // 4)
    H.run_pool(writers, writer, H.state)
    H.run_pool(H.funcs - writers, syncer, H.state)


if __name__ == "__main__":
    harness.main("p92_fs_sync", body, setup=setup, default_funcs=1500,
                 describe="poll-and-copy two dirs; atomic writes never tear")
