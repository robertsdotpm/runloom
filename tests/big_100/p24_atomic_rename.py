"""big_100 / 24 -- atomic rename test.

A fixed set of "slot" files.  Writer goroutines repeatedly build a payload with
a trailing checksum, write it to a temp file, and os.replace() it onto the slot
(atomic on POSIX).  Reader goroutines open slots and verify the checksum -- if
os.replace were not atomic a reader could observe a torn half-written file, and
the checksum would fail.

Stresses: filesystem consistency, race timing between replace and open.
"""
import hashlib
import os

import harness

NSLOTS = 64


def make_payload(rng):
    body = rng.randbytes(rng.randint(64, 4096))
    return body + hashlib.sha256(body).digest()


def verify(blob):
    if len(blob) < 32:
        return False
    body, digest = blob[:-32], blob[-32:]
    return hashlib.sha256(body).digest() == digest


def setup(H):
    base = H.make_tmpdir("big100_atomic_")
    slots = []
    import random
    seedrng = random.Random(H.seed)
    for i in range(NSLOTS):
        p = os.path.join(base, "slot{0}".format(i))
        with open(p, "wb") as f:
            f.write(make_payload(seedrng))
        slots.append(p)
    H.state = {"slots": slots, "base": base}


def writer(H, wid, rng, state):
    slots = state["slots"]
    base = state["base"]
    tmp = os.path.join(base, "tmp.w{0}".format(wid))
    H.sleep(rng.random() * 0.3)
    while H.running():
        slot = rng.choice(slots)
        try:
            with open(tmp, "wb") as f:
                f.write(make_payload(rng))
            os.replace(tmp, slot)       # atomic
            H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break


def reader(H, wid, rng, state):
    slots = state["slots"]
    H.sleep(rng.random() * 0.3)
    while H.running():
        slot = rng.choice(slots)
        try:
            with open(slot, "rb") as f:
                blob = f.read()
        except OSError:
            continue
        if not H.check(verify(blob),
                       "TORN read on {0}: {1} bytes (atomic rename violated)"
                       .format(os.path.basename(slot), len(blob))):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    writers = max(NSLOTS, H.funcs // 4)
    readers = H.funcs - writers
    H.run_pool(writers, writer, H.state)
    H.run_pool(readers, reader, H.state)


if __name__ == "__main__":
    harness.main("p24_atomic_rename", body, setup=setup, default_funcs=8000,
                 describe="atomic os.replace; readers never see a torn file")
