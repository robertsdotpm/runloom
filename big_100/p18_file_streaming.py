"""big_100 / 18 -- large file streaming pipeline.

A handful of large files are filled with content that is a deterministic
function of byte offset.  Many reader goroutines seek to random offsets and
read chunks, then hand (offset, chunk) to a pool of verifier goroutines over a
channel; the verifiers recompute the expected bytes from the offset and check
them.  Producers/consumers decoupled by a bounded queue.

Stresses: file reads at offset, channels + backpressure, scheduler fairness,
the reader/verifier split across hubs.
"""
import os

import harness
import runloom

NFILES = 8
FILESIZE = 2 << 20      # 2 MiB each
CHUNK = 4096
PERIOD = 251            # prime -> content is a simple tiled byte ramp


def expected_chunk(offset, n):
    return bytes((offset + i) % PERIOD for i in range(n))


def setup(H):
    base = H.make_tmpdir("big100_stream_")
    pattern = bytes(range(PERIOD))
    tile = (pattern * (FILESIZE // PERIOD + 2))[:FILESIZE]   # fast fill
    paths = []
    for k in range(NFILES):
        p = os.path.join(base, "big{0}.dat".format(k))
        with open(p, "wb") as f:
            f.write(tile)
        paths.append(p)
    H.state = {"paths": paths, "queue": runloom.Chan(4096)}


def reader(H, wid, rng, state):
    paths = state["paths"]
    queue = state["queue"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        path = rng.choice(paths)
        offset = rng.randrange(0, FILESIZE - CHUNK)
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                chunk = f.read(CHUNK)
        except OSError:
            if not H.running():
                break
            continue
        queue.send((offset, chunk))
        H.op(wid)


def verifier(H, wid, rng, state):
    # Poll the queue non-blockingly so we exit on our own once the run is over
    # and the queue is drained -- no channel close (and thus no send-on-closed
    # race against a reader parked in queue.send).
    queue = state["queue"]
    while True:
        got = queue.try_recv()          # None if empty, else (value, ok)
        if got is None:
            if not H.running():
                break
            runloom.sleep(0.002)
            continue
        item, ok = got
        if not ok:
            break
        offset, chunk = item
        if not H.check(chunk == expected_chunk(offset, len(chunk)),
                       "stream chunk mismatch at offset {0}".format(offset)):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    nverif = max(2, H.hubs * 2)
    H.run_pool(nverif, verifier, H.state)
    nread = max(1, H.funcs - nverif)
    H.run_pool(nread, reader, H.state)


if __name__ == "__main__":
    harness.main("p18_file_streaming", body, setup=setup, default_funcs=8000,
                 describe="read random file offsets, verify chunks via a queue")
