"""big_100 / 223 -- io_uring per-hub loop backend + multishot recv echo.

Drives the OPT-IN per-hub io_uring event-loop backend (single-issuer /
single-reaper per hub, RUNLOOM_IOURING_LOOP=1) together with the Stage-3
MULTISHOT | BUFFER_SELECT recv that delivers each chunk into a per-hub
provided buffer ring (RUNLOOM_IOURING_MS=1).  This is an ENTIRELY alternative
event loop, created only when opted in (commit 298da80); no other big_100
program selects it, and fc443e2 just fixed a multishot single-op spurious-wake
re-park, so the regression surface is live and otherwise untested here.

The server is the built-in all-C echo (runloom_c.serve(host, port, None)) --
that handler runs runloom_io_c_echo, the only path that actually arms the
multishot persistent SQE + provided-buffer-ring recv on the owning hub's ring.
Connections are long-lived and hub-pinned: a woken echo fiber routes to its
hub-local FIFO, so its multishot stays armed on the hub it was opened on.

We force buffer-ring recycle/exhaustion by setting RUNLOOM_IOURING_MS_BUFS
small (default 16) so the ring of provided buffers must be returned and reused
many times over a connection's lifetime, exercising the recycle path and the
spurious-wake re-park.

Each client streams a deterministic, position-tagged byte stream (byte i ==
the low 8 bits of a per-conn linear-congruential sequence) in many back-to-back
chunks and reads back exactly that many bytes.  ORACLE (H.check): the bytes
echoed back are the EXACT contiguous tagged stream in order -- a lost or
duplicated multishot CQE shows up as a byte-value break, and a short/long
read shows up as a received-byte-count mismatch (received == sent per conn).
A control run with --ms 0 keeps the loop backend on but multishot off.

The availability guard SKIPs cleanly (exit 0) when io_uring isn't usable
(Linux < 5.1, no liburing, or non-Linux), so the program is always safe in
the sweep.

Stresses: Stresses: per-hub io_uring loop backend (single-issuer/reaper) and
multishot recv + provided-buffer-ring (BUFFER_SELECT) on long-lived hub-pinned
streaming echo conns; buffer-ring exhaustion/recycle and the spurious-wake
re-park path.
"""
import os
import sys

# The loop backend + multishot + buffer-ring count are read ONCE from the
# environment and cached in C (io_uring_l_loop.c.inc) at first use / hub init,
# so they MUST be set before runloom is imported and the hubs come up.  We
# default them ON here; the --ms arg (handled in add_args/setup) can turn the
# multishot path off for a control run, and RUNLOOM_IOURING_MS_BUFS sizes the
# provided buffer ring small to force recycle/exhaustion.
os.environ.setdefault("RUNLOOM_IOURING_LOOP", "1")
os.environ.setdefault("RUNLOOM_IOURING_MS", "1")
os.environ.setdefault("RUNLOOM_IOURING_MS_BUFS", "16")

import harness          # noqa: E402  (sets up sys.path + imports runloom_c)
import runloom_c        # noqa: E402
import netutil          # noqa: E402

recv_exact = netutil.recv_exact


def add_args(ap):
    ap.add_argument("--ms", type=int, default=1,
                    help="1 = multishot recv ON (RUNLOOM_IOURING_MS=1, the "
                         "Stage-3 MULTISHOT|BUFFER_SELECT path); 0 = loop "
                         "backend only, single-shot proactor recv (control "
                         "run).  Must be set before the hubs start.")
    ap.add_argument("--ms-bufs", type=int, default=16,
                    help="provided-buffer-ring count (RUNLOOM_IOURING_MS_BUFS). "
                         "Small (16) forces buffer-ring recycle/exhaustion.")
    ap.add_argument("--chunks", type=int, default=64,
                    help="back-to-back chunks streamed per connection per round")
    ap.add_argument("--chunk-max", type=int, default=200,
                    help="max bytes per streamed chunk (1..chunk-max)")


def tag_stream(seed, n):
    """Deterministic position-tagged byte stream of length n.

    byte i = low 8 bits of an LCG advanced i times from `seed`.  A contiguous
    echo of this stream is self-describing: any lost / duplicated / reordered
    chunk (a dropped or double multishot CQE) breaks the value sequence, which
    the oracle detects exactly without needing to know chunk boundaries."""
    out = bytearray(n)
    x = seed & 0xFFFFFFFF
    for i in range(n):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out[i] = (x >> 16) & 0xFF
    return bytes(out)


def setup(H):
    # Availability guard: io_uring must be usable (probes liburing + a kernel
    # >= 5.1 with the needed ops; non-Linux returns 0).  If not, SKIP clean.
    if not runloom_c.iouring_available():
        print("SKIP: io_uring not available "
              "(non-Linux, kernel < 5.1, or liburing not built) -- "
              "loop/multishot backend cannot be exercised")
        sys.exit(0)

    # Re-assert the env toggles from the parsed args.  These were already
    # defaulted at module import (before runloom_c loaded), but --ms / --ms-bufs
    # let an explicit control run override; the C side reads them lazily at
    # hub-ring init, which has not happened yet inside setup().
    os.environ["RUNLOOM_IOURING_LOOP"] = "1"
    os.environ["RUNLOOM_IOURING_MS"] = "1" if H.args.ms else "0"
    os.environ["RUNLOOM_IOURING_MS_BUFS"] = str(max(1, H.args.ms_bufs))

    # Built-in all-C echo server (handler=None): this is the ONLY serve() path
    # that runs runloom_io_c_echo and thus arms the multishot persistent SQE +
    # provided-buffer-ring recv on the owning hub's ring.  SO_REUSEPORT
    # acceptors (one per hub) spread accept load and keep conns hub-pinned.
    acceptors = min(H.hubs, 8)
    port, listeners = runloom_c.serve(H.net_ip(0), 0, None, acceptors, 1024)
    for L in listeners:
        H.register_close(L)
    H.state = {"host": H.net_ip(0), "port": port,
               "ms": bool(H.args.ms),
               "chunks": max(1, H.args.chunks),
               "chunk_max": max(1, H.args.chunk_max)}
    H.log("io_uring loop backend ON, multishot={0}, ms_bufs={1}, port={2}, "
          "acceptors={3}".format("on" if H.args.ms else "off",
                                 os.environ["RUNLOOM_IOURING_MS_BUFS"],
                                 port, acceptors))


def client(H, wid, rng, state):
    import socket
    host = state["host"]
    port = state["port"]
    nchunks = state["chunks"]
    chunk_max = state["chunk_max"]

    # Spread the initial connect storm deterministically.
    H.sleep(rng.random() * 0.5)

    r = 0
    for _ in H.round_range():
        r += 1
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.connect((host, port))

            # A long-lived conn streaming many back-to-back chunks.  The whole
            # round is ONE contiguous tagged stream split into chunks of
            # varying size; the server echoes it byte-for-byte, so we verify
            # the reassembled echo equals the stream we sent (order + content).
            seed = (H.seed ^ (wid * 2654435761) ^ (r * 40503)) & 0xFFFFFFFF
            sizes = [rng.randint(1, chunk_max) for _ in range(nchunks)]
            total = sum(sizes)
            stream = tag_stream(seed, total)

            # Send all chunks back-to-back (stress the persistent multishot SQE
            # and provided buffer-ring recycle under a steady inbound flow),
            # then read the full echo back as a contiguous byte count.
            pos = 0
            for sz in sizes:
                if not H.running():
                    break
                sock.sendall(stream[pos:pos + sz])
                pos += sz
            sent = pos
            if sent == 0:
                continue

            got = recv_exact(sock, sent)

            # ORACLE 1: exact byte count echoed (received == sent).
            if not H.check(len(got) == sent,
                           "byte-count mismatch wid={0} round={1}: "
                           "recv {2} != sent {3}".format(
                               wid, r, len(got), sent)):
                return
            # ORACLE 2: the echo is the EXACT contiguous tagged stream in order
            # -- a lost/duplicated/reordered multishot CQE breaks this.
            if not H.check(got == stream[:sent],
                           "stream mismatch wid={0} round={1}: a lost or "
                           "double multishot CQE / buffer-ring recycle bug "
                           "broke the contiguous tagged sequence".format(
                               wid, r)):
                return
            H.op(wid, len(sizes))
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
        finally:
            netutil.close_quiet(sock)


def body(H):
    H.run_pool(H.funcs, client, H.state)


if __name__ == "__main__":
    harness.main(
        "p223_iouring_loop_multishot_echo", body, setup=setup,
        default_funcs=2000, add_args=add_args,
        describe="io_uring per-hub loop backend + multishot|buffer-select recv "
                 "on long-lived hub-pinned streaming echo conns")
