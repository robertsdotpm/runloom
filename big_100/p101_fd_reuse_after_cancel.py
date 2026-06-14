"""big_100 / 101 -- fd reuse after cancellation.

A goroutine connects to a local echo server, writes a payload, then ABANDONS
the in-flight reply (parks for readability with a short timeout, then closes the
fd WITHOUT reading the echoed bytes -- i.e. a cancelled/abandoned recv).  It
immediately opens a fresh socket, which almost always reuses the just-closed fd
NUMBER, connects, writes a UNIQUE tag, and reads it back.  The byte it reads
back must be THIS socket's own tag -- never the stale bytes the abandoned recv
left buffered on the old fd, and the fresh socket must not park forever on a
stale one-shot netpoll arm left behind by the cancelled wait.

Stresses: fd lifetime, stale readiness events, fd identity after cancel, the
netpoll per-fd arm cache across an fd-number reuse.  Fully local (loopback).
"""
import socket
import struct

import harness
import netutil
import runloom
import runloom_c


def round_trip(addr, tag):
    """Connect, send `tag`, read len(tag) bytes back, return them (or None)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(addr)
        s.sendall(tag)
        return netutil.recv_exact(s, len(tag))
    except OSError:
        return None
    finally:
        netutil.close_quiet(s)


def abandon(addr, payload):
    """Connect, send, park briefly for the reply, then close WITHOUT reading it
    -- an abandoned/cancelled recv that leaves bytes buffered + an fd to reuse."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect(addr)
        s.sendall(payload)
        # Park for readability with a short timeout (the "recv" we then cancel).
        runloom_c.wait_fd(s.fileno(), 1, 5)
        # Deliberately DO NOT recv -- abandon the reply and recycle the fd.
    except OSError:
        pass
    finally:
        netutil.close_quiet(s)


def worker(H, wid, rng, state):
    addr = (state["host"], state["port"])
    H.sleep(rng.random() * 0.3)
    seq = 0
    for _ in H.round_range():
        while H.running():
            # 1) abandon a recv so the kernel leaves an unread echo + frees an fd
            abandon(addr, b"STALE" + struct.pack("<Q", wid))
            # 2) a fresh socket (very likely reusing that fd number) must get
            #    ITS OWN tag back, exactly, and must not hang on a stale arm.
            seq += 1
            tag = struct.pack("<IIQ", 0xA5A5A5A5, seq, wid)
            got = round_trip(addr, tag)
            if got is None:
                if not H.running():
                    break
                continue
            if not H.check(got == tag,
                           "fd-reuse cross-talk wid={0} seq={1}: sent {2!r} "
                           "got {3!r}".format(wid, seq, tag, got)):
                return
            H.op(wid)
            break
        H.task_done(wid)


def setup(H):
    # Bind the echo server on the SAME explicit loopback IP the workers dial
    # (netutil's _DEFAULT_HOST is frozen at import time and may not match this
    # job's net_ips[0]).
    host = H.net_ip(0)
    port = netutil.start_echo_server(H, host=host)
    H.state = {"host": host, "port": port}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p101_fd_reuse_after_cancel", body, setup=setup,
                 default_funcs=3000,
                 describe="abandon a recv, recycle the fd; fresh socket gets "
                          "its own bytes, no stale-arm hang")
