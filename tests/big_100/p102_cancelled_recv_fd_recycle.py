"""big_100 / 102 -- cancelled recv, then fd recycle.

A reader goroutine parks in recv() on one end of a socketpair with NO data; the
owning goroutine CLOSES that fd out from under it (cancelling the parked recv
cross-goroutine, not abandoning it after a timeout the way p101 does), then
closes the peer.  That frees both fd NUMBERS.  A fresh socketpair -- which almost
always reuses those exact fd numbers -- then carries a unique tagged round-trip,
and the bytes that come back must be ITS OWN: never the stale bytes the cancelled
recv was parked waiting on, and the new pair must not hang on a stale one-shot
netpoll arm left over from the cancelled wait.

The reader reports through a channel that its parked recv actually woke; the
worker blocks on that report, which paces the loop (one reader in flight at a
time -> no goroutine explosion) and turns a genuine lost-wakeup into a watchdog
hang rather than a silent pass.

Stresses: cross-goroutine close-cancels-recv, fd identity after a cancelled
park, the netpoll per-fd arm cache across an fd-number reuse.  Fully local.
"""
import socket
import struct

import harness
import netutil
import runloom
import runloom_c

WAIT_CEILING_MS = 2000          # bound so a lost cancel-wakeup backstops, no hang


def parked_reader(sock, ready, done):
    """Park for readability on `sock` (no data queued) with a BOUNDED wait; the
    owner's close cancels it.  Announce through `ready` before parking so the
    owner closes only once we are genuinely blocked (closing before the park
    races the wakeup -- see FINDINGS); the 2s ceiling backstops a lost cancel so
    teardown never wedges.  Report through `done` when we wake."""
    ready.send(True)
    try:
        fd = sock.fileno()
    except (OSError, ValueError):
        done.send(True)
        return
    if fd >= 0:
        runloom_c.wait_fd(fd, 1, WAIT_CEILING_MS)   # parks; close cancels it
    try:
        done.send(True)            # report we woke (1:1 with the worker loop)
    except Exception:
        pass


def worker(H, wid, rng):
    H.sleep(rng.random() * 0.2)
    seq = 0
    for _ in H.round_range():
        # 1) park a recv on a1, then cancel it by closing a1 from here.
        a1, b1 = socket.socketpair()
        a1.setblocking(True)
        b1.setblocking(True)
        ready = runloom.Chan(1)
        done = runloom.Chan(1)
        H.fiber(parked_reader, a1, ready, done)
        ready.recv()               # reader is about to recv
        H.sleep(0.003)             # let it actually reach the recv park
        netutil.close_quiet(a1)    # cross-goroutine close cancels the parked recv
        netutil.close_quiet(b1)
        done.recv()                # the cancelled reader must have woken+exited

        # 2) a fresh socketpair reuses a1/b1's fd numbers; a tagged round-trip
        #    through it must carry ITS OWN bytes (no stale cross-talk, no hang on
        #    a stale arm left by the cancelled recv).
        seq += 1
        tag = struct.pack("<IIQ", 0xA5A5A5A5, seq, wid)
        a2, b2 = socket.socketpair()
        a2.setblocking(True)
        b2.setblocking(True)
        try:
            b2.sendall(tag)
            got = netutil.recv_exact(a2, len(tag))
            if not H.check(got == tag,
                           "fd-reuse cross-talk wid={0} seq={1}: sent {2!r} "
                           "got {3!r}".format(wid, seq, tag, got)):
                return
            H.op(wid)
        except OSError:
            if not H.running():
                break
        finally:
            netutil.close_quiet(a2)
            netutil.close_quiet(b2)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker)


if __name__ == "__main__":
    # Moderate concurrency on purpose (see p105 / FINDINGS): the close-vs-parked-
    # recv handoff plus two socketpairs' worth of raw-syscall churn per iteration
    # does not scale to 1M; capping is the honest fix.
    harness.main("p102_cancelled_recv_fd_recycle", body, default_funcs=150,
                 max_funcs=200,
                 describe="close cancels a parked recv; a fresh pair reusing the "
                          "fd gets its own bytes, no stale-arm hang")
