"""big_100 / 228 -- wait_fd park + cross-goroutine cancel + fd-reuse on raw pipes.

Drives the low-level fd-parking primitives DIRECTLY on raw pollable fds
(os.pipe()), not through the socket layer the way p101-p103 do.  Each worker
runs two phases:

  Phase A -- park / wake / cancel on a fresh pipe.  The worker makes an
  os.pipe(), spawns a sibling goroutine that calls
  runloom_c.wait_fd(read_fd, READ) and parks (the pipe is empty).  The worker
  then chooses, per round, one of two resolutions:
    (a) WRITE a uniquely tagged byte -> the parker's wait_fd returns readable,
        the parker os.read()s exactly that byte and reports it; the worker
        H.check()s the byte is the one IT wrote (no cross-talk), or
    (b) CANCEL via runloom_c.netpoll_cancel_fd(read_fd) while the sibling is
        parked -> the parker's wait_fd must return the WAIT_FD_CANCELLED
        sentinel (NOT readable, NOT a hang); the worker H.check()s the parker
        saw exactly that sentinel.
  Either way the sibling reports through a channel (1:1 with the worker loop),
  so a genuine lost-wakeup / lost-cancel becomes a WATCHDOG HANG, not a silent
  pass.  Then both fds are closed.

  Phase B -- stale-arm reuse.  The worker tightly loops open/close of pipes so
  the kernel recycles fd NUMBERS, then arms wait_fd on a freshly created pipe
  whose read fd very likely reuses a just-closed number.  A writer feeds it a
  byte; wait_fd must report THIS pipe's readiness.  A stale per-fd arm left from
  the prior fd of the same number would never re-arm -> the parker never wakes
  -> the watchdog fires.  netpoll_release_if_idle() is called on every closed
  read fd so the idle arm is dropped and the reused number re-registers cleanly.

Oracle: every park resolves to either the correct tagged byte or the cancelled
sentinel; zero hangs; no leaked fds.

Stresses: Stresses: runloom_c.wait_fd parking on raw pollable fds (os.pipe),
cross-goroutine netpoll_cancel_fd while parked (WAIT_FD_CANCELLED sentinel, not
a hang), and per-fd arm-cache correctness when the SAME fd NUMBER is closed and
re-created (stale-arm reuse hang class).
"""
import os
import sys

# ---- availability guard (POSIX-only: file/pipe fds must be pollable) -------
# Windows file/pipe fds are not pollable by the netpoll backend, so wait_fd on
# an os.pipe() read end can't park there.  Detect-and-skip-clean.
_POSIX = sys.platform.startswith(("linux", "darwin", "freebsd"))
if not _POSIX:
    print("SKIP: POSIX-only (raw pipe fds not pollable on this platform: "
          "{0})".format(sys.platform))
    sys.exit(0)

import harness
import runloom
import runloom_c

if not hasattr(runloom_c, "wait_fd") or not hasattr(runloom_c, "netpoll_cancel_fd"):
    print("SKIP: runloom_c.wait_fd / netpoll_cancel_fd unavailable")
    sys.exit(0)

READ = 1                              # wait_fd events bitmask: 1=read
CANCELLED = runloom_c.WAIT_FD_CANCELLED
# Short re-park ceiling: netpoll_cancel_fd(fd) only wakes a fiber that is
# ALREADY parked, so a cancel issued in the tiny window before the park lands is
# a benign no-op (NOT the bug we hunt) -- the parker must wake on the ceiling and
# RE-PARK so a retried cancel can land.  A genuinely LOST cancel (the real bug)
# means the worker's retried cancels never resolve the parker -> watchdog HANG.
REPARK_MS = 50


def parked_reader(read_fd, mode, expect_byte, done):
    """Park in wait_fd(read_fd, READ) on an empty pipe and report the OUTCOME
    through `done` so the worker can assert it 1:1:

      mode == "write":  the worker writes one tagged byte.  We must wake
                        readable, os.read() exactly that byte, and report
                        ("byte", <the byte>).
      mode == "cancel": the worker calls netpoll_cancel_fd(read_fd) while we are
                        parked.  wait_fd must return the WAIT_FD_CANCELLED
                        sentinel; we report ("cancelled", sentinel).

    We loop on a SHORT ceiling and RE-PARK on a bare timeout (0): the fd-based
    cancel can race the park-register, and a write can land between parks; a
    re-park turns that race benign while still wedging the watchdog if a wake or
    cancel is genuinely lost.  Any UNEXPECTED resolution is reported verbatim so
    the worker's H.check fails loudly rather than silently re-parking forever."""
    while True:
        try:
            ready = runloom_c.wait_fd(read_fd, READ, REPARK_MS)
        except OSError:
            done.send(("err", -1))
            return
        if ready == CANCELLED:
            # A cancel woke us.  In write-mode this is unexpected (no one should
            # cancel) -- report it so the check fails.
            done.send(("cancelled" if mode == "cancel" else "unexpected_cancel",
                       ready))
            return
        if ready & READ:
            if mode == "cancel":
                # Cancel-mode but the fd went readable instead of cancelled --
                # unexpected; surface it.
                done.send(("not_cancelled", ready))
                return
            try:
                b = os.read(read_fd, 1)
            except OSError:
                done.send(("err", -1))
                return
            done.send(("byte", b[0] if b else -1))
            return
        # ready == 0: bare timeout.  Re-park so a racing cancel/write can land.


def worker(H, wid, rng):
    H.sleep(rng.random() * 0.2)
    seq = 0
    for _ in H.round_range():
        # ----- Phase A: park then resolve by write OR by cross-g cancel -----
        try:
            rfd, wfd = os.pipe()
        except OSError:
            if not H.running():
                break
            continue
        os.set_blocking(rfd, False)   # raw non-blocking; wait_fd does the parking
        # Per-round, unique-ish tag byte so a stale/cross-talk byte is detectable.
        seq += 1
        tag = (seq * 31 + wid) & 0xFF
        do_cancel = rng.random() < 0.5
        done = runloom.Chan(1)
        H.go(parked_reader, rfd, "cancel" if do_cancel else "write",
             tag, done)
        H.sleep(0.002)                # let the sibling actually reach the park

        if do_cancel:
            # Cross-goroutine cancel of the parked wait_fd.  netpoll_cancel_fd
            # only wakes a fiber ALREADY parked, so retry it until the sibling
            # reports -- a cancel that races the park is a benign no-op (the
            # parker re-parks on its short ceiling); a genuinely LOST cancel
            # never resolves the sibling -> watchdog HANG (the real bug).
            res = None
            while res is None:
                runloom_c.netpoll_cancel_fd(rfd)
                res = done.try_recv()
                if res is None:
                    H.sleep(0.005)
            (kind, val), _ok = res            # Chan -> (value, ok)
            if not H.check(kind == "cancelled" and val == CANCELLED,
                           "cancel did not yield WAIT_FD_CANCELLED wid={0} "
                           "seq={1}: got ({2!r}, {3!r})".format(
                               wid, seq, kind, val)):
                os.close(rfd)
                os.close(wfd)
                return
        else:
            # Resolve by writing the tagged byte; sibling must read EXACTLY it.
            try:
                os.write(wfd, bytes((tag,)))
            except OSError:
                pass
            (kind, val), _ok = done.recv()    # Chan.recv -> (value, ok)
            if not H.check(kind == "byte" and val == tag,
                           "wait_fd wake/read cross-talk wid={0} seq={1}: "
                           "wrote {2} got ({3!r}, {4!r})".format(
                               wid, seq, tag, kind, val)):
                os.close(rfd)
                os.close(wfd)
                return
        # Drop any idle arm before closing so the fd number re-registers clean.
        try:
            runloom_c.netpoll_release_if_idle(rfd)
        except Exception:
            pass
        os.close(rfd)
        os.close(wfd)
        H.op(wid)

        # ----- Phase B: churn fd numbers, then arm wait_fd on a reused fd ----
        # Tightly open/close pipes so the kernel recycles the just-freed numbers,
        # then create a fresh pipe (whose read fd very likely reuses one) and
        # confirm wait_fd parks and WAKES on the NEW pipe's readiness.  A stale
        # arm from the prior fd of the same number would never re-arm -> hang.
        churn = []
        try:
            for _ in range(rng.randint(2, 6)):
                cr, cw = os.pipe()
                churn.append((cr, cw))
        except OSError:
            pass
        for cr, cw in churn:
            try:
                runloom_c.netpoll_release_if_idle(cr)
            except Exception:
                pass
            os.close(cr)
            os.close(cw)

        try:
            r2, w2 = os.pipe()
        except OSError:
            if not H.running():
                break
            continue
        os.set_blocking(r2, False)
        tag2 = (seq * 17 + wid + 3) & 0xFF
        done2 = runloom.Chan(1)
        H.go(parked_reader, r2, "write", tag2, done2)
        H.sleep(0.002)
        try:
            os.write(w2, bytes((tag2,)))
        except OSError:
            pass
        (kind2, val2), _ok2 = done2.recv()   # lost stale-arm wake -> watchdog HANG here
        ok = H.check(kind2 == "byte" and val2 == tag2,
                     "stale-arm reuse: wait_fd on a recycled fd did not wake on "
                     "the NEW pipe wid={0} seq={1}: wrote {2} got ({3!r}, "
                     "{4!r})".format(wid, seq, tag2, kind2, val2))
        try:
            runloom_c.netpoll_release_if_idle(r2)
        except Exception:
            pass
        os.close(r2)
        os.close(w2)
        if not ok:
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker)


if __name__ == "__main__":
    # Moderate concurrency on purpose (cf. p102 / FINDINGS): each round spawns a
    # sibling goroutine, does a cross-goroutine cancel handoff, and churns
    # several raw pipe fds -- the close-vs-parked-wait_fd handoff plus per-round
    # fd allocation does not scale to 1M, and each in-flight pipe holds 2 fds, so
    # we cap to stay well under the descriptor ceiling.
    harness.main("p228_waitfd_cancel_reuse_pipes", body,
                 default_funcs=200, max_funcs=400,
                 describe="wait_fd parks on raw os.pipe fds; cross-goroutine "
                          "netpoll_cancel_fd yields WAIT_FD_CANCELLED; reused fd "
                          "numbers re-arm cleanly (no stale-arm hang)")
