"""big_100 / 33 -- PTY interaction test.

If pseudo-terminals are available, each goroutine opens a pty, runs `cat` on
the slave end in raw mode (so the only bytes that come back are cat's echo),
writes a payload to the master, and reads the echo back cooperatively via the
monkey-patched os.read on the tty fd.

Stresses: terminal fds, blocking tty reads turned cooperative, odd EOF.
"""
import os
import subprocess

import harness
import procutil
import runloom

HAVE_PTY = hasattr(os, "openpty")
if HAVE_PTY:
    import tty

# At 100k goroutines, each PTY session holds master+slave (2 FDs) + a cat
# process.  The kernel defaults /proc/sys/kernel/pty/max to 4096 and glibc
# posix_spawn crashes when forking with a huge FD table (BUG #4).  Cap
# concurrent sessions so total PTY FDs stay well under the system limit.
MAX_SESSIONS = 512


def read_exact_fd(fd, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = os.read(fd, n - len(buf))     # cooperative for ttys via monkey
        if not chunk:
            raise OSError("pty eof")
        buf += chunk
    return bytes(buf)


def setup(H):
    sem = runloom.sync.Semaphore(MAX_SESSIONS)

    def _cancel_watcher(r=H.running, s=sem):
        while r():
            runloom.sleep(0.05)
        s.cancel_all()

    H.go(_cancel_watcher)
    H.state = {"sem": sem}


def worker(H, wid, rng, state):
    sem = state["sem"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        if not sem.acquire():
            break
        if not H.running():
            sem.release()
            break
        master = slave = -1
        proc = None
        try:
            master, slave = os.openpty()
            try:
                tty.setraw(slave)           # no echo/line discipline
            except Exception:
                pass
            proc = procutil.popen(["cat"], stdin=slave, stdout=slave,
                                  running=H.running)
            os.close(slave)
            slave = -1
            payload = bytes(rng.randint(33, 126) for _ in
                            range(rng.randint(1, 32)))
            os.write(master, payload)
            got = read_exact_fd(master, len(payload))
            if not H.check(got == payload,
                           "pty echo mismatch wid={0}: {1!r} != {2!r}".format(
                               wid, got, payload)):
                return
            H.op(wid)
            H.task_done(wid)
        except OSError:
            if not H.running():
                break
        finally:
            sem.release()
            if proc is not None:
                try:
                    proc.kill()
                    proc.wait()
                except OSError:
                    pass
            for fd in (master, slave):
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        pass


def body(H):
    if not HAVE_PTY:
        H.log("no pty support on this platform -- nothing to do")
        return
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p33_pty", body, setup=setup, default_funcs=150,
                 describe="drive `cat` over a pseudo-terminal, verify echo")
