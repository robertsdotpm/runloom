"""big_100 / 25 -- file descriptor leak detector.

Each goroutine repeatedly opens a grab-bag of fd-owning resources (a temp file,
an os.pipe, a socketpair, sometimes a subprocess) and closes them in a finally,
even when a random exception fires part-way through.  An auditor watches the
live fd count: a real leak from a missed-cleanup path would climb without
bound.

Stresses: cleanup on success, on exception, and on early return.
"""
import os
import socket

import harness
import procutil


class Boom(Exception):
    pass


def worker(H, wid, rng, state):
    H.sleep(rng.random() * 0.3)
    tmpdir = state["base"]
    path = os.path.join(tmpdir, "f{0}".format(wid))
    while H.running():
        opened = []
        proc = None
        try:
            f = open(path, "wb")
            opened.append(f)
            r, w = os.pipe()
            opened.append(r)
            opened.append(w)
            s1, s2 = socket.socketpair()
            opened.append(s1)
            opened.append(s2)
            # Exercise them a little.
            f.write(b"x" * 16)
            os.write(w, b"ping")
            os.read(r, 4)
            s1.sendall(b"hi")
            s2.recv(2)
            if rng.random() < 0.1:
                proc = procutil.popen(procutil.TRUE, running=H.running)
            # Randomly explode before the clean close to test the finally path.
            if rng.random() < 0.2:
                raise Boom()
            H.op(wid)
            H.task_done(wid)
        except Boom:
            pass
        except OSError:
            if not H.running():
                pass
        finally:
            for obj in opened:
                try:
                    if isinstance(obj, int):
                        os.close(obj)
                    else:
                        obj.close()
                except OSError:
                    pass
            if proc is not None:
                try:
                    proc.wait()
                except OSError:
                    pass


def setup(H):
    H.state = {"base": H.make_tmpdir("big100_fdleak_")}
    H.fd_ceiling = 0


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        base = harness.count_fds()
        while H.running():
            fds = harness.count_fds()
            H.fd_ceiling = max(H.fd_ceiling, fds)
            H.check(fds < base + H.funcs * 6 + 4000,
                    "fd leak: {0} open (base {1}, funcs {2})".format(
                        fds, base, H.funcs))
            H.sleep(1.0)
        H.log("fd_ceiling={0} base={1}".format(H.fd_ceiling, base))

    H.fiber(auditor)


if __name__ == "__main__":
    # Each worker holds a pipe + a socketpair; on mbuf-limited kernels (macOS/*BSD)
    # a high worker count exhausts the RAM-sized socket-buffer pool well before RAM
    # does.  Cap workers to a memory-safe ceiling for this box (loose/no-op on
    # Linux at these scales).  See harness.mem_safe_fd_cap.
    harness.main("p25_fd_leak", body, setup=setup, default_funcs=4000,
                 max_funcs=harness.mem_safe_fd_cap(),
                 describe="open files/pipes/sockets/subprocs; verify no fd leak")
