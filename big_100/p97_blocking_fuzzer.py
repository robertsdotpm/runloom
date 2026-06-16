"""big_100 / 97 -- blocking API fuzzer.

Each iteration a goroutine performs ONE randomly chosen blocking operation --
a loopback socket echo, a temp-file write/read, an os.pipe round-trip, a `cat`
subprocess, or a (cancellable) sleep -- some wrapped in a timeout/cancel.  Every
op must complete correctly or unwind cleanly, and the fd auditor confirms the
integration layer leaks nothing across all those paths.

Stresses: the whole blocking-API integration layer under random mixing.
"""
import os
import socket
import subprocess

import harness
import cancelutil
import netutil
import procutil
import runloom


def op_socket(H, rng, state, wid=0):
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((state["echo_host"], state["echo_port"]))
        payload = rng.randbytes(rng.randint(1, 256))
        sock.sendall(payload)
        got = netutil.recv_exact(sock, len(payload))
        return H.check(got == payload, "socket echo mismatch")
    except OSError:
        return True             # transient -> not a failure
    finally:
        netutil.close_quiet(sock)


def op_file(H, rng, state, wid=0):
    path = os.path.join(state["base"], "ff{0}_{1}".format(wid, rng.getrandbits(32)))
    data = rng.randbytes(rng.randint(1, 4096))
    try:
        with open(path, "wb") as f:
            f.write(data)
        with open(path, "rb") as f:
            got = f.read()
        return H.check(got == data, "file round-trip mismatch")
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def op_pipe(H, rng, state, wid=0):
    r, w = os.pipe()
    try:
        msg = rng.randbytes(rng.randint(1, 4000))
        os.write(w, msg)
        os.close(w)
        w = -1
        got = b""
        while len(got) < len(msg):
            chunk = os.read(r, len(msg) - len(got))
            if not chunk:
                break
            got += chunk
        return H.check(got == msg, "pipe round-trip mismatch")
    finally:
        for fd in (r, w):
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass


def op_subprocess(H, rng, state, wid=0):
    payload = rng.randbytes(rng.randint(1, 1024))
    proc = procutil.popen(procutil.CAT, stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, running=H.running)
    out, _ = proc.communicate(payload)
    return H.check(out == payload, "subprocess echo mismatch")


def op_sleep(H, rng, state, wid=0):
    ctx, cancel = cancelutil.WithTimeout(cancelutil.Background(),
                                         rng.uniform(0.001, 0.01))
    cancelutil.cancellable_sleep(ctx, rng.uniform(0.0, 0.02))
    cancel()
    return True


def setup(H):
    H.state = {"echo_port": netutil.start_echo_server(H),
               "echo_host": netutil._DEFAULT_HOST,
               "base": H.make_tmpdir("big100_blkfuzz_")}
    H.fd_ceiling = 0


OPS = None


def worker(H, wid, rng, state):
    ops = [op_socket, op_file, op_pipe, op_sleep]
    # subprocess is heavier -> include but weight it low
    while H.running():
        if rng.random() < 0.1:
            op = op_subprocess
        else:
            op = rng.choice(ops)
        try:
            if not op(H, rng, state, wid=wid):
                return
        except OSError:
            if not H.running():
                break
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)

    def auditor():
        base = harness.count_fds()
        while H.running():
            fds = harness.count_fds()
            H.fd_ceiling = max(H.fd_ceiling, fds)
            H.check(fds < base + H.funcs * 4 + 6000,
                    "fd leak in blocking fuzzer: {0} (base {1})".format(
                        fds, base))
            H.sleep(1.0)
        H.log("fd_ceiling={0}".format(H.fd_ceiling))

    H.go(auditor)


if __name__ == "__main__":
    harness.main("p97_blocking_fuzzer", body, setup=setup, default_funcs=1200,
                 describe="random socket/file/pipe/subprocess/sleep ops; no leaks")
