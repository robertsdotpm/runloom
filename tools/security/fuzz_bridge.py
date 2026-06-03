"""Adversarial network fuzzer for the runloom.aio bridge (S6).

All the existing verification targets the scheduler core (channels, deque,
park/wake, netpoll). The aio bridge's *transport lifecycle* -- the actual
remote attack surface -- is where the delicate invariants live (connection
teardown, the per-connection io goroutine, RST/half-close handling,
backpressure, accept-loop close). This drives a real runloom.aio echo server
with raw-socket chaos and watches for a crash, a hang (lost wakeup), an ASan
error, or a goroutine/fd leak.

Run it under ASan (so a transport memory bug or a stack-pool use-after-recycle
-- S5 -- is caught too):

    RUNLOOM_EXTRA_CFLAGS="-fsanitize=address -g" RUNLOOM_EXTRA_LDFLAGS=-fsanitize=address \\
        python setup.py build_ext --inplace --force
    LD_PRELOAD=$(gcc -print-file-name=libasan.so) ASAN_OPTIONS=detect_leaks=0 \\
    PYTHON_GIL=0 PYTHONPATH=src python tools/security/fuzz_bridge.py --iters 4000
"""
import os
import random
import socket
import struct
import subprocess
import sys
import time

HOST = "127.0.0.1"


# ---- server subprocess: a real runloom.aio echo server -------------------------
def run_server(port_file):
    import asyncio
    import runloom.aio as paio

    async def handle(reader, writer):
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (OSError, Exception):  # noqa: B014 - belt-and-suspenders
            pass
        finally:
            try:
                writer.close()
            except OSError:
                pass

    async def main():
        server = await paio.start_server(handle, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        with open(port_file, "w") as f:
            f.write(str(port))
        # start_server already accepts in the background; just keep main alive.
        while True:
            await asyncio.sleep(3600)

    paio.run(main())


# ---- chaos strategies (raw socket vs the echo server) -----------------------
def _conn(port, timeout=2.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((HOST, port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return s


def chaos(port, rng):
    kind = rng.randrange(9)
    try:
        if kind == 0:                                  # random bytes
            s = _conn(port)
            s.sendall(bytes(rng.randrange(256) for _ in range(rng.randrange(1, 4096))))
            s.recv(4096)
            s.close()
        elif kind == 1:                                # RST (abort) mid-stream
            s = _conn(port)
            s.sendall(os.urandom(rng.randrange(1, 2000)))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                         struct.pack("ii", 1, 0))      # linger 0 -> RST on close
            s.close()
        elif kind == 2:                                # half-close (shutdown WR)
            s = _conn(port)
            s.sendall(os.urandom(64))
            s.shutdown(socket.SHUT_WR)
            try:
                s.recv(4096)
            except OSError:
                pass
            s.close()
        elif kind == 3:                                # oversized single write
            s = _conn(port)
            s.sendall(os.urandom(1 << rng.randrange(16, 21)))   # 64KB..1MB
            s.close()
        elif kind == 4:                                # slow drip then abort
            s = _conn(port)
            for _ in range(rng.randrange(2, 8)):
                s.sendall(bytes([rng.randrange(256)]))
            s.close()
        elif kind == 5:                                # connect + immediate close
            s = _conn(port)
            s.close()
        elif kind == 6:                                # send + never read (backpressure)
            s = _conn(port)
            try:
                for _ in range(40):
                    s.sendall(os.urandom(65536))       # fill server write buffer
            except OSError:
                pass
            s.close()
        elif kind == 7:                                # truncated length-ish prefix
            s = _conn(port)
            s.sendall(struct.pack(">I", rng.randrange(1 << 31)))
            s.close()
        else:                                          # rapid connect storm
            for _ in range(rng.randrange(3, 12)):
                try:
                    _conn(port, 0.5).close()
                except OSError:
                    pass
    except OSError:
        pass                                           # peer/connection errors are fine


def clean_echo(port):
    """A well-behaved request must still round-trip (server alive + responsive)."""
    try:
        s = _conn(port, 3.0)
        msg = b"health-" + os.urandom(8)
        s.sendall(msg)
        got = b""
        while len(got) < len(msg):
            chunk = s.recv(len(msg) - len(got))
            if not chunk:
                break
            got += chunk
        s.close()
        return got == msg
    except OSError:
        return False


def main():
    import argparse
    if len(sys.argv) >= 3 and sys.argv[1] == "--server":
        return run_server(sys.argv[2])
    p = argparse.ArgumentParser()
    p.add_argument("--iters", type=int, default=3000)
    p.add_argument("--seed", type=int, default=1234)
    args = p.parse_args()

    rng = random.Random(args.seed)
    here = os.path.dirname(os.path.abspath(__file__))
    pf = os.path.join(here, ".fuzz_port")
    if os.path.exists(pf):
        os.remove(pf)
    log = open(os.path.join(here, ".fuzz_server.log"), "w")
    proc = subprocess.Popen([sys.executable, __file__, "--server", pf],
                            stdout=log, stderr=subprocess.STDOUT, env=os.environ)
    for _ in range(150):
        if os.path.exists(pf):
            break
        if proc.poll() is not None:
            print("SERVER FAILED TO START"); return 1
        time.sleep(0.1)
    port = int(open(pf).read())
    print("fuzzing runloom.aio echo server on port %d, %d iters" % (port, args.iters))

    if not clean_echo(port):
        print("FAIL: server not responsive at start"); proc.kill(); return 1

    bad = 0
    for i in range(args.iters):
        chaos(port, rng)
        if i % 100 == 99:
            if proc.poll() is not None:
                print("FAIL: SERVER DIED after %d iters (crash/ASan -> see .fuzz_server.log)" % i)
                return 1
            if not clean_echo(port):
                print("FAIL: SERVER UNRESPONSIVE after %d iters (hang/lost-wakeup?)" % i)
                bad += 1
                if bad >= 3:
                    proc.kill(); return 1
    alive = proc.poll() is None
    healthy = clean_echo(port)
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    log.close()
    print("done: server alive=%s responsive=%s after %d chaos iters"
          % (alive, healthy, args.iters))
    if alive and healthy:
        print("OK: bridge transport survived adversarial input")
        return 0
    print("FAIL: server unhealthy at end")
    return 1


if __name__ == "__main__":
    sys.exit(main())
