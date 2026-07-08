"""Byte-dribble epoll-vs-io_uring differential (QA-steal-V2 #7, Toxiproxy slicer).

A cooperative echo server echoes the client's payload back in ONE-byte slices
with a sched_yield between each, so every slice lands while the client's recv is
parked on the netpoll readiness edge -- exactly the disarm/re-arm window of the
disarm_out lost-wake lineage (529c0186) and, on io_uring, the multishot
partial-CQE / provided-buffer-ring recycle window that benign bulk LAN reads
never touch.  The client reassembles the dribble and reports a CRC + byte count.

The test runs the SAME workload as a subprocess under BOTH netpoll backends
(RUNLOOM_TCPCONN_IOURING 0 vs 1) and asserts:
  * each reassembles the payload byte-exact (CRC + length match the input), and
  * the two backends agree byte-for-byte (any divergence is a bug), and
  * neither hangs -- a dropped edge-triggered readiness on a 1-byte slice would
    strand the client recv, which the subprocess timeout turns into a failure.
"""
import os
import subprocess
import sys
import unittest
import zlib

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import runloom_c

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Self-contained; reads DRIBBLE_NBYTES / DRIBBLE_SLICE from the env, prints
# "OK <crc> <n>" on success or "ERR <repr>" + exit 2 on a fiber exception.
WORKLOAD = r'''
import os, socket, sys, zlib
import runloom_c
NB = int(os.environ["DRIBBLE_NBYTES"])
SLICE = int(os.environ.get("DRIBBLE_SLICE", "1"))

def pattern(n):
    b = bytes(range(256))
    return (b * ((n + 255) // 256))[:n]

payload = pattern(NB)
port = [0]
out = {}
box = []

def bound_port(l):
    fd = l.fileno()
    s = socket.socket(fileno=socket.dup(fd))
    p = s.getsockname()[1]
    s.close()
    return p

def server():
    l = runloom_c.TCPConn.listen("127.0.0.1", 0)
    port[0] = bound_port(l)
    c = l.accept()
    buf = bytearray(NB)
    got = 0
    while got < NB:
        n = c.recv_into(memoryview(buf)[got:])
        if not n:
            break
        got += n
    # Echo back one SLICE at a time, yielding so each slice arrives while the
    # client is parked mid-recv on the netpoll re-arm edge.
    i = 0
    while i < got:
        c.send_all(bytes(buf[i:i + SLICE]))
        i += SLICE
        runloom_c.sched_yield()
    c.close()
    l.close()

def client():
    while port[0] == 0:
        runloom_c.sched_yield()
    c = runloom_c.TCPConn.connect("127.0.0.1", port[0])
    c.send_all(payload)
    acc = bytearray()
    while len(acc) < NB:
        ch = c.recv(65536)
        if not ch:
            break
        acc += ch
    out["crc"] = zlib.crc32(bytes(acc))
    out["n"] = len(acc)
    c.close()

def wrap(fn):
    def r():
        try:
            fn()
        except BaseException as e:   # surfaced to the parent
            box.append(repr(e))
    return r

runloom_c.fiber(wrap(server))
runloom_c.fiber(wrap(client))
runloom_c.run()
if box:
    print("ERR", box[0])
    sys.exit(2)
print("OK", out.get("crc"), out.get("n"))
'''


def _run_backend(iouring, nbytes, slice_):
    env = dict(os.environ,
               PYTHON_GIL="0", PYTHON_TLBC="0",
               PYTHONPATH=os.path.join(REPO, "src"),
               RUNLOOM_TCPCONN_IOURING=("1" if iouring else "0"),
               DRIBBLE_NBYTES=str(nbytes), DRIBBLE_SLICE=str(slice_))
    return subprocess.run([sys.executable, "-c", WORKLOAD], env=env,
                          capture_output=True, text=True, timeout=90)


class TestNetpollDribble(unittest.TestCase):
    NBYTES = 8192          # fits socket buffers (send_all does not block), so the
    SLICE = 1              # focus is the dribbled-echo netpoll edge, not backpressure

    def _assert_ok(self, p, label):
        self.assertEqual(p.returncode, 0,
                         "{0}: rc={1}\nstdout={2!r}\nstderr={3!r}".format(
                             label, p.returncode, p.stdout, p.stderr))
        self.assertTrue(p.stdout.startswith("OK "),
                        "{0}: {1!r}".format(label, p.stdout))
        _, crc, n = p.stdout.split()
        want = zlib.crc32((bytes(range(256)) * ((self.NBYTES + 255) // 256))[:self.NBYTES])
        self.assertEqual(int(n), self.NBYTES,
                         "{0}: short reassembly {1}/{2}".format(label, n, self.NBYTES))
        self.assertEqual(int(crc), want,
                         "{0}: CRC mismatch -- dropped/reordered byte in the "
                         "dribbled echo".format(label))
        return p.stdout

    def test_dribble_epoll_bytewise(self):
        p = _run_backend(iouring=False, nbytes=self.NBYTES, slice_=self.SLICE)
        self._assert_ok(p, "epoll")

    def test_dribble_epoll_vs_iouring_agree(self):
        ep = _run_backend(iouring=False, nbytes=self.NBYTES, slice_=self.SLICE)
        epout = self._assert_ok(ep, "epoll")
        if not runloom_c.iouring_available():
            self.skipTest("io_uring not available on this kernel")
        io = _run_backend(iouring=True, nbytes=self.NBYTES, slice_=self.SLICE)
        ioout = self._assert_ok(io, "io_uring")
        self.assertEqual(epout.split(), ioout.split(),
                         "epoll vs io_uring diverged on the dribbled echo "
                         "(byte-for-byte differential): {0!r} vs {1!r}".format(
                             epout, ioout))


if __name__ == "__main__":
    unittest.main()
