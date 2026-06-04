"""big_100 / 30 -- stdin backpressure writer.

Each goroutine feeds a large input to a child that consumes it slowly, so the
child's stdin pipe fills and the parent's write blocks.  Under the cooperative
scheduler that block must yield -- other goroutines keep making progress while
this one is backpressured -- and the child must ultimately receive every byte.

Stresses: blocking pipe writes turned cooperative, backpressure, fairness.
"""
import subprocess

import procutil

import harness

# Child counts the bytes it received and prints the count -- it reads in small
# chunks with a tiny delay so the parent's writes back up.
SINK = ("import sys,time\n"
        "n=0\n"
        "while True:\n"
        "    b=sys.stdin.buffer.read(4096)\n"
        "    if not b: break\n"
        "    n+=len(b)\n"
        "    time.sleep(0.0005)\n"
        "sys.stdout.write(str(n))\n")


def worker(H, wid, rng, state):
    py = state["py"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        size = rng.randint(32768, 262144)
        payload = b"x" * size
        try:
            proc = procutil.popen([py, "-c", SINK], stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            out, _ = proc.communicate(payload)      # cooperative write+read
            if not H.check(out.strip() == str(size).encode(),
                           "child got {0!r}, expected {1} wid={2}".format(
                               out[:16], size, wid)):
                return
            H.op(wid, max(1, size // 4096))
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("backpressure error wid={0}: {1}".format(wid, e))
            return


def setup(H):
    import sys
    H.state = {"py": sys.executable}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p30_stdin_backpressure", body, setup=setup, default_funcs=300,
                 describe="feed a slow child huge stdin; writes must yield")
