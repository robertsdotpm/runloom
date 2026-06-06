"""big_100 / 29 -- stdout flood reader.

Each goroutine spawns a child that writes a large, known amount to stdout, and
must drain it without deadlocking (a child blocks once the pipe buffer fills if
the parent stops reading).  communicate() multiplexes the drain through a
cooperative selector so the goroutine yields while waiting.

Stresses: pipe draining, avoiding the classic fill-the-pipe deadlock,
scheduler fairness while a big transfer is in flight.
"""
import subprocess

import procutil

import harness

FLOOD = ("import sys\n"
         "buf=b'A'*4096\n"
         "for _ in range({0}): sys.stdout.buffer.write(buf)\n")


def worker(H, wid, rng, state):
    py = state["py"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        blocks = rng.randint(64, 1024)          # up to ~4 MiB
        expected = blocks * 4096
        try:
            proc = procutil.popen([py, "-c", FLOOD.format(blocks)],
                                    stdout=subprocess.PIPE,
                                    running=H.running)
            out, _ = proc.communicate()
            if not H.check(len(out) == expected,
                           "flood short read wid={0}: {1} != {2}".format(
                               wid, len(out), expected)):
                return
            H.op(wid, max(1, len(out) // 4096))
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("flood error wid={0}: {1}".format(wid, e))
            return


def setup(H):
    import sys
    H.state = {"py": sys.executable}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p29_stdout_flood", body, setup=setup, default_funcs=400,
                 describe="drain a flooding child's stdout without deadlock")
