"""big_100 / 32 -- many python -c workers.

Goroutines repeatedly launch short-lived `python -c` subprocesses that do a
small calculation and print the result, which the goroutine parses and
verifies.  Lots of process startup/teardown churn.

Stresses: process startup cost, pipes, wait/reap, resource churn.
"""
import subprocess

import procutil

import harness

CALC = "import sys; n=int(sys.argv[1]); print(n*(n-1)//2)"


def worker(H, wid, rng, state):
    py = state["py"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        n = rng.randint(2, 5000)
        expected = n * (n - 1) // 2
        try:
            proc = procutil.popen([py, "-c", CALC, str(n)],
                                    stdout=subprocess.PIPE,
                                    running=H.running)
            out, _ = proc.communicate()
            if not H.check(int(out.strip()) == expected,
                           "calc wrong wid={0}: {1} != {2}".format(
                               wid, out.strip(), expected)):
                return
            if not H.check(proc.returncode == 0,
                           "worker exited {0} wid={1}".format(
                               proc.returncode, wid)):
                return
            H.op(wid)
            H.task_done(wid)
        except (OSError, ValueError) as e:
            if not H.running():
                break
            H.fail("python worker error wid={0}: {1}".format(wid, e))
            return


def setup(H):
    import sys
    H.state = {"py": sys.executable}


def body(H):
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p32_python_workers", body, setup=setup, default_funcs=400,
                 describe="hundreds of short python -c subprocesses doing calcs")
