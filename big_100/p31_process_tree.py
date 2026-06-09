"""big_100 / 31 -- process tree reaper.

Each goroutine spawns a child (in its own session/process group) that spawns
several grandchildren, then kills the whole group and reaps the direct child.
Killing the process group takes the grandchildren down too; a botched teardown
would leak processes.

Stresses: signal/process-group cleanup, waitpid, session handling.
"""
import os
import signal

import harness
import procutil

# Child: spawn N grandchildren that sleep, then sleep itself.
PARENT = ("import subprocess,time\n"
          "kids=[subprocess.Popen(['sleep','3600']) for _ in range({0})]\n"
          "time.sleep(3600)\n")


def worker(H, wid, rng, state):
    py = state["py"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        n = rng.randint(2, 4)
        proc = None
        try:
            proc = procutil.popen(
                [py, "-c", PARENT.format(n)],
                start_new_session=True,
                running=H.running)             # own process group
            # Let the child come up and spawn its grandchildren.
            H.sleep(0.05 + rng.random() * 0.1)
            # Kill the whole group, then reap the direct child.
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            proc.wait()
            if not H.check(proc.returncode is not None,
                           "tree not reaped wid={0}".format(wid)):
                return
            H.op(wid)
            H.task_done(wid)
        except OSError as e:
            if not H.running():
                break
            H.fail("tree error wid={0}: {1}".format(wid, e))
            return


def setup(H):
    import sys
    H.state = {"py": sys.executable}


def body(H):
    H.run_pool(H.funcs, worker, H.state, max_concurrent=200)


if __name__ == "__main__":
    harness.main("p31_process_tree", body, setup=setup, default_funcs=200,
                 describe="spawn child+grandchildren, killpg the group, reap")
