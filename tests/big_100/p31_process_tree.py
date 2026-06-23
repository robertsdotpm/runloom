"""big_100 / 31 -- process tree reaper.

Each goroutine spawns a child (in its own session/process group) that spawns
several grandchildren, then kills the whole group and reaps the direct child.
Killing the process group takes the grandchildren down too; a botched teardown
would leak processes.

Stresses: signal/process-group cleanup, waitpid, session handling.
"""
import os
import subprocess
import sys

import harness
import procutil

_WIN = (sys.platform == "win32")

# Child: spawn N grandchildren that sleep, then sleep itself.  The grandchild is
# the Unix `sleep` binary on POSIX and the running interpreter on Windows (no
# coreutils).  The child evaluates its own sys.platform so one PARENT string
# serves both.  On Windows the grandchildren sleep only 30s (vs 3600): a
# tree-kill that races a not-yet-spawned grandchild can orphan it, and a 30s
# self-terminating sleeper bounds that leak; killpg on POSIX takes the whole
# group atomically so the long sleep there never leaks.
PARENT = (
    "import subprocess,sys,time\n"
    "S=3600 if sys.platform!='win32' else 30\n"
    "g=(['sleep',str(S)] if sys.platform!='win32'\n"
    "   else [sys.executable,'-c',"
    "'import time,sys;time.sleep(int(sys.argv[1]))',str(S)])\n"
    "kids=[subprocess.Popen(g) for _ in range({0})]\n"
    "time.sleep(S)\n")


def worker(H, wid, rng, state):
    py = state["py"]
    H.sleep(rng.random() * 0.5)
    while H.running():
        n = rng.randint(2, 4)
        proc = None
        try:
            # Windows has no process groups/sessions: a NEW PROCESS GROUP plus
            # `taskkill /T` (whole tree) is the analogue of start_new_session +
            # killpg -- both take the grandchildren down with the direct child.
            if _WIN:
                spawn_kw = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
            else:
                spawn_kw = {"start_new_session": True}
            proc = procutil.popen(
                [py, "-c", PARENT.format(n)],
                running=H.running, **spawn_kw)
            # Let the child come up and spawn its grandchildren.
            H.sleep(0.05 + rng.random() * 0.1)
            # Kill the whole group/tree, then reap the direct child.
            if _WIN:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                import signal
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
    H.run_pool(H.funcs, worker, H.state)


if __name__ == "__main__":
    harness.main("p31_process_tree", body, setup=setup, default_funcs=200,
                 describe="spawn child+grandchildren, killpg the group, reap")
