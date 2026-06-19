"""big_100 / 135 -- interpreter exit with live sockets.

Each iteration launches a CHILD runloom program that stands up a real loopback
TCP echo server and a pool of client goroutines doing continuous round-trips --
so at the moment of shutdown there are LIVE sockets actively parked in
accept()/recv() with bytes in flight (not idle socketpairs).  Two shutdown
shapes are exercised, chosen per child:

  * "clean": main flips a stop flag and closes every listener/socket so the
    parked accept/recv wake; runloom.run() joins the woken goroutines and the
    child exits 0, printing DONE-MARKER then MAIN-EXIT.  Tests deterministic
    socket teardown at scheduler shutdown.
  * "abrupt": main calls os._exit(0) while the server, accept loop, and clients
    are all still live and parked on their fds -- the interpreter never runs
    mn_fini / finalization.  Must terminate immediately with status 0, no
    segfault from tearing the process down on top of live netpoll registrations.

(Per the runloom join semantic noted in p134, a child that *returns* from main()
with goroutines parked FOREVER would hang run(); the clean path therefore wakes
them as part of returning, and the abrupt path bypasses run()'s join entirely.)

Stresses: interpreter finalization with live sockets, abrupt os._exit on top of
active netpoll registrations, socket teardown determinism, no segfault at exit.
"""
import os
import subprocess

import harness
import procutil

CHILD = r'''
import sys, os, socket, threading
sys.path.insert(0, {src!r})
import runloom
import runloom.monkey
runloom.monkey.patch()                    # cooperative socket I/O on the hubs

MODE = sys.argv[1] if len(sys.argv) > 1 else "clean"
stop = [False]
lock = threading.Lock()
live = []                                 # every live socket fd (server, conns, clients)

def track(s):
    with lock:
        live.append(s)
    return s

def echo_conn(conn):
    try:
        while not stop[0]:
            d = conn.recv(256)
            if not d:
                break
            conn.sendall(d)
    except OSError:
        pass

def accept_loop(srv):
    try:
        while not stop[0]:
            try:
                conn, _ = srv.accept()
            except OSError:
                break
            track(conn)
            runloom.fiber(echo_conn, conn)
    except OSError:
        pass

def client(addr):
    s = track(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    try:
        s.connect(addr)
        n = 0
        while not stop[0] and n < 100000:
            s.sendall(b"ping")
            d = s.recv(4)
            if d != b"ping":
                break
            n += 1
    except OSError:
        pass

def main():
    srv = track(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(128)
    addr = srv.getsockname()
    runloom.fiber(accept_loop, srv)
    for _ in range(24):
        runloom.fiber(client, addr)
    runloom.sleep(0.05)                  # let live traffic actually start
    sys.stdout.write("DONE-MARKER\n"); sys.stdout.flush()
    if MODE == "abrupt":
        # Server, accept loop, echo conns and clients are all live and parked
        # on their fds right now -- terminate on top of them.
        sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
        os._exit(0)                       # no mn_fini, no finalization
    # clean: wake EVERY parked recv/accept by closing all live sockets, then
    # let run() join the woken goroutines and fall through.
    stop[0] = True
    with lock:
        socks = list(live)
    for s in socks:
        try: s.close()
        except OSError: pass

runloom.run(4, main)
sys.stdout.write("MAIN-EXIT\n"); sys.stdout.flush()
'''


def setup(H):
    import sys
    src = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "src")
    script = os.path.join(H.make_tmpdir("big100_exitsock_"), "child.py")
    with open(script, "w") as f:
        f.write(CHILD.format(src=src))
    H.state = {"py": sys.executable, "script": script}


def worker(H, wid, rng, state):
    for _ in H.round_range():
        mode = "abrupt" if (rng.random() < 0.5) else "clean"
        env = dict(os.environ)
        env["PYTHON_GIL"] = "0"
        try:
            proc = procutil.popen([state["py"], state["script"], mode],
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, env=env,
                                  running=H.running)
        except OSError:
            break
        try:
            out, err = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            H.fail("child HUNG at exit-with-live-sockets ({0}) wid={1}".format(
                mode, wid))
            return
        except OSError:
            if not H.running():
                break
            raise
        # Both modes must exit 0 (clean teardown OR clean abrupt os._exit) and
        # never crash the interpreter on top of live netpoll registrations.
        if not H.check(proc.returncode == 0,
                       "child ({0}) exited {1} wid={2} (segfault at exit with "
                       "live sockets?) stderr={3!r}".format(
                           mode, proc.returncode, wid, err[-200:])):
            return
        # The clean path additionally proves the join-after-close drained;
        # the abrupt path proves os._exit terminated mid-flight without a crash.
        if not H.check(b"DONE-MARKER" in out,
                       "child ({0}) never reached live traffic wid={1}: {2!r}"
                       .format(mode, wid, out[:120])):
            return
        H.op(wid)
        H.task_done(wid)


def body(H):
    H.run_pool(H.funcs, worker, H.state)


def post(H):
    H.check(H.total_ops() > 0,
            "no child exited cleanly with live sockets")
    H.log("clean_exits={0} exited={1}/{2}".format(
        H.total_ops(), H.exited, H.expected))


if __name__ == "__main__":
    harness.main("p135_exit_live_sockets", body, setup=setup, post=post,
                 default_funcs=120,
                 describe="child runloom exits (clean join AND abrupt os._exit) "
                          "with live TCP sockets in flight; no segfault, exit 0")
