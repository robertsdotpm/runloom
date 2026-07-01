"""AF_UNIX stream connect with a full listen backlog: EAGAIN from connect()
means "not connectable now", NOT "in progress".  Patched connect treats
EAGAIN/EWOULDBLOCK as in-flight, waits for WRITE, reads SO_ERROR==0 and
returns success on a socket that was never connected."""
import socket, sys, os, tempfile, time

def scenario(tag):
    path = tempfile.mktemp(prefix="rl_unix_")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path); srv.listen(0)      # backlog 0-ish
    fillers = []
    # fill the backlog with raw non-blocking connects (bypass patch via connect_ex)
    for i in range(16):
        f = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        f.setblocking(False)
        rc = f.connect_ex(path)
        fillers.append(f)
        if rc not in (0,):
            break
    c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    c.settimeout(2.0)
    t0 = time.monotonic()
    try:
        c.connect(path)
        dt = time.monotonic() - t0
        # verify actually connected
        try:
            peer = c.getpeername()
            print(tag, "connect returned after %.2fs; getpeername=%r (connected)" % (dt, peer), flush=True)
        except OSError as e:
            print(tag, "connect FALSELY SUCCEEDED after %.2fs; getpeername -> %s (NOT CONNECTED)" % (dt, e), flush=True)
        try:
            c.send(b"x")
            print(tag, "send ok", flush=True)
        except OSError as e:
            print(tag, "send -> %s" % e, flush=True)
    except Exception as e:
        print(tag, "connect raised %s: %s (after %.2fs)" % (type(e).__name__, e, time.monotonic()-t0), flush=True)
    for f in fillers: f.close()
    c.close(); srv.close(); os.unlink(path)

if sys.argv[1] == "stock":
    scenario("stock:")
else:
    import runloom
    def main():
        runloom.fiber(lambda: scenario("patched-fiber:"))
    runloom.monkey.patch()
    runloom.run(2, main)
