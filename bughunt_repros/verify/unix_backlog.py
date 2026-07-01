import socket, sys, os, tempfile, time
import runloom

def scenario(tag):
    path = tempfile.mktemp(prefix="rl_unix_")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(path); srv.listen(0)
    fillers = []
    for i in range(16):
        f = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        f.setblocking(False)
        if f.connect_ex(path) != 0: fillers.append(f); break
        fillers.append(f)
    c = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); c.settimeout(2.0)
    try:
        c.connect(path)
        try: print(tag, "connected:", c.getpeername(), flush=True)
        except OSError as e:
            print(tag, "FALSE SUCCESS; getpeername ->", e, flush=True)
            try:
                c.send(b"x")
            except OSError as e2:
                print(tag, "send ->", e2, flush=True)
    except Exception as e:
        print(tag, "connect raised", type(e).__name__, e, flush=True)
    os.unlink(path)

if sys.argv[1] == "stock": scenario("stock:")
else:
    def main(): runloom.fiber(lambda: scenario("patched:"))
    runloom.monkey.patch(); runloom.run(2, main)
