# run: timeout 120 .venv/bin/python r6_close_park_window.py 8000   (free-threaded build, default env)
import os, socket, sys, threading, time
import runloom, runloom_c as rc
N = int(sys.argv[1]) if len(sys.argv) > 1 else 4000
progress = {"i": 0, "outcomes": {}}
def _port(lst):
    s = socket.socket(fileno=socket.dup(lst.fileno()))
    try: return s.getsockname()[1]
    finally: s.detach(); s.close()
def watchdog():
    last = -1; stall = 0
    while True:
        time.sleep(1.0)
        cur = progress["i"]
        if cur == N: return
        if cur == last:
            stall += 1
            if stall >= 8:
                print("HIT: iteration %d wedged; outcomes so far: %s" % (cur, progress["outcomes"]), flush=True)
                os._exit(2)
        else: stall = 0; last = cur
def main():
    lst = rc.TCPConn.listen("127.0.0.1", 0); port = _port(lst); srv = []
    def acceptor():
        while True:
            try: srv.append(lst.accept())
            except BaseException: return
    rc.fiber(acceptor)
    for i in range(N):
        c = rc.TCPConn.connect("127.0.0.1", port)
        flag = threading.Event()
        def closer(c=c, flag=flag):
            flag.wait()
            for _ in range((i * 7) % 400): pass
            c.close()
        t = threading.Thread(target=closer); t.start(); flag.set()
        try:
            r = c.recv(16); key = "ret:%r" % (r,)
        except OSError as e:
            key = "err:%d" % (e.errno or -1)
        progress["outcomes"][key] = progress["outcomes"].get(key, 0) + 1
        t.join(); c.close()
        if len(srv) > 64:
            for sc in srv: sc.close()
            del srv[:]
        progress["i"] = i + 1
    lst.close(); print("done: %s" % (progress["outcomes"],), flush=True)
threading.Thread(target=watchdog, daemon=True).start()
rc.fiber(main); rc.run()
