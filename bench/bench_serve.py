import os, sys, time
sys.path.insert(0,"src")
os.environ.setdefault("RUNLOOM_SYSMON_QUIET","1")
import runloom, runloom_c
REAL_MONO=time.monotonic
N=int(sys.argv[1]); H=int(sys.argv[2]); DUR=float(sys.argv[3]); WARM=1.0
NSH=1<<16; MASK=NSH-1
rts=[0]*NSH; stop=[False]; conn_flags=bytearray(N); win=[0]
def root():
    def handle(conn):
        buf=bytearray(64)
        try:
            while not stop[0]:
                n=conn.recv_into(buf,8)
                if not n: break
                conn.send_all(memoryview(buf)[:n])
        except OSError: pass
        finally:
            try: conn.close()
            except OSError: pass
    port, listeners = runloom_c.serve("127.0.0.1", 0, handle, acceptors=H, backlog=min(N,65535))
    def client(idx):
        try: c=runloom_c.TCPConn.connect("127.0.0.1", port)
        except OSError: return
        conn_flags[idx]=1
        buf=bytearray(64); slot=idx&MASK
        try:
            while not stop[0]:
                c.send_all(b"hellopyg")
                n=c.recv_into(buf,8)
                if not n: break
                rts[slot]=rts[slot]+1
        except OSError: pass
        finally:
            try: c.close()
            except OSError: pass
    for i in range(N): runloom.go(client, i)
    def controller():
        t0=REAL_MONO()
        while sum(conn_flags)<N:
            runloom.sleep(0.01)
            if REAL_MONO()-t0>60: break
        est=sum(conn_flags); runloom.sleep(WARM)
        start=sum(rts); m0=REAL_MONO(); runloom.sleep(DUR)
        w=REAL_MONO()-m0; win[0]=sum(rts)-start; stop[0]=True
        for ln in listeners:
            try: ln.close()
            except OSError: pass
        print("SERVE N=%d est=%d/%d %.1fK req/s"%(N,est,N,win[0]/w/1000.0))
    runloom.go(controller)
    while not stop[0]: runloom.sleep(0.05)
runloom.run(H, root)
