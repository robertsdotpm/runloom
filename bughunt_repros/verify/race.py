import os, sys, socket, threading
sys.path.insert(0, "/tmp/claude-1000/-home-x-projects-nat-simulator/d7b7a911-918e-435e-af6a-ee2aacf6c59d/scratchpad/pygo/src")
import runloom_c as rc
READ=1
# Race cancel_wait_fd against timeout/pump wake across hubs, with many parkers
# so the TLS freelist fills (64) and release() calls free(p) -> possible UAF of p->hub.
NHUB=8
NPARK=400
ROUNDS=60
handles=[None]*NPARK
socks=[]
def mk(i):
    def w():
        a,b=socket.socketpair()
        socks.append((a,b))
        handles[i]=rc.current_g()
        # short timeout -> pump/deadline sweep wakes+frees parker on this hub thread
        for _ in range(ROUNDS):
            rc.wait_fd(a.fileno(), READ, 1)   # 1ms timeout
        try: rc.netpoll_unregister(a.fileno())
        except Exception: pass
        a.close(); b.close()
    return w
def canceller():
    for _ in range(ROUNDS*NPARK):
        for h in handles:
            if h is not None:
                try: h.cancel_wait_fd()
                except Exception: pass
rc.mn_init(NHUB)
for i in range(NPARK):
    rc.mn_fiber(mk(i))
for _ in range(NHUB):
    rc.mn_fiber(canceller)
rc.mn_run()
rc.mn_fini()
print("DONE", rc._self_check(0))
