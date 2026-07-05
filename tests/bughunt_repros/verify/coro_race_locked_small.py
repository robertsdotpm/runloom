import threading, time
import runloom_c

N = 20000
def body():
    for _ in range(N):
        runloom_c.yield_()

c = runloom_c.Coro(body)
lk = threading.Lock()

def spin():
    while True:
        with lk:
            if c.done:
                return
            c.resume()

t0 = time.time()
ts = [threading.Thread(target=spin) for _ in range(4)]
for t in ts: t.start()
for t in ts: t.join()
print('locked control ok, %.2fs for %d yields' % (time.time()-t0, N))
