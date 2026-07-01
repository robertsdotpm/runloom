"""Same as repro_coro_resume_race but resume() calls fully SERIALIZED by a
threading.Lock -- distinguishes 'non-atomic executing guard' from 'cross-thread
resume is UB even when serialized'."""
import threading
import runloom_c

def body():
    for _ in range(200000):
        runloom_c.yield_()

c = runloom_c.Coro(body)
lk = threading.Lock()

def spin():
    while True:
        with lk:
            if c.done:
                return
            c.resume()

ts = [threading.Thread(target=spin) for _ in range(4)]
for t in ts: t.start()
for t in ts: t.join()
print("serialized cross-thread resume finished OK")
